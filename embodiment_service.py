"""
Embodiment Service — Proof of Concept
Virtual Human research project (Fontys ICT InnovationLab / MindLabs)

    vh.brain.output ─▶ classify emotions ─▶ build behaviour command ─▶ vh.embodiment.expression

The command carries a discrete expression *plus* blend-shape weights and a
per-sentence breakdown, so the MetaHuman blends smoothly instead of holding one
pose. Read this file top to bottom:

    settings              all tunables in one place
    Segment / Command     the data we produce
    EmotionAnalyzer       wraps the HuggingFace model
    EmbodimentPipeline    text → ExpressionCommand   (the brain, no Kafka)
    EmbodimentService     the Kafka consume → publish loop

Run:
    python3 embodiment_service.py
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

EmotionScores = Dict[str, float]          # {label: score}, ordered high → low


# --- Settings ---------------------------------------------------------------

HF_MODEL     = "j-hartmann/emotion-english-distilroberta-base"
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC  = "vh.brain.output"
OUTPUT_TOPIC = "vh.embodiment.expression"
GROUP_ID     = "embodiment-service"

# Mapping thresholds — tune these when comparing model output.
# UE plugin moods: Neutral Happy Sad Disgust Anger Surprise Fear
#                  Confident Excited Bored Playful Confused
JOY_EXCITED          = 0.60   # joy >= this + notable surprise    → Excited
JOY_CONFIDENT        = 0.65   # joy >= this + low negatives       → Confident
JOY_PLAYFUL          = 0.50   # joy >= this + mild surprise       → Playful (raised; inferred mood needs clear joy)
SURPRISE_EXCITED     = 0.15   # surprise threshold alongside joy  → Excited
SURPRISE_PLAYFUL     = 0.08   # surprise threshold alongside joy  → Playful
SADNESS_SAD          = 0.70   # sadness at/above this             → Sad (below → Bored)
FEAR_CONFUSED        = 0.20   # fear co-occurring with surprise   → Confused
CONFIDENCE_NEG_MAX   = 0.15   # max combined negatives for       → Confident
NEUTRAL_JOY_MIN      = 0.15   # joy below neutral needed for     → Happy
NEUTRAL_NEG_MIN      = 0.25   # combined negative below neutral  → non-neutral (raised to cut noise)

# Confidence gates — prevent weak/ambiguous model output from driving expressions.
# The HF model has 7 classes; random baseline ≈ 0.143 each.
MIN_CONFIDENCE       = 0.45   # top emotion must reach this to fire (non-neutral)
MIN_MARGIN           = 0.12   # top must lead second place by at least this much

# Emotion scores → MetaHuman blend-shape weights.
MORPH_MAPPING = {
    "brow_raise":         {"surprise": 0.9, "fear": 0.6, "joy": 0.1},
    "brow_furrow":        {"anger": 0.8, "sadness": 0.4, "disgust": 0.5},
    "lip_corner_pull":    {"joy": 0.9, "surprise": 0.2},
    "lip_corner_depress": {"sadness": 0.8, "anger": 0.3, "disgust": 0.3},
    "cheek_raise":        {"joy": 0.7, "surprise": 0.3},
    "lid_tighten":        {"anger": 0.6, "disgust": 0.4, "fear": 0.3},
}


# --- Data we produce --------------------------------------------------------

@dataclass
class Segment:
    """One sentence of the input with its own emotion read-out."""
    text: str
    start_char: int
    end_char: int
    dominant_emotion: str
    scores: EmotionScores

    def to_dict(self) -> dict:
        return vars(self)                 # field order already matches the wire format


@dataclass
class ExpressionCommand:
    """
    Behaviour command for UE. The first four wire fields preserve the original
    consumer contract; the rest let UE drive a smooth, blended face.
    """
    value: str
    duration_ms: int
    transition_ms: int
    scores: EmotionScores
    morph_weights: Dict[str, float]
    segments: List[Segment] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "type": "expression",
            "value": self.value,
            "duration_ms": self.duration_ms,
            "trigger_offset_ms": 0,
            "transition_ms": self.transition_ms,
            "scores": self.scores,
            "morph_weights": self.morph_weights,
            "segments": [s.to_dict() for s in self.segments],
            "sentence_count": len(self.segments),
        }


# --- Emotion model ----------------------------------------------------------

class EmotionAnalyzer:
    """Wraps the HuggingFace classifier; returns scores ordered high → low."""

    def __init__(self, model_name: str = HF_MODEL):
        from transformers import pipeline          # heavy import, kept local
        print(f"[INIT] Loading model {model_name} (first run downloads ~300 MB)...")
        self._classify = pipeline("text-classification", model=model_name, top_k=None)
        print("[INIT] Model ready.\n")

    def scores(self, text: str) -> EmotionScores:
        results = self._classify(text)[0]
        results.sort(key=lambda r: r["score"], reverse=True)
        return {r["label"]: round(r["score"], 4) for r in results}


# --- The brain: text → command ----------------------------------------------

class EmbodimentPipeline:
    """
    Turns text into an ExpressionCommand. Knows nothing about Kafka, so it can
    be unit-tested directly (see test_mapping.py). The emotion→behaviour rules
    are static methods — pure functions of the score map.
    """

    def __init__(self, analyzer: EmotionAnalyzer):
        self._analyzer = analyzer

    def process(self, text: str) -> ExpressionCommand:
        scores = self._analyzer.scores(text)
        value, duration_ms = self.map_expression(scores)
        return ExpressionCommand(
            value=value,
            duration_ms=duration_ms,
            transition_ms=self.transition_ms(scores),
            scores=scores,
            morph_weights=self.morph_weights(scores),
            segments=self.segment(text),
        )

    @staticmethod
    def map_expression(scores: EmotionScores) -> Tuple[str, int]:
        """
        Map the emotion distribution to (expression, duration_ms).
        UE plugin moods: Neutral Happy Sad Disgust Anger Surprise Fear
                         Confident Excited Bored Playful Confused
        """
        top = next(iter(scores))

        vals       = list(scores.values())
        top_score  = vals[0]
        margin     = top_score - (vals[1] if len(vals) > 1 else 0.0)

        joy      = scores.get("joy", 0)
        sadness  = scores.get("sadness", 0)
        fear     = scores.get("fear", 0)
        anger    = scores.get("anger", 0)
        disgust  = scores.get("disgust", 0)
        surprise = scores.get("surprise", 0)

        # Confused is a two-emotion co-occurrence: neither fear nor surprise needs to
        # dominate individually, but together they must be substantial.
        if fear >= FEAR_CONFUSED and surprise >= FEAR_CONFUSED and fear + surprise >= 0.60:
            return "Confused", 3000

        # Gate: if the model isn't confident, don't express anything.
        if top != "neutral" and (top_score < MIN_CONFIDENCE or margin < MIN_MARGIN):
            return "Neutral", 2000

        if top == "joy":
            if joy >= JOY_EXCITED and surprise >= SURPRISE_EXCITED:
                return "Excited", 4000
            if joy >= JOY_PLAYFUL and surprise >= SURPRISE_PLAYFUL:
                return "Playful", 3000
            return "Happy", 3000

        if top == "sadness":
            mixed_neg = disgust + anger
            if sadness >= SADNESS_SAD:
                return "Sad", 4000
            if mixed_neg > 0.25:
                return ("Disgust", 3000) if disgust > anger else ("Anger", 3000)
            return "Bored", 3000

        if top == "anger":
            return "Anger", 3500

        if top == "disgust":
            return "Disgust", 3500

        if top == "fear":
            return ("Confused", 3000) if surprise >= FEAR_CONFUSED else ("Fear", 3500)

        if top == "surprise":
            return ("Confused", 3000) if fear >= FEAR_CONFUSED else ("Surprise", 3000)

        # neutral leads — look at what is underneath
        if joy > NEUTRAL_JOY_MIN:
            negatives = fear + anger + disgust
            if negatives < CONFIDENCE_NEG_MAX and surprise < SURPRISE_PLAYFUL:
                return "Confident", 3000   # calm positive assurance
            return ("Playful", 3000) if surprise >= SURPRISE_PLAYFUL else ("Happy", 3000)
        if sadness + fear + anger + disgust > NEUTRAL_NEG_MIN:
            if fear >= 0.10 and surprise >= 0.10:
                return "Confused", 3000
            if sadness >= anger and sadness >= disgust:
                return "Bored", 3000
            return ("Anger", 3000) if anger > disgust else ("Disgust", 3000)
        return "Neutral", 2000

    @staticmethod
    def transition_ms(scores: EmotionScores) -> int:
        """Lerp time into the new pose: confident emotion = fast snap, ambiguous = slow blend."""
        dominant = max(scores.values(), default=0.0)
        if dominant > 0.8:
            return 200
        if dominant > 0.6:
            return 300
        if dominant >= 0.4:
            return 400
        return 500

    @staticmethod
    def morph_weights(scores: EmotionScores) -> Dict[str, float]:
        """Each blend-shape weight = Σ(emotion_score × contribution), clamped to 0.0–1.0."""
        return {
            morph: round(min(1.0, sum(scores.get(e, 0.0) * w for e, w in contrib.items())), 3)
            for morph, contrib in MORPH_MAPPING.items()
        }

    def segment(self, text: str) -> List[Segment]:
        """One Segment per sentence (single-sentence input yields one segment)."""
        segments = []
        for sentence, start, end in self._sentences(text):
            scores = self._analyzer.scores(sentence)
            dominant = next(iter(scores), "neutral")
            segments.append(Segment(sentence, start, end, dominant, scores))
        return segments

    @staticmethod
    def _sentences(text: str) -> List[Tuple[str, int, int]]:
        """Split on . ? ! into (sentence, start_char, end_char), keeping offsets."""
        bounds, start = [], 0
        for match in re.finditer(r"[.?!]+", text):
            bounds.append((start, match.end()))
            start = match.end()
        if start < len(text):                       # trailing text, no terminator
            bounds.append((start, len(text)))

        spans = []
        for begin, finish in bounds:
            chunk = text[begin:finish]
            stripped = chunk.strip()
            if stripped:
                offset = begin + (len(chunk) - len(chunk.lstrip()))
                spans.append((stripped, offset, offset + len(stripped)))
        return spans


# --- The plumbing: Kafka loop -----------------------------------------------

class EmbodimentService:
    """Consume text from Kafka, run it through the pipeline, publish the command."""

    def __init__(self, pipeline: EmbodimentPipeline, broker: str = KAFKA_BROKER,
                 in_topic: str = INPUT_TOPIC, out_topic: str = OUTPUT_TOPIC,
                 group_id: str = GROUP_ID):
        self._pipeline = pipeline
        self._broker = broker
        self._in_topic = in_topic
        self._out_topic = out_topic
        self._group_id = group_id
        self._consumer = None
        self._producer = None

    def run(self) -> None:
        from kafka import KafkaConsumer, KafkaProducer
        self._consumer = KafkaConsumer(
            self._in_topic, bootstrap_servers=self._broker,
            auto_offset_reset="latest", group_id=self._group_id,
            value_deserializer=lambda v: v.decode("utf-8"),
        )
        self._producer = KafkaProducer(
            bootstrap_servers=self._broker,
            value_serializer=lambda v: v.encode("utf-8"),
        )
        # Join the group and seek to the tail *before* we report ready, otherwise
        # the latest-offset reset silently skips messages produced in the gap.
        while not self._consumer.assignment():
            self._consumer.poll(timeout_ms=500)

        print("Waiting for messages... (Ctrl+C to stop)\n")
        try:
            for message in self._consumer:
                self._handle(message.value)
        except KeyboardInterrupt:
            print("\nShutting down.")
        finally:
            self._consumer.close()
            self._producer.close()

    def _handle(self, text: str) -> None:
        print("\n" + "=" * 60)
        print(f"[1. RECEIVED]   {text!r}")
        try:
            command = self._pipeline.process(text)
        except Exception as exc:
            print(f"[SKIP] Could not process message: {exc}")
            return

        emotions = "  ".join(f"{label}={score:.3f}" for label, score in command.scores.items())
        morphs   = "  ".join(f"{morph}={weight:.3f}" for morph, weight in command.morph_weights.items())
        print(f"[2. EMOTIONS]   {emotions}")
        print(f"[3. SEGMENTS]   {len(command.segments)} sentence(s) detected")
        print(f"[4. MORPHS]     {morphs}")
        print(f"[5. MAPPED]     value={command.value}  duration_ms={command.duration_ms}  "
              f"transition_ms={command.transition_ms}")

        payload = command.to_dict()
        print(f"[6. PUBLISHING] {self._out_topic}:")
        print(json.dumps(payload, indent=2))                # pretty to the console
        self._producer.send(self._out_topic, json.dumps(payload))   # minified to Kafka
        self._producer.flush()
        print("[6. PUBLISHED]  OK")


# --- Entry point ------------------------------------------------------------

def main() -> None:
    print("Embodiment Service — HuggingFace emotion backend")
    print(f"  broker {KAFKA_BROKER}   |   {INPUT_TOPIC} → {OUTPUT_TOPIC}\n")

    try:
        service = EmbodimentService(EmbodimentPipeline(EmotionAnalyzer()))
    except Exception as exc:
        sys.exit(f"[FATAL] Could not load model: {exc}\n        Run: pip install transformers torch")

    try:
        service.run()
    except Exception as exc:
        sys.exit(f"[FATAL] Kafka error: {exc}\n        Is a broker running at {KAFKA_BROKER}?")


if __name__ == "__main__":
    main()
