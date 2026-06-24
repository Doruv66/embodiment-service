# Embodiment Service — Proof of Concept

**Virtual Human research project — Fontys ICT InnovationLab / MindLabs**

This service sits between the AI brain and the avatar renderer. It reads what the AI says, analyses the emotional content, and publishes a structured behaviour command that Unreal Engine can use to drive expressions on a MetaHuman avatar.

This proof of concept does not include Unreal Engine. It proves that the pipeline from Kafka input through emotion analysis to structured JSON output works correctly.

---

## Pipeline

```
vh.brain.output  ──▶  embodiment_service.py  ──▶  vh.embodiment.expression
  (AI text)              HuggingFace emotion            (JSON command)
                         classification +
                         expression mapping
```

1. The service consumes a plain text message from `vh.brain.output`
2. It sends the text to a local HuggingFace emotion model
3. The model returns scores for 7 emotions: joy, sadness, fear, anger, disgust, surprise, neutral
4. From that distribution it builds a **rich behaviour command**: one of 6 discrete expression values, the full score map, pre-computed MetaHuman blend-shape (morph) weights, a blend/transition time, and a per-sentence breakdown
5. The JSON behaviour command is published to `vh.embodiment.expression`

The richer payload (added on top of the original four-field contract) lets Unreal Engine drive smooth, blended facial animation instead of snapping a single held pose back to neutral. See [Output contract](#output-contract) for the full schema.

---

## Files

| File | Purpose |
|------|---------|
| `embodiment_service.py` | Main service — consumes, classifies, builds the command, publishes |
| `demo_producer.py` | Publishes 8 test sentences (incl. multi-sentence) to `vh.brain.output` |
| `test_mapping.py` | Unit tests for the mapping logic — no Kafka or model needed |
| `requirements.txt` | Pinned Python dependencies |

### Code structure

`embodiment_service.py` reads top to bottom as three classes plus two data types:

| Component | Responsibility |
|-----------|----------------|
| `EmotionAnalyzer` | Wraps the HuggingFace model; returns the 7 scores ordered high→low |
| `EmbodimentPipeline` | `text → ExpressionCommand` — the brain. The rules (`map_expression`, `transition_ms`, `morph_weights`, `segment`) are pure and unit-testable |
| `EmbodimentService` | The Kafka consume → publish loop |
| `Segment`, `ExpressionCommand` | Dataclasses for the data we emit (each owns its `to_dict()`) |

---

## How the emotion mapping works

The HuggingFace model (`j-hartmann/emotion-english-distilroberta-base`) returns a score for each of the 7 emotions. The mapping looks at the full distribution, not just the top label, because the model often places `neutral` on top even when meaningful emotional signals are present underneath.

### Rule 1 — A non-neutral emotion clearly leads

| Top emotion | Condition | Expression | Duration |
|-------------|-----------|------------|----------|
| joy | score ≥ 0.95 | `happy_high` | 4000 ms |
| joy | score < 0.95 | `happy_low` | 3000 ms |
| sadness | score ≥ 0.90 | `sad_high` | 4000 ms |
| sadness | score < 0.90, mixed unease > 0.25 | `concerned` | 3000 ms |
| sadness | score < 0.90, no mixed unease | `sad_low` | 3000 ms |
| fear / anger / disgust / surprise | any | `concerned` | 3000 ms |

**Why the thresholds are high (0.95 / 0.90):** The model scores even mild polite language very strongly on joy (e.g. "That is a nice idea" returns joy=0.887). Without a high threshold, almost every positive sentence maps to `happy_high`. The high threshold ensures only genuinely intense sentences reach the high-intensity expressions.

**Mixed unease rule:** When sadness is top but not dominant, the service checks whether disgust + anger + fear combined exceeds 0.25. If so, the sentence likely carries dread or unease rather than clean sadness — `concerned` is a better fit for the avatar than `sad_low`. Example: "Something feels wrong, I just have a bad feeling" (sadness=0.644, disgust=0.231, anger=0.079 → concerned).

### Rule 2 — Neutral leads, but something is underneath

When `neutral` is the top emotion, the service inspects the secondary signals:

| Secondary signal | Condition | Expression | Duration |
|-----------------|-----------|------------|----------|
| joy | > 0.15 | `happy_low` | 3000 ms |
| fear + sadness + anger + disgust | > 0.20, sadness leads | `sad_low` | 3000 ms |
| fear + sadness + anger + disgust | > 0.20, fear/anger leads | `concerned` | 3000 ms |
| nothing significant | — | `neutral` | 2000 ms |

This rule is what makes `concerned` reachable for empathetic or tense language that the model rates as superficially neutral.

### Rule 3 — Genuinely neutral

If neutral is top and nothing meaningful is underneath, the result is `neutral` (2000 ms).

---

## Output contract

Every message published to `vh.embodiment.expression` is a JSON string. The
**first four fields are the original contract and are unchanged** — existing
consumers keep working. The remaining fields are additive: a consumer that only
reads `value`/`duration_ms` can ignore them.

```json
{
  "type": "expression",
  "value": "happy_high",
  "duration_ms": 4000,
  "trigger_offset_ms": 0,
  "transition_ms": 200,
  "scores": {
    "joy": 0.9851,
    "surprise": 0.0051,
    "neutral": 0.0051,
    "sadness": 0.0022,
    "anger": 0.0012,
    "disgust": 0.001,
    "fear": 0.0003
  },
  "morph_weights": {
    "brow_raise": 0.103,
    "brow_furrow": 0.002,
    "lip_corner_pull": 0.888,
    "lip_corner_depress": 0.002,
    "cheek_raise": 0.691,
    "lid_tighten": 0.001
  },
  "segments": [
    {
      "text": "That is great!",
      "start_char": 0,
      "end_char": 14,
      "dominant_emotion": "joy",
      "scores": { "joy": 0.9377, "surprise": 0.031, "neutral": 0.0219, "...": 0.0 }
    },
    {
      "text": "Really, that changes everything.",
      "start_char": 15,
      "end_char": 47,
      "dominant_emotion": "surprise",
      "scores": { "surprise": 0.8111, "neutral": 0.1111, "anger": 0.0376, "...": 0.0 }
    },
    {
      "text": "I am so relieved.",
      "start_char": 48,
      "end_char": 65,
      "dominant_emotion": "joy",
      "scores": { "joy": 0.9894, "sadness": 0.0049, "neutral": 0.0021, "...": 0.0 }
    }
  ],
  "sentence_count": 3
}
```

### Original contract (unchanged)

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"expression"` |
| `value` | string | One of: `happy_high`, `happy_low`, `sad_high`, `sad_low`, `concerned`, `neutral` |
| `duration_ms` | int | How long Unreal Engine holds the expression before blending back to neutral |
| `trigger_offset_ms` | int | Always `0` in this prototype (full system will calculate from word timing) |

### New fields

| Field | Type | Description |
|-------|------|-------------|
| `transition_ms` | int | How long UE should lerp (blend) from the current pose into the new `morph_weights`. Short for confident emotions, long for ambiguous ones — see below |
| `scores` | object | The full emotion distribution from the model — all 7 labels, ordered high→low, rounded to 4 decimals. Lets UE blend or react to secondary emotions, not just the winning label |
| `morph_weights` | object | Six pre-computed MetaHuman blend-shape weights (`0.0`–`1.0`). UE applies these directly to morph targets, so the face is shaped by the whole emotion mix rather than one canned pose |
| `segments` | array | Per-sentence breakdown of the input (see below). Empty/one entry for a single sentence; one entry per sentence otherwise |
| `sentence_count` | int | Number of sentences detected — `len(segments)`. Lets UE decide whether to schedule expression changes mid-speech |

### `morph_weights` — how they are derived

Each blend-shape weight is `Σ(emotion_score × contribution)` across its
contributing emotions, clamped to `0.0–1.0`. The contribution matrix lives in
`MORPH_MAPPING` at the top of `embodiment_service.py`:

| Morph target | Driven by (emotion × contribution) |
|--------------|-------------------------------------|
| `brow_raise` | surprise ×0.9, fear ×0.6, joy ×0.1 |
| `brow_furrow` | anger ×0.8, disgust ×0.5, sadness ×0.4 |
| `lip_corner_pull` | joy ×0.9, surprise ×0.2 |
| `lip_corner_depress` | sadness ×0.8, anger ×0.3, disgust ×0.3 |
| `cheek_raise` | joy ×0.7, surprise ×0.3 |
| `lid_tighten` | anger ×0.6, disgust ×0.4, fear ×0.3 |

Example: `brow_raise = min(1.0, surprise·0.9 + fear·0.6 + joy·0.1)`.

### `transition_ms` — how it is chosen

Based on how confident the dominant emotion is. A clear emotion snaps quickly; an
ambiguous/mixed one eases in slowly so the face does not twitch.

| Dominant score | `transition_ms` | Feel |
|----------------|-----------------|------|
| > 0.8 | 200 | Sharp, fast snap — unmistakable emotion |
| > 0.6 | 300 | Default |
| ≥ 0.4 | 400 | Somewhat mixed |
| < 0.4 | 500 | Ambiguous — slow, calm blend |

### `segments` — per-sentence breakdown

The input is split on `.`, `?`, `!` and each sentence is classified
independently. This lets UE queue expression changes **mid-speech** and follow an
emotional arc that the whole-utterance label would average away.

| Field | Type | Description |
|-------|------|-------------|
| `text` | string | The sentence (trimmed) |
| `start_char` / `end_char` | int | Character offsets into the original input |
| `dominant_emotion` | string | Highest-scoring emotion label for that sentence |
| `scores` | object | Full 7-emotion distribution for that sentence |

> **Note on the top-level `value` vs `segments`:** the top-level `value` reflects
> the model's read of the *whole* utterance, which can average out an arc. For
> *"I was worried at first. But now I see it. This is actually good news."* the
> top-level `value` is `concerned` (a blend of surprise/joy), while `segments`
> correctly capture the arc **fear → surprise → joy**. UE can prefer the segment
> stream when it wants the avatar to follow the sentence-by-sentence emotion.

---

## Setup

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

The HuggingFace model downloads ~300 MB of weights on first run. Subsequent runs load from cache instantly.

**2. Kafka**

A Kafka broker must be running on `localhost:9092`. The project uses a Docker container named `kafka-poc`.

---

## Running the demo

Start Terminal 1 before Terminal 3 — the consumer uses `latest` offset so it only sees messages published after it is listening.

**Terminal 1 — start the service:**
```bash
cd "/Users/doruvieru/Desktop/VirtualHuman/Embodiment Service"
python3 embodiment_service.py
```
Wait for `Waiting for messages...` before continuing. The service deliberately
joins the consumer group and seeks to the tail *before* printing that line, so
once you see it, no messages will be skipped by the `latest`-offset reset.

> If messages seem to vanish, check for a stale service instance still holding
> the single-partition consumer group: `pkill -f embodiment_service.py`.

**Terminal 2 — watch the output topic (proves JSON lands on Kafka):**
```bash
docker exec kafka-poc /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic vh.embodiment.expression
```

**Terminal 3 — fire the 8 test sentences:**
```bash
cd "/Users/doruvieru/Desktop/VirtualHuman/Embodiment Service"
python3 demo_producer.py
```

---

## Testing the mapping without Kafka

`test_mapping.py` feeds pre-built emotion distributions directly into the mapping function and prints a pass/fail table. No Kafka broker or HuggingFace model needed — runs in under a second.

```bash
cd "/Users/doruvieru/Desktop/VirtualHuman/Embodiment Service"
python3 test_mapping.py
```

Use this whenever you adjust a threshold in `embodiment_service.py` to check immediately whether it improved or broke something.

**Thresholds to tune (top of `embodiment_service.py`):**

| Constant | Default | Effect |
|----------|---------|--------|
| `JOY_HIGH` | 0.95 | Raise to make `happy_high` harder to reach |
| `SADNESS_HIGH` | 0.90 | Raise to make `sad_high` harder to reach |
| `POSITIVE` | 0.15 | Raise to require stronger joy before overriding neutral |
| `NEGATIVE` | 0.20 | Raise to require stronger negative signal before overriding neutral |
| `MIXED_NEGATIVE` | 0.25 | Raise to require more mixed unease before routing to `concerned` |

---

## Why HuggingFace over Google Natural Language API

Both backends were evaluated during development. Key findings:

| | Google NL API | HuggingFace (j-hartmann) |
|--|--------------|--------------------------|
| Output | Score (−1 to +1) + magnitude | 7 labelled emotion scores |
| "Alright I suppose" | `happy_high` (score 0.82) | `neutral` (correct) |
| "Must have been difficult" | `sad_high` (too intense) | `concerned` (defensible) |
| "We need to talk now" | `happy_low` (wrong direction) | `neutral` (correct direction) |
| Reasoning visible | No — single number | Yes — full distribution |

HuggingFace was chosen because:
- It understands that hedging language ("I suppose", "I think") is not strongly positive
- The full emotion distribution makes the reasoning transparent and tunable
- Discrete emotion labels map more naturally to avatar expression categories than a raw polarity score
- The model runs locally with no API key or network dependency

The main remaining limitation is that both models struggle with **urgency** ("we need to talk now") and **empathetic listening** ("I understand, that must have been difficult") — sentences that a human reads as emotionally loaded but which contain no emotionally strong words. This is the core motivation for exploring the PAD (Pleasure-Arousal-Dominance) model as a next step.
