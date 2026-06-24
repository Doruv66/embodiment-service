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
4. From that distribution it builds a **rich behaviour command**: one of 12 discrete expression values, the full score map, pre-computed MetaHuman blend-shape (morph) weights, a blend/transition time, and a per-sentence breakdown
5. The JSON behaviour command is published to `vh.embodiment.expression`

The richer payload lets Unreal Engine drive smooth, blended facial animation instead of snapping a single held pose back to neutral. See [Output contract](#output-contract) for the full schema.

---

## Files

| File | Purpose |
|------|---------|
| `embodiment_service.py` | Main service — consumes, classifies, builds the command, publishes |
| `demo_producer.py` | Publishes test sentences to `vh.brain.output` |
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

## Supported UE plugin moods

The `value` field in the output maps to exactly these 12 moods supported by the UE plugin:

| Mood | Driven by |
|------|-----------|
| `Happy` | joy dominant, no notable surprise |
| `Excited` | joy ≥ 0.60 and surprise ≥ 0.15 |
| `Playful` | joy ≥ 0.50 and surprise ≥ 0.08 |
| `Confident` | neutral leads, calm joy underneath, low negatives and surprise |
| `Sad` | sadness dominant and ≥ 0.70 |
| `Bored` | sadness dominant but < 0.70, no strong secondary negatives |
| `Disgust` | disgust dominant, or sadness top with strong disgust underneath |
| `Anger` | anger dominant, or neutral leads with anger heading the negative cluster |
| `Fear` | fear dominant, surprise too low to suggest confusion |
| `Surprise` | surprise dominant, fear too low to suggest confusion |
| `Confused` | fear ≥ 0.20 **and** surprise ≥ 0.20 **and** combined ≥ 0.60 (co-occurrence, not dominance) |
| `Neutral` | neutral leads with nothing significant underneath, or confidence gate not met |

---

## How the emotion mapping works

The HuggingFace model (`j-hartmann/emotion-english-distilroberta-base`) returns a score for each of the 7 emotions. The mapping looks at the full distribution, not just the top label.

### Confidence gate (false positive guard)

The model has 7 classes; the random baseline is ~0.143 each. Before mapping any non-neutral top emotion, two conditions must both be met:

| Constant | Value | Rule |
|----------|-------|------|
| `MIN_CONFIDENCE` | 0.45 | Top emotion score must reach this (~3× random baseline) |
| `MIN_MARGIN` | 0.12 | Top must beat second place by at least this much |

If either condition fails, the output is `Neutral`. This prevents weak or near-tied model output (e.g. joy=0.28, or joy=0.46 vs neutral=0.40) from driving expressions.

**Exception — Confused:** Confused is a two-emotion co-occurrence; neither fear nor surprise needs to dominate individually. It is checked *before* the gate: if `fear ≥ 0.20 and surprise ≥ 0.20 and fear + surprise ≥ 0.60`, the result is `Confused` regardless of which emotion tops.

### Rule 1 — Non-neutral tops (after confidence gate)

| Top emotion | Condition | Expression |
|-------------|-----------|------------|
| joy | joy ≥ 0.60 and surprise ≥ 0.15 | `Excited` |
| joy | joy ≥ 0.50 and surprise ≥ 0.08 | `Playful` |
| joy | anything else | `Happy` |
| sadness | sadness ≥ 0.70 | `Sad` |
| sadness | sadness < 0.70, disgust + anger > 0.25 | `Disgust` or `Anger` |
| sadness | sadness < 0.70, no strong secondaries | `Bored` |
| anger | — | `Anger` |
| disgust | — | `Disgust` |
| fear | surprise ≥ 0.20 | `Confused` |
| fear | surprise < 0.20 | `Fear` |
| surprise | fear ≥ 0.20 | `Confused` |
| surprise | fear < 0.20 | `Surprise` |

### Rule 2 — Neutral leads, but something is underneath

| Secondary signal | Condition | Expression |
|-----------------|-----------|------------|
| joy | > 0.15, low negatives and surprise | `Confident` |
| joy | > 0.15, notable surprise | `Playful` |
| joy | > 0.15, everything else | `Happy` |
| fear + sadness + anger + disgust | > 0.25, fear + surprise both ≥ 0.10 | `Confused` |
| fear + sadness + anger + disgust | > 0.25, sadness leads | `Bored` |
| fear + sadness + anger + disgust | > 0.25, anger leads | `Anger` |
| fear + sadness + anger + disgust | > 0.25, disgust leads | `Disgust` |
| nothing significant | — | `Neutral` |

---

## Output contract

Every message published to `vh.embodiment.expression` is a JSON string.

```json
{
  "type": "expression",
  "value": "Excited",
  "duration_ms": 4000,
  "trigger_offset_ms": 0,
  "transition_ms": 200,
  "scores": {
    "joy": 0.8090,
    "surprise": 0.1590,
    "neutral": 0.0200,
    "anger": 0.0060,
    "sadness": 0.0040,
    "fear": 0.0020,
    "disgust": 0.0010
  },
  "morph_weights": {
    "brow_raise": 0.238,
    "brow_furrow": 0.007,
    "lip_corner_pull": 0.761,
    "lip_corner_depress": 0.007,
    "cheek_raise": 0.614,
    "lid_tighten": 0.006
  },
  "segments": [
    {
      "text": "Just got a promotion — excited but also surprised.",
      "start_char": 0,
      "end_char": 49,
      "dominant_emotion": "joy",
      "scores": { "joy": 0.809, "surprise": 0.159, "...": 0.0 }
    }
  ],
  "sentence_count": 1
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"expression"` |
| `value` | string | One of the 12 UE plugin moods (see table above) |
| `duration_ms` | int | How long UE holds the expression before blending back to neutral |
| `trigger_offset_ms` | int | Always `0` in this prototype |
| `transition_ms` | int | How long UE lerps into the new pose. Short for confident emotions, long for ambiguous ones |
| `scores` | object | Full 7-emotion distribution — all labels, ordered high→low, rounded to 4 decimals |
| `morph_weights` | object | Six pre-computed MetaHuman blend-shape weights (`0.0–1.0`) |
| `segments` | array | Per-sentence breakdown (see below) |
| `sentence_count` | int | Number of sentences detected |

### `morph_weights` — how they are derived

Each blend-shape weight is `Σ(emotion_score × contribution)` clamped to `0.0–1.0`. The contribution matrix lives in `MORPH_MAPPING` at the top of `embodiment_service.py`:

| Morph target | Driven by |
|--------------|-----------|
| `brow_raise` | surprise ×0.9, fear ×0.6, joy ×0.1 |
| `brow_furrow` | anger ×0.8, disgust ×0.5, sadness ×0.4 |
| `lip_corner_pull` | joy ×0.9, surprise ×0.2 |
| `lip_corner_depress` | sadness ×0.8, anger ×0.3, disgust ×0.3 |
| `cheek_raise` | joy ×0.7, surprise ×0.3 |
| `lid_tighten` | anger ×0.6, disgust ×0.4, fear ×0.3 |

### `transition_ms` — how it is chosen

| Dominant score | `transition_ms` | Feel |
|----------------|-----------------|------|
| > 0.8 | 200 | Sharp snap — unmistakable emotion |
| > 0.6 | 300 | Default |
| ≥ 0.4 | 400 | Somewhat mixed |
| < 0.4 | 500 | Ambiguous — slow blend |

### `segments` — per-sentence breakdown

The input is split on `.`, `?`, `!` and each sentence is classified independently. This lets UE queue expression changes mid-speech and follow an emotional arc that the whole-utterance label would average away.

| Field | Type | Description |
|-------|------|-------------|
| `text` | string | The sentence (trimmed) |
| `start_char` / `end_char` | int | Character offsets into the original input |
| `dominant_emotion` | string | Highest-scoring emotion label for that sentence |
| `scores` | object | Full 7-emotion distribution for that sentence |

---

## Thresholds reference

All tunables live at the top of `embodiment_service.py`. Run `python3 test_mapping.py` after any change to see immediately whether it improved or broke something.

| Constant | Default | Effect |
|----------|---------|--------|
| `MIN_CONFIDENCE` | 0.45 | Lower to fire expressions from weaker model output |
| `MIN_MARGIN` | 0.12 | Lower to allow near-tied emotion races to fire |
| `JOY_EXCITED` | 0.60 | Raise to make Excited harder to reach |
| `JOY_PLAYFUL` | 0.50 | Raise to make Playful harder to reach |
| `SURPRISE_EXCITED` | 0.15 | Raise to require stronger surprise alongside joy for Excited |
| `SURPRISE_PLAYFUL` | 0.08 | Raise to require stronger surprise alongside joy for Playful |
| `SADNESS_SAD` | 0.70 | Raise to make Sad harder; more sadness falls through to Bored |
| `FEAR_CONFUSED` | 0.20 | Raise to require stronger fear+surprise co-occurrence for Confused |
| `CONFIDENCE_NEG_MAX` | 0.15 | Lower to make Confident (neutral-led) harder to reach |
| `NEUTRAL_JOY_MIN` | 0.15 | Raise to require stronger joy below neutral |
| `NEUTRAL_NEG_MIN` | 0.25 | Raise to require stronger negative signal below neutral |

---

## Setup

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

The HuggingFace model downloads ~300 MB of weights on first run. Subsequent runs load from cache.

**2. Kafka**

A Kafka broker must be running on `localhost:9092`. The project uses a Docker container named `kafka-poc`.

---

## Running the demo

Start Terminal 1 before Terminal 3 — the consumer uses `latest` offset so it only sees messages published after it is listening.

**Terminal 1 — start the service:**
```bash
python3 embodiment_service.py
```
Wait for `Waiting for messages...` before continuing.

**Terminal 2 — watch the output topic:**
```bash
docker exec kafka-poc /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic vh.embodiment.expression
```

**Terminal 3 — fire test sentences:**
```bash
python3 demo_producer.py
```

---

## Testing the mapping without Kafka

`test_mapping.py` feeds pre-built emotion distributions directly into the mapping function and prints a pass/fail table. No Kafka broker or HuggingFace model needed — runs in under a second.

```bash
python3 test_mapping.py
```

---

## Why HuggingFace over Google Natural Language API

Both backends were evaluated during development. Key findings:

| | Google NL API | HuggingFace (j-hartmann) |
|--|--------------|--------------------------|
| Output | Score (−1 to +1) + magnitude | 7 labelled emotion scores |
| "Alright I suppose" | `Happy` (score 0.82) | `Neutral` (correct) |
| "Must have been difficult" | `Sad` (too intense) | `Bored` (defensible) |
| "We need to talk now" | `Happy` (wrong direction) | `Neutral` (correct direction) |
| Reasoning visible | No — single number | Yes — full distribution |

HuggingFace was chosen because:
- It understands that hedging language is not strongly positive
- The full emotion distribution makes reasoning transparent and tunable
- Discrete emotion labels map naturally to the 12 UE plugin moods
- The model runs locally with no API key or network dependency
