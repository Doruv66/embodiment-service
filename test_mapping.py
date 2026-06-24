"""
Mapping unit tests — no Kafka or HuggingFace model required.

Feeds pre-built emotion distributions directly into
EmbodimentPipeline.map_expression() and checks the result against the intended
expression. No Kafka or model is needed — the mapping rules are pure static
methods. Run this whenever you change the thresholds in embodiment_service.py
to see immediately whether the mapping improved or regressed.

UE plugin moods: Neutral Happy Sad Disgust Anger Surprise Fear
                 Confident Excited Bored Playful Confused

Run:
  python3 test_mapping.py
"""

from embodiment_service import EmbodimentPipeline

# Each case: (description, emotion_dict, expected_expression)
# Emotion values are representative of what HuggingFace actually returns
# for sentences in this category.
TEST_CASES = [

    # --- Happy: joy dominant, no notable surprise ----------------------------
    (
        "So happy right now, best day ever",
        {"joy": 0.971, "surprise": 0.018, "neutral": 0.006,
         "anger": 0.002, "sadness": 0.001, "disgust": 0.001, "fear": 0.001},
        "Happy",
    ),
    (
        "That is a nice idea, could work out well",
        {"joy": 0.887, "neutral": 0.090, "surprise": 0.011,
         "disgust": 0.005, "sadness": 0.004, "anger": 0.003, "fear": 0.001},
        "Happy",
    ),

    # --- Excited: joy + notable surprise -------------------------------------
    (
        "Just got a promotion — excited but also surprised",
        {"joy": 0.809, "surprise": 0.159, "neutral": 0.020,
         "anger": 0.006, "sadness": 0.004, "fear": 0.002, "disgust": 0.001},
        "Excited",
    ),
    (
        "Oh wow, incredible news I did not see coming",
        {"joy": 0.742, "surprise": 0.198, "neutral": 0.030,
         "fear": 0.015, "sadness": 0.008, "anger": 0.004, "disgust": 0.003},
        "Excited",
    ),

    # --- Playful: moderate joy + mild surprise -------------------------------
    (
        "Let's try something fun and a little silly",
        {"joy": 0.540, "surprise": 0.120, "neutral": 0.180,
         "sadness": 0.060, "fear": 0.040, "anger": 0.030, "disgust": 0.030},
        "Playful",
    ),

    # --- Confident: neutral leads, joy underneath, low negatives/surprise ----
    (
        "We have a solid plan and I know we can deliver",
        {"neutral": 0.580, "joy": 0.220, "surprise": 0.030,
         "sadness": 0.060, "fear": 0.040, "anger": 0.040, "disgust": 0.030},
        "Confident",
    ),
    (
        "Appreciate your help, neutral leads, calm positive undertone",
        {"neutral": 0.502, "joy": 0.231, "surprise": 0.050,
         "sadness": 0.060, "anger": 0.050, "disgust": 0.030, "fear": 0.017},
        "Confident",
    ),

    # --- Sad: strong sorrow --------------------------------------------------
    (
        "Devastating, heartbroken by what happened",
        {"sadness": 0.983, "surprise": 0.007, "neutral": 0.004,
         "fear": 0.002, "disgust": 0.002, "anger": 0.001, "joy": 0.001},
        "Sad",
    ),
    (
        "I can't believe she is gone, completely lost without her",
        {"sadness": 0.920, "surprise": 0.035, "fear": 0.025,
         "neutral": 0.010, "disgust": 0.005, "anger": 0.003, "joy": 0.002},
        "Sad",
    ),
    (
        "Things have been tough lately, not feeling like myself",
        {"sadness": 0.873, "neutral": 0.048, "fear": 0.038,
         "disgust": 0.024, "joy": 0.012, "anger": 0.004, "surprise": 0.002},
        "Sad",
    ),

    # --- Bored: mild sadness, low engagement ---------------------------------
    (
        "Must have been difficult — neutral leads, sadness underneath",
        {"neutral": 0.572, "fear": 0.182, "sadness": 0.153,
         "disgust": 0.042, "surprise": 0.028, "anger": 0.012, "joy": 0.011},
        "Bored",
    ),
    (
        "Whatever, it does not really matter I guess",
        {"sadness": 0.480, "neutral": 0.320, "disgust": 0.080,
         "anger": 0.060, "joy": 0.030, "fear": 0.020, "surprise": 0.010},
        "Bored",
    ),

    # --- Disgust: disgust dominant --------------------------------------------
    (
        "That is absolutely revolting, I can't stand it",
        {"disgust": 0.912, "anger": 0.045, "neutral": 0.022,
         "sadness": 0.011, "fear": 0.006, "surprise": 0.002, "joy": 0.002},
        "Disgust",
    ),
    (
        "Something feels wrong — sadness top but disgust underneath",
        {"sadness": 0.644, "disgust": 0.231, "anger": 0.079,
         "neutral": 0.026, "fear": 0.015, "surprise": 0.003, "joy": 0.002},
        "Disgust",
    ),

    # --- Anger ---------------------------------------------------------------
    (
        "This is completely unacceptable — I am furious",
        {"anger": 0.941, "disgust": 0.032, "neutral": 0.015,
         "sadness": 0.007, "fear": 0.003, "surprise": 0.001, "joy": 0.001},
        "Anger",
    ),
    (
        "Tension underneath neutral — anger leads the negative cluster",
        {"neutral": 0.430, "anger": 0.210, "fear": 0.180,
         "disgust": 0.090, "sadness": 0.050, "surprise": 0.025, "joy": 0.015},
        "Anger",
    ),

    # --- Fear ----------------------------------------------------------------
    (
        "Worried about what might happen — fear clearly dominant",
        {"fear": 0.985, "sadness": 0.004, "neutral": 0.003,
         "surprise": 0.003, "anger": 0.003, "joy": 0.001, "disgust": 0.001},
        "Fear",
    ),

    # --- Surprise ------------------------------------------------------------
    (
        "I had no idea that was going to happen",
        {"surprise": 0.876, "fear": 0.055, "neutral": 0.030,
         "joy": 0.020, "sadness": 0.010, "anger": 0.005, "disgust": 0.004},
        "Surprise",
    ),

    # --- Confused: fear + surprise co-occurring ------------------------------
    (
        "Wait, what just happened? I don't understand",
        {"fear": 0.420, "surprise": 0.380, "neutral": 0.120,
         "sadness": 0.040, "anger": 0.020, "joy": 0.010, "disgust": 0.010},
        "Confused",
    ),
    (
        "Fear leads but there is also a lot of surprise mixed in",
        {"fear": 0.540, "surprise": 0.260, "neutral": 0.110,
         "sadness": 0.040, "anger": 0.030, "joy": 0.010, "disgust": 0.010},
        "Confused",
    ),

    # --- Neutral: flat, informational ----------------------------------------
    (
        "Meeting at three o'clock",
        {"neutral": 0.801, "joy": 0.053, "fear": 0.044,
         "surprise": 0.042, "sadness": 0.039, "anger": 0.013, "disgust": 0.008},
        "Neutral",
    ),
    (
        "Document updated and sent to the team",
        {"neutral": 0.951, "surprise": 0.017, "joy": 0.012,
         "anger": 0.007, "sadness": 0.006, "disgust": 0.004, "fear": 0.001},
        "Neutral",
    ),

    # --- Neutral: low-confidence false-positive guard ------------------------
    # Top emotion below MIN_CONFIDENCE — too weak to fire.
    (
        "Ambiguous sentence, model barely picks joy",
        {"joy": 0.280, "neutral": 0.260, "surprise": 0.150,
         "sadness": 0.110, "fear": 0.080, "anger": 0.070, "disgust": 0.050},
        "Neutral",
    ),
    # Top emotion passes MIN_CONFIDENCE but margin vs second is too small.
    (
        "Near-tie between joy and neutral — not a real signal",
        {"joy": 0.460, "neutral": 0.390, "surprise": 0.060,
         "sadness": 0.040, "fear": 0.020, "anger": 0.015, "disgust": 0.015},
        "Neutral",
    ),
    # Anger tops but low score — noisy background, not genuine anger.
    (
        "Mildly charged sentence, anger just edges out",
        {"anger": 0.320, "neutral": 0.290, "disgust": 0.160,
         "sadness": 0.100, "fear": 0.070, "surprise": 0.040, "joy": 0.020},
        "Neutral",
    ),
]


def ordered_scores(d):
    """Sort a plain dict high→low, the order EmbodimentPipeline.map_expression() expects."""
    return dict(sorted(d.items(), key=lambda kv: kv[1], reverse=True))


def run_tests():
    passed = 0
    failed = 0

    print("=" * 70)
    print(f"{'Description':<48} {'Expected':<14} {'Got':<14} {''}")
    print("=" * 70)

    for description, emotion_dict, expected in TEST_CASES:
        result, _ = EmbodimentPipeline.map_expression(ordered_scores(emotion_dict))
        ok = result == expected
        status = "PASS" if ok else "FAIL"
        print(f"{description:<48} {expected:<14} {result:<14} {status}")
        if ok:
            passed += 1
        else:
            failed += 1

    print("=" * 70)
    print(f"Results: {passed}/{passed + failed} passed", end="")
    if failed:
        print(f"  —  {failed} FAILED  <- adjust thresholds in embodiment_service.py")
    else:
        print("  — all good!")
    print()


if __name__ == "__main__":
    run_tests()
