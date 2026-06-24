"""
Demo producer for the Embodiment Service proof of concept.

Publishes test sentences to vh.brain.output so the Embodiment Service has
messages to consume. The set mixes single- and multi-sentence inputs so the
segmentation pipeline is exercised during a live demo.

Run:
  python3 demo_producer.py
"""

import time

# A mix of single- and multi-sentence inputs (the last three are multi-sentence).
TEST_SENTENCES = [
    "That is wonderful news, I am really glad to hear it!",
    "Okay, that sounds alright I suppose.",
    "This is devastating, I am heartbroken by what happened.",
    "I understand, that must have been a difficult situation.",
    "The document has been updated and sent to the team.",
    "I am not sure about this. Something feels off. Let me think it through.",   # multi-sentence
    "That is great! Really, that changes everything. I am so relieved.",         # multi-sentence positive shift
    "I was worried at first. But now I see it. This is actually good news.",     # multi-sentence arc
]


class DemoProducer:
    """Publishes a list of sentences to the brain-output topic, paced for a demo."""

    def __init__(self, broker: str = "localhost:9092",
                 topic: str = "vh.brain.output", delay_seconds: float = 3):
        self.broker = broker
        self.topic = topic
        self.delay_seconds = delay_seconds
        self._producer = None

    def connect(self) -> None:
        from kafka import KafkaProducer
        self._producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: v.encode("utf-8"),
        )

    def publish_all(self, sentences) -> None:
        for sentence in sentences:
            print(f"[SEND] {sentence!r}")
            self._producer.send(self.topic, sentence)
            self._producer.flush()
            time.sleep(self.delay_seconds)

    def close(self) -> None:
        if self._producer:
            self._producer.close()


def main() -> None:
    producer = DemoProducer()
    print("Demo Producer — publishing test sentences")
    print(f"  broker: {producer.broker}")
    print(f"  topic:  {producer.topic}")
    print(f"  delay:  {producer.delay_seconds}s between messages\n")

    try:
        producer.connect()
    except Exception as exc:
        print(f"[FATAL] Could not reach Kafka at {producer.broker}: {exc}")
        return

    try:
        producer.publish_all(TEST_SENTENCES)
    finally:
        producer.close()
    print("\nDone — all test sentences published.")


if __name__ == "__main__":
    main()
