"""Consume click events from Redpanda and print them to stdout."""

from __future__ import annotations

import os
import sys

from confluent_kafka import Consumer, KafkaError

BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:29092")
TOPIC = os.environ.get("CLICKS_TOPIC", "clicks")
GROUP_ID = os.environ.get("CLICKS_GROUP", "demo-consumer")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([TOPIC])
    print(f"consuming from {TOPIC} on {BROKERS} (group={GROUP_ID})")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"error: {msg.error()}", file=sys.stderr)
                continue
            print(f"{msg.partition()}@{msg.offset()} key={msg.key()} value={msg.value()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
