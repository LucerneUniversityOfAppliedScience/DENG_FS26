"""Generate fake click events and write them to the `clicks` topic."""

from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:29092")
TOPIC = os.environ.get("CLICKS_TOPIC", "clicks")
RATE_PER_SECOND = float(os.environ.get("CLICK_RATE", "20"))
DURATION_SECONDS = int(os.environ.get("CLICK_DURATION", "60"))

PAGES = ["/home", "/products", "/checkout", "/login", "/about", "/blog"]


def delivery_callback(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")


def main() -> None:
    fake = Faker()
    producer = Producer({"bootstrap.servers": BROKERS, "linger.ms": 50})

    print(f"producing to {TOPIC} on {BROKERS} at ~{RATE_PER_SECOND}/s for {DURATION_SECONDS}s", flush=True)
    sleep_between = 1.0 / RATE_PER_SECOND
    start = time.time()
    end = start + DURATION_SECONDS
    sent = 0
    last_tick = start
    last_sent = 0

    while time.time() < end:
        event = {
            "user_id": fake.uuid4(),
            "page": random.choice(PAGES),
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        producer.produce(
            TOPIC,
            key=event["user_id"],
            value=json.dumps(event).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        producer.poll(0)
        sent += 1
        time.sleep(sleep_between)

        now = time.time()
        if now - last_tick >= 1.0:
            elapsed = int(now - start)
            rate = sent - last_sent
            print(f"  t+{elapsed:>3}s  sent={sent:>6}  rate={rate}/s", flush=True)
            last_tick = now
            last_sent = sent

    producer.flush(10)
    print(f"done — sent {sent} events", flush=True)


if __name__ == "__main__":
    main()
