# ============================================================
# kafka/producer/bed_event_producer.py
# UNCHANGED from original project.
# Run with: python kafka/producer/bed_event_producer.py
# ============================================================

import json
import time
from pathlib import Path
from confluent_kafka import Producer
from config.settings import KafkaConfig


def on_delivery(err, msg):
    if err:
        print(f"  ❌ Delivery failed: {err}")
    else:
        print(f"  ✅ Sent → {msg.topic()} | bed={json.loads(msg.value())['bed_id']}")


def send_bed_events():
    producer = Producer({"bootstrap.servers": KafkaConfig.bootstrap_servers})

    sample_file = Path("data/sample/bed_events.json")
    if not sample_file.exists():
        print("❌ No sample data. Run: python data/generate_sample_data.py")
        return

    records = json.loads(sample_file.read_text())
    print(f"📤 Sending {len(records)} bed events to Kafka...")

    for record in records:
        producer.produce(
            topic    = KafkaConfig.topic_bed_events,
            key      = record["bed_id"],
            value    = json.dumps(record),
            callback = on_delivery,
        )
        producer.poll(0)
        time.sleep(0.05)

    producer.flush()
    print("\n✅ All bed events sent.")


if __name__ == "__main__":
    send_bed_events()
