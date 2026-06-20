# ============================================================
# kafka/producer/admission_producer.py
#
# UNCHANGED from original project.
# Kafka producers don't care whether the destination
# is Snowflake or Databricks — they just send to Kafka.
#
# Run with: python kafka/producer/admission_producer.py
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
        print(f"  ✅ Sent → topic={msg.topic()} partition={msg.partition()}")


def send_admissions():
    producer = Producer({"bootstrap.servers": KafkaConfig.bootstrap_servers})

    sample_file = Path("data/sample/admissions.json")
    if not sample_file.exists():
        print("❌ No sample data. Run: python data/generate_sample_data.py")
        return

    records = json.loads(sample_file.read_text())
    print(f"📤 Sending {len(records)} admission events to Kafka...")

    for record in records:
        producer.produce(
            topic    = KafkaConfig.topic_admissions,
            key      = record["patient_id"],
            value    = json.dumps(record),
            callback = on_delivery,
        )
        producer.poll(0)
        time.sleep(0.05)

    producer.flush()
    print("\n✅ All admission events sent.")


if __name__ == "__main__":
    send_admissions()
