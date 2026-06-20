import json
import time
from pathlib import Path
from confluent_kafka import Producer
from config.settings import KafkaConfig

def on_delivery(err, msg):
    if err:
        print(f"  ❌ Delivery failed: {err}")
    else:
        print(f"  ✅ Sent → {msg.topic()} | patient={json.loads(msg.value())['patient_id']}")

def send_diagnoses():
    producer = Producer({"bootstrap.servers": KafkaConfig.bootstrap_servers})
    sample_file = Path("data/sample/diagnoses.json")
    records = json.loads(sample_file.read_text())
    print(f"📤 Sending {len(records)} diagnosis events to Kafka...")
    for record in records:
        producer.produce(
            topic    = KafkaConfig.topic_diagnoses,
            key      = record["patient_id"],
            value    = json.dumps(record),
            callback = on_delivery,
        )
        producer.poll(0)
        time.sleep(0.05)
    producer.flush()
    print("\n✅ All diagnosis events sent.")

if __name__ == "__main__":
    send_diagnoses()