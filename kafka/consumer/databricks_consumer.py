# ============================================================
# kafka/consumer/databricks_consumer.py
#
# Reads messages from ALL hospital Kafka topics and
# writes them into the correct Databricks Bronze Delta table
# using the Databricks REST API.
#
# This replaces snowflake_consumer.py from the old project.
#
# Run with: python kafka/consumer/databricks_consumer.py
# ============================================================

import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from config.settings import KafkaConfig, DatabricksConfig

# ── Kafka consumer config ───────────────────────────────────
CONSUMER_CONFIG = {
    "bootstrap.servers": KafkaConfig.bootstrap_servers,
    "group.id":          "realtime-hospital-ops-consumer",
    "auto.offset.reset": "earliest",
}

# ── Databricks SQL warehouse ID ─────────────────────────────
# In Community Edition, use the built-in SQL warehouse.
# Find it at: SQL → SQL Warehouses → copy the ID
SQL_WAREHOUSE_ID = "0b01e265b7f82e12"   # fill this in


def insert_to_databricks(client, topic: str, record: dict):
    """
    Insert one Kafka message into the right Bronze Delta table
    using Databricks SQL Statement API.
    """
    now = datetime.utcnow().isoformat()
    raw_json = json.dumps(record).replace("'", "''")  # escape single quotes

    if topic == KafkaConfig.topic_admissions:
        sql = f"""
            INSERT INTO hospital.bronze_admissions VALUES (
                '{record.get("event_id")}',
                '{record.get("patient_id")}',
                '{record.get("patient_name", "").replace("'", "''")}',
                {record.get("age", 0)},
                '{record.get("gender")}',
                '{record.get("ward")}',
                '{record.get("admit_timestamp")}',
                '{record.get("discharge_ts")}',
                '{record.get("diagnosis_code")}',
                '{record.get("physician_id")}',
                '{record.get("insurance_type")}',
                '{raw_json}',
                '{now}'
            )
        """

    elif topic == KafkaConfig.topic_bed_events:
        sql = f"""
            INSERT INTO hospital.bronze_bed_events VALUES (
                '{record.get("event_id")}',
                '{record.get("bed_id")}',
                '{record.get("ward")}',
                '{record.get("status")}',
                '{record.get("patient_id")}',
                '{record.get("event_timestamp")}',
                '{raw_json}',
                '{now}'
            )
        """

    elif topic == KafkaConfig.topic_diagnoses:
        sql = f"""
            INSERT INTO hospital.bronze_diagnoses VALUES (
                '{record.get("event_id")}',
                '{record.get("patient_id")}',
                '{record.get("diagnosis_code")}',
                '{record.get("diagnosis_desc", "").replace("'", "''")}',
                '{record.get("icd10_category")}',
                '{record.get("recorded_at")}',
                '{raw_json}',
                '{now}'
            )
        """
    else:
        print(f"  ⚠️  Unknown topic: {topic} — skipping")
        return

    # Execute SQL via Databricks Statement API
    response = client.statement_execution.execute_statement(
        warehouse_id = SQL_WAREHOUSE_ID,
        statement    = sql,
        wait_timeout = "30s",
    )

    if response.status.state == StatementState.SUCCEEDED:
        print(f"  💾 Inserted → {topic} | event_id={record.get('event_id')}")
    else:
        print(f"  ❌ Insert failed: {response.status.error}")


def consume():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([
        KafkaConfig.topic_admissions,
        KafkaConfig.topic_bed_events,
        KafkaConfig.topic_diagnoses,
    ])

    # Connect to Databricks
    client = WorkspaceClient(
        host  = DatabricksConfig.host,
        token = DatabricksConfig.token,
    )

    print("🎧 Listening for Kafka messages... (Ctrl+C to stop)\n")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"❌ Kafka error: {msg.error()}")
                continue

            topic  = msg.topic()
            record = json.loads(msg.value().decode("utf-8"))
            print(f"📨 Received from {topic}")
            insert_to_databricks(client, topic, record)

    except KeyboardInterrupt:
        print("\n⛔ Stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume()
