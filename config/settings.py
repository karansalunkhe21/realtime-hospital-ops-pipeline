# ============================================================
# config/settings.py  —  Real-time Hospital Operations Pipeline
#
# Single place to read all environment variables.
# Every other file imports from here.
# ============================================================

import os
from dotenv import load_dotenv

load_dotenv()


class DatabricksConfig:
    """All Databricks connection details."""
    host       = os.getenv("DATABRICKS_HOST")        # e.g. https://community.cloud.databricks.com
    token      = os.getenv("DATABRICKS_TOKEN")        # personal access token
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")  # cluster to run notebooks on


class KafkaConfig:
    """Kafka broker and topic names."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_admissions  = os.getenv("KAFKA_TOPIC_ADMISSIONS",  "hospital.admissions")
    topic_bed_events  = os.getenv("KAFKA_TOPIC_BED_EVENTS",  "hospital.bed_events")
    topic_diagnoses   = os.getenv("KAFKA_TOPIC_DIAGNOSES",   "hospital.diagnoses")
