# ============================================================
# airflow/dags/hospital_pipeline_dag.py
#
# Orchestrates the full hospital pipeline daily.
# Uses DatabricksSubmitRunOperator to trigger notebooks.
#
# The DAG structure is identical to the original project —
# only the "run dbt" step is replaced with notebook tasks.
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
import os

CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "your_cluster_id")

DEFAULT_ARGS = {
    "owner":            "hospital-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id       = "hospital_pipeline_databricks",
    default_args = DEFAULT_ARGS,
    description  = "Real-time Hospital Operations Pipeline: Kafka → Databricks Bronze → Silver → Gold",
    schedule     = "0 6 * * *",
    start_date   = datetime(2024, 1, 1),
    catchup      = False,
    tags         = ["hospital", "kafka", "databricks"],
) as dag:

    # ── Step 1: Generate sample data ────────────────────────
    generate_data = BashOperator(
        task_id      = "generate_sample_data",
        bash_command = "python data/generate_sample_data.py",
        cwd          = "/opt/airflow/realtime-hospital-ops-pipeline",
    )

    # ── Step 2: Send to Kafka (parallel) ────────────────────
    send_admissions = BashOperator(
        task_id      = "send_admissions_to_kafka",
        bash_command = "python kafka/producer/admission_producer.py",
        cwd          = "/opt/airflow/realtime-hospital-ops-pipeline",
    )

    send_bed_events = BashOperator(
        task_id      = "send_bed_events_to_kafka",
        bash_command = "python kafka/producer/bed_event_producer.py",
        cwd          = "/opt/airflow/realtime-hospital-ops-pipeline",
    )

    # ── Step 3: Wait for consumer to land data ──────────────
    wait_for_consumer = BashOperator(
        task_id      = "wait_for_consumer",
        bash_command = "sleep 60",
    )

    # ── Step 4: Run Silver notebook in Databricks ───────────
    run_silver = DatabricksSubmitRunOperator(
        task_id           = "run_silver_notebook",
        databricks_conn_id = "databricks_default",   # set up in Airflow connections
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": "/hospital/02_silver",
            },
        },
    )

    # ── Step 5: Run Gold notebook in Databricks ─────────────
    run_gold = DatabricksSubmitRunOperator(
        task_id           = "run_gold_notebook",
        databricks_conn_id = "databricks_default",
        json={
            "existing_cluster_id": CLUSTER_ID,
            "notebook_task": {
                "notebook_path": "/hospital/03_gold",
            },
        },
    )

    # ── DAG graph ────────────────────────────────────────────
    #
    #   generate_data
    #        │
    #   ┌────┴────┐
    # admissions  bed_events    ← parallel
    #   └────┬────┘
    #   wait_for_consumer
    #        │
    #     run_silver
    #        │
    #      run_gold
    #
    generate_data >> [send_admissions, send_bed_events]
    [send_admissions, send_bed_events] >> wait_for_consumer
    wait_for_consumer >> run_silver >> run_gold
