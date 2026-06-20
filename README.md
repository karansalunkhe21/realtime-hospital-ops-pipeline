# Real-time Hospital Operations Pipeline

A production-grade data engineering pipeline that streams live hospital events — patient admissions, bed status changes, and diagnoses — from Kafka into Databricks Delta Lake, transforming raw data through Bronze → Silver → Gold layers to deliver real-time KPI dashboards for hospital operations teams.

---

## Tech stack

| Layer | Tool | Purpose |
|---|---|---|
| Streaming | Kafka | Real-time event ingestion |
| Storage | Databricks Delta Lake | Bronze / Silver / Gold tables |
| Transform | PySpark notebooks | SCD Type 1 & 3, KPI aggregations |
| Orchestration | Airflow | Daily pipeline scheduling |
| Infrastructure | Docker | Local Kafka for development |

---

## Pipeline architecture

```
Epic EHR (simulated)
     │
     ▼
  Kafka Topics
  ├── hospital.admissions
  ├── hospital.bed_events
  └── hospital.diagnoses
     │
     ▼
Databricks Bronze       ← raw Delta tables (unchanged)
     │
     ▼
Databricks Silver       ← cleaned dims + facts
  ├── dim_patients      (SCD Type 1 — latest demographics)
  ├── dim_diagnoses     (SCD Type 3 — original + current diagnosis)
  ├── dim_physicians
  ├── fact_admissions
  └── fact_bed_utilization
     │
     ▼
Databricks Gold         ← KPI tables for dashboards
  ├── kpi_bed_utilization    (% beds occupied per ward per day)
  ├── kpi_er_wait_times      (avg ER stay, median, p90)
  └── kpi_readmission_risk   (30-day readmission rate per ward)
```

---

## Project structure

```
realtime-hospital-ops-pipeline/
│
├── .env.example                 ← credentials template (safe to commit)
├── .gitignore                   ← excludes .env, venv/, __pycache__
├── requirements.txt             ← all Python dependencies
├── docker-compose.yml           ← local Kafka + Kafka UI
│
├── config/
│   └── settings.py              ← reads .env, exposes DatabricksConfig + KafkaConfig
│
├── data/
│   ├── generate_sample_data.py  ← generates realistic fake hospital events
│   └── sample/                  ← generated JSON files land here
│
├── kafka/
│   ├── producer/
│   │   ├── admission_producer.py    ← sends admissions to Kafka
│   │   └── bed_event_producer.py   ← sends bed events to Kafka
│   └── consumer/
│       └── databricks_consumer.py  ← reads Kafka → writes to Bronze Delta tables
│
├── databricks/
│   ├── connection/
│   │   └── client.py            ← get_client() used by consumer and notebook runner
│   └── notebooks/
│       ├── 01_setup_bronze.py   ← creates Delta Lake database + Bronze tables (run once)
│       ├── 02_silver.py         ← Bronze → Silver (SCD Type 1, SCD Type 3, facts)
│       └── 03_gold.py           ← Silver → Gold KPI aggregations
│
└── airflow/
    └── dags/
        └── hospital_pipeline_dag.py  ← orchestrates everything daily at 6am
```

---

## Quickstart

### 1 — Clone and set up virtual environment
```bash
git clone https://github.com/yourusername/realtime-hospital-ops-pipeline.git
cd realtime-hospital-ops-pipeline

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2 — Configure credentials
```bash
cp .env.example .env
# Fill in DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID
```

### 3 — Start Kafka locally
```bash
docker-compose up -d
# Kafka UI available at http://localhost:8080
```

### 4 — Set up Bronze tables in Databricks (run once)
Paste `databricks/notebooks/01_setup_bronze.py` into a Databricks notebook and click Run All.

### 5 — Generate sample data
```bash
python data/generate_sample_data.py
```

### 6 — Send events to Kafka
```bash
python kafka/producer/admission_producer.py
python kafka/producer/bed_event_producer.py
```

### 7 — Land data in Databricks Bronze
```bash
python kafka/consumer/databricks_consumer.py
```

### 8 — Run Silver and Gold notebooks in Databricks
Run `02_silver.py` then `03_gold.py` in your Databricks workspace.

### 9 — Query your KPIs
```sql
SELECT * FROM hospital.kpi_bed_utilization  ORDER BY report_date DESC;
SELECT * FROM hospital.kpi_er_wait_times    ORDER BY report_date DESC;
SELECT * FROM hospital.kpi_readmission_risk ORDER BY report_date DESC;
```

---

## Key concepts

**Medallion architecture** — data moves through three quality layers. Bronze is raw and untouched. Silver is cleaned, typed, and deduplicated. Gold is aggregated KPIs ready for dashboards.

**SCD Type 1 (`dim_patients`)** — always overwrites with the latest patient demographics. No history kept. When insurance type changes, the old value is gone.

**SCD Type 3 (`dim_diagnoses`)** — stores both `original_diagnosis_code` and `current_diagnosis_code` in the same row. Tells you at a glance whether a diagnosis was ever revised — critical for readmission analysis.

**30-day readmission KPI** — uses PySpark `LAG()` to look back at each patient's previous discharge date. Flags any admission within 30 days as a readmission. Hospitals are financially penalised for high readmission rates (CMS), making this a high-stakes metric.

---

## Commit history structure

| Commit | What it covers |
|---|---|
| `chore: project setup` | `.env.example`, `.gitignore`, `requirements.txt`, `README.md` |
| `feat: config and data generator` | `config/`, `data/` |
| `feat: kafka producers` | `kafka/producer/` |
| `feat: databricks consumer` | `kafka/consumer/` |
| `feat: bronze setup notebook` | `databricks/notebooks/01_setup_bronze.py` |
| `feat: silver layer (SCD 1 & 3)` | `databricks/notebooks/02_silver.py` |
| `feat: gold KPI layer` | `databricks/notebooks/03_gold.py` |
| `feat: airflow orchestration` | `airflow/` |
