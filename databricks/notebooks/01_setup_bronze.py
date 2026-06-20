# ============================================================
# Real-time Hospital Operations Pipeline
# Notebook: 01_setup_bronze
#
# Run this ONCE in Databricks to create the Delta Lake
# database and all three Bronze landing tables.
#
# How to run: paste into a Databricks notebook, attach to
# your cluster, then click "Run All".
#
# This replaces snowflake/setup_schemas.py from the old project.
# ============================================================

# COMMAND ----------
# Create a database (like a schema in Snowflake)
spark.sql("CREATE DATABASE IF NOT EXISTS hospital")
print("✅ Database 'hospital' ready")

# COMMAND ----------
# BRONZE TABLE 1: raw admissions
# One row per patient admission event arriving from Kafka.
spark.sql("""
    CREATE TABLE IF NOT EXISTS hospital.bronze_admissions (
        event_id         STRING,
        patient_id       STRING,
        patient_name     STRING,
        age              INT,
        gender           STRING,
        ward             STRING,
        admit_timestamp  STRING,
        discharge_ts     STRING,
        diagnosis_code   STRING,
        physician_id     STRING,
        insurance_type   STRING,
        raw_json         STRING,
        loaded_at        TIMESTAMP
    )
    USING DELTA
    LOCATION 'dbfs:/hospital/bronze/admissions'
""")
print("✅ bronze_admissions table ready")

# COMMAND ----------
# BRONZE TABLE 2: raw bed events
# One row per bed status change (OCCUPIED / AVAILABLE / CLEANING).
spark.sql("""
    CREATE TABLE IF NOT EXISTS hospital.bronze_bed_events (
        event_id         STRING,
        bed_id           STRING,
        ward             STRING,
        status           STRING,
        patient_id       STRING,
        event_timestamp  STRING,
        raw_json         STRING,
        loaded_at        TIMESTAMP
    )
    USING DELTA
    LOCATION 'dbfs:/hospital/bronze/bed_events'
""")
print("✅ bronze_bed_events table ready")

# COMMAND ----------
# BRONZE TABLE 3: raw diagnoses
# One row per diagnosis record (may have multiple per patient).
spark.sql("""
    CREATE TABLE IF NOT EXISTS hospital.bronze_diagnoses (
        event_id         STRING,
        patient_id       STRING,
        diagnosis_code   STRING,
        diagnosis_desc   STRING,
        icd10_category   STRING,
        recorded_at      STRING,
        raw_json         STRING,
        loaded_at        TIMESTAMP
    )
    USING DELTA
    LOCATION 'dbfs:/hospital/bronze/diagnoses'
""")
print("✅ bronze_diagnoses table ready")

# COMMAND ----------
print("\n🎉 Bronze setup complete! All tables are ready for Kafka data.")
