# ============================================================
# Real-time Hospital Operations Pipeline
# Notebook: 02_silver
#
# Reads raw Bronze tables → writes clean Silver tables.
# Implements SCD Type 1 (dim_patients) and
#             SCD Type 3 (dim_diagnoses).
#
# This replaces ALL the dbt/models/silver/*.sql files.
# Run daily after Kafka consumer has loaded new data.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# ── dim_patients (SCD Type 1) ──────────────────────────────
# Always keep only the LATEST demographics per patient.
# When a patient's name/insurance changes, we overwrite it.

raw_admissions = spark.table("hospital.bronze_admissions")

# ROW_NUMBER picks the newest row per patient (latest admit_timestamp wins)
window_latest = Window.partitionBy("patient_id").orderBy(F.col("admit_timestamp").desc())

dim_patients = (
    raw_admissions
    .withColumn("row_num", F.row_number().over(window_latest))
    .filter(F.col("row_num") == 1)                   # SCD Type 1: only the latest
    .select(
        "patient_id",
        "patient_name",
        "age",
        "gender",
        "insurance_type",
        F.current_timestamp().alias("updated_at"),   # when this row was last written
    )
)

dim_patients.write.format("delta").mode("overwrite").saveAsTable("hospital.dim_patients")
print(f"✅ dim_patients written — {dim_patients.count()} patients")

# COMMAND ----------
# ── dim_diagnoses (SCD Type 3) ─────────────────────────────
# Keep BOTH the first-ever AND the most-recent diagnosis
# in the same row. One row per patient.

raw_diagnoses = spark.table("hospital.bronze_diagnoses")

window_first  = Window.partitionBy("patient_id").orderBy(F.col("recorded_at").asc())
window_latest = Window.partitionBy("patient_id").orderBy(F.col("recorded_at").desc())

# Get the first diagnosis ever recorded
first_diag = (
    raw_diagnoses
    .withColumn("rn", F.row_number().over(window_first))
    .filter(F.col("rn") == 1)
    .select("patient_id", F.col("diagnosis_code").alias("original_diagnosis_code"))
)

# Get the most recent diagnosis
latest_diag = (
    raw_diagnoses
    .withColumn("rn", F.row_number().over(window_latest))
    .filter(F.col("rn") == 1)
    .select(
        "patient_id",
        F.col("diagnosis_code").alias("current_diagnosis_code"),
        F.col("diagnosis_desc").alias("current_diagnosis_desc"),
        "icd10_category",
        F.col("recorded_at").alias("last_updated"),
    )
)

# SCD Type 3: join both into one row per patient
dim_diagnoses = (
    latest_diag.join(first_diag, on="patient_id", how="left")
    .withColumn(
        "diagnosis_was_revised",
        F.col("original_diagnosis_code") != F.col("current_diagnosis_code")
    )
)

dim_diagnoses.write.format("delta").mode("overwrite").saveAsTable("hospital.dim_diagnoses")
print(f"✅ dim_diagnoses written — {dim_diagnoses.count()} patients")

# COMMAND ----------
# ── dim_physicians ─────────────────────────────────────────
# One row per physician, derived from admissions.

dim_physicians = (
    raw_admissions
    .groupBy("physician_id")
    .agg(
        F.countDistinct("patient_id").alias("total_patients_treated"),
        F.countDistinct("ward").alias("wards_worked_in"),
        F.min("admit_timestamp").alias("first_activity"),
        F.max("admit_timestamp").alias("last_activity"),
    )
)

dim_physicians.write.format("delta").mode("overwrite").saveAsTable("hospital.dim_physicians")
print(f"✅ dim_physicians written — {dim_physicians.count()} physicians")

# COMMAND ----------
# ── fact_admissions ────────────────────────────────────────
# One row per admission. Typed columns, length of stay, ER flag.

fact_admissions = (
    raw_admissions
    .withColumn("admit_ts",    F.to_timestamp("admit_timestamp"))
    .withColumn("discharge_ts", F.to_timestamp("discharge_ts"))
    .withColumn(
        "length_of_stay_hours",
        (F.unix_timestamp("discharge_ts") - F.unix_timestamp("admit_ts")) / 3600
    )
    .withColumn("is_er_admission", F.col("ward") == "Emergency")
    # Deduplicate — keep first occurrence of each event_id
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("event_id").orderBy(F.col("loaded_at").asc())
    ))
    .filter(F.col("rn") == 1)
    .select(
        "event_id", "patient_id", "physician_id", "ward",
        "diagnosis_code", "insurance_type",
        "admit_ts", "discharge_ts",
        "length_of_stay_hours", "is_er_admission",
    )
)

fact_admissions.write.format("delta").mode("overwrite").saveAsTable("hospital.fact_admissions")
print(f"✅ fact_admissions written — {fact_admissions.count()} records")

# COMMAND ----------
# ── fact_bed_utilization ───────────────────────────────────
# One row per bed status event.

raw_beds = spark.table("hospital.bronze_bed_events")

fact_bed_utilization = (
    raw_beds
    .withColumn("event_ts", F.to_timestamp("event_timestamp"))
    .withColumn("is_occupied", (F.col("status") == "OCCUPIED").cast("int"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("event_id").orderBy(F.col("loaded_at").asc())
    ))
    .filter(F.col("rn") == 1)
    .select("event_id", "bed_id", "ward", "status", "patient_id", "event_ts", "is_occupied")
)

fact_bed_utilization.write.format("delta").mode("overwrite").saveAsTable("hospital.fact_bed_utilization")
print(f"✅ fact_bed_utilization written — {fact_bed_utilization.count()} records")

# COMMAND ----------
print("\n🎉 Silver layer complete!")
