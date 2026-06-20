# ============================================================
# Real-time Hospital Operations Pipeline
# Notebook: 03_gold
#
# Reads Silver tables → writes Gold KPI tables.
# These are the final aggregated numbers for dashboards.
#
# This replaces ALL the dbt/models/gold/*.sql files.
# Run daily after 02_silver completes.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# ── KPI 1: bed utilization by ward per day ─────────────────
# "What % of beds were occupied each day, by ward?"

bed_kpi = (
    spark.table("hospital.fact_bed_utilization")
    .withColumn("report_date", F.to_date("event_ts"))
    .groupBy("report_date", "ward")
    .agg(
        F.countDistinct("bed_id").alias("total_beds"),
        F.sum("is_occupied").alias("occupied_count"),
        F.round(
            F.sum("is_occupied") * 100.0 / F.count("bed_id"), 1
        ).alias("utilization_pct"),
        F.countDistinct(
            F.when(F.col("status") == "AVAILABLE", F.col("bed_id"))
        ).alias("available_beds"),
    )
    .orderBy(F.col("report_date").desc(), "ward")
)

bed_kpi.write.format("delta").mode("overwrite").saveAsTable("hospital.kpi_bed_utilization")
print(f"✅ kpi_bed_utilization written — {bed_kpi.count()} rows")

# COMMAND ----------
# ── KPI 2: ER wait times per day ──────────────────────────
# "How long are patients staying in the ER on average?"

er_kpi = (
    spark.table("hospital.fact_admissions")
    .filter(F.col("is_er_admission") == True)
    .filter(F.col("admit_ts").isNotNull())
    .withColumn("report_date", F.to_date("admit_ts"))
    .groupBy("report_date")
    .agg(
        F.count("*").alias("er_admission_count"),
        F.round(F.avg("length_of_stay_hours"), 1).alias("avg_los_hours"),
        F.round(F.percentile_approx("length_of_stay_hours", 0.5), 1).alias("median_los_hours"),
        F.round(F.percentile_approx("length_of_stay_hours", 0.9), 1).alias("p90_los_hours"),
        F.sum(
            F.when(F.col("length_of_stay_hours") > 24, 1).otherwise(0)
        ).alias("long_stays_over_24h"),
    )
    .orderBy(F.col("report_date").desc())
)

er_kpi.write.format("delta").mode("overwrite").saveAsTable("hospital.kpi_er_wait_times")
print(f"✅ kpi_er_wait_times written — {er_kpi.count()} rows")

# COMMAND ----------
# ── KPI 3: 30-day readmission risk per ward per day ────────
# "Which wards have the highest readmission rate?"

admissions = spark.table("hospital.fact_admissions").filter(F.col("admit_ts").isNotNull())

# LAG() gives us the previous discharge timestamp for the same patient
window_patient = Window.partitionBy("patient_id").orderBy("admit_ts")

readmission_kpi = (
    admissions
    .withColumn("prev_discharge_ts", F.lag("discharge_ts").over(window_patient))
    .withColumn(
        "days_since_last_discharge",
        (F.unix_timestamp("admit_ts") - F.unix_timestamp("prev_discharge_ts")) / 86400
    )
    .withColumn(
        "is_30day_readmission",
        (F.col("prev_discharge_ts").isNotNull()) &
        (F.col("days_since_last_discharge") <= 30)
    )
    .withColumn("report_date", F.to_date("admit_ts"))
    .groupBy("report_date", "ward")
    .agg(
        F.count("*").alias("total_admissions"),
        F.sum(F.col("is_30day_readmission").cast("int")).alias("readmissions_30day"),
        F.round(
            F.sum(F.col("is_30day_readmission").cast("int")) * 100.0 / F.count("*"), 1
        ).alias("readmission_rate_pct"),
    )
    .orderBy(F.col("report_date").desc(), F.col("readmission_rate_pct").desc())
)

readmission_kpi.write.format("delta").mode("overwrite").saveAsTable("hospital.kpi_readmission_risk")
print(f"✅ kpi_readmission_risk written — {readmission_kpi.count()} rows")

# COMMAND ----------
print("\n🎉 Gold layer complete! All KPI tables are ready.")
print("\nQuery your KPIs:")
print("  SELECT * FROM hospital.kpi_bed_utilization ORDER BY report_date DESC LIMIT 20")
print("  SELECT * FROM hospital.kpi_er_wait_times   ORDER BY report_date DESC LIMIT 20")
print("  SELECT * FROM hospital.kpi_readmission_risk ORDER BY report_date DESC LIMIT 20")
