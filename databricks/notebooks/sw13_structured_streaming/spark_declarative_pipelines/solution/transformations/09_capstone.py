# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Capstone: Your Own Mini-Pipeline
# MAGIC
# MAGIC **Goal:** Apply everything you have learned to a different source dataset. The capstone
# MAGIC is intentionally open-ended — there is no single correct answer.
# MAGIC
# MAGIC ## The Task
# MAGIC
# MAGIC Build a small bronze → silver → gold pipeline on `samples.nyctaxi.trips` (or
# MAGIC `samples.tpch.*` if you prefer relational data).
# MAGIC
# MAGIC Required:
# MAGIC 1. **Bronze** — one `@dp.materialized_view` per source table (`bronze_trips` for nyctaxi).
# MAGIC 2. **Silver** — at least one cleaned/enriched view with **at least one** `@dp.expect_or_drop`
# MAGIC    rule.
# MAGIC 3. **Gold** — at least one aggregated view answering a real analytical question, e.g.
# MAGIC    "average fare per pickup zip code and hour of day", "monthly trip volume trend",
# MAGIC    "top 10 routes by passenger count".
# MAGIC
# MAGIC Suggested extension: include a window function in your gold layer (e.g. ranking pickup
# MAGIC zones by total fare per month).
# MAGIC
# MAGIC ## Hints
# MAGIC
# MAGIC - `samples.nyctaxi.trips` has columns like `tpep_pickup_datetime`, `tpep_dropoff_datetime`,
# MAGIC   `pickup_zip`, `dropoff_zip`, `trip_distance`, `fare_amount`.
# MAGIC - The reference solution below shows **one** possible answer using nyctaxi — yours can
# MAGIC   look very different.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

@dp.materialized_view(
    comment="Capstone bronze: raw NYC taxi trips"
)
def bronze_trips():
    return spark.read.table("samples.nyctaxi.trips")

# COMMAND ----------

@dp.materialized_view(
    comment="Capstone silver: trips with derived columns and basic quality rules"
)
@dp.expect_or_drop("positive_fare", "fare_amount > 0")
@dp.expect_or_drop("positive_distance", "trip_distance > 0")
@dp.expect_or_drop("valid_pickup_zip", "pickup_zip IS NOT NULL")
def silver_trips_cleaned():
    return (
        spark.read.table("bronze_trips")
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance"))
             .otherwise(None),
        )
    )

# COMMAND ----------

@dp.materialized_view(
    comment="Capstone gold: pickup zone performance per month with rank"
)
def gold_pickup_zone_performance():
    trips = spark.read.table("silver_trips_cleaned")

    return (
        trips
        .withColumn("pickup_month", F.date_trunc("month", F.col("pickup_date")))
        .groupBy("pickup_zip", "pickup_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("fare_amount").alias("total_fare"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
        )
        .withColumn(
            "fare_rank",
            F.row_number().over(
                Window.partitionBy("pickup_month").orderBy(F.desc("total_fare"))
            ),
        )
        .orderBy(F.desc("pickup_month"), "fare_rank")
    )
