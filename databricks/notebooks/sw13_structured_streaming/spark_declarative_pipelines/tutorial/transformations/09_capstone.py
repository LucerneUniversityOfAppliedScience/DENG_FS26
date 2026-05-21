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
# MAGIC 1. **Bronze** — one `@dp.materialized_view` per source table (e.g. `bronze_trips`).
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
# MAGIC - Use `F.unix_timestamp(...)` and subtraction to compute trip duration.
# MAGIC - Use `F.date_trunc("month", ...)` for monthly aggregates.
# MAGIC - Re-read the previous notebooks if you forget the patterns — the capstone uses nothing
# MAGIC   new.
# MAGIC
# MAGIC ## Starter code
# MAGIC
# MAGIC Implement at minimum a bronze table, a silver table, and a gold table. Add more if you
# MAGIC have time.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

@dp.materialized_view(
    comment="Capstone bronze: raw NYC taxi trips"
)
def bronze_trips():
    # TODO
    raise NotImplementedError("Implement bronze_trips")

# COMMAND ----------

@dp.materialized_view(
    comment="Capstone silver: cleaned trips"
)
# TODO: add at least one @dp.expect_or_drop
def silver_trips_cleaned():
    # TODO
    raise NotImplementedError("Implement silver_trips_cleaned")

# COMMAND ----------

@dp.materialized_view(
    comment="Capstone gold: your analytical question"
)
def gold_my_metric():
    # TODO
    raise NotImplementedError("Implement gold_my_metric")
