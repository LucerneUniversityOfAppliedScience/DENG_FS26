# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Streaming Tables
# MAGIC
# MAGIC **Goal:** Introduce **streaming tables** with `@dp.table` + `spark.readStream`. Until
# MAGIC now every table was a `@dp.materialized_view` — recomputed in full on each pipeline run.
# MAGIC A streaming table is **incremental**: it only processes *new* rows since the last run.
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ### `@dp.table` vs `@dp.materialized_view`
# MAGIC | Decorator | Refresh model | Function returns |
# MAGIC |-----------|---------------|------------------|
# MAGIC | `@dp.materialized_view` | Full recompute every run | Batch DataFrame (`spark.read.table(...)`) |
# MAGIC | `@dp.table` | Append-only / incremental | Streaming DataFrame (`spark.readStream.table(...)`) |
# MAGIC
# MAGIC Streaming tables are what you want for:
# MAGIC - **Auto Loader** ingestion from cloud storage (`cloudFiles` format)
# MAGIC - **CDC** ingestion from a source database
# MAGIC - Any source where rows arrive over time and reprocessing the whole history is wasteful
# MAGIC
# MAGIC ### This example
# MAGIC We re-ingest `samples.wanderbricks.bookings` as a **streaming source** to demonstrate the
# MAGIC syntax. In production, the source would be a Volume with new files arriving daily, read
# MAGIC via Auto Loader:
# MAGIC
# MAGIC ```python
# MAGIC return (
# MAGIC     spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .load("/Volumes/workspace/raw/landing/bookings/")
# MAGIC )
# MAGIC ```

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(
    comment="Streaming bronze: incremental ingestion of bookings"
)
def streaming_bookings():
    return spark.readStream.table("samples.wanderbricks.bookings")
