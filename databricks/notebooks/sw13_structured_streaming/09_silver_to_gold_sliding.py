# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: sliding-window carrier KPIs
# MAGIC
# MAGIC The sibling of [`07_silver_to_gold_tumbling`](./07_silver_to_gold_tumbling).
# MAGIC Same source, same aggregation idea (events per carrier per state per
# MAGIC window) — but the window **slides**: every 2 minutes a new
# MAGIC 10-minute window starts, and the new window overlaps with the
# MAGIC previous four.
# MAGIC
# MAGIC ```
# MAGIC |--- window @ 00:00 — 00:10 ---|
# MAGIC       |--- window @ 00:02 — 00:12 ---|
# MAGIC             |--- window @ 00:04 — 00:14 ---|
# MAGIC                   |--- window @ 00:06 — 00:16 ---|
# MAGIC                         |--- window @ 00:08 — 00:18 ---|
# MAGIC ```
# MAGIC
# MAGIC With a 10-minute size and 2-minute slide every event lands in
# MAGIC **size/slide = 5 windows**. That's the price of the smoother
# MAGIC trend curve.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC `workspace.silver.logistics_data_gen` (from `04_bronze_to_silver`)
# MAGIC must exist and have rows with an `event_ts` column.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("source_table",     "workspace.silver.logistics_data_gen",
                     "Silver source table")
dbutils.widgets.text("gold_table",       "workspace.gold.logistics_carrier_kpi_sliding",
                     "Gold target table")
dbutils.widgets.text("checkpoint_root",  "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.text("window_size",      "10 minutes",
                     "Sliding window size")
dbutils.widgets.text("slide_duration",   "2 minutes",
                     "Slide interval")
dbutils.widgets.text("watermark_delay",  "3 minutes",
                     "Allowed late-arrival delay (watermark)")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"],
                         "Drop Gold + wipe its checkpoint?")

source_table    = dbutils.widgets.get("source_table")
gold_table      = dbutils.widgets.get("gold_table")
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/")
window_size     = dbutils.widgets.get("window_size")
slide_duration  = dbutils.widgets.get("slide_duration")
watermark_delay = dbutils.widgets.get("watermark_delay")
cleanup         = dbutils.widgets.get("cleanup")

gold_checkpoint = f"{checkpoint_root}/{gold_table.replace('.', '_')}"

print(f"Source           : {source_table}")
print(f"Gold             : {gold_table}")
print(f"Window           : {window_size} sliding every {slide_duration}")
print(f"Watermark delay  : {watermark_delay}")
print(f"Checkpoint       : {gold_checkpoint}")

# COMMAND ----------

# DBTITLE 1,(Optional) Reset Gold table + checkpoint
if cleanup == "yes":
    try:
        dbutils.fs.rm(gold_checkpoint, recurse=True)
        print(f"✓ Deleted checkpoint  {gold_checkpoint}")
    except Exception as e:
        print(f"(nothing to delete at checkpoint: {e})")
    spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
    print(f"✓ Dropped table       {gold_table}")
else:
    print("Cleanup skipped — flip the widget to 'yes' once to reset.")

# COMMAND ----------

# DBTITLE 1,Schema bootstrap
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
print("✓ Schema workspace.gold ready.")

# COMMAND ----------

# DBTITLE 1,Verify the Silver source
n_silver = spark.sql(f"SELECT COUNT(*) AS n FROM {source_table}").collect()[0]["n"]
if n_silver == 0:
    raise RuntimeError(
        f"{source_table} has 0 rows. Run 04_bronze_to_silver first."
    )
print(f"✓ {source_table}: {n_silver:,} rows available.")

# COMMAND ----------

# DBTITLE 1,Read Silver as a streaming source + apply the watermark
from pyspark.sql.functions import window, col, count

silver_stream = (
    spark.readStream
        .table(source_table)
        .withWatermark("event_ts", watermark_delay)
)

# COMMAND ----------

# DBTITLE 1,Build the sliding (hopping) aggregation
# `window(time_col, window_duration, slide_duration)` returns a struct
# {start, end}. With a slide_duration smaller than the window_duration,
# each event participates in (window_duration / slide_duration) windows.
agg = (
    silver_stream
        .groupBy(
            window(col("event_ts"), window_size, slide_duration),
            col("carrier"),
            col("state"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("carrier"),
            col("state"),
            col("event_count"),
        )
)

agg.printSchema()

# COMMAND ----------

# DBTITLE 1,Stream → Gold
# Append mode + watermark: a window row is emitted exactly once, when
# the watermark passes the window end. Late events that would have
# updated the count are dropped.
query = (
    agg.writeStream
        .format("delta")
        .option("checkpointLocation", gold_checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(gold_table)
)
query.awaitTermination()
print(f"✓ Wrote new Gold rows to {gold_table}.")

# COMMAND ----------

# DBTITLE 1,Inspect the sliding-window output
display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {gold_table}"))

display(spark.sql(f"""
    SELECT *
    FROM {gold_table}
    ORDER BY window_start DESC, carrier, state
    LIMIT 100
"""))

# COMMAND ----------

# DBTITLE 1,Plot-friendly view: per-carrier curve over time
# A sliding window gives you a *smooth* per-carrier rate curve. The
# tumbling view has step changes at every window boundary.
display(spark.sql(f"""
    SELECT
        window_start,
        carrier,
        SUM(event_count) AS events_in_window
    FROM {gold_table}
    GROUP BY window_start, carrier
    ORDER BY window_start ASC, carrier
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tumbling (07) vs Sliding (09) — when to pick which
# MAGIC
# MAGIC | | Tumbling | Sliding (Hopping) |
# MAGIC |---|---|---|
# MAGIC | Window position | fixed grid | overlapping grid |
# MAGIC | Each event lives in | exactly **one** window per group | **size / slide** windows |
# MAGIC | Output volume | low | up to *N×* tumbling (size/slide multiplier) |
# MAGIC | Best for | BI reports, single-bucket counts | smooth dashboards, moving-average trends, anomaly detection |
# MAGIC | State size | bounded, smaller | bounded, but **size/slide × tumbling** |
# MAGIC
# MAGIC Practical rule: start with tumbling. Switch to sliding only when
# MAGIC stakeholders complain about "jumpy" charts at window boundaries.
# MAGIC
# MAGIC ## Notes for re-running
# MAGIC
# MAGIC If you change `window_size` or `slide_duration`, the new query
# MAGIC shape is incompatible with the old checkpoint — flip
# MAGIC `cleanup_checkpoints` (well, just `cleanup` here) to `yes` once,
# MAGIC re-run, then flip back to `no` for incremental processing.

# COMMAND ----------
