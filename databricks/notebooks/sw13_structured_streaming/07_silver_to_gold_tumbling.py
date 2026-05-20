# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: tumbling-window KPIs per carrier
# MAGIC
# MAGIC The first proper **stateful streaming** notebook. Until now we
# MAGIC ingested and parsed; here we **aggregate over event time** using
# MAGIC the three streaming concepts every engineer must know:
# MAGIC
# MAGIC 1. **Event time** vs processing time — we group by the timestamp
# MAGIC    *inside the data* (`event_ts`, derived from the Avro
# MAGIC    `time_utc` field), not by when Spark sees the record.
# MAGIC 2. **Watermarks** — tell Spark "I will not accept records more
# MAGIC    than X minutes late". That bounds the state size and lets
# MAGIC    Spark eventually *close* windows.
# MAGIC 3. **Tumbling windows** — fixed-size, non-overlapping buckets.
# MAGIC    Every event falls into exactly one window.
# MAGIC
# MAGIC ## The aggregation
# MAGIC
# MAGIC > For each (window, carrier, state) combination, count the events.
# MAGIC
# MAGIC Output schema:
# MAGIC
# MAGIC | column         | type      | meaning                                 |
# MAGIC |----------------|-----------|-----------------------------------------|
# MAGIC | `window_start` | timestamp | inclusive start of the tumbling window  |
# MAGIC | `window_end`   | timestamp | exclusive end                           |
# MAGIC | `carrier`      | string    | AN_POST / DHL / USPS / R_MAIL           |
# MAGIC | `state`        | string    | Received / Delivered / …                |
# MAGIC | `event_count`  | long      | how many events fell into the bucket    |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC `workspace.silver.logistics_data_gen` must exist and contain
# MAGIC rows with the `event_ts` column (from `04_bronze_to_silver`).

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("source_table",     "workspace.silver.logistics_data_gen",
                     "Silver source table")
dbutils.widgets.text("gold_table",       "workspace.gold.logistics_carrier_kpi_per_min",
                     "Gold target table")
dbutils.widgets.text("checkpoint_root",  "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.text("window_duration",  "1 minute",
                     "Tumbling window size (e.g. '1 minute', '5 minutes')")
dbutils.widgets.text("watermark_delay",  "3 minutes",
                     "Allowed late-arrival delay (watermark)")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"],
                         "Drop Gold + wipe its checkpoint?")

source_table    = dbutils.widgets.get("source_table")
gold_table      = dbutils.widgets.get("gold_table")
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/")
window_duration = dbutils.widgets.get("window_duration")
watermark_delay = dbutils.widgets.get("watermark_delay")
cleanup         = dbutils.widgets.get("cleanup")

gold_checkpoint = f"{checkpoint_root}/{gold_table.replace('.', '_')}"

print(f"Source           : {source_table}")
print(f"Gold             : {gold_table}")
print(f"Window           : {window_duration} tumbling")
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

# DBTITLE 1,Make sure workspace.gold exists
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

# Peek at the event-time column to sanity-check we have proper timestamps.
display(spark.sql(f"""
    SELECT
        MIN(event_ts) AS earliest_event,
        MAX(event_ts) AS latest_event,
        COUNT(DISTINCT carrier) AS distinct_carriers
    FROM {source_table}
"""))

# COMMAND ----------

# DBTITLE 1,Read Silver as a streaming source + apply the watermark
# A Delta table is a streaming source. `withWatermark` declares the
# event-time column and the late-data tolerance. From this point on,
# Spark knows it can drop state for windows whose end is older than
# (max event_ts seen so far) − watermark_delay.
from pyspark.sql.functions import window, col, count

silver_stream = (
    spark.readStream
        .table(source_table)
        .withWatermark("event_ts", watermark_delay)
)

# COMMAND ----------

# DBTITLE 1,Build the tumbling aggregation
# `window(time_col, duration)` creates a struct {start, end}. Group
# by that struct + the dimension columns we care about.
agg = (
    silver_stream
        .groupBy(
            window(col("event_ts"), window_duration),
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
# `outputMode("append")` for a windowed aggregation with watermark:
# Spark emits a row only once the watermark has *passed* the window
# end — i.e. when no more late records can extend that window's count.
# That's the canonical pattern; it gives stable, complete, append-only
# output, perfect for downstream BI.
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

# DBTITLE 1,Inspect the Gold table
display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {gold_table}"))

# Most recent windows first.
display(spark.sql(f"""
    SELECT *
    FROM {gold_table}
    ORDER BY window_start DESC, carrier, state
    LIMIT 100
"""))

# COMMAND ----------

# DBTITLE 1,Per-carrier delivery rate per window
# Pivot: how many events of each state per (window, carrier).
display(spark.sql(f"""
    WITH pivoted AS (
        SELECT
            window_start,
            carrier,
            SUM(CASE WHEN state = 'Received'  THEN event_count ELSE 0 END) AS received,
            SUM(CASE WHEN state = 'Delivered' THEN event_count ELSE 0 END) AS delivered
        FROM {gold_table}
        GROUP BY window_start, carrier
    )
    SELECT
        window_start,
        carrier,
        received,
        delivered,
        ROUND(delivered * 100.0 / NULLIF(received + delivered, 0), 1)
            AS delivery_pct
    FROM pivoted
    ORDER BY window_start DESC, carrier
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What just happened — the 3-second recap
# MAGIC
# MAGIC - Without `withWatermark` Spark would have to keep state for
# MAGIC   *every* window forever (in case a late record arrives) → the
# MAGIC   state grows without bound → OOM eventually.
# MAGIC - With `withWatermark("event_ts", "3 minutes")` Spark drops the
# MAGIC   state for any window whose end is older than `max(event_ts) −
# MAGIC   3 minutes`. The state size is *bounded*.
# MAGIC - `outputMode("append")` waits to emit each window until the
# MAGIC   watermark has passed its end — so each row is final and never
# MAGIC   re-emitted with updated counts.
# MAGIC - With `availableNow` trigger on Free Edition: each run advances
# MAGIC   the watermark and emits all windows that just closed. Re-run
# MAGIC   later → more events arrive → more windows close.
# MAGIC
# MAGIC ## What's next — session windows
# MAGIC
# MAGIC Continue with [`08_silver_to_gold_sessions`](./08_silver_to_gold_sessions):
# MAGIC instead of fixed-size buckets, group events by inactivity gaps
# MAGIC per `tracking_id` — the **lifecycle of each shipment**.

# COMMAND ----------
