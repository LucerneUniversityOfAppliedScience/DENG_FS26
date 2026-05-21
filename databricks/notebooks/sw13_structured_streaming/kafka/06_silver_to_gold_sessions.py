# Databricks notebook source
# MAGIC %md
# MAGIC # Silver → Gold: shipment journey via session windows
# MAGIC
# MAGIC While [`05`](./05_silver_to_gold_tumbling) buckets the stream into
# MAGIC fixed-size windows, **session windows** are *dynamic*: they grow
# MAGIC for as long as events keep arriving for the same key, and close
# MAGIC after a configurable **inactivity gap**.
# MAGIC
# MAGIC Perfect for our use case: each shipment (`tracking_id`) emits a
# MAGIC handful of events (`Received → transfer → … → Delivered`) over
# MAGIC some hours, then nothing. One session window per shipment.
# MAGIC
# MAGIC ```
# MAGIC tracking_id=track-1974256721
# MAGIC   t0   t1   t2   t3                           (gap > 30 min)
# MAGIC   │    │    │    │
# MAGIC   ▼    ▼    ▼    ▼
# MAGIC   |---- session window ----|
# MAGIC   Received  transfer  transfer  Delivered     [closed]
# MAGIC ```
# MAGIC
# MAGIC ## The aggregation
# MAGIC
# MAGIC For each `(tracking_id, session)`:
# MAGIC
# MAGIC | column            | how                                         |
# MAGIC |-------------------|---------------------------------------------|
# MAGIC | `tracking_id`     | grouping key                                |
# MAGIC | `session_start`   | first event in the session                  |
# MAGIC | `session_end`     | last event in the session                   |
# MAGIC | `duration_sec`    | `session_end − session_start`               |
# MAGIC | `event_count`     | how many records                            |
# MAGIC | `carrier`         | the (single) carrier handling the shipment  |
# MAGIC | `hops`            | array of `next_hop_location` codes          |
# MAGIC | `states`          | array of states observed                    |
# MAGIC | `final_state`     | `last(state)` — usually `Delivered`         |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC `workspace.silver.logistics_data_gen` from `04_bronze_to_silver`.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("source_table",     "workspace.silver.logistics_data_gen",
                     "Silver source table")
dbutils.widgets.text("gold_table",       "workspace.gold.logistics_shipment_journey",
                     "Gold target table")
dbutils.widgets.text("checkpoint_root",  "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.text("session_gap",      "30 minutes",
                     "Inactivity gap that closes a session")
dbutils.widgets.text("watermark_delay",  "1 hour",
                     "Allowed late-arrival delay (watermark)")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"],
                         "Drop Gold + wipe its checkpoint?")

source_table    = dbutils.widgets.get("source_table")
gold_table      = dbutils.widgets.get("gold_table")
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/")
session_gap     = dbutils.widgets.get("session_gap")
watermark_delay = dbutils.widgets.get("watermark_delay")
cleanup         = dbutils.widgets.get("cleanup")

gold_checkpoint = f"{checkpoint_root}/{gold_table.replace('.', '_')}"

print(f"Source           : {source_table}")
print(f"Gold             : {gold_table}")
print(f"Session gap      : {session_gap}")
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
print(f"✓ {source_table}: {n_silver:,} rows.")

# COMMAND ----------

# DBTITLE 1,Read Silver as a stream + apply the watermark
# Session windows need a *bigger* watermark than tumbling windows
# because a session can stay open longer. Rule of thumb:
#     watermark_delay >= session_gap + some slack
# Otherwise Spark would drop state for a session that's still alive,
# and the next event for that key would start a *new* session.
from pyspark.sql.functions import (
    col, session_window, count, min as smin, max as smax,
    first, last, collect_list, expr, unix_timestamp,
)

silver_stream = (
    spark.readStream
        .table(source_table)
        .withWatermark("event_ts", watermark_delay)
)

# COMMAND ----------

# DBTITLE 1,Build the session aggregation
agg = (
    silver_stream
        .groupBy(
            session_window(col("event_ts"), session_gap),
            col("tracking_id"),
        )
        .agg(
            smin("event_ts").alias("session_start"),
            smax("event_ts").alias("session_end"),
            count("*").alias("event_count"),
            first("carrier", ignorenulls=True).alias("carrier"),
            collect_list("next_hop_location").alias("hops"),
            collect_list("state").alias("states"),
            last("state", ignorenulls=True).alias("final_state"),
        )
        .withColumn(
            "duration_sec",
            (unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))).cast("long"),
        )
        .select(
            "tracking_id",
            "carrier",
            "session_start",
            "session_end",
            "duration_sec",
            "event_count",
            "hops",
            "states",
            "final_state",
        )
)

agg.printSchema()

# COMMAND ----------

# DBTITLE 1,Stream → Gold
# Same append-mode contract as tumbling: a session row is emitted only
# once the watermark has passed `session_end + session_gap`, i.e. when
# Spark can be sure no more events for this tracking_id will land in
# this session.
query = (
    agg.writeStream
        .format("delta")
        .option("checkpointLocation", gold_checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(gold_table)
)
query.awaitTermination()
print(f"✓ Wrote new shipment journeys to {gold_table}.")

# COMMAND ----------

# DBTITLE 1,How many sessions did we close?
display(spark.sql(f"SELECT COUNT(*) AS closed_sessions FROM {gold_table}"))

# COMMAND ----------

# DBTITLE 1,Longest-running shipments
display(spark.sql(f"""
    SELECT
        tracking_id,
        carrier,
        final_state,
        event_count,
        session_start,
        session_end,
        duration_sec,
        ROUND(duration_sec / 60.0, 1) AS duration_min,
        hops,
        states
    FROM {gold_table}
    ORDER BY duration_sec DESC
    LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Delivered vs not delivered, per carrier
display(spark.sql(f"""
    SELECT
        carrier,
        final_state,
        COUNT(*) AS shipments,
        ROUND(AVG(duration_sec) / 60.0, 1) AS avg_duration_min,
        ROUND(AVG(event_count), 1)        AS avg_hops
    FROM {gold_table}
    GROUP BY carrier, final_state
    ORDER BY carrier, final_state
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tumbling vs Session — when to use which
# MAGIC
# MAGIC | | Tumbling (07) | Session (08) |
# MAGIC |---|---|---|
# MAGIC | Window size | fixed, your choice | dynamic, ends after `gap` of silence |
# MAGIC | Each event belongs to | exactly one window per group | one session per `(key, gap)` |
# MAGIC | Best for | dashboards, "events per minute" KPIs | user / object lifecycles (clicks, shipments, trips) |
# MAGIC | State size | bounded by #windows × #keys | bounded by #active keys |
# MAGIC
# MAGIC ## Caveats and gotchas to remember
# MAGIC
# MAGIC - **`watermark_delay ≥ session_gap`** is essential — otherwise
# MAGIC   Spark forgets a session prematurely and the next event starts
# MAGIC   a phantom new one.
# MAGIC - `collect_list` here returns hops/states in **arbitrary order**
# MAGIC   inside one session window. If chronological order matters,
# MAGIC   `collect_list(struct(event_ts, next_hop_location))` then sort
# MAGIC   the array client-side, or use a stateful UDF
# MAGIC   (`applyInPandasWithState`) for full control.
# MAGIC - On Free Edition with `availableNow`, sessions close only when
# MAGIC   *enough* event-time has passed in a single run. If the source
# MAGIC   data is sparse, schedule the job recurrently so the watermark
# MAGIC   keeps moving forward.

# COMMAND ----------
