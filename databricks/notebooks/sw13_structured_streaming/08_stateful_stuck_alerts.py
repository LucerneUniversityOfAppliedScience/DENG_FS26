# Databricks notebook source
# MAGIC %md
# MAGIC # Stateful streaming: alert on stuck shipments
# MAGIC
# MAGIC Window aggregations are nice, but they can only answer **"what
# MAGIC happened in this fixed slice of time?"**. Some questions need
# MAGIC per-key state that lives across windows:
# MAGIC
# MAGIC > "Tell me every shipment that has been in `Received` for more
# MAGIC > than 30 minutes without further progress."
# MAGIC
# MAGIC That's the **stuck-shipment problem**. We need to:
# MAGIC
# MAGIC 1. Maintain *per-tracking_id* state: last seen state, last seen ts.
# MAGIC 2. When event time advances and a key has been quiet too long,
# MAGIC    emit one alert row (once — not on every batch).
# MAGIC 3. Drop the state when a shipment hits `Delivered`.
# MAGIC
# MAGIC Spark's tool for this is **`applyInPandasWithState`**: you write
# MAGIC a Python function that takes a batch of new events *and* the
# MAGIC current state object for one key, mutates state, and yields
# MAGIC output rows.
# MAGIC
# MAGIC > **Heads-up on Free Edition / serverless.** Custom stateful APIs
# MAGIC > (`applyInPandasWithState` / `mapGroupsWithState`) need a paid
# MAGIC > DBR cluster — they're **not supported on Free Edition
# MAGIC > serverless** (`UDF_USER_CODE_ERROR.STREAMING_STATE_NOT_SUPPORTED`
# MAGIC > if you try). Switch the notebook's compute to an interactive
# MAGIC > cluster (any "Standard" DBR ≥ 14.3) before running.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("source_table",      "workspace.silver.logistics_data_gen",
                     "Silver source table")
dbutils.widgets.text("alerts_table",      "workspace.gold.logistics_stuck_alerts",
                     "Alerts target table")
dbutils.widgets.text("checkpoint_root",   "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.text("stuck_threshold",   "30 minutes",
                     "How long in 'Received' before we alert")
dbutils.widgets.text("watermark_delay",   "1 hour",
                     "Allowed late-arrival delay (must exceed stuck_threshold)")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"],
                         "Drop alerts table + wipe checkpoint?")

source_table     = dbutils.widgets.get("source_table")
alerts_table     = dbutils.widgets.get("alerts_table")
checkpoint_root  = dbutils.widgets.get("checkpoint_root").rstrip("/")
stuck_threshold  = dbutils.widgets.get("stuck_threshold")
watermark_delay  = dbutils.widgets.get("watermark_delay")
cleanup          = dbutils.widgets.get("cleanup")

alerts_checkpoint = f"{checkpoint_root}/{alerts_table.replace('.', '_')}"

print(f"Source           : {source_table}")
print(f"Alerts           : {alerts_table}")
print(f"Stuck threshold  : {stuck_threshold}")
print(f"Watermark delay  : {watermark_delay}")
print(f"Checkpoint       : {alerts_checkpoint}")

# COMMAND ----------

# DBTITLE 1,(Optional) Reset alerts table + checkpoint
if cleanup == "yes":
    try:
        dbutils.fs.rm(alerts_checkpoint, recurse=True)
        print(f"✓ Deleted checkpoint  {alerts_checkpoint}")
    except Exception as e:
        print(f"(nothing to delete at checkpoint: {e})")
    spark.sql(f"DROP TABLE IF EXISTS {alerts_table}")
    print(f"✓ Dropped table       {alerts_table}")
else:
    print("Cleanup skipped — flip to 'yes' once if you want to reset.")

# COMMAND ----------

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
# Important: the watermark MUST exceed the stuck threshold, otherwise
# Spark would discard the state of a still-stuck key before we get a
# chance to fire the timer.
from pyspark.sql.functions import col
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
)
import pandas as pd

silver_stream = (
    spark.readStream
        .table(source_table)
        .withWatermark("event_ts", watermark_delay)
)

# COMMAND ----------

# DBTITLE 1,Define the output and the state schemas
# Note on timestamp handling
# ─────────────────────────
# Photon + Arrow + Pandas don't agree on timestamp precision in
# `applyInPandasWithState`. Returning a TimestampType column from the
# UDF triggers
#   ArrowInvalid: Casting from timestamp[us] to timestamp[ns] would
#   result in out of bounds timestamp
# (pandas stores ns, Arrow labels us, the writer re-casts ns→us and
# overflows). To stay portable, we use LongType (epoch milliseconds)
# for every timestamp inside the UDF and cast back to TimestampType
# *outside* the function, with a regular Spark `cast`.

# Output schema — one row per fired alert.
output_schema = StructType([
    StructField("tracking_id",          StringType(), nullable=False),
    StructField("carrier",              StringType()),
    StructField("last_state",           StringType()),
    StructField("stuck_since_epoch_ms", LongType()),
    StructField("last_known_hop",       StringType()),
    StructField("alert_epoch_ms",       LongType()),
])

# State schema — the per-tracking_id memory (all timestamps as long ms).
state_schema = StructType([
    StructField("first_epoch_ms", LongType()),
    StructField("last_epoch_ms",  LongType()),
    StructField("last_state",     StringType()),
    StructField("last_carrier",   StringType()),
    StructField("last_hop",       StringType()),
    StructField("alerted",        BooleanType()),
])

# Stuck threshold expressed in milliseconds (timer needs ms).
def _parse_duration_ms(text: str) -> int:
    """Convert '30 minutes' / '2 hours' / '45 seconds' → milliseconds."""
    n, unit = text.strip().split()
    n = int(n)
    unit = unit.lower().rstrip("s")
    return n * {"second": 1, "minute": 60, "hour": 3600, "day": 86400}[unit] * 1000

STUCK_MS = _parse_duration_ms(stuck_threshold)
print(f"Stuck threshold in ms: {STUCK_MS:,}")

# COMMAND ----------

# DBTITLE 1,The stateful function
# Called once per (tracking_id, batch). Spark hands us:
#   key:    a tuple — here just (tracking_id,)
#   pdfs:   iterator of pandas DataFrames with the new events for that key
#   state:  a GroupState object — read/update/remove the persisted state
#
# Everything inside the function is plain ints (epoch ms). We convert
# the incoming pandas Timestamps once on entry and never touch
# Timestamp/Arrow types again.
import time

def _ts_to_ms(ts) -> int:
    """pd.Timestamp or numpy datetime64 → int epoch ms."""
    return int(pd.Timestamp(ts).value // 1_000_000)

def detect_stuck(key, pdfs, state):
    tracking_id = key[0]

    # --- 1) Timer-only invocation (no new events, just timeout fired) ---
    if state.hasTimedOut:
        first_ms, last_ms, last_state, carrier, last_hop, alerted = state.get

        # Emit an alert exactly once when the timer expires while the
        # shipment is still in "Received".
        if last_state == "Received" and not alerted:
            now_ms = int(time.time() * 1000)
            yield pd.DataFrame([{
                "tracking_id":          tracking_id,
                "carrier":              carrier,
                "last_state":           last_state,
                "stuck_since_epoch_ms": last_ms,
                "last_known_hop":       last_hop,
                "alert_epoch_ms":       now_ms,
            }])
            # Mark as alerted and re-arm: if the shipment is *still*
            # stuck N more minutes later, we won't re-fire (we keep
            # the alerted flag). State stays alive until Delivered or
            # the watermark sweeps it away.
            state.update((first_ms, last_ms, last_state, carrier, last_hop, True))
            state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + STUCK_MS)
        else:
            # Either delivered already or alerted — drop the state.
            state.remove()
        return

    # --- 2) New events arrived for this tracking_id ---
    new_rows = pd.concat(list(pdfs), ignore_index=True)
    new_rows = new_rows.sort_values("event_ts")

    if state.exists:
        first_ms, last_ms, _, _, _, alerted = state.get
    else:
        first_ms = _ts_to_ms(new_rows["event_ts"].iloc[0])
        alerted  = False

    last_ms      = _ts_to_ms(new_rows["event_ts"].iloc[-1])
    last_state   = new_rows["state"].iloc[-1]
    last_carrier = new_rows["carrier"].iloc[-1]
    last_hop     = new_rows["next_hop_location"].iloc[-1]

    if last_state == "Delivered":
        # Happy path — clean up.
        state.remove()
        return

    # Still in flight: persist the latest snapshot and arm a timer
    # `stuck_threshold` after the last seen event.
    state.update((first_ms, last_ms, last_state, last_carrier, last_hop, alerted))
    state.setTimeoutTimestamp(last_ms + STUCK_MS)

    # Don't emit anything here — alerts come from the timer path.
    return iter([])

# COMMAND ----------

# DBTITLE 1,Wire the function into a streaming aggregation
alerts_raw = (
    silver_stream
        .select("tracking_id", "carrier", "state", "next_hop_location", "event_ts")
        .groupBy("tracking_id")
        .applyInPandasWithState(
            detect_stuck,
            outputStructType=output_schema,
            stateStructType=state_schema,
            outputMode="Append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
)

# Cast epoch-ms columns back to TimestampType for downstream friendliness.
# The cast happens *outside* the Python UDF so Arrow never sees Timestamps
# coming out of pandas — no precision drama.
alerts = (
    alerts_raw
        .withColumn("stuck_since_ts", (col("stuck_since_epoch_ms") / 1000).cast("timestamp"))
        .withColumn("alert_ts",       (col("alert_epoch_ms")       / 1000).cast("timestamp"))
        .select(
            "tracking_id", "carrier", "last_state",
            "stuck_since_ts", "last_known_hop", "alert_ts",
        )
)

alerts.printSchema()

# COMMAND ----------

# DBTITLE 1,Stream → Gold alerts table
query = (
    alerts.writeStream
        .format("delta")
        .option("checkpointLocation", alerts_checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(alerts_table)
)
query.awaitTermination()
print(f"✓ Wrote new alerts to {alerts_table}.")

# COMMAND ----------

# DBTITLE 1,Inspect the alerts
display(spark.sql(f"SELECT COUNT(*) AS total_alerts FROM {alerts_table}"))

display(spark.sql(f"""
    SELECT *
    FROM {alerts_table}
    ORDER BY alert_ts DESC
    LIMIT 50
"""))

# COMMAND ----------

# DBTITLE 1,Alerts per carrier
display(spark.sql(f"""
    SELECT
        carrier,
        COUNT(*) AS alerts,
        MIN(stuck_since_ts) AS earliest_stuck,
        MAX(stuck_since_ts) AS latest_stuck
    FROM {alerts_table}
    GROUP BY carrier
    ORDER BY alerts DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern recap
# MAGIC
# MAGIC - `applyInPandasWithState` exposes a per-key persisted object
# MAGIC   (`state`) plus a timer. It's the most flexible streaming API in
# MAGIC   Spark — anything you can write in Pandas + a `GroupState`,
# MAGIC   you can run on a stream.
# MAGIC - **Timers are the right tool for "absence-of-event" rules.**
# MAGIC   Window aggregations can't detect "nothing happened" — they
# MAGIC   only count what *did* happen.
# MAGIC - Watermark must be **longer than the longest possible timer**.
# MAGIC   Otherwise Spark drops the state before the timer fires.
# MAGIC - Clean up state explicitly with `state.remove()` when you no
# MAGIC   longer need it. Otherwise state grows for every key you ever
# MAGIC   see.
# MAGIC - On `availableNow`: each run advances the watermark to the
# MAGIC   max event time it saw. Timers tied to event-time fire only
# MAGIC   when the watermark crosses them. If your test data is from
# MAGIC   today and the latest event is too recent, no alert fires —
# MAGIC   re-run after the producer has sent older "Received-only"
# MAGIC   shipments.

# COMMAND ----------
