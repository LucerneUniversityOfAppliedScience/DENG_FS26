# Databricks notebook source
# MAGIC %md
# MAGIC # DLT — Gold: KPI tumbling + shipment journeys (sessions)
# MAGIC
# MAGIC Two `@dlt.table`s sitting next to each other, both reading
# MAGIC `silver_logistics` via `dlt.read_stream(...)`. The Pipeline UI
# MAGIC will show them as two parallel branches downstream of Silver.
# MAGIC
# MAGIC - `gold_carrier_kpi_per_min` — tumbling window aggregation
# MAGIC   (events per carrier per state per minute). Same idea as
# MAGIC   sw13's `05_silver_to_gold_tumbling.py`.
# MAGIC - `gold_shipment_journey` — session window per `tracking_id`
# MAGIC   with a 30-minute gap. Same idea as sw13's
# MAGIC   `06_silver_to_gold_sessions.py`.

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, window, session_window, count,
    min as smin, max as smax, first, last, collect_list, unix_timestamp,
)

# Configuration knobs — set on the pipeline, not per-table.
WINDOW_SIZE      = spark.conf.get("windows.window_size",  "1 minute")
WINDOW_WATERMARK = spark.conf.get("windows.watermark",    "3 minutes")
SESSION_GAP      = spark.conf.get("sessions.gap",         "30 minutes")
SESSION_WATERMARK = spark.conf.get("sessions.watermark",  "1 hour")

# COMMAND ----------

# DBTITLE 1,Tumbling window KPIs per carrier
@dlt.table(
    name="gold_carrier_kpi_per_min",
    comment=(
        "Event counts per (window, carrier, state) over a tumbling "
        f"window of {WINDOW_SIZE}, watermarked at {WINDOW_WATERMARK}."
    ),
    table_properties={"quality": "gold"},
)
def gold_carrier_kpi_per_min():
    return (
        dlt.read_stream("silver_logistics")
            .withWatermark("event_ts", WINDOW_WATERMARK)
            .groupBy(
                window(col("event_ts"), WINDOW_SIZE),
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

# COMMAND ----------

# DBTITLE 1,Session windows per shipment
# Watermark must exceed session_gap, otherwise Spark drops state while
# a session is still alive.
@dlt.table(
    name="gold_shipment_journey",
    comment=(
        "One row per shipment session: lifecycle of a tracking_id "
        f"with a {SESSION_GAP} inactivity gap. Watermark {SESSION_WATERMARK}."
    ),
    table_properties={"quality": "gold"},
)
def gold_shipment_journey():
    return (
        dlt.read_stream("silver_logistics")
            .withWatermark("event_ts", SESSION_WATERMARK)
            .groupBy(
                session_window(col("event_ts"), SESSION_GAP),
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
                (unix_timestamp(col("session_end"))
                 - unix_timestamp(col("session_start"))).cast("long"),
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
