# Databricks notebook source
# MAGIC %md
# MAGIC # DLT — Silver: parsed shipment events
# MAGIC
# MAGIC Reads Bronze via `dlt.read_stream(...)` (declarative dependency,
# MAGIC the DAG edge is inferred), parses the binary Avro `value` with
# MAGIC the Confluent-framing-aware decoder, and applies **data-quality
# MAGIC expectations** that surface in the Pipeline UI as metrics.
# MAGIC
# MAGIC ## Expectations and their effect
# MAGIC
# MAGIC | Decorator | If a row violates it… |
# MAGIC |---|---|
# MAGIC | `@dlt.expect_or_drop("non_null_payload", …)` | **Dropped**. Bronze still has it. |
# MAGIC | `@dlt.expect("valid_carrier", …)` | **Kept**, but counted as a warning. |
# MAGIC | `@dlt.expect_or_drop("non_null_event_ts", …)` | **Dropped**. |
# MAGIC
# MAGIC Spark exposes pass/fail counts per expectation in the Pipeline
# MAGIC UI's *Data Quality* tab.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr, from_unixtime, to_timestamp
from pyspark.sql.avro.functions import from_avro

# Avro schema matches the Aiven generator's `logistics` record.
LOGISTICS_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "logistics",
  "namespace": "data.gen.avro",
  "fields": [
    {"name": "time_utc",          "type": "long"},
    {"name": "tracking_id",       "type": "string"},
    {"name": "message",           "type": "string"},
    {"name": "carrier",           "type": "string"},
    {"name": "manifest",          "type": {"type": "array", "items": "string"}},
    {"name": "next_hop_location", "type": "string"},
    {"name": "state",             "type": "string"}
  ]
}
"""

# COMMAND ----------

# DBTITLE 1,Silver table — parsed and constrained
@dlt.table(
    name="silver_logistics",
    comment=(
        "Parsed logistics events. Confluent-framed Avro payload decoded "
        "with from_avro; event_ts derived from the Avro time_utc field. "
        "Bad parses are dropped (see expectations)."
    ),
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("non_null_payload", "tracking_id IS NOT NULL")
@dlt.expect_or_drop("non_null_event_ts", "event_ts IS NOT NULL")
@dlt.expect("valid_carrier",
            "carrier IN ('AN_POST', 'DHL', 'USPS', 'R_MAIL')")
def silver_logistics():
    bronze = dlt.read_stream("bronze_kafka_logistics")

    # Strip the 5-byte Confluent Schema Registry prefix:
    # 0x00 (magic byte) + 4-byte big-endian schema id.
    payload_bytes = expr("substring(value, 6, length(value) - 5)")

    return (
        bronze
            .select(
                "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                from_avro(
                    payload_bytes,
                    LOGISTICS_AVRO_SCHEMA,
                    {"mode": "PERMISSIVE"},
                ).alias("payload"),
            )
            .select(
                "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                to_timestamp(from_unixtime(col("payload.time_utc")))
                    .alias("event_ts"),
                "payload.tracking_id",
                "payload.message",
                "payload.carrier",
                "payload.manifest",
                "payload.next_hop_location",
                "payload.state",
            )
    )
