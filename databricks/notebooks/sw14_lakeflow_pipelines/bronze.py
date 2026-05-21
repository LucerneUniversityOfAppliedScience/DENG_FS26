# Databricks notebook source
# MAGIC %md
# MAGIC # DLT — Bronze: raw Kafka records
# MAGIC
# MAGIC The Bronze layer of the Lakeflow Declarative Pipeline. One
# MAGIC `@dlt.table` that opens a Spark Structured Streaming read on
# MAGIC the Aiven Kafka topic and returns the raw records.
# MAGIC
# MAGIC > **This is a DLT source file, not a runnable notebook.**
# MAGIC > Don't hit "Run All" here — register this file (together with
# MAGIC > `silver.py` and `gold.py`) as the source of a Lakeflow Pipeline
# MAGIC > via *Workflows → Pipelines → Create Pipeline*. The Databricks
# MAGIC > runner orchestrates the executions.
# MAGIC
# MAGIC ## Where the inputs come from
# MAGIC
# MAGIC DLT pipelines don't have widgets. Instead they read **pipeline
# MAGIC configuration** entries via `spark.conf.get(...)`. The expected
# MAGIC keys (and their defaults) are listed in
# MAGIC [`README.md`](./README) and set in the Pipeline UI's *Settings →
# MAGIC Configuration* section.
# MAGIC
# MAGIC Credentials still live in `secret_scope` (same scope as sw13).

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# Configuration (pipeline-level, NOT notebook widgets)
# ---------------------------------------------------------------------------
SECRET_SCOPE     = spark.conf.get("kafka.secret_scope",   "secret_scope")
TOPIC            = spark.conf.get("kafka.topic",          "logistics_data_gen")
TRUSTSTORE_PATH  = spark.conf.get("kafka.truststore_path",
                                  "/Volumes/workspace/landing/files/aiven/ca.pem")
SASL_MECHANISM   = spark.conf.get("kafka.sasl_mechanism", "SCRAM-SHA-256")
STARTING_OFFSETS = spark.conf.get("kafka.starting_offsets", "earliest")

# ---------------------------------------------------------------------------
# Build the Kafka options (same shape as sw13 — kafkashaded prefix included
# because DLT runs on the standard Databricks Kafka connector).
# ---------------------------------------------------------------------------
HOST     = dbutils.secrets.get(SECRET_SCOPE, "host")
PORT     = dbutils.secrets.get(SECRET_SCOPE, "port")
USER     = dbutils.secrets.get(SECRET_SCOPE, "user")
PASSWORD = dbutils.secrets.get(SECRET_SCOPE, "password")

if SASL_MECHANISM.startswith("SCRAM"):
    LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
else:
    LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"

JAAS_CONFIG = (
    f'{LOGIN_MODULE} required '
    f'username="{USER}" password="{PASSWORD}";'
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":       f"{HOST}:{PORT}",
    "kafka.security.protocol":       "SASL_SSL",
    "kafka.sasl.mechanism":          SASL_MECHANISM,
    "kafka.sasl.jaas.config":        JAAS_CONFIG,
    "kafka.ssl.truststore.type":     "PEM",
    "kafka.ssl.truststore.location": TRUSTSTORE_PATH,
    "subscribe":                     TOPIC,
    "startingOffsets":               STARTING_OFFSETS,
}

# COMMAND ----------

# DBTITLE 1,Bronze table
# - The function's *return value* IS the table — no writeStream call.
# - `pipelines.reset.allowed=false` means a "Full Refresh" in the UI
#   will skip this table, so a click-on-the-wrong-button doesn't wipe
#   the raw archive.
# - `value` is kept as BINARY: lossless for Confluent-framed Avro.
@dlt.table(
    name="bronze_kafka_logistics",
    comment=(
        "Raw Kafka records from the Aiven `logistics_data_gen` topic. "
        "Key cast to STRING, value kept as BINARY so the Confluent-framed "
        "Avro bytes survive verbatim into Silver."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
    },
)
def bronze_kafka_logistics():
    return (
        spark.readStream
            .format("kafka")
            .options(**KAFKA_OPTIONS)
            .load()
            .selectExpr(
                "CAST(key AS STRING) AS key",
                "value",                      # BINARY
                "topic",
                "partition",
                "offset",
                "timestamp AS kafka_ts",
                "timestampType AS kafka_ts_type",
                "current_timestamp() AS ingest_ts",
            )
    )
