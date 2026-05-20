# Databricks notebook source
# MAGIC %md
# MAGIC # Stream a Kafka topic into partitioned Avro files
# MAGIC
# MAGIC Same data, different sink: instead of writing to a Delta table
# MAGIC ([`03_kafka_to_bronze`](./03_kafka_to_bronze)) we land the records
# MAGIC as **Avro files** under a Unity Catalog Volume, partitioned by
# MAGIC ingestion date.
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/raw/files/<topic>/
# MAGIC     event_date=2026-05-20/
# MAGIC         part-00000-….avro
# MAGIC     event_date=2026-05-21/
# MAGIC         part-00000-….avro
# MAGIC ```
# MAGIC
# MAGIC ## Why Avro files in a lake?
# MAGIC
# MAGIC - **Cheap, lossless raw archive.** No table maintenance, no
# MAGIC   compaction, no vacuum. Pure object-storage layout, replayable
# MAGIC   into any future sink.
# MAGIC - **Binary-safe.** The Kafka `value` is preserved as raw bytes,
# MAGIC   so an Avro-encoded payload (e.g. `logistics_data_gen`) keeps
# MAGIC   its Confluent-Registry framing intact — no schema decisions at
# MAGIC   ingest time.
# MAGIC - **Partitioned.** Date-based folders make
# MAGIC   "give me yesterday" a single-folder scan instead of a full
# MAGIC   table read.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. [`00_setup`](./00_setup) ran — credentials in `secret_scope`.
# MAGIC 2. `ca.pem` is in a UC Volume
# MAGIC    (default `/Volumes/workspace/landing/files/aiven/ca.pem`).
# MAGIC 3. The target Volume `workspace.raw.files` exists. If not, run:
# MAGIC    ```sql
# MAGIC    CREATE SCHEMA IF NOT EXISTS workspace.raw;
# MAGIC    CREATE VOLUME  IF NOT EXISTS workspace.raw.files;
# MAGIC    ```

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("topic",          "logistics_data_gen", "Kafka topic")
dbutils.widgets.text("target_folder",  "logistics_data_gen", "Folder name under raw_root")
dbutils.widgets.text("raw_root",
                     "/Volumes/workspace/raw/files",
                     "Volume root for raw files")
dbutils.widgets.text("checkpoint_root",
                     "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.text("truststore_path",
                     "/Volumes/workspace/landing/files/aiven/ca.pem",
                     "Path to Aiven CA (ca.pem)")
dbutils.widgets.dropdown("starting_offsets", "earliest", ["earliest", "latest"], "Starting offsets")
dbutils.widgets.dropdown("sasl_mechanism",   "SCRAM-SHA-256",
                         ["SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"], "SASL mechanism")
dbutils.widgets.dropdown("cleanup_checkpoints", "no", ["no", "yes"],
                         "Wipe checkpoint + output folder?")

topic            = dbutils.widgets.get("topic")
target_folder    = dbutils.widgets.get("target_folder")
raw_root         = dbutils.widgets.get("raw_root").rstrip("/")
checkpoint_root  = dbutils.widgets.get("checkpoint_root").rstrip("/")
truststore_path  = dbutils.widgets.get("truststore_path")
starting_offsets = dbutils.widgets.get("starting_offsets")
sasl_mechanism   = dbutils.widgets.get("sasl_mechanism")
cleanup          = dbutils.widgets.get("cleanup_checkpoints")

output_path      = f"{raw_root}/{target_folder}"
avro_checkpoint  = f"{checkpoint_root}/{topic}/raw_avro"

print(f"Source topic     : {topic}")
print(f"Output folder    : {output_path}")
print(f"Checkpoint       : {avro_checkpoint}")
print(f"Truststore (CA)  : {truststore_path}")
print(f"Starting offsets : {starting_offsets}")

import os
if not os.path.exists(truststore_path):
    raise FileNotFoundError(
        f"CA file not found at {truststore_path}. Upload ca.pem from "
        "the Aiven service console into a Unity Catalog Volume."
    )

# COMMAND ----------

# DBTITLE 1,(Optional) Wipe checkpoint and output folder
# Use this when:
#  - you changed schema / partition key and want a fresh archive
#  - a previous run left the checkpoint in an incompatible shape
#  - you want to reset before re-running the demo

if cleanup == "yes":
    for path, label in [(avro_checkpoint, "checkpoint"),
                        (output_path,      "output    ")]:
        try:
            dbutils.fs.rm(path, recurse=True)
            print(f"✓ Deleted {label}  {path}")
        except Exception as e:
            print(f"(nothing to delete at {label}: {e})")
else:
    print("Cleanup skipped — set the widget to 'yes' once to reset.")

# COMMAND ----------

# DBTITLE 1,Load credentials from the secret scope
SCOPE = "secret_scope"

host     = dbutils.secrets.get(SCOPE, "host")
port     = dbutils.secrets.get(SCOPE, "port")
user     = dbutils.secrets.get(SCOPE, "user")
password = dbutils.secrets.get(SCOPE, "password")

bootstrap_servers = f"{host}:{port}"

# COMMAND ----------

# DBTITLE 1,Build the SASL JAAS config (Databricks-shaded Kafka client)
LOGIN_MODULE_PREFIX = "kafkashaded.org.apache.kafka"

if sasl_mechanism.startswith("SCRAM"):
    login_module = f"{LOGIN_MODULE_PREFIX}.common.security.scram.ScramLoginModule"
else:
    login_module = f"{LOGIN_MODULE_PREFIX}.common.security.plain.PlainLoginModule"

jaas_config = (
    f'{login_module} required '
    f'username="{user}" password="{password}";'
)

kafka_options = {
    "kafka.bootstrap.servers":      bootstrap_servers,
    "kafka.security.protocol":      "SASL_SSL",
    "kafka.sasl.mechanism":         sasl_mechanism,
    "kafka.sasl.jaas.config":       jaas_config,
    "kafka.ssl.truststore.type":     "PEM",
    "kafka.ssl.truststore.location": truststore_path,
}

assert jaas_config.startswith("kafkashaded."), (
    "JAAS prefix is wrong — restart Python and re-run from the top."
)
print(f"Kafka options ready. JAAS module: {login_module}")

# COMMAND ----------

# DBTITLE 1,Define the raw projection
# We keep `value` as BINARY so the original Kafka payload bytes are
# preserved verbatim (works for both JSON and binary Avro producers).
# `key` is usually text → cast to STRING for readability.
# `event_date` becomes the partition column.
from pyspark.sql.functions import col, to_date, current_timestamp

raw_stream = (
    spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
)

raw = (
    raw_stream.select(
        col("key").cast("string").alias("key"),
        col("value"),                                       # keep binary!
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
        col("timestampType").alias("kafka_ts_type"),
        current_timestamp().alias("ingest_ts"),
        to_date(col("timestamp")).alias("event_date"),      # partition key
    )
)

raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Stream → partitioned Avro files
# `partitionBy("event_date")` writes one folder per day:
#   /Volumes/workspace/raw/files/<topic>/event_date=YYYY-MM-DD/part-*.avro
#
# `availableNow=True` processes whatever is in Kafka now, then stops.
# The Avro file format is the de-facto raw-archive standard: small,
# splittable, schema-evolution-friendly. Spark's spark-avro is bundled
# in DBR; no extra installs.
query = (
    raw.writeStream
        .format("avro")
        .option("checkpointLocation", avro_checkpoint)
        .option("path",               output_path)
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
)
query.awaitTermination()
print(f"✓ Wrote new records to {output_path}.")

# COMMAND ----------

# DBTITLE 1,How does the output folder look?
# Top-level: one folder per ingestion date.
files = dbutils.fs.ls(output_path)
print(f"Date partitions in {output_path}:")
for f in files:
    print(f"  {f.name:<40}  ({f.size} bytes)")

# COMMAND ----------

# DBTITLE 1,Sample rows from the latest partition
# Read it back as a static DataFrame to confirm the payload is intact.
# Note: value remains binary; cast to STRING here only for preview.
sample = (
    spark.read.format("avro")
        .load(output_path)
        .selectExpr(
            "event_date",
            "key",
            "CAST(value AS STRING) AS value",
            "topic", "partition", "offset",
            "kafka_ts", "ingest_ts",
        )
)

display(
    sample.orderBy(col("kafka_ts").desc()).limit(50)
)
print(f"Total rows across all partitions: {sample.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to use Avro files vs the Bronze Delta table
# MAGIC
# MAGIC | Sink | Best for |
# MAGIC |---|---|
# MAGIC | **Bronze Delta** ([03](./03_kafka_to_bronze)) | analytics, time-travel, ACID merges, Silver-layer streaming source |
# MAGIC | **Avro files** (this notebook) | cold archive, replay, vendor-agnostic format, simple file-based downstream pipelines (Auto Loader, Glue, etc.) |
# MAGIC
# MAGIC Many teams run **both** in parallel: Avro files are the long-term
# MAGIC append-only history; the Delta Bronze table is the actively
# MAGIC queried, optimised mirror.

# COMMAND ----------
