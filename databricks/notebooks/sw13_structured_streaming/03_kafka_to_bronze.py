# Databricks notebook source
# MAGIC %md
# MAGIC # Stream a Kafka topic into a Bronze Delta table
# MAGIC
# MAGIC Reads from an Aiven-hosted Kafka topic and writes the **raw** records
# MAGIC (key + value as strings + Kafka metadata) into a Delta table at
# MAGIC `workspace.bronze.<table_name>`. No parsing — Bronze is a lossless,
# MAGIC replayable copy of the source.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. [`00_setup`](./00_setup) — credentials in `secret_scope`.
# MAGIC 2. Aiven CA `ca.pem` in a UC Volume (default
# MAGIC    `/Volumes/workspace/landing/files/aiven/ca.pem`).
# MAGIC 3. The target topic exists, e.g. `logistics_data_gen` (from Aiven's
# MAGIC    data generator) or `sensor_readings` (produced by
# MAGIC    [`02_kafka_producer`](./02_kafka_producer)).
# MAGIC 4. The schema `workspace.bronze` exists. If not, run once:
# MAGIC    ```sql
# MAGIC    CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC    ```
# MAGIC
# MAGIC ## How it runs on Free Edition
# MAGIC
# MAGIC The query uses `trigger(availableNow=True)`: each run processes
# MAGIC everything currently in Kafka and stops. The Delta table's
# MAGIC `checkpointLocation` makes the load **resumable** — the next run
# MAGIC only picks up new messages. Schedule the notebook every few minutes
# MAGIC and you have a micro-batch Bronze ingestion job.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("topic",          "logistics_data_gen", "Kafka topic")
dbutils.widgets.text("table_name",     "logistics_data_gen", "Bronze table name (workspace.bronze.<name>)")
dbutils.widgets.text("checkpoint_root", "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.text("truststore_path",
                     "/Volumes/workspace/landing/files/aiven/ca.pem",
                     "Path to Aiven CA (ca.pem)")
dbutils.widgets.dropdown("starting_offsets", "earliest", ["earliest", "latest"], "Starting offsets")
dbutils.widgets.dropdown("sasl_mechanism",   "SCRAM-SHA-256",
                         ["SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"], "SASL mechanism")
dbutils.widgets.dropdown("cleanup_checkpoints", "no", ["no", "yes"],
                         "Wipe checkpoint + table before run?")

topic            = dbutils.widgets.get("topic")
table_name       = dbutils.widgets.get("table_name").replace("-", "_")
checkpoint_root  = dbutils.widgets.get("checkpoint_root").rstrip("/")
truststore_path  = dbutils.widgets.get("truststore_path")
starting_offsets = dbutils.widgets.get("starting_offsets")
sasl_mechanism   = dbutils.widgets.get("sasl_mechanism")
cleanup          = dbutils.widgets.get("cleanup_checkpoints")

bronze_table       = f"workspace.bronze.{table_name}"
bronze_checkpoint  = f"{checkpoint_root}/{topic}/bronze"

print(f"Source topic     : {topic}")
print(f"Target table     : {bronze_table}")
print(f"Checkpoint       : {bronze_checkpoint}")
print(f"Truststore (CA)  : {truststore_path}")
print(f"Starting offsets : {starting_offsets}")

import os
if not os.path.exists(truststore_path):
    raise FileNotFoundError(
        f"CA file not found at {truststore_path}. Upload ca.pem from "
        "the Aiven service console into a Unity Catalog Volume."
    )

# COMMAND ----------

# DBTITLE 1,(Optional) Wipe checkpoint and table for a clean re-run
# Use this when:
#  - you changed the source topic and want to backfill from scratch
#  - a previous run left the checkpoint in an incompatible shape
#    ("This query does not support recovering from checkpoint location.")
#  - you want a fresh Bronze for the demo

if cleanup == "yes":
    try:
        dbutils.fs.rm(bronze_checkpoint, recurse=True)
        print(f"✓ Deleted checkpoint  {bronze_checkpoint}")
    except Exception as e:
        print(f"(nothing to delete at checkpoint: {e})")
    spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
    print(f"✓ Dropped table       {bronze_table}")
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

# DBTITLE 1,Define the Bronze projection
# Bronze rule: keep the *whole* Kafka record, byte-faithful.
#
# - `key` is conventionally a short identifier → cast to STRING for
#   readability.
# - `value` stays BINARY. Casting binary Avro to STRING via UTF-8
#   decoding mangles the bytes; the Silver layer needs the original
#   bytes to call from_avro on them. For JSON producers, BINARY is
#   still fine — Silver casts to STRING right before from_json.
# - To preview the value in a SELECT, cast it then:
#     SELECT CAST(value AS STRING) FROM workspace.bronze.<t>
from pyspark.sql.functions import col

raw_stream = (
    spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
)

bronze = (
    raw_stream.selectExpr(
        "CAST(key AS STRING) AS key",
        "value",                              # keep BINARY — lossless
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_ts",
        "timestampType AS kafka_ts_type",
        "current_timestamp() AS ingest_ts",   # when this row landed in Bronze
    )
)

bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Make sure the bronze schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
print("✓ Schema workspace.bronze ready.")

# COMMAND ----------

# DBTITLE 1,Stream Bronze → Delta
# `availableNow=True` processes whatever is in Kafka right now and stops.
# The checkpoint location records how far we got — the next run is
# incremental, no duplicates.
query = (
    bronze.writeStream
        .format("delta")
        .option("checkpointLocation", bronze_checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(bronze_table)
)
query.awaitTermination()
print(f"✓ Wrote new records to {bronze_table}.")

# COMMAND ----------

# DBTITLE 1,How much did we ingest? Latest 50 rows
display(spark.sql(f"""
    SELECT COUNT(*) AS total_rows
    FROM {bronze_table}
"""))

display(spark.sql(f"""
    SELECT
        key,
        CAST(value AS STRING) AS value_preview,     -- value is BINARY, cast for the eye
        topic, partition, offset,
        kafka_ts, ingest_ts
    FROM {bronze_table}
    ORDER BY kafka_ts DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's next — Silver
# MAGIC
# MAGIC Bronze stays untouched and replayable. The Silver layer parses the
# MAGIC `value` bytes into proper columns. Continue with
# MAGIC [`04_bronze_to_silver`](./04_bronze_to_silver): it reads
# MAGIC `workspace.bronze.<table>` as a streaming source, applies
# MAGIC `from_avro` (for `logistics_data_gen`) or `from_json` (for
# MAGIC `sensor_readings`), and writes to `workspace.silver.<table>`.

# COMMAND ----------
