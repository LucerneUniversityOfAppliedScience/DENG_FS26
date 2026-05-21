# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver: parse the raw payload
# MAGIC
# MAGIC Reads the Bronze table written by
# MAGIC [`03_kafka_to_bronze`](./03_kafka_to_bronze) as a **streaming source**
# MAGIC (Delta tables can be both sink *and* source), parses the binary
# MAGIC `value` column into proper columns, and writes the result to
# MAGIC `workspace.silver.<table_name>`.
# MAGIC
# MAGIC ## Why a separate notebook?
# MAGIC
# MAGIC | Layer  | Job             | Output                            |
# MAGIC |--------|-----------------|-----------------------------------|
# MAGIC | Bronze | ingest (03)     | `workspace.bronze.<table>` (raw)  |
# MAGIC | Silver | parse + clean   | `workspace.silver.<table>`        |
# MAGIC | Gold   | aggregate       | downstream notebooks              |
# MAGIC
# MAGIC Each layer has its own checkpoint, so Bronze can be replayed and
# MAGIC re-derived into Silver without re-pulling from Kafka.
# MAGIC
# MAGIC ## Supported payload formats
# MAGIC
# MAGIC The `payload_format` widget picks the parser:
# MAGIC
# MAGIC | Value | When to use | How it parses |
# MAGIC |---|---|---|
# MAGIC | `avro_confluent` | `logistics_data_gen` (Aiven generator) | strip 5-byte Schema Registry prefix → `from_avro` |
# MAGIC | `avro_plain`     | producers without Schema Registry      | `from_avro` directly |
# MAGIC | `json`           | `sensor_readings` (from `02_kafka_producer`) | `from_json` on `CAST(value AS STRING)` |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. The Bronze table `workspace.bronze.<table_name>` exists and
# MAGIC    contains rows. Run [`03_kafka_to_bronze`](./03_kafka_to_bronze)
# MAGIC    against the same topic first.
# MAGIC 2. The schema `workspace.silver` exists. The cell below
# MAGIC    auto-creates it if missing.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("table_name",      "logistics_data_gen",
                     "Bronze / Silver table name (without catalog & schema)")
dbutils.widgets.text("checkpoint_root", "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.dropdown("payload_format",
                         "avro_confluent",
                         ["avro_confluent", "avro_plain", "json"],
                         "Payload format")
dbutils.widgets.dropdown("cleanup_silver", "no", ["no", "yes"],
                         "Drop Silver table + checkpoint before run?")

table_name      = dbutils.widgets.get("table_name").replace("-", "_")
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/")
payload_format  = dbutils.widgets.get("payload_format")
cleanup         = dbutils.widgets.get("cleanup_silver")

bronze_table      = f"workspace.bronze.{table_name}"
silver_table      = f"workspace.silver.{table_name}"
silver_checkpoint = f"{checkpoint_root}/{table_name}/silver"

print(f"Source (Bronze)  : {bronze_table}")
print(f"Target (Silver)  : {silver_table}")
print(f"Checkpoint       : {silver_checkpoint}")
print(f"Payload format   : {payload_format}")

# COMMAND ----------

# DBTITLE 1,(Optional) Drop Silver and wipe its checkpoint
# Use this when:
#  - you changed the parser (e.g. swapped json -> avro_confluent)
#  - the schema of the parsed columns is different from before
#  - you want a fresh Silver for the demo

if cleanup == "yes":
    try:
        dbutils.fs.rm(silver_checkpoint, recurse=True)
        print(f"✓ Deleted checkpoint  {silver_checkpoint}")
    except Exception as e:
        print(f"(nothing to delete at checkpoint: {e})")
    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
    print(f"✓ Dropped table       {silver_table}")
else:
    print("Cleanup skipped — set the widget to 'yes' once to reset.")

# COMMAND ----------

# DBTITLE 1,Make sure the Silver schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
print("✓ Schema workspace.silver ready.")

# COMMAND ----------

# DBTITLE 1,Verify the Bronze source exists and has rows
bronze_count = spark.sql(f"SELECT COUNT(*) AS n FROM {bronze_table}").collect()[0]["n"]
if bronze_count == 0:
    raise RuntimeError(
        f"{bronze_table} has 0 rows. Run 03_kafka_to_bronze first "
        "(with the same topic / table_name)."
    )
print(f"✓ {bronze_table}: {bronze_count} rows available for parsing.")

# COMMAND ----------

# DBTITLE 1,Read Bronze as a streaming source
# A Delta table is a valid streaming source. `readStream.table()`
# tails it from the offset stored in our Silver checkpoint, so each
# run picks up only the new rows since the last Silver run.
from pyspark.sql.functions import col, expr, from_unixtime, to_timestamp

bronze_stream = spark.readStream.table(bronze_table)
bronze_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Parse the payload — three branches
# Each branch builds a `parsed` DataFrame with the same metadata
# columns (topic / partition / offset / kafka_ts / ingest_ts) plus
# whatever fields the payload carried.

if payload_format in ("avro_confluent", "avro_plain"):
    from pyspark.sql.avro.functions import from_avro

    logistics_avro_schema = """
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

    # Aiven's data generator uses Confluent Schema Registry framing:
    # the first 5 bytes (0x00 magic + 4-byte schema id) must be stripped.
    if payload_format == "avro_confluent":
        payload_bytes = expr("substring(value, 6, length(value) - 5)")
    else:
        payload_bytes = col("value")

    # NOTE: we used to pass `{"mode": "PERMISSIVE"}` here. On Databricks
    # Serverless / Spark Connect the options-dict variant of `from_avro`
    # currently throws an INTERNAL_ERROR ("Cannot resolve the runtime
    # replaceable expression"). Default mode is FAILFAST — which is
    # fine for the Aiven data generator since it only emits valid
    # Avro. If you need to tolerate corrupt records, wrap the
    # parsing in a Python UDF that catches exceptions per row.
    parsed = (
        bronze_stream
            .select(
                "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                from_avro(payload_bytes, logistics_avro_schema).alias("payload"),
            )
            .select(
                "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                to_timestamp(from_unixtime(col("payload.time_utc"))).alias("event_ts"),
                "payload.tracking_id",
                "payload.message",
                "payload.carrier",
                "payload.manifest",
                "payload.next_hop_location",
                "payload.state",
            )
    )

elif payload_format == "json":
    from pyspark.sql.functions import from_json
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType,
    )

    sensor_schema = StructType([
        StructField("event_id",      StringType()),
        StructField("room_id",       StringType()),
        StructField("temperature_c", DoubleType()),
        StructField("humidity_pct",  DoubleType()),
        StructField("event_ts",      TimestampType()),
    ])

    parsed = (
        bronze_stream
            .select(
                "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                from_json(col("value").cast("string"), sensor_schema).alias("payload"),
            )
            .select(
                "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                "payload.event_id",
                "payload.room_id",
                "payload.temperature_c",
                "payload.humidity_pct",
                col("payload.event_ts").alias("event_ts"),
            )
    )

else:
    raise ValueError(f"Unknown payload_format: {payload_format!r}")

parsed.printSchema()

# COMMAND ----------

# DBTITLE 1,Stream Bronze → Silver
query = (
    parsed.writeStream
        .format("delta")
        .option("checkpointLocation", silver_checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(silver_table)
)
query.awaitTermination()
print(f"✓ Wrote new parsed rows to {silver_table}.")

# COMMAND ----------

# DBTITLE 1,Sample of Silver
display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {silver_table}"))
display(spark.sql(f"""
    SELECT *
    FROM {silver_table}
    ORDER BY kafka_ts DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's next — Gold
# MAGIC
# MAGIC Silver is now the well-typed source for any analytical work:
# MAGIC
# MAGIC - **Aggregations:** counts per carrier per minute, average
# MAGIC   temperature per room, etc.
# MAGIC - **Joins** with reference data (route master, sensor catalog).
# MAGIC - **SCD2** dimensions if you need to track changes over time.
# MAGIC
# MAGIC The shape stays the same: `spark.readStream.table("workspace.silver.<table>")`
# MAGIC → transform → `writeStream.format("delta").trigger(availableNow=True)`
# MAGIC → `workspace.gold.<table>`.

# COMMAND ----------
