# Databricks notebook source
# MAGIC %md
# MAGIC # foreachBatch + Dead Letter Queue
# MAGIC
# MAGIC Production-grade Silver ingestion. So far our `04_bronze_to_silver`
# MAGIC has been **lossy**: messages with corrupt Avro just become
# MAGIC `payload IS NULL` rows that we silently keep. In a real system
# MAGIC you want those bad bytes **captured separately** so you can:
# MAGIC
# MAGIC - investigate what broke
# MAGIC - replay them later once the parser is fixed
# MAGIC - alert if the error rate spikes
# MAGIC
# MAGIC This notebook shows the canonical pattern: parse with
# MAGIC `mode="PERMISSIVE"`, then split inside `foreachBatch` into a
# MAGIC **clean Silver** table and a **Dead Letter Queue** table.
# MAGIC
# MAGIC ```
# MAGIC Bronze (binary value)
# MAGIC    │
# MAGIC    ▼  from_avro(..., mode="PERMISSIVE")
# MAGIC parsed (payload + raw value)
# MAGIC    │
# MAGIC    ├── payload IS NOT NULL ──▶ workspace.silver.<topic>
# MAGIC    └── payload IS NULL     ──▶ workspace.silver.<topic>_errors
# MAGIC ```
# MAGIC
# MAGIC One checkpoint, one micro-batch step — Spark guarantees that
# MAGIC either both writes happen or neither does (per-batch atomicity).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC `workspace.bronze.logistics_data_gen` (from `03_kafka_to_bronze`,
# MAGIC with `value` stored as BINARY).

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("source_table",      "workspace.bronze.logistics_data_gen",
                     "Bronze source table")
dbutils.widgets.text("silver_table",      "workspace.silver.logistics_data_gen_clean",
                     "Silver target (parsed rows)")
dbutils.widgets.text("errors_table",      "workspace.silver.logistics_data_gen_errors",
                     "DLQ target (parse failures)")
dbutils.widgets.text("checkpoint_root",   "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.dropdown("payload_format",
                         "avro_confluent",
                         ["avro_confluent", "avro_plain", "json"],
                         "Payload format")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"],
                         "Drop silver + errors + checkpoint?")

source_table     = dbutils.widgets.get("source_table")
silver_table     = dbutils.widgets.get("silver_table")
errors_table     = dbutils.widgets.get("errors_table")
checkpoint_root  = dbutils.widgets.get("checkpoint_root").rstrip("/")
payload_format   = dbutils.widgets.get("payload_format")
cleanup          = dbutils.widgets.get("cleanup")

# Both sinks share ONE checkpoint — foreachBatch is one logical write.
dlq_checkpoint = f"{checkpoint_root}/{silver_table.replace('.', '_')}_with_dlq"

print(f"Source           : {source_table}")
print(f"Silver           : {silver_table}")
print(f"DLQ              : {errors_table}")
print(f"Checkpoint       : {dlq_checkpoint}")
print(f"Payload format   : {payload_format}")

# COMMAND ----------

# DBTITLE 1,(Optional) Reset both targets + the checkpoint
if cleanup == "yes":
    try:
        dbutils.fs.rm(dlq_checkpoint, recurse=True)
        print(f"✓ Deleted checkpoint  {dlq_checkpoint}")
    except Exception as e:
        print(f"(nothing to delete at checkpoint: {e})")
    for t in (silver_table, errors_table):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print(f"✓ Dropped table       {t}")
else:
    print("Cleanup skipped — flip the widget to 'yes' once to reset.")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
print("✓ Schema workspace.silver ready.")

# COMMAND ----------

# DBTITLE 1,Verify the Bronze source
n_bronze = spark.sql(f"SELECT COUNT(*) AS n FROM {source_table}").collect()[0]["n"]
if n_bronze == 0:
    raise RuntimeError(
        f"{source_table} has 0 rows. Run 03_kafka_to_bronze first."
    )
print(f"✓ {source_table}: {n_bronze:,} rows available for parsing.")

# COMMAND ----------

# DBTITLE 1,Build the parsed stream (without writing yet)
# Same three-branch parser as 04_bronze_to_silver, with PERMISSIVE
# so a bad message produces payload=NULL instead of crashing.
from pyspark.sql.functions import col, expr, from_unixtime, to_timestamp

bronze_stream = spark.readStream.table(source_table)

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
    if payload_format == "avro_confluent":
        payload_bytes = expr("substring(value, 6, length(value) - 5)")
    else:
        payload_bytes = col("value")

    parsed = bronze_stream.select(
        "topic", "partition", "offset", "kafka_ts", "ingest_ts", "value",
        from_avro(payload_bytes, logistics_avro_schema, {"mode": "PERMISSIVE"})
            .alias("payload"),
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
    parsed = bronze_stream.select(
        "topic", "partition", "offset", "kafka_ts", "ingest_ts", "value",
        from_json(col("value").cast("string"), sensor_schema).alias("payload"),
    )
else:
    raise ValueError(f"Unknown payload_format: {payload_format!r}")

parsed.printSchema()

# COMMAND ----------

# DBTITLE 1,The foreachBatch function
# Spark calls this exactly once per micro-batch with:
#   batch_df: a *static* DataFrame containing this batch's rows
#   batch_id: monotonically increasing integer — useful for idempotency
#
# Both writes inside this function share the streaming query's
# checkpoint. If the function crashes mid-way, the whole batch is
# retried — so writes MUST be idempotent. `mergeSchema=true` plus
# the standard Delta "append" semantics handles that.
from pyspark.sql.functions import current_timestamp, lit

def split_and_write(batch_df, batch_id):
    # Cache so we scan the source once and write twice.
    batch_df = batch_df.persist()
    try:
        good = (
            batch_df.where("payload IS NOT NULL")
                .select(
                    "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                    to_timestamp(from_unixtime(col("payload.time_utc")))
                        .alias("event_ts")
                        if payload_format != "json"
                        else col("payload.event_ts").alias("event_ts"),
                    "payload.*",
                )
        )
        # Drop the redundant time_utc column on Avro paths (kept inside
        # payload.* but we promoted it to event_ts).
        if payload_format != "json" and "time_utc" in good.columns:
            good = good.drop("time_utc")

        bad = (
            batch_df.where("payload IS NULL")
                .select(
                    "topic", "partition", "offset", "kafka_ts", "ingest_ts",
                    col("value").alias("raw_value"),     # keep the bytes!
                )
                .withColumn("batch_id",     lit(batch_id))
                .withColumn("dlq_ingest_ts", current_timestamp())
        )

        (good.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(silver_table))

        (bad.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(errors_table))

        n_good = good.count()
        n_bad  = bad.count()
        print(f"batch {batch_id}: {n_good} ok, {n_bad} parse failures")
    finally:
        batch_df.unpersist()

# COMMAND ----------

# DBTITLE 1,Run the stream
query = (
    parsed.writeStream
        .foreachBatch(split_and_write)
        .option("checkpointLocation", dlq_checkpoint)
        .trigger(availableNow=True)
        .start()
)
query.awaitTermination()
print("✓ Done.")

# COMMAND ----------

# DBTITLE 1,How many rows landed where?
display(spark.sql(f"""
    SELECT
        '{silver_table}' AS target,
        COUNT(*)                AS rows
    FROM {silver_table}
    UNION ALL
    SELECT
        '{errors_table}',
        COUNT(*)
    FROM {errors_table}
"""))

# COMMAND ----------

# DBTITLE 1,Sample of the DLQ — what couldn't be parsed?
display(spark.sql(f"""
    SELECT
        batch_id,
        topic,
        partition,
        offset,
        CAST(raw_value AS STRING) AS raw_value_str_preview,
        LENGTH(raw_value)         AS raw_value_bytes,
        kafka_ts,
        dlq_ingest_ts
    FROM {errors_table}
    ORDER BY dlq_ingest_ts DESC, offset
    LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Sample of the clean Silver
display(spark.sql(f"""
    SELECT *
    FROM {silver_table}
    ORDER BY kafka_ts DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern recap
# MAGIC
# MAGIC | Concept | Why it matters |
# MAGIC |---|---|
# MAGIC | `mode="PERMISSIVE"` in `from_avro` / `from_json` | turn parse failures into NULLs instead of crashing the stream |
# MAGIC | `foreachBatch(fn, batch_id)` | run *any* DataFrame-level code per batch (multi-sink writes, REST calls, dynamic SQL) |
# MAGIC | One checkpoint shared across the two writes | atomicity per batch — Spark retries the whole `fn` on failure |
# MAGIC | DLQ keeps the **raw bytes** + Kafka offset | replay-friendly: once you fix the parser, you can re-process the DLQ |
# MAGIC | `mergeSchema=true` | new columns from upgraded producers don't break Silver |
# MAGIC
# MAGIC ## What you typically add next
# MAGIC
# MAGIC - **Quality alert**: another notebook or job watches
# MAGIC   `COUNT(*) FROM <errors_table> WHERE dlq_ingest_ts > now() - 1h`
# MAGIC   and pages on-call if the rate spikes.
# MAGIC - **DLQ replay job**: periodic batch that re-parses rows from
# MAGIC   the errors table with an updated schema and moves successes
# MAGIC   into Silver, leaving the rest behind.

# COMMAND ----------
