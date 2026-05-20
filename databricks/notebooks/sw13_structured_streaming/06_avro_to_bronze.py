# Databricks notebook source
# MAGIC %md
# MAGIC # Avro files → Bronze (Auto Loader)
# MAGIC
# MAGIC The complement to [`05_kafka_to_avro_files`](./05_kafka_to_avro_files):
# MAGIC pick up the date-partitioned Avro archive from the Volume and
# MAGIC stream it into a Delta Bronze table — using **Auto Loader** for
# MAGIC incremental, checkpoint-based ingestion.
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/raw/files/<folder>/             ← produced by 05
# MAGIC     event_date=2026-05-20/part-….avro
# MAGIC     event_date=2026-05-21/part-….avro
# MAGIC                  │
# MAGIC                  ▼  Auto Loader (cloudFiles)
# MAGIC workspace.bronze.<table_name>                       ← this notebook
# MAGIC ```
# MAGIC
# MAGIC ## Why Auto Loader (and not `spark.read.format("avro")`)?
# MAGIC
# MAGIC - **Incremental.** Tracks every file it has already processed
# MAGIC   inside the schema-location folder, so re-runs only see the
# MAGIC   new files. No `WHERE event_date > …` cursor logic in your code.
# MAGIC - **Schema evolution.** Avro files embed their own schema —
# MAGIC   Auto Loader picks it up per file and adds new columns as they
# MAGIC   appear.
# MAGIC - **No external infrastructure.** Default is *directory listing*
# MAGIC   on the Volume; no notification queue / event grid required.
# MAGIC - **Scales.** The same pattern works whether you have 10 files
# MAGIC   or 10 million.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. [`05_kafka_to_avro_files`](./05_kafka_to_avro_files) ran at
# MAGIC    least once and produced files under
# MAGIC    `/Volumes/workspace/raw/files/<folder>/`.
# MAGIC 2. The Volume `workspace.landing.files` is writable (we put the
# MAGIC    Auto-Loader checkpoint + schema-location there).
# MAGIC 3. The schema `workspace.bronze` exists (auto-created below).

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("folder",          "logistics_data_gen",
                     "Source folder under raw_root")
dbutils.widgets.text("table_name",      "logistics_data_gen_from_files",
                     "Bronze table name (workspace.bronze.<name>)")
dbutils.widgets.text("raw_root",
                     "/Volumes/workspace/raw/files",
                     "Volume root for raw files")
dbutils.widgets.text("checkpoint_root",
                     "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"],
                         "Drop table + wipe Auto Loader state?")

folder           = dbutils.widgets.get("folder")
table_name       = dbutils.widgets.get("table_name").replace("-", "_")
raw_root         = dbutils.widgets.get("raw_root").rstrip("/")
checkpoint_root  = dbutils.widgets.get("checkpoint_root").rstrip("/")
cleanup          = dbutils.widgets.get("cleanup")

input_path        = f"{raw_root}/{folder}"
bronze_table      = f"workspace.bronze.{table_name}"
bronze_checkpoint = f"{checkpoint_root}/{folder}/avro_to_bronze_ckpt"
schema_location   = f"{checkpoint_root}/{folder}/avro_to_bronze_schema"

print(f"Source folder    : {input_path}")
print(f"Target table     : {bronze_table}")
print(f"Checkpoint       : {bronze_checkpoint}")
print(f"Schema location  : {schema_location}")

# COMMAND ----------

# DBTITLE 1,(Optional) Wipe Auto Loader state + Bronze table
# Auto Loader stores two pieces of state, both in writable Volumes:
#   - checkpoint  : how far the stream has progressed (offsets)
#   - schema loc  : inferred / evolving schema for the source files
# If either is corrupt or mismatched with the table, the stream
# refuses to start. Setting cleanup=yes wipes both AND drops the
# Delta table — perfect for re-running the demo.

if cleanup == "yes":
    for path, label in [(bronze_checkpoint, "checkpoint    "),
                        (schema_location,   "schema location")]:
        try:
            dbutils.fs.rm(path, recurse=True)
            print(f"✓ Deleted {label}  {path}")
        except Exception as e:
            print(f"(nothing to delete at {label}: {e})")
    spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")
    print(f"✓ Dropped table       {bronze_table}")
else:
    print("Cleanup skipped — set the widget to 'yes' once to reset.")

# COMMAND ----------

# DBTITLE 1,Make sure the bronze schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
print("✓ Schema workspace.bronze ready.")

# COMMAND ----------

# DBTITLE 1,Read the Avro archive with Auto Loader
# `cloudFiles` is the Auto Loader source. Key options for this case:
#   - cloudFiles.format        : the underlying file format ("avro")
#   - cloudFiles.schemaLocation: where Auto Loader caches the inferred
#                                schema. Must be a writable path that
#                                survives between runs.
#   - cloudFiles.inferColumnTypes: parse partition columns (event_date)
#                                  with their actual type instead of
#                                  defaulting to string.
#
# Notes:
#   - Directory listing is used by default — no SQS / EventBridge /
#     event-grid needed on a UC Volume.
#   - Recursive discovery is automatic; partition layout
#     `event_date=YYYY-MM-DD` is inferred as a column.
#   - The Avro file's embedded schema is the source of truth for the
#     payload columns (key, value, topic, …).
from pyspark.sql.functions import current_timestamp, input_file_name

stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",          "avro")
        .option("cloudFiles.schemaLocation",  schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(input_path)
)

stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Bronze metadata columns
# `_metadata.file_path` is a hidden built-in column added by Spark for
# every file-based source — handy for lineage and debugging.
bronze = (
    stream
        .withColumn("source_file",     input_file_name())   # which Avro file
        .withColumn("bronze_ingest_ts", current_timestamp()) # when *we* ingested
)

bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Stream → Delta (Bronze)
# availableNow=True processes whatever new files Auto Loader can see,
# then stops. Re-run the cell to ingest later batches; the checkpoint
# guarantees no duplicates.
query = (
    bronze.writeStream
        .format("delta")
        .option("checkpointLocation",      bronze_checkpoint)
        .option("mergeSchema",             "true")          # tolerate new columns
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(bronze_table)
)
query.awaitTermination()
print(f"✓ Wrote new records to {bronze_table}.")

# COMMAND ----------

# DBTITLE 1,How many rows landed, by source file?
display(spark.sql(f"SELECT COUNT(*) AS total_rows FROM {bronze_table}"))

display(spark.sql(f"""
    SELECT
        event_date,
        source_file,
        COUNT(*) AS rows
    FROM {bronze_table}
    GROUP BY event_date, source_file
    ORDER BY event_date DESC, source_file
"""))

# COMMAND ----------

# DBTITLE 1,Sample rows
display(spark.sql(f"""
    SELECT
        event_date,
        key,
        CAST(value AS STRING) AS value_preview,    -- value is BINARY
        topic, partition, offset,
        kafka_ts, ingest_ts, bronze_ingest_ts
    FROM {bronze_table}
    ORDER BY kafka_ts DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to pick this path vs `03_kafka_to_bronze`
# MAGIC
# MAGIC | Path | Latency | Re-playable? | Typical use |
# MAGIC |---|---|---|---|
# MAGIC | `03 Kafka → Bronze` (Delta) directly | seconds | Limited (Kafka retention only) | always-on micro-batch from a live broker |
# MAGIC | `05 Kafka → Avro files`, then this notebook (`06`) | minutes / hours / days | Yes — files are the source of truth, replay them any time | cold archive, audit, backfill, vendor-neutral handoff |
# MAGIC
# MAGIC The two Bronze tables (`workspace.bronze.<topic>` from 03 and
# MAGIC `workspace.bronze.<topic>_from_files` from this notebook) carry
# MAGIC the same data. Compare them — they should agree row-for-row up
# MAGIC to ingest timing:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM workspace.bronze.logistics_data_gen;
# MAGIC SELECT COUNT(*) FROM workspace.bronze.logistics_data_gen_from_files;
# MAGIC ```

# COMMAND ----------
