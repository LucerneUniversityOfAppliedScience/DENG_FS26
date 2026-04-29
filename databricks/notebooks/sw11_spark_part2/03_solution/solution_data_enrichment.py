# Databricks notebook source

# MAGIC %md
# MAGIC # Data Enrichment — Solution
# MAGIC
# MAGIC In this notebook you learn how to **enrich raw ingested data** with the
# MAGIC standard set of metadata columns every Bronze table should carry.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC When something goes wrong in your data pipeline at 3 AM, the questions
# MAGIC are always the same: *which file did this row come from? when was it
# MAGIC loaded? by which pipeline run? was it the same row twice?* Answering
# MAGIC them takes seconds if your Bronze rows carry the right metadata —
# MAGIC otherwise it takes hours of grep, log spelunking, and S3 listings.
# MAGIC
# MAGIC The standard enrichment columns we add here:
# MAGIC
# MAGIC | Column | Purpose |
# MAGIC |---|---|
# MAGIC | File `_metadata` (path, name, size, mod time) | Lineage from source file |
# MAGIC | `load_timestamp` | When this batch landed in Bronze |
# MAGIC | `load_id` | Pipeline run identifier — groups all rows from the same run |
# MAGIC | `source_system` | Upstream system (logical name, not host) |
# MAGIC | `pipeline_name` | Which pipeline produced this row |
# MAGIC | `run_as` | User / service principal that executed the load |
# MAGIC | `hash_key` | SHA-256 over all columns — fast change detection downstream |
# MAGIC
# MAGIC In a real pipeline, `load_id` would be a UUID generated at run-start
# MAGIC and `source_system` / `pipeline_name` would be passed in via parameters
# MAGIC or pipeline-level tags. Here we hard-code them for clarity.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC The `states.csv` sample file (already staged by `copy_sample_data`) — a
# MAGIC small list of US states with abbreviation and name.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume in the UC
# MAGIC bundle. If you cloned the repo or pulled new changes, you must
# MAGIC **redeploy the bundle** before running any sw11 notebook — even ones
# MAGIC that don't need the new volume — so your workspace's bundle state
# MAGIC matches the repo. In the bundle UI, click **Deploy** once.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.functions import col, lit, to_timestamp, sha2, concat_ws

CSV_PATH      = "/Volumes/workspace/raw/sample_data/csv/states.csv"
BRONZE_TABLE  = "workspace.bronze.states_enriched"

print(f"CSV   : {CSV_PATH}")
print(f"Bronze: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop the target table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
print(f"Dropped (if existed): {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Load the raw file
# MAGIC
# MAGIC Read the CSV with header inference. This is the unenriched baseline —
# MAGIC the columns you would have without any of the metadata work below.

# COMMAND ----------

df_raw = (spark.read
    .format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(CSV_PATH))

display(df_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Add the file `_metadata` system column
# MAGIC
# MAGIC `_metadata` is a Spark system column available on every file source. It
# MAGIC is a struct containing `file_path`, `file_name`, `file_size`,
# MAGIC `file_modification_time`, `file_block_start`, and `file_block_length`.
# MAGIC The cost is zero — Spark already knows these values during the read.
# MAGIC
# MAGIC Reference: `https://docs.databricks.com/en/ingestion/file-metadata-column.html`

# COMMAND ----------

df_with_metadata = (spark.read
    .format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(CSV_PATH)
    .select("*", "_metadata"))

display(df_with_metadata.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Add a load timestamp
# MAGIC
# MAGIC `load_timestamp` is the **wall-clock time when this batch landed in
# MAGIC Bronze**. Use UTC consistently — never local time. Generate it once at
# MAGIC the start of the load and stamp every row from this batch with the
# MAGIC same value, so you can group rows by load batch later.

# COMMAND ----------

load_timestamp = datetime.now(timezone.utc).isoformat()
print(f"Load timestamp: {load_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Add a load id
# MAGIC
# MAGIC `load_id` is a unique identifier for the pipeline run. In a real
# MAGIC pipeline it would be a UUID generated at run start, or a job run ID
# MAGIC from your orchestrator (Databricks Jobs, Airflow, etc.). It groups all
# MAGIC rows from the same run, which is invaluable when you need to roll back
# MAGIC a bad load — `DELETE FROM bronze.x WHERE load_id = '<bad>'`.

# COMMAND ----------

# In production: load_id = str(uuid.uuid4()) or dbutils.notebook.run-context.runId
load_id = "demo_load_001"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Add source system and pipeline name
# MAGIC
# MAGIC `source_system` identifies the upstream system as a logical name (not a
# MAGIC hostname — hostnames change). `pipeline_name` identifies the ingestion
# MAGIC code path. Together they answer "where did this data come from and how
# MAGIC did it get here?" without reading the code.

# COMMAND ----------

source_system = "iot_eventhub_4536"
pipeline_name = "iot_ingestion"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Capture the executor (`run_as`)
# MAGIC
# MAGIC The user or service principal that executed the load. On Databricks
# MAGIC this is `current_user()`. Useful for auditing — "who loaded this?".

# COMMAND ----------

run_as = spark.sql("SELECT current_user() AS user").collect()[0]["user"]
print(f"This notebook runs as: {run_as}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Flatten the `_metadata` struct
# MAGIC
# MAGIC Struct columns are awkward to query — every consumer has to know the
# MAGIC nested path. Flatten the relevant fields into top-level columns with
# MAGIC `meta_*` prefixes, then drop the original struct. The same data, but
# MAGIC immediately useful for `WHERE meta_file_name = '...'` style filters.

# COMMAND ----------

df_flat = (spark.read
    .format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(CSV_PATH)
    .select("*", "_metadata")
    .withColumn("load_timestamp", to_timestamp(lit(load_timestamp)))
    .withColumn("load_id",        lit(load_id))
    .withColumn("source_system",  lit(source_system))
    .withColumn("pipeline_name",  lit(pipeline_name))
    .withColumn("run_as",         lit(run_as))
    .withColumn("meta_file_path",              col("_metadata.file_path"))
    .withColumn("meta_file_name",              col("_metadata.file_name"))
    .withColumn("meta_file_size",              col("_metadata.file_size"))
    .withColumn("meta_file_block_start",       col("_metadata.file_block_start"))
    .withColumn("meta_file_block_length",      col("_metadata.file_block_length"))
    .withColumn("meta_file_modification_time", col("_metadata.file_modification_time"))
    .drop("_metadata"))

display(df_flat.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Add a hash key
# MAGIC
# MAGIC A SHA-256 hash over all original (non-metadata) columns is the
# MAGIC standard primitive for **change detection**. Downstream pipelines can
# MAGIC compare today's hash with yesterday's per natural key — if different,
# MAGIC the row changed; if equal, no work needed. Compute it before
# MAGIC adding metadata columns so the hash isn't polluted by the load
# MAGIC timestamp.
# MAGIC
# MAGIC `concat_ws("||", col1, col2, ...)` joins all columns with a separator
# MAGIC that won't appear in the data. `sha2(..., 256)` returns the 64-char
# MAGIC hex digest.

# COMMAND ----------

# Capture the original (pre-enrichment) column list so the hash only covers source data.
original_columns = df_raw.columns

df_final = df_flat.withColumn(
    "hash_key",
    sha2(concat_ws("||", *[col(c) for c in original_columns]), 256)
)

display(df_final.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Write to Bronze
# MAGIC
# MAGIC Persist the enriched DataFrame as a Delta table. Subsequent runs of the
# MAGIC notebook produce the same enrichment with a new `load_timestamp` and
# MAGIC `load_id`, building a history of loads in the Bronze table — the
# MAGIC perfect substrate for the SCD2 patterns from `solution_scd2_merge.py`.

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable(BRONZE_TABLE)
print(f"Bronze table: {BRONZE_TABLE}  ({spark.table(BRONZE_TABLE).count():,} rows)")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.states_enriched LIMIT 10
