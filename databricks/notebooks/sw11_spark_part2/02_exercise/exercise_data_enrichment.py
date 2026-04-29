# Databricks notebook source

# MAGIC %md
# MAGIC # Data Enrichment — Exercise
# MAGIC
# MAGIC In this exercise you learn how to **enrich raw ingested data** with the
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
# MAGIC The standard enrichment columns:
# MAGIC
# MAGIC | Column | Purpose |
# MAGIC |---|---|
# MAGIC | File `_metadata` (path, name, size, mod time) | Lineage from source file |
# MAGIC | `load_timestamp` | When this batch landed in Bronze |
# MAGIC | `load_id` | Pipeline run identifier |
# MAGIC | `source_system` | Upstream system (logical name) |
# MAGIC | `pipeline_name` | Which pipeline produced this row |
# MAGIC | `run_as` | User / service principal that executed the load |
# MAGIC | `hash_key` | SHA-256 over all columns — fast change detection |
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC The `states.csv` sample file (already staged by `copy_sample_data`).
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume in the UC
# MAGIC bundle. If you cloned the repo or pulled new changes, you must
# MAGIC **redeploy the bundle** before running any sw11 notebook.

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
# MAGIC Read `CSV_PATH` with header inference. This is the unenriched baseline.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: load the raw CSV")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Add the file `_metadata` system column
# MAGIC
# MAGIC `_metadata` is a Spark system column on every file source — a struct
# MAGIC with `file_path`, `file_name`, `file_size`, `file_modification_time`,
# MAGIC `file_block_start`, `file_block_length`. Cost is zero.
# MAGIC
# MAGIC Reference: `https://docs.databricks.com/en/ingestion/file-metadata-column.html`
# MAGIC
# MAGIC **Task:** select `*` plus `_metadata` from the same read; display.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: add _metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Add a load timestamp
# MAGIC
# MAGIC `load_timestamp` is the wall-clock time when this batch landed in
# MAGIC Bronze. **Use UTC consistently** — never local time. Generate it once
# MAGIC and stamp every row.

# COMMAND ----------

# TODO: load_timestamp = datetime.now(timezone.utc).isoformat()
# YOUR CODE HERE
raise NotImplementedError("Step 3: capture a UTC load timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Add a load id
# MAGIC
# MAGIC In production: a UUID generated at run start, or an orchestrator job ID.
# MAGIC For the exercise, hard-code a string like `"demo_load_001"`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: assign a load_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Add source system and pipeline name
# MAGIC
# MAGIC Logical names that won't change when infrastructure changes.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: assign source_system and pipeline_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Capture `run_as`
# MAGIC
# MAGIC Use `SELECT current_user()` via Spark SQL.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: capture run_as via current_user()")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Flatten the `_metadata` struct
# MAGIC
# MAGIC Struct columns are awkward downstream. Flatten the six relevant fields
# MAGIC into top-level `meta_*` columns and drop the original struct.
# MAGIC
# MAGIC Combine all enrichments from Steps 3–6 plus the flattened metadata in
# MAGIC one chained `select(...).withColumn(...)...drop("_metadata")` pipeline.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7: flatten metadata, add all enrichment columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Add a hash key
# MAGIC
# MAGIC SHA-256 over all original (non-metadata) columns is the standard
# MAGIC primitive for change detection. Compute it on the **original column
# MAGIC list** so load metadata doesn't pollute the hash.
# MAGIC
# MAGIC `concat_ws("||", col1, col2, ...)` joins with a separator unlikely to
# MAGIC appear in the data; `sha2(..., 256)` returns the hex digest.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 8: add hash_key over the original columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Write to Bronze
# MAGIC
# MAGIC Save the final enriched DataFrame as a Delta table at `BRONZE_TABLE`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 9: write to BRONZE_TABLE")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.states_enriched LIMIT 10
