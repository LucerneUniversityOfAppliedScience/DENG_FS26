# Databricks notebook source

# MAGIC %md
# MAGIC # Auto Loader — Exercise
# MAGIC
# MAGIC In this exercise you learn how to use **Auto Loader (`cloudFiles`)** to
# MAGIC ingest files from a landing folder incrementally — exactly-once, with
# MAGIC schema inference, schema evolution, and `cleanSource` lifecycle
# MAGIC management.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Real ingestion pipelines see the same problem over and over: files arrive
# MAGIC continuously, you want to load only the new ones, and the source folder
# MAGIC mustn't grow forever. Naïve approaches re-read everything or hand-roll a
# MAGIC tracker table, both of which break in subtle ways. Auto Loader solves it
# MAGIC properly.
# MAGIC
# MAGIC ## What Auto Loader gives you
# MAGIC
# MAGIC | Feature | Without Auto Loader | With Auto Loader |
# MAGIC |---|---|---|
# MAGIC | Incremental ingestion | Hand-rolled tracker table, easy to miss late-arriving files | Built-in checkpointing |
# MAGIC | Schema inference / evolution | Hard-coded schema, manual `ALTER TABLE` | `cloudFiles.schemaLocation` + evolution modes |
# MAGIC | File discovery | Listing every run | Directory listing or file notifications |
# MAGIC | Exactly-once delivery | Difficult | Built-in via checkpoint |
# MAGIC | Source folder cleanup | Fragile shell scripts | `cloudFiles.cleanSource` |
# MAGIC
# MAGIC Databricks docs entry point (verify on your workspace; URL paths shift):
# MAGIC `https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/`
# MAGIC
# MAGIC ## Free Edition note
# MAGIC
# MAGIC We use `.trigger(availableNow=True)` — process all currently-visible
# MAGIC files in one micro-batch and stop. Right trigger for batch-style
# MAGIC notebook workflows.
# MAGIC
# MAGIC ## Before you run — REQUIRED
# MAGIC
# MAGIC This notebook writes to `/Volumes/workspace/landing/files/...`, which is
# MAGIC a **new volume** added to the UC bundle for sw11. Before running this
# MAGIC notebook you must **redeploy the bundle** so the new `landing/files`
# MAGIC volume exists in your workspace. In the bundle UI, click **Deploy**
# MAGIC once. Without this the first `dbutils.fs.mkdirs(INCOMING_DIR)` call
# MAGIC will fail with a "volume does not exist" error.

# COMMAND ----------

CATALOG     = "workspace"
LANDING_ROOT     = f"/Volumes/{CATALOG}/landing/files/sw11_autoloader/countries"
INCOMING_DIR     = f"{LANDING_ROOT}/incoming"
SCHEMA_LOCATION  = f"{LANDING_ROOT}/_schemas"
CHECKPOINT_LOC   = f"{LANDING_ROOT}/_checkpoints"
ARCHIVE_DIR      = f"{LANDING_ROOT}/_archive"

BRONZE_TABLE = f"{CATALOG}.bronze.countries_stream"

print(f"Landing root: {LANDING_ROOT}")
print(f"Incoming    : {INCOMING_DIR}")
print(f"Bronze      : {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop the Bronze table and clear the landing folder
# MAGIC
# MAGIC The notebook is designed to be re-run from scratch. We drop the Bronze
# MAGIC table and recursively delete the four landing subfolders. Without this,
# MAGIC the next run starts mid-stream against a stale checkpoint.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
print(f"Dropped (if existed): {BRONZE_TABLE}")

for path in [INCOMING_DIR, SCHEMA_LOCATION, CHECKPOINT_LOC, ARCHIVE_DIR]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Cleared: {path}")
    except Exception:
        print(f"Skipped (not present): {path}")

dbutils.fs.mkdirs(INCOMING_DIR)
print(f"Created: {INCOMING_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Why Auto Loader?
# MAGIC
# MAGIC Compare the three common approaches:
# MAGIC
# MAGIC **(a) Full refresh** — `spark.read.csv(...).mode("overwrite")` every run.
# MAGIC Wasteful and not exactly-once.
# MAGIC
# MAGIC **(b) Hand-rolled tracker table** — record `(file_path, load_ts)` per
# MAGIC batch. Brittle: late-arriving files can be missed, the tracker can drift,
# MAGIC concurrent runs corrupt it.
# MAGIC
# MAGIC **(c) Auto Loader** — checkpoint persists which files have been
# MAGIC committed; new files discovered automatically; schema persisted;
# MAGIC `cleanSource` archives processed files.
# MAGIC
# MAGIC No code in this step — read and internalise the trade-offs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Seed the landing folder with the initial batch
# MAGIC
# MAGIC The notebook synthesises its own input data so it's fully reproducible.
# MAGIC
# MAGIC **Task:** write three small CSV files into `INCOMING_DIR` representing
# MAGIC initial country master data. Use `dbutils.fs.put(path, content,
# MAGIC overwrite=True)`. Each file should have header `country_id,country_name,iso_code`
# MAGIC plus 2–3 rows. Verify with `dbutils.fs.ls(INCOMING_DIR)`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: seed the landing folder with the initial batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: First Auto Loader run with `availableNow`
# MAGIC
# MAGIC Configure a `readStream` with these `cloudFiles` options:
# MAGIC
# MAGIC | Option | Purpose |
# MAGIC |---|---|
# MAGIC | `cloudFiles.format = "csv"` | File format |
# MAGIC | `cloudFiles.schemaLocation = SCHEMA_LOCATION` | Persisted schema |
# MAGIC | `cloudFiles.inferColumnTypes = "true"` | Type inference |
# MAGIC | `header = "true"` | CSV-specific |
# MAGIC
# MAGIC Capture `_metadata.file_path` and `_metadata.file_modification_time` into
# MAGIC `_load_file` and `_load_ts` columns. Write side: `.toTable(BRONZE_TABLE)`
# MAGIC with `checkpointLocation` and `.trigger(availableNow=True)`. Wrap the
# MAGIC stream in a function `run_autoloader(extra_options=None)` so you can
# MAGIC re-use it in later steps.
# MAGIC
# MAGIC Call `run_autoloader()` once. Print Bronze row count before/after.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: define run_autoloader and run it once")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Inspect the checkpoint and schema state
# MAGIC
# MAGIC - **`_schemas/`** — inferred schema, persisted across runs
# MAGIC - **`_checkpoints/`** — tracks which files have been committed
# MAGIC
# MAGIC Don't open the files inside — they are internal. Listing them just makes
# MAGIC the persisted state tangible.
# MAGIC
# MAGIC **Task:** list both folders with `dbutils.fs.ls(...)`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: list _schemas/ and _checkpoints/")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Drop more files and re-run incrementally
# MAGIC
# MAGIC **Task:** add two new CSV files to `INCOMING_DIR` (one with new countries,
# MAGIC one renaming an existing one). Re-run `run_autoloader()`. Confirm only
# MAGIC the new files were picked up via the row-count delta.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: drop more files and re-run")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Schema evolution
# MAGIC
# MAGIC Drop a third batch with a new column `phone_code`. Auto Loader's default
# MAGIC `schemaEvolutionMode = "addNewColumns"` causes a **two-phase behaviour**:
# MAGIC the first run after a schema change fails on purpose so you notice; the
# MAGIC second run picks up the evolved schema.
# MAGIC
# MAGIC Other modes:
# MAGIC
# MAGIC | Mode | Behaviour |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (default) | Fail-then-evolve |
# MAGIC | `rescue` | Unknown columns → `_rescued_data` JSON column |
# MAGIC | `failOnNewColumns` | Fail until manual schema update |
# MAGIC | `none` | Silently ignore new columns |
# MAGIC
# MAGIC **Task:** drop one CSV with an extra `phone_code` column. Run
# MAGIC `run_autoloader()` inside `try/except` — first call expected to fail. Run
# MAGIC again — second call should succeed.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: schema evolution two-phase run")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: `cleanSource` — file lifecycle management
# MAGIC
# MAGIC In production, landing folders accumulate millions of files.
# MAGIC `cleanSource` archives or deletes processed files automatically.
# MAGIC
# MAGIC ### The three modes
# MAGIC
# MAGIC | Mode | Effect |
# MAGIC |---|---|
# MAGIC | `OFF` (default) | Files stay forever |
# MAGIC | `MOVE` | Move to `cloudFiles.cleanSource.moveDestination` after retention |
# MAGIC | `DELETE` | Delete after retention |
# MAGIC
# MAGIC ### Retention safety net
# MAGIC
# MAGIC `cloudFiles.cleanSource.retentionDuration` (default 30 days). The
# MAGIC retention is a safety window for re-processing if the checkpoint is
# MAGIC corrupt. **Never less than your worst-case incident response time.**
# MAGIC
# MAGIC ### Gotchas
# MAGIC
# MAGIC - `cleanSource` only acts on files committed to a checkpoint
# MAGIC - `MOVE` requires the destination to be reachable from the workspace
# MAGIC - `cleanSource` is not retroactive in a protective sense — files older
# MAGIC   than the retention will be cleaned regardless of when you turned it on
# MAGIC - `DELETE` is irreversible
# MAGIC - On Free Edition Serverless, `MOVE` may fail due to UC volume rules —
# MAGIC   fall back to `DELETE` and document the limitation
# MAGIC
# MAGIC **Task:** drop one more batch of files. Re-run `run_autoloader` with
# MAGIC `extra_options` containing:
# MAGIC - `cloudFiles.cleanSource = "MOVE"`
# MAGIC - `cloudFiles.cleanSource.moveDestination = ARCHIVE_DIR`
# MAGIC - `cloudFiles.cleanSource.retentionDuration = "5 minutes"` (DEMO ONLY,
# MAGIC   never use a retention this short in production)
# MAGIC
# MAGIC Wrap in try/except and fall back to `DELETE` if `MOVE` fails.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7: cleanSource MOVE with fallback to DELETE")

# COMMAND ----------

# MAGIC %md
# MAGIC `cleanSource` doesn't act immediately — it waits for the retention to
# MAGIC elapse. List both `incoming/` and `_archive/` to see where files end up.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7b: list incoming/ and _archive/")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Verify the Bronze table
# MAGIC
# MAGIC Two queries:
# MAGIC 1. Total row count
# MAGIC 2. Rows attributed to each source file (use `regexp_extract` on
# MAGIC    `_load_file` to get just the file name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: SELECT count(*) FROM workspace.bronze.countries_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: SELECT file_name, count(*) ... GROUP BY file_name ORDER BY file_name
# MAGIC --       Use regexp_extract(_load_file, '/([^/]+)$', 1) for the file name.
