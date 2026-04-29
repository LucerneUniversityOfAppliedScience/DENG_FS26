# Databricks notebook source

# MAGIC %md
# MAGIC # Auto Loader — Solution
# MAGIC
# MAGIC In this notebook you learn how to use **Auto Loader (`cloudFiles`)** to
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
# MAGIC The Databricks docs entry point for Auto Loader (verify the exact URL on
# MAGIC your workspace; Databricks reorganizes their docs occasionally):
# MAGIC `https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/`
# MAGIC
# MAGIC ## Free Edition note
# MAGIC
# MAGIC Auto Loader uses Structured Streaming under the hood. We use
# MAGIC `.trigger(availableNow=True)` so the stream runs as a single batch over
# MAGIC currently-visible files and stops — perfect for notebook-driven workflows
# MAGIC and Free Edition Serverless. If `cloudFiles` is not available on your
# MAGIC runtime, fall back to a manual tracker pattern (sketched at the end).
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
# MAGIC the next run starts mid-stream against a stale checkpoint — a common
# MAGIC source of confusion.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
print(f"Dropped (if existed): {BRONZE_TABLE}")

for path in [INCOMING_DIR, SCHEMA_LOCATION, CHECKPOINT_LOC, ARCHIVE_DIR]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Cleared: {path}")
    except Exception as e:
        # First run: folder doesn't exist yet — that's fine.
        print(f"Skipped (not present): {path}")

dbutils.fs.mkdirs(INCOMING_DIR)
print(f"Created: {INCOMING_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Why Auto Loader?
# MAGIC
# MAGIC Compare the three common approaches to incremental file ingestion:
# MAGIC
# MAGIC **(a) Full refresh** — `spark.read.csv(...).mode("overwrite")` every run.
# MAGIC Wasteful (re-reads everything), and not exactly-once if the source mutates
# MAGIC during a load.
# MAGIC
# MAGIC **(b) Hand-rolled tracker table** — record `(file_path, load_ts)` per
# MAGIC batch, filter `dbutils.fs.ls(...)` against it. Brittle: late-arriving
# MAGIC files can be missed if mtime is unreliable, the tracker table can drift
# MAGIC from reality, and concurrent runs corrupt it.
# MAGIC
# MAGIC **(c) Auto Loader** — Spark Structured Streaming with the `cloudFiles`
# MAGIC source. The checkpoint persists which files have been committed; new
# MAGIC files are discovered by directory listing or file notifications;
# MAGIC schema is inferred and persisted; evolved schema triggers a controlled
# MAGIC restart; `cleanSource` archives or deletes processed files.
# MAGIC
# MAGIC We use option (c) for the rest of this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Seed the landing folder with the initial batch
# MAGIC
# MAGIC The notebook synthesises its own input data so it's fully reproducible.
# MAGIC We write three small CSV files representing the initial country master
# MAGIC data into `incoming/`.

# COMMAND ----------

batch_v1 = [
    ("countries_v1_eu.csv",  "country_id,country_name,iso_code\nCH,Switzerland,756\nDE,Germany,276\nFR,France,250\n"),
    ("countries_v1_uk.csv",  "country_id,country_name,iso_code\nGB,United Kingdom,826\nIE,Ireland,372\n"),
    ("countries_v1_apac.csv","country_id,country_name,iso_code\nJP,Japan,392\nSG,Singapore,702\n"),
]

for name, content in batch_v1:
    dbutils.fs.put(f"{INCOMING_DIR}/{name}", content, overwrite=True)

display(dbutils.fs.ls(INCOMING_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: First Auto Loader run with `availableNow`
# MAGIC
# MAGIC The `readStream` configuration uses these `cloudFiles` options:
# MAGIC
# MAGIC | Option | Purpose |
# MAGIC |---|---|
# MAGIC | `cloudFiles.format = "csv"` | File format (also supports json, parquet, avro, ...) |
# MAGIC | `cloudFiles.schemaLocation` | Where Auto Loader persists the inferred schema |
# MAGIC | `cloudFiles.inferColumnTypes = "true"` | Type inference on first run |
# MAGIC | `header = "true"` | CSV-specific |
# MAGIC
# MAGIC The write side uses `.trigger(availableNow=True)` — process all
# MAGIC currently-visible files in one micro-batch and stop. This is the right
# MAGIC trigger for batch-style notebook workflows. Use `.trigger(processingTime=...)`
# MAGIC or `.trigger(continuous=...)` only for true always-on streams.
# MAGIC
# MAGIC We capture `_metadata.file_path` and `_metadata.file_modification_time`
# MAGIC into lineage columns. `_metadata` is a Spark system column available on
# MAGIC any file source.

# COMMAND ----------

from pyspark.sql.functions import col

def run_autoloader(extra_options=None):
    """Run Auto Loader once via availableNow trigger and return new row count."""
    options = {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaLocation": SCHEMA_LOCATION,
        "cloudFiles.inferColumnTypes": "true",
        "header": "true",
    }
    if extra_options:
        options.update(extra_options)

    df_stream = (spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load(INCOMING_DIR)
        .withColumn("_load_file", col("_metadata.file_path"))
        .withColumn("_load_ts",   col("_metadata.file_modification_time")))

    before = spark.table(BRONZE_TABLE).count() if spark.catalog.tableExists(BRONZE_TABLE) else 0

    (df_stream.writeStream
        .option("checkpointLocation", CHECKPOINT_LOC)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
        .awaitTermination())

    after = spark.table(BRONZE_TABLE).count()
    print(f"Bronze rows: {before} -> {after}  (added: {after - before})")
    return after - before

run_autoloader()

# COMMAND ----------

# MAGIC %md
# MAGIC `Bronze rows: 0 -> 7  (added: 7)` — three input files, seven country rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Inspect the checkpoint and schema state
# MAGIC
# MAGIC Auto Loader maintains two pieces of internal state on disk:
# MAGIC
# MAGIC - **`_schemas/`** — the inferred schema, persisted across runs. Auto
# MAGIC   Loader checks here on every run and only re-infers when new columns
# MAGIC   appear (depending on `schemaEvolutionMode`).
# MAGIC - **`_checkpoints/`** — the streaming checkpoint. Tracks which files have
# MAGIC   been committed, so the next `availableNow` run only picks up new files.
# MAGIC
# MAGIC Don't open the files inside — they are internal. Listing them just makes
# MAGIC the persisted state tangible.

# COMMAND ----------

print("=== _schemas/ ===")
display(dbutils.fs.ls(SCHEMA_LOCATION))

print("=== _checkpoints/ ===")
display(dbutils.fs.ls(CHECKPOINT_LOC))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Drop more files and re-run incrementally
# MAGIC
# MAGIC Add two new CSV files to `incoming/`. Then re-run the same Auto Loader
# MAGIC block. Only the **new** files are picked up — this is the headline
# MAGIC feature.

# COMMAND ----------

batch_v2 = [
    ("countries_v2_americas.csv",
     "country_id,country_name,iso_code\nUS,United States,840\nCA,Canada,124\nBR,Brazil,76\n"),
    ("countries_v2_renames.csv",
     "country_id,country_name,iso_code\nCH,Swiss Confederation,756\n"),  # CH renamed
]

for name, content in batch_v2:
    dbutils.fs.put(f"{INCOMING_DIR}/{name}", content, overwrite=True)

display(dbutils.fs.ls(INCOMING_DIR))

# COMMAND ----------

run_autoloader()

# COMMAND ----------

# MAGIC %md
# MAGIC `Bronze rows: 7 -> 11  (added: 4)` — only the four new rows from the two
# MAGIC new files. Auto Loader skipped the three files from batch v1 because the
# MAGIC checkpoint already records them as committed.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Schema evolution
# MAGIC
# MAGIC Drop a third batch with a new column `phone_code`. Auto Loader notices
# MAGIC the schema change and reacts according to `cloudFiles.schemaEvolutionMode`:
# MAGIC
# MAGIC | Mode | Behaviour |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (default) | First run after change fails on purpose; second run succeeds with evolved schema |
# MAGIC | `rescue` | Unknown columns go into a `_rescued_data` JSON column |
# MAGIC | `failOnNewColumns` | Strict — fails until you manually update the schema location |
# MAGIC | `none` | Silently ignore new columns |
# MAGIC
# MAGIC The default `addNewColumns` is the right choice for most pipelines: it
# MAGIC notifies you of changes (run 1 fails) and then evolves cleanly (run 2
# MAGIC succeeds). Forced two-phase behaviour is a common confusion point.

# COMMAND ----------

batch_v3 = [
    ("countries_v3_with_phone.csv",
     "country_id,country_name,iso_code,phone_code\n"
     "IT,Italy,380,39\n"
     "ES,Spain,724,34\n"),
]

for name, content in batch_v3:
    dbutils.fs.put(f"{INCOMING_DIR}/{name}", content, overwrite=True)

# First run after schema change — expected to fail by design.
try:
    run_autoloader()
    print("Stream completed without schema-change retry — your runtime may handle this differently.")
except Exception as e:
    print(f"Expected failure on first run after schema change: {type(e).__name__}")
    print("This is by design — Auto Loader wants you to notice the change. Re-run to evolve.")

# COMMAND ----------

# Second run — schema has been updated in _schemas/, ingestion succeeds.
run_autoloader()

# COMMAND ----------

# MAGIC %md
# MAGIC The `phone_code` column is now part of the Bronze schema. Older rows
# MAGIC (from batches v1 and v2) have NULL in this column.

# COMMAND ----------

display(spark.table(BRONZE_TABLE).orderBy("_load_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: `cleanSource` — file lifecycle management
# MAGIC
# MAGIC In production, landing folders accumulate millions of files. Directory
# MAGIC listing slows to a crawl, billing creeps up, and people resort to
# MAGIC fragile bash scripts to clean up. Auto Loader's `cleanSource` does it
# MAGIC properly — checkpoint-aware, retention-protected.
# MAGIC
# MAGIC ### The three modes
# MAGIC
# MAGIC | Mode | Effect |
# MAGIC |---|---|
# MAGIC | `OFF` (default) | Files stay forever |
# MAGIC | `MOVE` | Move processed files to `cloudFiles.cleanSource.moveDestination` after retention |
# MAGIC | `DELETE` | Delete processed files after retention |
# MAGIC
# MAGIC ### The retention safety net
# MAGIC
# MAGIC `cloudFiles.cleanSource.retentionDuration` (default **30 days**). The
# MAGIC retention is a safety window: if your checkpoint is corrupt and you have
# MAGIC to re-process, the source files must still be there. Recommendation: at
# MAGIC least 7 days, ideally 30. **Never less than your worst-case incident
# MAGIC response time.**
# MAGIC
# MAGIC ### Gotchas
# MAGIC
# MAGIC - `cleanSource` only acts on files that have been **committed to a
# MAGIC   checkpoint** — it never deletes unprocessed data.
# MAGIC - `MOVE` requires the destination to be reachable from the same
# MAGIC   workspace (UC volume / cloud storage rules apply).
# MAGIC - `cleanSource` is **not retroactive in the protective sense**: files
# MAGIC   older than the retention will be cleaned regardless of when you turned
# MAGIC   it on. This surprises people who expected a "grace period from now".
# MAGIC - `DELETE` is irreversible. Prefer `MOVE` and let lifecycle policies on
# MAGIC   the archive bucket handle final deletion.
# MAGIC - On Free Edition Serverless, `cleanSource=MOVE` may have UC volume
# MAGIC   restrictions; if `MOVE` fails, fall back to `DELETE` for the demo.
# MAGIC
# MAGIC ### Hands-on
# MAGIC
# MAGIC We reconfigure the stream with `cleanSource = "MOVE"` and a **5-minute
# MAGIC retention — NOT a production-safe value**, only short enough to see
# MAGIC archival happen during the lesson.

# COMMAND ----------

# Drop a fresh batch so cleanSource has new files to track lifecycle on.
batch_v4 = [
    ("countries_v4_extra.csv",
     "country_id,country_name,iso_code,phone_code\nNL,Netherlands,528,31\nBE,Belgium,56,32\n"),
]
for name, content in batch_v4:
    dbutils.fs.put(f"{INCOMING_DIR}/{name}", content, overwrite=True)

# Run Auto Loader with cleanSource enabled.
clean_source_options = {
    "cloudFiles.cleanSource": "MOVE",
    "cloudFiles.cleanSource.moveDestination": ARCHIVE_DIR,
    "cloudFiles.cleanSource.retentionDuration": "5 minutes",  # DEMO ONLY
}

try:
    run_autoloader(extra_options=clean_source_options)
    print("cleanSource MOVE configured. Files older than retention will be archived on the next run.")
except Exception as e:
    print(f"cleanSource MOVE failed: {type(e).__name__}: {e}")
    print("Falling back to DELETE mode for the demo.")
    delete_options = {
        "cloudFiles.cleanSource": "DELETE",
        "cloudFiles.cleanSource.retentionDuration": "5 minutes",
    }
    run_autoloader(extra_options=delete_options)

# COMMAND ----------

# MAGIC %md
# MAGIC `cleanSource` doesn't archive files immediately — it waits for the
# MAGIC retention to elapse. The 5-minute retention is short enough to see in
# MAGIC the lesson, but in production never go below your incident-response
# MAGIC window.
# MAGIC
# MAGIC Inspect both folders. Right after the run, `incoming/` still contains
# MAGIC everything (retention hasn't elapsed yet); after a follow-up run later
# MAGIC than 5 minutes from each file's commit, files migrate to `_archive/`.

# COMMAND ----------

print("=== incoming/ ===")
try:
    display(dbutils.fs.ls(INCOMING_DIR))
except Exception:
    print("(empty or missing)")

print("=== _archive/ ===")
try:
    display(dbutils.fs.ls(ARCHIVE_DIR))
except Exception:
    print("(empty or missing — wait for retention to elapse, then re-run cleanSource)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Verify the Bronze table
# MAGIC
# MAGIC Two queries that show the full picture: total rows, and rows attributed
# MAGIC to each source file. The lineage columns (`_load_file`, `_load_ts`)
# MAGIC capture which file every row came from — invaluable for debugging.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS total_rows FROM workspace.bronze.countries_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regexp_extract(_load_file, '/([^/]+)$', 1) AS file_name,
# MAGIC        count(*) AS rows
# MAGIC FROM workspace.bronze.countries_stream
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Free Edition fallback (if `cloudFiles` is unavailable)
# MAGIC
# MAGIC If your runtime rejects `cloudFiles`, replace `readStream.format("cloudFiles")`
# MAGIC with this manual pattern:
# MAGIC
# MAGIC ```python
# MAGIC tracker_table = "workspace.bronze._loaded_files"
# MAGIC spark.sql(f"CREATE TABLE IF NOT EXISTS {tracker_table} (file_path STRING, load_ts TIMESTAMP)")
# MAGIC
# MAGIC loaded = {row.file_path for row in spark.table(tracker_table).collect()}
# MAGIC all_files = [f.path for f in dbutils.fs.ls(INCOMING_DIR)]
# MAGIC new_files = [f for f in all_files if f not in loaded]
# MAGIC
# MAGIC if new_files:
# MAGIC     df_new = spark.read.option("header", "true").csv(new_files)
# MAGIC     df_new.write.mode("append").saveAsTable(BRONZE_TABLE)
# MAGIC     spark.createDataFrame(
# MAGIC         [(f, None) for f in new_files], "file_path STRING, load_ts TIMESTAMP"
# MAGIC     ).write.mode("append").saveAsTable(tracker_table)
# MAGIC ```
# MAGIC
# MAGIC Same Bronze content; you lose schema evolution and `cleanSource`. This
# MAGIC fallback is the reason Auto Loader exists in the first place — every
# MAGIC team that builds the manual version eventually hits the gotchas.
