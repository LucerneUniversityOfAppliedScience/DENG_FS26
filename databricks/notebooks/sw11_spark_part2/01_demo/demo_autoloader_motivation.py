# Databricks notebook source

# MAGIC %md
# MAGIC # Demo: Why Auto Loader Exists
# MAGIC
# MAGIC This is a **live-presentation demo**. Run the cells in order while
# MAGIC narrating — each phase reveals a problem, then the next phase fixes
# MAGIC it. Not an exercise, no NotImplementedErrors.
# MAGIC
# MAGIC ## The arc
# MAGIC
# MAGIC | Step | Pattern | Outcome |
# MAGIC |---|---|---|
# MAGIC | 1 | Naïve append: re-read the whole folder, write to Bronze | **Duplicates** |
# MAGIC | 2 | Hand-rolled tracker table of loaded files | Works, but **fragile** |
# MAGIC | 3 | Auto Loader (`cloudFiles`) | Robust **without writing tracker code** |
# MAGIC | 4 | Need a full refresh? Delete the checkpoint | **Reset and re-ingest** |
# MAGIC
# MAGIC The deliberate twist: filenames are **non-descriptive** (random hex IDs
# MAGIC like `data_a3f9.csv`). You can't tell which file came first by looking
# MAGIC at the names — exactly the situation in real object-storage landings
# MAGIC where producers use UUIDs.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume in the UC
# MAGIC bundle. Redeploy the bundle once if you haven't yet.

# COMMAND ----------

CATALOG       = "workspace"
DEMO_ROOT     = f"/Volumes/{CATALOG}/landing/files/sw11_demo_autoloader"
INCOMING      = f"{DEMO_ROOT}/incoming"
SCHEMAS_DIR   = f"{DEMO_ROOT}/_schemas"
CHECKPOINTS   = f"{DEMO_ROOT}/_checkpoints"

BRONZE_NAIVE  = f"{CATALOG}.bronze.demo_orders_naive"
BRONZE_TRACKED = f"{CATALOG}.bronze.demo_orders_tracked"
BRONZE_AUTO   = f"{CATALOG}.bronze.demo_orders_auto"
TRACKER_TABLE = f"{CATALOG}.meta.demo_loaded_files"

print(f"Landing : {DEMO_ROOT}")
print(f"Bronze (naive)   : {BRONZE_NAIVE}")
print(f"Bronze (tracker) : {BRONZE_TRACKED}")
print(f"Bronze (auto)    : {BRONZE_AUTO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: reset the demo
# MAGIC
# MAGIC Drop all three Bronze tables, the tracker, and wipe the landing
# MAGIC folder so every run starts from zero.

# COMMAND ----------

for table in [BRONZE_NAIVE, BRONZE_TRACKED, BRONZE_AUTO, TRACKER_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

for path in [INCOMING, SCHEMAS_DIR, CHECKPOINTS]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Cleared: {path}")
    except Exception:
        print(f"Skipped (not present): {path}")

dbutils.fs.mkdirs(INCOMING)
print(f"Created: {INCOMING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Helper: drop a "batch" of files with random hex names
# MAGIC
# MAGIC The names look like `data_a3f9.csv` — no date, no sequence, no hint
# MAGIC about arrival order. Realistic for upstream systems that use UUIDs.
# MAGIC
# MAGIC This cell only **defines** the helper. Each Step calls it explicitly
# MAGIC when it wants to seed a new batch.

# COMMAND ----------

import secrets

def seed_batch(rows_per_file, n_files=2):
    """Drop n_files CSVs into INCOMING with random hex names. Returns the file names."""
    names = []
    for _ in range(n_files):
        suffix = secrets.token_hex(2)  # 4-char hex, e.g. 'a3f9'
        name = f"data_{suffix}.csv"
        header = "order_id,customer_id,amount\n"
        body = "\n".join(
            f"{secrets.token_hex(4)},cust_{secrets.token_hex(2)},{50 + i}"
            for i in range(rows_per_file)
        )
        dbutils.fs.put(f"{INCOMING}/{name}", header + body + "\n", overwrite=True)
        names.append(name)
    return names

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 1 — Naïve append (and how it duplicates)
# MAGIC
# MAGIC The simplest thing that "works": read the whole folder and append to
# MAGIC Bronze. We seed two files, run once → 6 rows. Then we run the same
# MAGIC code a second time with no new files and see what happens.

# COMMAND ----------

# Seed Batch 1: two files, three rows each.
batch_1 = seed_batch(rows_per_file=3, n_files=2)
print(f"Batch 1 written: {batch_1}")
display(dbutils.fs.ls(INCOMING))

# COMMAND ----------

def naive_append():
    df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(INCOMING))
    df.write.mode("append").saveAsTable(BRONZE_NAIVE)
    n = spark.table(BRONZE_NAIVE).count()
    print(f"{BRONZE_NAIVE}: {n:,} rows")

naive_append()

# COMMAND ----------

# MAGIC %md
# MAGIC Now run the **same code again** with no new files. What happens?

# COMMAND ----------

naive_append()

# COMMAND ----------

# MAGIC %md
# MAGIC **12 rows.** The naïve append re-read the same two files and added
# MAGIC them to Bronze a second time. **Every row is now a duplicate.**
# MAGIC
# MAGIC In production this is catastrophic — the dashboard says revenue
# MAGIC doubled overnight but actually you just re-loaded yesterday. The
# MAGIC fix in this notebook is going to be Auto Loader, but first let's
# MAGIC see what hand-rolling looks like.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, count(*) AS times_loaded
# MAGIC FROM workspace.bronze.demo_orders_naive
# MAGIC GROUP BY order_id
# MAGIC ORDER BY times_loaded DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 2 — Hand-rolled tracker table
# MAGIC
# MAGIC The standard hand-rolled fix: keep a Delta table that records every
# MAGIC file already loaded. Before each load, list the folder and skip
# MAGIC files in the tracker. After a successful load, append the file
# MAGIC names to the tracker.
# MAGIC
# MAGIC Looks fine in a notebook. Breaks in production for a dozen reasons:
# MAGIC
# MAGIC - Concurrent runs race on the tracker → same file loaded twice
# MAGIC - File overwritten upstream → tracker says "loaded", new content lost
# MAGIC - Tracker out of sync after a manual fix → rows missed or duplicated
# MAGIC - No exactly-once: a crash between "wrote Bronze" and "wrote tracker"
# MAGIC   loads the file again on the next run
# MAGIC - Schema drift handling: zero (tracker doesn't know about schemas)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TRACKER_TABLE} (
        file_path STRING,
        loaded_at TIMESTAMP
    ) USING DELTA
""")

def tracked_append():
    loaded = {row["file_path"] for row in spark.table(TRACKER_TABLE).collect()}
    all_files = [f.path for f in dbutils.fs.ls(INCOMING) if f.path.endswith(".csv")]
    new_files = [f for f in all_files if f not in loaded]

    if not new_files:
        print("No new files. Skipping.")
        return

    print(f"New files: {[f.rsplit('/', 1)[-1] for f in new_files]}")
    df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(new_files))
    df.write.mode("append").saveAsTable(BRONZE_TRACKED)

    # Update tracker AFTER the write — same ordering rule as the HWM notebook
    spark.createDataFrame(
        [(f, None) for f in new_files], "file_path STRING, loaded_at TIMESTAMP"
    ).selectExpr("file_path", "current_timestamp() AS loaded_at") \
     .write.mode("append").saveAsTable(TRACKER_TABLE)

    print(f"{BRONZE_TRACKED}: {spark.table(BRONZE_TRACKED).count():,} rows")

tracked_append()

# COMMAND ----------

# MAGIC %md
# MAGIC Re-run with no new files: should be a no-op.

# COMMAND ----------

tracked_append()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop a second batch of files (more random hex names — you can't
# MAGIC tell from the name whether they're newer or older than batch 1).

# COMMAND ----------

batch_2 = seed_batch(rows_per_file=4, n_files=2)
print(f"Batch 2: {batch_2}")
display(dbutils.fs.ls(INCOMING))

# COMMAND ----------

tracked_append()

# COMMAND ----------

# MAGIC %md
# MAGIC The tracker correctly detected the two new files by **comparing
# MAGIC against the set of already-loaded paths**, not by sorting filenames
# MAGIC (which would have given the wrong order — `secrets.token_hex` produces
# MAGIC arbitrary suffixes).
# MAGIC
# MAGIC ### What's wrong with this?
# MAGIC
# MAGIC The pattern works **for this notebook** because the demo is
# MAGIC single-user, single-process. In production every one of the bullet
# MAGIC points above triggers eventually. The Auto Loader team at Databricks
# MAGIC built `cloudFiles` precisely because every team they met had built
# MAGIC their own buggy version of this tracker.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 3 — Auto Loader
# MAGIC
# MAGIC The same use case, with **no tracker code in your codebase**. The
# MAGIC checkpoint is internal to Spark Structured Streaming and managed for
# MAGIC you.
# MAGIC
# MAGIC Three options, every one explained in the slides:
# MAGIC - `cloudFiles.format = "csv"` — file format
# MAGIC - `cloudFiles.schemaLocation` — persisted inferred schema
# MAGIC - `header = "true"` — CSV-specific
# MAGIC
# MAGIC Plus `.trigger(availableNow=True)` — process all visible files in one
# MAGIC batch and stop. No always-on stream needed.

# COMMAND ----------

from pyspark.sql.functions import col

def autoloader_run():
    df_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMAS_DIR)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(INCOMING)
        .withColumn("_load_file", col("_metadata.file_path"))
        .withColumn("_load_ts",   col("_metadata.file_modification_time")))

    (df_stream.writeStream
        .option("checkpointLocation", CHECKPOINTS)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(BRONZE_AUTO)
        .awaitTermination())

    n = spark.table(BRONZE_AUTO).count()
    print(f"{BRONZE_AUTO}: {n:,} rows")

# First run — should pick up all 4 files seeded so far.
autoloader_run()

# COMMAND ----------

# MAGIC %md
# MAGIC Re-run with no new files. Auto Loader's checkpoint records every
# MAGIC file already committed → 0 new rows.

# COMMAND ----------

autoloader_run()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop a third batch — Auto Loader picks only the new ones.

# COMMAND ----------

batch_3 = seed_batch(rows_per_file=5, n_files=3)
print(f"Batch 3: {batch_3}")
autoloader_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 4 — Full refresh by deleting the checkpoint
# MAGIC
# MAGIC Sometimes you **need** to re-ingest everything: bug in the
# MAGIC transformation logic, schema migration, source corrected upstream,
# MAGIC etc. With Auto Loader, full refresh is one operation:
# MAGIC
# MAGIC 1. Drop the target table
# MAGIC 2. Delete the `_checkpoints/` folder (and `_schemas/` if you also
# MAGIC    want to re-infer the schema)
# MAGIC 3. Re-run the same Auto Loader cell
# MAGIC
# MAGIC The next run sees an empty checkpoint state, so every file in
# MAGIC `INCOMING` is treated as new. Same code path as forward runs —
# MAGIC **no special "fix" pipeline**.

# COMMAND ----------

# Step 1: drop the target
spark.sql(f"DROP TABLE IF EXISTS {BRONZE_AUTO}")
print(f"Dropped {BRONZE_AUTO}")

# Step 2: wipe the checkpoint (and the inferred-schema cache for good measure)
for path in [CHECKPOINTS, SCHEMAS_DIR]:
    dbutils.fs.rm(path, recurse=True)
    print(f"Cleared: {path}")

# COMMAND ----------

# Step 3: re-run Auto Loader. Same code, same options. All files re-ingested.
autoloader_run()

# COMMAND ----------

# MAGIC %md
# MAGIC The Bronze row count is now back to "all files in `INCOMING`" — full
# MAGIC refresh complete. The same Auto Loader code drives both forward
# MAGIC ingestion and full refresh; the only difference is the state of the
# MAGIC checkpoint folder when the job starts.
# MAGIC
# MAGIC ## Takeaway
# MAGIC
# MAGIC | Approach | LOC you maintain | Exactly-once | Schema evolution | Full refresh |
# MAGIC |---|---|---|---|---|
# MAGIC | Naïve append | ~3 | ❌ | ❌ | "drop and reload" |
# MAGIC | Hand-rolled tracker | ~30 + a tracker table | ⚠ tries | ❌ | manual tracker reset |
# MAGIC | Auto Loader | ~10 | ✅ | ✅ (`schemaEvolutionMode`) | delete `_checkpoints/` |
# MAGIC
# MAGIC The exercise + solution version of this notebook
# MAGIC (`exercise_autoloader.py` / `solution_autoloader.py`) goes deeper into
# MAGIC schema evolution and `cleanSource` lifecycle management. This demo
# MAGIC just makes the case for *why* Auto Loader exists in the first place.
