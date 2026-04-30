# Databricks notebook source

# MAGIC %md
# MAGIC # Delta Change Data Feed (CDF) — Exercise
# MAGIC
# MAGIC In this exercise you learn how to use **Delta Change Data Feed** to
# MAGIC propagate changes (inserts, updates, deletes) from a Silver table
# MAGIC down to a Gold table without re-reading the whole Silver snapshot.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC | Strategy | What it reads | Catches deletes? |
# MAGIC |---|---|---|
# MAGIC | Full snapshot | Everything every run | Yes (by absence) |
# MAGIC | `WHERE updated_at > :hwm` | Recently-modified rows | **No** — hard deletes invisible |
# MAGIC | **CDF** | Just the change events | **Yes** — explicit `delete` events |
# MAGIC
# MAGIC ## The four `_change_type` values
# MAGIC
# MAGIC | Value | Meaning | What you usually do |
# MAGIC |---|---|---|
# MAGIC | `insert` | New row | INSERT into target |
# MAGIC | `update_postimage` | Row after update | UPDATE SET * |
# MAGIC | `update_preimage` | Row before update | drop — diagnostics only |
# MAGIC | `delete` | Row was deleted | DELETE matching key |
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC Redeploy the UC bundle once if you haven't yet.

# COMMAND ----------

CATALOG = "workspace"

SILVER_TABLE  = f"{CATALOG}.silver.customers_cdf"
GOLD_TABLE    = f"{CATALOG}.gold.customers_synced"
META_TABLE    = f"{CATALOG}.meta.cdf_state"
PIPELINE_NAME = "customers_silver_to_gold"

print(f"Silver  : {SILVER_TABLE}")
print(f"Gold    : {GOLD_TABLE}")
print(f"Meta    : {META_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop all three tables

# COMMAND ----------

for table in [SILVER_TABLE, GOLD_TABLE, META_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Why CDF? (markdown)
# MAGIC
# MAGIC Without CDF, "where updated_at > hwm" silently misses hard deletes.
# MAGIC CDF emits explicit `_change_type = 'delete'` events for every
# MAGIC deleted row.
# MAGIC
# MAGIC One caveat: CDF only contains changes **after enabling**. The
# MAGIC initial snapshot still needs a full read.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Create the Silver table and enable CDF
# MAGIC
# MAGIC **Task:** create `SILVER_TABLE` with columns
# MAGIC `(id INT, name STRING, email STRING, country STRING)` USING DELTA,
# MAGIC and TBLPROPERTIES `delta.enableChangeDataFeed = true`. The flag is
# MAGIC the magic switch that turns CDF on.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: CREATE TABLE workspace.silver.customers_cdf (...) USING DELTA
# MAGIC --       TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Initial load
# MAGIC
# MAGIC **Task:** INSERT four rows into Silver:
# MAGIC ```
# MAGIC (1, 'Alice',   'alice@example.com',   'CH'),
# MAGIC (2, 'Bob',     'bob@example.com',     'DE'),
# MAGIC (3, 'Charlie', 'charlie@example.com', 'FR'),
# MAGIC (4, 'Diana',   'diana@example.com',   'IT')
# MAGIC ```
# MAGIC Then run `DESCRIBE HISTORY` to confirm one commit.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: INSERT INTO ... VALUES (...);
# MAGIC -- TODO: DESCRIBE HISTORY workspace.silver.customers_cdf

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Build the initial Gold snapshot + metadata table
# MAGIC
# MAGIC The first time downstream runs, it has no CDF state, so it reads
# MAGIC the full Silver snapshot. Subsequent runs read from CDF.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Snapshot Silver into Gold (`overwrite`).
# MAGIC 2. Create `META_TABLE` with columns
# MAGIC    `(pipeline STRING, last_cdf_version BIGINT, updated_at TIMESTAMP)`.
# MAGIC 3. Insert one row for `PIPELINE_NAME` with the current Silver
# MAGIC    version (read from `DESCRIBE HISTORY`, take `max(version)`).

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: Gold snapshot + meta table seed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Make changes upstream
# MAGIC
# MAGIC Three commits on Silver:
# MAGIC 1. INSERT two new rows: `(5, 'Eve', 'eve@example.com', 'ES')` and
# MAGIC    `(6, 'Finn', 'finn@example.com', 'IE')`
# MAGIC 2. UPDATE Charlie's email to `'charlie.new@example.com'`
# MAGIC 3. DELETE Bob (`id = 2`)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: INSERT new rows for Eve and Finn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: UPDATE Charlie's email

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: DELETE Bob

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Read the changes
# MAGIC
# MAGIC `spark.read.format("delta").option("readChangeFeed", "true")
# MAGIC .option("startingVersion", N).table(SILVER_TABLE)` returns one row
# MAGIC per change event since version N. The free metadata columns are
# MAGIC `_change_type`, `_commit_version`, `_commit_timestamp`.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Read `last_consumed` from `META_TABLE` for this pipeline.
# MAGIC 2. Read changes starting from `last_consumed + 1`.
# MAGIC 3. Display ordered by `_commit_version`, `_change_type`, `id`.
# MAGIC
# MAGIC You should see 6 events for 3 logical changes (every UPDATE emits
# MAGIC a pre/post pair).

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: read CDF events since last_consumed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Apply changes downstream
# MAGIC
# MAGIC The standard CDF apply pattern:
# MAGIC - Drop `update_preimage` rows (diagnostics only)
# MAGIC - `insert` and `update_postimage` go through MERGE as upserts
# MAGIC - `delete` goes through MERGE as a key-matched delete
# MAGIC
# MAGIC **Task:** two MERGE branches against `GOLD_TABLE`:
# MAGIC 1. Branch 1 — `_change_type IN ('insert', 'update_postimage')`,
# MAGIC    `MERGE ... WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`.
# MAGIC 2. Branch 2 — `_change_type = 'delete'`,
# MAGIC    `MERGE ... WHEN MATCHED THEN DELETE`.
# MAGIC
# MAGIC Display Gold afterwards. Bob (id=2) should be gone, Charlie's email
# MAGIC should be the new one, Eve and Finn should be present.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7: two MERGE branches (upserts, deletes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Commit the new CDF watermark
# MAGIC
# MAGIC Same ordering rule as the HWM notebook: bookkeeping comes after the
# MAGIC sink write succeeds.
# MAGIC
# MAGIC **Task:** read the latest version from `DESCRIBE HISTORY SILVER_TABLE`
# MAGIC and UPDATE `META_TABLE` for this pipeline.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 8: update last_cdf_version in META_TABLE")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Idempotency proof
# MAGIC
# MAGIC Read changes from the new HWM. Should be zero rows. Wrap in
# MAGIC try/except — `table_changes` raises if `startingVersion` exceeds
# MAGIC table history; treat that as 0 events.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 9: re-read CDF, expect 0 events")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 10: Caveats (markdown)
# MAGIC
# MAGIC ### CDF only captures changes after enabling
# MAGIC The initial 1M rows of an existing table won't be in CDF history.
# MAGIC The first downstream consumer still needs a full snapshot to
# MAGIC bootstrap.
# MAGIC
# MAGIC ### CDF retention follows VACUUM
# MAGIC When `VACUUM` removes old data files past retention (default 7
# MAGIC days), CDF events for those versions are also gone. If a consumer
# MAGIC falls behind by more than the VACUUM retention, re-bootstrap from
# MAGIC a full snapshot.
# MAGIC
# MAGIC ### Combine with `WITH SCHEMA EVOLUTION`
# MAGIC If Silver gains a new column mid-pipeline, MERGE needs
# MAGIC `MERGE WITH SCHEMA EVOLUTION` to propagate it.
