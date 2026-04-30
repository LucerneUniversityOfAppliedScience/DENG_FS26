# Databricks notebook source

# MAGIC %md
# MAGIC # HWM-driven Incremental JDBC Ingest — Exercise
# MAGIC
# MAGIC In this exercise you learn the **everyday incremental ingestion
# MAGIC pattern**: pull only rows newer than the last successful run from a
# MAGIC SQL Server source, using a high-water mark (HWM) stored in a
# MAGIC metadata table.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC The full-refresh ingest (`exercise_sql_ingest.py`) re-reads the
# MAGIC entire source on every run. Doesn't scale: hour 1 reads 1M rows,
# MAGIC hour 24 reads 24M rows even if only 1k changed. Incremental
# MAGIC ingestion bounds the per-run cost to "what changed since last time".
# MAGIC
# MAGIC The HWM pattern is the simplest incremental pattern and the right
# MAGIC default for any source that exposes a reliable `updated_at` /
# MAGIC `ModifiedDate` column.
# MAGIC
# MAGIC ## The three incremental patterns
# MAGIC
# MAGIC | Pattern | Source signal | Target write | Typical use |
# MAGIC |---|---|---|---|
# MAGIC | **Append-only** | Immutable events | `INSERT` / append | Logs, clicks |
# MAGIC | **Upsert (MERGE)** | Mutable rows + key | `MERGE INTO` | Master data |
# MAGIC | **CDC** | Insert / update / **delete** events | `MERGE` per type | OLTP replicas |
# MAGIC
# MAGIC This exercise covers append-style HWM.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume **and** a
# MAGIC new `meta` schema. Redeploy the bundle once if you haven't yet. Click
# MAGIC **Deploy** in the bundle UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # ⚠ SECURITY WARNING — READ BEFORE RUNNING
# MAGIC
# MAGIC ## **DO NOT USE THIS PATTERN IN PRODUCTION.**
# MAGIC
# MAGIC The cells below read SQL Server credentials from **notebook widgets**.
# MAGIC Acceptable for **classroom demos only**. In any real pipeline:
# MAGIC
# MAGIC - **NEVER** hard-code credentials in a notebook
# MAGIC - **NEVER** type credentials into a widget that gets saved with the notebook
# MAGIC - **NEVER** commit notebooks containing real credentials to git
# MAGIC - **NEVER** log or `print()` the password
# MAGIC
# MAGIC In production: **Databricks Secret Scopes** with
# MAGIC `dbutils.secrets.get(scope, key)` (output is auto-redacted).
# MAGIC
# MAGIC The user/password for the AdventureWorks demo are published on
# MAGIC **ILIAS**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

CATALOG       = "workspace"
PIPELINE_NAME = "sales_customer"

BRONZE_TABLE  = f"{CATALOG}.bronze.sales_customer_incremental"
META_TABLE    = f"{CATALOG}.meta.ingest_state"

DB_HOST = "nonacomp-sql.database.windows.net"
DB_NAME = "AdventureWorks"
DB_PORT = "1433"
SOURCE_SCHEMA = "Sales"
SOURCE_TABLE  = "Customer"
HWM_COLUMN    = "ModifiedDate"

print(f"Pipeline   : {PIPELINE_NAME}")
print(f"Bronze     : {BRONZE_TABLE}")
print(f"Meta       : {META_TABLE}")
print(f"Source     : {DB_NAME}.{SOURCE_SCHEMA}.{SOURCE_TABLE} (HWM column: {HWM_COLUMN})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop Bronze + metadata
# MAGIC
# MAGIC In production you would NOT drop the metadata table. Here we want a
# MAGIC clean slate so each run demonstrates the pattern from zero.

# COMMAND ----------

for table in [BRONZE_TABLE, META_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read credentials from widgets
# MAGIC
# MAGIC Two text widgets `user` and `pw`. Validate that both are non-empty.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: declare and read user/pw widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Initialise the metadata table
# MAGIC
# MAGIC The metadata table holds one row per pipeline:
# MAGIC
# MAGIC | column | purpose |
# MAGIC |---|---|
# MAGIC | `pipeline` | logical pipeline identifier |
# MAGIC | `hwm` | last successfully ingested `ModifiedDate` |
# MAGIC | `updated_at` | when this row was last written (audit) |
# MAGIC
# MAGIC **Task:**
# MAGIC 1. `CREATE TABLE IF NOT EXISTS META_TABLE (pipeline STRING, hwm TIMESTAMP, updated_at TIMESTAMP) USING DELTA`
# MAGIC 2. Insert a seed row for `PIPELINE_NAME` with HWM `1900-01-01`, but
# MAGIC    only if no row for this pipeline exists yet (`WHERE NOT EXISTS`).

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: create metadata table and seed the pipeline row")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Read the current HWM

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: read hwm for PIPELINE_NAME from META_TABLE")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Pull the delta from SQL Server
# MAGIC
# MAGIC Two boundaries in the JDBC query:
# MAGIC
# MAGIC - **Lower:** `WHERE ModifiedDate > '<hwm>'` (strict)
# MAGIC - **Upper:** `AND ModifiedDate <= DATEADD(MINUTE, -5, GETDATE())`
# MAGIC   (5-minute clock-skew safety)
# MAGIC
# MAGIC Wrap the SELECT in `(...) AS delta` so the JDBC connector accepts it
# MAGIC as a derived table.
# MAGIC
# MAGIC **Task:** read the delta DataFrame, cache it (we'll use it for count
# MAGIC + max + write), print the row count.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: pull and cache the delta DataFrame, print row count")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Write to Bronze and compute the new HWM
# MAGIC
# MAGIC - Add `_load_ts = current_timestamp()` audit column
# MAGIC - Write mode: `append` (Bronze is append-only)
# MAGIC - New HWM = `max(ModifiedDate)` of this delta
# MAGIC - If delta is empty, leave HWM unchanged
# MAGIC
# MAGIC Do NOT call `df.unpersist()` — Serverless rejects it.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: write to Bronze, compute new HWM")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Commit the new HWM
# MAGIC
# MAGIC Order matters. **Always update the HWM AFTER the write succeeds** —
# MAGIC otherwise a failed write loses rows on the next pass.
# MAGIC
# MAGIC **Task:** update `META_TABLE` setting `hwm = new_hwm` and
# MAGIC `updated_at = current_timestamp()` for `PIPELINE_NAME`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: commit new HWM to META_TABLE")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS bronze_rows FROM workspace.bronze.sales_customer_incremental

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Idempotency proof — re-run with no changes
# MAGIC
# MAGIC Repeat Steps 3–6. With nothing changed at the source, the delta
# MAGIC should be 0 rows and the HWM should not move.
# MAGIC
# MAGIC **Task:** read the now-committed HWM, pull a fresh delta, print the
# MAGIC delta row count. Should be 0.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7: re-run delta pull, expect 0 rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Three things that bite (markdown only)
# MAGIC
# MAGIC ### 1. Clock skew between source and pipeline
# MAGIC The 5-minute upper boundary covers source vs pipeline clock drift.
# MAGIC Never read up to "now" exactly.
# MAGIC
# MAGIC ### 2. Strict `>` boundary leaks duplicates
# MAGIC When multiple rows share `ModifiedDate`, strict `>` misses the
# MAGIC boundary row. Switching to `>=` causes duplicates instead. Robust
# MAGIC fixes:
# MAGIC - Sink-side dedupe with MERGE or `dropDuplicates(["id"])`
# MAGIC - Tuple HWM `(updated_at, id)`
# MAGIC
# MAGIC ### 3. Commit the HWM only after the sink write succeeds
# MAGIC The order in this notebook is correct (write first, then update HWM).
# MAGIC The opposite order silently loses rows.
# MAGIC
# MAGIC ## Bonus: hard deletes are invisible to HWM ingest
# MAGIC
# MAGIC `DELETE FROM source` doesn't change any `ModifiedDate` — HWM ingest
# MAGIC never sees it. Mitigate with soft deletes upstream, periodic full
# MAGIC reconciliation, or real CDC (next notebook).
