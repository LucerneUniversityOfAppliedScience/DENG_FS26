# Databricks notebook source

# MAGIC %md
# MAGIC # HWM-driven Incremental JDBC Ingest — Solution
# MAGIC
# MAGIC In this notebook you learn the **everyday incremental ingestion
# MAGIC pattern**: pull only rows newer than the last successful run from a
# MAGIC SQL Server source, using a high-water mark (HWM) stored in a
# MAGIC metadata table.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC The full-refresh ingest (`solution_sql_ingest.py`) re-reads the entire
# MAGIC source on every run. That doesn't scale: hour 1 reads 1M rows,
# MAGIC hour 24 reads 24M rows even if only 1k rows changed. Incremental
# MAGIC ingestion bounds the per-run cost to "what changed since last time"
# MAGIC — typically a constant or near-constant volume.
# MAGIC
# MAGIC The HWM pattern is the simplest incremental pattern and the right
# MAGIC default for any source that exposes a reliable `updated_at` /
# MAGIC `ModifiedDate` column.
# MAGIC
# MAGIC ## The three incremental patterns (slide 29)
# MAGIC
# MAGIC | Pattern | Source signal | Target write | Typical use |
# MAGIC |---|---|---|---|
# MAGIC | **Append-only** | Immutable events (logs, clicks) | `INSERT` / append | Event streams, audit trails |
# MAGIC | **Upsert (MERGE)** | Mutable rows + key | `MERGE INTO` | Master data, OLTP mirrors |
# MAGIC | **CDC** | Insert / update / **delete** events | `MERGE` per change type | Reliable replicas of OLTP sources |
# MAGIC
# MAGIC This notebook covers append-style HWM. The "Three things that bite"
# MAGIC at the end addresses why the strict `WHERE updated_at > :hwm`
# MAGIC boundary silently breaks on hard deletes and what to do about it.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume **and** a
# MAGIC new `meta` schema in the UC bundle. Before running this notebook
# MAGIC **redeploy the bundle** so the `meta` schema exists. In the bundle
# MAGIC UI, click **Deploy** once.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # ⚠ SECURITY WARNING — READ BEFORE RUNNING
# MAGIC
# MAGIC ## **DO NOT USE THIS PATTERN IN PRODUCTION.**
# MAGIC
# MAGIC The cells below read the SQL Server username and password from
# MAGIC **notebook widgets**. Acceptable for **classroom demos and short live
# MAGIC exploration only**. In any real pipeline:
# MAGIC
# MAGIC - **NEVER** hard-code credentials in a notebook
# MAGIC - **NEVER** type credentials into a widget that gets saved with the notebook
# MAGIC - **NEVER** commit notebooks containing real credentials to git
# MAGIC - **NEVER** log or `print()` the password
# MAGIC
# MAGIC ### What to do in production
# MAGIC
# MAGIC Use **Databricks Secret Scopes** (backed by Azure Key Vault, AWS
# MAGIC Secrets Manager, or a Databricks-backed scope), and read with:
# MAGIC
# MAGIC ```python
# MAGIC db_user     = dbutils.secrets.get(scope="adventureworks", key="user")
# MAGIC db_password = dbutils.secrets.get(scope="adventureworks", key="password")
# MAGIC ```
# MAGIC
# MAGIC Secrets fetched this way are **redacted from notebook output and logs**
# MAGIC by Databricks.
# MAGIC
# MAGIC ### Where are the credentials for this exercise?
# MAGIC
# MAGIC The SQL Server **user** and **password** for the AdventureWorks demo
# MAGIC database are published on **ILIAS** in the module materials. Paste
# MAGIC them into the widgets below before running.

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
HWM_COLUMN    = "ModifiedDate"  # the SQL Server column we filter on

print(f"Pipeline   : {PIPELINE_NAME}")
print(f"Bronze     : {BRONZE_TABLE}")
print(f"Meta       : {META_TABLE}")
print(f"Source     : {DB_NAME}.{SOURCE_SCHEMA}.{SOURCE_TABLE} (HWM column: {HWM_COLUMN})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop Bronze + metadata so the notebook is fully re-runnable
# MAGIC
# MAGIC In real life you would NOT drop the metadata table — that would lose
# MAGIC the HWM and trigger a full re-ingest. Here we want a clean slate so
# MAGIC each notebook run demonstrates the pattern from zero.

# COMMAND ----------

for table in [BRONZE_TABLE, META_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read the credentials from widgets

# COMMAND ----------

dbutils.widgets.text("user", "", "SQL Server User")
dbutils.widgets.text("pw",   "", "SQL Server Password")

db_user     = dbutils.widgets.get("user")
db_password = dbutils.widgets.get("pw")

if not db_user or not db_password:
    raise ValueError("Set the 'user' and 'pw' widgets with the credentials from ILIAS.")

print(f"User: {db_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Initialise the metadata table
# MAGIC
# MAGIC The metadata table holds one row per pipeline with the latest
# MAGIC successfully-committed HWM. We seed it with `1900-01-01` so the first
# MAGIC run reads everything (anything newer than 1900 — i.e. all rows).
# MAGIC
# MAGIC | column | purpose |
# MAGIC |---|---|
# MAGIC | `pipeline` | logical pipeline identifier (e.g. `sales_customer`) |
# MAGIC | `hwm` | last successfully ingested `ModifiedDate` |
# MAGIC | `updated_at` | when this row was last written (audit) |

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {META_TABLE} (
        pipeline   STRING,
        hwm        TIMESTAMP,
        updated_at TIMESTAMP
    ) USING DELTA
""")

# Insert the seed row only if this pipeline isn't already tracked.
spark.sql(f"""
    INSERT INTO {META_TABLE}
    SELECT '{PIPELINE_NAME}', TIMESTAMP'1900-01-01', current_timestamp()
    WHERE NOT EXISTS (
        SELECT 1 FROM {META_TABLE} WHERE pipeline = '{PIPELINE_NAME}'
    )
""")

display(spark.table(META_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Read the current HWM

# COMMAND ----------

hwm_row = (spark.table(META_TABLE)
    .filter(f"pipeline = '{PIPELINE_NAME}'")
    .select("hwm")
    .first())

current_hwm = hwm_row["hwm"]
print(f"Current HWM for {PIPELINE_NAME}: {current_hwm}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Pull the delta from SQL Server
# MAGIC
# MAGIC The JDBC query has two boundaries:
# MAGIC
# MAGIC - **Lower (inclusive of HWM means dupes; we use strict `>`):**
# MAGIC   `WHERE ModifiedDate > '<hwm>'`
# MAGIC - **Upper (clock-skew safety, slide 42):**
# MAGIC   `AND ModifiedDate <= DATEADD(MINUTE, -5, GETDATE())`
# MAGIC
# MAGIC The upper boundary is critical: source clocks may run a few seconds
# MAGIC ahead or behind the pipeline. Reading "up to now" risks missing rows
# MAGIC that get committed at the source after we read but with a timestamp
# MAGIC slightly earlier than our cutoff. The 5-minute lag is the standard
# MAGIC safety margin.
# MAGIC
# MAGIC The query is wrapped in `(...)` so SQL Server treats it as a derived
# MAGIC table — that's the JDBC connector's `dbtable` requirement when you
# MAGIC want to push a custom query.

# COMMAND ----------

# Format the timestamp for SQL Server (ISO-style works)
hwm_str = current_hwm.strftime("%Y-%m-%d %H:%M:%S")

delta_query = f"""
    (SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
     WHERE {HWM_COLUMN} > '{hwm_str}'
       AND {HWM_COLUMN} <= DATEADD(MINUTE, -5, GETDATE())
    ) AS delta
"""

print(f"Pulling delta with query:{delta_query}")

df_delta = (spark.read
    .format("sqlserver")
    .option("host",     DB_HOST)
    .option("port",     DB_PORT)
    .option("user",     db_user)
    .option("password", db_password)
    .option("database", DB_NAME)
    .option("dbtable",  delta_query)
    .load())

# Cache so we don't re-issue the JDBC query for count + max + write
df_delta = df_delta.cache()
delta_rows = df_delta.count()
print(f"Delta: {delta_rows:,} rows newer than {current_hwm}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Write to Bronze and compute the new HWM
# MAGIC
# MAGIC We add a `_load_ts = current_timestamp()` audit column. Write mode is
# MAGIC `append` — Bronze is append-only by definition (slide 21).
# MAGIC
# MAGIC The new HWM is `max(ModifiedDate)` of this delta. If the delta is
# MAGIC empty (no new rows since last run), the HWM stays unchanged.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

if delta_rows == 0:
    print("No new rows — Bronze unchanged, HWM unchanged.")
    new_hwm = current_hwm
else:
    (df_delta
        .withColumn("_load_ts", current_timestamp())
        .write
        .mode("append")
        .saveAsTable(BRONZE_TABLE))
    new_hwm = df_delta.agg({HWM_COLUMN: "max"}).first()[0]
    print(f"Wrote {delta_rows:,} rows to {BRONZE_TABLE}")
    print(f"New HWM: {new_hwm}")

df_delta.unpersist() if False else None  # Serverless rejects unpersist — we leave the cache to platform GC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Commit the new HWM (only after the write succeeded)
# MAGIC
# MAGIC **Order matters.** If we updated the HWM first and the write then
# MAGIC failed, the next run would skip those rows forever. Updating the HWM
# MAGIC after a successful write is the only safe ordering. (At extreme
# MAGIC scale you'd wrap both in a transaction across two systems, but that
# MAGIC requires more machinery than we cover here.)

# COMMAND ----------

spark.sql(f"""
    UPDATE {META_TABLE}
       SET hwm = TIMESTAMP'{new_hwm}',
           updated_at = current_timestamp()
     WHERE pipeline = '{PIPELINE_NAME}'
""")

display(spark.table(META_TABLE).filter(f"pipeline = '{PIPELINE_NAME}'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS bronze_rows FROM workspace.bronze.sales_customer_incremental

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Re-run idempotency proof
# MAGIC
# MAGIC Re-execute Step 3 (read HWM), Step 4 (pull delta), Step 5 (write +
# MAGIC compute new HWM), Step 6 (commit). With nothing changed at the
# MAGIC source, the delta should be **0 rows** and the HWM should not move.
# MAGIC
# MAGIC This is the same idempotency property as the Auto Loader notebook,
# MAGIC just driven by an HWM in a metadata table instead of a streaming
# MAGIC checkpoint.

# COMMAND ----------

# Read the now-committed HWM
current_hwm = spark.table(META_TABLE).filter(f"pipeline = '{PIPELINE_NAME}'").first()["hwm"]
hwm_str = current_hwm.strftime("%Y-%m-%d %H:%M:%S")
print(f"Re-run from HWM: {current_hwm}")

delta_query = f"""
    (SELECT * FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
     WHERE {HWM_COLUMN} > '{hwm_str}'
       AND {HWM_COLUMN} <= DATEADD(MINUTE, -5, GETDATE())
    ) AS delta
"""

df_delta2 = (spark.read
    .format("sqlserver")
    .option("host", DB_HOST).option("port", DB_PORT)
    .option("user", db_user).option("password", db_password)
    .option("database", DB_NAME)
    .option("dbtable", delta_query)
    .load())

n2 = df_delta2.count()
print(f"Re-run delta: {n2} rows  ({'idempotent ✓' if n2 == 0 else 'unexpected new rows'})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Three things that bite (slide 42)
# MAGIC
# MAGIC ### 1. Clock skew between source and pipeline
# MAGIC
# MAGIC The source DB clock and the pipeline clock are never identical. The
# MAGIC `<= now() - INTERVAL '5 minutes'` upper boundary covers both
# MAGIC directions: rows whose timestamps are slightly future-dated by the
# MAGIC source still get picked up on the next run. **Never read up to "now"
# MAGIC exactly.**
# MAGIC
# MAGIC ### 2. Strict `>` boundary leaks duplicates
# MAGIC
# MAGIC If multiple rows share the same `ModifiedDate` value (common with
# MAGIC bulk inserts), strict `>` leaves the boundary row in place — but on
# MAGIC the next run, we filter `> hwm` again with hwm = that exact value,
# MAGIC so we miss it.
# MAGIC
# MAGIC Switching to `>=` solves the miss but causes duplicates (the
# MAGIC boundary row is read twice). Two robust workarounds:
# MAGIC - **Sink-side dedupe:** `MERGE` or `dropDuplicates(["id"])` on the
# MAGIC   primary key.
# MAGIC - **Tuple HWM:** `(updated_at, id)` — strict `>` on the tuple is
# MAGIC   safe because no two rows share the full tuple.
# MAGIC
# MAGIC ### 3. Commit the HWM only after the sink write succeeds
# MAGIC
# MAGIC The order in this notebook (write Bronze, then update HWM) means a
# MAGIC failed write leaves the HWM unchanged and the next run retries the
# MAGIC same delta. The opposite order silently loses rows. **Always commit
# MAGIC bookkeeping after the data lands.**
# MAGIC
# MAGIC ## Bonus: hard deletes are invisible to HWM ingest
# MAGIC
# MAGIC If a row is `DELETE FROM Sales.Customer WHERE id = 42` upstream, no
# MAGIC `ModifiedDate` change occurs (the row is gone). HWM ingest will
# MAGIC never see it. Mitigations:
# MAGIC - Soft deletes upstream (`is_deleted = true` instead of `DELETE`)
# MAGIC - Periodic full reconciliation (full refresh once a week)
# MAGIC - Real CDC (next notebook — Delta CDF on the source side, or DBM CDC
# MAGIC   on SQL Server)
