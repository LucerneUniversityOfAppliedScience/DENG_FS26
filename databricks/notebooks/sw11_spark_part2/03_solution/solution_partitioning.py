# Databricks notebook source

# MAGIC %md
# MAGIC # Partition-aware Processing + replaceWhere — Solution
# MAGIC
# MAGIC In this notebook you learn how data layout on disk dictates query
# MAGIC performance: **Hive partitioning** for plain Parquet, **Liquid
# MAGIC Clustering** as the modern Delta alternative, and **`replaceWhere`**
# MAGIC for atomic idempotent partition overwrites.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Choosing the partition column is **the single most important read
# MAGIC optimization** for any data lake. Same query, same data — wrong
# MAGIC partition key, scan a TB; right partition key, scan a few MB. The
# MAGIC difference is 1000× the cost.
# MAGIC
# MAGIC `replaceWhere` is the killer feature for incremental pipelines: it
# MAGIC lets you re-run the same job for the same date partition any number
# MAGIC of times and end up with exactly the same result — true partition-level
# MAGIC idempotency in a single Delta transaction.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC Redeploy the UC bundle once if you haven't yet. Click **Deploy**
# MAGIC in the bundle UI.

# COMMAND ----------

CATALOG = "workspace"

LANDING_ROOT      = f"/Volumes/{CATALOG}/landing/files/sw11_partitioning"
HIVE_PARQUET_DIR  = f"{LANDING_ROOT}/sales_hive"
OVERPART_DIR      = f"{LANDING_ROOT}/sales_overpart"

DELTA_PARTITIONED = f"{CATALOG}.silver.sales_partitioned"
DELTA_CLUSTERED   = f"{CATALOG}.silver.sales_clustered"
DELTA_FLAT        = f"{CATALOG}.silver.sales_flat"

print(f"Hive parquet : {HIVE_PARQUET_DIR}")
print(f"Delta partitioned : {DELTA_PARTITIONED}")
print(f"Delta clustered   : {DELTA_CLUSTERED}")
print(f"Delta flat        : {DELTA_FLAT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop tables and wipe landing folders

# COMMAND ----------

for table in [DELTA_PARTITIONED, DELTA_CLUSTERED, DELTA_FLAT]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

for path in [HIVE_PARQUET_DIR, OVERPART_DIR]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Cleared: {path}")
    except Exception:
        print(f"Skipped (not present): {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Generate synthetic sales data
# MAGIC
# MAGIC 500k rows spanning 30 days × 3 source topics. Big enough to make
# MAGIC partition pruning measurable, small enough to finish quickly on
# MAGIC Free Edition Serverless.

# COMMAND ----------

from pyspark.sql.functions import expr, col

n_rows = 500_000
days   = 30  # April 2026

df_sales = (spark.range(n_rows)
    .withColumn("event_date", expr(f"date_add(date'2026-04-01', cast(rand(7) * {days} as int))"))
    .withColumn("year",       expr("year(event_date)"))
    .withColumn("month",      expr("month(event_date)"))
    .withColumn("day",        expr("day(event_date)"))
    .withColumn("topic",      expr("CASE cast(rand(8) * 3 as int) WHEN 0 THEN 'crm' WHEN 1 THEN 'erp' ELSE 'iot' END"))
    .withColumn("customer_id", (expr("rand(9) * 10000") + 1).cast("int"))
    .withColumn("amount",     expr("round(rand(10) * 500, 2)"))
    .drop("id"))
# Note: we deliberately do NOT call .cache() — Serverless rejects PERSIST
# TABLE. Each downstream step will re-evaluate df_sales, which is fine
# for a 500k-row generator at this scale.

print(f"Synthetic rows: {df_sales.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Why partition?
# MAGIC
# MAGIC Same query, two layouts. Flat: every file scanned, filter applied in
# MAGIC memory. Hive-partitioned: only the matching folder is read, the rest
# MAGIC is pruned at planning time.
# MAGIC
# MAGIC We measure both with `time_query`.

# COMMAND ----------

import time

def time_query(label, query, warmup=False):
    if isinstance(query, str):
        run = lambda: spark.sql(query).count()
    else:
        run = lambda: query.count()
    if warmup:
        run()
    t0 = time.perf_counter()
    n = run()
    elapsed = time.perf_counter() - t0
    print(f"{label:55s} {elapsed:6.2f}s   (rows: {n:,})")
    return elapsed

# COMMAND ----------

# Flat Delta table — no partitioning at all.
df_sales.write.mode("overwrite").saveAsTable(DELTA_FLAT)
print(f"{DELTA_FLAT}: {spark.table(DELTA_FLAT).count():,} rows (flat)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Hive-style partitioning
# MAGIC
# MAGIC `partitionBy("topic", "year", "month", "day")` writes the data into a
# MAGIC folder hierarchy:
# MAGIC ```
# MAGIC sales_hive/
# MAGIC ├── topic=crm/year=2026/month=4/day=1/part-...parquet
# MAGIC ├── topic=crm/year=2026/month=4/day=2/...
# MAGIC └── ...
# MAGIC ```
# MAGIC Spark reads the `key=value` folder names as partition columns
# MAGIC automatically. A query that filters on `topic` and `event_date`
# MAGIC opens **only the matching folders**.

# COMMAND ----------

(df_sales
    .write
    .mode("overwrite")
    .partitionBy("topic", "year", "month", "day")
    .parquet(HIVE_PARQUET_DIR))

print(f"Wrote partitioned Parquet to {HIVE_PARQUET_DIR}")

# Show the directory structure
display(dbutils.fs.ls(HIVE_PARQUET_DIR))

# COMMAND ----------

# Compare: filter for crm on a single day.
filter_sql_flat = f"SELECT count(*) FROM {DELTA_FLAT} WHERE topic = 'crm' AND event_date = date'2026-04-15'"

# For the partitioned read, use a temp view over the parquet folder.
df_hive = spark.read.parquet(HIVE_PARQUET_DIR)
df_hive.createOrReplaceTempView("sales_hive_view")
filter_sql_hive = "SELECT count(*) FROM sales_hive_view WHERE topic = 'crm' AND year = 2026 AND month = 4 AND day = 15"

print("--- Filter: topic=crm, event_date=2026-04-15 ---")
t_flat = time_query("Flat Delta (full scan, in-memory filter)", filter_sql_flat, warmup=True)
t_hive = time_query("Hive-partitioned Parquet (one folder)",     filter_sql_hive, warmup=True)
print(f"\nSpeedup: {t_flat / max(t_hive, 0.01):.1f}× faster with Hive partitioning.")

# COMMAND ----------

# MAGIC %md
# MAGIC `EXPLAIN` of the partitioned query shows `PartitionFilters`. That's
# MAGIC the planner recognising the predicate matches partition columns and
# MAGIC pruning at folder level.

# COMMAND ----------

spark.sql(filter_sql_hive).explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Choosing partition columns
# MAGIC
# MAGIC | Rule | What it means |
# MAGIC |---|---|
# MAGIC | **Query-driven** | Partition by what your queries actually filter on (usually `date`) |
# MAGIC | **Cardinality** | Sweet spot is **tens to a few thousand** distinct values, never millions |
# MAGIC | **Volume per partition** | Aim for **128 MB – 1 GB** per partition (Parquet/Delta sweet spot) |
# MAGIC | **Match ingestion granularity** | Daily partitions for daily loads; hourly only for high-volume streams |
# MAGIC
# MAGIC | Use case | Recommended |
# MAGIC |---|---|
# MAGIC | Daily batch ingestion (~1 GB/day) | `year/month/day` |
# MAGIC | Streaming ingestion (~100 GB/hour) | `year/month/day/hour` |
# MAGIC | Multi-tenant (10 customers) | `tenant_id/year/month` |
# MAGIC | Slowly-changing master data | **Don't partition at all** |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Anti-patterns
# MAGIC
# MAGIC ### Over-partitioning — small-files problem
# MAGIC Partitioning down to the minute creates 1440 partitions per day.
# MAGIC With 100 KB per file, metadata overhead eats performance: directory
# MAGIC listing alone takes minutes.

# COMMAND ----------

# Demonstrate over-partitioning: small-files problem.
# Partition by topic + customer_id (10000 distinct customers!) — far too granular.
print("Writing over-partitioned table (this is intentionally bad)...")
(df_sales.limit(50_000)  # smaller subset, this is for demonstration of the explosion
    .write.mode("overwrite")
    .partitionBy("topic", "customer_id")
    .parquet(OVERPART_DIR))

# Count files written
all_files = []
def _list_recursive(path):
    for f in dbutils.fs.ls(path):
        if f.path.endswith(".parquet"):
            all_files.append(f.path)
        elif f.isDir():
            _list_recursive(f.path)

_list_recursive(OVERPART_DIR)
print(f"Files written: {len(all_files):,}  (50k rows split across this many tiny files)")
print("In production this means: slow LIST calls, slow planning, no compression efficiency.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Under-partitioning
# MAGIC One giant table (Step 1's `DELTA_FLAT`) — every query reads
# MAGIC everything. Already demonstrated above: 500k rows, full scan, no
# MAGIC pruning.
# MAGIC
# MAGIC ### High-cardinality keys
# MAGIC `partitionBy("user_id")` with 10M users → 10M micro-folders. Object
# MAGIC store LIST API rate-limits before the query even starts.
# MAGIC
# MAGIC ### Inconsistent file names
# MAGIC `data.parquet`, `Data.parquet`, `DATA_FINAL_v2.parquet` — no safe
# MAGIC re-runs, traceability lost. Convention: `<topic>_<isoTimestamp>_run-<id>.parquet`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Liquid Clustering vs Hive partitioning
# MAGIC
# MAGIC | Aspect | Hive `partitionBy` | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | When applied | Write time, immutable layout | Continuously, declared at create |
# MAGIC | Re-clustering | Rewrite the table | Automatic via `OPTIMIZE` |
# MAGIC | Adding columns | Rewrite all data | `ALTER TABLE ... CLUSTER BY` |
# MAGIC | High cardinality | Bad (small-files) | Fine (`customer_id`, `device_id`) |
# MAGIC | Skew resistance | Poor | Self-tuning |
# MAGIC | Best for | Static datasets, batch ETL | Frequently-updated tables |
# MAGIC
# MAGIC Databricks recommendation: **Liquid Clustering for all new Delta
# MAGIC tables**, except (a) tables that need `replaceWhere` on a partition
# MAGIC column for atomic backfills, (b) tables read by engines without
# MAGIC Liquid Clustering support, (c) very large dimension columns where
# MAGIC `DROP PARTITION` retention is needed.

# COMMAND ----------

# Build a Delta version with classic partitionBy
(df_sales
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .saveAsTable(DELTA_PARTITIONED))
print(f"{DELTA_PARTITIONED}: {spark.table(DELTA_PARTITIONED).count():,} rows (partitioned by event_date)")

# COMMAND ----------

# Build a Liquid-Clustered Delta version of the same data
try:
    spark.sql(f"DROP TABLE IF EXISTS {DELTA_CLUSTERED}")
    spark.sql(f"""
        CREATE TABLE {DELTA_CLUSTERED}
        CLUSTER BY (event_date, customer_id)
        AS SELECT * FROM {DELTA_PARTITIONED}
    """)
    spark.sql(f"OPTIMIZE {DELTA_CLUSTERED}")
    print(f"{DELTA_CLUSTERED}: {spark.table(DELTA_CLUSTERED).count():,} rows (clustered)")
except Exception as e:
    print(f"Liquid Clustering not available on this runtime: {type(e).__name__}: {e}")
    print("The lessons below using DELTA_PARTITIONED still work.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Raw / Landing partitioning is still essential
# MAGIC
# MAGIC Liquid Clustering only applies to **Delta** tables. The
# MAGIC Raw / Landing zone is plain Parquet / JSON / CSV on object storage —
# MAGIC no Delta features apply. There, classic Hive partitioning is the
# MAGIC only way to:
# MAGIC
# MAGIC - Prune object-store LIST calls (slow and metered)
# MAGIC - Re-run a single day without touching the rest
# MAGIC - Apply lifecycle policies (archive / delete by partition folder)
# MAGIC - Make backfills atomic and cheap (only the matching partitions are
# MAGIC   touched)
# MAGIC
# MAGIC The Hive layout written in Step 2 is the right starting point for
# MAGIC any Bronze pipeline that reads from Parquet on object storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: replaceWhere — atomic idempotent partition overwrite
# MAGIC
# MAGIC `replaceWhere` lets you overwrite **only the rows matching a
# MAGIC predicate**, in a single Delta transaction. The classic use case:
# MAGIC re-running a daily job for one specific day without touching the
# MAGIC other days.
# MAGIC
# MAGIC Properties:
# MAGIC - **Atomic** — single Delta commit, no partial state
# MAGIC - **Validating** — input data must satisfy the predicate
# MAGIC - **Idempotent** — same job, same source, same predicate → same target
# MAGIC - **Cheap** — only the matching files are touched

# COMMAND ----------

# Snapshot today's state for one specific day
target_date = "2026-04-15"
print(f"Original count for event_date = '{target_date}':")
display(spark.sql(f"SELECT count(*) FROM {DELTA_PARTITIONED} WHERE event_date = '{target_date}'"))

# COMMAND ----------

# Build a new "corrected" version of one day's data
df_corrected = (df_sales
    .filter(f"event_date = '{target_date}'")
    .withColumn("amount", expr("amount * 1.10")))  # imagine: a tax fix recomputed all amounts

print(f"Corrected day rows: {df_corrected.count():,}")

# Apply via replaceWhere — only touches the one day
(df_corrected.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"event_date = '{target_date}'")
    .saveAsTable(DELTA_PARTITIONED))

print(f"After replaceWhere — total rows: {spark.table(DELTA_PARTITIONED).count():,}")

# COMMAND ----------

# Run it again with the same source — true partition-level idempotency.
(df_corrected.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"event_date = '{target_date}'")
    .saveAsTable(DELTA_PARTITIONED))

print(f"After second replaceWhere — total rows: {spark.table(DELTA_PARTITIONED).count():,}  (unchanged)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY shows two separate WRITE operations, both atomic
# MAGIC DESCRIBE HISTORY workspace.silver.sales_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: replaceWhere vs MERGE INTO
# MAGIC
# MAGIC Both are atomic, both run in a single Delta transaction. They solve
# MAGIC **different problems**.
# MAGIC
# MAGIC | Criterion | `replaceWhere` | `MERGE INTO` |
# MAGIC |---|---|---|
# MAGIC | Granularity | A whole time window / partition block | Individual rows |
# MAGIC | Data shape | Append-only, immutable events | Mutable state (customers, orders) |
# MAGIC | Performance | Very fast — no join, just file swap | Shuffle + join, more expensive |
# MAGIC | Required predicate | On a **partition column** for pruning | A usable **match key** |
# MAGIC | What "idempotent" means | Re-running rewrites the same window identically | Re-running converges row-by-row to the source state |
# MAGIC | Typical use case | Backfill of one day, re-run of windowed aggregation, late-arriving partition data | CDC apply, slowly changing dimension, dedup refresh |
# MAGIC
# MAGIC **Heuristic from the slide:** immutable + time-windowed → `replaceWhere`.
# MAGIC Mutable + key-addressable → `MERGE`. Both can coexist in the same
# MAGIC pipeline at different layers.

# COMMAND ----------

# Code sketch: same target table, two patterns side by side.

# replaceWhere — backfill one day from corrected source
(df_corrected.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"event_date = '{target_date}'")
    .saveAsTable(DELTA_PARTITIONED))
print("replaceWhere applied: one window swapped atomically.")

# COMMAND ----------

# MERGE — apply individual row-level CDC events
df_corrected.createOrReplaceTempView("daily_changes")

spark.sql(f"""
    MERGE INTO {DELTA_PARTITIONED} t
    USING daily_changes s
       ON t.event_date = s.event_date
      AND t.customer_id = s.customer_id
    WHEN MATCHED AND s.amount <> t.amount THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *
""")
print("MERGE applied: row-level UPDATE + INSERT.")

# COMMAND ----------

# MAGIC %md
# MAGIC The two patterns produce the same end state for this dataset (same
# MAGIC source, same predicate range). At scale, `replaceWhere` is much
# MAGIC cheaper because it just swaps file pointers, while MERGE has to
# MAGIC join, compare every column, and rewrite every changed row.
# MAGIC
# MAGIC ## When you've used both, you'll feel the rule
# MAGIC
# MAGIC - "I just want yesterday's slice to look exactly like the source for
# MAGIC   yesterday" → `replaceWhere`
# MAGIC - "I want each customer record to converge to the latest state from
# MAGIC   a CDC stream" → `MERGE`
