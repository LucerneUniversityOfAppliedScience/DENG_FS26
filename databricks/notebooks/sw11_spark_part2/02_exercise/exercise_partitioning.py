# Databricks notebook source

# MAGIC %md
# MAGIC # Partition-aware Processing + replaceWhere — Exercise
# MAGIC
# MAGIC In this exercise you learn how data layout on disk dictates query
# MAGIC performance: **Hive partitioning** for plain Parquet, **Liquid
# MAGIC Clustering** as the modern Delta alternative, and **`replaceWhere`**
# MAGIC for atomic idempotent partition overwrites.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Choosing the partition column is **the single most important read
# MAGIC optimization** for any data lake. Same query, same data — wrong
# MAGIC partition key, scan a TB; right partition key, scan a few MB.
# MAGIC
# MAGIC `replaceWhere` is the killer feature for incremental pipelines: it
# MAGIC lets you re-run the same job for the same date partition any number
# MAGIC of times and end up with exactly the same result.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC Redeploy the UC bundle once if you haven't yet. Click **Deploy**.

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
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Generate synthetic sales data
# MAGIC
# MAGIC 500k rows × 30 days × 3 topics. Provided so you can focus on the
# MAGIC partitioning lessons.

# COMMAND ----------

from pyspark.sql.functions import expr, col

n_rows = 500_000
days   = 30

df_sales = (spark.range(n_rows)
    .withColumn("event_date", expr(f"date_add(date'2026-04-01', cast(rand(7) * {days} as int))"))
    .withColumn("year",       expr("year(event_date)"))
    .withColumn("month",      expr("month(event_date)"))
    .withColumn("day",        expr("day(event_date)"))
    .withColumn("topic",      expr("CASE cast(rand(8) * 3 as int) WHEN 0 THEN 'crm' WHEN 1 THEN 'erp' ELSE 'iot' END"))
    .withColumn("customer_id", (expr("rand(9) * 10000") + 1).cast("int"))
    .withColumn("amount",     expr("round(rand(10) * 500, 2)"))
    .drop("id")
    .cache())

print(f"Synthetic rows: {df_sales.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Timing helper

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

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Why partition? — flat baseline
# MAGIC
# MAGIC Same query, two layouts. First the flat baseline.
# MAGIC
# MAGIC **Task:** write `df_sales` as a non-partitioned Delta table to
# MAGIC `DELTA_FLAT` (overwrite mode).

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: write df_sales as DELTA_FLAT (no partitioning)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Hive-style partitioning
# MAGIC
# MAGIC `partitionBy("topic", "year", "month", "day")` writes the data into
# MAGIC a folder hierarchy `topic=crm/year=2026/month=4/day=15/...`. Spark
# MAGIC reads `key=value` folder names as partition columns automatically.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Write `df_sales` to `HIVE_PARQUET_DIR` with `partitionBy("topic",
# MAGIC    "year", "month", "day")` in Parquet format (overwrite).
# MAGIC 2. Print the directory structure with `dbutils.fs.ls(HIVE_PARQUET_DIR)`.
# MAGIC 3. Time a filter query on the flat table:
# MAGIC    `SELECT count(*) FROM DELTA_FLAT WHERE topic = 'crm' AND event_date = date'2026-04-15'`
# MAGIC 4. Time the same logical query on the Hive layout (read parquet,
# MAGIC    filter on partition columns). Print the speedup.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: write Hive-partitioned Parquet, time flat vs partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC `EXPLAIN FORMATTED` of the partitioned query should show
# MAGIC `PartitionFilters: [topic=crm, year=2026, month=4, day=15]` —
# MAGIC the planner pruning at folder level.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2b: explain the partitioned query, identify PartitionFilters")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Choosing partition columns (markdown only)
# MAGIC
# MAGIC | Rule | What it means |
# MAGIC |---|---|
# MAGIC | **Query-driven** | Partition by what queries actually filter on (usually `date`) |
# MAGIC | **Cardinality** | Sweet spot: tens to a few thousand distinct values, never millions |
# MAGIC | **Volume per partition** | Aim for 128 MB – 1 GB per partition |
# MAGIC | **Match ingestion granularity** | Daily for daily loads; hourly only for high-volume streams |
# MAGIC
# MAGIC | Use case | Recommended |
# MAGIC |---|---|
# MAGIC | Daily batch ingestion | `year/month/day` |
# MAGIC | Streaming ingestion | `year/month/day/hour` |
# MAGIC | Multi-tenant | `tenant_id/year/month` |
# MAGIC | Slowly-changing master data | **Don't partition** |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Anti-patterns — over-partitioning
# MAGIC
# MAGIC Partitioning by a high-cardinality column creates micro-folders:
# MAGIC tons of tiny files, slow LIST calls, no compression.
# MAGIC
# MAGIC **Task:** write a small subset (50k rows) of `df_sales` to
# MAGIC `OVERPART_DIR` partitioned by `topic` and `customer_id` (10000
# MAGIC distinct customers). Then walk the directory recursively and count
# MAGIC the parquet files.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: demonstrate over-partitioning, count files written")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Liquid Clustering vs Hive partitioning
# MAGIC
# MAGIC | Aspect | Hive `partitionBy` | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | When applied | Write time, immutable | Continuously, declared at create |
# MAGIC | Re-clustering | Rewrite the table | Automatic via `OPTIMIZE` |
# MAGIC | High cardinality | Bad | Fine |
# MAGIC | Best for | Static datasets | Frequently-updated tables |
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Write `df_sales` to `DELTA_PARTITIONED` with `partitionBy("event_date")`.
# MAGIC 2. Create `DELTA_CLUSTERED` with
# MAGIC    `CLUSTER BY (event_date, customer_id)` `AS SELECT * FROM DELTA_PARTITIONED`,
# MAGIC    then `OPTIMIZE` it. Wrap in try/except — Liquid Clustering may
# MAGIC    not be available on the runtime.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: build partitioned and clustered Delta versions")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Raw / Landing partitioning is still essential (markdown)
# MAGIC
# MAGIC Liquid Clustering only applies to **Delta**. The Raw / Landing zone
# MAGIC is plain Parquet / JSON / CSV on object storage. There, classic
# MAGIC Hive partitioning is the only way to:
# MAGIC - Prune object-store LIST calls
# MAGIC - Re-run a single day without touching the rest
# MAGIC - Apply lifecycle policies by partition
# MAGIC - Make backfills atomic and cheap

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: replaceWhere — atomic idempotent partition overwrite
# MAGIC
# MAGIC `replaceWhere` overwrites only the rows matching a predicate, in a
# MAGIC single Delta transaction. Properties:
# MAGIC - **Atomic** — single Delta commit
# MAGIC - **Validating** — input data must satisfy the predicate
# MAGIC - **Idempotent** — same job, same source, same predicate → same target
# MAGIC - **Cheap** — only matching files touched
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Pick `target_date = '2026-04-15'`. Print the original row count
# MAGIC    for that day in `DELTA_PARTITIONED`.
# MAGIC 2. Build `df_corrected` = filter df_sales for that date, then
# MAGIC    `withColumn("amount", expr("amount * 1.10"))` (simulate a fix).
# MAGIC 3. Apply via `replaceWhere`:
# MAGIC    ```python
# MAGIC    (df_corrected.write.format("delta")
# MAGIC        .mode("overwrite")
# MAGIC        .option("replaceWhere", f"event_date = '{target_date}'")
# MAGIC        .saveAsTable(DELTA_PARTITIONED))
# MAGIC    ```
# MAGIC 4. Run the same write a second time. Total row count should be
# MAGIC    unchanged (true idempotency).
# MAGIC 5. `DESCRIBE HISTORY` should show two separate WRITE operations.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7: replaceWhere atomic backfill, twice, identical result")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: DESCRIBE HISTORY of DELTA_PARTITIONED, observe two atomic WRITE commits

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: replaceWhere vs MERGE INTO
# MAGIC
# MAGIC | Criterion | `replaceWhere` | `MERGE INTO` |
# MAGIC |---|---|---|
# MAGIC | Granularity | Whole time window / partition block | Individual rows |
# MAGIC | Data shape | Append-only, immutable events | Mutable state |
# MAGIC | Performance | Very fast — file swap, no join | Shuffle + join |
# MAGIC | Required predicate | On a partition column | A usable match key |
# MAGIC | Typical use case | Backfill of one day | CDC apply, SCD |
# MAGIC
# MAGIC **Heuristic:** immutable + time-windowed → `replaceWhere`.
# MAGIC Mutable + key-addressable → `MERGE`.
# MAGIC
# MAGIC **Task:** apply the same correction (one day's amount × 1.10) via
# MAGIC `MERGE INTO` instead of `replaceWhere`. Match on
# MAGIC `(event_date, customer_id)`. Compare what the two patterns
# MAGIC actually do in the explain plans.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 8: same correction via MERGE INTO, contrast with replaceWhere")
