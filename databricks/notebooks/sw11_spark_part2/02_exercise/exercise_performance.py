# Databricks notebook source

# MAGIC %md
# MAGIC # Performance and Optimization — Exercise
# MAGIC
# MAGIC In this exercise you learn how to control Spark's **join strategy**,
# MAGIC choose between **caching strategies**, and improve **read performance**
# MAGIC with Z-Order or Liquid Clustering — by **measuring the time impact** of
# MAGIC each technique.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Most Spark code that "works" can be made 5–50× faster by tuning a handful
# MAGIC of decisions: which join algorithm, when to broadcast a small side, when
# MAGIC to cache, and how the data is laid out on disk. Reading a physical plan
# MAGIC (`.explain()`) tells you **why** something is slow; running with
# MAGIC `time.perf_counter()` tells you **how slow** in seconds. We do both —
# MAGIC measurement first, then the plan to explain the measurement.
# MAGIC
# MAGIC ## Free Edition note
# MAGIC
# MAGIC On Databricks Free Edition Serverless, several Spark configs are
# MAGIC **read-only** (`spark.sql.autoBroadcastJoinThreshold`,
# MAGIC `spark.sql.adaptive.enabled`) and `unpersist()` is rejected. The
# MAGIC notebook works around all of these.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC `workspace.nyc_taxi.trips_2025` joined with `workspace.nyc_taxi.vendor_list`.
# MAGIC The seed cell materialises the trips table from real parquet files if
# MAGIC available, else generates 2M rows of synthetic NYC-shaped data.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC Redeploy the UC bundle once if you haven't yet — sw11 added a new
# MAGIC `landing/files` volume. Click **Deploy** in the bundle UI.

# COMMAND ----------

TRIPS_TABLE      = "workspace.nyc_taxi.trips_2025"
VENDOR_TABLE     = "workspace.nyc_taxi.vendor_list"
GOLD_ZORDER      = "workspace.gold.taxi_trips_zordered"
GOLD_CLUSTERED   = "workspace.gold.taxi_trips_clustered"

NYC_PARQUET_DIR  = "/Volumes/workspace/nyc_taxi/raw_files"

print(f"Trips    : {TRIPS_TABLE}")
print(f"Vendors  : {VENDOR_TABLE}")
print(f"Z-order  : {GOLD_ZORDER}")
print(f"Clustered: {GOLD_CLUSTERED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Seed `trips_2025` (one-time)
# MAGIC
# MAGIC Three-tier fallback: existing table → real parquet files → synthetic.
# MAGIC No action required, the cell handles itself.

# COMMAND ----------

from pyspark.sql.functions import expr

def _generate_synthetic_trips(n_rows: int):
    seconds_in_year = 365 * 24 * 3600
    return (spark.range(n_rows)
        .withColumn("VendorID",        (expr("rand(42) * 7") + 1).cast("int"))
        .withColumn("PULocationID",    (expr("rand(43) * 263") + 1).cast("int"))
        .withColumn("DOLocationID",    (expr("rand(44) * 263") + 1).cast("int"))
        .withColumn("passenger_count", (expr("rand(45) * 4") + 1).cast("int"))
        .withColumn("trip_distance",
            expr("CASE WHEN rand(46) < 0.99 THEN round(rand(47) * 30, 2) "
                 "ELSE round(rand(48) * 80 + 100, 2) END"))
        .withColumn("fare_amount",     expr("round(2.5 + trip_distance * 2.5 + rand(49) * 5, 2)"))
        .withColumn("tip_amount",      expr("round(rand(50) * fare_amount * 0.25, 2)"))
        .withColumn("total_amount",    expr("round(fare_amount + tip_amount + 3.5, 2)"))
        .withColumn("payment_type",    expr("cast(rand(51) * 6 as int)"))
        .withColumn("RatecodeID",
            expr("CASE WHEN rand(52) < 0.95 THEN 1 "
                 "WHEN rand(53) < 0.5 THEN 2 ELSE 99 END"))
        .withColumn("tpep_pickup_datetime",
            expr(f"timestamp_seconds(unix_timestamp(timestamp '2025-01-01 00:00:00') "
                 f"+ cast(rand(54) * {seconds_in_year} as bigint))"))
        .withColumn("tpep_dropoff_datetime",
            expr("timestamp_seconds(unix_timestamp(tpep_pickup_datetime) "
                 "+ cast(trip_distance * 240 as bigint))"))
        .drop("id"))

if spark.catalog.tableExists(TRIPS_TABLE):
    n = spark.table(TRIPS_TABLE).count()
    print(f"{TRIPS_TABLE} already exists ({n:,} rows). Skipping seed.")
else:
    print(f"{TRIPS_TABLE} not found — seeding.")
    parquet_files = []
    try:
        parquet_files = sorted(
            f.path for f in dbutils.fs.ls(NYC_PARQUET_DIR)
            if f.name.endswith(".parquet")
        )
    except Exception:
        pass
    if parquet_files:
        print(f"Loading {len(parquet_files)} real parquet file(s) from {NYC_PARQUET_DIR}.")
        (spark.read.parquet(*parquet_files)
            .write.mode("overwrite")
            .saveAsTable(TRIPS_TABLE))
    else:
        print(f"No parquet files in {NYC_PARQUET_DIR} — generating 2,000,000 synthetic rows.")
        (_generate_synthetic_trips(2_000_000)
            .write.mode("overwrite")
            .saveAsTable(TRIPS_TABLE))
    print(f"Seeded {spark.table(TRIPS_TABLE).count():,} rows into {TRIPS_TABLE}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop existing gold tables

# COMMAND ----------

for table in [GOLD_ZORDER, GOLD_CLUSTERED]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Timing helper
# MAGIC
# MAGIC We measure every variant with the same helper. `.count()` is a cheap
# MAGIC action that forces the **entire DAG** to execute (including any join,
# MAGIC group-by, or shuffle), and returns a verifiable scalar.
# MAGIC
# MAGIC The optional `warmup` first runs the query once and discards the
# MAGIC result. Cold-cache effects can otherwise dominate the first
# MAGIC measurement and hide the real difference.

# COMMAND ----------

import time

def time_query(label, query, warmup=False):
    """Time a Spark action. `query` may be a SQL string or a DataFrame."""
    if isinstance(query, str):
        run = lambda: spark.sql(query).count()
    else:
        run = lambda: query.count()
    if warmup:
        run()
    t0 = time.perf_counter()
    n = run()
    elapsed = time.perf_counter() - t0
    print(f"{label:50s} {elapsed:6.2f}s   (rows: {n:,})")
    return elapsed

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Join strategies — measure first, explain second
# MAGIC
# MAGIC Spark picks one of three join algorithms:
# MAGIC
# MAGIC | Operator | When chosen | Cost |
# MAGIC |---|---|---|
# MAGIC | `BroadcastHashJoin` | One side fits in `autoBroadcastJoinThreshold` | Cheapest — no shuffle |
# MAGIC | `ShuffledHashJoin` | Small side hashable, large side too big to broadcast | One shuffle |
# MAGIC | `SortMergeJoin` | Both sides are large | Two shuffles + sorts |
# MAGIC
# MAGIC On Free Edition Serverless we can't toggle
# MAGIC `spark.sql.autoBroadcastJoinThreshold`. Instead we use **SQL join
# MAGIC hints**:
# MAGIC
# MAGIC - `/*+ MERGE(table) */` — force `SortMergeJoin`
# MAGIC - `/*+ BROADCAST(table) */` — force `BroadcastHashJoin`
# MAGIC
# MAGIC **Task A:** build three SQL queries that compute the same revenue
# MAGIC aggregation:
# MAGIC ```sql
# MAGIC SELECT v.VendorName, sum(t.fare_amount) AS revenue
# MAGIC FROM trips t INNER JOIN vendor_list v ON t.VendorID = v.VendorID
# MAGIC GROUP BY v.VendorName
# MAGIC ```
# MAGIC One default, one with `/*+ MERGE(v) */`, one with `/*+ BROADCAST(v) */`.
# MAGIC
# MAGIC **Task B:** time all three with `time_query(label, sql, warmup=True)`.
# MAGIC Print a summary line comparing SortMergeJoin vs BroadcastHashJoin.
# MAGIC
# MAGIC **Task C:** print `.explain()` of the SortMergeJoin variant. Identify
# MAGIC the two `Exchange hashpartitioning(VendorID, ...)` shuffles in the
# MAGIC plan.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: time three join strategies, compare, explain the slow one")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bonus: `F.broadcast(...)` in Python
# MAGIC
# MAGIC The DataFrame API equivalent of `/*+ BROADCAST */` is
# MAGIC `F.broadcast(df_small)`. Same plan, same time.
# MAGIC
# MAGIC **Task D:** rewrite the broadcast variant using `F.broadcast(df_vendors)`,
# MAGIC time it, confirm the time matches the SQL version.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1 bonus: F.broadcast() Python equivalent")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE re-plans the query at runtime. AQE is on by default and not
# MAGIC user-toggleable on Free Edition Serverless, so we can't compare AQE on
# MAGIC vs off — we just observe it in the plan.
# MAGIC
# MAGIC **Task:** define a query with a very selective filter (e.g.
# MAGIC `trip_distance > 100`), aggregate by `VendorID`, time it, and
# MAGIC `.explain()`. Look for `AdaptiveSparkPlan` near the top of the plan.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: time the AQE-wrapped query and inspect the plan")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Caching — `cache()` vs no cache
# MAGIC
# MAGIC | Method | Storage level |
# MAGIC |---|---|
# MAGIC | `df.cache()` | `MEMORY_AND_DISK` (default) |
# MAGIC | `df.persist(level)` | Whatever level you specify |
# MAGIC
# MAGIC ### Free Edition Serverless note
# MAGIC
# MAGIC Serverless rejects **both** `df.cache()` (`PERSIST TABLE`) and
# MAGIC `df.unpersist()` (`UNPERSIST TABLE`) at runtime. Wrap the `.cache()`
# MAGIC call in `try/except` so the cell skips gracefully on Serverless and
# MAGIC runs normally on classic compute.
# MAGIC
# MAGIC **Task:** measure the same aggregation
# MAGIC (`fare_amount > 0 AND trip_distance > 0`, group by `VendorID`,
# MAGIC avg fare/distance) twice without caching, then attempt `.cache()`
# MAGIC inside try/except. On classic compute, measure twice more after
# MAGIC caching and compare run-2-uncached vs run-2-cached. On Serverless,
# MAGIC the except branch prints the restriction.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: time without and with cache")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Z-Order — data skipping at scan time
# MAGIC
# MAGIC `OPTIMIZE table ZORDER BY (col)` rewrites the underlying parquet files
# MAGIC so values of `col` are co-located. Subsequent queries that filter on
# MAGIC `col` skip more files at scan time.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Materialise a copy of `trips_2025` into `GOLD_ZORDER` with **32
# MAGIC    files** (use `df.repartition(32)` before write so OPTIMIZE has
# MAGIC    something to consolidate). Select only the columns
# MAGIC    `VendorID, tpep_pickup_datetime, PULocationID, DOLocationID,
# MAGIC    fare_amount, trip_distance`.
# MAGIC 2. Time a filter query `WHERE PULocationID = 132` (warmup=True).
# MAGIC 3. Run `OPTIMIZE GOLD_ZORDER ZORDER BY (PULocationID)`.
# MAGIC 4. Time the same filter query again. Print the speedup.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: time PULocationID filter before and after Z-order")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: DESCRIBE DETAIL of GOLD_ZORDER to see numFiles after OPTIMIZE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Liquid Clustering
# MAGIC
# MAGIC | Aspect | Z-Order | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | When applied | After load, via `OPTIMIZE ... ZORDER BY` | Continuously, declared at table creation |
# MAGIC | Re-clustering | Manual `OPTIMIZE` again | Automatic |
# MAGIC | Adding columns | Rewrite all data | Just `ALTER TABLE` |
# MAGIC | Best for | Static datasets, batch ETL | Frequently-updated tables |
# MAGIC
# MAGIC **Task:** create `GOLD_CLUSTERED` with `CLUSTER BY (PULocationID)` at
# MAGIC creation, populated `AS SELECT * FROM GOLD_ZORDER`. Run `OPTIMIZE`
# MAGIC (no `ZORDER BY` clause needed on a clustered table). Time the same
# MAGIC `WHERE PULocationID = 132` query. Compare to the Z-order time from
# MAGIC Step 4. Wrap in try/except — Liquid Clustering may not be available.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: Liquid Clustering create + time same query")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Repartition vs Coalesce
# MAGIC
# MAGIC | Operation | What it does | Shuffle? | When to use |
# MAGIC |---|---|---|---|
# MAGIC | `df.repartition(N)` | New partitioning, even sizes | Yes | Increase parallelism, balance skew |
# MAGIC | `df.repartition(N, col)` | New partitioning by hash of `col` | Yes | Co-locate by key for upcoming join |
# MAGIC | `df.coalesce(N)` | Merge existing partitions | No (only when N < current) | Reduce small files before write |
# MAGIC
# MAGIC **Task:** print initial `df_trips.rdd.getNumPartitions()`, then after
# MAGIC `coalesce(8)` and after `repartition(32, "VendorID")`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: print partition counts after coalesce and repartition")
