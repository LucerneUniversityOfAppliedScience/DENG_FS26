# Databricks notebook source

# MAGIC %md
# MAGIC # Performance and Optimization — Solution
# MAGIC
# MAGIC In this notebook you learn how to control Spark's **join strategy**,
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
# MAGIC measurement first so you see the impact, then the plan to explain it.
# MAGIC
# MAGIC ## Free Edition note
# MAGIC
# MAGIC On Databricks Free Edition Serverless, several Spark configs are
# MAGIC **read-only** (e.g. `spark.sql.autoBroadcastJoinThreshold`,
# MAGIC `spark.sql.adaptive.enabled`) and the `unpersist()` API is rejected.
# MAGIC The notebook works around all of these and notes them where they bite.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC `workspace.nyc_taxi.trips_2025` joined with the small `vendor_list`
# MAGIC lookup. The seed cell below materialises the trips table from real
# MAGIC parquet files if uploaded, otherwise generates 2M rows of synthetic
# MAGIC NYC-shaped data — sufficient for every measurement in this notebook.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume in the UC
# MAGIC bundle. If you cloned the repo or pulled new changes, you must
# MAGIC **redeploy the bundle** before running any sw11 notebook — even ones
# MAGIC that don't need the new volume — so your workspace's bundle state
# MAGIC matches the repo. In the bundle UI, click **Deploy** once.

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
# MAGIC The cell below materialises the trips Delta table **only if it does not
# MAGIC exist yet**, with a three-tier fallback:
# MAGIC
# MAGIC 1. Table already exists → skip
# MAGIC 2. Real NYC parquet files in `/Volumes/workspace/nyc_taxi/raw_files/`
# MAGIC    (manually uploaded — CloudFront is firewall-blocked on Free Edition)
# MAGIC    → use them
# MAGIC 3. Otherwise → generate 2,000,000 rows of **synthetic** NYC-shaped data

# COMMAND ----------

from pyspark.sql.functions import expr

def _generate_synthetic_trips(n_rows: int):
    """2M rows by default — adjust if you want bigger plans / more dramatic effects."""
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
# MAGIC
# MAGIC Drop the two gold copies before re-running. We do NOT drop `trips_2025`
# MAGIC itself — that's our source data.

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
# MAGIC result. Cold-cache effects (executor JIT warm-up, Delta metadata
# MAGIC fetch) can otherwise dominate the first measurement and hide the real
# MAGIC difference.

# COMMAND ----------

import time

def time_query(label, query, warmup=False):
    """Time a Spark action. `query` may be a SQL string or a DataFrame."""
    if isinstance(query, str):
        run = lambda: spark.sql(query).count()
    else:
        run = lambda: query.count()
    if warmup:
        run()  # discard result
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
# MAGIC | `BroadcastHashJoin` | One side fits in `autoBroadcastJoinThreshold` (10 MB by default) | Cheapest — no shuffle |
# MAGIC | `ShuffledHashJoin` | Small side hashable, large side too big to broadcast | One shuffle |
# MAGIC | `SortMergeJoin` | Both sides are large | Two shuffles + sorts |
# MAGIC
# MAGIC On Free Edition Serverless we can't toggle
# MAGIC `spark.sql.autoBroadcastJoinThreshold` (the config is read-only —
# MAGIC `spark.conf.set` raises `[CONFIG_NOT_AVAILABLE]`). Instead we use
# MAGIC **SQL join hints**, which are per-query and always allowed:
# MAGIC
# MAGIC - `/*+ MERGE(table) */` — force `SortMergeJoin`
# MAGIC - `/*+ BROADCAST(table) */` — force `BroadcastHashJoin`
# MAGIC - `/*+ SHUFFLE_HASH(table) */` — force `ShuffledHashJoin`
# MAGIC
# MAGIC We run the same join three ways, **time** each, then look at the plan
# MAGIC of the slow one to see why.

# COMMAND ----------

# Aggregation forces the join + group-by to run end-to-end (an alias-only
# count would be optimised away). The result is a small group-by per vendor.
sql_default = f"""
    SELECT v.VendorName, sum(t.fare_amount) AS revenue
    FROM {TRIPS_TABLE} t
    INNER JOIN {VENDOR_TABLE} v ON t.VendorID = v.VendorID
    GROUP BY v.VendorName
"""
sql_merge = f"""
    SELECT /*+ MERGE(v) */ v.VendorName, sum(t.fare_amount) AS revenue
    FROM {TRIPS_TABLE} t
    INNER JOIN {VENDOR_TABLE} v ON t.VendorID = v.VendorID
    GROUP BY v.VendorName
"""
sql_bcast = sql_merge.replace("/*+ MERGE(v) */", "/*+ BROADCAST(v) */")

print("--- Timing (warmup pass first to dodge cold-cache noise) ---")
t_default = time_query("Default join (Spark decides)",     sql_default, warmup=True)
t_merge   = time_query("/*+ MERGE */ -> SortMergeJoin",    sql_merge,   warmup=True)
t_bcast   = time_query("/*+ BROADCAST */ -> BroadcastHashJoin", sql_bcast, warmup=True)

print(f"\nSortMergeJoin took {t_merge / max(t_bcast, 0.01):.1f}× as long as BroadcastHashJoin.")

# COMMAND ----------

# MAGIC %md
# MAGIC The default and the explicit broadcast match (Spark chose broadcast
# MAGIC automatically because `vendor_list` is tiny — 7 rows). The
# MAGIC SortMergeJoin variant is meaningfully slower. To see *why*, look at
# MAGIC the physical plan of the slow one:

# COMMAND ----------

print("=== /*+ MERGE */ plan (the slow one) ===")
spark.sql(sql_merge).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC The two `Exchange hashpartitioning(VendorID, ...)` nodes are the
# MAGIC shuffles SortMergeJoin requires — both sides have to be redistributed
# MAGIC by `VendorID` so matching rows end up on the same executor. That
# MAGIC redistribution is the cost difference you measured above.
# MAGIC
# MAGIC `BroadcastHashJoin` skips the shuffle entirely: the small side is
# MAGIC copied to every executor, and the large side stays put.
# MAGIC
# MAGIC ### Bonus: `F.broadcast(...)` in Python
# MAGIC
# MAGIC The DataFrame API equivalent of `/*+ BROADCAST */` is
# MAGIC `F.broadcast(df_small)`. Same plan, same time. Use whichever fits
# MAGIC your codebase.

# COMMAND ----------

from pyspark.sql.functions import broadcast

df_trips   = spark.table(TRIPS_TABLE)
df_vendors = spark.table(VENDOR_TABLE)

df_bcast_python = (df_trips
    .join(broadcast(df_vendors), "VendorID", "inner")
    .groupBy(df_vendors.VendorName)
    .sum("fare_amount"))

t_python = time_query("F.broadcast() (Python API)", df_bcast_python, warmup=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE re-plans the query at runtime based on actual statistics. Headline
# MAGIC features:
# MAGIC
# MAGIC - **Coalesce shuffle partitions** — combines tiny post-shuffle partitions
# MAGIC - **Switch join strategy** — converts SortMergeJoin to BroadcastHashJoin
# MAGIC   when a runtime statistic shows one side is small after filtering
# MAGIC - **Skew join handling** — splits one giant partition across multiple tasks
# MAGIC
# MAGIC AQE is **on by default and not user-toggleable on Free Edition
# MAGIC Serverless** (`spark.conf.set("spark.sql.adaptive.enabled", ...)`
# MAGIC raises `[CONFIG_NOT_AVAILABLE]`), so we can't do a clean before/after
# MAGIC timing comparison locally. What we can do: run a query, time it, and
# MAGIC verify in the plan that AQE is wrapping the operators. The actual plan
# MAGIC that runs may be different from the printed plan because AQE
# MAGIC re-optimises after the first shuffle completes.

# COMMAND ----------

aqe_query = (df_trips
    .filter("trip_distance > 100")        # very selective: 1% of rows
    .groupBy("VendorID")
    .count())

t_aqe = time_query("Filtered aggregation (AQE on)", aqe_query, warmup=True)

print("\n=== Plan ===")
aqe_query.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Look for `AdaptiveSparkPlan isFinalPlan=false` near the top — that's
# MAGIC the AQE wrapper. In production: leave AQE on. The few cases where it
# MAGIC hurts (highly predictable workloads with no skew) are not worth the
# MAGIC cognitive cost of remembering the toggle.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Caching — `cache()` vs `persist()`
# MAGIC
# MAGIC When the same DataFrame is used multiple times, Spark recomputes it
# MAGIC from scratch each time unless told otherwise. Two ways to keep it:
# MAGIC
# MAGIC | Method | Storage level |
# MAGIC |---|---|
# MAGIC | `df.cache()` | `MEMORY_AND_DISK` (a sensible default) |
# MAGIC | `df.persist(level)` | Whatever level you specify |
# MAGIC
# MAGIC Common levels: `MEMORY_ONLY` (fastest, evicted under pressure),
# MAGIC `MEMORY_AND_DISK` (default, spills to disk when memory full),
# MAGIC `DISK_ONLY` (no memory cost, slower reads).
# MAGIC
# MAGIC ### Free Edition Serverless note
# MAGIC
# MAGIC Serverless rejects **both** `df.cache()` (`PERSIST TABLE`) and
# MAGIC `df.unpersist()` (`UNPERSIST TABLE`) at runtime. Serverless manages
# MAGIC cache lifecycle internally — there is no user-facing persist API.
# MAGIC The cache demo below is wrapped in `try/except`: on Serverless it
# MAGIC reports the restriction and skips; on classic compute or dedicated
# MAGIC clusters it runs normally and you'll see the 2–5× speedup of the
# MAGIC cached run vs the uncached run.

# COMMAND ----------

# Use a moderately complex transformation so the recomputation cost is visible.
df_filtered = (df_trips
    .filter("fare_amount > 0 AND trip_distance > 0")
    .groupBy("VendorID")
    .agg({"fare_amount": "avg", "trip_distance": "avg"}))

print("--- Without caching: every action recomputes from scratch. ---")
time_query("Without cache, run 1", df_filtered)
time_query("Without cache, run 2", df_filtered)

# --- Materialise the cache, then measure cached reads.
try:
    df_filtered.cache()
    print("\n--- With caching: first action materialises, second reads from cache. ---")
    time_query("With cache, run 1 (materialise)", df_filtered)
    time_query("With cache, run 2 (cached)",      df_filtered)
except Exception as e:
    if "NOT_SUPPORTED_WITH_SERVERLESS" in str(e):
        print("\nSkipping cache demo: PERSIST TABLE is blocked on Serverless.")
        print("This step runs on classic compute / dedicated clusters.")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC On classic compute the second cached run should be measurably faster
# MAGIC than the second uncached run — typically 2–5× on this size of data.
# MAGIC On Serverless the cache call itself is rejected, so the cell prints
# MAGIC the restriction and continues.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Z-Order — data skipping at scan time
# MAGIC
# MAGIC `OPTIMIZE table ZORDER BY (col)` rewrites the underlying parquet files
# MAGIC so values of `col` are **co-located** in fewer files. Subsequent
# MAGIC queries that filter on `col` skip more files at scan time (Delta's
# MAGIC "data skipping").
# MAGIC
# MAGIC We materialise a copy of `trips_2025` with multiple files (so there's
# MAGIC something to prune), time a filter query, run `OPTIMIZE ... ZORDER`,
# MAGIC time the same query again, and compare.

# COMMAND ----------

# Repartition into 32 files so OPTIMIZE has something to consolidate. With
# only 2-3 files Z-order has nothing to demonstrate.
(df_trips
    .repartition(32)
    .select("VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID",
            "fare_amount", "trip_distance")
    .write
    .mode("overwrite")
    .saveAsTable(GOLD_ZORDER))

print(f"{GOLD_ZORDER}: {spark.table(GOLD_ZORDER).count():,} rows")

# COMMAND ----------

zorder_query = f"SELECT * FROM {GOLD_ZORDER} WHERE PULocationID = 132"

print("--- Before Z-order (warmup, then measured) ---")
t_before = time_query("Filter PULocationID = 132 (BEFORE Z-order)", zorder_query, warmup=True)

# COMMAND ----------

# Apply Z-order — files are rewritten clustered by PULocationID.
spark.sql(f"OPTIMIZE {GOLD_ZORDER} ZORDER BY (PULocationID)")
print("OPTIMIZE ZORDER BY (PULocationID) complete.")

# COMMAND ----------

print("--- After Z-order (warmup, then measured) ---")
t_after = time_query("Filter PULocationID = 132 (AFTER Z-order)", zorder_query, warmup=True)

print(f"\nSpeedup: {t_before / max(t_after, 0.01):.1f}× faster after Z-order.")

# COMMAND ----------

# MAGIC %md
# MAGIC The post-Z-order query is faster because Delta only reads the files
# MAGIC that actually contain `PULocationID = 132`. `DESCRIBE DETAIL` shows
# MAGIC the file count of the rewritten table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.gold.taxi_trips_zordered

# COMMAND ----------

# MAGIC %md
# MAGIC The `EXPLAIN FORMATTED` of the filtered query shows
# MAGIC `numFilesAfterPruning < numFiles` — Delta skipped files at scan time.

# COMMAND ----------

spark.sql(zorder_query).explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Liquid Clustering
# MAGIC
# MAGIC Liquid Clustering is the modern alternative to Z-order. Differences:
# MAGIC
# MAGIC | Aspect | Z-Order | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | When applied | After load, via `OPTIMIZE ... ZORDER BY` | Continuously, declared at table creation |
# MAGIC | Re-clustering | Manual `OPTIMIZE` again | Automatic |
# MAGIC | Adding columns | Rewrite all data | Just `ALTER TABLE` |
# MAGIC | Best for | Static datasets, batch ETL | Frequently-updated tables |
# MAGIC
# MAGIC Liquid Clustering requires Delta protocol writer 7+ — wrapped in
# MAGIC try/except in case the runtime rejects it.

# COMMAND ----------

cluster_query = f"SELECT * FROM {GOLD_CLUSTERED} WHERE PULocationID = 132"

try:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_CLUSTERED}")
    spark.sql(f"""
        CREATE TABLE {GOLD_CLUSTERED}
        CLUSTER BY (PULocationID)
        AS SELECT * FROM {GOLD_ZORDER}
    """)
    spark.sql(f"OPTIMIZE {GOLD_CLUSTERED}")
    print(f"{GOLD_CLUSTERED}: {spark.table(GOLD_CLUSTERED).count():,} rows (clustered)")

    t_cluster = time_query("Filter PULocationID = 132 (Liquid Clustering)", cluster_query, warmup=True)
    print(f"\nFor reference — Z-order: {t_after:.2f}s, Liquid Clustering: {t_cluster:.2f}s")
except Exception as e:
    print(f"Liquid Clustering not available on this runtime: {type(e).__name__}: {e}")
    print("Skipping; the Z-ordered table from Step 4 is sufficient for the lessons.")

# COMMAND ----------

# MAGIC %md
# MAGIC On a clustered table, `OPTIMIZE` triggers clustering automatically (no
# MAGIC `ZORDER BY` clause needed). For new Delta tables, Databricks now
# MAGIC recommends Liquid Clustering by default — Z-order remains a fine
# MAGIC choice for static tables that you optimise once and read many times.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Repartition vs Coalesce
# MAGIC
# MAGIC Both change the number of partitions, but they're not interchangeable:
# MAGIC
# MAGIC | Operation | What it does | Shuffle? | When to use |
# MAGIC |---|---|---|---|
# MAGIC | `df.repartition(N)` | New partitioning, even sizes | Yes | Increase parallelism, balance skew |
# MAGIC | `df.repartition(N, col)` | New partitioning by hash of `col` | Yes | Co-locate by key for upcoming join |
# MAGIC | `df.coalesce(N)` | Merge existing partitions | No (only when N < current) | Reduce small files before write |
# MAGIC
# MAGIC Rule of thumb: use `coalesce` to **shrink**, `repartition` to **grow**
# MAGIC or **rebalance**. Coalesce on growth would force a shuffle anyway, so
# MAGIC it's pointless.

# COMMAND ----------

print(f"Initial partitions:  {df_trips.rdd.getNumPartitions()}")

df_coalesced = df_trips.coalesce(8)
print(f"After coalesce(8):   {df_coalesced.rdd.getNumPartitions()}")

df_repartitioned = df_trips.repartition(32, "VendorID")
print(f"After repartition(32, VendorID): {df_repartitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC When writing to a partitioned Delta table, `repartition(col)` before
# MAGIC the write avoids "many tiny files per partition" — each Spark
# MAGIC partition writes one file per Delta partition.
