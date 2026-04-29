# Databricks notebook source

# MAGIC %md
# MAGIC # Performance and Optimization — Exercise
# MAGIC
# MAGIC In this exercise you learn how to read **Spark physical plans**, control
# MAGIC the **join strategy**, decide between **caching strategies**, and improve
# MAGIC **read performance** with Z-Order or Liquid Clustering.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Most Spark code that "works" can be made 5–50× faster by tuning a handful
# MAGIC of decisions: which join algorithm, when to broadcast a small side, when
# MAGIC to cache, and how the data is laid out on disk. The single most important
# MAGIC habit is reading the physical plan via `.explain()` — without it you are
# MAGIC guessing.
# MAGIC
# MAGIC ## Free Edition note
# MAGIC
# MAGIC Databricks Free Edition runs on Serverless and has **limited Spark UI
# MAGIC access**. Instead of taking screenshots from the UI we measure with
# MAGIC `time.perf_counter()` and read `.explain()` output directly. Liquid
# MAGIC Clustering may not be enabled on every Free Edition workspace — your
# MAGIC code should fail gracefully if so.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC `workspace.nyc_taxi.trips_2025` joined with `workspace.nyc_taxi.vendor_list`.
# MAGIC The trips table is pre-staged from monthly parquet files; the lookup
# MAGIC table is created by `copy_sample_data.py`.
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
# MAGIC The cell below materialises the trips Delta table **only if it does
# MAGIC not exist yet**, with a three-tier fallback:
# MAGIC
# MAGIC 1. Table already exists → skip
# MAGIC 2. Real NYC parquet files in `/Volumes/workspace/nyc_taxi/raw_files/`
# MAGIC    → use them
# MAGIC 3. Otherwise → generate 2,000,000 rows of **synthetic** NYC-shaped
# MAGIC    data with realistic distributions
# MAGIC
# MAGIC Synthetic data is more than enough for the lessons in this notebook.
# MAGIC No action required — the cell takes care of itself.

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
        pass  # volume may not exist or be empty — fall through to synthetic

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
# MAGIC We do NOT drop `trips_2025` itself — that's our source data.

# COMMAND ----------

for table in [GOLD_ZORDER, GOLD_CLUSTERED]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Reading a physical plan — the baseline join
# MAGIC
# MAGIC `.explain()` prints the physical plan that actually runs. The first thing
# MAGIC to look at is the join operator. Spark picks one of:
# MAGIC
# MAGIC | Operator | When chosen | Cost |
# MAGIC |---|---|---|
# MAGIC | `BroadcastHashJoin` | One side fits in `autoBroadcastJoinThreshold` (10 MB by default) | Cheapest — no shuffle |
# MAGIC | `ShuffledHashJoin` | Small side hashable, large side too big to broadcast | One shuffle |
# MAGIC | `SortMergeJoin` | Both sides are large | Two shuffles + sorts |
# MAGIC
# MAGIC ### Free Edition Serverless note
# MAGIC
# MAGIC On Free Edition Serverless `spark.sql.autoBroadcastJoinThreshold` and
# MAGIC `spark.sql.adaptive.enabled` are **read-only** — `spark.conf.set(...)`
# MAGIC raises `[CONFIG_NOT_AVAILABLE]`. Use **SQL join hints** instead, which
# MAGIC are per-query and always allowed:
# MAGIC
# MAGIC - `/*+ MERGE(table) */` — force SortMergeJoin
# MAGIC - `/*+ BROADCAST(table) */` — force BroadcastHashJoin
# MAGIC - `/*+ SHUFFLE_HASH(table) */` — force ShuffledHashJoin
# MAGIC
# MAGIC **Task A:** join `trips_2025` with `vendor_list` on `VendorID` (default
# MAGIC plan, no hint) and `.explain()`. Note which join Spark picks.
# MAGIC
# MAGIC **Task B:** force a SortMergeJoin via a `%sql` query with the
# MAGIC `/*+ MERGE(v) */` hint. `.explain()` (use `EXPLAIN` in SQL) and
# MAGIC confirm `SortMergeJoin` with the two `Exchange hashpartitioning`
# MAGIC shuffles.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: default plan, then forced SortMergeJoin via /*+ MERGE */")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Broadcast join — explicit
# MAGIC
# MAGIC Two equivalent ways to force a broadcast:
# MAGIC
# MAGIC 1. **Python:** `F.broadcast(df_small)` in a join
# MAGIC 2. **SQL:** `/*+ BROADCAST(table) */`
# MAGIC
# MAGIC Both produce `BroadcastHashJoin` regardless of the autobroadcast
# MAGIC threshold. Useful when Spark's automatic choice is wrong (e.g. small
# MAGIC side is the result of a filter Spark can't size accurately).
# MAGIC
# MAGIC **Task:** demonstrate both — Python via `F.broadcast(df_vendors)` and
# MAGIC SQL via `/*+ BROADCAST(v) */`. `.explain()` both. Confirm
# MAGIC `BroadcastHashJoin` with `BroadcastExchange` on the small side.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: explicit broadcast via Python F.broadcast and SQL /*+ BROADCAST */")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE re-plans the query at runtime. Headline features:
# MAGIC
# MAGIC - **Coalesce shuffle partitions** — combines tiny post-shuffle partitions
# MAGIC - **Switch join strategy** — converts SortMergeJoin to BroadcastHashJoin
# MAGIC   if a runtime statistic shows one side is small after filtering
# MAGIC - **Skew join handling** — splits one giant partition across multiple tasks
# MAGIC
# MAGIC AQE is **on by default and not user-toggleable on Free Edition
# MAGIC Serverless**. We can still see AQE markers in the plan.
# MAGIC
# MAGIC **Task:** define a query with a very selective filter (e.g.
# MAGIC `trip_distance > 100`), aggregate by `VendorID`, then `.explain()`.
# MAGIC Look for `AdaptiveSparkPlan` near the top of the plan — that's the
# MAGIC AQE wrapper.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: explain a filtered aggregation, observe the AdaptiveSparkPlan wrapper")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Caching — `cache()` vs `persist()`
# MAGIC
# MAGIC | Method | Storage level |
# MAGIC |---|---|
# MAGIC | `df.cache()` | `MEMORY_AND_DISK` (default) |
# MAGIC | `df.persist(level)` | Whatever level you specify |
# MAGIC
# MAGIC **Task:** measure the same aggregation twice without caching and twice
# MAGIC with `.cache()`. Use `time.perf_counter()` and an action like `.count()`
# MAGIC to materialise. Always `.unpersist()` when done.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: cache vs no cache timing")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Z-Order on a Delta table
# MAGIC
# MAGIC `OPTIMIZE table ZORDER BY (col)` rewrites the underlying parquet files so
# MAGIC values of `col` are co-located. Subsequent queries that filter on `col`
# MAGIC skip more files at scan time.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Materialise a small subset of `trips_2025` into `GOLD_ZORDER`
# MAGIC    (select 5–6 useful columns).
# MAGIC 2. Run `EXPLAIN FORMATTED` on a filtered query against `PULocationID`.
# MAGIC 3. Run `OPTIMIZE GOLD_ZORDER ZORDER BY (PULocationID)`.
# MAGIC 4. Run the same `EXPLAIN FORMATTED` again. Compare `numFiles` /
# MAGIC    `numFilesAfterPruning` in the scan node.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: Z-order materialisation and explain comparison")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Liquid Clustering
# MAGIC
# MAGIC | Aspect | Z-Order | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | When applied | After load, via `OPTIMIZE ... ZORDER BY` | Continuously, declared at table creation |
# MAGIC | Re-clustering | Manual `OPTIMIZE` again | Automatic |
# MAGIC | Adding columns | Rewrite all data | Just `ALTER TABLE` |
# MAGIC | Best for | Static datasets, batch ETL | Frequently-updated tables |
# MAGIC
# MAGIC **Task:** create `GOLD_CLUSTERED` with `CLUSTER BY (PULocationID)` at
# MAGIC creation time, populated `AS SELECT * FROM GOLD_ZORDER`. Wrap in a
# MAGIC try/except — Liquid Clustering may not be available on this runtime.
# MAGIC
# MAGIC On the clustered table, `OPTIMIZE` triggers clustering automatically
# MAGIC (no `ZORDER BY` clause).

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: Liquid Clustering")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Repartition vs Coalesce
# MAGIC
# MAGIC | Operation | What it does | Shuffle? | When to use |
# MAGIC |---|---|---|---|
# MAGIC | `df.repartition(N)` | New partitioning, even sizes | Yes | Increase parallelism, balance skew |
# MAGIC | `df.repartition(N, col)` | New partitioning by hash of `col` | Yes | Co-locate by key for upcoming join |
# MAGIC | `df.coalesce(N)` | Merge existing partitions | No (only when N < current) | Reduce small files before write |
# MAGIC
# MAGIC **Task:** print the initial partition count of `df_trips`, then show the
# MAGIC partition count after `coalesce(8)` and after `repartition(32, "VendorID")`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7: repartition vs coalesce")
