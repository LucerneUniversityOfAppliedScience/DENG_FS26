# Databricks notebook source

# MAGIC %md
# MAGIC # Performance and Optimization — Solution
# MAGIC
# MAGIC In this notebook you learn how to read **Spark physical plans**, control
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
# MAGIC Clustering may not be enabled on every Free Edition workspace — the
# MAGIC notebook flags this and continues if so.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC `workspace.nyc_taxi.trips_2025` (12 months of yellow taxi trips, manually
# MAGIC pre-staged on Free Edition because the upstream CloudFront URL is blocked
# MAGIC by the egress firewall) joined with the small `vendor_list` lookup created
# MAGIC by `copy_sample_data.py`.
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
# MAGIC The performance notebook joins `trips_2025` with the small `vendor_list`
# MAGIC lookup. The cell below materialises the trips Delta table **only if it
# MAGIC does not exist yet**, with a three-tier fallback so it always succeeds:
# MAGIC
# MAGIC 1. Table already exists → skip
# MAGIC 2. Real NYC parquet files in `/Volumes/workspace/nyc_taxi/raw_files/`
# MAGIC    (manually uploaded — CloudFront is firewall-blocked on Free Edition)
# MAGIC    → use them
# MAGIC 3. Otherwise → generate 2,000,000 rows of **synthetic** NYC-shaped data
# MAGIC    with realistic distributions (uniform `VendorID` 1–7, 263 NYC zones,
# MAGIC    99% short trips with a thin tail above 100 miles)
# MAGIC
# MAGIC Synthetic data is more than enough to demonstrate join strategies,
# MAGIC AQE markers, caching effects, and Z-order data skipping. Real data is
# MAGIC preferred when available because the actual `PULocationID`
# MAGIC distribution is heavily skewed toward Manhattan and that makes Z-order
# MAGIC visibly better — the synthetic uniform distribution still works, just
# MAGIC less dramatically.

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
        # 99% of trips ≤ 30 miles, 1% in [100, 180] for a meaningful AQE filter on > 100
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
        # ~4 minutes per mile (rough NYC pace)
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
        # Volume may not exist yet, or be empty — fall through to synthetic.
        pass

    if parquet_files:
        print(f"Loading {len(parquet_files)} real parquet file(s) from {NYC_PARQUET_DIR}.")
        for f in parquet_files:
            print(f"  - {f}")
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
# MAGIC Drop the two gold copies before re-running so a stale schema does not
# MAGIC block the overwrite. We do NOT drop `trips_2025` itself — that's our
# MAGIC source data.

# COMMAND ----------

for table in [GOLD_ZORDER, GOLD_CLUSTERED]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Reading a physical plan — the baseline join
# MAGIC
# MAGIC Every Spark query goes through three plan stages: parsed → analysed →
# MAGIC optimised → physical. `.explain()` prints the **physical plan** that
# MAGIC actually runs.
# MAGIC
# MAGIC The first thing to look at is the join operator. Spark picks one of:
# MAGIC
# MAGIC | Operator | When chosen | Cost |
# MAGIC |---|---|---|
# MAGIC | `BroadcastHashJoin` | One side fits in `autoBroadcastJoinThreshold` (10 MB by default) | Cheapest — no shuffle |
# MAGIC | `ShuffledHashJoin` | Small side hashable, large side too big to broadcast | One shuffle |
# MAGIC | `SortMergeJoin` | Both sides are large | Two shuffles + sorts |
# MAGIC
# MAGIC ### Free Edition Serverless note
# MAGIC
# MAGIC On Free Edition Serverless many session-level Spark configs are
# MAGIC **read-only** — including `spark.sql.autoBroadcastJoinThreshold` and
# MAGIC `spark.sql.adaptive.enabled`. Trying to call `spark.conf.set(...)` on
# MAGIC those keys raises `[CONFIG_NOT_AVAILABLE]`. Instead we use **SQL join
# MAGIC hints**, which are per-query and always allowed:
# MAGIC
# MAGIC - `/*+ MERGE(table) */` — force SortMergeJoin
# MAGIC - `/*+ BROADCAST(table) */` — force BroadcastHashJoin
# MAGIC - `/*+ SHUFFLE_HASH(table) */` — force ShuffledHashJoin
# MAGIC
# MAGIC We let Spark pick the default plan first, then force each strategy via
# MAGIC hint and compare.

# COMMAND ----------

df_trips   = spark.table(TRIPS_TABLE)
df_vendors = spark.table(VENDOR_TABLE)

# Default plan — Spark decides. With a 7-row vendor lookup it will almost
# certainly auto-broadcast.
df_joined = df_trips.join(df_vendors, "VendorID", "left")
print("=== DEFAULT PLAN ===")
df_joined.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC The default plan most likely shows `BroadcastHashJoin` because the
# MAGIC vendor lookup is well below the 10 MB auto-broadcast threshold. Now
# MAGIC force a SortMergeJoin via SQL hint to see the alternative.

# COMMAND ----------

print("=== FORCED SortMergeJoin via /*+ MERGE */ hint ===")
spark.sql(f"""
    SELECT /*+ MERGE(v) */
           t.VendorID,
           v.VendorName,
           t.fare_amount
    FROM {TRIPS_TABLE} t
    LEFT JOIN {VENDOR_TABLE} v ON t.VendorID = v.VendorID
""").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC In this plan you should see `SortMergeJoin` with both sides going
# MAGIC through `Exchange hashpartitioning(VendorID, ...)` — that's the shuffle
# MAGIC SortMergeJoin requires. On a 30M-row trips table this shuffle would be
# MAGIC the dominant cost — exactly why Spark prefers broadcast for tiny
# MAGIC lookups.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Broadcast join — explicit
# MAGIC
# MAGIC Two equivalent ways to force a broadcast:
# MAGIC
# MAGIC 1. **Python:** wrap the small side with `F.broadcast(...)` — explicit,
# MAGIC    scoped to one DataFrame query
# MAGIC 2. **SQL hint:** `/*+ BROADCAST(table) */` — same effect, scoped to one
# MAGIC    query
# MAGIC
# MAGIC Both produce a `BroadcastHashJoin` regardless of the autobroadcast
# MAGIC threshold. Useful when the auto-detection is wrong, e.g. when the small
# MAGIC side is the result of a filter and Spark doesn't know its post-filter
# MAGIC size.

# COMMAND ----------

from pyspark.sql.functions import broadcast

# Python API: F.broadcast() hint
df_joined_bcast = df_trips.join(broadcast(df_vendors), "VendorID", "left")
print("=== EXPLICIT BROADCAST via F.broadcast() ===")
df_joined_bcast.explain()

# COMMAND ----------

# SQL: /*+ BROADCAST */ hint — same effect
print("=== EXPLICIT BROADCAST via SQL hint ===")
spark.sql(f"""
    SELECT /*+ BROADCAST(v) */
           t.VendorID,
           v.VendorName,
           t.fare_amount
    FROM {TRIPS_TABLE} t
    LEFT JOIN {VENDOR_TABLE} v ON t.VendorID = v.VendorID
""").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Both plans contain `BroadcastHashJoin` with a `BroadcastExchange`
# MAGIC feeding the small side. No shuffle on the large side. When in doubt
# MAGIC about Spark's automatic choice, the hint removes ambiguity.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE re-plans the query at runtime based on actual statistics. The three
# MAGIC headline features:
# MAGIC
# MAGIC - **Coalesce shuffle partitions** — combines tiny post-shuffle partitions
# MAGIC - **Switch join strategy** — converts SortMergeJoin to BroadcastHashJoin
# MAGIC   when a runtime statistic shows one side is small after filtering
# MAGIC - **Skew join handling** — splits one giant partition across multiple
# MAGIC   tasks
# MAGIC
# MAGIC AQE is **on by default and not user-toggleable on Free Edition
# MAGIC Serverless** — `spark.conf.set("spark.sql.adaptive.enabled", ...)`
# MAGIC raises `[CONFIG_NOT_AVAILABLE]`. We can still see AQE in action by
# MAGIC reading the explain output: any plan wrapped in `AdaptiveSparkPlan`
# MAGIC will be re-optimised at runtime.

# COMMAND ----------

aqe_query = (df_trips
    .filter("trip_distance > 100")        # very selective: long trips only
    .groupBy("VendorID")
    .count())

print("=== AQE plan markers ===")
aqe_query.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Look for `AdaptiveSparkPlan isFinalPlan=false` near the top — that's
# MAGIC the AQE wrapper. The printed plan shows the **initial** plan; the plan
# MAGIC that actually runs may be different because AQE re-optimises after the
# MAGIC first shuffle completes and real partition sizes are known.
# MAGIC
# MAGIC In production: leave AQE on. The few cases where it hurts (highly
# MAGIC predictable workloads with no skew) are not worth the cognitive cost
# MAGIC of remembering the toggle.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Caching — `cache()` vs `persist()`
# MAGIC
# MAGIC When the same DataFrame is used multiple times, Spark recomputes it from
# MAGIC scratch each time unless told otherwise. Two ways to keep it around:
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
# MAGIC We measure the same aggregation twice with and without caching.

# COMMAND ----------

import time

def time_action(label, fn):
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    print(f"{label}: {elapsed:.2f}s  (result preview: {result})")
    return elapsed

# Use a moderately complex transformation so the recomputation cost is visible.
df_filtered = (df_trips
    .filter("fare_amount > 0 AND trip_distance > 0")
    .groupBy("VendorID")
    .agg({"fare_amount": "avg", "trip_distance": "avg"}))

# --- Without caching: every action recomputes from scratch.
df_filtered.unpersist()  # ensure clean state
time_action("Without cache, run 1", lambda: df_filtered.count())
time_action("Without cache, run 2", lambda: df_filtered.count())

# --- With caching: first action materialises, second action reads from cache.
df_filtered.cache()
time_action("With cache, run 1 (materialise)", lambda: df_filtered.count())
time_action("With cache, run 2 (cached)",      lambda: df_filtered.count())

df_filtered.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC The second cached run should be measurably faster than the second
# MAGIC uncached run — typically 2–5× on this size of data. Always call
# MAGIC `.unpersist()` when you're done so cache memory is reclaimed.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Z-Order on a Delta table
# MAGIC
# MAGIC `OPTIMIZE table ZORDER BY (col)` rewrites the underlying parquet files
# MAGIC so that values of `col` are co-located. Subsequent queries that filter on
# MAGIC `col` skip more files at scan time (Delta's "data skipping").
# MAGIC
# MAGIC We materialise a copy of `trips_2025`, Z-order it on `PULocationID`,
# MAGIC then compare an `EXPLAIN FORMATTED` of a filtered query before and
# MAGIC after.

# COMMAND ----------

# Create a smaller working copy so OPTIMIZE finishes quickly on Free Edition.
(df_trips
    .select("VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID",
            "fare_amount", "trip_distance")
    .write
    .mode("overwrite")
    .saveAsTable(GOLD_ZORDER))

print(f"{GOLD_ZORDER}: {spark.table(GOLD_ZORDER).count():,} rows")

# COMMAND ----------

# Run a filtered query before Z-ordering and capture its plan.
print("=== BEFORE ZORDER ===")
spark.sql(f"SELECT count(*) FROM {GOLD_ZORDER} WHERE PULocationID = 132").explain("formatted")

# COMMAND ----------

# Apply Z-order. Files are rewritten clustered by PULocationID.
spark.sql(f"OPTIMIZE {GOLD_ZORDER} ZORDER BY (PULocationID)")

print("=== AFTER ZORDER ===")
spark.sql(f"SELECT count(*) FROM {GOLD_ZORDER} WHERE PULocationID = 132").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC In the second plan, look for the `PartitionFilters` and especially the
# MAGIC `numFiles` / `numFilesAfterPruning` metrics on the scan — Z-order should
# MAGIC reduce the number of files actually read.
# MAGIC
# MAGIC `DESCRIBE DETAIL` shows the file count of the rewritten table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.gold.taxi_trips_zordered

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Liquid Clustering
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
# MAGIC Liquid Clustering is declared with `CLUSTER BY` at table creation. It
# MAGIC requires Delta protocol writer 7+ — flagged below if not available.

# COMMAND ----------

try:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_CLUSTERED}")
    spark.sql(f"""
        CREATE TABLE {GOLD_CLUSTERED}
        CLUSTER BY (PULocationID)
        AS SELECT * FROM {GOLD_ZORDER}
    """)
    print(f"{GOLD_CLUSTERED}: {spark.table(GOLD_CLUSTERED).count():,} rows")
    print("Liquid Clustering active.")
except Exception as e:
    print(f"Liquid Clustering not available on this runtime: {e}")
    print("Skipping; the Z-ordered table from Step 5 is sufficient for the rest of the notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC On a clustered table, `OPTIMIZE` will trigger clustering automatically
# MAGIC (no `ZORDER BY` clause needed):

# COMMAND ----------

try:
    spark.sql(f"OPTIMIZE {GOLD_CLUSTERED}")
    print("Clustered OPTIMIZE complete.")
except Exception as e:
    print(f"Clustered OPTIMIZE skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Repartition vs Coalesce
# MAGIC
# MAGIC Both change the number of partitions, but they're not interchangeable:
# MAGIC
# MAGIC | Operation | What it does | Shuffle? | When to use |
# MAGIC |---|---|---|---|
# MAGIC | `df.repartition(N)` | New partitioning, even sizes | Yes | Increase parallelism, balance skew |
# MAGIC | `df.repartition(N, col)` | New partitioning by hash of `col` | Yes | Co-locate by key for upcoming join |
# MAGIC | `df.coalesce(N)` | Merge existing partitions | No (only when N < current) | Reduce small files before write |
# MAGIC
# MAGIC Rule of thumb: use `coalesce` to **shrink**, `repartition` to **grow** or
# MAGIC **rebalance**. Coalesce on growth would force a shuffle anyway, so it's
# MAGIC pointless.

# COMMAND ----------

# Initial number of partitions (depends on file count).
print(f"Initial partitions:  {df_trips.rdd.getNumPartitions()}")

# Coalesce down to 8 — no shuffle.
df_coalesced = df_trips.coalesce(8)
print(f"After coalesce(8):   {df_coalesced.rdd.getNumPartitions()}")

# Repartition up to 32 — full shuffle.
df_repartitioned = df_trips.repartition(32, "VendorID")
print(f"After repartition(32, VendorID): {df_repartitioned.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC When writing to a partitioned Delta table, `repartition(col)` before the
# MAGIC write avoids "many tiny files per partition" — each Spark partition
# MAGIC writes one file per Delta partition.
