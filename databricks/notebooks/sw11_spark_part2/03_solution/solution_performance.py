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

# COMMAND ----------

TRIPS_TABLE      = "workspace.nyc_taxi.trips_2025"
VENDOR_TABLE     = "workspace.nyc_taxi.vendor_list"
GOLD_ZORDER      = "workspace.gold.taxi_trips_zordered"
GOLD_CLUSTERED   = "workspace.gold.taxi_trips_clustered"

print(f"Trips    : {TRIPS_TABLE}")
print(f"Vendors  : {VENDOR_TABLE}")
print(f"Z-order  : {GOLD_ZORDER}")
print(f"Clustered: {GOLD_CLUSTERED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop existing tables
# MAGIC
# MAGIC Drop the two gold copies before re-running so a stale schema does not
# MAGIC block the overwrite.

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
# MAGIC We start with a join that Spark refuses to broadcast (we artificially
# MAGIC raise the threshold above the lookup size), so it falls back to
# MAGIC `SortMergeJoin`.

# COMMAND ----------

# Force SortMergeJoin by disabling autobroadcast.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

df_trips   = spark.table(TRIPS_TABLE)
df_vendors = spark.table(VENDOR_TABLE)

df_joined = df_trips.join(df_vendors, "VendorID", "left")
df_joined.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC In the printed plan you should see `SortMergeJoin` with both sides going
# MAGIC through `Exchange hashpartitioning(VendorID, ...)` — that's the shuffle.
# MAGIC On a 30M-row trips table this shuffle is the dominant cost.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Broadcast join
# MAGIC
# MAGIC The `vendor_list` lookup has 7 rows. Broadcasting it to every executor
# MAGIC eliminates the shuffle entirely. There are two ways to force a broadcast:
# MAGIC
# MAGIC 1. **Hint via `F.broadcast(...)`** — explicit, scoped to one query
# MAGIC 2. **Raise `autoBroadcastJoinThreshold`** — global, applies to any small
# MAGIC    enough table
# MAGIC
# MAGIC The hint is the cleaner choice when you know which side should be
# MAGIC broadcast.

# COMMAND ----------

from pyspark.sql.functions import broadcast

df_joined_bcast = df_trips.join(broadcast(df_vendors), "VendorID", "left")
df_joined_bcast.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC The plan now contains `BroadcastHashJoin` with `BroadcastExchange` feeding
# MAGIC the small side. No shuffle on the large side. This is almost always the
# MAGIC fastest option when one side is small enough.
# MAGIC
# MAGIC Re-enable autobroadcast for the rest of the notebook (so we don't fight
# MAGIC the optimiser elsewhere):

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB default
print("autoBroadcastJoinThreshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Adaptive Query Execution (AQE)
# MAGIC
# MAGIC AQE re-plans the query at runtime based on actual statistics. The three
# MAGIC headline features are:
# MAGIC
# MAGIC - **Coalesce shuffle partitions** — combines tiny post-shuffle partitions
# MAGIC - **Switch join strategy** — converts SortMergeJoin to BroadcastHashJoin
# MAGIC   if a runtime statistic shows one side is small after filtering
# MAGIC - **Skew join handling** — splits one giant partition across multiple
# MAGIC   tasks
# MAGIC
# MAGIC AQE is on by default in modern Databricks runtimes. Toggle it off and
# MAGIC observe the plan difference on a query whose filter shrinks one side.

# COMMAND ----------

aqe_query = (df_trips
    .filter("trip_distance > 100")        # very selective: long trips only
    .groupBy("VendorID")
    .count())

# AQE off
spark.conf.set("spark.sql.adaptive.enabled", False)
print("=== AQE OFF ===")
aqe_query.explain()

# AQE on
spark.conf.set("spark.sql.adaptive.enabled", True)
print("=== AQE ON ===")
aqe_query.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC With AQE on you'll see `AdaptiveSparkPlan` wrapping the operators. The
# MAGIC actual plan that runs is determined at runtime — the printed plan shows
# MAGIC the initial plan plus markers indicating where AQE may intervene.
# MAGIC
# MAGIC In production: leave AQE on. Turning it off is almost never the right
# MAGIC answer.

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
