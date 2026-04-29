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
# MAGIC **Task:** disable autobroadcast (set `spark.sql.autoBroadcastJoinThreshold`
# MAGIC to `-1`), join `trips_2025` with `vendor_list` on `VendorID`, and call
# MAGIC `.explain()` on the result. Confirm you see `SortMergeJoin`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: SortMergeJoin baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Broadcast join
# MAGIC
# MAGIC The `vendor_list` lookup has 7 rows. Broadcasting it eliminates the
# MAGIC shuffle. Two ways to force a broadcast:
# MAGIC
# MAGIC 1. Hint via `F.broadcast(...)` — explicit, scoped to one query
# MAGIC 2. Raise `autoBroadcastJoinThreshold` — global
# MAGIC
# MAGIC **Task:** join again, this time wrapping `df_vendors` in `broadcast(...)`.
# MAGIC `.explain()` should now show `BroadcastHashJoin`. Then re-enable
# MAGIC autobroadcast at 10 MB so the rest of the notebook isn't crippled.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: broadcast join")

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
# MAGIC **Task:** define a query with a very selective filter (e.g.
# MAGIC `trip_distance > 100`), aggregate by `VendorID`, then `.explain()` once
# MAGIC with AQE off and once with AQE on. Compare the printed plans — the AQE-on
# MAGIC plan is wrapped in `AdaptiveSparkPlan`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: AQE on/off comparison")

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
