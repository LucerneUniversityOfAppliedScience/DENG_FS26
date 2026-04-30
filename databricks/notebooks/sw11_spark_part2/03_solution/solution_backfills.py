# Databricks notebook source

# MAGIC %md
# MAGIC # Backfills as a First-Class Operation — Solution
# MAGIC
# MAGIC In this notebook you learn how to design pipelines so **backfills are
# MAGIC just another invocation of the same code path** — not a special "fix"
# MAGIC pipeline that diverges from the forward run.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC At small scale you can always full-refresh: cheap, correct by
# MAGIC construction, no state to manage. At large scale you must run
# MAGIC incrementally — but bugs happen, schemas evolve, source data gets
# MAGIC corrected. **Recomputation is the escape hatch** that makes
# MAGIC incremental safe; without it your pipeline has cornered you.
# MAGIC
# MAGIC The only way recomputation stays safe is if your transformation is:
# MAGIC - **Idempotent** — same inputs produce the same outputs, on every run
# MAGIC - **Deterministic** — no `current_timestamp()`, no random IDs, no
# MAGIC   ordering-dependent logic
# MAGIC - **Partition-scoped** — touches only the partition you're rebuilding,
# MAGIC   never adjacent ones
# MAGIC
# MAGIC With those three properties, the backfill is just `transform(date)`
# MAGIC in a loop. Without them, every backfill is a manual, hairy operation.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC Redeploy the UC bundle once if you haven't yet.

# COMMAND ----------

CATALOG = "workspace"
TARGET_TABLE = f"{CATALOG}.silver.events_partitioned"

print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop the target

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
print(f"Dropped (if existed): {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: When you need a backfill (slide 53)
# MAGIC
# MAGIC Five recurring scenarios:
# MAGIC
# MAGIC | Scenario | Example |
# MAGIC |---|---|
# MAGIC | **Bug in transformation** | A division had the wrong denominator for a month |
# MAGIC | **New column added retroactively** | New `region` dim, populate for the last 90 days |
# MAGIC | **Source corrected upstream** | Salesforce fixed the customer master, re-ingest all history |
# MAGIC | **Schema migration** | Switch from `STRING amount` to `DECIMAL`, reprocess everything |
# MAGIC | **Late-arriving data** | A day's events arrived 3 days late, re-run that day |
# MAGIC
# MAGIC Every team encounters all five, repeatedly.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: The anti-pattern — a separate "fix" pipeline
# MAGIC
# MAGIC The temptation: write a one-off Python script that reads from
# MAGIC source, applies the fix, and writes to the target. Different code
# MAGIC path from the forward pipeline. Faster to write, slower to maintain.
# MAGIC
# MAGIC Why it goes wrong:
# MAGIC - **Drift** — the fix script gets a slight tweak, the forward
# MAGIC   pipeline doesn't. Future forward runs have a subtle bug.
# MAGIC - **No tests** — fix scripts rarely get reviewed or tested.
# MAGIC - **No observability** — different metrics, different alerting.
# MAGIC - **Not repeatable** — three months later, no one remembers exactly
# MAGIC   what the fix script did.
# MAGIC
# MAGIC The discipline: **one transform function, used for both forward and
# MAGIC backfill runs**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: The discipline — a single `transform(date)` (slide 53)
# MAGIC
# MAGIC The function below has the contract: **idempotent, deterministic,
# MAGIC partition-scoped**. Implementation uses `replaceWhere` (slide 40)
# MAGIC on the date partition.
# MAGIC
# MAGIC The same function is called by the forward pipeline (`transform(today)`)
# MAGIC and by backfills (`transform(any_past_date)`). Same code, same
# MAGIC guarantees, same observability.

# COMMAND ----------

from pyspark.sql.functions import expr, lit, col

def read_source(date: str):
    """
    Synthetic source: deterministic events for a given date.
    In production this would read from Bronze, an OLTP mirror, or a Kafka
    topic with a date filter. Whatever it is, it must be deterministic
    given the date — same input, same rows.
    """
    seed_for_date = sum(ord(c) for c in date)  # stable per date
    return (spark.range(1000)
        .withColumn("event_date", lit(date).cast("date"))
        .withColumn("user_id",    (expr(f"rand({seed_for_date}) * 500") + 1).cast("int"))
        .withColumn("amount",     expr(f"round(rand({seed_for_date + 1}) * 100, 2)"))
        .drop("id"))

def compute(df_source):
    """
    The pure transformation. No clock reads, no randomness — only
    deterministic SQL on the input. Same input, same output, every run.
    """
    return (df_source
        .withColumn("amount_with_tax", expr("round(amount * 1.077, 2)"))
        .withColumn("user_bucket",     expr("user_id % 10")))

def transform(date: str):
    """
    The forward + backfill workhorse.
    Idempotent: re-running with the same source produces the same target.
    Partition-scoped: only event_date = `date` is touched.
    """
    df_src = read_source(date)
    df_out = compute(df_src)

    (df_out.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .option("replaceWhere", f"event_date = '{date}'")
        .saveAsTable(TARGET_TABLE))

    n = spark.sql(f"SELECT count(*) AS n FROM {TARGET_TABLE} WHERE event_date = '{date}'").first()["n"]
    print(f"transform({date}) -> {n:,} rows in partition")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Forward run — today's slice
# MAGIC
# MAGIC The pipeline normally runs once per day with `transform(today)`. We
# MAGIC simulate "today" with a fixed date for reproducibility.

# COMMAND ----------

transform("2026-04-30")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Backfill loop — same code, many dates
# MAGIC
# MAGIC The backfill calls the same `transform()` for each historical date.
# MAGIC No new code, no separate pipeline.

# COMMAND ----------

import pandas as pd

backfill_dates = [d.date().isoformat() for d in pd.date_range("2026-04-01", "2026-04-15", freq="D")]
print(f"Backfilling {len(backfill_dates)} dates: {backfill_dates[0]} ... {backfill_dates[-1]}")

for d in backfill_dates:
    transform(d)

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE HISTORY` shows one separate Delta commit per backfill day
# MAGIC plus the original forward run — each is atomic, each can be rolled
# MAGIC back individually via Delta time travel.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, timestamp, operation, operationParameters
# MAGIC FROM (DESCRIBE HISTORY workspace.silver.events_partitioned)
# MAGIC ORDER BY version

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total rows after the forward run + 15 backfill days = 16 partitions × 1000 rows
# MAGIC SELECT event_date, count(*) AS n
# MAGIC FROM workspace.silver.events_partitioned
# MAGIC GROUP BY event_date
# MAGIC ORDER BY event_date

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Re-run safety — idempotency in action
# MAGIC
# MAGIC Run `transform("2026-04-10")` a second time. Because the source is
# MAGIC deterministic and the write uses `replaceWhere`, the row count for
# MAGIC that date should be **identical** to before, and the partition is
# MAGIC swapped atomically (single Delta commit).

# COMMAND ----------

print("Before re-run:")
display(spark.sql(
    "SELECT event_date, count(*) AS n FROM workspace.silver.events_partitioned "
    "WHERE event_date = '2026-04-10' GROUP BY event_date"
))

transform("2026-04-10")

print("\nAfter re-run (should be unchanged):")
display(spark.sql(
    "SELECT event_date, count(*) AS n FROM workspace.silver.events_partitioned "
    "WHERE event_date = '2026-04-10' GROUP BY event_date"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variant: source got corrected — re-run with the new source
# MAGIC
# MAGIC Imagine the source for `2026-04-10` was wrong and has been fixed
# MAGIC upstream. The same `transform("2026-04-10")` call now picks up the
# MAGIC corrected source and rewrites just that partition. Other partitions
# MAGIC are untouched — `replaceWhere` guarantees that.
# MAGIC
# MAGIC We simulate a corrected source by overriding `read_source` for one
# MAGIC date, calling `transform()`, then verifying only that partition's
# MAGIC content changed.

# COMMAND ----------

# Capture row count of an unrelated partition so we can verify it was untouched
n_other_before = spark.sql(
    "SELECT count(*) AS n FROM workspace.silver.events_partitioned "
    "WHERE event_date = '2026-04-05'"
).first()["n"]

# Override the source for one date — pretend upstream corrected the data
def read_source_corrected(date: str):
    if date == "2026-04-10":
        # Different shape: half the rows, doubled amounts
        return (spark.range(500)
            .withColumn("event_date", lit(date).cast("date"))
            .withColumn("user_id",    expr("cast(rand(99) * 500 + 1 as int)"))
            .withColumn("amount",     expr("round(rand(98) * 200, 2)"))
            .drop("id"))
    return read_source(date)

# Patch and re-run
read_source_orig = read_source
try:
    read_source = read_source_corrected
    transform("2026-04-10")
finally:
    read_source = read_source_orig

n_other_after = spark.sql(
    "SELECT count(*) AS n FROM workspace.silver.events_partitioned "
    "WHERE event_date = '2026-04-05'"
).first()["n"]

n_corrected = spark.sql(
    "SELECT count(*) AS n FROM workspace.silver.events_partitioned "
    "WHERE event_date = '2026-04-10'"
).first()["n"]

print(f"event_date = 2026-04-05 row count before: {n_other_before}, after: {n_other_after}  (untouched ✓)")
print(f"event_date = 2026-04-10 row count after correction: {n_corrected}")

# COMMAND ----------

# MAGIC %md
# MAGIC The corrected partition has 500 rows (down from 1000), every other
# MAGIC partition is unchanged. **One commit, one partition, atomic.**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Operational considerations (slide 54)
# MAGIC
# MAGIC ### Resource isolation
# MAGIC Backfilling 2 years of daily partitions in parallel saturates the
# MAGIC cluster. Forward jobs starve, SLA breaks. **Fix:** dedicated job
# MAGIC pool / cluster, capped concurrency (`max_active_runs` in Airflow,
# MAGIC concurrency limits in Databricks Jobs).
# MAGIC
# MAGIC ### Ordering
# MAGIC Backfilling backwards may produce briefly inconsistent downstream
# MAGIC views (April-15 fixed but April-12 still buggy). **Fix:** gate
# MAGIC downstream consumers behind a high-water mark until the backfill
# MAGIC completes.
# MAGIC
# MAGIC ### Schema drift over time
# MAGIC Historical raw data may have different schemas than today's
# MAGIC pipeline expects. **Fix:** the forward pipeline and the backfill
# MAGIC must share a tolerant decoder (Auto Loader rescue mode, Avro
# MAGIC schema registry, explicit cast layer).
# MAGIC
# MAGIC ### Cost control
# MAGIC Backfills are full reads of historical data — easy to surprise the
# MAGIC bill. **Fix:** estimate before running; use spot instances where
# MAGIC possible; run in chunks (one month at a time), not 5 years in one
# MAGIC go.
# MAGIC
# MAGIC ## The bigger picture
# MAGIC
# MAGIC Scalability requires **recomputation discipline**. At small scale
# MAGIC you can always full-refresh; at large scale you must go incremental
# MAGIC AND be ready to recompute. Both paths are only safe if your
# MAGIC pipeline is deterministic, idempotent, and partition-scoped.
# MAGIC
# MAGIC This notebook's `transform(date)` function is the smallest possible
# MAGIC artifact that satisfies all three. Build everything on top of that
# MAGIC contract.
