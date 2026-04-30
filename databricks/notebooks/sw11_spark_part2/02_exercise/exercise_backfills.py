# Databricks notebook source

# MAGIC %md
# MAGIC # Backfills as a First-Class Operation — Exercise
# MAGIC
# MAGIC In this exercise you learn how to design pipelines so **backfills are
# MAGIC just another invocation of the same code path** — not a special "fix"
# MAGIC pipeline that diverges from the forward run.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Recomputation is the escape hatch that makes incremental safe. It
# MAGIC only stays safe if your transformation is:
# MAGIC - **Idempotent** — same inputs produce the same outputs, every run
# MAGIC - **Deterministic** — no clock reads, no random IDs, no ordering
# MAGIC   dependence
# MAGIC - **Partition-scoped** — touches only the partition you're rebuilding
# MAGIC
# MAGIC With those three properties, the backfill is just `transform(date)`
# MAGIC in a loop.
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
# MAGIC ## Step 1: When you need a backfill (markdown only)
# MAGIC
# MAGIC Five recurring scenarios:
# MAGIC
# MAGIC | Scenario | Example |
# MAGIC |---|---|
# MAGIC | Bug in transformation | Wrong denominator for a month |
# MAGIC | New column added retroactively | Populate `region` for last 90 days |
# MAGIC | Source corrected upstream | Re-ingest after Salesforce fix |
# MAGIC | Schema migration | `STRING amount` → `DECIMAL`, reprocess all |
# MAGIC | Late-arriving data | Day's events arrived 3 days late |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Anti-pattern — separate "fix" pipeline (markdown)
# MAGIC
# MAGIC One-off scripts diverge from the forward pipeline, lack tests,
# MAGIC observability, repeatability. The discipline: **one transform
# MAGIC function** for both forward and backfill.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: The discipline — a single `transform(date)`
# MAGIC
# MAGIC Build three functions:
# MAGIC
# MAGIC 1. **`read_source(date)`** — deterministic synthetic source. Same
# MAGIC    `date`, same rows. (Hint: derive a numeric seed from the date
# MAGIC    string so `rand(seed)` is stable per date.)
# MAGIC 2. **`compute(df_source)`** — pure transformation, no clock or
# MAGIC    randomness. Add `amount_with_tax = amount * 1.077` and
# MAGIC    `user_bucket = user_id % 10`.
# MAGIC 3. **`transform(date)`** — calls `read_source`, calls `compute`,
# MAGIC    writes via `replaceWhere` to TARGET_TABLE partitioned by
# MAGIC    `event_date`. Print the row count for that partition.
# MAGIC
# MAGIC Source schema: `event_date DATE, user_id INT, amount DOUBLE`.
# MAGIC Generate ~1000 rows per date.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: implement read_source, compute, transform")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Forward run — today's slice
# MAGIC
# MAGIC **Task:** call `transform("2026-04-30")` once.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: forward run for 2026-04-30")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Backfill loop — same code, many dates
# MAGIC
# MAGIC **Task:** call `transform()` for every date in
# MAGIC `pd.date_range("2026-04-01", "2026-04-15")`.
# MAGIC
# MAGIC `DESCRIBE HISTORY` afterward should show one Delta commit per
# MAGIC backfill day plus the original forward run.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: backfill loop over the date range")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: SELECT event_date, count(*) GROUP BY event_date — verify 16 partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Re-run safety — idempotency
# MAGIC
# MAGIC **Task A:** print row count for `event_date = '2026-04-10'`. Re-run
# MAGIC `transform("2026-04-10")`. Print row count again. They should be
# MAGIC identical.
# MAGIC
# MAGIC **Task B (optional):** override `read_source` to return a different
# MAGIC shape for one date (e.g. half the rows), call `transform("2026-04-10")`,
# MAGIC verify only that partition's count changed and other partitions are
# MAGIC untouched. This proves `replaceWhere` is partition-scoped.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: re-run idempotency proof")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Operational considerations (markdown)
# MAGIC
# MAGIC ### Resource isolation
# MAGIC Backfilling 2 years in parallel saturates the cluster, forward jobs
# MAGIC starve. Use dedicated job pools / capped concurrency
# MAGIC (`max_active_runs`, Job concurrency).
# MAGIC
# MAGIC ### Ordering
# MAGIC Backfilling backwards may produce briefly inconsistent downstream
# MAGIC views. Gate consumers behind an HWM until backfill completes.
# MAGIC
# MAGIC ### Schema drift over time
# MAGIC Historical raw data may have different schemas. Forward and
# MAGIC backfill must share a tolerant decoder (Auto Loader rescue mode,
# MAGIC explicit cast layer).
# MAGIC
# MAGIC ### Cost control
# MAGIC Backfills are full reads of history — estimate before running, use
# MAGIC spot instances, run in chunks not 5 years at once.
# MAGIC
# MAGIC ## Bigger picture
# MAGIC
# MAGIC Scalability requires **recomputation discipline**. The
# MAGIC `transform(date)` contract — idempotent + deterministic +
# MAGIC partition-scoped — is the smallest artifact that makes both
# MAGIC forward incremental runs and backfills safe.
