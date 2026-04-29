# Databricks notebook source

# MAGIC %md
# MAGIC # Slowly Changing Dimensions (Type 2) with MERGE — Exercise
# MAGIC
# MAGIC In this exercise you implement an **SCD Type 2 dimension** in Delta Lake
# MAGIC using the classic two-statement MERGE pattern.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC When dimension attributes change over time, you have a choice:
# MAGIC overwrite the old value (Type 1) or keep the full history (Type 2).
# MAGIC Type 1 is fine for typo fixes; Type 2 is the right answer when historical
# MAGIC fact rows must continue to link to the dimension state that was current
# MAGIC at fact-time. Without Type 2, last year's revenue report silently changes
# MAGIC every time a customer moves house.
# MAGIC
# MAGIC ## What is SCD Type 2?
# MAGIC
# MAGIC SCD Type 2 is a dimensional-modeling technique. Instead of overwriting
# MAGIC changed attributes, the dimension table keeps every version of every
# MAGIC entity, marking each row with a validity window. Fact rows join to the
# MAGIC version that was current when the fact occurred.
# MAGIC
# MAGIC ### The standard column set
# MAGIC
# MAGIC | Column | Purpose |
# MAGIC |---|---|
# MAGIC | `country_sk` | Surrogate key — internal monotonic ID, the actual join key |
# MAGIC | `country_id` | Natural key from the source — multiple rows over time |
# MAGIC | `country_name`, `iso_code`, ... | Business attributes that change |
# MAGIC | `valid_from` | Timestamp the row became current |
# MAGIC | `valid_to` | Timestamp the row stopped being current; NULL means open |
# MAGIC | `is_current` | Boolean, true exactly once per natural key |
# MAGIC
# MAGIC `is_current` is redundant with `valid_to IS NULL`, but it's commonly
# MAGIC used because index-friendly equality filters are faster than NULL
# MAGIC checks. `valid_to IS NULL` vs a far-future sentinel (`9999-12-31`):
# MAGIC NULL is clearer about open-window semantics; a sentinel makes `BETWEEN`
# MAGIC simpler. Pick one and stick with it.
# MAGIC
# MAGIC ### The two SCD2 cases the MERGE must handle
# MAGIC
# MAGIC 1. **New natural key** → insert one row, `is_current = true`
# MAGIC 2. **Existing natural key with changed attributes** → close the current
# MAGIC    row AND insert a new row
# MAGIC
# MAGIC ## Read more
# MAGIC
# MAGIC - Wikipedia overview: `https://en.wikipedia.org/wiki/Slowly_changing_dimension`
# MAGIC - Kimball Group's "Dimensional Modeling Techniques" (originator of the
# MAGIC   SCD typology) — see `kimballgroup.com`
# MAGIC - Databricks docs on MERGE: `https://docs.databricks.com/aws/en/delta/merge`
# MAGIC - dbt snapshots (same idea in another tool): `https://docs.getdbt.com/docs/build/snapshots`
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume in the UC
# MAGIC bundle. If you cloned the repo or pulled new changes, you must
# MAGIC **redeploy the bundle** before running any sw11 notebook — even ones
# MAGIC that don't need the new volume — so your workspace's bundle state
# MAGIC matches the repo. In the bundle UI, click **Deploy** once.

# COMMAND ----------

DIM_TABLE = "workspace.silver.dim_country_scd2"

print(f"Dimension table: {DIM_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop the dimension table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {DIM_TABLE}")
print(f"Dropped (if existed): {DIM_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Build the initial dimension
# MAGIC
# MAGIC Build a small in-memory DataFrame with the first snapshot of country
# MAGIC data. Add the SCD2 metadata columns at write-time:
# MAGIC - `valid_from = current_timestamp()`
# MAGIC - `valid_to = NULL` (cast to timestamp)
# MAGIC - `is_current = true`
# MAGIC - `country_sk = monotonically_increasing_id()`
# MAGIC
# MAGIC `monotonically_increasing_id()` produces unique 64-bit IDs (per
# MAGIC partition) — fine for surrogate keys, which need uniqueness, not
# MAGIC contiguity.
# MAGIC
# MAGIC Use this initial data:
# MAGIC ```
# MAGIC ("CH", "Switzerland",    756),
# MAGIC ("DE", "Germany",        276),
# MAGIC ("FR", "France",         250),
# MAGIC ("GB", "United Kingdom", 826),
# MAGIC ("IE", "Ireland",         372),
# MAGIC ("JP", "Japan",          392),
# MAGIC ("SG", "Singapore",      702),
# MAGIC ```
# MAGIC
# MAGIC Save to `DIM_TABLE` with `mode("overwrite")`. Display the result.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: build the initial SCD2 dimension")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Simulate a change set
# MAGIC
# MAGIC Build a "today" DataFrame representing the next-day snapshot. Compared to
# MAGIC the initial state:
# MAGIC - `CH` was renamed `Switzerland` → `Swiss Confederation`
# MAGIC - `IT` is brand new
# MAGIC - All others unchanged
# MAGIC
# MAGIC Register it as a temp view named `source_changes_today` so the SQL
# MAGIC MERGE in the next step can reference it.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: build today's source view")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: SCD2 MERGE — the two-statement pattern
# MAGIC
# MAGIC Spark's MERGE doesn't support inserting a new row **and** updating an
# MAGIC old row for the same source row in a single statement. The standard
# MAGIC SCD2 workaround is two statements:
# MAGIC
# MAGIC 1. **MERGE 1** — close current rows whose attributes changed:
# MAGIC    `WHEN MATCHED AND <attrs differ> THEN UPDATE SET valid_to=now(),
# MAGIC    is_current=false`. Match on `tgt.country_id = src.country_id AND
# MAGIC    tgt.is_current = true` so only the open row per natural key is
# MAGIC    affected.
# MAGIC 2. **INSERT 2** — insert new versions and brand-new keys:
# MAGIC    `INSERT INTO ... SELECT ... FROM source LEFT JOIN dim ON
# MAGIC    tgt.country_id = src.country_id AND tgt.is_current = true WHERE
# MAGIC    <new key OR attrs differ>`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Statement 1 — close current rows whose attributes changed.
# MAGIC -- MERGE INTO ... USING source_changes_today ...
# MAGIC -- ON tgt.country_id = src.country_id AND tgt.is_current = true
# MAGIC -- WHEN MATCHED AND (... attrs differ ...) THEN UPDATE SET valid_to, is_current

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Statement 2 — insert new versions and brand-new keys.
# MAGIC -- INSERT INTO ... SELECT monotonically_increasing_id(), ...
# MAGIC -- FROM source_changes_today src
# MAGIC -- LEFT JOIN dim tgt ON tgt.country_id = src.country_id AND tgt.is_current = true
# MAGIC -- WHERE tgt.country_id IS NULL OR <attrs differ>

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Verify the SCD2 dimension
# MAGIC
# MAGIC Three queries:
# MAGIC 1. Current state — `is_current = true`
# MAGIC 2. Full history of `CH` — should return two rows (old closed, new open)
# MAGIC 3. Historical query — "as of one minute ago" using
# MAGIC    `valid_from <= ts AND (valid_to > ts OR valid_to IS NULL)`. This is
# MAGIC    a SCD2 historical query, NOT Delta time-travel. They answer different
# MAGIC    questions — see sw10's `solution_delta.sql` time-travel section for
# MAGIC    contrast.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Q1 — current state of the dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Q2 — full history of CH ordered by valid_from

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Q3 — historical query for "one minute ago" using current_timestamp() - INTERVAL 1 MINUTE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Optional — feed from Auto Loader Bronze
# MAGIC
# MAGIC In production, the input isn't a static in-memory DataFrame — it's the
# MAGIC Bronze table produced by the Auto Loader notebook. The MERGE logic is
# MAGIC unchanged; only the source view differs.
# MAGIC
# MAGIC If you've run `exercise_autoloader.py`, swap `source_changes_today` to
# MAGIC `bronze.countries_stream`:
# MAGIC
# MAGIC ```python
# MAGIC (spark.table("workspace.bronze.countries_stream")
# MAGIC     .select("country_id", "country_name", "iso_code")
# MAGIC     .dropDuplicates(["country_id"])
# MAGIC     .createOrReplaceTempView("source_changes_today"))
# MAGIC ```
# MAGIC
# MAGIC Then re-run the two MERGE/INSERT statements from Step 3. Same logic,
# MAGIC different source.
