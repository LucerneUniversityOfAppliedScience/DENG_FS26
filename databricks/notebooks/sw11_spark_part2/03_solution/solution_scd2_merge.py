# Databricks notebook source

# MAGIC %md
# MAGIC # Slowly Changing Dimensions (Type 2) with MERGE — Solution
# MAGIC
# MAGIC In this notebook you implement an **SCD Type 2 dimension** in Delta Lake
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
# MAGIC version that was current when the fact occurred, so historical reports
# MAGIC stay accurate.
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
# MAGIC `is_current` is redundant with `valid_to IS NULL`, but it's commonly used
# MAGIC because index-friendly equality filters are faster than NULL checks.
# MAGIC
# MAGIC `valid_to IS NULL` vs a far-future sentinel (`9999-12-31`): NULL is
# MAGIC clearer about the open-window semantics; a sentinel makes `BETWEEN`
# MAGIC queries simpler. Both schools have valid arguments — pick one and stick
# MAGIC to it across all your dimensions.
# MAGIC
# MAGIC ### The two SCD2 cases the MERGE must handle
# MAGIC
# MAGIC 1. **New natural key arrives** → insert one row, `is_current = true`
# MAGIC 2. **Existing natural key with changed attributes** → close the current
# MAGIC    row (`valid_to = now()`, `is_current = false`) AND insert a new row
# MAGIC    with the new values
# MAGIC
# MAGIC ### What Type 2 does NOT do
# MAGIC
# MAGIC - Audit logging — Type 2 captures what changed, not who changed it
# MAGIC - Hard deletes — handle with a soft-delete column (`is_deleted`) or a
# MAGIC   dedicated terminal version
# MAGIC
# MAGIC ## Read more
# MAGIC
# MAGIC - Wikipedia overview (covers all SCD types):
# MAGIC   `https://en.wikipedia.org/wiki/Slowly_changing_dimension`
# MAGIC - Kimball Group's dimensional-modeling techniques (the originator of the
# MAGIC   SCD typology). The site `kimballgroup.com` has one page per type
# MAGIC   under "Dimensional Modeling Techniques".
# MAGIC - Databricks docs on MERGE in Delta Lake (the building block we use):
# MAGIC   `https://docs.databricks.com/aws/en/delta/merge`
# MAGIC - dbt snapshots — same idea expressed in another tool, useful for
# MAGIC   cross-reference: `https://docs.getdbt.com/docs/build/snapshots`
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
# MAGIC We start from a small in-memory snapshot of country data — small enough
# MAGIC to read each row by eye when verifying the MERGE later. In a real
# MAGIC pipeline this would be the output of yesterday's ingestion, e.g. the
# MAGIC `bronze.countries_stream` Bronze table from the Auto Loader notebook.
# MAGIC
# MAGIC Add the SCD2 metadata columns at write-time:
# MAGIC - `valid_from = current_timestamp()` — opening of the validity window
# MAGIC - `valid_to = NULL` — open-ended
# MAGIC - `is_current = true`
# MAGIC - `country_sk = monotonically_increasing_id()` — internal join key
# MAGIC
# MAGIC `monotonically_increasing_id()` produces unique 64-bit values per Spark
# MAGIC partition, NOT a contiguous sequence. That's fine for surrogate keys —
# MAGIC they only need to be unique, not pretty.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

initial_data = [
    ("CH", "Switzerland",    756),
    ("DE", "Germany",        276),
    ("FR", "France",         250),
    ("GB", "United Kingdom", 826),
    ("IE", "Ireland",         372),
    ("JP", "Japan",          392),
    ("SG", "Singapore",      702),
]

df_initial = (spark
    .createDataFrame(initial_data, "country_id STRING, country_name STRING, iso_code INT")
    .withColumn("country_sk",  monotonically_increasing_id())
    .withColumn("valid_from",  current_timestamp())
    .withColumn("valid_to",    lit(None).cast("timestamp"))
    .withColumn("is_current",  lit(True))
    .select("country_sk", "country_id", "country_name", "iso_code",
            "valid_from", "valid_to", "is_current"))

df_initial.write.mode("overwrite").saveAsTable(DIM_TABLE)
print(f"Initial dimension: {spark.table(DIM_TABLE).count()} rows")
display(spark.table(DIM_TABLE).orderBy("country_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Every row has `is_current = true` and `valid_to = NULL`. This is the
# MAGIC starting state of any SCD2 dimension.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Simulate a change set
# MAGIC
# MAGIC Imagine the upstream system sent us today's full snapshot. Compared to
# MAGIC yesterday:
# MAGIC - `CH` was renamed `Switzerland` → `Swiss Confederation`
# MAGIC - `IT` is brand new
# MAGIC - All other countries are unchanged
# MAGIC
# MAGIC The dimension MERGE must handle both changes correctly: close the old
# MAGIC `CH` row, open a new one, and insert `IT` as a new key.

# COMMAND ----------

today_data = [
    ("CH", "Swiss Confederation", 756),  # renamed
    ("DE", "Germany",             276),  # unchanged
    ("FR", "France",              250),  # unchanged
    ("GB", "United Kingdom",      826),  # unchanged
    ("IE", "Ireland",             372),  # unchanged
    ("IT", "Italy",               380),  # new
    ("JP", "Japan",               392),  # unchanged
    ("SG", "Singapore",           702),  # unchanged
]

df_today = spark.createDataFrame(
    today_data, "country_id STRING, country_name STRING, iso_code INT"
)
df_today.createOrReplaceTempView("source_changes_today")

display(df_today)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: SCD2 MERGE — the two-statement pattern
# MAGIC
# MAGIC Spark's MERGE doesn't support inserting a new row **and** updating an
# MAGIC old row for the same source row in a single statement. Every other DWH
# MAGIC dialect calls this an SCD2 MERGE workaround, and the standard answer is
# MAGIC two statements:
# MAGIC
# MAGIC 1. **MERGE 1 — close changed rows**: for each source row whose key
# MAGIC    matches a current dimension row but whose attributes differ, set
# MAGIC    `valid_to = now()` and `is_current = false`.
# MAGIC 2. **INSERT 2 — open new versions and brand-new keys**: for every
# MAGIC    source row that is either new or has changed attributes, insert a
# MAGIC    fresh row with `is_current = true`.
# MAGIC
# MAGIC The two statements are idempotent together: re-running them on the same
# MAGIC source produces no further changes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Statement 1: close current rows whose attributes changed.
# MAGIC MERGE INTO workspace.silver.dim_country_scd2 AS tgt
# MAGIC USING source_changes_today AS src
# MAGIC ON tgt.country_id = src.country_id
# MAGIC    AND tgt.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC        tgt.country_name <> src.country_name OR
# MAGIC        tgt.iso_code     <> src.iso_code
# MAGIC      )
# MAGIC THEN UPDATE SET
# MAGIC        tgt.valid_to   = current_timestamp(),
# MAGIC        tgt.is_current = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Statement 2: insert new versions of changed rows AND brand-new keys.
# MAGIC INSERT INTO workspace.silver.dim_country_scd2
# MAGIC   (country_sk, country_id, country_name, iso_code, valid_from, valid_to, is_current)
# MAGIC SELECT
# MAGIC   monotonically_increasing_id() AS country_sk,
# MAGIC   src.country_id,
# MAGIC   src.country_name,
# MAGIC   src.iso_code,
# MAGIC   current_timestamp()           AS valid_from,
# MAGIC   CAST(NULL AS TIMESTAMP)       AS valid_to,
# MAGIC   true                          AS is_current
# MAGIC FROM source_changes_today src
# MAGIC LEFT JOIN workspace.silver.dim_country_scd2 tgt
# MAGIC        ON tgt.country_id = src.country_id
# MAGIC        AND tgt.is_current = true
# MAGIC WHERE tgt.country_id IS NULL                          -- new natural key
# MAGIC    OR tgt.country_name <> src.country_name            -- attribute change
# MAGIC    OR tgt.iso_code     <> src.iso_code;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Verify the SCD2 dimension
# MAGIC
# MAGIC Three queries that show the full picture:
# MAGIC 1. Current state — `is_current = true`
# MAGIC 2. Full history of one key — all versions of `CH`
# MAGIC 3. Historical query — what did the dimension look like as of a past
# MAGIC    timestamp?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1: current state of the dimension
# MAGIC SELECT * FROM workspace.silver.dim_country_scd2
# MAGIC WHERE is_current = true
# MAGIC ORDER BY country_id

# COMMAND ----------

# MAGIC %md
# MAGIC `CH` should now read `Swiss Confederation`, `IT` is present, all other
# MAGIC countries are unchanged.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: full history of CH — both rows should be visible
# MAGIC SELECT country_sk, country_id, country_name, valid_from, valid_to, is_current
# MAGIC FROM workspace.silver.dim_country_scd2
# MAGIC WHERE country_id = 'CH'
# MAGIC ORDER BY valid_from

# COMMAND ----------

# MAGIC %md
# MAGIC Two rows for `CH`: the old one with `is_current = false` and a populated
# MAGIC `valid_to`, and the new one with `is_current = true` and `valid_to = NULL`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q3: historical query — what did the dimension look like one minute ago?
# MAGIC --
# MAGIC -- This is a SCD2 historical query, NOT Delta time-travel.
# MAGIC -- Time-travel asks "what was in this table at version N?".
# MAGIC -- SCD2 historical asks "what was the state of business reality at time T?".
# MAGIC -- They answer different questions; on this dataset they happen to align,
# MAGIC -- but in general only the SCD2 query gives you accurate as-of joins
# MAGIC -- against fact tables.
# MAGIC
# MAGIC SELECT country_id, country_name, iso_code, valid_from, valid_to
# MAGIC FROM workspace.silver.dim_country_scd2
# MAGIC WHERE valid_from <= current_timestamp() - INTERVAL 1 MINUTE
# MAGIC   AND (valid_to > current_timestamp() - INTERVAL 1 MINUTE OR valid_to IS NULL)
# MAGIC ORDER BY country_id

# COMMAND ----------

# MAGIC %md
# MAGIC One minute ago, `CH` was still `Switzerland` and `IT` did not exist —
# MAGIC the historical query reflects that.
# MAGIC
# MAGIC sw10's `solution_delta.sql` has a Delta time-travel section. Re-read it
# MAGIC to understand the difference: Delta time-travel reverts the **storage**;
# MAGIC SCD2 historical queries reflect **business validity windows**.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Optional — feed from Auto Loader Bronze
# MAGIC
# MAGIC In production, the input isn't a static in-memory DataFrame — it's the
# MAGIC Bronze table produced by the Auto Loader notebook. The MERGE logic is
# MAGIC unchanged; only the source view differs.
# MAGIC
# MAGIC Uncomment the cell below if you've run `solution_autoloader.py` and want
# MAGIC to apply SCD2 MERGE on top of the streamed Bronze data. The MERGE
# MAGIC statements above don't need any change — only `source_changes_today`
# MAGIC needs to be redefined.

# COMMAND ----------

# # Uncomment to use Auto Loader Bronze as the source.
# (spark.table("workspace.bronze.countries_stream")
#     .select("country_id", "country_name", "iso_code")
#     .dropDuplicates(["country_id"])
#     .createOrReplaceTempView("source_changes_today"))
#
# # Re-run the two MERGE/INSERT statements from Step 3.
