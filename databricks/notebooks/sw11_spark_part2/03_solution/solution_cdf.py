# Databricks notebook source

# MAGIC %md
# MAGIC # Delta Change Data Feed (CDF) — Solution
# MAGIC
# MAGIC In this notebook you learn how to use **Delta Change Data Feed** to
# MAGIC propagate changes (inserts, updates, deletes) from a Silver table
# MAGIC down to a Gold table without re-reading the whole Silver snapshot.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Three approaches to keeping a downstream table in sync with an
# MAGIC upstream one:
# MAGIC
# MAGIC | Strategy | What it reads downstream | Catches deletes? |
# MAGIC |---|---|---|
# MAGIC | Full snapshot | Everything every run | Yes (by absence) |
# MAGIC | `WHERE updated_at > :hwm` | Just the recently-modified rows | **No** — hard deletes are invisible |
# MAGIC | **CDF** | Just the change events since last commit | **Yes** — explicit `_change_type = 'delete'` |
# MAGIC
# MAGIC CDF is the only option that catches **hard deletes** without a full
# MAGIC reconciliation pass. That makes it the right primitive for any
# MAGIC pipeline that needs to mirror a source faithfully.
# MAGIC
# MAGIC ## The four `_change_type` values (slide 51)
# MAGIC
# MAGIC | Value | Meaning | What you usually do |
# MAGIC |---|---|---|
# MAGIC | `insert` | New row | INSERT into target |
# MAGIC | `update_postimage` | Row after update | UPDATE SET * |
# MAGIC | `update_preimage` | Row before update | drop — diagnostics only |
# MAGIC | `delete` | Row was deleted | DELETE matching key |
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume **and** a
# MAGIC new `meta` schema. Redeploy the bundle once if you haven't yet.

# COMMAND ----------

CATALOG = "workspace"

SILVER_TABLE  = f"{CATALOG}.silver.customers_cdf"
GOLD_TABLE    = f"{CATALOG}.gold.customers_synced"
META_TABLE    = f"{CATALOG}.meta.cdf_state"
PIPELINE_NAME = "customers_silver_to_gold"

print(f"Silver  : {SILVER_TABLE}")
print(f"Gold    : {GOLD_TABLE}")
print(f"Meta    : {META_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop all three tables

# COMMAND ----------

for table in [SILVER_TABLE, GOLD_TABLE, META_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Why CDF? (markdown)
# MAGIC
# MAGIC Without CDF, the canonical "where updated_at > hwm" pattern from the
# MAGIC HWM notebook silently **misses hard deletes**. If row 42 is deleted
# MAGIC upstream, no `updated_at` change occurs, the row is just gone, and
# MAGIC downstream filters never see it.
# MAGIC
# MAGIC CDF emits an explicit `_change_type = 'delete'` event for every row
# MAGIC that was deleted, so downstream pipelines can `DELETE` the matching
# MAGIC key. It also emits `update_preimage` / `update_postimage` pairs for
# MAGIC every UPDATE, giving you both the before and after state.
# MAGIC
# MAGIC One caveat (covered at the end): CDF only contains changes **after
# MAGIC enabling**. The initial snapshot still needs a full read.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Create the Silver table and enable CDF

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.silver.customers_cdf (
# MAGIC     id          INT,
# MAGIC     name        STRING,
# MAGIC     email       STRING,
# MAGIC     country     STRING
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC `delta.enableChangeDataFeed = true` is the magic switch. Once set,
# MAGIC every subsequent commit on this table emits change events that can
# MAGIC be read via the `table_changes(...)` function.
# MAGIC
# MAGIC The property can also be enabled on an existing table with
# MAGIC `ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Initial load
# MAGIC
# MAGIC Insert four rows representing today's customer master data. This is
# MAGIC the snapshot the Gold sink will consume to bootstrap.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.silver.customers_cdf VALUES
# MAGIC   (1, 'Alice',   'alice@example.com',   'CH'),
# MAGIC   (2, 'Bob',     'bob@example.com',     'DE'),
# MAGIC   (3, 'Charlie', 'charlie@example.com', 'FR'),
# MAGIC   (4, 'Diana',   'diana@example.com',   'IT');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.customers_cdf ORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One commit so far (the INSERT). DESCRIBE HISTORY shows it.
# MAGIC DESCRIBE HISTORY workspace.silver.customers_cdf

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Build the initial Gold snapshot
# MAGIC
# MAGIC The first time downstream runs, it has no CDF state yet, so it
# MAGIC reads the **full Silver snapshot**. Subsequent runs will read from
# MAGIC CDF.
# MAGIC
# MAGIC We also create the metadata table that tracks the last consumed
# MAGIC CDF version per consumer pipeline.

# COMMAND ----------

# Initial Gold snapshot
(spark.table(SILVER_TABLE)
    .write
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE))

# Metadata table for CDF state
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {META_TABLE} (
        pipeline           STRING,
        last_cdf_version   BIGINT,
        updated_at         TIMESTAMP
    ) USING DELTA
""")

# Record the version we just consumed
current_version = (spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}")
    .orderBy("version", ascending=False)
    .first()["version"])
spark.sql(f"""
    INSERT INTO {META_TABLE}
    SELECT '{PIPELINE_NAME}', {current_version}, current_timestamp()
    WHERE NOT EXISTS (SELECT 1 FROM {META_TABLE} WHERE pipeline = '{PIPELINE_NAME}')
""")

print(f"Gold initial snapshot rows: {spark.table(GOLD_TABLE).count()}")
display(spark.table(META_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Make changes upstream
# MAGIC
# MAGIC Three commits on Silver: an INSERT, an UPDATE, a DELETE. Each one
# MAGIC will emit different `_change_type` events.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Commit: INSERT 2 new customers
# MAGIC INSERT INTO workspace.silver.customers_cdf VALUES
# MAGIC   (5, 'Eve',  'eve@example.com',  'ES'),
# MAGIC   (6, 'Finn', 'finn@example.com', 'IE');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Commit: UPDATE Charlie's email
# MAGIC UPDATE workspace.silver.customers_cdf
# MAGIC    SET email = 'charlie.new@example.com'
# MAGIC  WHERE id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Commit: DELETE Bob
# MAGIC DELETE FROM workspace.silver.customers_cdf WHERE id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.silver.customers_cdf

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE HISTORY` should now show four versions: the INSERT (initial),
# MAGIC the second INSERT (Eve + Finn), the UPDATE (Charlie), and the DELETE
# MAGIC (Bob).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Read the changes
# MAGIC
# MAGIC `table_changes(table, start_version)` returns one row per change
# MAGIC event since the start version. The free metadata columns
# MAGIC (`_change_type`, `_commit_version`, `_commit_timestamp`) come for
# MAGIC every row.

# COMMAND ----------

last_consumed = (spark.table(META_TABLE)
    .filter(f"pipeline = '{PIPELINE_NAME}'")
    .first()["last_cdf_version"])
print(f"Last consumed CDF version: {last_consumed}")

# Read changes after that version
df_changes = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_consumed + 1) \
    .table(SILVER_TABLE)

display(df_changes.orderBy("_commit_version", "_change_type", "id"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see:
# MAGIC - 2× `_change_type = 'insert'` (Eve, Finn)
# MAGIC - 1× `_change_type = 'update_preimage'` (Charlie's old email)
# MAGIC - 1× `_change_type = 'update_postimage'` (Charlie's new email)
# MAGIC - 1× `_change_type = 'delete'` (Bob)
# MAGIC
# MAGIC Six events total for three logical changes — every UPDATE gets a
# MAGIC pre/post pair so consumers can choose what they care about.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Apply changes downstream
# MAGIC
# MAGIC Standard CDF apply pattern (slide 51):
# MAGIC - Drop `update_preimage` rows (diagnostics only)
# MAGIC - `INSERT` and `update_postimage` go through MERGE as upserts
# MAGIC - `delete` goes through MERGE as a key-matched delete
# MAGIC
# MAGIC One MERGE per branch keeps the SQL straightforward.

# COMMAND ----------

# Branch 1: upserts (insert + update_postimage)
upserts = df_changes.where("_change_type IN ('insert', 'update_postimage')")
upserts.createOrReplaceTempView("cdf_upserts")

spark.sql(f"""
    MERGE INTO {GOLD_TABLE} t
    USING cdf_upserts s
       ON t.id = s.id
    WHEN MATCHED      THEN UPDATE SET *
    WHEN NOT MATCHED  THEN INSERT *
""")

# Branch 2: deletes
deletes = df_changes.where("_change_type = 'delete'")
deletes.createOrReplaceTempView("cdf_deletes")

spark.sql(f"""
    MERGE INTO {GOLD_TABLE} t
    USING cdf_deletes s
       ON t.id = s.id
    WHEN MATCHED THEN DELETE
""")

print("Gold table after CDF apply:")
display(spark.table(GOLD_TABLE).orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC The Gold table should now have 5 rows:
# MAGIC - Alice (1), unchanged
# MAGIC - Charlie (3) with the new email
# MAGIC - Diana (4), unchanged
# MAGIC - Eve (5), Finn (6), inserted
# MAGIC - **Bob (2) is gone** — caught by the `delete` event

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Commit the new CDF watermark
# MAGIC
# MAGIC Same ordering rule as the HWM notebook: bookkeeping comes **after**
# MAGIC the sink write succeeds.

# COMMAND ----------

new_version = spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE}").orderBy("version", ascending=False).first()["version"]

spark.sql(f"""
    UPDATE {META_TABLE}
       SET last_cdf_version = {new_version},
           updated_at       = current_timestamp()
     WHERE pipeline = '{PIPELINE_NAME}'
""")

display(spark.table(META_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Idempotency proof — re-run with no upstream changes
# MAGIC
# MAGIC Read changes from the now-current CDF version. Result: zero rows.

# COMMAND ----------

last_consumed = spark.table(META_TABLE).filter(f"pipeline = '{PIPELINE_NAME}'").first()["last_cdf_version"]
print(f"Now consuming from version > {last_consumed}")

# table_changes raises if the start version is past the latest
try:
    df_idempotent = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_consumed + 1) \
        .table(SILVER_TABLE)
    n = df_idempotent.count()
    print(f"New change events: {n}")
except Exception as e:
    # If startingVersion exceeds the table's history, Delta raises.
    # Practically: catch and treat as zero events.
    print(f"No new versions yet ({type(e).__name__}). Treat as 0 events.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 10: Caveats
# MAGIC
# MAGIC ### CDF only captures changes after enabling
# MAGIC
# MAGIC If you enable CDF on an existing table that already has 1M rows,
# MAGIC those 1M rows are **not** in the CDF history. The first downstream
# MAGIC consumer still needs a full snapshot read to bootstrap, then can
# MAGIC switch to CDF for incremental updates after that. The Gold snapshot
# MAGIC build in Step 4 is exactly this bootstrap step.
# MAGIC
# MAGIC ### CDF retention follows VACUUM
# MAGIC
# MAGIC Change events are stored alongside the data files. When `VACUUM`
# MAGIC removes old data files past the retention window
# MAGIC (default 7 days), the CDF events for those versions are also gone.
# MAGIC If your downstream consumer falls behind by more than the VACUUM
# MAGIC retention, you have to re-bootstrap from a full snapshot.
# MAGIC
# MAGIC ### Combining with `WITH SCHEMA EVOLUTION`
# MAGIC
# MAGIC If Silver gains a new column mid-pipeline, the MERGE statements
# MAGIC need `MERGE WITH SCHEMA EVOLUTION` to propagate the new column to
# MAGIC Gold without manual `ALTER TABLE`. See slide 48 for the syntax.
