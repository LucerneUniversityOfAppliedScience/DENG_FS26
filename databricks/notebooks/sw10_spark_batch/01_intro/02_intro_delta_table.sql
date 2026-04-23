-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Delta Table Demo
-- MAGIC
-- MAGIC In this notebook we explore **Delta Lake** step by step — starting from plain Parquet files
-- MAGIC and building up to a fully managed Delta table in Unity Catalog.
-- MAGIC
-- MAGIC **What you will learn:**
-- MAGIC 1. Why plain Parquet files are **not enough** for data engineering (no UPDATE, DELETE, MERGE)
-- MAGIC 2. How Delta Lake adds a **transaction log** on top of Parquet to enable ACID operations
-- MAGIC 3. How to inspect Delta files directly on disk (the `_delta_log/` folder)
-- MAGIC 4. DML operations: UPDATE, DELETE, MERGE
-- MAGIC 5. Time travel, history, and restore
-- MAGIC 6. Schema evolution
-- MAGIC 7. Change Data Feed (CDF)
-- MAGIC 8. Optimizations: OPTIMIZE, Z-ORDER, VACUUM
-- MAGIC 9. Registering file-based Delta tables as **managed tables** in Unity Catalog
-- MAGIC
-- MAGIC **Key insight:** Delta Lake = Parquet files + JSON transaction log. That's it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1. Preparation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define paths — all data lives on a Unity Catalog Volume so students can browse the files
-- MAGIC vol_base     = "/Volumes/workspace/demo/data"
-- MAGIC parquet_path = f"{vol_base}/banking_parquet"
-- MAGIC delta_path   = f"{vol_base}/banking_delta"
-- MAGIC
-- MAGIC print(f"Volume base   : {vol_base}")
-- MAGIC print(f"Parquet path  : {parquet_path}")
-- MAGIC print(f"Delta path    : {delta_path}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Clean up from previous runs (so the demo is repeatable)
-- MAGIC import shutil, os
-- MAGIC for p in [parquet_path, delta_path]:
-- MAGIC     if os.path.exists(p):
-- MAGIC         shutil.rmtree(p)
-- MAGIC         print(f"Cleaned: {p}")
-- MAGIC
-- MAGIC # Also drop the managed table if it exists
-- MAGIC spark.sql("DROP TABLE IF EXISTS workspace.demo.banking")
-- MAGIC print("Ready.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 2. Create sample data as Parquet
-- MAGIC
-- MAGIC We start by writing banking transaction data as **plain Parquet** files to a Volume.
-- MAGIC Parquet is a columnar file format — great for analytics, but it has no transaction log
-- MAGIC and therefore **does not support UPDATE, DELETE, or MERGE**.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, DoubleType
-- MAGIC from datetime import datetime, date
-- MAGIC
-- MAGIC schema = StructType([
-- MAGIC     StructField("msg_id", StringType()),
-- MAGIC     StructField("msg_created", TimestampType()),
-- MAGIC     StructField("notification_id", StringType()),
-- MAGIC     StructField("notification_created", TimestampType()),
-- MAGIC     StructField("account_iban", StringType()),
-- MAGIC     StructField("account_owner", StringType()),
-- MAGIC     StructField("account_currency", StringType()),
-- MAGIC     StructField("bank_bic", StringType()),
-- MAGIC     StructField("bank_name", StringType()),
-- MAGIC     StructField("entry_amount", DoubleType()),
-- MAGIC     StructField("entry_currency", StringType()),
-- MAGIC     StructField("credit_debit_indicator", StringType()),
-- MAGIC     StructField("booking_date", DateType()),
-- MAGIC     StructField("value_date", DateType()),
-- MAGIC     StructField("tx_amount", DoubleType()),
-- MAGIC     StructField("tx_currency", StringType()),
-- MAGIC     StructField("creditor_name", StringType()),
-- MAGIC     StructField("debtor_name", StringType()),
-- MAGIC     StructField("creditor_iban", StringType()),
-- MAGIC     StructField("debtor_iban", StringType()),
-- MAGIC     StructField("remittance_info", StringType()),
-- MAGIC     StructField("end_to_end_id", StringType()),
-- MAGIC ])
-- MAGIC
-- MAGIC data = [
-- MAGIC     ("MSG001", datetime(2024,9,22,10,15), "NOTIF001", datetime(2024,9,22,10,16),
-- MAGIC      "CH93 0000 0000 0000 0001", "Altyca AG", "CHF", "UBSWCHZH80A", "UBS Switzerland AG",
-- MAGIC      1500.0, "CHF", "CRDT", date(2024,9,22), date(2024,9,22),
-- MAGIC      1500.0, "CHF", "Altyca AG", "Client Company Ltd",
-- MAGIC      "CH93 0000 0000 0000 0001", "DE89 3704 0044 0532 0130 00",
-- MAGIC      "Invoice payment INV-2024-001", "E2E-001-2024-09-22"),
-- MAGIC
-- MAGIC     ("MSG002", datetime(2024,9,21,14,30), "NOTIF002", datetime(2024,9,21,14,31),
-- MAGIC      "CH93 0000 0000 0000 0001", "Altyca AG", "CHF", "UBSWCHZH80A", "UBS Switzerland AG",
-- MAGIC      -750.0, "CHF", "DBIT", date(2024,9,21), date(2024,9,21),
-- MAGIC      -750.0, "CHF", "Office Supplies Inc", "Altyca AG",
-- MAGIC      "CH44 0900 0000 8754 2832", "CH93 0000 0000 0000 0001",
-- MAGIC      "Office equipment purchase", "E2E-002-2024-09-21"),
-- MAGIC
-- MAGIC     ("MSG003", datetime(2024,9,20,9,45), "NOTIF003", datetime(2024,9,20,9,46),
-- MAGIC      "CH93 0000 0000 0000 0001", "Altyca AG", "CHF", "UBSWCHZH80A", "UBS Switzerland AG",
-- MAGIC      2250.0, "CHF", "CRDT", date(2024,9,20), date(2024,9,20),
-- MAGIC      2250.0, "CHF", "Altyca AG", "Tech Solutions GmbH",
-- MAGIC      "CH93 0000 0000 0000 0001", "DE44 5001 0517 5407 3249 31",
-- MAGIC      "Consulting services Q3 2024", "E2E-003-2024-09-20"),
-- MAGIC
-- MAGIC     ("MSG004", datetime(2024,9,19,16,20), "NOTIF004", datetime(2024,9,19,16,21),
-- MAGIC      "CH44 0900 0000 8754 2832", "Stefan Koch", "CHF", "ZKBKCHZZ80A", "Zuercher Kantonalbank",
-- MAGIC      -320.50, "CHF", "DBIT", date(2024,9,19), date(2024,9,19),
-- MAGIC      -320.50, "CHF", "Migros", "Stefan Koch",
-- MAGIC      "CH18 0900 0000 3001 2345 6", "CH44 0900 0000 8754 2832",
-- MAGIC      "Grocery shopping", "E2E-004-2024-09-19"),
-- MAGIC
-- MAGIC     ("MSG005", datetime(2024,9,18,11,10), "NOTIF005", datetime(2024,9,18,11,11),
-- MAGIC      "CH44 0900 0000 8754 2832", "Stefan Koch", "CHF", "ZKBKCHZZ80A", "Zuercher Kantonalbank",
-- MAGIC      5000.0, "CHF", "CRDT", date(2024,9,18), date(2024,9,18),
-- MAGIC      5000.0, "CHF", "Stefan Koch", "Altyca AG",
-- MAGIC      "CH44 0900 0000 8754 2832", "CH93 0000 0000 0000 0001",
-- MAGIC      "Salary September 2024", "E2E-005-2024-09-18"),
-- MAGIC
-- MAGIC     ("MSG006", datetime(2024,9,17,13,45), "NOTIF006", datetime(2024,9,17,13,46),
-- MAGIC      "CH93 0000 0000 0000 0001", "Altyca AG", "EUR", "UBSWCHZH80A", "UBS Switzerland AG",
-- MAGIC      1200.0, "EUR", "CRDT", date(2024,9,17), date(2024,9,17),
-- MAGIC      1200.0, "EUR", "Altyca AG", "European Client SA",
-- MAGIC      "CH93 0000 0000 0000 0001", "FR14 2004 1010 0505 0001 3M02 606",
-- MAGIC      "Project delivery milestone 2", "E2E-006-2024-09-17"),
-- MAGIC
-- MAGIC     ("MSG007", datetime(2024,9,16,15,30), "NOTIF007", datetime(2024,9,16,15,31),
-- MAGIC      "CH93 0000 0000 0000 0001", "Altyca AG", "CHF", "UBSWCHZH80A", "UBS Switzerland AG",
-- MAGIC      -25.0, "CHF", "DBIT", date(2024,9,16), date(2024,9,16),
-- MAGIC      -25.0, "CHF", "UBS Switzerland AG", "Altyca AG",
-- MAGIC      "CH80 0024 0240 5917 4470 1", "CH93 0000 0000 0000 0001",
-- MAGIC      "International transfer fee", "E2E-007-2024-09-16"),
-- MAGIC
-- MAGIC     ("MSG008", datetime(2024,9,15,8,20), "NOTIF008", datetime(2024,9,15,8,21),
-- MAGIC      "CH44 0900 0000 8754 2832", "Stefan Koch", "CHF", "ZKBKCHZZ80A", "Zuercher Kantonalbank",
-- MAGIC      450.0, "CHF", "CRDT", date(2024,9,15), date(2024,9,15),
-- MAGIC      450.0, "CHF", "Stefan Koch", "Theater Escholzmatt",
-- MAGIC      "CH44 0900 0000 8754 2832", "CH21 0900 0000 1234 5678 9",
-- MAGIC      "Freelance web development", "E2E-008-2024-09-15"),
-- MAGIC ]
-- MAGIC
-- MAGIC df = spark.createDataFrame(data, schema)
-- MAGIC df.write.mode("overwrite").format("parquet").save(parquet_path)
-- MAGIC print(f"Wrote {df.count()} rows as Parquet to: {parquet_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inspect the Parquet files on disk

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(parquet_path))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Why are there multiple `.parquet` files instead of just one?**
-- MAGIC
-- MAGIC Spark is a **distributed** engine. When it writes data, each worker (partition) writes its own
-- MAGIC file **in parallel**. That's why you see files like `part-00000-...parquet`, `part-00001-...parquet`, etc.
-- MAGIC
-- MAGIC The `_SUCCESS` marker is written **after** all partitions have finished successfully.
-- MAGIC It signals to other systems that the write completed without errors.
-- MAGIC
-- MAGIC This is by design — parallel writes are fast, and downstream readers (Spark, Photon)
-- MAGIC can read all parts in parallel too. But there is **no transaction log** here, just raw files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Alternative: Save as a single file with Pandas
-- MAGIC
-- MAGIC Sometimes you need **exactly one file** — for example to share with a colleague,
-- MAGIC upload to a web tool, or feed into a non-Spark system.
-- MAGIC
-- MAGIC In that case, convert the Spark DataFrame to a **Pandas DataFrame** first, then write
-- MAGIC with `pandas.to_parquet()`. Pandas always writes a single file.
-- MAGIC
-- MAGIC | Approach | Files | Use case |
-- MAGIC |---|---|---|
-- MAGIC | `spark.write.parquet()` | Multiple (parallel) | Big data pipelines, data lakes |
-- MAGIC | `df.toPandas().to_parquet()` | Single file | Export, sharing, small datasets |
-- MAGIC
-- MAGIC > **Warning:** `.toPandas()` collects **all data into driver memory**. This works fine for
-- MAGIC > small datasets (< 1 GB), but will crash the driver on large datasets. For big data,
-- MAGIC > use `spark.write.parquet()` or `coalesce(1)`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert Spark DataFrame to Pandas and save as a single Parquet file
-- MAGIC single_file_path = f"{vol_base}/banking_single.parquet"
-- MAGIC
-- MAGIC pdf = df.toPandas()
-- MAGIC pdf.to_parquet(single_file_path, index=False)
-- MAGIC
-- MAGIC print(f"Single file written to: {single_file_path}")
-- MAGIC print(f"File size: {os.path.getsize(single_file_path):,} bytes")
-- MAGIC print(f"Rows: {len(pdf)}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Compare: Spark wrote a folder with multiple files, Pandas wrote one file
-- MAGIC import glob
-- MAGIC
-- MAGIC spark_files = [f for f in os.listdir(parquet_path) if f.endswith(".parquet")]
-- MAGIC print(f"Spark output  : {len(spark_files)} file(s) in folder '{parquet_path}'")
-- MAGIC print(f"Pandas output : 1 file '{single_file_path}'")
-- MAGIC print()
-- MAGIC print("Both contain the same data — the difference is how it's stored on disk.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 3. Query the Parquet data
-- MAGIC
-- MAGIC We can read Parquet files directly from the Volume path without registering a table.

-- COMMAND ----------

SELECT * FROM parquet.`/Volumes/workspace/demo/data/banking_parquet`

-- COMMAND ----------

SELECT
    account_iban, account_owner, tx_amount, remittance_info, booking_date, end_to_end_id
FROM parquet.`/Volumes/workspace/demo/data/banking_parquet`
WHERE
    end_to_end_id = 'E2E-003-2024-09-20'
    AND account_iban = 'CH93 0000 0000 0000 0001'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 4. Try to UPDATE Parquet — this will fail!
-- MAGIC
-- MAGIC Parquet files are **immutable** — they have no transaction log and therefore do not support
-- MAGIC `UPDATE`, `DELETE`, or `MERGE`.
-- MAGIC
-- MAGIC In **Unity Catalog**, Databricks enforces this even more strictly: you cannot run DML
-- MAGIC commands (`UPDATE`, `DELETE`, `MERGE`) on anything that is not a Delta table.
-- MAGIC
-- MAGIC Run the cell below and observe the error.

-- COMMAND ----------

-- This will FAIL with: [UC_COMMAND_NOT_SUPPORTED] UpdateTable not supported in Unity Catalog
UPDATE parquet.`/Volumes/workspace/demo/data/banking_parquet`
SET
    tx_amount = 2000.0,
    remittance_info = 'Updated: Consulting services Q3 2024 - Final payment'
WHERE
    end_to_end_id = 'E2E-003-2024-09-20'
    AND account_iban = 'CH93 0000 0000 0000 0001'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Why did this fail?**
-- MAGIC
-- MAGIC Parquet is a **file format**, not a **table format**. It stores data efficiently in columns,
-- MAGIC but it has no concept of transactions, versioning, or row-level changes.
-- MAGIC
-- MAGIC Unity Catalog only allows DML operations on **Delta tables**, because Delta provides:
-- MAGIC - A **transaction log** that tracks every change
-- MAGIC - **ACID guarantees** (atomicity, consistency, isolation, durability)
-- MAGIC - **Schema enforcement** so you can't accidentally corrupt the data
-- MAGIC
-- MAGIC Without Delta, the only way to "update" Parquet data would be to **read everything into memory,
-- MAGIC modify the rows, and rewrite the entire dataset** — slow, error-prone, and not atomic.
-- MAGIC
-- MAGIC This is exactly the problem that **Delta Lake** solves.
-- MAGIC
-- MAGIC But first, let's see how you **would** do it without Delta — the hard way.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The workaround: Read → Modify → Rewrite
-- MAGIC
-- MAGIC Without Delta, the only way to "update" Parquet data is to:
-- MAGIC 1. **Read** the entire dataset into a DataFrame
-- MAGIC 2. **Modify** the rows you want to change
-- MAGIC 3. **Overwrite** the entire dataset with the modified data
-- MAGIC
-- MAGIC This works, but it's **slow**, **not atomic** (readers see partial writes),
-- MAGIC and **error-prone** (a crash mid-write corrupts the data).

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import when, col, lit
-- MAGIC
-- MAGIC # Step 1: Read the entire Parquet dataset
-- MAGIC df_parquet = spark.read.parquet(parquet_path)
-- MAGIC print(f"Read {df_parquet.count()} rows from Parquet")
-- MAGIC
-- MAGIC # Step 2: Modify the rows we want to change
-- MAGIC df_updated = df_parquet.withColumn(
-- MAGIC     "tx_amount",
-- MAGIC     when(
-- MAGIC         (col("end_to_end_id") == "E2E-003-2024-09-20") & (col("account_iban") == "CH93 0000 0000 0000 0001"),
-- MAGIC         lit(2000.0)
-- MAGIC     ).otherwise(col("tx_amount"))
-- MAGIC ).withColumn(
-- MAGIC     "remittance_info",
-- MAGIC     when(
-- MAGIC         (col("end_to_end_id") == "E2E-003-2024-09-20") & (col("account_iban") == "CH93 0000 0000 0000 0001"),
-- MAGIC         lit("Updated: Consulting services Q3 2024 - Final payment")
-- MAGIC     ).otherwise(col("remittance_info"))
-- MAGIC )
-- MAGIC
-- MAGIC # Step 3: Overwrite the ENTIRE dataset
-- MAGIC df_updated.write.mode("overwrite").format("parquet").save(parquet_path)
-- MAGIC print("Overwrote all Parquet files — the 'update' is done.")

-- COMMAND ----------

-- Verify: the row has been changed
SELECT account_iban, account_owner, tx_amount, remittance_info, end_to_end_id
FROM parquet.`/Volumes/workspace/demo/data/banking_parquet`
WHERE end_to_end_id = 'E2E-003-2024-09-20'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **It works — but at what cost?**
-- MAGIC
-- MAGIC | Problem | Details |
-- MAGIC |---|---|
-- MAGIC | **Full rewrite** | We had to read and rewrite **all 8 rows**, even though we only changed 1 |
-- MAGIC | **Not atomic** | If the job crashes mid-write, readers see incomplete data |
-- MAGIC | **No history** | The old values are gone — there's no way to undo or audit the change |
-- MAGIC | **No concurrency** | Two writers at the same time will corrupt each other's output |
-- MAGIC
-- MAGIC With a real dataset of millions of rows, this approach is unacceptable.
-- MAGIC **Delta Lake solves all of these problems.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 5. Convert to Delta
-- MAGIC
-- MAGIC Delta Lake = **Parquet files + a JSON transaction log** (`_delta_log/`).
-- MAGIC
-- MAGIC The transaction log records every change as an atomic commit. This enables:
-- MAGIC - ACID transactions (UPDATE, DELETE, MERGE)
-- MAGIC - Time travel (query any previous version)
-- MAGIC - Schema enforcement and evolution
-- MAGIC - Audit history
-- MAGIC
-- MAGIC We create a Delta table on the Volume by reading from the Parquet files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read Parquet and write as Delta to the Volume
-- MAGIC df = spark.read.parquet(parquet_path)
-- MAGIC df.write.mode("overwrite").format("delta").save(delta_path)
-- MAGIC print(f"Converted {df.count()} rows to Delta at: {delta_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inspect the Delta files on disk
-- MAGIC
-- MAGIC Compare this to the Parquet folder above — notice the new `_delta_log/` directory!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(delta_path))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # The transaction log — one JSON file per commit (version)
-- MAGIC display(dbutils.fs.ls(f"{delta_path}/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read the first commit log — shows which files were added, the schema, etc.
-- MAGIC display(spark.read.json(f"{delta_path}/_delta_log/*.json"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 6. Query the Delta table
-- MAGIC
-- MAGIC We can query Delta files from the Volume path just like Parquet, but using `delta.` instead of `parquet.`

-- COMMAND ----------

SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 7. UPDATE — now it works!
-- MAGIC
-- MAGIC Because Delta has a transaction log, it can track which rows changed.
-- MAGIC Under the hood, Delta writes **new Parquet files** with the updated rows and records the
-- MAGIC change in the `_delta_log/`.

-- COMMAND ----------

UPDATE delta.`/Volumes/workspace/demo/data/banking_delta`
SET
    tx_amount = 2000.0,
    entry_amount = 2000.0,
    remittance_info = 'Updated: Consulting services Q3 2024 - Final payment'
WHERE
    end_to_end_id = 'E2E-003-2024-09-20'
    AND account_iban = 'CH93 0000 0000 0000 0001'

-- COMMAND ----------

-- Verify the update
SELECT account_iban, account_owner, tx_amount, remittance_info, end_to_end_id
FROM delta.`/Volumes/workspace/demo/data/banking_delta`
WHERE end_to_end_id = 'E2E-003-2024-09-20'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 8. DELETE
-- MAGIC
-- MAGIC Delta also supports deleting specific rows.
-- MAGIC Again — no full rewrite needed, just a new commit in the transaction log.

-- COMMAND ----------

SELECT COUNT(*) AS rows_before FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

DELETE FROM delta.`/Volumes/workspace/demo/data/banking_delta`
WHERE account_currency = 'EUR'

-- COMMAND ----------

SELECT COUNT(*) AS rows_after FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What happened on disk? — Deletion Vectors
-- MAGIC
-- MAGIC After a `DELETE`, Delta does **not** immediately rewrite the Parquet files to remove the deleted rows.
-- MAGIC Instead, it creates a **Deletion Vector** (DV) — a small file (`.bin`) that marks which rows
-- MAGIC in the existing Parquet file are "logically deleted".
-- MAGIC
-- MAGIC ```
-- MAGIC banking_delta/
-- MAGIC   part-00000-...parquet        ← still contains the deleted row physically
-- MAGIC   deletion_vector_...bin       ← marks which rows to skip when reading
-- MAGIC   _delta_log/
-- MAGIC     00000...00001.json         ← commit log references the DV
-- MAGIC ```
-- MAGIC
-- MAGIC **Why?** Rewriting large Parquet files just to remove a few rows would be expensive.
-- MAGIC Deletion Vectors make `DELETE` and `UPDATE` much faster because only a tiny bitmap
-- MAGIC needs to be written. The actual cleanup (removing the old data) happens later during `OPTIMIZE` or `VACUUM`.
-- MAGIC
-- MAGIC > Deletion Vectors are enabled by default on Databricks since Delta Lake 2.4+.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Inspect the Delta folder after DELETE — look for .bin files (Deletion Vectors)
-- MAGIC print("=== Delta folder after DELETE ===")
-- MAGIC for f in dbutils.fs.ls(delta_path):
-- MAGIC     if f.name.startswith("_"):
-- MAGIC         label = "(metadata)"
-- MAGIC     elif f.name.endswith(".bin"):
-- MAGIC         label = "← Deletion Vector!"
-- MAGIC     elif f.name.endswith(".parquet"):
-- MAGIC         label = "(data file)"
-- MAGIC     else:
-- MAGIC         label = ""
-- MAGIC     print(f"  {f.name:<55} {f.size:>8,} bytes  {label}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Look at the latest commit log to see the DV reference
-- MAGIC import json
-- MAGIC
-- MAGIC log_files = sorted([f.path for f in dbutils.fs.ls(f"{delta_path}/_delta_log") if f.name.endswith(".json")])
-- MAGIC latest_log = spark.read.text(log_files[-1])
-- MAGIC
-- MAGIC print(f"=== Latest commit: {log_files[-1].split('/')[-1]} ===")
-- MAGIC for row in latest_log.collect():
-- MAGIC     entry = json.loads(row.value)
-- MAGIC     if "remove" in entry:
-- MAGIC         print(f"  REMOVE: {entry['remove']['path'][:60]}...")
-- MAGIC     if "add" in entry:
-- MAGIC         path = entry["add"]["path"]
-- MAGIC         dv = entry["add"].get("deletionVector")
-- MAGIC         if dv:
-- MAGIC             print(f"  ADD (with DV): {path[:50]}...")
-- MAGIC             print(f"    → DV storage: {dv.get('storageType')}, size: {dv.get('sizeInBytes')} bytes, cardinality: {dv.get('cardinality')}")
-- MAGIC         else:
-- MAGIC             print(f"  ADD: {path[:60]}...")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 9. MERGE (Upsert)
-- MAGIC
-- MAGIC `MERGE` combines UPDATE and INSERT in a single atomic operation:
-- MAGIC - If a row with the same key **exists** → update it
-- MAGIC - If it **doesn't exist** → insert it
-- MAGIC
-- MAGIC This is the most powerful DML operation in Delta Lake.

-- COMMAND ----------

MERGE INTO delta.`/Volumes/workspace/demo/data/banking_delta` AS target
USING (
    SELECT * FROM (
        VALUES
            -- Update: change the amount for MSG001
            ('MSG001', TIMESTAMP'2024-09-22 10:15:00', 'NOTIF001', TIMESTAMP'2024-09-22 10:16:00',
             'CH93 0000 0000 0000 0001', 'Altyca AG', 'CHF', 'UBSWCHZH80A', 'UBS Switzerland AG',
             9999.0, 'CHF', 'CRDT', DATE'2024-09-22', DATE'2024-09-22',
             9999.0, 'CHF', 'Altyca AG', 'Client Company Ltd',
             'CH93 0000 0000 0000 0001', 'DE89 3704 0044 0532 0130 00',
             'MERGED: Updated amount', 'E2E-001-2024-09-22'),
            -- Insert: brand new transaction
            ('MSG009', TIMESTAMP'2024-09-23 09:00:00', 'NOTIF009', TIMESTAMP'2024-09-23 09:01:00',
             'CH93 0000 0000 0000 0001', 'Altyca AG', 'CHF', 'UBSWCHZH80A', 'UBS Switzerland AG',
             3500.0, 'CHF', 'CRDT', DATE'2024-09-23', DATE'2024-09-23',
             3500.0, 'CHF', 'Altyca AG', 'New Client GmbH',
             'CH93 0000 0000 0000 0001', 'AT61 1904 3002 3457 3201',
             'MERGED: New project deposit', 'E2E-009-2024-09-23')
    ) AS t(msg_id, msg_created, notification_id, notification_created,
           account_iban, account_owner, account_currency, bank_bic, bank_name,
           entry_amount, entry_currency, credit_debit_indicator, booking_date, value_date,
           tx_amount, tx_currency, creditor_name, debtor_name,
           creditor_iban, debtor_iban, remittance_info, end_to_end_id)
) AS source
ON target.msg_id = source.msg_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- Verify: MSG001 should be updated, MSG009 should be new
SELECT msg_id, account_owner, tx_amount, remittance_info
FROM delta.`/Volumes/workspace/demo/data/banking_delta`
ORDER BY msg_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 10. Show history
-- MAGIC
-- MAGIC Every DML operation creates a new **version** in the transaction log.
-- MAGIC `DESCRIBE HISTORY` shows the full audit trail.

-- COMMAND ----------

DESCRIBE HISTORY delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 11. Time travel by version
-- MAGIC
-- MAGIC Delta keeps all previous versions of the data (as long as the files haven't been vacuumed).
-- MAGIC You can query any historical version using `VERSION AS OF`.

-- COMMAND ----------

-- Version 0: the original data right after conversion from Parquet
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta` VERSION AS OF 0

-- COMMAND ----------

-- Version 1: after the UPDATE
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta` VERSION AS OF 1

-- COMMAND ----------

-- What changed between version 0 and the current version?
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta`
EXCEPT ALL
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta` VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 12. Time travel by timestamp
-- MAGIC
-- MAGIC You can also query data as it was at a specific point in time.

-- COMMAND ----------

-- Query the data as it was 10 minutes ago
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta`
TIMESTAMP AS OF current_timestamp() - INTERVAL 10 MINUTES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 13. Audit trail — diff between versions
-- MAGIC
-- MAGIC Using `EXCEPT` you can see exactly which rows changed between any two versions.

-- COMMAND ----------

-- Rows added or changed in version 1 compared to version 0
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta` VERSION AS OF 1
EXCEPT
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta` VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 14. Restore table to an earlier version
-- MAGIC
-- MAGIC `RESTORE` resets the table to a previous version. This creates a **new version** in the
-- MAGIC history (it doesn't delete the intermediate versions).

-- COMMAND ----------

RESTORE TABLE delta.`/Volumes/workspace/demo/data/banking_delta` TO VERSION AS OF 0

-- COMMAND ----------

-- The table now has the original data again
SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- But history still shows everything — including the restore operation
DESCRIBE HISTORY delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 15. Schema evolution
-- MAGIC
-- MAGIC Delta tables support adding new columns without rewriting existing data.
-- MAGIC Old rows will have `NULL` in the new column. You can also set a default value for future inserts.

-- COMMAND ----------

ALTER TABLE delta.`/Volumes/workspace/demo/data/banking_delta`
ADD COLUMN transaction_category STRING

-- COMMAND ----------

-- Old rows have NULL in the new column
SELECT msg_id, account_owner, transaction_category
FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- Populate the new column
UPDATE delta.`/Volumes/workspace/demo/data/banking_delta`
SET transaction_category = CASE
    WHEN remittance_info LIKE '%Salary%'  THEN 'Payroll'
    WHEN remittance_info LIKE '%Invoice%' THEN 'Revenue'
    WHEN remittance_info LIKE '%office%' OR remittance_info LIKE '%Office%' THEN 'Office Expenses'
    WHEN remittance_info LIKE '%fee%'     THEN 'Banking Fees'
    ELSE 'General'
END

-- COMMAND ----------

SELECT msg_id, account_owner, tx_amount, transaction_category, remittance_info
FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 16. Change Data Feed (CDF)
-- MAGIC
-- MAGIC CDF tracks **row-level changes** (insert, update, delete) so downstream systems
-- MAGIC can process only what changed instead of re-reading the full table.
-- MAGIC
-- MAGIC This is the foundation for **incremental data pipelines**.

-- COMMAND ----------

ALTER TABLE delta.`/Volumes/workspace/demo/data/banking_delta`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- Make some changes after enabling CDF
UPDATE delta.`/Volumes/workspace/demo/data/banking_delta`
SET tx_amount = tx_amount + 100
WHERE msg_id = 'MSG005'

-- COMMAND ----------

INSERT INTO delta.`/Volumes/workspace/demo/data/banking_delta`
VALUES ('MSG010', TIMESTAMP'2024-09-24 12:00:00', 'NOTIF010', TIMESTAMP'2024-09-24 12:01:00',
        'CH93 0000 0000 0000 0001', 'Altyca AG', 'CHF', 'UBSWCHZH80A', 'UBS Switzerland AG',
        800.0, 'CHF', 'CRDT', DATE'2024-09-24', DATE'2024-09-24',
        800.0, 'CHF', 'Altyca AG', 'Freelancer XY',
        'CH93 0000 0000 0000 0001', 'CH12 3456 7890 1234 5678 9',
        'CDF demo: new transaction', 'E2E-010-2024-09-24', 'General')

-- COMMAND ----------

-- Read the change feed — shows exactly what was inserted, updated (pre/post image), or deleted
-- Note: table_changes() does not support path-based Delta tables, so we read it via PySpark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # table_changes() requires a registered table name, not a file path.
-- MAGIC # For path-based Delta tables, we use spark.read with the "readChangeFeed" option.
-- MAGIC #
-- MAGIC # IMPORTANT: CDF only records changes AFTER it was enabled.
-- MAGIC # We find the version where CDF was enabled and start reading from there.
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC history = DeltaTable.forPath(spark, delta_path).history()
-- MAGIC cdf_version = (history
-- MAGIC     .filter("operation = 'SET TBLPROPERTIES'")
-- MAGIC     .select("version")
-- MAGIC     .orderBy("version")
-- MAGIC     .first()["version"])
-- MAGIC
-- MAGIC print(f"CDF was enabled at version {cdf_version} — reading changes from there.")
-- MAGIC
-- MAGIC cdf_df = (spark.read.format("delta")
-- MAGIC     .option("readChangeFeed", "true")
-- MAGIC     .option("startingVersion", cdf_version)
-- MAGIC     .load(delta_path))
-- MAGIC
-- MAGIC cdf_df.select("_change_type", "_commit_version", "_commit_timestamp",
-- MAGIC               "msg_id", "tx_amount", "remittance_info") \
-- MAGIC       .orderBy("_commit_version", "_change_type") \
-- MAGIC       .display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Where does CDF store its data?
-- MAGIC
-- MAGIC After enabling CDF and making changes, Delta creates a `_change_data/` subfolder
-- MAGIC alongside the regular Parquet files and the `_delta_log/`.
-- MAGIC
-- MAGIC ```
-- MAGIC banking_delta/
-- MAGIC   _delta_log/           ← transaction log (JSON commits)
-- MAGIC   _change_data/         ← CDF records (pre/post images of changed rows)
-- MAGIC   part-00000-...parquet ← data files
-- MAGIC   part-00001-...parquet
-- MAGIC   ...
-- MAGIC ```
-- MAGIC
-- MAGIC The `_change_data/` files are **separate from the main data** — they only store the
-- MAGIC rows that changed (with `_change_type` = `insert`, `update_preimage`, `update_postimage`, `delete`).
-- MAGIC This keeps the overhead small.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Top-level contents of the Delta folder — notice _change_data/ alongside _delta_log/
-- MAGIC print("=== Delta folder contents ===")
-- MAGIC for f in dbutils.fs.ls(delta_path):
-- MAGIC     print(f"  {f.name:<40} {f.size / 1024:>8.1f} KB")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # What's inside _change_data/?
-- MAGIC print("=== _change_data/ contents ===")
-- MAGIC for f in dbutils.fs.ls(f"{delta_path}/_change_data"):
-- MAGIC     print(f"  {f.name:<60} {f.size / 1024:>6.1f} KB")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # And for comparison: the transaction log
-- MAGIC print("=== _delta_log/ contents ===")
-- MAGIC for f in dbutils.fs.ls(f"{delta_path}/_delta_log"):
-- MAGIC     print(f"  {f.name:<40} {f.size / 1024:>6.1f} KB")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 17. Optimizations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inspect files before optimization
-- MAGIC
-- MAGIC After many DML operations, Delta tables accumulate many small Parquet files.
-- MAGIC This is called the **small file problem** — too many files means too many I/O operations,
-- MAGIC which slows down queries.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # How many Parquet files are in the Delta folder now?
-- MAGIC files = [f for f in dbutils.fs.ls(delta_path) if f.name.endswith(".parquet")]
-- MAGIC print(f"Number of Parquet files: {len(files)}")
-- MAGIC for f in files:
-- MAGIC     print(f"  {f.name}  ({f.size / 1024:.1f} KB)")

-- COMMAND ----------

DESCRIBE DETAIL delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### OPTIMIZE — file compaction
-- MAGIC
-- MAGIC `OPTIMIZE` merges many small files into fewer, larger files.
-- MAGIC This dramatically improves read performance.

-- COMMAND ----------

OPTIMIZE delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Check files after OPTIMIZE
-- MAGIC files = [f for f in dbutils.fs.ls(delta_path) if f.name.endswith(".parquet")]
-- MAGIC print(f"Number of Parquet files after OPTIMIZE: {len(files)}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Z-ORDER
-- MAGIC
-- MAGIC Z-ordering co-locates rows with similar values in the same files.
-- MAGIC When a query filters on a Z-ordered column, the engine can **skip entire files**
-- MAGIC by reading min/max statistics from the file footer.
-- MAGIC
-- MAGIC Choose columns that are frequently used in `WHERE` clauses.

-- COMMAND ----------

OPTIMIZE delta.`/Volumes/workspace/demo/data/banking_delta`
ZORDER BY (account_owner, booking_date)

-- COMMAND ----------

DESCRIBE DETAIL delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 18. VACUUM — cleanup old files
-- MAGIC
-- MAGIC After OPTIMIZE, old small files are no longer referenced by the current version
-- MAGIC but still sit on disk. `VACUUM` deletes these **orphaned files**.
-- MAGIC
-- MAGIC The default retention is **168 hours (7 days)** — this keeps files around long enough
-- MAGIC for running queries and time travel to still work.

-- COMMAND ----------

DESCRIBE DETAIL delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

VACUUM delta.`/Volumes/workspace/demo/data/banking_delta` RETAIN 168 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aggressive VACUUM (development only!)
-- MAGIC
-- MAGIC Setting retention to 0 hours removes **all** old files immediately.
-- MAGIC
-- MAGIC **Risks in production:**
-- MAGIC 1. **Time travel is destroyed** — you can no longer query previous versions
-- MAGIC 2. **Running queries may fail** — a job reading an older snapshot gets `FileNotFoundException`
-- MAGIC 3. The 7-day default exists to give ongoing jobs a safe window

-- COMMAND ----------

-- Override the retention at table level so VACUUM with 0 HOURS is allowed
ALTER TABLE delta.`/Volumes/workspace/demo/data/banking_delta`
SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 0 hours')

-- COMMAND ----------

VACUUM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- Restore the safe default
ALTER TABLE delta.`/Volumes/workspace/demo/data/banking_delta`
SET TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 168 hours')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # After vacuum — only the active files remain
-- MAGIC files = [f for f in dbutils.fs.ls(delta_path) if f.name.endswith(".parquet")]
-- MAGIC print(f"Parquet files after VACUUM: {len(files)}")

-- COMMAND ----------

DESCRIBE HISTORY delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 19. Register as Managed Table in Unity Catalog
-- MAGIC
-- MAGIC So far we worked with **file-based Delta** on a Volume — great for learning and seeing the files.
-- MAGIC
-- MAGIC In production, you typically register Delta tables in **Unity Catalog** as **managed tables**.
-- MAGIC This gives you:
-- MAGIC - A clean table name (`workspace.demo.banking`) instead of a file path
-- MAGIC - Centralized access control and governance
-- MAGIC - Discoverability via the Data Explorer
-- MAGIC - Databricks manages the file lifecycle for you
-- MAGIC
-- MAGIC Managed tables also store their data as Delta (Parquet + `_delta_log/`) — the same structure
-- MAGIC we explored above. You can see the storage location via `DESCRIBE DETAIL`. However, on the
-- MAGIC **Databricks Free Edition** (Community Edition) the underlying cloud storage path is not
-- MAGIC directly browsable — the files are managed by Databricks and hidden from the user.
-- MAGIC That's one reason we used a **Volume** throughout this demo: it lets us inspect every file.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.demo.banking
AS SELECT * FROM delta.`/Volumes/workspace/demo/data/banking_delta`

-- COMMAND ----------

-- Now you can query by table name — no file paths needed
SELECT * FROM workspace.demo.banking

-- COMMAND ----------

-- The table is managed by Unity Catalog
DESCRIBE DETAIL workspace.demo.banking

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 20. Cleanup

-- COMMAND ----------

DROP TABLE IF EXISTS workspace.demo.banking

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Remove the Volume files
-- MAGIC import shutil
-- MAGIC single_file = f"{vol_base}/banking_single.parquet"
-- MAGIC for p in [parquet_path, delta_path]:
-- MAGIC     if os.path.exists(p):
-- MAGIC         shutil.rmtree(p)
-- MAGIC         print(f"Deleted: {p}")
-- MAGIC if os.path.exists(single_file):
-- MAGIC     os.remove(single_file)
-- MAGIC     print(f"Deleted: {single_file}")
-- MAGIC print("Cleanup complete.")
