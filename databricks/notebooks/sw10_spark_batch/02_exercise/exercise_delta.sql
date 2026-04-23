-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Delta Tables: Exercise
-- MAGIC
-- MAGIC In this exercise you will work with a **managed Delta table** in Unity Catalog
-- MAGIC using the NYC Yellow Taxi 2025 data.
-- MAGIC
-- MAGIC The table has already been created for you in the setup below.
-- MAGIC Your job starts at **Task 1**.
-- MAGIC
-- MAGIC ## Learning Goals
-- MAGIC - Inspect managed Delta tables (DESCRIBE DETAIL, DESCRIBE HISTORY)
-- MAGIC - Use time travel to query previous versions
-- MAGIC - Perform UPDATE, DELETE, and MERGE operations
-- MAGIC - Optimize a table with compaction and Z-ordering
-- MAGIC - Vacuum old files
-- MAGIC - Schema evolution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Setup
-- MAGIC
-- MAGIC The following cells create the table and load two months of NYC Taxi data.
-- MAGIC **Just run them — no changes needed.**

-- COMMAND ----------

-- Clean slate
DROP TABLE IF EXISTS workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC
-- MAGIC RAW_DIR = "/Volumes/workspace/nyc_taxi/raw_files/NYC"
-- MAGIC TABLE_NAME = "workspace.demo.yellow_taxi_exercise"
-- MAGIC
-- MAGIC COLUMNS = [
-- MAGIC     "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
-- MAGIC     "passenger_count", "trip_distance", "PULocationID", "DOLocationID",
-- MAGIC     "payment_type", "fare_amount", "tip_amount", "total_amount"
-- MAGIC ]
-- MAGIC
-- MAGIC files = sorted([f.path for f in dbutils.fs.ls(RAW_DIR) if f.name.endswith(".parquet")])
-- MAGIC if not files:
-- MAGIC     raise FileNotFoundError(f"No Parquet files in {RAW_DIR}. Upload the NYC parquet files to the volume first.")
-- MAGIC
-- MAGIC # Load January (150k rows)
-- MAGIC df_jan = (spark.read.parquet(files[0])
-- MAGIC     .select(COLUMNS)
-- MAGIC     .withColumn("source_month", lit("January"))
-- MAGIC     .limit(150_000))
-- MAGIC
-- MAGIC df_jan.write.format("delta").saveAsTable(TABLE_NAME)
-- MAGIC print(f"Created {TABLE_NAME} with {spark.table(TABLE_NAME).count():,} rows (January)")
-- MAGIC
-- MAGIC # Append February (150k rows)
-- MAGIC if len(files) >= 2:
-- MAGIC     df_feb = (spark.read.parquet(files[1])
-- MAGIC         .select(COLUMNS)
-- MAGIC         .withColumn("source_month", lit("February"))
-- MAGIC         .limit(150_000))
-- MAGIC else:
-- MAGIC     df_feb = (spark.read.parquet(files[0])
-- MAGIC         .select(COLUMNS)
-- MAGIC         .withColumn("source_month", lit("February (simulated)"))
-- MAGIC         .limit(50_000))
-- MAGIC
-- MAGIC df_feb.write.mode("append").format("delta").saveAsTable(TABLE_NAME)
-- MAGIC print(f"Appended February. Total rows: {spark.table(TABLE_NAME).count():,}")

-- COMMAND ----------

SELECT count(*) AS total_rows FROM workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 1 – Inspect the Table
-- MAGIC
-- MAGIC Use `DESCRIBE DETAIL` and `DESCRIBE HISTORY` to understand what was created.
-- MAGIC
-- MAGIC **Questions (answer below):**
-- MAGIC 1. Where is the table stored? (look at the `location` field)
-- MAGIC 2. How many versions exist after setup?
-- MAGIC 3. What operations were performed?

-- COMMAND ----------

-- TODO: Inspect the table
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Your answers:**
-- MAGIC 1. …
-- MAGIC 2. …
-- MAGIC 3. …

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 2 – Time Travel
-- MAGIC
-- MAGIC Query the table at **version 0** (January only) and compare to the current version.
-- MAGIC
-- MAGIC - Count rows for version 0 and the current version
-- MAGIC - Confirm version 0 contains only January data

-- COMMAND ----------

-- TODO: Compare row counts and source_month values across versions
-- Hint: SELECT count(*) FROM workspace.demo.yellow_taxi_exercise VERSION AS OF 0
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 3 – UPDATE
-- MAGIC
-- MAGIC Increase the `fare_amount` by 15% for all trips where `VendorID = 1` and `source_month = 'January'`.
-- MAGIC
-- MAGIC Then verify the change by comparing the average fare before and after (use time travel!).

-- COMMAND ----------

-- TODO: Run the UPDATE
-- YOUR CODE HERE

-- COMMAND ----------

-- TODO: Compare avg(fare_amount) before vs after for VendorID = 1, source_month = 'January'
-- Hint: use VERSION AS OF for the previous version
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 4 – DELETE
-- MAGIC
-- MAGIC Delete all trips with `trip_distance = 0` (likely invalid records).
-- MAGIC
-- MAGIC - Count how many rows match **before** deleting
-- MAGIC - Run the DELETE
-- MAGIC - Verify the new total row count

-- COMMAND ----------

-- TODO: Count, delete, verify
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 5 – MERGE (Upsert)
-- MAGIC
-- MAGIC Create a temp view `corrections` and merge it into the table.
-- MAGIC
-- MAGIC The corrections view should have 2 rows:
-- MAGIC 1. One row that **matches** an existing trip (same `VendorID` + `tpep_pickup_datetime`) → update `fare_amount` to `999.99`
-- MAGIC 2. One row with `VendorID = 999` (new) → should be inserted
-- MAGIC
-- MAGIC Then verify both the update and insert happened.

-- COMMAND ----------

-- TODO: Create temp view and MERGE
-- Hint: CREATE OR REPLACE TEMP VIEW corrections AS SELECT ...
-- Then: MERGE INTO ... USING corrections ON ... WHEN MATCHED ... WHEN NOT MATCHED ...
-- YOUR CODE HERE

-- COMMAND ----------

-- TODO: Verify — query for VendorID = 999 or fare_amount = 999.99
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 6 – Compact & Z-Order
-- MAGIC
-- MAGIC 1. Run `OPTIMIZE` on the table
-- MAGIC 2. Run `OPTIMIZE ... ZORDER BY (PULocationID)`

-- COMMAND ----------

-- TODO: OPTIMIZE and ZORDER
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Why does Z-ordering on `PULocationID` help? (your explanation):**
-- MAGIC
-- MAGIC …

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 7 – Vacuum
-- MAGIC
-- MAGIC 1. Run `DESCRIBE DETAIL` to see `numFiles` before
-- MAGIC 2. Run `VACUUM` with default retention (168 hours)

-- COMMAND ----------

-- TODO: DESCRIBE DETAIL, then VACUUM
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Why is the default retention 168 hours? What risks come with 0 hours? (your explanation):**
-- MAGIC
-- MAGIC …

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 8 – History
-- MAGIC
-- MAGIC Show the full history of the table.
-- MAGIC How many versions are there? What operation does each represent?

-- COMMAND ----------

-- TODO: DESCRIBE HISTORY
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 9 – Schema Evolution
-- MAGIC
-- MAGIC Add a new column `congestion_zone` to the table.
-- MAGIC
-- MAGIC 1. `ALTER TABLE ... ADD COLUMN congestion_zone STRING`
-- MAGIC 2. Populate: `'Midtown'` where `PULocationID IN (161, 162, 163, 230, 237, 186)`, otherwise `'Other'`
-- MAGIC 3. Verify: count rows per `congestion_zone`

-- COMMAND ----------

-- TODO: Add column, populate, verify
-- YOUR CODE HERE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Cleanup

-- COMMAND ----------

-- DROP TABLE IF EXISTS workspace.demo.yellow_taxi_exercise
