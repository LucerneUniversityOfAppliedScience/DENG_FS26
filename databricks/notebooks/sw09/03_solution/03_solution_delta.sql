-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Delta Tables: Exercise Solution

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Setup

-- COMMAND ----------

DROP TABLE IF EXISTS workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC
-- MAGIC RAW_DIR = "/Volumes/workspace/raw/files/NYC"
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
-- MAGIC     raise FileNotFoundError(f"No Parquet files in {RAW_DIR}. Run copy_sample_data job first.")
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

-- COMMAND ----------

DESCRIBE DETAIL workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

DESCRIBE HISTORY workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Answers:**
-- MAGIC 1. The storage location is managed by Unity Catalog (a cloud storage path not directly browsable on Free Edition)
-- MAGIC 2. Two versions exist: version 0 (CREATE TABLE) and version 1 (WRITE/append)
-- MAGIC 3. Operations: CREATE TABLE AS SELECT, then WRITE (append)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 2 – Time Travel

-- COMMAND ----------

SELECT 'Version 0' AS version, count(*) AS rows
FROM workspace.demo.yellow_taxi_exercise VERSION AS OF 0
UNION ALL
SELECT 'Current' AS version, count(*) AS rows
FROM workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

-- Confirm version 0 is January only
SELECT DISTINCT source_month
FROM workspace.demo.yellow_taxi_exercise VERSION AS OF 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 3 – UPDATE

-- COMMAND ----------

-- Average fare before update
SELECT round(avg(fare_amount), 2) AS avg_fare_before
FROM workspace.demo.yellow_taxi_exercise
WHERE VendorID = 1 AND source_month = 'January'

-- COMMAND ----------

UPDATE workspace.demo.yellow_taxi_exercise
SET fare_amount = fare_amount * 1.15
WHERE VendorID = 1 AND source_month = 'January'

-- COMMAND ----------

-- Compare: before (previous version) vs after (current)
SELECT 'Before' AS state, round(avg(fare_amount), 2) AS avg_fare
FROM workspace.demo.yellow_taxi_exercise VERSION AS OF 1
WHERE VendorID = 1 AND source_month = 'January'
UNION ALL
SELECT 'After' AS state, round(avg(fare_amount), 2) AS avg_fare
FROM workspace.demo.yellow_taxi_exercise
WHERE VendorID = 1 AND source_month = 'January'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 4 – DELETE

-- COMMAND ----------

SELECT count(*) AS zero_distance_rows
FROM workspace.demo.yellow_taxi_exercise
WHERE trip_distance = 0

-- COMMAND ----------

DELETE FROM workspace.demo.yellow_taxi_exercise
WHERE trip_distance = 0

-- COMMAND ----------

SELECT count(*) AS rows_after_delete
FROM workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 5 – MERGE (Upsert)

-- COMMAND ----------

-- Get a real pickup datetime to use for the matching row
SELECT VendorID, tpep_pickup_datetime, fare_amount
FROM workspace.demo.yellow_taxi_exercise
WHERE VendorID = 1
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW corrections AS
-- Matched row: take real pickup/dropoff timestamps from the table, override fare
SELECT
    1                AS VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    1.0              AS passenger_count,
    5.0              AS trip_distance,
    161              AS PULocationID,
    237              AS DOLocationID,
    1                AS payment_type,
    999.99           AS fare_amount,
    50.0             AS tip_amount,
    1099.99          AS total_amount,
    'Correction'     AS source_month
FROM (
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime,
           ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime) AS rn
    FROM workspace.demo.yellow_taxi_exercise
    WHERE VendorID = 1
) AS t
WHERE t.rn = 1
UNION ALL
-- New row: will be inserted (VendorID 999 does not exist)
SELECT
    999                                    AS VendorID,
    CAST('2025-01-15 12:00:00' AS TIMESTAMP) AS tpep_pickup_datetime,
    CAST('2025-01-15 12:30:00' AS TIMESTAMP) AS tpep_dropoff_datetime,
    2.0                                    AS passenger_count,
    10.0                                   AS trip_distance,
    100                                    AS PULocationID,
    200                                    AS DOLocationID,
    1                                      AS payment_type,
    55.00                                  AS fare_amount,
    10.0                                   AS tip_amount,
    65.00                                  AS total_amount,
    'New vendor'                           AS source_month

-- COMMAND ----------

MERGE INTO workspace.demo.yellow_taxi_exercise AS target
USING corrections AS source
ON target.VendorID = source.VendorID
   AND target.tpep_pickup_datetime = source.tpep_pickup_datetime
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- Verify: the correction and the new vendor row
SELECT VendorID, fare_amount, source_month
FROM workspace.demo.yellow_taxi_exercise
WHERE VendorID = 999 OR fare_amount = 999.99
ORDER BY VendorID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 6 – Compact & Z-Order

-- COMMAND ----------

OPTIMIZE workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

OPTIMIZE workspace.demo.yellow_taxi_exercise
ZORDER BY (PULocationID)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Why Z-order helps:**
-- MAGIC
-- MAGIC Z-ordering sorts rows so that records with the same `PULocationID` end up in the same
-- MAGIC Parquet files. When a query filters on `WHERE PULocationID = 161`, the engine reads
-- MAGIC min/max statistics from each file's footer and **skips entire files** that don't contain
-- MAGIC that value. Without Z-ordering, rows for zone 161 are scattered across all files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 7 – Vacuum

-- COMMAND ----------

DESCRIBE DETAIL workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

VACUUM workspace.demo.yellow_taxi_exercise RETAIN 168 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Why 168 hours?**
-- MAGIC
-- MAGIC The 7-day default gives running queries and time travel a safe window.
-- MAGIC With 0 hours: time travel is destroyed, and long-running queries may fail with
-- MAGIC `FileNotFoundException` because the files they reference have been deleted.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 8 – History

-- COMMAND ----------

DESCRIBE HISTORY workspace.demo.yellow_taxi_exercise

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Task 9 – Schema Evolution

-- COMMAND ----------

ALTER TABLE workspace.demo.yellow_taxi_exercise
ADD COLUMN congestion_zone STRING

-- COMMAND ----------

UPDATE workspace.demo.yellow_taxi_exercise
SET congestion_zone = CASE
    WHEN PULocationID IN (161, 162, 163, 230, 237, 186) THEN 'Midtown'
    ELSE 'Other'
END

-- COMMAND ----------

SELECT congestion_zone, count(*) AS rows
FROM workspace.demo.yellow_taxi_exercise
GROUP BY congestion_zone
ORDER BY congestion_zone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## Cleanup

-- COMMAND ----------

-- DROP TABLE IF EXISTS workspace.demo.yellow_taxi_exercise
