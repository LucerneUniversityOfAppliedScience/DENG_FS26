# Databricks notebook source

# MAGIC %md
# MAGIC # NYC Yellow Taxi Analysis: Exercise
# MAGIC
# MAGIC Analyze the 2025 NYC yellow taxi trip data (12 monthly parquet files) with PySpark.
# MAGIC The tasks build on each other: load → explore → clean → aggregate → visualize.
# MAGIC
# MAGIC ## Learning Goals
# MAGIC - Load and combine many parquet files at once
# MAGIC - Explore a large dataset (schema, row count, date range)
# MAGIC - Filter out implausible values
# MAGIC - Aggregate along time dimensions (month, weekday, hour)
# MAGIC - Join with reference tables
# MAGIC - Visualize results with matplotlib

# COMMAND ----------

NYC_PATH = "/Volumes/workspace/nyc_taxi/raw_files/NYC/"
VENDOR_TABLE = "workspace.nyc_taxi.vendor_list"
PAYMENT_TABLE = "workspace.nyc_taxi.payment_types"

print(f"Data source: {NYC_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Load all 12 parquet files into a single DataFrame
# MAGIC
# MAGIC Use `spark.read.parquet()` with a glob pattern (e.g. `"{NYC_PATH}*.parquet"`) so that
# MAGIC all 12 monthly files are combined into one DataFrame. Print the schema at the end.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: load all parquet files into df")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Basic exploration
# MAGIC
# MAGIC Answer with code:
# MAGIC 1. How many rows does `df` have?
# MAGIC 2. How many columns?
# MAGIC 3. What are the earliest and latest values of `tpep_pickup_datetime`?
# MAGIC
# MAGIC Hint: `pyspark.sql.functions.min` / `max` (import them with aliases because `min` / `max`
# MAGIC are also Python built-ins).

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: row count, col count, date range")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Null values per column
# MAGIC
# MAGIC For each column in `df`, count how many `NULL` values it has. Display the result.
# MAGIC
# MAGIC Hint: Use a list comprehension with `count(when(col(c).isNull(), c)).alias(c)`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: null count per column")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Build a cleaned DataFrame `df_clean`
# MAGIC
# MAGIC Keep only trips with plausible values:
# MAGIC - `trip_distance` between 0 (exclusive) and 100 miles
# MAGIC - `fare_amount` between 0 (exclusive) and 500 USD
# MAGIC - `passenger_count` between 1 and 6
# MAGIC - `tpep_pickup_datetime` within the year 2025
# MAGIC
# MAGIC Print how many rows were removed.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: build df_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Trips per month
# MAGIC
# MAGIC Group `df_clean` by pickup month and count trips. Then plot as a bar chart with
# MAGIC matplotlib (x = month 1..12, y = trip count).
# MAGIC
# MAGIC Hint: `pyspark.sql.functions.month(...)`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: trips per month + bar chart")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Trips per day of the week
# MAGIC
# MAGIC Group by `dayofweek(tpep_pickup_datetime)` and count. Plot as a bar chart ordered
# MAGIC Mon–Sun.
# MAGIC
# MAGIC Hint: Spark's `dayofweek` returns 1=Sunday, 2=Monday, …, 7=Saturday — reorder
# MAGIC accordingly.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: trips per weekday + bar chart")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Trips per hour of the day
# MAGIC
# MAGIC Group by `hour(tpep_pickup_datetime)` and count. Plot as a line chart (x = 0..23).

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 7: trips per hour + line chart")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Financial summary
# MAGIC
# MAGIC From `df_clean`, compute in one DataFrame:
# MAGIC - Total revenue (sum of `total_amount`)
# MAGIC - Average `fare_amount`
# MAGIC - Average `tip_amount`
# MAGIC - Average `trip_distance`
# MAGIC
# MAGIC Round all to 2 decimals.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 8: financial summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Fare amount distribution
# MAGIC
# MAGIC Plot a histogram of `fare_amount` for trips with `fare_amount < 100`. To keep the
# MAGIC plotting cheap, draw a 5% sample (`df.sample(fraction=0.05, seed=42)`) before
# MAGIC converting to pandas.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 9: fare histogram")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 10: Tip percentage by payment type
# MAGIC
# MAGIC Join `df_clean` with `workspace.nyc_taxi.payment_types` on `payment_type`. Compute
# MAGIC the average tip as a percentage of the fare (`tip_amount / fare_amount * 100`),
# MAGIC grouped by `payment_description`. Plot as a bar chart.
# MAGIC
# MAGIC Filter out rows with `fare_amount <= 0` before dividing.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 10: tip percentage by payment type")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 11: Top 10 pickup locations
# MAGIC
# MAGIC Group by `PULocationID`, count, and keep the top 10. Plot as a bar chart.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 11: top 10 pickup locations")
