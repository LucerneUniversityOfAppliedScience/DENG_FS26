# Databricks notebook source

# MAGIC %md
# MAGIC # JSON to Medallion: Solution

# COMMAND ----------

BRONZE_TABLE = "workspace.bronze.travel_offers_raw"
SILVER_TABLE = "workspace.silver.travel_offers"
JSON_PATH = "/Volumes/workspace/raw/sample_data/json/travel_offers_cyprus_20260223_095026.json"

print(f"JSON source : {JSON_PATH}")
print(f"Bronze table: {BRONZE_TABLE}")
print(f"Silver table: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read the JSON file

# COMMAND ----------

df_raw = spark.read.json(JSON_PATH)
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Fix the problem
# MAGIC
# MAGIC `display(df_raw)` fails with **"Results too large"** because:
# MAGIC - The entire JSON file is read as **1 row** (one giant nested object)
# MAGIC - That single row contains the `root.Page` array with thousands of nested offers
# MAGIC - Spark tries to serialize this massive nested structure for display and exceeds the limit
# MAGIC
# MAGIC **Fix:** Use `explode()` to turn the `root.Page` array into individual rows.

# COMMAND ----------

print(f"Row count: {df_raw.count()}")  # → 1 row!

# COMMAND ----------

from pyspark.sql.functions import explode, col

df_exploded = df_raw.select(explode(col("root.Page")).alias("offer"))
print(f"Number of offers: {df_exploded.count():,}")
df_exploded.printSchema()

# COMMAND ----------

display(df_exploded.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Write to Bronze

# COMMAND ----------

df_exploded.write.mode("overwrite").saveAsTable(BRONZE_TABLE)
print(f"Bronze table written: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.travel_offers_raw LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Flatten to Silver

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)

df_silver = df_bronze.select(
    col("offer.Position.Hotel.Name").alias("hotel_name"),
    col("offer.Position.Hotel.Class").alias("hotel_class"),
    col("offer.Position.Hotel.Lat").cast("double").alias("lat"),
    col("offer.Position.Hotel.Lon").cast("double").alias("lon"),
    col("offer.Position.Destination.Country.Name").alias("country_name"),
    col("offer.Position.Destination.Region.Name").alias("region_name"),
    col("offer.Position.Destination.Location.Name").alias("location_name"),
    col("offer.Position.Price.Value").cast("double").alias("price"),
    col("offer.Position.Price.Currency").alias("currency"),
    col("offer.Position.Duration.Value").cast("int").alias("duration_days"),
    col("offer.Position.Package.DepartureDate.Value").alias("departure_date"),
    col("offer.Position.Package.TourOperator.Name").alias("tour_operator"),
    col("offer.Position.Package.MealPlan.Name").alias("meal_plan"),
    col("offer.Position.Package.Hotelrating.Rating").cast("double").alias("rating"),
)

df_silver.write.mode("overwrite").saveAsTable(SILVER_TABLE)
print(f"Silver table written: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.travel_offers LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Explore the Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Total offers
# MAGIC SELECT count(*) AS total_offers FROM workspace.silver.travel_offers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Distinct hotels
# MAGIC SELECT count(DISTINCT hotel_name) AS distinct_hotels FROM workspace.silver.travel_offers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Cheapest and most expensive offer
# MAGIC SELECT
# MAGIC   min(price) AS cheapest,
# MAGIC   max(price) AS most_expensive
# MAGIC FROM workspace.silver.travel_offers
# MAGIC WHERE price IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Tour operator with most offers
# MAGIC SELECT tour_operator, count(*) AS num_offers
# MAGIC FROM workspace.silver.travel_offers
# MAGIC GROUP BY tour_operator
# MAGIC ORDER BY num_offers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus: JSONL format
# MAGIC
# MAGIC JSONL (JSON Lines) = one JSON object per line. Spark can read this natively
# MAGIC without `multiline=true`, and each line becomes one row — no "Results too large" problem.

# COMMAND ----------

JSONL_PATH = "/Volumes/workspace/raw/sample_data/json/travel_offers.jsonl"

spark.table(SILVER_TABLE).write.mode("overwrite").json(JSONL_PATH)
print(f"JSONL written to: {JSONL_PATH}")

# COMMAND ----------

# Read back — no multiline needed!
df_jsonl = spark.read.json(JSONL_PATH)
print(f"Rows: {df_jsonl.count():,}")
display(df_jsonl.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.bronze.travel_offers_raw;
# MAGIC -- DROP TABLE IF EXISTS workspace.silver.travel_offers;

# COMMAND ----------

# Cleanup JSONL
# dbutils.fs.rm(JSONL_PATH, recurse=True)
