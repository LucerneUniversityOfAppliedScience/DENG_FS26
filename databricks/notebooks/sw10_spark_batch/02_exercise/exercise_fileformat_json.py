# Databricks notebook source

# MAGIC %md
# MAGIC # JSON to Medallion: Exercise
# MAGIC
# MAGIC In this exercise you will load a nested JSON file into a **Bronze** table (raw, as-is)
# MAGIC and then flatten it into a **Silver** table (clean, flat structure).
# MAGIC
# MAGIC ## Learning Goals
# MAGIC - Understand how Spark reads nested JSON
# MAGIC - Discover limitations of large nested JSON and learn about JSONL
# MAGIC - Build a Bronze/Silver medallion pipeline

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
# MAGIC ## Cleanup: drop existing tables
# MAGIC
# MAGIC Drop any existing Bronze/Silver tables before re-running so a stale schema
# MAGIC from a previous run does not block the overwrite (Delta refuses schema
# MAGIC changes on `overwrite` when Table ACLs are enabled).

# COMMAND ----------

for table in [BRONZE_TABLE, SILVER_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read the JSON file
# MAGIC
# MAGIC Use `spark.read.json()` to load the file and explore the structure with `.printSchema()`.

# COMMAND ----------

df_raw = spark.read.json(JSON_PATH)
df_raw.printSchema()

# COMMAND ----------

display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Fix the problem
# MAGIC
# MAGIC You should see an error like **"Results too large"**.
# MAGIC
# MAGIC **Questions:**
# MAGIC 1. How many rows does `df_raw` have? (Use `.count()`)
# MAGIC 2. Why is the result "too large" for Spark to display?
# MAGIC 3. Fix the problem so that each travel offer becomes its own row.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: fix the problem")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Write to Bronze (as-is)
# MAGIC
# MAGIC Save the exploded offers into the Bronze table. Each row contains one offer
# MAGIC with the full nested `Position` structure — no flattening yet.

# COMMAND ----------

# TODO: Write the exploded DataFrame to BRONZE_TABLE
# Hint: .write.mode("overwrite").saveAsTable(...)

# YOUR CODE HERE
raise NotImplementedError("Step 3: write to Bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: query the Bronze table
# MAGIC SELECT * FROM workspace.bronze.travel_offers_raw LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Flatten to Silver
# MAGIC
# MAGIC Read from the Bronze table and extract the nested fields into flat columns.
# MAGIC
# MAGIC **Fields to extract:**
# MAGIC
# MAGIC | Silver column | Source path |
# MAGIC |---|---|
# MAGIC | `hotel_name` | `offer.Position.Hotel.Name` |
# MAGIC | `hotel_class` | `offer.Position.Hotel.Class` |
# MAGIC | `lat` | `offer.Position.Hotel.Lat` |
# MAGIC | `lon` | `offer.Position.Hotel.Lon` |
# MAGIC | `country_name` | `offer.Position.Destination.Country.Name` |
# MAGIC | `region_name` | `offer.Position.Destination.Region.Name` |
# MAGIC | `location_name` | `offer.Position.Destination.Location.Name` |
# MAGIC | `price` | `offer.Position.Price.Value` |
# MAGIC | `currency` | `offer.Position.Price.Currency` |
# MAGIC | `duration_days` | `offer.Position.Duration.Value` |
# MAGIC | `departure_date` | `offer.Position.Package.DepartureDate.Value` |
# MAGIC | `tour_operator` | `offer.Position.Package.TourOperator.Name` |
# MAGIC | `meal_plan` | `offer.Position.Package.MealPlan.Name` |
# MAGIC | `rating` | `offer.Position.Package.Hotelrating.Rating` |
# MAGIC
# MAGIC **Hint:** Use `col("offer.Position.Hotel.Name").alias("hotel_name")` etc. For numeric
# MAGIC fields (`lat`, `lon`, `price`, `duration_days`, `rating`), use
# MAGIC `try_cast(col(...), "double")` instead of `.cast("double")` — the raw JSON has empty
# MAGIC strings in some rows, and plain `cast` fails under Serverless ANSI mode.

# COMMAND ----------

# TODO: Read from BRONZE_TABLE
# TODO: Select and rename the nested fields into flat columns
# TODO: Write to SILVER_TABLE

# YOUR CODE HERE
raise NotImplementedError("Step 4: flatten to Silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: query the Silver table
# MAGIC SELECT * FROM workspace.silver.travel_offers LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Explore the Silver table
# MAGIC
# MAGIC Write SQL queries to answer:
# MAGIC 1. How many offers are there in total?
# MAGIC 2. How many distinct hotels?
# MAGIC 3. What is the cheapest and most expensive offer?
# MAGIC 4. Which tour operator has the most offers?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Your queries here

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus: JSONL format
# MAGIC
# MAGIC The "Results too large" problem happened because the entire JSON file is **one big
# MAGIC nested object**. A better format for big data is **JSONL** (JSON Lines) — one JSON
# MAGIC object per line.
# MAGIC
# MAGIC **Task:** Read your Silver table and export it as a JSONL file to the Volume.
# MAGIC Then read it back with `spark.read.json()` (without `multiline`!) and verify it works.
# MAGIC
# MAGIC Hint: `df.write.mode("overwrite").json("/Volumes/...")`

# COMMAND ----------

# TODO (Bonus): Export Silver table as JSONL and read it back

# YOUR CODE HERE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.bronze.travel_offers_raw;
# MAGIC -- DROP TABLE IF EXISTS workspace.silver.travel_offers;
