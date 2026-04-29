# Databricks notebook source

# MAGIC %md
# MAGIC # XML to Medallion: Solution

# COMMAND ----------

XML_PATH = "/Volumes/workspace/raw/sample_data/xml/countries.xml"

print(f"XML file: {XML_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop existing tables
# MAGIC
# MAGIC Drop any existing Bronze/Silver tables before re-running so a stale schema
# MAGIC from a previous run does not block the overwrite (Delta refuses schema
# MAGIC changes on `overwrite` when Table ACLs are enabled).

# COMMAND ----------

for table in [
    "workspace.bronze.countries_raw",
    "workspace.silver.countries",
]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read XML into Bronze

# COMMAND ----------

df_countries_raw = (spark.read
    .format("xml")
    .option("rowTag", "country")
    .load(XML_PATH))

df_countries_raw.printSchema()

# COMMAND ----------

print(f"Number of countries: {df_countries_raw.count()}")

# COMMAND ----------

display(df_countries_raw.limit(5))

# COMMAND ----------

df_countries_raw.write.mode("overwrite").saveAsTable("workspace.bronze.countries_raw")
print("Bronze table written: workspace.bronze.countries_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.countries_raw LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Flatten to Silver

# COMMAND ----------

from pyspark.sql.functions import col

df_countries_silver = spark.table("workspace.bronze.countries_raw").select(
    col("id").cast("int").alias("id"),
    col("name"),
    col("iso3"),
    col("iso2"),
    col("capital"),
    col("region"),
    col("subregion"),
    col("latitude").cast("double").alias("latitude"),
    col("longitude").cast("double").alias("longitude"),
    col("currency"),
    col("currency_name"),
    col("phone_code"),
    col("tld"),
)

df_countries_silver.write.mode("overwrite").saveAsTable("workspace.silver.countries")
print("Silver table written: workspace.silver.countries")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.countries LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick check: countries per region
# MAGIC SELECT region, count(*) AS num_countries
# MAGIC FROM workspace.silver.countries
# MAGIC GROUP BY region
# MAGIC ORDER BY num_countries DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.bronze.countries_raw;
# MAGIC -- DROP TABLE IF EXISTS workspace.silver.countries;
