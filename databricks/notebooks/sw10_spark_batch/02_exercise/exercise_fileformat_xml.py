# Databricks notebook source

# MAGIC %md
# MAGIC # XML to Medallion: Exercise
# MAGIC
# MAGIC In this exercise you will load an **XML** file into a Bronze table,
# MAGIC then flatten it into a Silver table.
# MAGIC
# MAGIC ## Learning Goals
# MAGIC - Read XML files with Spark
# MAGIC - Apply the Bronze/Silver medallion pattern

# COMMAND ----------

XML_PATH = "/Volumes/workspace/raw/sample_data/xml/countries.xml"

print(f"XML file: {XML_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read XML into Bronze
# MAGIC
# MAGIC Use `spark.read.format("xml")` to read the file. You need to specify
# MAGIC the `rowTag` option — this tells Spark which XML element represents one row.
# MAGIC
# MAGIC Look at the XML structure: `<countries>` → `<country>` → fields...
# MAGIC
# MAGIC 1. Read the XML file with the correct `rowTag`
# MAGIC 2. Inspect the schema with `.printSchema()`
# MAGIC 3. How many countries are in the file?
# MAGIC 4. Save as Bronze table `workspace.bronze.countries_raw`

# COMMAND ----------

# Hint: spark.read.format("xml").option("rowTag", "country").load(XML_PATH)
# YOUR CODE HERE
raise NotImplementedError("Step 1: read XML and write to Bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.countries_raw LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Flatten to Silver
# MAGIC
# MAGIC The Bronze table has nested columns (`timezones`, `translations`).
# MAGIC Create a Silver table `workspace.silver.countries` with these flat columns:
# MAGIC
# MAGIC | Silver column | Source |
# MAGIC |---|---|
# MAGIC | `id` | `id` |
# MAGIC | `name` | `name` |
# MAGIC | `iso3` | `iso3` |
# MAGIC | `iso2` | `iso2` |
# MAGIC | `capital` | `capital` |
# MAGIC | `region` | `region` |
# MAGIC | `subregion` | `subregion` |
# MAGIC | `latitude` | `latitude` (cast to double) |
# MAGIC | `longitude` | `longitude` (cast to double) |
# MAGIC | `currency` | `currency` |
# MAGIC | `currency_name` | `currency_name` |
# MAGIC | `phone_code` | `phone_code` |
# MAGIC | `tld` | `tld` |

# COMMAND ----------

# Hint: read the bronze table, then df.select(col("id"), col("name"), ...,
#       try_cast(col("latitude"), "double").alias("latitude"), ...)
# YOUR CODE HERE
raise NotImplementedError("Step 2: flatten to Silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.countries LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.bronze.countries_raw;
# MAGIC -- DROP TABLE IF EXISTS workspace.silver.countries;
