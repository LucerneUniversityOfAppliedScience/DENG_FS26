# Databricks notebook source

# MAGIC %md
# MAGIC # Databricks Free Edition – Getting Started with PySpark
# MAGIC
# MAGIC This notebook is a hands-on introduction to **PySpark** in Databricks.
# MAGIC You will learn how to:
# MAGIC
# MAGIC - Create Spark DataFrames from scratch
# MAGIC - Define schemas explicitly
# MAGIC - Read files (CSV, Parquet) from **Unity Catalog Volumes**
# MAGIC - Save data as **Delta tables** in Unity Catalog
# MAGIC - Query tables with Python and SQL
# MAGIC
# MAGIC ## Infrastructure
# MAGIC
# MAGIC We are using **Databricks Free Edition** with **Unity Catalog**. Key concepts:
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | **Catalog** | Top-level namespace (we use `workspace`) |
# MAGIC | **Schema** | Groups tables within a catalog (e.g. `demo`, `bronze`, `silver`, `gold`) |
# MAGIC | **Table** | Always referenced as `catalog.schema.table` (3-part name) |
# MAGIC | **Volume** | File storage in Unity Catalog — replaces `dbfs:/FileStore/` |
# MAGIC
# MAGIC > **Note:** In Databricks, the `spark` session is created automatically.
# MAGIC > You do not need to call `SparkSession.builder`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup
# MAGIC
# MAGIC We define the paths and table names once at the top so the whole notebook stays easy to adapt.
# MAGIC
# MAGIC All sample files are pre-loaded into the Volume by the `copy_sample_data` job.

# COMMAND ----------

CATALOG      = "workspace"
SCHEMA       = "demo"
TABLE_CITIES = f"{CATALOG}.{SCHEMA}.intro_cities"
TABLE_EU     = f"{CATALOG}.{SCHEMA}.intro_eu_cities"

CSV_PATH     = f"/Volumes/{CATALOG}/raw/sample_data/csv/cities.csv"
PARQUET_PATH = (
    f"/Volumes/{CATALOG}/raw/sample_data/parquet/"
    "Kanton_BS_Wohnbevoelkerung_nach_Geschlecht_Staatsangehoerigkeit_und_Wohnviertel.parquet"
)

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"CSV     : {CSV_PATH}")
print(f"Parquet : {PARQUET_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1 – Create a DataFrame from Scratch
# MAGIC
# MAGIC Similar to Pandas, you can build a Spark DataFrame from a Python dictionary.
# MAGIC
# MAGIC **Naming convention** — use clear prefixes to distinguish DataFrame types:
# MAGIC - `df` → Spark DataFrame
# MAGIC - `pdf` → Pandas DataFrame

# COMMAND ----------

import pandas as pd

# Simple Python dictionary
data = {
    "id":   [1, 2, 3],
    "name": ["Zurich", "Bern", "Basel"],
    "population": [415367, 134794, 178120],
}

# Create a Pandas DataFrame first …
pdf_cities = pd.DataFrame(data)

# … then convert to a Spark DataFrame
df_cities = spark.createDataFrame(pdf_cities)
df_cities.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Built-in visualizations:** Click the **+** icon above any table output and select *Visualization*
# MAGIC to create charts directly in the notebook — no extra libraries needed.
# MAGIC
# MAGIC Try turning the table above into a bar chart (x: `name`, y: `population`).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2 – Define a Schema Explicitly
# MAGIC
# MAGIC By default, Spark infers column types automatically. You can inspect the inferred schema with `printSchema()`.

# COMMAND ----------

df_cities.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark inferred `population` as `LongType` (64-bit integer, max ~9.2 × 10¹⁸).
# MAGIC For city populations, `IntegerType` (max ~2.1 × 10⁹) is more than enough.
# MAGIC
# MAGIC Use `StructType` + `StructField` to define the schema explicitly:

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType

city_schema = StructType([
    StructField("id",         LongType(),    nullable=True),
    StructField("name",       StringType(),  nullable=True),
    StructField("population", IntegerType(), nullable=True),
])

df_cities_typed = spark.createDataFrame(pdf_cities, schema=city_schema)
df_cities_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `population` is now `IntegerType` instead of `LongType`. Explicit schemas are especially
# MAGIC important in production pipelines where wrong types can cause silent data corruption.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3 – Read a CSV File from a Unity Catalog Volume
# MAGIC
# MAGIC Sample files are stored in Unity Catalog Volumes under `/Volumes/{catalog}/{schema}/{volume}/`.
# MAGIC No manual file upload is needed — the `copy_sample_data` job takes care of that.
# MAGIC
# MAGIC We read a global cities dataset (≈ 150 k rows):

# COMMAND ----------

df_csv = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(CSV_PATH)
)

print(f"Rows: {df_csv.count():,}")
df_csv.limit(5).display()

# COMMAND ----------

df_csv.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4 – Save as a Delta Table in Unity Catalog
# MAGIC
# MAGIC **Unity Catalog** replaces the old Hive Metastore. Tables are always addressed with a
# MAGIC three-part name: `catalog.schema.table`.
# MAGIC
# MAGIC `saveAsTable()` writes the data as a managed **Delta table** and registers it in the Catalog.
# MAGIC You can then find it in the left sidebar under **Catalog → workspace → demo**.

# COMMAND ----------

(
    df_csv
    .write
    .mode("overwrite")
    .saveAsTable(TABLE_CITIES)
)

print(f"Table saved: {TABLE_CITIES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5 – Query the Table
# MAGIC
# MAGIC There are three equivalent ways to read a managed table.
# MAGIC
# MAGIC **Option A** — `spark.table()`:

# COMMAND ----------

df_from_table = spark.table(TABLE_CITIES)
df_from_table.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Option B** — `spark.sql()` (useful for dynamic queries in Python):

# COMMAND ----------

df_europe = spark.sql(f"""
    SELECT name, country_name, latitude, longitude
    FROM {TABLE_CITIES}
    WHERE country_name IN ('Germany', 'France', 'Italy', 'Spain', 'Switzerland')
    ORDER BY country_name, name
""")

print(f"European cities: {df_europe.count():,}")
df_europe.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Option C** — pure `%sql` magic (no Python wrapper needed):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country_name, COUNT(*) AS city_count
# MAGIC FROM workspace.demo.intro_cities
# MAGIC WHERE country_name IN ('Germany', 'France', 'Italy', 'Spain', 'Switzerland')
# MAGIC GROUP BY country_name
# MAGIC ORDER BY city_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC You can also **create a new table** directly in SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.demo.intro_eu_cities AS
# MAGIC SELECT name, country_name, latitude, longitude
# MAGIC FROM workspace.demo.intro_cities
# MAGIC WHERE country_name IN ('Germany', 'France', 'Italy', 'Spain', 'Switzerland',
# MAGIC                        'Austria', 'Netherlands', 'Belgium', 'Portugal', 'Poland')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country_name, COUNT(*) AS cities
# MAGIC FROM workspace.demo.intro_eu_cities
# MAGIC GROUP BY country_name
# MAGIC ORDER BY cities DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6 – Read a Parquet File
# MAGIC
# MAGIC Parquet is the standard format in modern data lakes: columnar, compressed, self-describing.
# MAGIC Reading it in Spark requires almost no changes compared to CSV — just drop the `.format()` call
# MAGIC (or use `.format("parquet")` explicitly).
# MAGIC
# MAGIC We read population data for the Canton of Basel-Stadt (open government data):

# COMMAND ----------

df_bs = spark.read.parquet(PARQUET_PATH)

print(f"Rows  : {df_bs.count():,}")
df_bs.printSchema()

# COMMAND ----------

df_bs.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that Spark reads the schema directly from the Parquet footer — no `inferSchema` option needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.demo.intro_cities;
# MAGIC DROP TABLE IF EXISTS workspace.demo.intro_eu_cities
