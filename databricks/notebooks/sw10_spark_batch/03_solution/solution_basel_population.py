# Databricks notebook source

# MAGIC %md
# MAGIC # Basel Population Analysis – Solution
# MAGIC
# MAGIC In this exercise you will analyse the residential population of the Canton of Basel-Stadt
# MAGIC using PySpark and SQL.
# MAGIC
# MAGIC **Dataset:** Wohnbevölkerung nach Geschlecht und Staatsangehörigkeit (opendata.swiss)
# MAGIC — already available in the Unity Catalog Volume.
# MAGIC
# MAGIC **Columns:**
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `datum` | Reference date (string, e.g. "31. Dezember 2023") |
# MAGIC | `gemeinde` | Municipality: Basel, Riehen, or Bettingen |
# MAGIC | `geschlecht` | Gender: M (male) or W (female) |
# MAGIC | `staatsangehoerigkeit` | Nationality (German name) |
# MAGIC | `anzahl` | Number of residents |
# MAGIC | `jahr` | Year (string, e.g. "2023") |
# MAGIC | `wohnviertel_name` | Neighbourhood name |
# MAGIC | `wohnviertel_id` | Neighbourhood ID |

# COMMAND ----------

CATALOG      = "workspace"
SCHEMA       = "demo"
PARQUET_PATH = (
    f"/Volumes/{CATALOG}/raw/sample_data/parquet/"
    "Kanton_BS_Wohnbevoelkerung_nach_Geschlecht_Staatsangehoerigkeit_und_Wohnviertel.parquet"
)
TABLE_POP    = f"{CATALOG}.{SCHEMA}.bs_population"
TABLE_AGG    = f"{CATALOG}.{SCHEMA}.bs_population_by_country"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 1 – Read the Parquet File

# COMMAND ----------

df_pop = spark.read.parquet(PARQUET_PATH)

print(f"Rows: {df_pop.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 – Inspect the Data

# COMMAND ----------

df_pop.limit(10).display()

# COMMAND ----------

df_pop.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC All columns are strings except `anzahl`, which Spark infers as `LongType` (64-bit integer).
# MAGIC For population counts, `IntegerType` (max ~2.1 billion) is sufficient.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 – Explicit Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

bs_schema = StructType([
    StructField("datum",                StringType(), True),
    StructField("gemeinde",             StringType(), True),
    StructField("geschlecht",           StringType(), True),
    StructField("staatsangehoerigkeit", StringType(), True),
    StructField("anzahl",               LongType(),   True),
    StructField("jahr",                 StringType(), True),
    StructField("wohnviertel_name",     StringType(), True),
    StructField("wohnviertel_id",       StringType(), True),
])

df_pop = spark.read.schema(bs_schema).parquet(PARQUET_PATH)
df_pop.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `anzahl` is `LongType` (matches the Parquet file's INT64 storage).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 – Save as Delta Table

# COMMAND ----------

(
    df_pop
    .write
    .mode("overwrite")
    .saveAsTable(TABLE_POP)
)

print(f"Saved: {TABLE_POP}  ({spark.table(TABLE_POP).count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC The table is now visible in the left sidebar under **Catalog → workspace → demo → bs_population**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 – Test: Iran in 1980

# COMMAND ----------

df_iran_1980 = spark.sql(f"""
    SELECT *
    FROM {TABLE_POP}
    WHERE staatsangehoerigkeit = 'Iran'
      AND jahr = '1980'
""")

print(f"Rows found: {df_iran_1980.count()}")
df_iran_1980.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 – Population by Country and Year

# COMMAND ----------

df_by_country = spark.sql(f"""
    SELECT
        staatsangehoerigkeit,
        jahr,
        SUM(anzahl) AS anzahl
    FROM {TABLE_POP}
    WHERE staatsangehoerigkeit != 'Schweiz'
    GROUP BY staatsangehoerigkeit, jahr
    ORDER BY anzahl DESC
""")

df_by_country.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 – Save the Aggregation as a Delta Table

# COMMAND ----------

(
    df_by_country
    .write
    .mode("overwrite")
    .saveAsTable(TABLE_AGG)
)

print(f"Saved: {TABLE_AGG}  ({spark.table(TABLE_AGG).count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 – Visualize: Population by Year and Country
# MAGIC
# MAGIC We filter to the top 10 nationalities (by total residents) so the chart stays readable.
# MAGIC After running this cell, click the **+** button above the output table and select
# MAGIC *Visualization* to create a bar chart:
# MAGIC - X axis: `jahr`
# MAGIC - Y axis: `anzahl`
# MAGIC - Group by: `staatsangehoerigkeit`

# COMMAND ----------

# Determine top 10 nationalities by total anzahl
top10 = spark.sql(f"""
    SELECT staatsangehoerigkeit
    FROM {TABLE_AGG}
    GROUP BY staatsangehoerigkeit
    ORDER BY SUM(anzahl) DESC
    LIMIT 10
""").toPandas()["staatsangehoerigkeit"].tolist()

# Load the aggregation table filtered to top 10
df_top10 = spark.sql(f"""
    SELECT staatsangehoerigkeit, jahr, anzahl
    FROM {TABLE_AGG}
    WHERE staatsangehoerigkeit IN ({", ".join(f"'{c}'" for c in top10)})
    ORDER BY jahr, anzahl DESC
""")

df_top10.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 9 – Filter: Women in Basel, 2010

# COMMAND ----------

df_women_2010 = spark.sql(f"""
    SELECT *
    FROM {TABLE_POP}
    WHERE gemeinde    = 'Basel'
      AND geschlecht  = 'W'
      AND jahr        = '2010'
""")

print(f"Rows: {df_women_2010.count():,}")
df_women_2010.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 10 – Visualize: Where Do the Women Come From?
# MAGIC
# MAGIC Display the data sorted by `anzahl` descending, then use the **Databricks built-in
# MAGIC visualization** (+) to create a bar chart with `staatsangehoerigkeit` on the X axis
# MAGIC and `anzahl` on the Y axis.

# COMMAND ----------

df_women_2010_by_country = spark.sql(f"""
    SELECT staatsangehoerigkeit, SUM(anzahl) AS anzahl
    FROM {TABLE_POP}
    WHERE gemeinde   = 'Basel'
      AND geschlecht = 'W'
      AND jahr       = '2010'
    GROUP BY staatsangehoerigkeit
    ORDER BY anzahl DESC
""")

df_women_2010_by_country.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.demo.bs_population;
# MAGIC DROP TABLE IF EXISTS workspace.demo.bs_population_by_country
