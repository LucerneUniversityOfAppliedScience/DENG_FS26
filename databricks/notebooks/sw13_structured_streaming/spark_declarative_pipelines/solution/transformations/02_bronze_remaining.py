# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: Remaining Source Tables
# MAGIC
# MAGIC **Goal:** Repeat the bronze pattern for the four remaining `samples.wanderbricks` tables:
# MAGIC `hosts`, `properties`, `bookings`, `reviews`.
# MAGIC
# MAGIC ## Concept
# MAGIC One notebook can declare **multiple** pipeline tables — each `@dp.materialized_view`-decorated
# MAGIC function becomes its own table in the pipeline graph. Bronze tables are typically trivial
# MAGIC (just read the source), so grouping them in a single notebook keeps the project tidy.

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw host data from wanderbricks sample"
)
def bronze_hosts():
    return spark.read.table("samples.wanderbricks.hosts")

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw property data from wanderbricks sample"
)
def bronze_properties():
    return spark.read.table("samples.wanderbricks.properties")

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw booking data from wanderbricks sample"
)
def bronze_bookings():
    return spark.read.table("samples.wanderbricks.bookings")

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw review data from wanderbricks sample"
)
def bronze_reviews():
    return spark.read.table("samples.wanderbricks.reviews")
