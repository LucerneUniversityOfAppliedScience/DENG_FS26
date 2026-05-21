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
# MAGIC
# MAGIC ## Your Task
# MAGIC Implement all four functions below. They all follow exactly the same shape as
# MAGIC `bronze_users` from notebook 01.

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw host data from wanderbricks sample"
)
def bronze_hosts():
    # TODO
    raise NotImplementedError("Implement bronze_hosts")

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw property data from wanderbricks sample"
)
def bronze_properties():
    # TODO
    raise NotImplementedError("Implement bronze_properties")

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw booking data from wanderbricks sample"
)
def bronze_bookings():
    # TODO
    raise NotImplementedError("Implement bronze_bookings")

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw review data from wanderbricks sample"
)
def bronze_reviews():
    # TODO
    raise NotImplementedError("Implement bronze_reviews")
