# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: First Materialized View
# MAGIC
# MAGIC **Goal:** Create the first table in our pipeline using `@dp.materialized_view`.
# MAGIC
# MAGIC ## Concept
# MAGIC A **materialized view** in a Declarative Pipeline is a Delta table whose contents are
# MAGIC **recomputed in full** every time the pipeline runs. You define it as a Python function
# MAGIC decorated with `@dp.materialized_view`; the function returns a Spark DataFrame, and the
# MAGIC pipeline framework takes care of writing it to storage with the function's name as the
# MAGIC table name.
# MAGIC
# MAGIC The **bronze layer** is the raw landing zone — minimal transformation, just bring the
# MAGIC data in. Here we read directly from `samples.wanderbricks.users` (a built-in Databricks
# MAGIC sample dataset) and expose it as a pipeline table called `bronze_users`.

# COMMAND ----------

from pyspark import pipelines as dp

# COMMAND ----------

@dp.materialized_view(
    comment="Bronze layer: Raw user data from wanderbricks sample"
)
def bronze_users():
    return spark.read.table("samples.wanderbricks.users")
