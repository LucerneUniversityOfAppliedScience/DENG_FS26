# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver: Cleaning + First Expectations
# MAGIC
# MAGIC **Goal:** Build `silver_users_cleaned` — a cleaned-up version of `bronze_users` — and
# MAGIC introduce **data quality expectations** with `@dp.expect_or_drop`.
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ### Reading from another pipeline table
# MAGIC Inside a pipeline function, refer to an upstream table by name with
# MAGIC `spark.read.table("bronze_users")`. The framework figures out the dependency graph
# MAGIC automatically — no need to declare it explicitly.
# MAGIC
# MAGIC ### Expectations
# MAGIC `@dp.expect_or_drop(name, condition)` attaches a data-quality rule to the table. Rows
# MAGIC that fail the SQL condition are **silently dropped** and counted in the pipeline event
# MAGIC log. Other flavors:
# MAGIC - `@dp.expect(name, condition)` — log a warning, keep the rows.
# MAGIC - `@dp.expect_or_fail(name, condition)` — fail the pipeline run on any violation.
# MAGIC
# MAGIC ### Typical silver-layer cleaning
# MAGIC - Trim/normalize strings (`F.lower`, `F.trim`, `F.upper`)
# MAGIC - Coalesce nulls to sensible defaults
# MAGIC - Cast types (`F.to_date`)
# MAGIC - Drop duplicates

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

@dp.materialized_view(
    comment="Silver layer: Cleaned and standardized user data"
)
@dp.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dp.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def silver_users_cleaned():
    return (
        spark.read.table("bronze_users")
        .select(
            "user_id",
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.trim(F.col("name")).alias("name"),
            F.upper(F.col("country")).alias("country"),
            "user_type",
            F.to_date(F.col("created_at")).alias("created_date"),
            F.coalesce(F.col("is_business"), F.lit(False)).alias("is_business"),
            F.trim(F.col("company_name")).alias("company_name"),
        )
        .dropDuplicates(["user_id"])
    )
