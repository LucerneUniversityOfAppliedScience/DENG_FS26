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
# MAGIC - Coalesce nulls to sensible defaults (`F.coalesce(col, F.lit(False))`)
# MAGIC - Cast types (`F.to_date`)
# MAGIC - Drop duplicates (`.dropDuplicates(["user_id"])`)
# MAGIC
# MAGIC ## Your Task
# MAGIC Build `silver_users_cleaned` from `bronze_users` with these columns:
# MAGIC `user_id`, `email` (trimmed and lowercased), `name` (trimmed), `country` (uppercased),
# MAGIC `user_type`, `created_date` (cast `created_at` to date), `is_business` (coalesce nulls
# MAGIC to `False`), `company_name` (trimmed). Drop duplicates by `user_id`. Add expectations
# MAGIC for a non-null `user_id` and a plausible email (`'%@%'`).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

@dp.materialized_view(
    comment="Silver layer: Cleaned and standardized user data"
)
# TODO: add two @dp.expect_or_drop decorators here (valid_email, valid_user_id)
def silver_users_cleaned():
    # TODO
    raise NotImplementedError("Implement silver_users_cleaned")
