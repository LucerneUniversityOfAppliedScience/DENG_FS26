# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver: First Join
# MAGIC
# MAGIC **Goal:** Build `silver_properties_enriched` by joining `bronze_properties` with
# MAGIC `bronze_hosts` to get host info alongside each property.
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ### Joins between pipeline tables
# MAGIC Read both upstream tables with `spark.read.table(...)` and join them with the usual
# MAGIC `DataFrame.join(other, on, how)` API. Use a `"left"` join to keep every property even
# MAGIC if its host record is missing.
# MAGIC
# MAGIC ### Coalesce + aliasing
# MAGIC Replace nulls in count-like columns with `0` via `F.coalesce(col, F.lit(0))`, and use
# MAGIC `.alias(...)` to give selected columns clearer names — especially important when both
# MAGIC sides of a join have columns with the same name (e.g. `name`).
# MAGIC
# MAGIC ## Your Task
# MAGIC Join `bronze_properties` LEFT with `bronze_hosts` on `host_id`. Select these columns:
# MAGIC `property_id`, `title` → `property_title`, `property_type`, `destination_id`,
# MAGIC `bedrooms` (coalesce null → 0), `bathrooms` (coalesce null → 0),
# MAGIC `max_guests` (coalesce null → 1), `base_price`, `property_latitude`,
# MAGIC `property_longitude`, `host_id`, `name` (from hosts) → `host_name`, `joined_at`,
# MAGIC `is_verified` (coalesce null → False). Add an expectation that `property_id` is
# MAGIC non-null.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

@dp.materialized_view(
    comment="Silver layer: Properties enriched with host information"
)
# TODO: add @dp.expect_or_drop("valid_property", "property_id IS NOT NULL")
def silver_properties_enriched():
    # TODO
    raise NotImplementedError("Implement silver_properties_enriched")
