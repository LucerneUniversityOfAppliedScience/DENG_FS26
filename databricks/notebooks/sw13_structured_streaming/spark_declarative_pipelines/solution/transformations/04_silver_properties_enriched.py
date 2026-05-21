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

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

@dp.materialized_view(
    comment="Silver layer: Properties enriched with host information"
)
@dp.expect_or_drop("valid_property", "property_id IS NOT NULL")
def silver_properties_enriched():
    properties = spark.read.table("bronze_properties")
    hosts = spark.read.table("bronze_hosts")

    return (
        properties
        .join(hosts, properties.host_id == hosts.host_id, "left")
        .select(
            properties["property_id"],
            properties["title"].alias("property_title"),
            properties["property_type"],
            properties["destination_id"],
            F.coalesce(properties["bedrooms"], F.lit(0)).alias("bedrooms"),
            F.coalesce(properties["bathrooms"], F.lit(0)).alias("bathrooms"),
            F.coalesce(properties["max_guests"], F.lit(1)).alias("max_guests"),
            properties["base_price"],
            properties["property_latitude"],
            properties["property_longitude"],
            properties["host_id"],
            hosts["name"].alias("host_name"),
            hosts["joined_at"],
            F.coalesce(hosts["is_verified"], F.lit(False)).alias("is_verified"),
        )
    )
