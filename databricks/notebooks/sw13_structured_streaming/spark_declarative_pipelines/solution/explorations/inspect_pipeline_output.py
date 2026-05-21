# Databricks notebook source
# MAGIC %md
# MAGIC # Inspect Pipeline Output
# MAGIC
# MAGIC After your pipeline runs successfully, use this notebook to inspect the resulting
# MAGIC bronze / silver / gold tables.
# MAGIC
# MAGIC **Adjust `CATALOG` and `SCHEMA` below** to match the target you chose when configuring
# MAGIC the pipeline.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "wanderbricks_tutorial"  # adjust to your pipeline target schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables produced by the pipeline

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample analytics queries

# COMMAND ----------

# Top 10 properties by total revenue
display(spark.sql(f"""
    SELECT property_title, destination_id, total_bookings, total_revenue,
           avg_rating, occupancy_score
    FROM {CATALOG}.{SCHEMA}.gold_property_performance
    ORDER BY total_revenue DESC
    LIMIT 10
"""))

# COMMAND ----------

# Monthly booking trends
display(spark.sql(f"""
    SELECT booking_month,
           COUNT(DISTINCT property_id) AS active_properties,
           SUM(total_bookings)         AS total_bookings,
           SUM(total_revenue)          AS total_revenue
    FROM {CATALOG}.{SCHEMA}.gold_booking_analytics
    GROUP BY booking_month
    ORDER BY booking_month DESC
    LIMIT 12
"""))

# COMMAND ----------

# Top revenue-ranked property per month
display(spark.sql(f"""
    SELECT booking_month, property_title, total_revenue, revenue_rank
    FROM {CATALOG}.{SCHEMA}.gold_booking_analytics
    WHERE revenue_rank = 1
    ORDER BY booking_month DESC
"""))
