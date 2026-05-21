# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Wanderbricks Pipeline - Data Exploration
# MAGIC
# MAGIC This notebook provides exploratory data analysis of the materialized pipeline datasets.
# MAGIC
# MAGIC **Pipeline Architecture:**
# MAGIC * **Bronze Layer**: Raw data from Wanderbricks sample (Users, Properties, Bookings, Hosts, Reviews)
# MAGIC * **Silver Layer**: Cleaned and enriched data with joins and data quality checks
# MAGIC * **Gold Layer**: Business analytics and aggregated metrics
# MAGIC
# MAGIC **Note**: This notebook is NOT executed as part of the pipeline.

# COMMAND ----------

# DBTITLE 1,Cell 2
import sys
import os

# Dynamically get pipeline root path from current notebook location
current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
pipeline_root = os.path.dirname(os.path.dirname(current_notebook_path))  # Go up two levels from explorations/
sys.path.append(pipeline_root)

print(f"Pipeline root: {pipeline_root}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# Example 1: Gold Layer - Top 10 Properties by Revenue
print("=== Top 10 Properties by Total Revenue ===")
display(spark.sql("""
    SELECT 
        property_title,
        destination_id,
        total_bookings,
        total_revenue,
        avg_rating,
        occupancy_score
    FROM workspace.default.gold_property_performance
    ORDER BY total_revenue DESC
    LIMIT 10
"""))

# Example 2: Gold Layer - Host Performance Overview
print("\n=== Top Hosts by Revenue ===")
display(spark.sql("""
    SELECT 
        host_name,
        total_properties,
        total_bookings,
        total_revenue,
        avg_rating,
        positive_review_rate,
        is_verified
    FROM workspace.default.gold_host_performance
    ORDER BY total_revenue DESC
    LIMIT 10
"""))

# Example 3: Gold Layer - Monthly Booking Trends
print("\n=== Recent Booking Trends ===")
display(spark.sql("""
    SELECT 
        booking_month,
        COUNT(DISTINCT property_id) as active_properties,
        SUM(total_bookings) as total_bookings,
        SUM(total_revenue) as total_revenue,
        AVG(avg_nights_per_booking) as avg_nights
    FROM workspace.default.gold_booking_analytics
    GROUP BY booking_month
    ORDER BY booking_month DESC
    LIMIT 12
"""))
