# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Gold: Aggregations
# MAGIC
# MAGIC **Goal:** Build the first two gold tables — `gold_review_summary` and
# MAGIC `gold_property_performance` — using `groupBy().agg()` patterns.
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ### `groupBy().agg()`
# MAGIC Group rows by one or more columns and apply aggregate functions. Common aggregates:
# MAGIC `F.count`, `F.sum`, `F.avg`, `F.min`, `F.max`, `F.countDistinct`.
# MAGIC
# MAGIC ### Multi-stage aggregation
# MAGIC `gold_property_performance` joins three pre-aggregated views (`booking_stats`,
# MAGIC `review_stats`, and the properties table itself). Build the small grouped DataFrames
# MAGIC first, then join them — this is much cheaper than joining and *then* grouping.
# MAGIC
# MAGIC ### Conditional aggregation
# MAGIC `F.sum(F.when(cond, 1).otherwise(0))` is the classic SQL `COUNT(CASE WHEN ...)` idiom —
# MAGIC count rows that match a condition within a group.
# MAGIC
# MAGIC ## Your Task
# MAGIC
# MAGIC **`gold_review_summary`**: from `silver_reviews_enriched`, derive `review_month` via
# MAGIC `date_trunc("month", review_date)`. Group by
# MAGIC `(property_id, property_title, destination_id, review_month)` and aggregate:
# MAGIC `total_reviews`, `avg_rating`, `min_rating`, `max_rating`, sentiment counts
# MAGIC (positive/neutral/negative via the conditional aggregation idiom), `avg_review_length`.
# MAGIC Compute `positive_rate` and `negative_rate` as percentages (rounded to 2 decimals).
# MAGIC
# MAGIC **`gold_property_performance`**: aggregate confirmed bookings and reviews per
# MAGIC `property_id`, then LEFT-join those onto `silver_properties_enriched`. Add derived
# MAGIC metrics `occupancy_score` and `review_sentiment_ratio` (see the solution for exact
# MAGIC formulas).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

@dp.materialized_view(
    comment="Gold layer: Review analytics and sentiment summary"
)
def gold_review_summary():
    # TODO
    raise NotImplementedError("Implement gold_review_summary")

# COMMAND ----------

@dp.materialized_view(
    comment="Gold layer: Property performance metrics"
)
def gold_property_performance():
    # TODO
    raise NotImplementedError("Implement gold_property_performance")
