# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Gold: Window Functions & Rankings
# MAGIC
# MAGIC **Goal:** Build the remaining two gold tables — `gold_booking_analytics` (with a monthly
# MAGIC revenue ranking via a **window function**) and `gold_host_performance` (multi-stage
# MAGIC aggregation per host).
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ### Window functions
# MAGIC A window function computes a value over a *window* of rows defined relative to the
# MAGIC current row, without collapsing them like `groupBy` does. The recipe is:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC w = Window.partitionBy("group_col").orderBy(F.desc("metric"))
# MAGIC df.withColumn("rank", F.row_number().over(w))
# MAGIC ```
# MAGIC
# MAGIC `row_number()` assigns 1, 2, 3, ... within each partition; `rank()` and `dense_rank()`
# MAGIC handle ties differently.
# MAGIC
# MAGIC ### `date_trunc`
# MAGIC `F.date_trunc("month", date_col)` rounds a date down to the start of the month — handy
# MAGIC for monthly aggregates.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

@dp.materialized_view(
    comment="Gold layer: Booking analytics aggregated by property and month, with revenue rank"
)
def gold_booking_analytics():
    bookings = spark.read.table("silver_bookings_enriched")

    return (
        bookings
        .filter(F.col("status") == "confirmed")
        .withColumn("booking_month", F.date_trunc("month", F.col("check_in")))
        .groupBy("property_id", "property_title", "destination_id", "booking_month")
        .agg(
            F.count("booking_id").alias("total_bookings"),
            F.sum("nights").alias("total_nights_booked"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_booking_value"),
            F.avg("nights").alias("avg_nights_per_booking"),
            F.countDistinct("user_id").alias("unique_guests"),
        )
        .withColumn(
            "revenue_rank",
            F.row_number().over(
                Window.partitionBy("booking_month").orderBy(F.desc("total_revenue"))
            ),
        )
        .orderBy(F.desc("booking_month"), "revenue_rank")
    )

# COMMAND ----------

@dp.materialized_view(
    comment="Gold layer: Host performance metrics and rankings"
)
def gold_host_performance():
    bookings = spark.read.table("silver_bookings_enriched").filter(F.col("status") == "confirmed")
    reviews = spark.read.table("silver_reviews_enriched")
    properties = spark.read.table("silver_properties_enriched")

    property_counts = (
        properties
        .groupBy("host_id")
        .agg(F.count("property_id").alias("total_properties"))
    )

    booking_stats = (
        bookings
        .groupBy("host_id", "host_name", "is_verified")
        .agg(
            F.count("booking_id").alias("total_bookings"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_booking_value"),
            F.countDistinct("property_id").alias("active_properties"),
            F.countDistinct("user_id").alias("unique_guests"),
        )
    )

    review_stats = (
        reviews
        .groupBy("host_id")
        .agg(
            F.count("review_id").alias("total_reviews"),
            F.avg("rating").alias("avg_rating"),
            F.sum(F.when(F.col("sentiment") == "Positive", 1).otherwise(0)).alias("positive_reviews"),
        )
    )

    return (
        booking_stats
        .join(property_counts, "host_id", "left")
        .join(review_stats, "host_id", "left")
        .select(
            "host_id",
            "host_name",
            "is_verified",
            F.coalesce("total_properties", F.lit(0)).alias("total_properties"),
            "total_bookings",
            F.round("total_revenue", 2).alias("total_revenue"),
            F.round("avg_booking_value", 2).alias("avg_booking_value"),
            "unique_guests",
            F.coalesce("total_reviews", F.lit(0)).alias("total_reviews"),
            F.round(F.coalesce("avg_rating", F.lit(0.0)), 2).alias("avg_rating"),
            F.coalesce("positive_reviews", F.lit(0)).alias("positive_reviews"),
        )
        .withColumn(
            "revenue_per_property",
            F.when(F.col("total_properties") > 0,
                   F.round(F.col("total_revenue") / F.col("total_properties"), 2)
            ).otherwise(0),
        )
        .withColumn(
            "bookings_per_property",
            F.when(F.col("total_properties") > 0,
                   F.round(F.col("total_bookings") / F.col("total_properties"), 2)
            ).otherwise(0),
        )
        .withColumn(
            "positive_review_rate",
            F.when(F.col("total_reviews") > 0,
                   F.round(F.col("positive_reviews") / F.col("total_reviews") * 100, 2)
            ).otherwise(0),
        )
        .orderBy(F.desc("total_revenue"))
    )
