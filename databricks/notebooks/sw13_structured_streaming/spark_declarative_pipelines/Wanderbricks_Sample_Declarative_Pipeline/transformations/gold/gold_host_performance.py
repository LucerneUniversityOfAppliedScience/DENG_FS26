from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold layer: Host performance metrics and rankings"
)
def gold_host_performance():
    bookings = spark.read.table("silver_bookings_enriched").filter(F.col("status") == "confirmed")
    reviews = spark.read.table("silver_reviews_enriched")
    properties = spark.read.table("silver_properties_enriched")
    
    # Property count per host
    property_counts = (
        properties
        .groupBy("host_id")
        .agg(F.count("property_id").alias("total_properties"))
    )
    
    # Booking stats per host
    booking_stats = (
        bookings
        .groupBy("host_id", "host_name", "is_verified")
        .agg(
            F.count("booking_id").alias("total_bookings"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_booking_value"),
            F.countDistinct("property_id").alias("active_properties"),
            F.countDistinct("user_id").alias("unique_guests")
        )
    )
    
    # Review stats per host
    review_stats = (
        reviews
        .groupBy("host_id")
        .agg(
            F.count("review_id").alias("total_reviews"),
            F.avg("rating").alias("avg_rating"),
            F.sum(F.when(F.col("sentiment") == "Positive", 1).otherwise(0)).alias("positive_reviews")
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
            F.coalesce("positive_reviews", F.lit(0)).alias("positive_reviews")
        )
        .withColumn(
            "revenue_per_property",
            F.when(F.col("total_properties") > 0,
                   F.round(F.col("total_revenue") / F.col("total_properties"), 2)
            ).otherwise(0)
        )
        .withColumn(
            "bookings_per_property",
            F.when(F.col("total_properties") > 0,
                   F.round(F.col("total_bookings") / F.col("total_properties"), 2)
            ).otherwise(0)
        )
        .withColumn(
            "positive_review_rate",
            F.when(F.col("total_reviews") > 0,
                   F.round(F.col("positive_reviews") / F.col("total_reviews") * 100, 2)
            ).otherwise(0)
        )
        .orderBy(F.desc("total_revenue"))
    )
