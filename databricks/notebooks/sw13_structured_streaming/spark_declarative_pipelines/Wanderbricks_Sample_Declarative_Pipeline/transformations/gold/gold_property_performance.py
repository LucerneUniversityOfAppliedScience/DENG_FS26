from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold layer: Property performance metrics and rankings"
)
def gold_property_performance():
    bookings = spark.read.table("silver_bookings_enriched").filter(F.col("status") == "confirmed")
    reviews = spark.read.table("silver_reviews_enriched")
    properties = spark.read.table("silver_properties_enriched")
    
    # Aggregate bookings
    booking_stats = (
        bookings
        .groupBy("property_id")
        .agg(
            F.count("booking_id").alias("total_bookings"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("nights").alias("avg_nights"),
            F.countDistinct("user_id").alias("unique_guests")
        )
    )
    
    # Aggregate reviews
    review_stats = (
        reviews
        .groupBy("property_id")
        .agg(
            F.count("review_id").alias("total_reviews"),
            F.avg("rating").alias("avg_rating"),
            F.sum(F.when(F.col("sentiment") == "Positive", 1).otherwise(0)).alias("positive_reviews"),
            F.sum(F.when(F.col("sentiment") == "Negative", 1).otherwise(0)).alias("negative_reviews")
        )
    )
    
    return (
        properties
        .join(booking_stats, "property_id", "left")
        .join(review_stats, "property_id", "left")
        .select(
            "property_id",
            "property_title",
            "property_type",
            "destination_id",
            "bedrooms",
            "bathrooms",
            "max_guests",
            "base_price",
            "host_id",
            "host_name",
            "is_verified",
            F.coalesce("total_bookings", F.lit(0)).alias("total_bookings"),
            F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
            F.coalesce("avg_nights", F.lit(0.0)).alias("avg_nights"),
            F.coalesce("unique_guests", F.lit(0)).alias("unique_guests"),
            F.coalesce("total_reviews", F.lit(0)).alias("total_reviews"),
            F.round(F.coalesce("avg_rating", F.lit(0.0)), 2).alias("avg_rating"),
            F.coalesce("positive_reviews", F.lit(0)).alias("positive_reviews"),
            F.coalesce("negative_reviews", F.lit(0)).alias("negative_reviews")
        )
        .withColumn(
            "occupancy_score",
            F.when(F.col("total_bookings") > 0, 
                   F.round((F.col("total_bookings") * F.col("avg_nights")) / 365 * 100, 2)
            ).otherwise(0)
        )
        .withColumn(
            "review_sentiment_ratio",
            F.when(F.col("total_reviews") > 0,
                   F.round(F.col("positive_reviews") / F.col("total_reviews") * 100, 2)
            ).otherwise(0)
        )
    )
