from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold layer: Review analytics and sentiment summary"
)
def gold_review_summary():
    reviews = spark.read.table("silver_reviews_enriched")
    
    return (
        reviews
        .withColumn("review_month", F.date_trunc("month", F.col("review_date")))
        .groupBy("property_id", "property_title", "destination_id", "review_month")
        .agg(
            F.count("review_id").alias("total_reviews"),
            F.avg("rating").alias("avg_rating"),
            F.min("rating").alias("min_rating"),
            F.max("rating").alias("max_rating"),
            F.sum(F.when(F.col("sentiment") == "Positive", 1).otherwise(0)).alias("positive_count"),
            F.sum(F.when(F.col("sentiment") == "Neutral", 1).otherwise(0)).alias("neutral_count"),
            F.sum(F.when(F.col("sentiment") == "Negative", 1).otherwise(0)).alias("negative_count"),
            F.avg("review_length").alias("avg_review_length")
        )
        .withColumn(
            "positive_rate",
            F.round(F.col("positive_count") / F.col("total_reviews") * 100, 2)
        )
        .withColumn(
            "negative_rate",
            F.round(F.col("negative_count") / F.col("total_reviews") * 100, 2)
        )
        .withColumn(
            "rating_rounded",
            F.round(F.col("avg_rating"), 1)
        )
        .orderBy(F.desc("review_month"), F.desc("total_reviews"))
    )
