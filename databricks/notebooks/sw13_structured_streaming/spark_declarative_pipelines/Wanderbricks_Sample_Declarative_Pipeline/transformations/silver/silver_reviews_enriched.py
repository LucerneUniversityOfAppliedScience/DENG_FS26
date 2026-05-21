from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Silver layer: Reviews enriched with property, user, and booking context"
)
@dp.expect_or_drop("valid_review", "review_id IS NOT NULL")
@dp.expect_or_drop("valid_rating", "rating >= 1 AND rating <= 5")
def silver_reviews_enriched():
    reviews = spark.read.table("bronze_reviews").filter(F.col("is_deleted") == False)
    properties = spark.read.table("silver_properties_enriched")
    users = spark.read.table("silver_users_cleaned")
    
    return (
        reviews
        .join(properties, reviews.property_id == properties.property_id, "left")
        .join(users, reviews.user_id == users.user_id, "left")
        .select(
            reviews["review_id"],
            reviews["booking_id"],
            reviews["property_id"],
            properties["property_title"],
            properties["destination_id"],
            reviews["user_id"],
            users["name"].alias("reviewer_name"),
            reviews["rating"],
            reviews["comment"],
            F.to_date(reviews["created_at"]).alias("review_date"),
            properties["host_id"],
            properties["host_name"],
            properties["is_verified"],
            F.length(reviews["comment"]).alias("review_length"),
            F.when(F.col("rating") >= 4, "Positive")
             .when(F.col("rating") >= 3, "Neutral")
             .otherwise("Negative").alias("sentiment")
        )
    )
