# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Silver: Bookings & Reviews (Multi-Table Joins + More Expectations)
# MAGIC
# MAGIC **Goal:** Build the remaining two silver tables — `silver_bookings_enriched` and
# MAGIC `silver_reviews_enriched` — each joining three upstream tables and adding computed
# MAGIC columns.
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ### Chained joins
# MAGIC `DataFrame.join(...).join(...)` chains joins. Be explicit about which side of each join
# MAGIC owns which column (e.g. `bookings["user_id"]`) to avoid ambiguity.
# MAGIC
# MAGIC ### Computed columns
# MAGIC - `F.datediff(end, start)` — number of days between two dates.
# MAGIC - `F.when(cond, x).otherwise(y)` — SQL `CASE WHEN`.
# MAGIC - `F.length(string_col)` — string length.
# MAGIC
# MAGIC ### Multiple expectations
# MAGIC Stack multiple `@dp.expect_or_drop` decorators on the same function to enforce several
# MAGIC rules. Each is evaluated independently.
# MAGIC
# MAGIC ### Filtering at the source
# MAGIC `spark.read.table("bronze_reviews").filter(F.col("is_deleted") == False)` filters
# MAGIC tombstoned rows before any downstream work.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

@dp.materialized_view(
    comment="Silver layer: Bookings enriched with property and user information"
)
@dp.expect_or_drop("valid_booking", "booking_id IS NOT NULL")
@dp.expect_or_drop("valid_dates", "check_in < check_out")
def silver_bookings_enriched():
    bookings = spark.read.table("bronze_bookings")
    properties = spark.read.table("silver_properties_enriched")
    users = spark.read.table("silver_users_cleaned")

    return (
        bookings
        .join(properties, bookings.property_id == properties.property_id, "left")
        .join(users, bookings.user_id == users.user_id, "left")
        .select(
            bookings["booking_id"],
            bookings["property_id"],
            properties["property_title"],
            properties["destination_id"],
            bookings["user_id"],
            users["name"].alias("guest_name"),
            users["email"].alias("guest_email"),
            bookings["check_in"],
            bookings["check_out"],
            F.datediff(bookings["check_out"], bookings["check_in"]).alias("nights"),
            bookings["total_amount"],
            bookings["status"],
            bookings["guests_count"],
            F.to_timestamp(bookings["created_at"]).alias("booking_created_at"),
            properties["host_id"],
            properties["host_name"],
            properties["is_verified"],
        )
        .withColumn(
            "price_per_night_calculated",
            F.when(F.col("nights") > 0, F.col("total_amount") / F.col("nights"))
             .otherwise(F.col("total_amount")),
        )
    )

# COMMAND ----------

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
             .otherwise("Negative").alias("sentiment"),
        )
    )
