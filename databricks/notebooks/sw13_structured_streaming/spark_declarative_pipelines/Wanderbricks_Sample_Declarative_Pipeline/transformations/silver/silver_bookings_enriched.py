from pyspark import pipelines as dp
from pyspark.sql import functions as F

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
            properties["is_verified"]
        )
        .withColumn(
            "price_per_night_calculated",
            F.when(F.col("nights") > 0, F.col("total_amount") / F.col("nights")).otherwise(F.col("total_amount"))
        )
    )
