from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Gold layer: Booking analytics aggregated by property and month"
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
            F.countDistinct("user_id").alias("unique_guests")
        )
        .withColumn(
            "revenue_rank",
            F.row_number().over(
                Window.partitionBy("booking_month").orderBy(F.desc("total_revenue"))
            )
        )
        .orderBy(F.desc("booking_month"), "revenue_rank")
    )
