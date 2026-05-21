from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Bronze layer: Raw booking data from wanderbricks sample"
)
def bronze_bookings():
    return spark.read.table("samples.wanderbricks.bookings")
