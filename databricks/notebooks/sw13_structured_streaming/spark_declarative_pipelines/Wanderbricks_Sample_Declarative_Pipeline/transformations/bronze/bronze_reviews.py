from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Bronze layer: Raw review data from wanderbricks sample"
)
def bronze_reviews():
    return spark.read.table("samples.wanderbricks.reviews")
