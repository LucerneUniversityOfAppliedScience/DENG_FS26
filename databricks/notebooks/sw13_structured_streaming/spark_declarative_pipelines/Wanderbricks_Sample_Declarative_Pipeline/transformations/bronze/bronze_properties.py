from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Bronze layer: Raw property data from wanderbricks sample"
)
def bronze_properties():
    return spark.read.table("samples.wanderbricks.properties")
