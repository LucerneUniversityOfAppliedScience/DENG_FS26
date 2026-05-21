from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Bronze layer: Raw user data from wanderbricks sample"
)
def bronze_users():
    return spark.read.table("samples.wanderbricks.users")
