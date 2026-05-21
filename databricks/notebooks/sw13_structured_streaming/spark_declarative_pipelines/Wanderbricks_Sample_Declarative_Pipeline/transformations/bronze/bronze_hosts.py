from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Bronze layer: Raw host data from wanderbricks sample"
)
def bronze_hosts():
    return spark.read.table("samples.wanderbricks.hosts")
