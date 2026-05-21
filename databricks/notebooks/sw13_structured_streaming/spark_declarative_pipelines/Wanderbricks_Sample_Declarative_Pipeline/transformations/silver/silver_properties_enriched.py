from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Silver layer: Properties enriched with host information"
)
@dp.expect_or_drop("valid_property", "property_id IS NOT NULL")
def silver_properties_enriched():
    properties = spark.read.table("bronze_properties")
    hosts = spark.read.table("bronze_hosts")
    
    return (
        properties
        .join(hosts, properties.host_id == hosts.host_id, "left")
        .select(
            properties["property_id"],
            properties["title"].alias("property_title"),
            properties["property_type"],
            properties["destination_id"],
            F.coalesce(properties["bedrooms"], F.lit(0)).alias("bedrooms"),
            F.coalesce(properties["bathrooms"], F.lit(0)).alias("bathrooms"),
            F.coalesce(properties["max_guests"], F.lit(1)).alias("max_guests"),
            properties["base_price"],
            properties["property_latitude"],
            properties["property_longitude"],
            properties["host_id"],
            hosts["name"].alias("host_name"),
            hosts["joined_at"],
            F.coalesce(hosts["is_verified"], F.lit(False)).alias("is_verified")
        )
    )
