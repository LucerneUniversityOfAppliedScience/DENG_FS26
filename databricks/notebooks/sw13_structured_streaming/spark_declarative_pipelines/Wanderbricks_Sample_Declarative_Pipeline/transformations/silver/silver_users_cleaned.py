from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Silver layer: Cleaned and standardized user data"
)
@dp.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dp.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def silver_users_cleaned():
    return (
        spark.read.table("bronze_users")
        .select(
            "user_id",
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.trim(F.col("name")).alias("name"),
            F.upper(F.col("country")).alias("country"),
            "user_type",
            F.to_date(F.col("created_at")).alias("created_date"),
            F.coalesce(F.col("is_business"), F.lit(False)).alias("is_business"),
            F.trim(F.col("company_name")).alias("company_name")
        )
        .dropDuplicates(["user_id"])
    )
