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
# MAGIC
# MAGIC ## Your Task
# MAGIC
# MAGIC **`silver_bookings_enriched`**: join `bronze_bookings` LEFT with `silver_properties_enriched`
# MAGIC on `property_id`, then LEFT with `silver_users_cleaned` on `user_id`. Compute
# MAGIC `nights = datediff(check_out, check_in)`, `price_per_night_calculated`
# MAGIC (`total_amount / nights` when nights > 0 else `total_amount`). Add expectations:
# MAGIC `valid_booking` (booking_id not null), `valid_dates` (check_in < check_out).
# MAGIC
# MAGIC **`silver_reviews_enriched`**: start from `bronze_reviews` filtered to non-deleted rows;
# MAGIC LEFT-join `silver_properties_enriched` on `property_id` and `silver_users_cleaned` on
# MAGIC `user_id`. Compute `review_date` (to_date), `review_length` (F.length of comment), and
# MAGIC a `sentiment` column: `"Positive"` if rating ≥ 4, `"Neutral"` if rating ≥ 3, else
# MAGIC `"Negative"`. Add expectations: `valid_review` (review_id not null),
# MAGIC `valid_rating` (rating between 1 and 5).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

@dp.materialized_view(
    comment="Silver layer: Bookings enriched with property and user information"
)
# TODO: expectations
def silver_bookings_enriched():
    # TODO
    raise NotImplementedError("Implement silver_bookings_enriched")

# COMMAND ----------

@dp.materialized_view(
    comment="Silver layer: Reviews enriched with property, user, and booking context"
)
# TODO: expectations
def silver_reviews_enriched():
    # TODO
    raise NotImplementedError("Implement silver_reviews_enriched")
