# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Gold: Window Functions & Rankings
# MAGIC
# MAGIC **Goal:** Build the remaining two gold tables — `gold_booking_analytics` (with a monthly
# MAGIC revenue ranking via a **window function**) and `gold_host_performance` (multi-stage
# MAGIC aggregation per host).
# MAGIC
# MAGIC ## Concepts
# MAGIC
# MAGIC ### Window functions
# MAGIC A window function computes a value over a *window* of rows defined relative to the
# MAGIC current row, without collapsing them like `groupBy` does. The recipe is:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC w = Window.partitionBy("group_col").orderBy(F.desc("metric"))
# MAGIC df.withColumn("rank", F.row_number().over(w))
# MAGIC ```
# MAGIC
# MAGIC `row_number()` assigns 1, 2, 3, ... within each partition; `rank()` and `dense_rank()`
# MAGIC handle ties differently.
# MAGIC
# MAGIC ### `date_trunc`
# MAGIC `F.date_trunc("month", date_col)` rounds a date down to the start of the month — handy
# MAGIC for monthly aggregates.
# MAGIC
# MAGIC ## Your Task
# MAGIC
# MAGIC **`gold_booking_analytics`**: filter `silver_bookings_enriched` to `status == "confirmed"`,
# MAGIC add a `booking_month` column, group by
# MAGIC `(property_id, property_title, destination_id, booking_month)`, aggregate booking
# MAGIC metrics. Then add a **window-function ranking** `revenue_rank` (1 = top earner each
# MAGIC month) using `row_number().over(Window.partitionBy("booking_month").orderBy(F.desc("total_revenue")))`.
# MAGIC
# MAGIC **`gold_host_performance`**: aggregate confirmed bookings and reviews per host, plus a
# MAGIC property count per host. Join all three. Add derived per-property and per-review-rate
# MAGIC metrics. See the solution for the exact column list.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

@dp.materialized_view(
    comment="Gold layer: Booking analytics aggregated by property and month, with revenue rank"
)
def gold_booking_analytics():
    # TODO
    raise NotImplementedError("Implement gold_booking_analytics")

# COMMAND ----------

@dp.materialized_view(
    comment="Gold layer: Host performance metrics and rankings"
)
def gold_host_performance():
    # TODO
    raise NotImplementedError("Implement gold_host_performance")
