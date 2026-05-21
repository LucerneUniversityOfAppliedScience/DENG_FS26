# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Explore the Source Data
# MAGIC
# MAGIC Before we build a pipeline, let's get familiar with the **source dataset**:
# MAGIC `samples.wanderbricks.*`. This is a built-in Databricks sample modelling a vacation-rental
# MAGIC platform (think Airbnb-style).
# MAGIC
# MAGIC **This notebook is NOT part of the pipeline.** It's an exploration notebook — run it
# MAGIC cell by cell to inspect the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The five source tables

# COMMAND ----------

display(spark.sql("SHOW TABLES IN samples.wanderbricks"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect each table's schema and a few sample rows

# COMMAND ----------

for table in ["users", "hosts", "properties", "bookings", "reviews"]:
    print(f"\n===== samples.wanderbricks.{table} =====")
    df = spark.read.table(f"samples.wanderbricks.{table}")
    print(f"Row count: {df.count():,}")
    df.printSchema()
    display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick relationship check
# MAGIC
# MAGIC The tables form a small relational schema:
# MAGIC - `users` ↔ `bookings.user_id`, `reviews.user_id`
# MAGIC - `hosts` ↔ `properties.host_id`
# MAGIC - `properties` ↔ `bookings.property_id`, `reviews.property_id`
# MAGIC - `bookings` ↔ `reviews.booking_id`

# COMMAND ----------

display(spark.sql("""
    SELECT
        (SELECT COUNT(*) FROM samples.wanderbricks.users)      AS users,
        (SELECT COUNT(*) FROM samples.wanderbricks.hosts)      AS hosts,
        (SELECT COUNT(*) FROM samples.wanderbricks.properties) AS properties,
        (SELECT COUNT(*) FROM samples.wanderbricks.bookings)   AS bookings,
        (SELECT COUNT(*) FROM samples.wanderbricks.reviews)    AS reviews
"""))
