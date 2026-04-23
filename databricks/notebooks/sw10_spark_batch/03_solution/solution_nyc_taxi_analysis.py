# Databricks notebook source

# MAGIC %md
# MAGIC # NYC Yellow Taxi Analysis: Solution
# MAGIC
# MAGIC Analyze the 2025 NYC yellow taxi trip data (12 monthly parquet files) with PySpark.
# MAGIC Tasks build on each other: load → explore → clean → aggregate → visualize.

# COMMAND ----------

NYC_PATH = "/Volumes/workspace/nyc_taxi/raw_files/NYC/"
VENDOR_TABLE = "workspace.nyc_taxi.vendor_list"
PAYMENT_TABLE = "workspace.nyc_taxi.payment_types"

print(f"Data source: {NYC_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Load all 12 parquet files into a single DataFrame
# MAGIC
# MAGIC Use `spark.read.parquet()` with a glob pattern so that all 12 monthly files are
# MAGIC read at once into one DataFrame.

# COMMAND ----------

df = spark.read.parquet(f"{NYC_PATH}*.parquet")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Basic exploration
# MAGIC
# MAGIC - How many rows?
# MAGIC - How many columns?
# MAGIC - What is the date range (earliest vs. latest pickup)?

# COMMAND ----------

from pyspark.sql.functions import min as spark_min, max as spark_max

row_count = df.count()
col_count = len(df.columns)

date_range = df.select(
    spark_min("tpep_pickup_datetime").alias("earliest"),
    spark_max("tpep_pickup_datetime").alias("latest"),
).collect()[0]

print(f"Rows:      {row_count:,}")
print(f"Columns:   {col_count}")
print(f"Earliest:  {date_range['earliest']}")
print(f"Latest:    {date_range['latest']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Cache the DataFrame
# MAGIC
# MAGIC All subsequent steps reuse this DataFrame, so caching speeds things up significantly.

# COMMAND ----------

df.cache()
df.count()  # materialize the cache
print("DataFrame cached.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Data quality — null values per column
# MAGIC
# MAGIC Count how many `NULL` values each column has.

# COMMAND ----------

from pyspark.sql.functions import col, count, when

null_counts = df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
])
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Build a cleaned DataFrame
# MAGIC
# MAGIC Keep only trips with plausible values:
# MAGIC - `trip_distance` between 0 (exclusive) and 100 miles
# MAGIC - `fare_amount` between 0 (exclusive) and 500 USD
# MAGIC - `passenger_count` between 1 and 6
# MAGIC - `tpep_pickup_datetime` within the year 2025

# COMMAND ----------

from pyspark.sql.functions import year

df_clean = (
    df
    .filter((col("trip_distance") > 0) & (col("trip_distance") < 100))
    .filter((col("fare_amount") > 0) & (col("fare_amount") < 500))
    .filter((col("passenger_count") >= 1) & (col("passenger_count") <= 6))
    .filter(year("tpep_pickup_datetime") == 2025)
)

df_clean.cache()
clean_count = df_clean.count()
print(f"Clean rows:    {clean_count:,}")
print(f"Removed rows:  {row_count - clean_count:,} ({(row_count - clean_count) / row_count:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Trips per month
# MAGIC
# MAGIC Count the clean trips per month and visualize them as a bar chart.

# COMMAND ----------

from pyspark.sql.functions import month

trips_per_month = (
    df_clean
    .groupBy(month("tpep_pickup_datetime").alias("month"))
    .count()
    .orderBy("month")
)
display(trips_per_month)

# COMMAND ----------

import matplotlib.pyplot as plt

pdf = trips_per_month.toPandas()

fig, ax = plt.subplots(figsize=(9, 4))
ax.bar(pdf["month"], pdf["count"], color="#1f77b4")
ax.set_xticks(range(1, 13))
ax.set_xlabel("Month")
ax.set_ylabel("Number of trips")
ax.set_title("NYC Yellow Taxi trips per month (2025)")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Trips per day of the week
# MAGIC
# MAGIC Which weekday has the most trips? Visualize as a bar chart (Mon–Sun).

# COMMAND ----------

from pyspark.sql.functions import dayofweek

trips_per_dow = (
    df_clean
    .groupBy(dayofweek("tpep_pickup_datetime").alias("dow"))
    .count()
    .orderBy("dow")
)

pdf = trips_per_dow.toPandas()
# Spark dayofweek: 1=Sunday, 2=Monday, ... reorder to Mon-Sun
weekday_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
pdf["weekday"] = pdf["dow"].apply(lambda d: weekday_labels[d - 1])
order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
pdf = pdf.set_index("weekday").reindex(order).reset_index()

fig, ax = plt.subplots(figsize=(9, 4))
ax.bar(pdf["weekday"], pdf["count"], color="#ff7f0e")
ax.set_xlabel("Day of week")
ax.set_ylabel("Number of trips")
ax.set_title("NYC Yellow Taxi trips per weekday (2025)")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Trips per hour of the day
# MAGIC
# MAGIC Aggregate trips by pickup hour (0–23) and plot as a line chart.

# COMMAND ----------

from pyspark.sql.functions import hour

trips_per_hour = (
    df_clean
    .groupBy(hour("tpep_pickup_datetime").alias("hour"))
    .count()
    .orderBy("hour")
)

pdf = trips_per_hour.toPandas()

fig, ax = plt.subplots(figsize=(9, 4))
ax.plot(pdf["hour"], pdf["count"], marker="o", color="#2ca02c")
ax.set_xticks(range(0, 24))
ax.set_xlabel("Hour of day")
ax.set_ylabel("Number of trips")
ax.set_title("NYC Yellow Taxi trips per hour (2025)")
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Financial summary
# MAGIC
# MAGIC Compute total revenue, average fare, average tip, and average trip distance.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, avg, round as spark_round

summary = df_clean.select(
    spark_round(spark_sum("total_amount"), 2).alias("total_revenue_usd"),
    spark_round(avg("fare_amount"), 2).alias("avg_fare_usd"),
    spark_round(avg("tip_amount"), 2).alias("avg_tip_usd"),
    spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 10: Fare amount distribution
# MAGIC
# MAGIC Plot a histogram of `fare_amount` (for trips under USD 100 to keep the chart readable).

# COMMAND ----------

fare_sample = (
    df_clean
    .filter(col("fare_amount") < 100)
    .select("fare_amount")
    .sample(fraction=0.05, seed=42)  # sample so plotting stays cheap
    .toPandas()
)

fig, ax = plt.subplots(figsize=(9, 4))
ax.hist(fare_sample["fare_amount"], bins=50, color="#9467bd", edgecolor="white")
ax.set_xlabel("Fare amount (USD)")
ax.set_ylabel("Frequency")
ax.set_title("Distribution of fare amounts (5% sample, fare < USD 100)")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 11: Tip percentage by payment type
# MAGIC
# MAGIC Join the clean DataFrame with `workspace.nyc_taxi.payment_types`, compute the average
# MAGIC tip as a percentage of the fare (`tip_amount / fare_amount * 100`), and plot per
# MAGIC payment method.

# COMMAND ----------

payment_types = spark.table(PAYMENT_TABLE)

tip_by_payment = (
    df_clean
    .filter(col("fare_amount") > 0)
    .join(payment_types, on="payment_type", how="left")
    .groupBy("payment_description")
    .agg(
        spark_round(avg(col("tip_amount") / col("fare_amount") * 100), 2).alias("avg_tip_pct"),
        count("*").alias("trip_count"),
    )
    .orderBy(col("trip_count").desc())
)
display(tip_by_payment)

# COMMAND ----------

pdf = tip_by_payment.toPandas()

fig, ax = plt.subplots(figsize=(9, 4))
ax.bar(pdf["payment_description"], pdf["avg_tip_pct"], color="#d62728")
ax.set_xlabel("Payment method")
ax.set_ylabel("Average tip (% of fare)")
ax.set_title("Tip percentage by payment method")
ax.grid(axis="y", alpha=0.3)
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 12: Top 10 pickup locations
# MAGIC
# MAGIC Find the 10 most frequent pickup `LocationID`s and plot them.

# COMMAND ----------

top_pickups = (
    df_clean
    .groupBy("PULocationID")
    .count()
    .orderBy(col("count").desc())
    .limit(10)
)
display(top_pickups)

# COMMAND ----------

pdf = top_pickups.toPandas()

fig, ax = plt.subplots(figsize=(9, 4))
ax.bar(pdf["PULocationID"].astype(str), pdf["count"], color="#17becf")
ax.set_xlabel("Pickup LocationID")
ax.set_ylabel("Number of trips")
ax.set_title("Top 10 pickup locations")
ax.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

df.unpersist()
df_clean.unpersist()
print("Caches released.")
