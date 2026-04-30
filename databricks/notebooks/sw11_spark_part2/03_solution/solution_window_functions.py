# Databricks notebook source

# MAGIC %md
# MAGIC # Window Functions and Pivot — Solution
# MAGIC
# MAGIC In this notebook you learn how to use **window functions** and **pivot/unpivot**
# MAGIC for analytic queries on real-world time-series data.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Window functions are the bridge between row-level transformations and
# MAGIC group-level aggregations. They let you compute things like running totals,
# MAGIC moving averages, ranks, and previous/next values **without collapsing rows**.
# MAGIC In SQL terms: anything that uses `OVER (PARTITION BY ... ORDER BY ...)`.
# MAGIC
# MAGIC Pivot reshapes data from long to wide. It's how you turn a tidy fact table
# MAGIC into a dashboard-ready cross-tab. Unpivot is the inverse — useful for
# MAGIC normalising spreadsheet exports back into a queryable shape.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC Pedestrian and bike counts from the city of Zurich, 15-minute intervals
# MAGIC across all of 2025, ~850k rows. Open data from the city of Zurich, already
# MAGIC staged in your Unity Catalog volume by `copy_sample_data`.
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `FK_STANDORT` | Station ID |
# MAGIC | `DATUM` | Timestamp (ISO `T`-separated, 15-min intervals) |
# MAGIC | `VELO_IN`, `VELO_OUT` | Bikes counted in each direction |
# MAGIC | `FUSS_IN`, `FUSS_OUT` | Pedestrians counted (NULL on bike-only stations) |
# MAGIC | `OST`, `NORD` | Swiss coordinates of the station |
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume in the UC
# MAGIC bundle. If you cloned the repo or pulled new changes, you must
# MAGIC **redeploy the bundle** before running any sw11 notebook — even ones
# MAGIC that don't need the new volume — so your workspace's bundle state
# MAGIC matches the repo. In the bundle UI, click **Deploy** once.

# COMMAND ----------

CSV_PATH      = "/Volumes/workspace/raw/sample_data/csv/2025_verkehrszaehlungen_werte_fussgaenger_velo.csv"
BRONZE_TABLE  = "workspace.bronze.traffic_counts"
GOLD_DAILY    = "workspace.gold.traffic_daily_by_station"

print(f"CSV   : {CSV_PATH}")
print(f"Bronze: {BRONZE_TABLE}")
print(f"Gold  : {GOLD_DAILY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop existing tables
# MAGIC
# MAGIC Drop any existing target tables before re-running so a stale schema from a
# MAGIC previous run does not block the overwrite (Delta refuses schema changes on
# MAGIC `overwrite` when Table ACLs are enabled).

# COMMAND ----------

for table in [BRONZE_TABLE, GOLD_DAILY]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read the CSV with an explicit schema
# MAGIC
# MAGIC We parse `DATUM` as a timestamp directly using the `timestampFormat` option,
# MAGIC then derive `date`, `hour`, and `weekday` columns. Deriving these once
# MAGIC up-front means later window queries don't have to re-parse strings on every
# MAGIC row.
# MAGIC
# MAGIC `dayofweek` returns 1=Sunday … 7=Saturday in Spark. We convert to an
# MAGIC ISO-style 1=Monday … 7=Sunday for readability.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
from pyspark.sql.functions import col, to_date, hour, dayofweek, when, expr

traffic_schema = StructType([
    StructField("FK_STANDORT", IntegerType(),   True),
    StructField("DATUM",       TimestampType(), True),
    StructField("VELO_IN",     IntegerType(),   True),
    StructField("VELO_OUT",    IntegerType(),   True),
    StructField("FUSS_IN",     IntegerType(),   True),
    StructField("FUSS_OUT",    IntegerType(),   True),
    StructField("OST",         DoubleType(),    True),
    StructField("NORD",        DoubleType(),    True),
])

df_raw = (spark.read
    .option("header", "true")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm")
    .schema(traffic_schema)
    .csv(CSV_PATH))

df_bronze = (df_raw
    .withColumnRenamed("FK_STANDORT", "station_id")
    .withColumnRenamed("DATUM",       "ts")
    .withColumn("date",    to_date(col("ts")))
    .withColumn("hour",    hour(col("ts")))
    # Spark's dayofweek is 1=Sunday..7=Saturday. Convert to ISO 1=Monday..7=Sunday.
    .withColumn("weekday", ((dayofweek(col("ts")) + 5) % 7) + 1)
)

df_bronze.write.mode("overwrite").saveAsTable(BRONZE_TABLE)
print(f"Bronze rows: {spark.table(BRONZE_TABLE).count():,}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Aggregate to daily totals per station
# MAGIC
# MAGIC Window functions need a clean grain. We collapse the 15-minute observations
# MAGIC into one row per `(station_id, date)`, summing the four direction counters.
# MAGIC `coalesce(... , 0)` treats missing pedestrian counts as zero for stations
# MAGIC that only count bikes — otherwise the sum becomes NULL.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, coalesce, lit

df_daily = (spark.table(BRONZE_TABLE)
    .groupBy("station_id", "date")
    .agg(
        spark_sum(coalesce(col("VELO_IN"),  lit(0)) + coalesce(col("VELO_OUT"),  lit(0))).alias("bikes_total"),
        spark_sum(coalesce(col("FUSS_IN"),  lit(0)) + coalesce(col("FUSS_OUT"),  lit(0))).alias("peds_total"),
    )
)

df_daily.write.mode("overwrite").saveAsTable(GOLD_DAILY)
print(f"Daily rows: {spark.table(GOLD_DAILY).count():,}")
display(spark.table(GOLD_DAILY).orderBy("station_id", "date").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Window — running totals (cumulative sum)
# MAGIC
# MAGIC A running total of bikes per station, accumulating from the start of the
# MAGIC year to the current date. The frame `ROWS BETWEEN UNBOUNDED PRECEDING AND
# MAGIC CURRENT ROW` is the textbook running-total window.
# MAGIC
# MAGIC Two ways to write this — Python API and SQL — produce the same plan.
# MAGIC We show the Python API here; the SQL form is used in Step 4.

# COMMAND ----------

from pyspark.sql.window import Window

w_running = (Window
    .partitionBy("station_id")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df_running = (spark.table(GOLD_DAILY)
    .withColumn("bikes_ytd", spark_sum("bikes_total").over(w_running)))

# Pick one busy station for a readable preview.
display(df_running.filter("station_id = 4241").orderBy("date").limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how `bikes_ytd` strictly grows. If you change the order column, the
# MAGIC running total takes a different path through the same data — order matters.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Window — 7-day moving average
# MAGIC
# MAGIC Smooth the daily counts with a 7-day rolling mean, ending at the current
# MAGIC row. The frame `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` covers the
# MAGIC current day plus the six days before it — seven values total.
# MAGIC
# MAGIC SQL form for variety:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   station_id,
# MAGIC   date,
# MAGIC   bikes_total,
# MAGIC   round(
# MAGIC     avg(bikes_total) OVER (
# MAGIC       PARTITION BY station_id
# MAGIC       ORDER BY date
# MAGIC       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC     ),
# MAGIC     1
# MAGIC   ) AS bikes_ma7
# MAGIC FROM workspace.gold.traffic_daily_by_station
# MAGIC WHERE station_id = 4241
# MAGIC ORDER BY date
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC The first six rows of any station show a moving average computed over fewer
# MAGIC than seven days — Spark just takes whatever rows are available. If you want
# MAGIC to suppress those, filter where the row index inside the partition is `>= 7`
# MAGIC using `row_number()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Window — dense_rank (busiest days per station)
# MAGIC
# MAGIC Ranking functions assign positions to rows inside a partition. The three
# MAGIC common variants behave differently when there are ties:
# MAGIC
# MAGIC | Function | On ties | Gaps in rank? |
# MAGIC |---|---|---|
# MAGIC | `row_number()` | Arbitrary tiebreak | No |
# MAGIC | `rank()` | Same rank | Yes (skips next rank) |
# MAGIC | `dense_rank()` | Same rank | No |
# MAGIC
# MAGIC We use `dense_rank()` to find the busiest 5 days per station.

# COMMAND ----------

from pyspark.sql.functions import dense_rank

w_rank = Window.partitionBy("station_id").orderBy(col("bikes_total").desc())

df_top5 = (spark.table(GOLD_DAILY)
    .withColumn("rank_busiest", dense_rank().over(w_rank))
    .filter("rank_busiest <= 5"))

display(df_top5.filter("station_id = 4241").orderBy("rank_busiest"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Window — lag/lead and day-over-day spike flag
# MAGIC
# MAGIC `lag(col, n)` returns the value of `col` from `n` rows earlier in the
# MAGIC partition; `lead(col, n)` looks ahead. Both default to NULL when there is
# MAGIC no neighbour, which is why the first row per station has no previous-day
# MAGIC delta.
# MAGIC
# MAGIC We flag any day where the bike count grew by more than 50% relative to the
# MAGIC previous day at the same station — the kind of anomaly a data quality check
# MAGIC would surface.

# COMMAND ----------

from pyspark.sql.functions import lag

w_neighbours = Window.partitionBy("station_id").orderBy("date")

df_dod = (spark.table(GOLD_DAILY)
    .withColumn("bikes_prev_day", lag("bikes_total", 1).over(w_neighbours))
    .withColumn(
        "pct_change",
        when(col("bikes_prev_day").isNull() | (col("bikes_prev_day") == 0), None)
        .otherwise((col("bikes_total") - col("bikes_prev_day")) / col("bikes_prev_day"))
    )
    .withColumn("is_spike", col("pct_change") > 0.5))

display(df_dod.filter("station_id = 4241 AND is_spike = true").orderBy("date").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC In a real DQ pipeline you'd persist `is_spike` rows to a quarantine table
# MAGIC for review. Here we just inspect them.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Pivot — hour × weekday matrix
# MAGIC
# MAGIC `groupBy(...).pivot(col).agg(...)` reshapes from long to wide. The pivot
# MAGIC column becomes one column per distinct value. Listing the pivot values
# MAGIC explicitly is faster than letting Spark discover them, because it avoids a
# MAGIC second pass over the data.
# MAGIC
# MAGIC Output: average bike count per (hour-of-day × weekday) across all stations
# MAGIC and dates.

# COMMAND ----------

from pyspark.sql.functions import avg, sum as spark_sum

# Aggregate first to one row per (date, hour, weekday) per station, then pivot.
df_hourly = (spark.table(BRONZE_TABLE)
    .groupBy("date", "hour", "weekday")
    .agg(spark_sum(coalesce(col("VELO_IN"), lit(0)) + coalesce(col("VELO_OUT"), lit(0))).alias("bikes_hour")))

df_pivot = (df_hourly
    .groupBy("hour")
    .pivot("weekday", [1, 2, 3, 4, 5, 6, 7])
    .agg(avg("bikes_hour")))

# Rename pivot columns from "1","2",... to "mon","tue",... for clarity.
weekday_names = {1: "mon", 2: "tue", 3: "wed", 4: "thu", 5: "fri", 6: "sat", 7: "sun"}
for num, name in weekday_names.items():
    df_pivot = df_pivot.withColumnRenamed(str(num), name)

display(df_pivot.orderBy("hour"))

# COMMAND ----------

# MAGIC %md
# MAGIC The morning and evening commuter peaks should be visible on weekdays
# MAGIC (mon–fri) but flatter on weekends.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Unpivot — back to long form with `stack()`
# MAGIC
# MAGIC Sometimes you receive a wide spreadsheet-style table and want to unpivot it
# MAGIC for further analysis. Spark has no direct `unpivot()` in Python until very
# MAGIC recent versions, but the SQL `stack()` function works on every runtime.
# MAGIC
# MAGIC `stack(n, k1, v1, k2, v2, ...)` produces `n` rows from one input row, each
# MAGIC with one `(key, value)` pair.

# COMMAND ----------

df_long = df_pivot.selectExpr(
    "hour",
    "stack(7, 'mon', mon, 'tue', tue, 'wed', wed, 'thu', thu, 'fri', fri, 'sat', sat, 'sun', sun) AS (weekday_name, bikes_avg)"
)

display(df_long.orderBy("hour", "weekday_name").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC The unpivoted long-form table is what you'd typically feed into a
# MAGIC visualisation library or another window function.
