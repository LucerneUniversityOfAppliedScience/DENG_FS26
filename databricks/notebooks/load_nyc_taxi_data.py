# Databricks notebook source
# MAGIC %md
# MAGIC # Download NYC Taxi Data 

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "nyc_taxi", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import pandas as pd

# COMMAND ----------

table_name = f"{catalog}.{schema}.trips_2025"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")
print(f"Dropped table (if existed): {table_name}")

# COMMAND ----------

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-{month:02d}.parquet"

for month in range(1, 13):
    url = base_url.format(month=month)
    print(f"Loading month {month:02d}: {url}")
    try:
        pdf = pd.read_parquet(url)
        df = spark.createDataFrame(pdf)
        write_mode = "overwrite" if month == 1 else "append"
        df.write.format("delta").mode(write_mode).option("overwriteSchema", "true").saveAsTable(table_name)
        print(f"  -> {len(pdf):,} rows written (mode={write_mode})")
    except Exception as e:
        print(f"  -> Skipped (error: {e})")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(:catalog || '.' || :schema || '.trips_2025')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM IDENTIFIER(:catalog || '.' || :schema || '.trips_2025');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.vendor_list') (
# MAGIC   VendorID    INT,
# MAGIC   VendorName  STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(:catalog || '.' || :schema || '.vendor_list') VALUES
# MAGIC   (1, 'Creative Mobile Technologies (CMT)'),
# MAGIC   (2, 'Curb Mobility (Curb)'),
# MAGIC   (3, 'Arro'),
# MAGIC   (4, 'Dispatch'),
# MAGIC   (5, 'NYC Taxi'),
# MAGIC   (6, 'Myle Technologies'),
# MAGIC   (7, 'Helix');
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(:catalog || '.' || :schema || '.vendor_list');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.payment_types') (
# MAGIC   payment_type        INT,
# MAGIC   payment_description STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(:catalog || '.' || :schema || '.payment_types') VALUES
# MAGIC   (0, 'Flex Fare trip'),
# MAGIC   (1, 'Credit card'),
# MAGIC   (2, 'Cash'),
# MAGIC   (3, 'No charge'),
# MAGIC   (4, 'Dispute'),
# MAGIC   (5, 'Unknown'),
# MAGIC   (6, 'Voided trip');
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(:catalog || '.' || :schema || '.payment_types');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.rate_codes') (
# MAGIC   RatecodeID          INT,
# MAGIC   RatecodeDescription STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(:catalog || '.' || :schema || '.rate_codes') VALUES
# MAGIC   (1,  'Standard rate'),
# MAGIC   (2,  'JFK flat fare'),
# MAGIC   (3,  'Newark flat fare'),
# MAGIC   (4,  'Nassau or Westchester'),
# MAGIC   (5,  'Negotiated fare'),
# MAGIC   (6,  'Group ride'),
# MAGIC   (99, 'Unknown or null');
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(:catalog || '.' || :schema || '.rate_codes');

# COMMAND ----------

