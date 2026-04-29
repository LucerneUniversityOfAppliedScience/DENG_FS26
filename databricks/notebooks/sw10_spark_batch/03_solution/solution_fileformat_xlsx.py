# Databricks notebook source

# MAGIC %md
# MAGIC # XLSX to Medallion: Solution
# MAGIC
# MAGIC Databricks Serverless does not have the `com.crealytics.spark.excel` package.
# MAGIC We read the Excel file with **pandas** (`pdf`) and then convert it to a
# MAGIC **PySpark DataFrame** (`df`) for the Bronze/Silver pipeline.

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re

import pandas as pd

XLSX_PATH = "/Volumes/workspace/raw/sample_data/xlsx/FinancialsSampleData.xlsx"
print(f"XLSX file: {XLSX_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop existing tables
# MAGIC
# MAGIC Drop any existing Bronze/Silver tables before re-running so a stale schema
# MAGIC from a previous run does not block the overwrite (Delta refuses schema
# MAGIC changes on `overwrite` when Table ACLs are enabled).

# COMMAND ----------

for table in [
    "workspace.bronze.financials_budget",
    "workspace.bronze.financials_sales",
    "workspace.silver.financials_budget",
    "workspace.silver.financials_sales",
]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped (if existed): {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read both sheets into Bronze
# MAGIC
# MAGIC `pd.read_excel` can read one sheet at a time. We create one pandas DataFrame
# MAGIC per sheet, convert it to a Spark DataFrame, then persist it as a Bronze Delta
# MAGIC table.

# COMMAND ----------

pdf_budget = pd.read_excel(XLSX_PATH, sheet_name="Financials1")
df_budget = spark.createDataFrame(pdf_budget)

print(f"Financials1: {df_budget.count()} rows")
df_budget.printSchema()

# COMMAND ----------

pdf_sales = pd.read_excel(XLSX_PATH, sheet_name="Financials2")
df_sales = spark.createDataFrame(pdf_sales)

print(f"Financials2: {df_sales.count()} rows")
df_sales.printSchema()

# COMMAND ----------

def clean_cols(df):
    return df.toDF(*[re.sub(r"[ ,;{}()\n\t=]", "_", c) for c in df.columns])

df_budget = clean_cols(df_budget)
df_sales = clean_cols(df_sales)

df_budget.write.mode("overwrite").saveAsTable("workspace.bronze.financials_budget")
df_sales.write.mode("overwrite").saveAsTable("workspace.bronze.financials_sales")
print("Bronze tables written")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.financials_budget LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.financials_sales LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Clean up to Silver
# MAGIC
# MAGIC **Issues found:**
# MAGIC - `Financials1`: Column name typo "Businees Unit" → "Business Unit"
# MAGIC - `Financials2`: Column " Sales" has a leading space

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.bronze.financials_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.bronze.financials_sales

# COMMAND ----------

from pyspark.sql.functions import expr

# Silver: Budget table — fix column name typo, ensure numeric types for months
df_budget_silver = (spark.table("workspace.bronze.financials_budget")
    .withColumnRenamed("Businees_Unit", "business_unit")
    .withColumnRenamed("Account", "account")
    .withColumnRenamed("Currency", "currency")
    .withColumnRenamed("Year", "year")
    .withColumnRenamed("Scenario", "scenario")
)

# Rename month columns to lowercase first, then cast — avoid withColumn+drop with
# case-only differences (Spark resolves names case-insensitively by default and
# would treat Jan and jan as the same column).
for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]:
    df_budget_silver = df_budget_silver.withColumnRenamed(month, month.lower())

for month in ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]:
    df_budget_silver = df_budget_silver.withColumn(month, expr(f"try_cast({month} as double)"))

df_budget_silver.write.mode("overwrite").saveAsTable("workspace.silver.financials_budget")
print("Silver table written: workspace.silver.financials_budget")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.financials_budget LIMIT 10

# COMMAND ----------

# Silver: Sales table — trim column names and cast numeric types
df_sales_silver = (spark.table("workspace.bronze.financials_sales")
    .withColumnRenamed("Segment", "segment")
    .withColumnRenamed("Country", "country")
    .withColumnRenamed("Product", "product")
    .withColumnRenamed("Discount_Band", "discount_band")
    .withColumnRenamed("Units_Sold", "units_sold")
    .withColumnRenamed("Manufacturing_Price", "manufacturing_price")
    .withColumnRenamed("Sale_Price", "sale_price")
    .withColumnRenamed("Gross_Sales", "gross_sales")
    .withColumnRenamed("Discounts", "discounts")
    .withColumnRenamed("_Sales", "sales")
    .withColumnRenamed("COGS", "cogs")
    .withColumnRenamed("Profit", "profit")
    .withColumnRenamed("Date", "date")
    .withColumnRenamed("Month_Number", "month_number")
    .withColumnRenamed("Month_Name", "month_name")
    .withColumnRenamed("Year", "year")
    .withColumn("units_sold", expr("try_cast(units_sold as double)"))
    .withColumn("manufacturing_price", expr("try_cast(manufacturing_price as double)"))
    .withColumn("sale_price", expr("try_cast(sale_price as double)"))
    .withColumn("gross_sales", expr("try_cast(gross_sales as double)"))
    .withColumn("discounts", expr("try_cast(discounts as double)"))
    .withColumn("sales", expr("try_cast(sales as double)"))
    .withColumn("cogs", expr("try_cast(cogs as double)"))
    .withColumn("profit", expr("try_cast(profit as double)"))
    .withColumn("month_number", expr("try_cast(month_number as int)"))
    .withColumn("year", expr("try_cast(year as int)"))
)

df_sales_silver.write.mode("overwrite").saveAsTable("workspace.silver.financials_sales")
print("Silver table written: workspace.silver.financials_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.financials_sales LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick check: profit by segment
# MAGIC SELECT segment, round(sum(profit), 2) AS total_profit
# MAGIC FROM workspace.silver.financials_sales
# MAGIC GROUP BY segment
# MAGIC ORDER BY total_profit DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.bronze.financials_budget;
# MAGIC -- DROP TABLE IF EXISTS workspace.bronze.financials_sales;
# MAGIC -- DROP TABLE IF EXISTS workspace.silver.financials_budget;
# MAGIC -- DROP TABLE IF EXISTS workspace.silver.financials_sales;
