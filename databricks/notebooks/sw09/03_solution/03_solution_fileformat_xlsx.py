# Databricks notebook source

# MAGIC %md
# MAGIC # XLSX to Medallion: Solution

# COMMAND ----------

XLSX_PATH = "/Volumes/workspace/raw/sample_data/xlsx/FinancialsSampleData.xlsx"

print(f"XLSX file: {XLSX_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read both sheets into Bronze

# COMMAND ----------

df_budget = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'Financials1'!A1")
    .load(XLSX_PATH))

print(f"Financials1: {df_budget.count()} rows")
df_budget.printSchema()

# COMMAND ----------

df_sales = (spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'Financials2'!A1")
    .load(XLSX_PATH))

print(f"Financials2: {df_sales.count()} rows")
df_sales.printSchema()

# COMMAND ----------

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
# MAGIC - `Financials1`: Column name typo "Businees Unit" → "Business Unit", monthly values may be strings
# MAGIC - `Financials2`: `Date` column is an Excel serial number (days since 1900-01-01), column " Sales" has a leading space

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.bronze.financials_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.bronze.financials_sales

# COMMAND ----------

from pyspark.sql.functions import col

# Silver: Budget table — fix column name typo, ensure numeric types
df_budget_silver = (spark.table("workspace.bronze.financials_budget")
    .withColumnRenamed("Businees Unit", "business_unit")
    .withColumnRenamed("Account", "account")
    .withColumnRenamed("Currency", "currency")
    .withColumnRenamed("Year", "year")
    .withColumnRenamed("Scenario", "scenario")
)

# Cast monthly columns to double
for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]:
    df_budget_silver = df_budget_silver.withColumn(month.lower(), col(month).cast("double")).drop(month)

df_budget_silver.write.mode("overwrite").saveAsTable("workspace.silver.financials_budget")
print("Silver table written: workspace.silver.financials_budget")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.financials_budget LIMIT 10

# COMMAND ----------

from pyspark.sql.functions import expr

# Silver: Sales table — fix Date serial number, trim column names, cast types
df_sales_silver = (spark.table("workspace.bronze.financials_sales")
    .withColumnRenamed("Segment", "segment")
    .withColumnRenamed("Country", "country")
    .withColumnRenamed("Product", "product")
    .withColumnRenamed("Discount Band", "discount_band")
    .withColumn("units_sold", col("Units Sold").cast("double")).drop("Units Sold")
    .withColumn("manufacturing_price", col("Manufacturing Price").cast("double")).drop("Manufacturing Price")
    .withColumn("sale_price", col("Sale Price").cast("double")).drop("Sale Price")
    .withColumn("gross_sales", col("Gross Sales").cast("double")).drop("Gross Sales")
    .withColumn("discounts", col("Discounts").cast("double")).drop("Discounts")
    .withColumn("sales", col(" Sales").cast("double")).drop(" Sales")
    .withColumn("cogs", col("COGS").cast("double")).drop("COGS")
    .withColumn("profit", col("Profit").cast("double")).drop("Profit")
    # Date is Excel serial number: days since 1899-12-30
    .withColumn("date", expr("date_add('1899-12-30', CAST(Date AS INT))")).drop("Date")
    .withColumn("month_number", col("Month Number").cast("int")).drop("Month Number")
    .withColumnRenamed("Month Name", "month_name")
    .withColumn("year", col("Year").cast("int")).drop("Year")
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
