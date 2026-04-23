# Databricks notebook source

# MAGIC %md
# MAGIC # XLSX to Medallion: Exercise
# MAGIC
# MAGIC In this exercise you will load an **Excel file** with 2 sheets into Bronze tables,
# MAGIC then clean them up into Silver tables.
# MAGIC
# MAGIC ## Learning Goals
# MAGIC - Read Excel files with Spark (multiple sheets)
# MAGIC - Apply the Bronze/Silver medallion pattern

# COMMAND ----------

XLSX_PATH = "/Volumes/workspace/raw/sample_data/xlsx/FinancialsSampleData.xlsx"

print(f"XLSX file: {XLSX_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read both sheets into Bronze
# MAGIC
# MAGIC The file `FinancialsSampleData.xlsx` has **2 sheets**:
# MAGIC - **Financials1**: Monthly budget data (Account, Business Unit, Year, Jan–Dec)
# MAGIC - **Financials2**: Sales transactions (Segment, Country, Product, Sales, Profit, ...)
# MAGIC
# MAGIC Use `spark.read.format("com.crealytics.spark.excel")` to read each sheet.
# MAGIC
# MAGIC Key options:
# MAGIC - `.option("header", "true")` — first row contains column names
# MAGIC - `.option("dataAddress", "'Financials1'!A1")` — specify which sheet to read
# MAGIC
# MAGIC Save each sheet as a separate Bronze table:
# MAGIC - `workspace.bronze.financials_budget`
# MAGIC - `workspace.bronze.financials_sales`

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: read XLSX sheets and write to Bronze")

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
# MAGIC Inspect the Bronze tables:
# MAGIC 1. Check the data types with `DESCRIBE`
# MAGIC 2. Are there any issues that need fixing?
# MAGIC
# MAGIC **Hints to look for:**
# MAGIC - Are numeric columns actually numeric or stored as strings?
# MAGIC - Does the `Date` column in Financials2 look correct?
# MAGIC - Are there column names with typos or extra spaces?
# MAGIC
# MAGIC Create Silver tables:
# MAGIC - `workspace.silver.financials_budget`
# MAGIC - `workspace.silver.financials_sales`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.bronze.financials_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE workspace.bronze.financials_sales

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: clean up to Silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.financials_budget LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.financials_sales LIMIT 10

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
