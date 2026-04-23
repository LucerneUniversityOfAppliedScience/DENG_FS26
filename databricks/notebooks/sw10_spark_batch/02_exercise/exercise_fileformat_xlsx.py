# Databricks notebook source

# MAGIC %md
# MAGIC # XLSX to Medallion: Exercise
# MAGIC
# MAGIC In this exercise you will load an **Excel file** with 2 sheets into Bronze tables,
# MAGIC then clean them up into Silver tables.
# MAGIC
# MAGIC Databricks Serverless does not have a native Excel reader — we read the file with
# MAGIC **pandas** and then convert it to a **PySpark** DataFrame.
# MAGIC
# MAGIC **Naming convention used below:**
# MAGIC - `pdf` = pandas DataFrame
# MAGIC - `df`  = PySpark DataFrame
# MAGIC
# MAGIC ## Learning Goals
# MAGIC - Read Excel sheets with pandas and bridge to Spark
# MAGIC - Apply the Bronze/Silver medallion pattern

# COMMAND ----------

import pandas as pd

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
# MAGIC For each sheet:
# MAGIC 1. `pdf = pd.read_excel(XLSX_PATH, sheet_name="Financials1")`
# MAGIC 2. `df = spark.createDataFrame(pdf)`
# MAGIC 3. Write `df` as a Bronze Delta table.
# MAGIC
# MAGIC Target tables:
# MAGIC - `workspace.bronze.financials_budget`
# MAGIC - `workspace.bronze.financials_sales`

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: read XLSX sheets with pandas, create Spark DataFrames, write to Bronze")

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
# MAGIC - Are there column names with typos or extra spaces?
# MAGIC
# MAGIC **Hint on casting:** Serverless Spark has ANSI mode on. For numeric casts use
# MAGIC `try_cast(col(...), "double")` instead of `.cast("double")` so that malformed or
# MAGIC empty values become `NULL` instead of raising an error.
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
