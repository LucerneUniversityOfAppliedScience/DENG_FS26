# Databricks notebook source

# MAGIC %md
# MAGIC # SQL Server Ingestion — Single Table — Exercise
# MAGIC
# MAGIC In this exercise you learn how to **ingest a single table from an
# MAGIC external SQL Server (Azure SQL Database)** into a Bronze Delta table
# MAGIC using the Databricks SQL Server connector.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Most enterprise data lives in operational SQL Server / Azure SQL
# MAGIC databases. The first step of any lakehouse migration is moving that
# MAGIC data into Bronze, where you can layer Silver/Gold transformations on
# MAGIC top without touching the operational system.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC `Sales.Customer` from the **AdventureWorks** sample database — the
# MAGIC classic Microsoft sample schema (fictional bicycle retailer).
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC Redeploy the UC bundle once if you haven't yet — sw11 added a new
# MAGIC `landing/files` volume. Click **Deploy** in the bundle UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # ⚠ SECURITY WARNING — READ BEFORE RUNNING
# MAGIC
# MAGIC ## **DO NOT USE THIS PATTERN IN PRODUCTION.**
# MAGIC
# MAGIC The cells below read the SQL Server username and password from
# MAGIC **notebook widgets**. Acceptable for **classroom demos and short live
# MAGIC exploration only**. In any real pipeline:
# MAGIC
# MAGIC - **NEVER** hard-code credentials in a notebook
# MAGIC - **NEVER** type credentials into a widget that gets saved with the notebook
# MAGIC - **NEVER** commit notebooks containing real credentials to git
# MAGIC - **NEVER** log or `print()` the password
# MAGIC
# MAGIC ### What to do in production
# MAGIC
# MAGIC Use **Databricks Secret Scopes** (backed by Azure Key Vault, AWS
# MAGIC Secrets Manager, or a Databricks-backed scope), and read with:
# MAGIC
# MAGIC ```python
# MAGIC db_user     = dbutils.secrets.get(scope="adventureworks", key="user")
# MAGIC db_password = dbutils.secrets.get(scope="adventureworks", key="password")
# MAGIC ```
# MAGIC
# MAGIC Secrets fetched this way are **redacted from notebook output and logs**
# MAGIC by Databricks — even `print(db_password)` shows `[REDACTED]`.
# MAGIC
# MAGIC ### Where are the credentials for this exercise?
# MAGIC
# MAGIC The SQL Server **user** and **password** for the AdventureWorks demo
# MAGIC database are published on **ILIAS** in the module materials. Paste
# MAGIC them into the widgets below before running. Do not share, screenshot,
# MAGIC or commit them.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

SCHEMA_NAME  = "workspace.bronze"
TARGET_TABLE = f"{SCHEMA_NAME}.adventureworks_customer"

DB_HOST = "nonacomp-sql.database.windows.net"
DB_NAME = "AdventureWorks"
DB_PORT = "1433"

SOURCE_TABLE = "Sales.Customer"

print(f"Target table: {TARGET_TABLE}")
print(f"SQL Server  : {DB_HOST}")
print(f"Database    : {DB_NAME}")
print(f"Source table: {SOURCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup: drop the target table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
print(f"Dropped (if existed): {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read the credentials from widgets
# MAGIC
# MAGIC Two text widgets `user` and `pw`. Paste the values from ILIAS before
# MAGIC running. Use `dbutils.widgets.text("name", "", "label")` to declare
# MAGIC them, then `dbutils.widgets.get("name")` to read.
# MAGIC
# MAGIC Validate that both values are non-empty — raise a clear error
# MAGIC otherwise so the failure mode is obvious.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: declare and read user/pw widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Connect to SQL Server and read the table
# MAGIC
# MAGIC Use `spark.read.format("sqlserver")` with options `host`, `port`,
# MAGIC `database`, `dbtable`, `user`, `password`. `.load()` is lazy — call
# MAGIC `.count()` to actually run the query and print the row count.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: read Sales.Customer via the SQL Server connector")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Inspect the data and schema
# MAGIC
# MAGIC `display(df.limit(10))`, `df.printSchema()`, and a small loop printing
# MAGIC `field.name: field.dataType` per column.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: inspect data and schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Save as a Delta table
# MAGIC
# MAGIC Write the JDBC DataFrame to `TARGET_TABLE` as Delta with
# MAGIC `mode("overwrite")`. From here on, downstream queries hit Delta — no
# MAGIC more round-trips to the operational system.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: write to TARGET_TABLE")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Verify

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: verify with a SELECT and a row count")
