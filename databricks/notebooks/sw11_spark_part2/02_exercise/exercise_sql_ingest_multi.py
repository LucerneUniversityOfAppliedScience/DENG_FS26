# Databricks notebook source

# MAGIC %md
# MAGIC # SQL Server Ingestion — Multiple Tables — Exercise
# MAGIC
# MAGIC In this exercise you learn how to **ingest every table from a SQL
# MAGIC Server schema** in one run, by querying the source's `INFORMATION_SCHEMA`
# MAGIC and looping over the result. This is the standard pattern for the
# MAGIC initial load of an entire database into Bronze.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Hand-coding a separate read per table doesn't scale and drifts when
# MAGIC the source schema evolves. Querying `INFORMATION_SCHEMA` makes the
# MAGIC ingestion **schema-driven** — add a table to the source and the next
# MAGIC pipeline run picks it up automatically.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC The full `Sales` schema from the **AdventureWorks** sample database.
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
# MAGIC by Databricks.
# MAGIC
# MAGIC ### Where are the credentials for this exercise?
# MAGIC
# MAGIC The SQL Server **user** and **password** for the AdventureWorks demo
# MAGIC database are published on **ILIAS**. Paste them into the widgets below
# MAGIC before running. Do not share, screenshot, or commit them.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

SCHEMA_NAME = "workspace.bronze"

DB_HOST = "nonacomp-sql.database.windows.net"
DB_NAME = "AdventureWorks"
DB_PORT = "1433"

SOURCE_SCHEMA = "Sales"

print(f"Target schema  : {SCHEMA_NAME}")
print(f"SQL Server     : {DB_HOST}")
print(f"Database       : {DB_NAME}")
print(f"Source schema  : {SOURCE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Read the credentials from widgets
# MAGIC
# MAGIC Two text widgets `user` and `pw`. Paste the values from ILIAS before
# MAGIC running. Validate that both are non-empty.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 1: declare and read user/pw widgets")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Discover tables via `INFORMATION_SCHEMA`
# MAGIC
# MAGIC The SQL Server connector accepts a parenthesised subquery as
# MAGIC `dbtable`. Push a metadata query to the source:
# MAGIC
# MAGIC ```sql
# MAGIC (SELECT TABLE_SCHEMA, TABLE_NAME
# MAGIC  FROM INFORMATION_SCHEMA.TABLES
# MAGIC  WHERE TABLE_SCHEMA = 'Sales'
# MAGIC    AND TABLE_TYPE   = 'BASE TABLE'
# MAGIC  ORDER BY TABLE_NAME) AS table_list
# MAGIC ```
# MAGIC
# MAGIC Filter `TABLE_TYPE = 'BASE TABLE'` so we don't accidentally import
# MAGIC views. Read the result into `df_tables`, count the rows, display.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 2: query INFORMATION_SCHEMA via the SQL Server connector")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Cleanup — drop existing target tables
# MAGIC
# MAGIC Compute the target table name per source row as
# MAGIC `bronze.<schema>_<table>` (lowercase). Drop each one before the import
# MAGIC loop so we re-run cleanly.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 3: build target_tables list and DROP each one")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Loop over all tables and ingest
# MAGIC
# MAGIC For each row in `df_tables`:
# MAGIC 1. Read the source table via `spark.read.format("sqlserver").option("dbtable", "<schema>.<table>")`
# MAGIC 2. Write to `bronze.<schema>_<table>` (lowercase) as Delta with `mode("overwrite")`
# MAGIC 3. Track success / failure in a list of dicts (`source_table`, `target_table`, `rows`, `columns`, `status`)
# MAGIC
# MAGIC Wrap the inner read+write in `try/except` — never let one bad table
# MAGIC take down the whole batch. Print progress as you go.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 4: loop and ingest with try/except per table")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Import summary
# MAGIC
# MAGIC Convert the summary list to a DataFrame, display, and print
# MAGIC `successful / total`.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 5: render the import summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Verify Bronze contents
# MAGIC
# MAGIC Run `SHOW TABLES IN workspace.bronze LIKE 'sales_*'` and display the
# MAGIC result.

# COMMAND ----------

# YOUR CODE HERE
raise NotImplementedError("Step 6: verify Bronze contents")
