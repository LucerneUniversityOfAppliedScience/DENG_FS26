# Databricks notebook source

# MAGIC %md
# MAGIC # SQL Server Ingestion — Single Table — Solution
# MAGIC
# MAGIC In this notebook you learn how to **ingest a single table from an
# MAGIC external SQL Server (Azure SQL Database)** into a Bronze Delta table
# MAGIC using the Databricks SQL Server connector.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Most enterprise data lives in operational SQL Server / Azure SQL
# MAGIC databases. The first step of any lakehouse migration is moving that
# MAGIC data into Bronze, where you can layer Silver/Gold transformations on
# MAGIC top without touching the operational system. The SQL Server connector
# MAGIC handles the JDBC plumbing for you.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC `Sales.Customer` from the **AdventureWorks** sample database hosted on
# MAGIC `nonacomp-sql.database.windows.net`. AdventureWorks is the classic
# MAGIC Microsoft sample schema — fictional bicycle retailer with sales,
# MAGIC inventory, HR, and production data.
# MAGIC
# MAGIC ## Before you run
# MAGIC
# MAGIC The sw11 notebooks introduced a new `landing/files` volume in the UC
# MAGIC bundle. If you cloned the repo or pulled new changes, you must
# MAGIC **redeploy the bundle** before running any sw11 notebook. In the
# MAGIC bundle UI, click **Deploy** once.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # ⚠ SECURITY WARNING — READ BEFORE RUNNING
# MAGIC
# MAGIC ## **DO NOT USE THIS PATTERN IN PRODUCTION.**
# MAGIC
# MAGIC The cells below read the SQL Server username and password from
# MAGIC **notebook widgets**. This is acceptable for **classroom demos and
# MAGIC short live exploration only**. In any real pipeline:
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
# MAGIC by Databricks — even `print(db_password)` shows `[REDACTED]`. That is
# MAGIC the only acceptable production pattern.
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
# MAGIC
# MAGIC SQL Server connection details and target schema. The host is the Azure
# MAGIC SQL Database FQDN; port 1433 is the SQL Server default.

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
# MAGIC running the next cell.
# MAGIC
# MAGIC **Reminder:** demo pattern only — see the security warning above.

# COMMAND ----------

dbutils.widgets.text("user", "", "SQL Server User")
dbutils.widgets.text("pw",   "", "SQL Server Password")

db_user     = dbutils.widgets.get("user")
db_password = dbutils.widgets.get("pw")

if not db_user or not db_password:
    raise ValueError("Set the 'user' and 'pw' widgets with the credentials from ILIAS.")

print(f"User: {db_user}")
print("Password: (set, not shown — would be [REDACTED] if read from a secret scope)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Connect to SQL Server and read the table
# MAGIC
# MAGIC The Databricks SQL Server connector uses JDBC under the hood. Options:
# MAGIC
# MAGIC | Option | Purpose |
# MAGIC |---|---|
# MAGIC | `host` | SQL Server FQDN |
# MAGIC | `port` | TCP port (1433 default) |
# MAGIC | `database` | Database name |
# MAGIC | `dbtable` | Either `Schema.Table` or a parenthesised subquery |
# MAGIC | `user` / `password` | Auth credentials |
# MAGIC
# MAGIC `.load()` is **lazy** — Spark issues `SELECT * FROM <dbtable>` to the
# MAGIC source only when an action runs (here `df.count()`).

# COMMAND ----------

df = (spark.read
    .format("sqlserver")
    .option("host",     DB_HOST)
    .option("port",     DB_PORT)
    .option("user",     db_user)
    .option("password", db_password)
    .option("database", DB_NAME)
    .option("dbtable",  SOURCE_TABLE)
    .load())

print(f"Connected. Rows: {df.count():,}, Columns: {len(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Inspect the data and schema

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

print("Columns:")
for field in df.schema.fields:
    print(f"  - {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Save as a Delta table
# MAGIC
# MAGIC Materialise the JDBC read into Bronze. From now on, downstream queries
# MAGIC hit Delta — no more round-trips to the operational SQL Server.

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)

print(f"Saved: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Verify

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 10"))

# COMMAND ----------

row_count = spark.sql(f"SELECT COUNT(*) AS c FROM {TARGET_TABLE}").collect()[0]["c"]
print(f"Total rows in Bronze: {row_count:,}")
