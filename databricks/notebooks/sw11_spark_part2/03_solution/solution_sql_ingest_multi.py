# Databricks notebook source

# MAGIC %md
# MAGIC # SQL Server Ingestion — Multiple Tables — Solution
# MAGIC
# MAGIC In this notebook you learn how to **ingest every table from a SQL
# MAGIC Server schema** in one run, by querying the source's `INFORMATION_SCHEMA`
# MAGIC and looping over the result. This is the standard pattern for the
# MAGIC initial load of an entire database into Bronze.
# MAGIC
# MAGIC ## Why this matters
# MAGIC
# MAGIC Hand-coding a separate read per table doesn't scale past ~10 tables and
# MAGIC drifts immediately when the source schema evolves. Querying
# MAGIC `INFORMATION_SCHEMA` makes the ingestion **schema-driven** — add a
# MAGIC table to the source and the next pipeline run picks it up automatically.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC The full `Sales` schema from the **AdventureWorks** sample database.
# MAGIC We list every base table in that schema and import each into Bronze
# MAGIC with a name like `bronze.sales_customer`, `bronze.sales_salesorderheader`,
# MAGIC etc.
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
# MAGIC database are published on **ILIAS** in the module materials. Paste
# MAGIC them into the widgets below before running. Do not share, screenshot,
# MAGIC or commit them.

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

# COMMAND ----------

dbutils.widgets.text("user", "", "SQL Server User")
dbutils.widgets.text("pw",   "", "SQL Server Password")

db_user     = dbutils.widgets.get("user")
db_password = dbutils.widgets.get("pw")

if not db_user or not db_password:
    raise ValueError("Set the 'user' and 'pw' widgets with the credentials from ILIAS.")

print(f"User: {db_user}")
print("Password: (set, not shown)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Discover tables via `INFORMATION_SCHEMA`
# MAGIC
# MAGIC The SQL Server connector accepts a parenthesised subquery as
# MAGIC `dbtable`. We use that to push a metadata query down to the source —
# MAGIC `INFORMATION_SCHEMA.TABLES` is part of the SQL standard and exposed by
# MAGIC every modern RDBMS.
# MAGIC
# MAGIC Filter `TABLE_TYPE = 'BASE TABLE'` so we don't accidentally import
# MAGIC views (their semantics may differ — views can be expensive to
# MAGIC materialise, may include parameters, etc.). For a real ingestion you
# MAGIC may want a configurable allow/deny list per table on top of this.

# COMMAND ----------

metadata_query = f"""
    (SELECT TABLE_SCHEMA, TABLE_NAME
     FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}'
       AND TABLE_TYPE   = 'BASE TABLE'
     ORDER BY TABLE_NAME) AS table_list
"""

df_tables = (spark.read
    .format("sqlserver")
    .option("host",     DB_HOST)
    .option("port",     DB_PORT)
    .option("user",     db_user)
    .option("password", db_password)
    .option("database", DB_NAME)
    .option("dbtable",  metadata_query)
    .load())

print(f"Found {df_tables.count()} tables in schema '{SOURCE_SCHEMA}'")
display(df_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Cleanup — drop existing target tables
# MAGIC
# MAGIC Drop every Bronze target this run is going to write, so we re-run
# MAGIC cleanly. We compute the target name from the same source list.

# COMMAND ----------

target_tables = [
    f"{SCHEMA_NAME}.{row['TABLE_SCHEMA']}_{row['TABLE_NAME']}".lower()
    for row in df_tables.collect()
]
for t in target_tables:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    print(f"Dropped (if existed): {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Loop over all tables and ingest
# MAGIC
# MAGIC One `spark.read.format("sqlserver").option("dbtable", ...).load()` per
# MAGIC source table, each written to a Bronze Delta table named
# MAGIC `bronze.<schema>_<table>` (lowercase). We track success and failure
# MAGIC per table — never let one bad table take down the whole batch.
# MAGIC
# MAGIC In production the loop body would also:
# MAGIC - tag each row with `load_id` / `load_timestamp` (see
# MAGIC   `solution_data_enrichment.py`)
# MAGIC - run incrementally with a watermark, not full overwrites
# MAGIC - parallelise across tables with a thread pool

# COMMAND ----------

import_summary = []

for row in df_tables.collect():
    table_schema    = row["TABLE_SCHEMA"]
    table_name      = row["TABLE_NAME"]
    full_source     = f"{table_schema}.{table_name}"
    target_table    = f"{SCHEMA_NAME}.{table_schema}_{table_name}".lower()

    print(f"\n{'=' * 60}")
    print(f"Importing: {full_source} -> {target_table}")
    print(f"{'=' * 60}")

    try:
        df = (spark.read
            .format("sqlserver")
            .option("host",     DB_HOST)
            .option("port",     DB_PORT)
            .option("user",     db_user)
            .option("password", db_password)
            .option("database", DB_NAME)
            .option("dbtable",  full_source)
            .load())

        rows = df.count()
        cols = len(df.columns)
        print(f"  Rows: {rows:,}  Columns: {cols}")

        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"  Saved.")

        import_summary.append({
            "source_table": full_source,
            "target_table": target_table,
            "rows":         rows,
            "columns":      cols,
            "status":       "Success",
        })
    except Exception as e:
        print(f"  FAILED: {e}")
        import_summary.append({
            "source_table": full_source,
            "target_table": "N/A",
            "rows":         0,
            "columns":      0,
            "status":       f"Failed: {e}",
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Import summary
# MAGIC
# MAGIC Surface the per-table outcome as a DataFrame so it's queryable. In a
# MAGIC real pipeline you'd persist this to an audit table for monitoring.

# COMMAND ----------

df_summary = spark.createDataFrame(import_summary)
display(df_summary)

# COMMAND ----------

success = sum(1 for s in import_summary if s["status"] == "Success")
total   = len(import_summary)
print(f"\nResults: {success}/{total} tables imported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Verify Bronze contents

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {SCHEMA_NAME} LIKE 'sales_*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## What's next
# MAGIC
# MAGIC - Add `load_id` / `load_timestamp` / `hash_key` to every row — see
# MAGIC   `solution_data_enrichment.py` for the recipe
# MAGIC - Replace the full overwrite with an incremental load using a
# MAGIC   watermark column (e.g. `ModifiedDate`)
# MAGIC - Apply the SCD Type 2 pattern from `solution_scd2_merge.py` for
# MAGIC   dimensional sources
# MAGIC - Schedule as a Databricks Job with the credentials moved to a Secret
# MAGIC   Scope — see the security warning at the top
