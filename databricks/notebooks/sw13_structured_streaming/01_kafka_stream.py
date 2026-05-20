# Databricks notebook source
# MAGIC %md
# MAGIC # Read the Aiven Kafka stream
# MAGIC
# MAGIC Connect Databricks Structured Streaming to the Aiven-hosted Kafka
# MAGIC cluster (topic `logistics_data_gen`) and peek at the live messages.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. Run [`00_setup`](./00_setup) first. It stores the connection
# MAGIC    details in the secret scope `secret_scope` (`host`, `port`,
# MAGIC    `user`, `password`).
# MAGIC 2. **Upload the Aiven CA certificate** to a Unity Catalog Volume so
# MAGIC    every executor can read it. Aiven signs its broker certificates
# MAGIC    with its own CA — the JDK's default truststore doesn't trust it,
# MAGIC    so SSL would fail without this step.
# MAGIC
# MAGIC    - Download `ca.pem` from the Aiven service console
# MAGIC      *(Service → Overview → CA Certificate)*.
# MAGIC    - Upload it to a Volume, e.g.
# MAGIC      `/Volumes/workspace/landing/files/aiven/ca.pem`
# MAGIC      (Data → Volumes → upload).
# MAGIC    - Adjust the `truststore_path` widget below if you put it
# MAGIC      somewhere else.
# MAGIC
# MAGIC ### ⚠️ Security note — cert and key handling
# MAGIC
# MAGIC | Artefact | Sensitivity | Where it belongs |
# MAGIC |---|---|---|
# MAGIC | `ca.pem` (the Aiven CA cert) | Public — anyone could fetch it. **But** it pins your client to *this* Aiven project, so still treat it as semi-internal. | UC Volume with a Unity Catalog grant limiting it to your workspace / class group. |
# MAGIC | `service.cert` + `service.key` (client mTLS, if you switch from SASL) | **Private key — full impersonation** of your service if leaked. | Databricks **Secret Scope** (one secret per file content), *not* a Volume. |
# MAGIC | SASL password (`AVNS_…`) | **Sensitive** — full broker access. | Already in `secret_scope` via `00_setup`. Never paste it into a notebook cell. |
# MAGIC
# MAGIC **Things to avoid:**
# MAGIC
# MAGIC - ❌ Don't commit `ca.pem`, `service.cert` or `service.key` to Git —
# MAGIC   if your `databricks/` folder is synced to a repo, add the cert
# MAGIC   path to `.gitignore` or keep the cert only in Volumes / Secrets.
# MAGIC - ❌ Don't drop the cert into a public Volume (`/Volumes/<catalog>/<schema>/<volume>`
# MAGIC   with broad `READ VOLUME` grants). Restrict the volume's grants to
# MAGIC   the user/group that actually needs it
# MAGIC   (`GRANT READ VOLUME ON VOLUME workspace.landing.files TO <group>`).
# MAGIC - ❌ Don't `print(open(ca_pem).read())` in a notebook that is shared
# MAGIC   or scheduled — the cert content ends up in the run history.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC 1. Read the Kafka credentials from the secret scope.
# MAGIC 2. Build the SASL_SSL/SCRAM connection options, pointing the Kafka
# MAGIC    client at the Aiven CA.
# MAGIC 3. Open a `readStream` and `display()` the live messages.
# MAGIC 4. Parse the JSON payload into proper columns.

# COMMAND ----------

# DBTITLE 1,Widgets
# Topic, starting offset, SASL mechanism and the path to the Aiven CA.
dbutils.widgets.text("topic",            "logistics_data_gen", "Kafka topic")
dbutils.widgets.dropdown("starting_offsets", "latest", ["latest", "earliest"], "Starting offsets")
dbutils.widgets.dropdown("sasl_mechanism",   "SCRAM-SHA-256",
                         ["SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"], "SASL mechanism")
dbutils.widgets.text("truststore_path",
                     "/Volumes/workspace/landing/files/aiven/ca.pem",
                     "Path to Aiven CA (ca.pem)")
# Checkpoint location for the live `display()` previews below.
# On Databricks Free Edition / serverless, implicit temp checkpoints
# are disabled, so we always pass an explicit one.
dbutils.widgets.text("checkpoint_root",
                     "/Volumes/workspace/landing/files/sw13_checkpoints",
                     "Checkpoint root (writable Volume)")

topic            = dbutils.widgets.get("topic")
starting_offsets = dbutils.widgets.get("starting_offsets")
sasl_mechanism   = dbutils.widgets.get("sasl_mechanism")
truststore_path  = dbutils.widgets.get("truststore_path")
checkpoint_root  = dbutils.widgets.get("checkpoint_root").rstrip("/")

print(f"Topic            : {topic}")
print(f"Starting offsets : {starting_offsets}")
print(f"SASL mechanism   : {sasl_mechanism}")
print(f"Truststore (CA)  : {truststore_path}")
print(f"Checkpoint root  : {checkpoint_root}")

# Fail fast if the CA file isn't there — every executor needs to read it.
import os
if not os.path.exists(truststore_path):
    raise FileNotFoundError(
        f"CA file not found at {truststore_path}. Upload ca.pem from "
        "the Aiven service console into a Unity Catalog Volume and "
        "point this widget at it."
    )
print(f"✓ CA file found ({os.path.getsize(truststore_path)} bytes)")

# COMMAND ----------

# DBTITLE 1,Load credentials from the secret scope
SCOPE = "secret_scope"

host     = dbutils.secrets.get(SCOPE, "host")
port     = dbutils.secrets.get(SCOPE, "port")
user     = dbutils.secrets.get(SCOPE, "user")
password = dbutils.secrets.get(SCOPE, "password")

bootstrap_servers = f"{host}:{port}"

# Sanity output — values themselves are redacted by Databricks.
print(f"Bootstrap servers : {bootstrap_servers}")
print(f"User              : {user}")
print(f"Password          : (redacted, length={len(password)})")

# COMMAND ----------

# DBTITLE 1,Build the SASL JAAS config
# Spark's Kafka client expects the JAAS config as a single string. Which
# login module to use depends on the SASL mechanism:
#   SCRAM-* -> ...security.scram.ScramLoginModule
#   PLAIN   -> ...security.plain.PlainLoginModule
#
# IMPORTANT: Databricks ships a *shaded* Kafka client to avoid version
# conflicts with user libraries — the classes live under the
# `kafkashaded.org.apache.kafka` prefix. Using the plain
# `org.apache.kafka` prefix throws
#   "No LoginModule found for org.apache.kafka.common.security.scram.ScramLoginModule".
# On non-Databricks Spark you'd drop the `kafkashaded.` prefix.
LOGIN_MODULE_PREFIX = "kafkashaded.org.apache.kafka"

if sasl_mechanism.startswith("SCRAM"):
    login_module = f"{LOGIN_MODULE_PREFIX}.common.security.scram.ScramLoginModule"
else:
    login_module = f"{LOGIN_MODULE_PREFIX}.common.security.plain.PlainLoginModule"

jaas_config = (
    f'{login_module} required '
    f'username="{user}" password="{password}";'
)

# These options are reused for both readStream and writeStream calls.
# The PEM truststore tells the Kafka client to trust certificates
# signed by the Aiven CA. Without it SSL handshake fails with
# "PKIX path building failed".
kafka_options = {
    "kafka.bootstrap.servers":    bootstrap_servers,
    "kafka.security.protocol":    "SASL_SSL",
    "kafka.sasl.mechanism":       sasl_mechanism,
    "kafka.sasl.jaas.config":     jaas_config,
    "kafka.ssl.truststore.type":     "PEM",
    "kafka.ssl.truststore.location": truststore_path,
}

print("Kafka options built (jaas_config not printed for safety).")

# COMMAND ----------

# DBTITLE 1,Open the stream
# `readStream` returns a streaming DataFrame whose schema is fixed:
#   key:           binary
#   value:         binary
#   topic:         string
#   partition:     int
#   offset:        long
#   timestamp:     timestamp     (broker-side ingestion time)
#   timestampType: int
#
# `value` carries the actual payload bytes — JSON, Avro, Protobuf, …
# depending on what the producer wrote.
raw_stream = (
    spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
)

raw_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Decode key and value to strings
from pyspark.sql.functions import col, expr

decoded = (
    raw_stream
        .selectExpr(
            "CAST(key   AS STRING) AS key",
            "CAST(value AS STRING) AS value",
            "topic",
            "partition",
            "offset",
            "timestamp",
        )
)

decoded.printSchema()

# COMMAND ----------

# DBTITLE 1,Snapshot preview
# Free Edition / serverless gotchas baked in:
#  1. `checkpointLocation` — implicit temp checkpoints are disabled.
#  2. `outputMode="append"` — non-aggregated streams only support
#     "append". Without it Databricks may pick "complete" and fail
#     with STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION.
#  3. `trigger={"availableNow": True}` — continuous triggers (default
#     ProcessingTime) are blocked on this cluster type with
#     INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED. AvailableNow processes
#     whatever is in Kafka right now, then stops. To see fresh data,
#     re-run the cell.
display(
    decoded,
    checkpointLocation=f"{checkpoint_root}/{topic}/_preview_raw",
    outputMode="append",
    trigger={"availableNow": True},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — parse the payload (`logistics_data_gen`)
# MAGIC
# MAGIC The producer emits records that follow this Avro schema:
# MAGIC
# MAGIC ```text
# MAGIC record logistics {
# MAGIC   long   time_utc           # epoch seconds
# MAGIC   string tracking_id        # e.g. "track-1974256721"
# MAGIC   string message            # e.g. "transfer"
# MAGIC   string carrier            # AN_POST | DHL | USPS | R_MAIL
# MAGIC   array<string> manifest
# MAGIC   string next_hop_location  # DUB | LON | BER | NYC | PIT | TOR | MAD
# MAGIC   string state              # Received | Delivered
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Aiven's Kafka Data Generator delivers these records as **JSON**
# MAGIC on the wire (one JSON object per Kafka message), so we parse with
# MAGIC `from_json`. We also convert `time_utc` (epoch seconds) into a
# MAGIC proper timestamp.
# MAGIC
# MAGIC > If the `value` column in the live preview above shows binary
# MAGIC > garbage instead of readable JSON, the topic is encoded with
# MAGIC > Confluent-style binary Avro — in that case skip `from_json` and
# MAGIC > use `from_avro` from the `spark-avro` package.

# COMMAND ----------

# DBTITLE 1,JSON parsing for logistics_data_gen
from pyspark.sql.functions import from_json, from_unixtime, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, ArrayType,
)

logistics_schema = StructType([
    StructField("time_utc",          LongType()),
    StructField("tracking_id",       StringType()),
    StructField("message",           StringType()),
    StructField("carrier",           StringType()),
    StructField("manifest",          ArrayType(StringType())),
    StructField("next_hop_location", StringType()),
    StructField("state",             StringType()),
])

parsed = (
    decoded
        .select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            from_json(col("value"), logistics_schema).alias("payload"),
        )
        .select(
            "topic", "partition", "offset",
            col("timestamp").alias("kafka_ts"),
            to_timestamp(from_unixtime(col("payload.time_utc"))).alias("event_ts"),
            "payload.tracking_id",
            "payload.message",
            "payload.carrier",
            "payload.manifest",
            "payload.next_hop_location",
            "payload.state",
        )
)

display(
    parsed,
    checkpointLocation=f"{checkpoint_root}/{topic}/_preview_parsed",
    outputMode="append",
    trigger={"availableNow": True},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step — persist to a Delta table
# MAGIC
# MAGIC Once you trust the parsing, write the stream into Bronze with a
# MAGIC checkpoint so the load is resumable:
# MAGIC
# MAGIC ```python
# MAGIC (parsed.writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation",
# MAGIC             f"{checkpoint_root}/{topic}/bronze")
# MAGIC     .outputMode("append")
# MAGIC     .trigger(availableNow=True)   # required on Free Edition / serverless
# MAGIC     .toTable(f"workspace.bronze.{topic.replace('-', '_')}_raw")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC On Free Edition this is a *micro-batch ETL pattern*: schedule the
# MAGIC notebook every few minutes and each run picks up everything that
# MAGIC arrived since the last checkpoint, then stops. Same checkpoint
# MAGIC across runs means no duplicates.
# MAGIC
# MAGIC That's the canonical Kafka → Bronze pattern. From there continue
# MAGIC with Silver (cleaned) and Gold (aggregated) as in the previous
# MAGIC weeks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fallback — preview via the memory sink
# MAGIC
# MAGIC If `display(streaming_df, …)` still misbehaves on your workspace
# MAGIC (e.g. checkpoint-related errors after a previous run), write
# MAGIC the stream into an **in-memory table** and query that table like
# MAGIC any normal DataFrame:
# MAGIC
# MAGIC 1. Run the next cell — it starts a streaming query with the
# MAGIC    `availableNow` trigger. It processes everything currently in
# MAGIC    Kafka, writes it into `kafka_raw_preview`, then stops on its
# MAGIC    own.
# MAGIC 2. Run the `display(spark.sql(...))` cell to look at the
# MAGIC    snapshot. Re-run the previous cell to refresh.
# MAGIC 3. There is no continuous query to clean up — Free Edition
# MAGIC    doesn't support `ProcessingTime` triggers anyway.

# COMMAND ----------

# DBTITLE 1,Start the memory-sink query (optional fallback)
# `availableNow=True` is required on Free Edition / serverless. The
# query processes everything currently in Kafka and stops on its own —
# no need to call `query.stop()` manually. Re-run the cell to pick up
# new messages.
query = (
    parsed.writeStream
        .format("memory")
        .queryName("kafka_raw_preview")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation",
                f"{checkpoint_root}/{topic}/_memory_preview")
        .start()
)
query.awaitTermination()    # wait until the batch finished writing
print(f"Done. Memory table 'kafka_raw_preview' now has the latest snapshot.")

# COMMAND ----------

# DBTITLE 1,Snapshot of the memory table
display(spark.sql("SELECT * FROM kafka_raw_preview ORDER BY kafka_ts DESC LIMIT 200"))

# COMMAND ----------

# DBTITLE 1,(Optional) Stop the memory-sink query
# With `availableNow=True` the query already stopped itself — this is
# only here for completeness if you switch to a continuous trigger on
# a non-Free cluster.
# query.stop()
