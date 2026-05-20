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
dbutils.widgets.dropdown("starting_offsets", "earliest", ["earliest", "latest"], "Starting offsets")
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

# DBTITLE 1,(Optional) Reset checkpoints for this topic
# When you change the *shape* of a streaming query (e.g. switched from
# `display(streaming_df, …)` to a memory sink, or renamed columns)
# Spark refuses to resume from the existing checkpoint and prints:
#
#   This query does not support recovering from checkpoint location.
#   Delete .../offsets to start over.
#
# Flip the widget below to "yes" once to wipe the topic's checkpoint
# subtree and start fresh, then flip it back to "no" so you don't
# accidentally lose progress on the next run.
dbutils.widgets.dropdown("cleanup_checkpoints", "no", ["no", "yes"],
                         "Wipe checkpoints for this topic?")
cleanup = dbutils.widgets.get("cleanup_checkpoints")

topic_checkpoints = f"{checkpoint_root}/{topic}"

if cleanup == "yes":
    try:
        dbutils.fs.rm(topic_checkpoints, recurse=True)
        print(f"✓ Deleted {topic_checkpoints}")
    except Exception as e:
        # Volume path didn't exist yet — also fine.
        print(f"(nothing to delete: {e})")
else:
    print(f"Checkpoint subtree kept: {topic_checkpoints}")
    print("Set the 'cleanup_checkpoints' widget to 'yes' if you want to reset.")

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

# Sanity check — the JAAS config MUST start with "kafkashaded." on
# Databricks. If it doesn't, restart the kernel and re-run from the
# top: a stale variable from an earlier run can leak the unshaded
# prefix into the streaming query and cause
# "No LoginModule found for org.apache.kafka.common.security.scram.ScramLoginModule".
assert jaas_config.startswith("kafkashaded."), (
    "JAAS prefix is wrong — restart the kernel and Run All from the top."
)
print(f"Kafka options built. JAAS module: {login_module}")

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

# DBTITLE 1,Snapshot via memory sink — raw key/value
# `display(streaming_df, …)` is fragile on Free Edition (defaults to
# `complete` output mode, which non-aggregated streams reject). The
# robust pattern is:
#
#   writeStream → format("memory") → trigger(availableNow=True) → start()
#   awaitTermination()  ← wait for the micro-batch to finish
#   display(spark.table("name"))  ← now a plain static query
#
# Each call processes whatever is in Kafka right now and stops. Re-run
# the cell to refresh.
#
# Empty result? Two likely reasons:
#  1. `starting_offsets="latest"` plus `availableNow` = "from now on,
#     until now" → 0 rows. Switch the widget to `earliest`.
#  2. The checkpoint already consumed all messages on a previous run.
#     Set `cleanup_checkpoints` to `yes` once and re-run.
query_raw = (
    decoded.writeStream
        .format("memory")
        .queryName("kafka_raw")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation",
                f"{checkpoint_root}/{topic}/_preview_raw")
        .start()
)
query_raw.awaitTermination()
print(f"Memory table 'kafka_raw' refreshed.")

# COMMAND ----------

# DBTITLE 1,Look at the raw data
display(spark.sql("""
    SELECT *
    FROM kafka_raw
    ORDER BY timestamp DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — parse the payload (`logistics_data_gen`)
# MAGIC
# MAGIC The producer emits **binary Avro** following this schema
# MAGIC (namespace `data.gen.avro`, record `logistics`):
# MAGIC
# MAGIC ```text
# MAGIC long   time_utc           # epoch seconds
# MAGIC string tracking_id        # e.g. "track-1974256721"
# MAGIC string message            # e.g. "transfer"
# MAGIC string carrier            # AN_POST | DHL | USPS | R_MAIL
# MAGIC array<string> manifest
# MAGIC string next_hop_location  # DUB | LON | BER | NYC | PIT | TOR | MAD
# MAGIC string state              # Received | Delivered
# MAGIC ```
# MAGIC
# MAGIC Binary Avro can't be parsed with `from_json` (you'd get all nulls —
# MAGIC the bytes look nothing like text JSON). We use `from_avro` from
# MAGIC `pyspark.sql.avro.functions` and pass the schema as a JSON string.
# MAGIC
# MAGIC > If the resulting columns are still null, the producer is using
# MAGIC > **Confluent Schema Registry framing** — every Kafka message
# MAGIC > starts with `0x00` + 4-byte schema ID before the Avro payload.
# MAGIC > Strip those 5 bytes with `expr("substring(value, 6,
# MAGIC > length(value) - 5)")` before calling `from_avro`. The cell
# MAGIC > below has the variant ready as a comment.

# COMMAND ----------

# DBTITLE 1,Avro parsing for logistics_data_gen
from pyspark.sql.functions import from_unixtime, to_timestamp, expr
from pyspark.sql.avro.functions import from_avro

# Avro schema as a JSON string — strip the "examples" metadata that
# Aiven's generator UI adds (Avro itself doesn't understand it).
logistics_avro_schema = """
{
  "type": "record",
  "name": "logistics",
  "namespace": "data.gen.avro",
  "fields": [
    {"name": "time_utc",          "type": "long"},
    {"name": "tracking_id",       "type": "string"},
    {"name": "message",           "type": "string"},
    {"name": "carrier",           "type": "string"},
    {"name": "manifest",          "type": {"type": "array", "items": "string"}},
    {"name": "next_hop_location", "type": "string"},
    {"name": "state",             "type": "string"}
  ]
}
"""

# Aiven framing: every message starts with a 5-byte Confluent prefix
#   byte 0:    0x00 magic byte
#   bytes 1-4: 4-byte big-endian Avro schema id
# from_avro chokes on those bytes with
#   "Malformed data. Length is negative: -NN"
# so we strip them with substring(value, 6, length(value) - 5).
# `mode=PERMISSIVE` is a safety net — a single bad message returns nulls
# instead of killing the whole streaming query.

payload_bytes = expr("substring(value, 6, length(value) - 5)")

parsed = (
    raw_stream
        .select(
            "topic",
            "partition",
            "offset",
            col("timestamp").alias("kafka_ts"),
            from_avro(
                payload_bytes,
                logistics_avro_schema,
                {"mode": "PERMISSIVE"},
            ).alias("payload"),
        )
        .select(
            "topic", "partition", "offset", "kafka_ts",
            to_timestamp(from_unixtime(col("payload.time_utc"))).alias("event_ts"),
            "payload.tracking_id",
            "payload.message",
            "payload.carrier",
            "payload.manifest",
            "payload.next_hop_location",
            "payload.state",
        )
)

# --- alternative: plain Avro single-object encoding (no Confluent prefix) ---
# Use this if your producer does NOT use Schema Registry. Pass the raw
# `value` column straight to from_avro.
#
# parsed = (
#     raw_stream
#         .select(
#             "topic", "partition", "offset",
#             col("timestamp").alias("kafka_ts"),
#             from_avro(raw_stream["value"], logistics_avro_schema,
#                       {"mode": "PERMISSIVE"}).alias("payload"),
#         )
#         .select(
#             "topic", "partition", "offset", "kafka_ts",
#             to_timestamp(from_unixtime(col("payload.time_utc"))).alias("event_ts"),
#             "payload.tracking_id",
#             "payload.message",
#             "payload.carrier",
#             "payload.manifest",
#             "payload.next_hop_location",
#             "payload.state",
#         )
# )

# DBTITLE 1,Snapshot via memory sink — parsed view
query_parsed = (
    parsed.writeStream
        .format("memory")
        .queryName("kafka_parsed")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation",
                f"{checkpoint_root}/{topic}/_preview_parsed")
        .start()
)
query_parsed.awaitTermination()
print("Memory table 'kafka_parsed' refreshed.")

# COMMAND ----------

# DBTITLE 1,Look at the parsed data
display(spark.sql("""
    SELECT *
    FROM kafka_parsed
    ORDER BY kafka_ts DESC
    LIMIT 50
"""))

