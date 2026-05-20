# Databricks notebook source
# MAGIC %md
# MAGIC # Produce Events to Aiven Kafka
# MAGIC
# MAGIC The mirror image of [`01_kafka_stream`](./01_kafka_stream): instead of
# MAGIC reading the Aiven-hosted `logistics_data_gen` topic, we **create our
# MAGIC own topic** and write events into it. A second notebook (or a re-run
# MAGIC of `01_kafka_stream` pointed at this topic) can then consume them.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. Run [`00_setup`](./00_setup) first so credentials are in
# MAGIC    `secret_scope`.
# MAGIC 2. The Aiven CA certificate is at the path in the
# MAGIC    `truststore_path` widget (same as in `01_kafka_stream`).
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC 1. Install `confluent-kafka` so we can talk to the Kafka **AdminClient**
# MAGIC    — Spark itself only reads/writes data, it can't create topics.
# MAGIC 2. Create the target topic (idempotent).
# MAGIC 3. Generate N sensor-reading events with `spark.range()`.
# MAGIC 4. Encode each event as JSON and write the batch to Kafka.
# MAGIC
# MAGIC We use a **batch write** (`df.write.format("kafka").save()`), not a
# MAGIC streaming write — generating N events once is the cleanest way to
# MAGIC seed a topic for the exercise. Re-run the notebook to send another
# MAGIC batch.

# COMMAND ----------

# DBTITLE 1,Install the Kafka admin client
# MAGIC %pip install confluent-kafka

# COMMAND ----------

# DBTITLE 1,Restart Python so the new package is on the path
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("topic",           "sensor_readings", "Target topic")
dbutils.widgets.text("partitions",      "1",               "Partitions (if creating)")
dbutils.widgets.text("replication",     "2",               "Replication factor (if creating)")
dbutils.widgets.text("n_events",        "200",             "Number of events to send")
dbutils.widgets.text("truststore_path",
                     "/Volumes/workspace/landing/files/aiven/ca.pem",
                     "Path to Aiven CA (ca.pem)")
dbutils.widgets.dropdown("create_topic", "yes", ["yes", "no"],
                         "Create topic if missing?")
dbutils.widgets.dropdown("sasl_mechanism", "SCRAM-SHA-256",
                         ["SCRAM-SHA-256", "SCRAM-SHA-512", "PLAIN"],
                         "SASL mechanism")

topic           = dbutils.widgets.get("topic")
partitions      = int(dbutils.widgets.get("partitions"))
replication     = int(dbutils.widgets.get("replication"))
n_events        = int(dbutils.widgets.get("n_events"))
truststore_path = dbutils.widgets.get("truststore_path")
create_topic    = dbutils.widgets.get("create_topic")
sasl_mechanism  = dbutils.widgets.get("sasl_mechanism")

print(f"Topic           : {topic}")
print(f"Partitions      : {partitions}")
print(f"Replication     : {replication}")
print(f"Events to send  : {n_events}")
print(f"Truststore (CA) : {truststore_path}")
print(f"Create topic    : {create_topic}")

import os
if not os.path.exists(truststore_path):
    raise FileNotFoundError(
        f"CA file not found at {truststore_path}. Upload ca.pem from "
        "the Aiven service console into a Unity Catalog Volume and "
        "point this widget at it."
    )

# COMMAND ----------

# DBTITLE 1,Load credentials from the secret scope
SCOPE = "secret_scope"

host     = dbutils.secrets.get(SCOPE, "host")
port     = dbutils.secrets.get(SCOPE, "port")
user     = dbutils.secrets.get(SCOPE, "user")
password = dbutils.secrets.get(SCOPE, "password")

bootstrap_servers = f"{host}:{port}"
print(f"Bootstrap: {bootstrap_servers}  user={user}")

# COMMAND ----------

# DBTITLE 1,Create the topic if it doesn't exist (AdminClient)
# Kafka itself doesn't expose topic creation through the Spark
# connector — we use the confluent-kafka AdminClient instead.
# It speaks the same SASL_SSL/SCRAM dance as the Spark Kafka source.
#
# Aiven plans vary in how many brokers they have:
#   Hobbyist  -> 3 brokers, RF up to 3
#   Startup+  -> 3 brokers, RF up to 3
# If your plan has fewer brokers than `replication`, creation fails
# with "Replication factor: X larger than available brokers: Y".
# Reduce the widget value and re-run.

if create_topic == "yes":
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    sasl_mechanism,
        "sasl.username":     user,
        "sasl.password":     password,
        "ssl.ca.location":   truststore_path,
    })

    cluster_md = admin.list_topics(timeout=10)
    if topic in cluster_md.topics:
        existing = cluster_md.topics[topic]
        print(f"✓ Topic {topic!r} already exists "
              f"(partitions={len(existing.partitions)}).")
    else:
        new_topic = NewTopic(topic,
                             num_partitions=partitions,
                             replication_factor=replication)
        futures = admin.create_topics([new_topic])
        for t, f in futures.items():
            try:
                f.result(timeout=30)
                print(f"✓ Topic {t!r} created "
                      f"(partitions={partitions}, replication={replication}).")
            except Exception as e:
                raise RuntimeError(f"Failed to create topic {t}: {e}")
else:
    print("Skipping topic creation (widget = 'no'). The topic must already exist.")

# COMMAND ----------

# DBTITLE 1,Build the Spark Kafka write options
# Same shape as in 01_kafka_stream — use the shaded login module.
LOGIN_MODULE_PREFIX = "kafkashaded.org.apache.kafka"

if sasl_mechanism.startswith("SCRAM"):
    login_module = f"{LOGIN_MODULE_PREFIX}.common.security.scram.ScramLoginModule"
else:
    login_module = f"{LOGIN_MODULE_PREFIX}.common.security.plain.PlainLoginModule"

jaas_config = (
    f'{login_module} required '
    f'username="{user}" password="{password}";'
)

write_options = {
    "kafka.bootstrap.servers":     bootstrap_servers,
    "kafka.security.protocol":     "SASL_SSL",
    "kafka.sasl.mechanism":        sasl_mechanism,
    "kafka.sasl.jaas.config":      jaas_config,
    "kafka.ssl.truststore.type":     "PEM",
    "kafka.ssl.truststore.location": truststore_path,
    "topic":                       topic,
}

assert jaas_config.startswith("kafkashaded."), (
    "JAAS prefix is wrong — restart Python and re-run from the top."
)
print(f"Write options ready. JAAS module: {login_module}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the events
# MAGIC
# MAGIC We simulate a fleet of room sensors. Each event has:
# MAGIC
# MAGIC | Field            | Type        | Example                              |
# MAGIC |------------------|-------------|--------------------------------------|
# MAGIC | `event_id`       | string      | "evt-0000123"                        |
# MAGIC | `room_id`        | string      | "room_2" (used as the Kafka key)     |
# MAGIC | `temperature_c`  | double      | 22.4                                 |
# MAGIC | `humidity_pct`   | double      | 47.1                                 |
# MAGIC | `event_ts`       | timestamp   | 2026-05-20 14:31:09.608+00:00        |
# MAGIC
# MAGIC Encoding: **JSON** (one record per Kafka message). The key
# MAGIC (`room_id`) routes events of the same room to the same partition.

# COMMAND ----------

# DBTITLE 1,Build the events DataFrame
from pyspark.sql.functions import (
    col, lit, rand, struct, to_json, current_timestamp,
    concat, lpad, pmod, round as spark_round,
)

ROOMS = ["room_0", "room_1", "room_2", "room_3", "room_4"]
n_rooms = len(ROOMS)

events = (
    spark.range(n_events)
        .withColumn("event_id",      concat(lit("evt-"), lpad(col("id").cast("string"), 7, "0")))
        .withColumn("room_id",       concat(lit("room_"), pmod(col("id"), lit(n_rooms)).cast("string")))
        .withColumn("temperature_c", spark_round(lit(20.0) + rand() * lit(10.0), 2))   # 20-30 °C
        .withColumn("humidity_pct",  spark_round(lit(40.0) + rand() * lit(30.0), 2))   # 40-70 %
        .withColumn("event_ts",      current_timestamp())
        .drop("id")
)

print(f"Built {events.count()} events. Sample:")
display(events.limit(10))

# COMMAND ----------

# DBTITLE 1,Encode key + value for Kafka
# Kafka expects two columns when writing: `key` (optional) and `value`.
# Both must be BINARY or STRING. We use STRING for both — the broker
# treats them as bytes regardless.
records = events.select(
    col("room_id").alias("key"),
    to_json(struct(
        "event_id", "room_id", "temperature_c", "humidity_pct", "event_ts",
    )).alias("value"),
)

display(records.limit(5))

# COMMAND ----------

# DBTITLE 1,Write the batch to Kafka
# `.write` (no Stream): one-shot send of the whole DataFrame.
# On Free Edition this avoids all the streaming-trigger pitfalls.
(records.write
    .format("kafka")
    .options(**write_options)
    .save()
)

print(f"✓ Sent {records.count()} events to topic {topic!r}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify in Aiven
# MAGIC
# MAGIC Open the Aiven console → *Topics* → click your topic → *Messages*
# MAGIC tab. You should see the JSON payloads arriving.
# MAGIC
# MAGIC ## Read them back in `01_kafka_stream`
# MAGIC
# MAGIC In [`01_kafka_stream`](./01_kafka_stream):
# MAGIC
# MAGIC 1. Set the `topic` widget to the same name you used here.
# MAGIC 2. Set `cleanup_checkpoints` to `yes` once, then back to `no`
# MAGIC    (you've changed which topic the checkpoint tracks).
# MAGIC 3. Set `starting_offsets` to `earliest`.
# MAGIC 4. Replace the Avro-parsing step with JSON parsing — this
# MAGIC    notebook writes plain JSON, no Confluent prefix, no Avro
# MAGIC    binary:
# MAGIC
# MAGIC    ```python
# MAGIC    from pyspark.sql.functions import from_json, col
# MAGIC    from pyspark.sql.types import (
# MAGIC        StructType, StructField, StringType, DoubleType, TimestampType,
# MAGIC    )
# MAGIC
# MAGIC    sensor_schema = StructType([
# MAGIC        StructField("event_id",      StringType()),
# MAGIC        StructField("room_id",       StringType()),
# MAGIC        StructField("temperature_c", DoubleType()),
# MAGIC        StructField("humidity_pct",  DoubleType()),
# MAGIC        StructField("event_ts",      TimestampType()),
# MAGIC    ])
# MAGIC
# MAGIC    parsed = raw_stream.select(
# MAGIC        from_json(col("value").cast("string"), sensor_schema).alias("payload")
# MAGIC    ).select("payload.*")
# MAGIC    ```
# MAGIC
# MAGIC Then run the rest of `01_kafka_stream` as before.

# COMMAND ----------
