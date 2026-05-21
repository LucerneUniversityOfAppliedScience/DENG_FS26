# sw13 — Streaming with Aiven Kafka + Spark Structured Streaming

This module walks through a full streaming stack on Databricks Free
Edition: ingest from a managed Kafka cluster on **Aiven**, build a
Bronze → Silver → Gold medallion, archive raw data to Avro files, and
add stateful Gold-layer aggregations and a Dead-Letter-Queue pattern.

The companion folder [`../sw14_lakeflow_pipelines/`](../sw14_lakeflow_pipelines/)
rebuilds the same pipeline declaratively with Lakeflow / DLT — start
with sw13 first.

## What the notebooks do

| # | Notebook | Topic |
|---|---|---|
| 00 | [`00_setup.py`](./00_setup.py) | Create the `secret_scope` and store Aiven credentials |
| 01 | [`01_kafka_consumer.py`](./01_kafka_consumer.py) | Read the `logistics_data_gen` topic, decode key/value, preview via memory sink |
| 02 | [`02_kafka_producer.py`](./02_kafka_producer.py) | Create a topic via the Kafka AdminClient, generate IoT sensor events, batch-write to Kafka |
| 03 | [`03_kafka_to_bronze.py`](./03_kafka_to_bronze.py) | Stream Kafka → `workspace.bronze.<table>` (raw key + binary value) |
| 04 | [`04_bronze_to_silver.py`](./04_bronze_to_silver.py) | Parse Bronze → `workspace.silver.<table>` (Avro / JSON), three formats supported via widget |
| 05 | [`05_silver_to_gold_tumbling.py`](./05_silver_to_gold_tumbling.py) | Event-time **tumbling window** aggregation per carrier per state |
| 06 | [`06_silver_to_gold_sessions.py`](./06_silver_to_gold_sessions.py) | **Session windows** per `tracking_id` — the lifecycle of each shipment |
| 07 | [`07_silver_to_gold_sliding.py`](./07_silver_to_gold_sliding.py) | **Sliding (hopping)** windows — same KPI, smoother curve |
| 08 | [`08_stateful_stuck_alerts.py`](./08_stateful_stuck_alerts.py) | Custom stateful streaming with `applyInPandasWithState` — alert on stuck shipments (needs non-shared cluster) |
| 09 | [`09_foreach_batch_dlq.py`](./09_foreach_batch_dlq.py) | `foreachBatch` + **Dead-Letter-Queue** pattern for parse failures |

Suggested reading order: 00 → 01 → 03 → 04 → 05 → 06, then branch out
to 02 (alternative producer) and 07/08/09 (advanced).

## Prerequisites

1. A Databricks workspace (Free Edition works).
2. Unity Catalog `workspace.landing.files` volume — used for the
   Aiven CA certificate and streaming checkpoints. Create with:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.landing;
   CREATE VOLUME IF NOT EXISTS workspace.landing.files;
   ```
3. An Aiven Kafka instance with a `logistics_data_gen` topic that
   receives messages — see the next section.

## Setting up Aiven Kafka (free trial)

Aiven offers a 30-day free trial with $300 of credits — enough for a
small Kafka project for the duration of the course.

### 1. Create the account

- Open <https://console.aiven.io/signup>.
- Sign up with email or GitHub. You don't need a credit card for the
  trial.
- Create a *Project* — any name (e.g. `deng-streaming`). The project
  is where services live.

### 2. Provision a Kafka service

- *Services → Create service → Aiven for Apache Kafka*.
- **Service plan:** *Startup-2* (smallest plan that supports the data
  generator — fits inside the trial credits). Region: pick one close
  to you, e.g. `aws-eu-central-1` or `google-europe-west3`.
- **Service name:** pick something memorable (e.g. `kafka-deng`).
- Click *Create service* and wait ~5 minutes until status turns to
  **Running**.

### 3. Note the connection details

Open the service → tab *Overview*. In the *Connection information*
panel:

| Field on the Aiven page | Goes into `secret_scope` as |
|---|---|
| **Service URI** (the `kafka-xxxx-xxxx.aivencloud.com:NNNNN` thing) | `service_uri` |
| Host part (before the colon) | `host` |
| Port part (after the colon) | `port` |
| **User** (usually `avnadmin`) | `user` |
| **Password** (click reveal) | `password` |

### 4. Download the CA certificate

Below the username field, under *Authentication method = SASL*:
*CA certificate → Download*. You'll get a file called `ca.pem`.

Upload it to your UC volume so every executor can read it:

- In Databricks: *Catalog → workspace → landing → files →* create a
  folder `aiven` → *Upload* `ca.pem` into it.
- Final path: `/Volumes/workspace/landing/files/aiven/ca.pem`.

### 5. Allow your IP / open the firewall

Aiven services come with an IP allowlist by default. For Databricks
Serverless you don't have a fixed IP, so add a permissive rule:

- *Service → Advanced configuration → IP filter →* set to `0.0.0.0/0`
  for the duration of the course (open to all IPs).
- For production, switch to a private link / VPC peering instead.

## Generating the test data with Aiven's Kafka data generator

Aiven ships a one-click data generator that emits synthetic events
into one of your topics. We'll point it at `logistics_data_gen`.

1. Inside your Kafka service: tab **Integrations**.
2. Find **Kafka data generator** in the list of available integrations
   and click *Create*.
3. **Topic:** `logistics_data_gen` (the integration will auto-create
   the topic if it doesn't exist — pick partitions = 1 if asked).
4. **Schema / template:** select **Logistics** from the bundled
   templates. (If the UI offers a custom schema instead, paste the
   one documented at the top of
   [`04_bronze_to_silver.py`](./04_bronze_to_silver.py).)
5. **Format:** *Avro* with *Schema Registry* enabled — that's why our
   payload has the 5-byte Confluent prefix and we strip it in
   `from_avro`.
6. **Throughput:** ~10 messages/second is plenty for the course.
7. Click *Enable*.

Open *Topics → `logistics_data_gen` → Messages* — events should be
streaming in. Sample value field (binary): looks like garbage in the
UI (Avro is binary). Sample fields if you decode it:

```
time_utc          1747052327
tracking_id       track-1974256721
message           transfer
carrier           DHL
manifest          ["small box","fragile"]
next_hop_location DUB
state             Received
```

## Quickstart in Databricks

1. **Run `00_setup.py`** — fill in `host`, `port`, `user`, `password`
   from Aiven, then run all cells. This creates `secret_scope` and
   stores the credentials.
2. **Run `01_kafka_consumer.py`** — set `cleanup_checkpoints=yes` once,
   then back to `no`. You should see logistics messages in the
   `kafka_parsed` snapshot.
3. **Run `03_kafka_to_bronze.py`** — produces
   `workspace.bronze.logistics_data_gen`.
4. **Run `04_bronze_to_silver.py`** with `payload_format=avro_confluent`
   — produces `workspace.silver.logistics_data_gen`.
5. **Run `05_silver_to_gold_tumbling.py`** and
   **`06_silver_to_gold_sessions.py`** — the Gold KPIs.

The advanced notebooks (07 / 08 / 09) can be done in any order on top
of an already-populated Silver layer.

## Security note

Three artefacts need different homes:

| What | Where it belongs |
|---|---|
| `ca.pem` (Aiven CA cert — public, but pins to your project) | UC Volume with restricted grants |
| SASL password (`AVNS_…`) | Secret scope only — never paste into a notebook cell |
| `service.cert` + `service.key` (mTLS — not used here) | Secret scope only |

Do **not** commit any of these into Git.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `KafkaIllegalStateException: No LoginModule found for org.apache.kafka...ScramLoginModule` | Stale `jaas_config` Python variable. Restart the kernel and re-run from the top. |
| `Malformed Avro messages... Length is negative` | Producer uses Confluent Schema Registry framing — make sure you're stripping the 5-byte prefix (`substring(value, 6, length(value)-5)`). |
| `TEMP_CHECKPOINT_LOCATION_NOT_SUPPORTED` | You're on Free Edition / serverless — pass `checkpointLocation=...` explicitly. |
| `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` | Free Edition forbids `ProcessingTime` triggers — use `.trigger(availableNow=True)`. |
| `This query does not support recovering from checkpoint location` | You changed the query shape. Flip the notebook's `cleanup_checkpoints` widget to `yes` once. |
| `Object 'workspace.bronze.<table>' not found` (in 04) | `03_kafka_to_bronze.py` hasn't run yet, or the table_name widget differs between 03 and 04. |
| Aiven topic shows 0 messages | Data generator integration is disabled — re-enable it under *Integrations*. |

## Costs

Aiven's $300 free credits comfortably cover an idle Startup-2 Kafka
service for the trial month. Stop the service from the Aiven console
when you're done with the module to avoid surprise charges after the
trial period.

Databricks Free Edition is free as long as you stay inside its quotas
(serverless compute, limited concurrency). Pause notebooks when
you're done.
