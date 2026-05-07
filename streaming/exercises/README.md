# Streaming Exercises

Two progressive tracks. Both run end-to-end inside the Codespace stack —
see [../README.md](../README.md) for setup and the smoke test.

## Track A — Kafka with Python (`exercise_produce_*` and `exercise_consume_*`)

Use case: **IoT sensors** in several houses report electricity (`strom`)
and water (`wasser`) consumption. Each event is keyed by house id
(`haus_a`, `haus_b`, …) so events of the same house land in the same
partition.

| File | What you practice |
|---|---|
| [exercise_produce_01.ipynb](exercise_produce_01.ipynb) | first producer, keys, broken-broker behaviour |
| [exercise_produce_02.ipynb](exercise_produce_02.ipynb) | batch producer, partition assignment, keyless events |
| [exercise_produce_03.ipynb](exercise_produce_03.ipynb) | continuous streaming + anomaly simulation |
| [exercise_consume_01.ipynb](exercise_consume_01.ipynb) | basic poll loop, consumer groups, live read |
| [exercise_consume_02.ipynb](exercise_consume_02.ipynb) | manual offset commits, two consumers in one group |
| [exercise_consume_03.ipynb](exercise_consume_03.ipynb) | aggregation + anomaly detection |

Recommended order: skim [`../demo/`](../demo/) → produce_01 → 02 → 03 →
consume_01 → 02 → 03. Open producer and consumer notebooks in two tabs
to watch events flow in real time.

Solutions live in [../solutions/](../solutions/).

Inside the Codespace's docker network the broker is reachable as
`redpanda:29092`. All notebooks already use that address.

## Track B — Flink SQL on the Bluesky firehose

[flink_exercises.md](flink_exercises.md) — progressive Flink SQL
exercises (filter, tumble, hop, session, reply rate, keyword) running on
the live Bluesky public-post firehose.

### Prerequisites for the Flink exercises

1. **Start the Bluesky stream** (from a VS Code terminal):

   ```bash
   docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect
   ```

   This pipes ~50–100 posts/second into the `bluesky` topic.

2. **Open Dinky** (port `8888` in the *PORTS* tab) and register the
   Flink cluster (once per Codespace):

   - *Registration Center* → *Cluster* → *Flink Instance* → *Add*
   - **Name:** `local-flink`
   - **JM Address:** `http://flink-jobmanager:8081`
   - Save and check the status is **Normal**.

3. **Create the source table** (Dinky → new FlinkSQL task →
   Catalog *DefaultCatalog*, Cluster *local-flink*):

   ```sql
   CREATE TABLE IF NOT EXISTS bluesky_posts (
       did          STRING,
       time_us      BIGINT,
       operation    STRING,
       `text`       STRING,
       created_at   STRING,
       langs        ARRAY<STRING>,
       reply_parent STRING,
       reply_root   STRING,
       is_reply     BOOLEAN,
       embed        STRING,
       facets       STRING,
       `timestamp`  STRING,
       proc_time AS PROCTIME()
   ) WITH (
       'connector' = 'kafka',
       'topic' = 'bluesky',
       'properties.bootstrap.servers' = 'redpanda:29092',
       'scan.startup.mode' = 'latest-offset',
       'format' = 'json',
       'json.fail-on-missing-field' = 'false',
       'json.ignore-parse-errors' = 'true'
   );
   ```

   The DefaultCatalog persists the table — you only run this once per
   Codespace.

4. Run a sanity-check query in a new task:

   ```sql
   SELECT `text`, langs, is_reply, created_at FROM bluesky_posts;
   ```

   Stop it (red ■) when you've seen enough — it occupies a slot.

> **Slots are limited.** The TaskManager has **4** slots. Each running
> `SELECT` or `INSERT INTO` uses one. Stop unused jobs before starting
> new ones.

When you're done with the day, stop the firehose:

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky stop redpanda-connect
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `KafkaError: Broker not available` (Python) | wait ~10 s after Codespace start, or run `rpk cluster info -X brokers=redpanda:29092` |
| Notebook can't find `confluent_kafka` | from a terminal: `cd streaming && uv sync`, then re-pick the kernel |
| `Object 'bluesky_posts' not found` (Dinky) | re-run the CREATE TABLE; ensure catalog is *DefaultCatalog* |
| `No more slots available` (Flink) | stop another running job (red ■) |
| Dinky UI is in Chinese | open it in an incognito tab or clear localStorage for the Dinky tab |
