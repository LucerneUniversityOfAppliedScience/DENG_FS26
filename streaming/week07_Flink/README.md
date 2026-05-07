# Week 07 — Stream Processing with Flink SQL

This week we run **Flink SQL** queries via **Dinky** (a SQL IDE that submits
jobs to the Flink cluster). The data source is the live **Bluesky firehose**
— every public post on bsky.app, in real time.

The whole stack runs in **GitHub Codespaces** — the smoke test in
[streaming/README.md](../README.md) covers the basics.

## Stack at a glance

| Port | Tool             | What for                                                  |
|------|------------------|-----------------------------------------------------------|
| 8080 | Redpanda Console | inspect topics and messages                                |
| 8081 | Flink Web UI     | monitor running jobs and task slots                        |
| 8888 | Dinky            | write SQL → submit to Flink → see results                  |

> **Task slots:** the Flink TaskManager has **4 slots**. Each running streaming
> query (`SELECT` or `INSERT INTO`) uses 1 slot. Stop jobs you no longer need
> via the red ■ button in Dinky.

## Step 1 — start the Bluesky stream

From a VS Code terminal:

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect
```

This starts a small Go service that reads the public Bluesky WebSocket,
flattens each post into a simple JSON, and writes it to the Redpanda topic
`bluesky` (~50–100 messages/second).

Verify in **Redpanda Console** (port 8080) → **Topics** → `bluesky`. New
messages should appear in real time.

To stop the firehose later:

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky stop redpanda-connect
```

## Step 2 — open Dinky and register the cluster (once per Codespace)

1. Open **Dinky** at port `8888` from the *PORTS* tab.
2. First time: leave defaults → *Next* until done.
3. **Registration Center → Cluster → Flink Instance → Add**
   - **Name:** `local-flink`
   - **JM Address:** `http://flink-jobmanager:8081`
4. Save and verify the status is **Normal** (green dot).

## Step 3 — define the source table (Dinky, once)

In **Data Studio**, create a new task (type *FlinkSQL*). Use:
- **Catalog:** DefaultCatalog
- **Cluster:** local-flink

Run this once — the table definition is persisted in DefaultCatalog and
reusable from any other task.

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

> **Why `proc_time AS PROCTIME()`?** Flink window functions need a time
> attribute. `PROCTIME()` uses the cluster's wall clock when each record is
> processed. The alternative is *event time* (using `created_at`), which
> requires watermarks — we'll stick with processing time for these
> exercises.

## Step 4 — first query: peek at the stream

In a new task, run:

```sql
SELECT `text`, langs, is_reply, created_at
FROM bluesky_posts;
```

Posts appear in the **Result** tab as they arrive. Stop the job (red ■) when
you've seen enough — it occupies a slot.

## Step 5 — first transform: filter English posts to a new topic

This pattern (filter → write back to Kafka) is at the heart of stream
processing.

```sql
CREATE TABLE IF NOT EXISTS bluesky_english (
    did STRING,
    `text` STRING,
    created_at STRING,
    is_reply BOOLEAN,
    `timestamp` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky-english',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'format' = 'json'
);

INSERT INTO bluesky_english
SELECT did, `text`, created_at, is_reply, `timestamp`
FROM bluesky_posts
WHERE langs[1] = 'en';
```

`INSERT INTO` is a **continuous streaming job** — it keeps running until you
stop it. Check the new topic `bluesky-english` in Redpanda Console.

## What's next

Work through the progressive exercises in
[exercises/flink_exercises.md](exercises/flink_exercises.md). Solutions live
in [solutions/flink_exercises.md](solutions/flink_exercises.md).

## Troubleshooting

| Symptom                                          | Fix                                                                                  |
|--------------------------------------------------|--------------------------------------------------------------------------------------|
| `Object 'bluesky_posts' not found`               | Run the `CREATE TABLE` from Step 3; ensure catalog is **DefaultCatalog**             |
| `Table already exists`                           | Use `CREATE TABLE IF NOT EXISTS` or `DROP TABLE … FIRST`                             |
| No data in Result tab                            | Check the bluesky-connect container is running: `docker ps \| grep redpanda-connect` |
| Cluster shows as **Abnormal** in Dinky           | Make sure JM Address is `http://flink-jobmanager:8081` (Docker DNS)                  |
| `No more slots available`                        | Stop other Flink jobs — there are 4 task slots in total                              |
| Dinky UI in Chinese                              | Open Dinky in an incognito window or clear localStorage for the Dinky tab            |
