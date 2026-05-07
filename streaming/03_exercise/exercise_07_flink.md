# Flink SQL on the Bluesky Firehose

## Background

[Bluesky](https://bsky.app) is a decentralised social network similar to
X/Twitter. Every public post is published in real time over a free,
unauthenticated WebSocket API called **Jetstream** — no API key, no rate
limit. That makes it an ideal data source for learning stream processing:
real, messy, multilingual data flowing at ~50–100 events/second.

In this track you'll:

1. Pipe the live Bluesky firehose into a Redpanda topic.
2. Define a Flink SQL table over that topic.
3. Run streaming queries that filter, window and aggregate posts.
4. Write transformed results back to new Redpanda topics.

### Architecture

```
Bluesky Jetstream (public WebSocket)
        │
        ▼
┌──────────────────┐      ┌──────────────────┐
│ Redpanda Connect │─────▶│     Redpanda     │
│  (bluesky.yaml)  │      │  topic: bluesky  │
└──────────────────┘      └────────┬─────────┘
                                   │
                          ┌────────┴─────────┐
                          ▼                  ▼
                   ┌──────────────┐   ┌──────────────┐
                   │  Flink SQL   │   │  Flink SQL   │
                   │ (filter/win) │   │ (filter/win) │
                   └───────┬──────┘   └───────┬──────┘
                           ▼                  ▼
                    ┌────────────┐     ┌────────────┐
                    │  Redpanda  │     │  Redpanda  │
                    │  english   │     │  german    │
                    └────────────┘     └────────────┘
```

> **Slots are limited (4 total).** Each running query (`SELECT` or
> `INSERT INTO`) uses one slot. Stop unused jobs with the red ■ button
> before starting new ones.

---

## Setup

Do these four steps once. After that, every exercise can re-use the
`bluesky_posts` source table without re-creating it.

### S1 — Start the Bluesky firehose

In a VS Code terminal:

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect
```

This starts a small service (Redpanda Connect) that subscribes to the
Bluesky Jetstream WebSocket, flattens each post to a tidy JSON object
([`streaming/connect/bluesky.yaml`](../connect/bluesky.yaml)) and writes
it to the Redpanda topic `bluesky`.

Stop it again later with:

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky stop redpanda-connect
```

### S2 — Verify in Redpanda Console

Open **Redpanda Console** (port `8080` in the *PORTS* tab) → *Topics* →
`bluesky`. You should see new messages arriving once a second or two.

### S3 — Register the Flink cluster in Dinky (once per Codespace)

Walked through in [../01_setup/dinky_guide.md](../01_setup/dinky_guide.md).
Cluster name `deng`, JM Address `flink-jobmanager:8081`, Type `Standalone`.

### S4 — Create the source table

In Dinky → Data Studio → new FlinkSQL task.

> ⚠️ **For every new task** in this track, set in the right-hand panel:
>
> - **Cluster Configuration / Flink Instance** → `deng`
> - **Catalog** → `DefaultCatalog`
>
> Without `DefaultCatalog` your table is invisible to other tasks. Without
> `deng` the SQL runs in Dinky's local mini-cluster, which has no Kafka
> connector and your `INSERT INTO` will fail with cryptic errors.

Run this **once**:

```sql
-- DROP TABLE IF EXISTS bluesky_posts;
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

> **Why `proc_time AS PROCTIME()`?** Window functions need a time
> attribute. `PROCTIME()` is the cluster's wall clock when each record
> is processed. Simpler than event-time + watermarks for these
> exercises.

### S5 — First sanity-check query

In a new task, run:

```sql
SELECT `text`, langs, is_reply, created_at FROM bluesky_posts;
```

Posts appear in the **Result** tab as they arrive. Stop the job (red ■)
when you've seen enough — it occupies a slot.

### S6 — Tutorial: filter English posts to a new topic

This is the canonical pattern (filter → write back to Kafka) and the
basis for most exercises. Run it in a new task — *don't* run all the
later exercises if a slot is occupied here.

```sql
-- DROP TABLE IF EXISTS bluesky_english;
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

`INSERT INTO` is a **continuous streaming job** — it runs until you stop
it. Verify in Redpanda Console: the new topic `bluesky-english` should
appear and fill up. Then stop the job (red ■).

---

## Exercise 01 — filter German posts to a new topic

**Goal:** mirror the English filter from the README — but for German.

### Step 1 — create the sink (Dinky, once)

```sql
CREATE TABLE IF NOT EXISTS bluesky_german (
    did STRING,
    `text` STRING,
    created_at STRING,
    is_reply BOOLEAN,
    `timestamp` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky-german',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'format' = 'json'
);
```

### Step 2 — write the streaming job

```sql
-- TODO: INSERT INTO bluesky_german  ...  FROM bluesky_posts WHERE langs[1] = 'de';
```

### Verify

In Redpanda Console, open the topic `bluesky-german`. German posts should
arrive within seconds.

**Questions**

- Why is there much less traffic than in `bluesky_english`?
- What does `langs[1]` mean in Flink SQL? (Hint: SQL arrays are 1-indexed.)

---

## Exercise 02 — tumbling window: posts per language (2 min)

**Goal:** count how many posts arrive in **English (`en`), German (`de`),
French (`fr`), Italian (`it`)** every 2 minutes. Write the per-window
counts to a Kafka topic.

### Step 1 — create the sink (Dinky, once)

```sql
CREATE TABLE IF NOT EXISTS language_counts (
    lang STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    post_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'language-counts',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

### Step 2 — write the streaming job

Hints:
- `TUMBLE_START(proc_time, INTERVAL '2' MINUTES) AS window_start`
- `TUMBLE_END(proc_time,   INTERVAL '2' MINUTES) AS window_end`
- `WHERE langs[1] IN ('en', 'de', 'fr', 'it')`
- `GROUP BY langs[1], TUMBLE(proc_time, INTERVAL '2' MINUTES)`

```sql
-- TODO: INSERT INTO language_counts SELECT ... FROM bluesky_posts ...
```

### Verify

Stop the `INSERT` job (you need the slot) and run in a new task:

```sql
SELECT * FROM language_counts;
```

> **Tumbling window**
> Splits the stream into fixed, non-overlapping intervals. Every event
> belongs to exactly one window.
> ```
> |--- Window 1 ---|--- Window 2 ---|--- Window 3 ---|
> 00:00          02:00            04:00            06:00
> ```

**Questions**

- How many rows per 2-minute window do you get? Why?
- Which language dominates?
- What changes if the interval is `1` minute?

---

## Exercise 03 — tumbling window: total posts (3 min)

**Goal:** count all posts every 3 minutes, regardless of language.

### Step 1 — sink (once)

```sql
CREATE TABLE IF NOT EXISTS post_counts_3min (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_posts BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'post-counts-3min',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

### Step 2 — streaming job

Hints: no `WHERE` needed; `GROUP BY TUMBLE(proc_time, INTERVAL '3' MINUTES)`.

```sql
-- TODO: INSERT INTO post_counts_3min SELECT ... FROM bluesky_posts ...
```

**Questions**

- How many posts arrive in 3 minutes? Does the rate vary over time?
- Why exactly 1 row per window (compared to Exercise 02)?

---

## Exercise 04 — sliding (hopping) window: language trends

**Goal:** track each language with a **10-minute** wide window that **slides
every 2 minutes** — a smoother trend curve than tumbling.

### Step 1 — sink (once)

```sql
CREATE TABLE IF NOT EXISTS language_trends (
    lang STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    post_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'language-trends',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

### Step 2 — streaming job

Hints — `HOP()` takes **slide first, then size**:
- `HOP_START(proc_time, INTERVAL '2' MINUTES, INTERVAL '10' MINUTES)`
- `HOP_END(proc_time,   INTERVAL '2' MINUTES, INTERVAL '10' MINUTES)`
- `GROUP BY langs[1], HOP(proc_time, INTERVAL '2' MINUTES, INTERVAL '10' MINUTES)`

```sql
-- TODO: INSERT INTO language_trends SELECT ... FROM bluesky_posts ...
```

> **Hopping (sliding) window**
> Fixed size, but overlaps. Each event belongs to **multiple** windows.
> ```
> |---------- W1 (10 min) ----------|
>       |---------- W2 (10 min) ----------|
>             |---------- W3 (10 min) ----------|
>   slide: 2 min
> ```
> With size 10 and slide 2, each event is counted in 5 windows.

**Questions**

- How does the output rate compare to Exercise 02?
- Why do consecutive windows overlap?
- How many windows does a single event belong to? (size / slide)

---

## Exercise 05 — session window: per-user activity bursts

**Goal:** group posts per user (`did`) into **sessions**: a session ends when
the user is silent for 60 seconds.

### Step 1 — sink (once)

```sql
CREATE TABLE IF NOT EXISTS user_sessions (
    did STRING,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    post_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'user-sessions',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

### Step 2 — streaming job

Hints:
- `SESSION_START(proc_time, INTERVAL '60' SECONDS)`
- `SESSION_END(proc_time,   INTERVAL '60' SECONDS)`
- `GROUP BY did, SESSION(proc_time, INTERVAL '60' SECONDS)`

```sql
-- TODO: INSERT INTO user_sessions SELECT ... FROM bluesky_posts ...
```

> **Session window**
> Defined by a **gap of inactivity**, not a fixed size. As long as events
> keep coming within the gap, the window grows. After the gap it closes.
> ```
> User A: |-post--post-post----post-|   (gap > 60 s)   |--post--post--|
>              Session 1 (4 posts)                     Session 2 (2 posts)
> ```

**Questions**

- What's the longest session you find?
- How does the gap duration change the session count?

---

## Exercise 06 — reply rate per 5-minute window

**Goal:** for each 5-minute tumbling window, compute the percentage of posts
that are replies (`is_reply = TRUE`). No sink — just a `SELECT` to look at.

Hints:
- `SUM(CASE WHEN is_reply THEN 1 ELSE 0 END)` to count replies
- `CAST(... AS DOUBLE) / COUNT(*) * 100` to get a percentage
- `GROUP BY TUMBLE(proc_time, INTERVAL '5' MINUTES)`

```sql
-- TODO: SELECT window_start, total_posts, replies, reply_pct FROM ...
```

**Questions**

- What share of Bluesky posts are replies?
- Does the share drift over time?

---

## Exercise 07 — bonus: see your own post arrive

**Goal:** filter the firehose for the keyword `HSLU_Flink`, post that string from
your own Bluesky account, and watch it appear in your Flink output within
seconds.

### Step 1 — sink (once)

```sql
CREATE TABLE IF NOT EXISTS deng_post_table (
    did STRING,
    `text` STRING,
    created_at STRING,
    `timestamp` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'deng-post',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

### Step 2 — filter job

Hints:
- `WHERE \`text\` LIKE '%HSLU_Flink%'`

```sql
-- TODO: INSERT INTO deng_post_table SELECT ... FROM bluesky_posts WHERE ...
```

### Step 3 — observe

In a new task (after stopping the INSERT to free a slot):

```sql
SELECT * FROM deng_post_table;
```

### Step 4 — post on Bluesky

1. If you don't have an account, create one at <https://bsky.app>.
2. Write a post containing the literal string `HSLU_Flink`. Example:
   > Testing stream processing for **HSLU_Flink** — hello from Flink!
3. Re-start the `INSERT` job, then `SELECT` again. Your post should appear
   within seconds. The topic `deng-post` in Redpanda Console will also
   contain it.

**Questions**

- How long from posting to seeing it in Dinky?
- Trace the path of the message: WebSocket → ? → ? → ? → your screen.

---

## Cleanup

When you're done, stop all running Flink jobs (red ■ in Dinky) and stop
the Bluesky firehose so it doesn't fill your disk:

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky stop redpanda-connect
```
