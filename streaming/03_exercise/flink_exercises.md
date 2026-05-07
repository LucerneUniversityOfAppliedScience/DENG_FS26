# Flink SQL — Progressive Exercises

These exercises build on the Bluesky setup from
[../README.md](../README.md). All SQL is executed in **Dinky**
(Data Studio → new FlinkSQL task, cluster `local-flink`).

**Prerequisites:**
- The Bluesky stream is running:
  ```bash
  docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect
  ```
- The `bluesky_posts` source table from the README's Step 3 has been created.
- The Flink cluster is registered in Dinky and shows status **Normal**.

> **Slots are limited (4 total).** Each running query (`SELECT` or
> `INSERT INTO`) uses one slot. Stop unused jobs with the red ■ button
> before starting new ones.

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

**Goal:** filter the firehose for the keyword `Bsc_EDS`, post that string from
your own Bluesky account, and watch it appear in your Flink output within
seconds.

### Step 1 — sink (once)

```sql
CREATE TABLE IF NOT EXISTS bsc_eds_posts (
    did STRING,
    `text` STRING,
    created_at STRING,
    `timestamp` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'bsc-eds-posts',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
```

### Step 2 — filter job

Hints:
- `WHERE \`text\` LIKE '%Bsc_EDS%'`

```sql
-- TODO: INSERT INTO bsc_eds_posts SELECT ... FROM bluesky_posts WHERE ...
```

### Step 3 — observe

In a new task (after stopping the INSERT to free a slot):

```sql
SELECT * FROM bsc_eds_posts;
```

### Step 4 — post on Bluesky

1. If you don't have an account, create one at <https://bsky.app>.
2. Write a post containing the literal string `Bsc_EDS`. Example:
   > Testing stream processing for **Bsc_EDS** — hello from Flink!
3. Re-start the `INSERT` job, then `SELECT` again. Your post should appear
   within seconds. The topic `bsc-eds-posts` in Redpanda Console will also
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
