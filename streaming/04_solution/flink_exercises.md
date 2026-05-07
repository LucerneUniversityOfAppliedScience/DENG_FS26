# Flink SQL — Solutions

Reference answers for [exercises/flink_exercises.md](../03_exercise/flink_exercises.md).

---

## Exercise 01 — German filter

```sql
INSERT INTO bluesky_german
SELECT did, `text`, created_at, is_reply, `timestamp`
FROM bluesky_posts
WHERE langs[1] = 'de';
```

---

## Exercise 02 — tumbling, posts per language (2 min)

```sql
INSERT INTO language_counts
SELECT
    langs[1] AS lang,
    TUMBLE_START(proc_time, INTERVAL '2' MINUTES) AS window_start,
    TUMBLE_END(proc_time,   INTERVAL '2' MINUTES) AS window_end,
    COUNT(*) AS post_count
FROM bluesky_posts
WHERE langs[1] IN ('en', 'de', 'fr', 'it')
GROUP BY langs[1], TUMBLE(proc_time, INTERVAL '2' MINUTES);
```

---

## Exercise 03 — tumbling, total (3 min)

```sql
INSERT INTO post_counts_3min
SELECT
    TUMBLE_START(proc_time, INTERVAL '3' MINUTES) AS window_start,
    TUMBLE_END(proc_time,   INTERVAL '3' MINUTES) AS window_end,
    COUNT(*) AS total_posts
FROM bluesky_posts
GROUP BY TUMBLE(proc_time, INTERVAL '3' MINUTES);
```

---

## Exercise 04 — hopping (10 min / 2 min)

```sql
INSERT INTO language_trends
SELECT
    langs[1] AS lang,
    HOP_START(proc_time, INTERVAL '2' MINUTES, INTERVAL '10' MINUTES) AS window_start,
    HOP_END(proc_time,   INTERVAL '2' MINUTES, INTERVAL '10' MINUTES) AS window_end,
    COUNT(*) AS post_count
FROM bluesky_posts
WHERE langs[1] IN ('en', 'de', 'fr', 'it')
GROUP BY
    langs[1],
    HOP(proc_time, INTERVAL '2' MINUTES, INTERVAL '10' MINUTES);
```

---

## Exercise 05 — session window (60 s gap)

```sql
INSERT INTO user_sessions
SELECT
    did,
    SESSION_START(proc_time, INTERVAL '60' SECONDS) AS session_start,
    SESSION_END(proc_time,   INTERVAL '60' SECONDS) AS session_end,
    COUNT(*) AS post_count
FROM bluesky_posts
GROUP BY did, SESSION(proc_time, INTERVAL '60' SECONDS);
```

---

## Exercise 06 — reply rate per 5-minute window

```sql
SELECT
    TUMBLE_START(proc_time, INTERVAL '5' MINUTES) AS window_start,
    COUNT(*) AS total_posts,
    SUM(CASE WHEN is_reply THEN 1 ELSE 0 END) AS replies,
    CAST(SUM(CASE WHEN is_reply THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 AS reply_pct
FROM bluesky_posts
GROUP BY TUMBLE(proc_time, INTERVAL '5' MINUTES);
```

---

## Exercise 07 — keyword filter

```sql
INSERT INTO bsc_eds_posts
SELECT did, `text`, created_at, `timestamp`
FROM bluesky_posts
WHERE `text` LIKE '%Bsc_EDS%';
```

> **How it works.** The Bluesky Jetstream WebSocket emits *all* public posts.
> Redpanda Connect flattens and writes them to the `bluesky` topic. Your
> Flink SQL job continuously filters for `Bsc_EDS` and re-publishes matches
> to `bsc-eds-posts`. When you post on bsky.app, your message rides this
> exact pipeline in real time.
