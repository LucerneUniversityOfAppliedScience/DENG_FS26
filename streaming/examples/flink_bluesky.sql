-- Flink SQL: ingest the Bluesky firehose from the `bluesky` topic.
--
-- Prerequisite: the Redpanda Connect bluesky pipeline is running:
--   docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect
--
-- The pipeline flattens the Jetstream payload into a simple JSON schema
-- (see streaming/connect/bluesky.yaml).

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

-- Posts per minute (tumbling window over processing time)
SELECT
    TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(proc_time,   INTERVAL '1' MINUTE) AS window_end,
    COUNT(*) AS posts
FROM bluesky_posts
WHERE operation = 'create'
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE);
