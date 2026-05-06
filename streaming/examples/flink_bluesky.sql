-- Flink SQL: ingest the Bluesky firehose from the bluesky topic.
--
-- Prerequisite: the Redpanda Connect bluesky pipeline is running:
--   docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect

CREATE TABLE IF NOT EXISTS bluesky_raw (
    did       STRING,
    time_us   BIGINT,
    kind      STRING,
    `commit`  ROW<
        operation  STRING,
        collection STRING,
        record ROW<
            `createdAt` STRING,
            text        STRING,
            langs       ARRAY<STRING>
        >
    >,
    event_ts AS TO_TIMESTAMP_LTZ(time_us / 1000, 3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bluesky',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-bluesky',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Posts per minute (tumbling window)
SELECT
    window_start,
    window_end,
    COUNT(*) AS posts
FROM TABLE(
    TUMBLE(TABLE bluesky_raw, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
WHERE `commit`.operation = 'create'
  AND `commit`.collection = 'app.bsky.feed.post'
GROUP BY window_start, window_end;
