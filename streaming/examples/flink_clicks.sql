-- Flink SQL: read fake click events from Redpanda and aggregate per minute.
--
-- Usage:
--   1. Open Dinky at http://localhost:8888 (or the forwarded port in Codespaces)
--   2. Create a new FlinkSQL job
--   3. Paste this script and run it
--
-- Or run from the Flink SQL client inside the jobmanager container:
--   docker exec -it flink-jobmanager ./bin/sql-client.sh
--   then paste the statements below.

CREATE TABLE IF NOT EXISTS clicks (
    user_id   STRING,
    page      STRING,
    event_ts  TIMESTAMP_LTZ(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'clicks',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'properties.group.id' = 'flink-clicks',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Map the producer's `ts` JSON field onto event_ts via a computed column.
-- (If you adjust the producer to emit `event_ts` directly, drop the alias.)

-- Tumbling window: clicks per page per minute.
SELECT
    window_start,
    window_end,
    page,
    COUNT(*) AS clicks
FROM TABLE(
    TUMBLE(TABLE clicks, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, page;
