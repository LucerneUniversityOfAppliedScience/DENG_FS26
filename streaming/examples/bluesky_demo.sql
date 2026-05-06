-- Bluesky Jetstream demo: ingest the firehose via Redpanda Connect.
--
-- Prerequisite: the bluesky topic is being filled. Start the firehose with:
--   docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect
--
-- Then run this file:
--   psql -h risingwave -p 4566 -d dev -U root -f streaming/examples/bluesky_demo.sql

CREATE SOURCE IF NOT EXISTS bluesky_raw (
    did varchar,
    time_us bigint,
    kind varchar,
    commit struct<
        operation varchar,
        collection varchar,
        record struct<
            "createdAt" varchar,
            text varchar,
            langs varchar[]
        >
    >
) WITH (
    connector = 'kafka',
    topic = 'bluesky',
    properties.bootstrap.server = 'redpanda:29092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- Flatten one row per created post; ignore deletes/updates for the demo.
CREATE MATERIALIZED VIEW IF NOT EXISTS bluesky_posts AS
SELECT
    did,
    to_timestamp(time_us / 1000000.0) AS event_ts,
    ((commit).record).text AS text,
    ((commit).record).langs AS langs
FROM bluesky_raw
WHERE (commit).operation = 'create'
  AND (commit).collection = 'app.bsky.feed.post';

-- Posts per minute, tumbling window.
CREATE MATERIALIZED VIEW IF NOT EXISTS posts_per_minute AS
SELECT window_start, count(*) AS posts
FROM TUMBLE(bluesky_posts, event_ts, INTERVAL '1 MINUTE')
GROUP BY window_start;

-- Language popularity (cumulative since the source started).
CREATE MATERIALIZED VIEW IF NOT EXISTS top_languages AS
SELECT lang, count(*) AS posts
FROM bluesky_posts, unnest(langs) AS lang
GROUP BY lang;
