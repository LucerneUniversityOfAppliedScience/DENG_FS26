-- RisingWave demo: ingest the `clicks` topic from Redpanda and aggregate it.
-- Run with: psql -h risingwave -p 4566 -d dev -U root -f risingwave_demo.sql

CREATE SOURCE IF NOT EXISTS clicks (
    user_id varchar,
    page varchar,
    ts timestamptz
) WITH (
    connector = 'kafka',
    topic = 'clicks',
    properties.bootstrap.server = 'redpanda:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW IF NOT EXISTS clicks_per_minute AS
SELECT
    window_start,
    page,
    count(*) AS n
FROM TUMBLE(clicks, ts, INTERVAL '1 MINUTE')
GROUP BY window_start, page;

CREATE MATERIALIZED VIEW IF NOT EXISTS top_pages AS
SELECT page, count(*) AS n
FROM clicks
GROUP BY page;
