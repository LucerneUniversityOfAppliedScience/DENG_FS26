# Streaming Sandbox: Redpanda + Flink + Dinky

This module provides a ready-to-use streaming environment for the DENG course,
running entirely inside **GitHub Codespaces** so students do not need to
install anything locally or pay for cloud services.

## What's inside

| Service           | Port (forwarded) | Purpose                                              |
|-------------------|------------------|------------------------------------------------------|
| Redpanda          | 9092             | Kafka-compatible broker                              |
| Redpanda Connect  | -                | Bluesky firehose → topic `bluesky` (opt-in profile)  |
| Flink JobManager  | 8081             | Cluster coordinator + Web UI                         |
| Flink TaskManager | -                | Worker process (4 task slots)                        |
| Dinky             | 8888             | Flink SQL development platform                       |
| Workspace         | -                | Python 3.12 + uv + rpk + docker CLI                  |

The machine is configured for **4 CPUs / 16 GB RAM** in Codespaces.

## Open in Codespaces

1. On the GitHub repo page click **Code → Codespaces → Create codespace on
   main**.
2. The first build pulls/builds the Flink + Dinky + Redpanda images
   (~3–5 min). Subsequent rebuilds are much faster.
3. Once VS Code is up, port `8888` (Dinky) opens automatically in the
   *Simple Browser*.

> **Cost note for students:** GitHub's free tier covers 120 core-hours/month,
> i.e. ~30 hours on the 4-core machine. Always **stop the codespace** when
> done (Codespaces panel → "Stop codespace") — closing the tab keeps it
> billing.

## Quick smoke test

```bash
# 1. Verify the broker is up
rpk cluster info -X brokers=redpanda:29092

# 2. Create the topic and produce 60s of fake clicks
rpk topic create clicks -X brokers=redpanda:29092
uv run --project streaming streaming/examples/producer.py

# 3. Inspect messages from the CLI…
rpk topic consume clicks -n 5 -X brokers=redpanda:29092

# 4. Open Dinky (port 8888) and paste the contents of
#    streaming/examples/flink_clicks.sql into a new FlinkSQL job.
#    Run it — windowed counts appear in the result panel.
```

## Bluesky firehose (opt-in)

Redpanda Connect can subscribe to the public **Bluesky Jetstream** WebSocket
and forward every post into the `bluesky` Kafka topic. It is gated behind the
`bluesky` Compose profile (the firehose pushes ~50–100 events/s; only enable
it when you want to demo it).

```bash
# Start the firehose
docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect

# In Dinky, paste streaming/examples/flink_bluesky.sql and run it.

# Stop the firehose when done
docker compose -f streaming/docker-compose.yml --profile bluesky stop redpanda-connect
```

## Opening the web UIs

Both UIs are reachable via Codespaces' port forwarding:

1. Open the **PORTS** tab in the bottom panel of VS Code.
2. Find the row for *Flink Web UI* (8081) or *Dinky* (8888).
3. Click the globe icon (**Open in Browser**) or magnifier (**Preview in
   Editor**).

If the PORTS tab is empty, the containers are still starting — run
`docker compose ps` and wait for `redpanda` to be *healthy* and the others
*running*.

## Files

- [docker-compose.yml](docker-compose.yml) — Redpanda + Connect + Flink JM/TM + Dinky
- [pyproject.toml](pyproject.toml) — Python deps for the click producer
- [flink/Dockerfile](flink/Dockerfile) — Flink image with the Kafka SQL connector
- [connect/bluesky.yaml](connect/bluesky.yaml) — Redpanda Connect pipeline (Jetstream → Kafka)
- [examples/producer.py](examples/producer.py) — Faker → Redpanda
- [examples/consumer.py](examples/consumer.py) — Redpanda → stdout
- [examples/flink_clicks.sql](examples/flink_clicks.sql) — Flink SQL on the `clicks` topic
- [examples/flink_bluesky.sql](examples/flink_bluesky.sql) — Flink SQL on the Bluesky firehose

## Running locally (without Codespaces)

```bash
docker compose -f streaming/docker-compose.yml up -d
cd streaming && uv sync
KAFKA_BROKERS=localhost:9092 uv run examples/producer.py
# Open http://localhost:8888 for Dinky and http://localhost:8081 for Flink.
```
