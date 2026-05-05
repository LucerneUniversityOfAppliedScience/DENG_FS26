# Streaming Sandbox: Redpanda + RisingWave

This module provides a ready-to-use streaming environment for the DENG course,
running entirely inside **GitHub Codespaces** so students do not need to install
anything locally or pay for cloud services.

## What's inside

| Service          | Port (forwarded) | Purpose                                    |
|------------------|------------------|--------------------------------------------|
| Redpanda         | 9092, 8081, 8082 | Kafka-compatible broker + Schema Registry  |
| Redpanda Console | 8080             | Web UI for topics, messages, consumers     |
| RisingWave       | 4566, 5691       | Streaming SQL engine (Postgres wire)       |
| Workspace        | -                | Python 3.12 + uv + rpk + psql              |

The machine is configured for **4 CPUs / 16 GB RAM**, which is the smallest
Codespaces size that comfortably runs all three services.

## Open in Codespaces

1. On the GitHub repo page click **Code → Codespaces → Create codespace on
   main**.
2. The first build takes ~3 minutes (image build + service pull).
3. Once VS Code is up, switch to the **Ports** tab — port 8080 (Redpanda
   Console) opens automatically.

> **Cost note for students:** GitHub's free tier covers 120 core-hours/month,
> i.e. ~30 hours on the 4-core machine. Always **stop the codespace** when you
> finish working (Codespaces panel → "Stop codespace"); just closing the tab
> keeps it billing.

## Quick smoke test

```bash
# 1. Verify the broker is up
rpk cluster info -X brokers=redpanda:29092

# 2. Create the topic and produce 60s of fake clicks
rpk topic create clicks -X brokers=redpanda:29092
uv run --project streaming streaming/examples/producer.py

# 3. Inspect messages from the CLI…
rpk topic consume clicks -n 5 -X brokers=redpanda:29092

# 4. …or from the browser: open the forwarded port 8080 → Redpanda Console.

# 5. Wire RisingWave to the topic and run the demo SQL
psql -h risingwave -p 4566 -d dev -U root -f streaming/examples/risingwave_demo.sql

# 6. Query the materialized view (re-run while the producer is active)
psql -h risingwave -p 4566 -d dev -U root \
    -c "SELECT * FROM clicks_per_minute ORDER BY window_start DESC LIMIT 10;"
```

## Files

- [docker-compose.yml](docker-compose.yml) — Redpanda + Console + RisingWave
- [pyproject.toml](pyproject.toml) — uv-managed Python deps
- [examples/producer.py](examples/producer.py) — Faker → Redpanda
- [examples/consumer.py](examples/consumer.py) — Redpanda → stdout
- [examples/risingwave_demo.sql](examples/risingwave_demo.sql) — source + materialized views

## Running locally (without Codespaces)

```bash
docker compose -f streaming/docker-compose.yml up -d
cd streaming && uv sync
KAFKA_BROKERS=localhost:9092 uv run examples/producer.py
psql -h localhost -p 4566 -d dev -U root -f examples/risingwave_demo.sql
```

When running locally the broker advertises `localhost:9092` for clients on the
host. Inside the Codespace devcontainer everything talks via the internal name
`redpanda:29092`.
