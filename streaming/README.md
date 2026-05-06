# Streaming Sandbox: Redpanda + RisingWave

This module provides a ready-to-use streaming environment for the DENG course,
running entirely inside **GitHub Codespaces** so students do not need to install
anything locally or pay for cloud services.

## What's inside

| Service              | Port (forwarded) | Purpose                                              |
|----------------------|------------------|------------------------------------------------------|
| Redpanda             | 9092, 8081, 8082 | Kafka-compatible broker + Schema Registry            |
| Redpanda Console     | 8080             | Web UI for topics, messages, consumers               |
| RisingWave           | 4566, 5691       | Streaming SQL engine (Postgres wire)                 |
| Redpanda Connect     | -                | Bluesky firehose → topic `bluesky` (opt-in profile)  |
| Workspace            | 2718             | Python 3.12 + uv + rpk + psql + DuckDB + Marimo      |

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

## Opening the web UIs

Both UIs are reachable through Codespaces' port forwarding — there is no need
to expose anything publicly.

**Redpanda Console** (port `8080`) and **RisingWave Dashboard** (port `5691`):

1. In VS Code (browser or desktop) open the **PORTS** tab in the bottom panel
   (next to *Terminal* / *Problems*).
2. Find the row for the port you want — labels are pre-set:
   - `8080` → Redpanda Console
   - `5691` → RisingWave Dashboard
3. Hover the row and click the globe icon (**Open in Browser**) or the
   magnifier icon (**Preview in Editor**).

If the PORTS tab is empty, the containers are still starting — run
`docker compose ps` and wait until `redpanda` and `risingwave` are *healthy*.

Direct URL pattern (useful for sharing or bookmarks within your own session):

```
https://<codespace-name>-<port>.app.github.dev
```

The codespace name appears in the top-left of the VS Code window and in the
browser tab title.

For RisingWave SQL there is no web UI — connect via `psql` from the codespace
terminal:

```bash
psql -h risingwave -p 4566 -d dev -U root
```

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

## Bluesky firehose (opt-in)

The compose stack ships a [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/)
container that subscribes to the public **Bluesky Jetstream** WebSocket and
forwards every post into the `bluesky` Kafka topic. It is gated behind the
`bluesky` Compose profile so that it does not auto-start (the firehose pushes
~50–100 events/s and would fill the broker volume otherwise).

Inside the Codespace terminal:

```bash
# Start the firehose
docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect

# Wire RisingWave to the topic and create the demo views
psql -h risingwave -p 4566 -d dev -U root -f streaming/examples/bluesky_demo.sql

# Watch posts roll in
psql -h risingwave -p 4566 -d dev -U root \
    -c "SELECT * FROM posts_per_minute ORDER BY window_start DESC LIMIT 5;"

# Stop the firehose when done
docker compose -f streaming/docker-compose.yml --profile bluesky stop redpanda-connect
```

## DuckDB + Marimo

The workspace image includes [DuckDB](https://duckdb.org) and
[Marimo](https://marimo.io). Marimo notebooks can query RisingWave
materialized views *as if they were tables* via DuckDB's Postgres extension —
useful for interactive exploration alongside local Parquet files.

```bash
uv run --project streaming \
    marimo edit --host 0.0.0.0 --port 2718 \
    streaming/notebooks/bluesky_marimo.py
```

Then open the forwarded port `2718` from the **PORTS** tab. The example
notebook attaches to RisingWave and pulls live rows from the Bluesky
materialized views.

## Port labels

The forwarded ports come with friendly labels (e.g. *Redpanda Console*,
*RisingWave Dashboard*, *Marimo Notebook*) configured via
`portsAttributes` in [.devcontainer/devcontainer.json](../.devcontainer/devcontainer.json).
If your existing codespace shows only port numbers, the labels were added
after it was built — run **Codespaces: Rebuild Container** from the command
palette (`F1`) to pick them up. You can also relabel ad-hoc by right-clicking
a port row → *Set Port Label*.

## Files

- [docker-compose.yml](docker-compose.yml) — Redpanda + Console + RisingWave + Connect
- [pyproject.toml](pyproject.toml) — uv-managed Python deps (incl. DuckDB, Marimo)
- [connect/bluesky.yaml](connect/bluesky.yaml) — Redpanda Connect pipeline (Jetstream → Kafka)
- [examples/producer.py](examples/producer.py) — Faker → Redpanda
- [examples/consumer.py](examples/consumer.py) — Redpanda → stdout
- [examples/risingwave_demo.sql](examples/risingwave_demo.sql) — clicks source + materialized views
- [examples/bluesky_demo.sql](examples/bluesky_demo.sql) — Bluesky source + materialized views
- [notebooks/bluesky_marimo.py](notebooks/bluesky_marimo.py) — Marimo + DuckDB → RisingWave demo

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
