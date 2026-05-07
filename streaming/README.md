# Streaming Sandbox

A pre-built environment for the streaming chapter of the DENG course. You
don't install anything on your laptop — everything runs in **GitHub
Codespaces** (a development computer in the browser, free for students).

## What you get

When you open this folder in a Codespace, four programs start automatically
inside the cloud computer:

| Program           | Web address you'll use   | What it does                                              |
|-------------------|--------------------------|-----------------------------------------------------------|
| **Redpanda**      | (no UI, port `9092`)     | The "post office" — stores streams of messages (topics)   |
| **Redpanda Console** | open port **`8080`**  | Web page to look inside topics and read messages          |
| **Flink**         | open port **`8081`**     | The "calculator" — runs queries on streaming data         |
| **Dinky**         | open port **`8888`**     | Web page to write SQL queries and send them to Flink      |

A fifth program, **Redpanda Connect**, is available on demand to pull the
public Bluesky feed. It only starts when you ask for it (see below).

## Step 1 — Open the Codespace

1. On the GitHub repo page, click the green **Code** button.
2. Choose the **Codespaces** tab → **Create codespace on main**.
3. Wait ~3–5 minutes the first time. You'll see Visual Studio Code in your
   browser. Subsequent starts are much faster.

> 💡 **Stop it when done.** Codespaces are free for ~30 hours/month. Closing
> the browser tab does *not* stop billing — go to *github.com/codespaces*
> and click *Stop* on yours.

## Step 2 — Open the web pages

At the bottom of VS Code, click the **PORTS** tab. You'll see a list:

| Port | Label              | Click on this to open |
|------|--------------------|-----------------------|
| 8080 | Redpanda Console   | the message viewer    |
| 8081 | Flink Web UI       | Flink's status page   |
| 8888 | Dinky              | the SQL editor        |

Hover any row → click the small **globe icon** ("Open in Browser") or the
**magnifier icon** ("Preview in Editor"). Both work.

## Step 3 — Try the smoke test

Open a terminal in VS Code (menu *Terminal → New Terminal*) and paste these
commands one block at a time.

**a) Make sure Redpanda is awake:**

```bash
rpk cluster info -X brokers=redpanda:29092
```

**b) Create a topic called `clicks` and produce 60 seconds of fake clicks:**

```bash
rpk topic create clicks -X brokers=redpanda:29092
uv run --project streaming streaming/examples/producer.py
```

The producer prints one line per second so you can watch it work.

**c) Watch the messages live in your terminal** (open a *second* terminal
for this so the producer keeps running):

```bash
rpk topic consume clicks -X brokers=redpanda:29092
```

Press **Ctrl+C** to stop watching.

**d) Or watch them in your browser:** open the **Redpanda Console** (port
8080) → click the *Topics* menu → click *clicks*. Messages appear in real
time.

**e) Run a Flink SQL query in Dinky:**

1. Open **Dinky** (port 8888).
2. First-time setup: leave everything at default → *Next* until done.
3. Create a new *FlinkSQL* job, paste the contents of
   [examples/flink_clicks.sql](examples/flink_clicks.sql), click *Run*.
4. The result panel shows clicks per page per minute, updating live.

## Bluesky firehose (optional, fun)

Bluesky publishes every public post to a public WebSocket. We can pipe that
firehose into our Redpanda topic `bluesky` and run Flink SQL on it.

**Start the firehose:**

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky up -d redpanda-connect
```

In Redpanda Console, the new topic `bluesky` appears with ~50–100
messages/second. In Dinky, paste
[examples/flink_bluesky.sql](examples/flink_bluesky.sql) and run it to count
posts per minute.

**Stop the firehose** when you're done so it doesn't eat your storage:

```bash
docker compose -f streaming/docker-compose.yml --profile bluesky stop redpanda-connect
```

## What's where

- [week06_kafka/](week06_kafka/) — **Week 06 exercises**: Python producers
  and consumers, partitioning, consumer groups
- [week07_Flink/](week07_Flink/) — **Week 07 exercises**: Flink SQL on the
  Bluesky firehose (windows, sessions, filters)
- [docker-compose.yml](docker-compose.yml) — defines all the running programs
- [pyproject.toml](pyproject.toml) — Python tool list (kafka client +
  notebook kernel)
- [flink/Dockerfile](flink/Dockerfile) — recipe for the Flink image (with
  the Kafka connector pre-installed)
- [dinky/Dockerfile](dinky/Dockerfile) — recipe for the Dinky image (with
  English UI and the Kafka connector)
- [connect/bluesky.yaml](connect/bluesky.yaml) — pipeline that reads
  Bluesky and writes to the `bluesky` topic
- [examples/producer.py](examples/producer.py) — Python script that makes
  fake clicks
- [examples/consumer.py](examples/consumer.py) — Python script that prints
  messages from a topic
- [examples/flink_clicks.sql](examples/flink_clicks.sql) — Flink SQL for the
  `clicks` topic
- [examples/flink_bluesky.sql](examples/flink_bluesky.sql) — Flink SQL for
  the `bluesky` topic

## Troubleshooting

**"docker: command not found"** — the codespace was built before docker was
added. Run *Codespaces: Rebuild Container* from the command palette (`F1`).

**Dinky shows Chinese text** — clear browser localStorage for the Dinky tab
or open it in an incognito window. The container patches the language on
first start, but a cached old session can override it.

**A port shows the wrong label** — same fix: rebuild the container so the
new `portsAttributes` from `.devcontainer/devcontainer.json` take effect.

**Flink job fails with "topic not found"** — create the topic first:
`rpk topic create <name> -X brokers=redpanda:29092`.

## Running locally (without Codespaces)

If you have Docker on your laptop and want to skip Codespaces:

```bash
docker compose -f streaming/docker-compose.yml up -d
cd streaming && uv sync
KAFKA_BROKERS=localhost:9092 uv run examples/producer.py
```

Open the same web addresses on `localhost`: <http://localhost:8080>,
<http://localhost:8081>, <http://localhost:8888>.
