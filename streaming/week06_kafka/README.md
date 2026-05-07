# Week 06 — Event Streaming with Kafka / Redpanda

This week you write Python producers and consumers, inspect events in the
Redpanda Console, and explore partitioning and consumer groups.

The whole stack runs in **GitHub Codespaces** — no installs on your laptop.
Open the repo in a Codespace and the broker, console and Flink/Dinky start
automatically (see [streaming/README.md](../README.md) for the smoke test).

## Use case

We simulate **IoT sensors** in several houses measuring electricity (`strom`)
and water (`wasser`) consumption. The sensors push readings into Redpanda as
JSON events, keyed by house id (`haus_a`, `haus_b`, ...).

| Field        | Example      | Meaning                              |
|--------------|--------------|--------------------------------------|
| topic        | `strom`      | sensor type — one topic per category |
| key          | `haus_a`     | routes events of the same house into the same partition |
| value (JSON) | `{...}`      | `wert`, `einheit`, `timestamp`       |

## Environment

Codespaces forwards the following ports — find them in the **PORTS** tab of
VS Code:

| Port | Tool                | What for                                |
|------|---------------------|-----------------------------------------|
| 8080 | Redpanda Console    | look at topics, messages, consumer groups |
| 9092 | Redpanda Kafka API  | (no UI — clients connect here)          |

Inside the workspace container the broker is reachable as `redpanda:29092`
(Docker network). All notebooks already use that address — you don't need to
change anything.

## Setup (once per Codespace)

The `postCreateCommand` in `.devcontainer/devcontainer.json` already runs
`uv sync` for you. To select the right kernel in a notebook:

1. Open the notebook
2. Top-right → **Select Kernel**
3. **Python Environments…** → pick `streaming/.venv`

If the venv is missing (e.g. after a rebuild) recreate it from a terminal:

```bash
cd streaming
uv sync
```

## Folder layout

```
week06_kafka/
├── README.md                                 # this file
├── demo/                                     # tiny working examples — run top-to-bottom
│   ├── demo_produce.ipynb
│   └── demo_consume.ipynb
├── exercises/                                # progressive exercises — TODOs to fill in
│   ├── exercise_produce_01.ipynb             # single event, keys, broken broker
│   ├── exercise_produce_02.ipynb             # batch + partition assignment
│   ├── exercise_produce_03.ipynb             # streaming + anomaly simulation
│   ├── exercise_consume_01.ipynb             # basic poll loop, consumer groups
│   ├── exercise_consume_02.ipynb             # manual offsets, two consumers in one group
│   └── exercise_consume_03.ipynb             # aggregation + anomaly detection
└── solutions/                                # full working answers — only after a try
    └── … same names …
```

**Recommended order**

1. Skim `demo/demo_produce.ipynb` and `demo/demo_consume.ipynb`.
2. Work through `exercise_produce_01` → `_02` → `_03`.
3. Then `exercise_consume_01` → `_02` → `_03`.

Producer and consumer exercises are designed to be opened in **two tabs side
by side** so you can see events flow through Redpanda in real time.

## Helpful CLI commands

Open a terminal in VS Code and try:

```bash
# list topics
rpk topic list -X brokers=redpanda:29092

# create a topic with 2 partitions (auto-create works too, but here you can pick the partition count)
rpk topic create strom  -p 2 -X brokers=redpanda:29092
rpk topic create wasser -p 2 -X brokers=redpanda:29092

# tail a topic live (Ctrl-C to stop)
rpk topic consume strom -X brokers=redpanda:29092

# produce a one-off event from the shell
echo '{"wert": 99.9}' | rpk topic produce strom -k haus_c -X brokers=redpanda:29092
```

## Troubleshooting

| Symptom                                        | Fix                                                                 |
|------------------------------------------------|---------------------------------------------------------------------|
| `KafkaError: Broker not available`             | wait ~10 s after Codespace start, or run `rpk cluster info -X brokers=redpanda:29092` to check |
| Notebook can't find `confluent_kafka`          | run `cd streaming && uv sync`, then re-pick the kernel              |
| Topic doesn't appear in the Console            | refresh the page; or create explicitly with `rpk topic create`      |
| Same key lands in different partitions         | you probably re-created the topic with a different partition count  |

## References

- [Apache Kafka docs](https://kafka.apache.org/documentation/)
- [Redpanda docs](https://docs.redpanda.com/)
- [confluent-kafka Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Confluent Kafka 101 (videos)](https://developer.confluent.io/courses/apache-kafka/events/)
