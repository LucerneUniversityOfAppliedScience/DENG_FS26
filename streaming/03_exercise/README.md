# Streaming Exercises

Two progressive tracks. Both run end-to-end inside the Codespace stack —
do [../01_setup/](../01_setup/) first.

## Track A — Kafka with Python

Use case: **IoT sensors** in several houses report electricity (`strom`)
and water (`wasser`) consumption. Each event is keyed by house id
(`haus_a`, `haus_b`, …) so events from the same house land in the same
partition.

The six notebooks build on each other — work them top to bottom.

| Step | File | What you practice |
|---|---|---|
| 01 | [exercise_01_produce_single.ipynb](exercise_01_produce_single.ipynb) | first producer, keys, broken-broker behaviour |
| 02 | [exercise_02_consume_basic.ipynb](exercise_02_consume_basic.ipynb) | basic poll loop, live consumer, `group.id` |
| 03 | [exercise_03_produce_batch.ipynb](exercise_03_produce_batch.ipynb) | batch producer, partition assignment, keyless events |
| 04 | [exercise_04_consume_groups.ipynb](exercise_04_consume_groups.ipynb) | manual offset commits, two consumers in one group |
| 05 | [exercise_05_produce_stream.ipynb](exercise_05_produce_stream.ipynb) | continuous stream + anomaly simulation |
| 06 | [exercise_06_consume_aggregate.ipynb](exercise_06_consume_aggregate.ipynb) | running aggregation + anomaly detection |

> Tip: open producer and consumer notebooks in **two tabs side by side**
> so you see events flow in real time. Drag a notebook tab to the right
> edge to split the editor.

Solutions live in [../04_solution/](../04_solution/). Try the exercise
first.

## Track B — Flink SQL on the Bluesky firehose

[flink_exercises.md](flink_exercises.md) — progressive Flink SQL
exercises (filter → tumble → hop → session → reply rate → keyword)
running on the live Bluesky public-post firehose.

Before starting, walk through [../01_setup/dinky_guide.md](../01_setup/dinky_guide.md):
it covers the one-time Dinky/Flink registration and creates the
`bluesky_posts` source table you'll use throughout the track.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `KafkaError: Broker not available` (Python) | wait ~10 s after Codespace start, or run `rpk cluster info -X brokers=redpanda:29092` |
| Notebook can't find `confluent_kafka` | from a terminal: `cd streaming && uv sync`, then re-pick the kernel |
| `Object 'bluesky_posts' not found` (Dinky) | re-run the `CREATE TABLE`; ensure catalog is *DefaultCatalog* |
| `No more slots available` (Flink) | stop another running job (red ■) |
| Dinky UI is in Chinese | open it in an incognito tab or clear localStorage for the Dinky tab |
