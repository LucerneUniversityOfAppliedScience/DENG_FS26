# Dinky Guide

[Dinky](http://www.dlink.top/) is a web IDE for Apache Flink. You write
SQL in the browser, it ships the job to the Flink cluster, and shows you
results.

You only need Dinky for the **Flink track**
([../03_exercise/exercise_07_flink.md](../03_exercise/exercise_07_flink.md)).
The Kafka notebooks don't use it.

## Open Dinky

In VS Code, click the **PORTS** tab at the bottom → port `8888` → globe
icon (*Open in Browser*).

First time only: leave everything at its default → *Next* through any
welcome dialogs. You'll land in the Dinky home page.

> If the UI is in Chinese, open Dinky in an incognito tab — the
> container patches the language on first run, but a cached old session
> can override it.

## Step 1 — Register the Flink cluster (once per Codespace)

Dinky needs to know where to send your SQL jobs.

> ⚠️ **Watch the submenu name.** Under *Registration Center → Cluster*
> there are **two** options that look similar:
>
> | Submenu | Use it for | Use it here? |
> |---|---|---|
> | **Cluster Instance** (a.k.a. *Flink Instance*) | a long-running Flink session you connect to | ✅ **Yes** |
> | **Cluster Config** | Yarn / Kubernetes / Application-mode jobs (Hadoop config, Flink Lib Path, etc.) | ❌ no |
>
> If the dialog you see says *"Yarn (Pre-Job/Application)"* and asks for
> Hadoop config → you're in the wrong place. Go back and pick
> *Cluster Instance* instead.

1. Top menu: **Registration Center**
2. Side menu: **Cluster** → **Cluster Instance** (sometimes labelled
   *Flink Instance*)
3. Click **+ Add** (or *Create*)
4. Fill in:

   | Field | Value |
   |---|---|
   | **Name** | `local-flink` |
   | **Type** | **`Standalone`** (not Yarn!) |
   | **JobManager HA address** | `http://flink-jobmanager:8081` |
   | (everything else) | leave at default |

5. Click **Heartbeat** / **Test** — you should get a green ✓.
6. Click **Submit** / **Save**.

The cluster status indicator should turn **green / Normal**. If it stays
red, double-check the JobManager URL — it must be `http://flink-jobmanager:8081`
(Docker DNS), *not* `localhost:8081`.

## Step 2 — Open Data Studio and create a task

The actual SQL editor is under **Data Studio** in the top menu.

1. **Data Studio** → click the **+** in the file tree to create a new
   task.
2. Pick **FlinkSQL** as the type and give it any name (e.g. `scratch`).
3. In the right-hand panel set:
   - **Cluster Configuration** → `local-flink` (the one you registered).
   - **Catalog** → `DefaultCatalog` (this is what persists `CREATE TABLE`
     definitions across tasks).

You only need to do this configuration on each *new* task — once it's
set, the task remembers it.

## Step 3 — Run something

Paste a tiny test:

```sql
SELECT 'hello from flink' AS msg;
```

Click the green ▶ **Execute** button (top-right of the editor). After
~10 s the **Result** tab at the bottom shows one row.

Stop a job with the red ■ button.

> Why "stop"? `SELECT` queries on streaming data run **forever** by
> default. They're not snapshots like in normal SQL — they're standing
> queries that keep producing rows. Each running job uses a Flink task
> slot, and there are only 4. **Stop jobs you no longer watch.**

## Step 4 — Work with persistent tables

Most exercises define a Kafka source table that lives across multiple
tasks. The pattern is always the same:

```sql
CREATE TABLE IF NOT EXISTS my_topic_table (
    -- ... columns ...
) WITH (
    'connector' = 'kafka',
    'topic' = 'my-topic',
    'properties.bootstrap.servers' = 'redpanda:29092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);
```

- Run this **once** (in any task) with **Catalog: DefaultCatalog**. The
  table now exists for the whole Codespace lifetime.
- Other tasks can `SELECT * FROM my_topic_table` directly — no need to
  re-create.
- If you ever need to change the schema: `DROP TABLE my_topic_table;`
  then re-create.

## Useful tabs in the Dinky UI

| Tab | What for |
|---|---|
| **Data Studio** | The SQL editor |
| **Result** (bottom of editor) | live rows from the job |
| **Job** | history of submitted jobs, click to inspect |
| **Registration Center → Cluster** | check that Flink is reachable |
| **Flink Web UI** (port 8081 in PORTS) | low-level: task slots, backpressure, checkpoints |

## Common pitfalls

| Symptom | Fix |
|---|---|
| `Object 'bluesky_posts' not found` | the table is in the wrong catalog. Set the task's catalog to **DefaultCatalog** and re-run the `CREATE TABLE`. |
| `No more slots available` | another job is hogging a slot. Go to the previous task → red ■. |
| Cluster status is **Abnormal** | JobManager URL wrong. It must be `http://flink-jobmanager:8081` (Docker DNS, not `localhost`). |
| `Result` tab shows nothing | the source topic is empty. Check Redpanda Console. For Bluesky: did you start the firehose? |
| You hit **Execute** and nothing happens | look at the bottom-left status bar — if it says "submitting…" wait. Otherwise check that the task's *Cluster Configuration* is set. |
