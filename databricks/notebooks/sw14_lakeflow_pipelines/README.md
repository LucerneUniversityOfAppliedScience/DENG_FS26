# sw14 — Spark Declarative Pipelines (Lakeflow / DLT)

This folder rebuilds the medallion stack from
[`../sw13_structured_streaming/`](../sw13_structured_streaming/) as a
**Lakeflow Declarative Pipeline** (a.k.a. DLT — Delta Live Tables).

Same data, same Bronze/Silver/Gold logic — different paradigm:

| Imperative (sw13) | Declarative (sw14) |
|---|---|
| `spark.readStream.format("kafka")…` | `@dlt.table` whose function returns the read |
| `df.writeStream.toTable(...)` | Return value of `@dlt.table` *is* the table |
| Manual `checkpointLocation` per query | Auto-managed by the pipeline |
| `spark.readStream.table("workspace.bronze.x")` | `dlt.read_stream("bronze_x")` (reference by function name) |
| Hand-rolled "null payload" filter | `@dlt.expect_or_drop` + metrics in the UI |
| DAG implicit in run order | DAG inferred from `dlt.read_stream` calls, visible in the UI |

## What's in this folder

```
sw14_lakeflow_pipelines/
├── README.md      ← you are here
├── bronze.py      ← @dlt.table bronze_kafka_logistics
├── silver.py      ← @dlt.table silver_logistics + 3 expectations
└── gold.py        ← @dlt.table gold_carrier_kpi_per_min
                    @dlt.table gold_shipment_journey
```

Inferred DAG:

```
bronze_kafka_logistics
        │
        ▼ dlt.read_stream("bronze_kafka_logistics")
silver_logistics  (expectations: non_null_payload, non_null_event_ts, valid_carrier)
        │
        ├──── dlt.read_stream("silver_logistics") ────▶ gold_carrier_kpi_per_min
        └──── dlt.read_stream("silver_logistics") ────▶ gold_shipment_journey
```

## Prerequisites

- `secret_scope` exists with the Aiven Kafka credentials (set up by
  [`../sw13_structured_streaming/00_setup.py`](../sw13_structured_streaming/00_setup.py)).
- `ca.pem` is uploaded to
  `/Volumes/workspace/landing/files/aiven/ca.pem` (or wherever you'll
  point the `kafka.truststore_path` configuration entry).
- Free Edition / Serverless works — see *Compute* below.

## Option A — create the pipeline via the UI

1. **Workflows → Pipelines → Create pipeline**.
2. **Pipeline mode:** *Triggered* (we want micro-batch runs, not
   continuous; that's also the only option on Free Edition).
3. **Source code:** add all three of `bronze.py`, `silver.py`, `gold.py`
   from this folder.
4. **Destination:** select Unity Catalog → catalog `workspace`,
   schema `dlt_logistics` (will be created on first run).
5. **Compute:** *Serverless* if you're on Free Edition; otherwise any
   *current* DBR.
6. **Configuration** (the key/value table — these replace sw13's
   widgets):

   | Key | Value |
   |---|---|
   | `kafka.secret_scope`     | `secret_scope` |
   | `kafka.topic`            | `logistics_data_gen` |
   | `kafka.truststore_path`  | `/Volumes/workspace/landing/files/aiven/ca.pem` |
   | `kafka.sasl_mechanism`   | `SCRAM-SHA-256` |
   | `kafka.starting_offsets` | `earliest` |
   | `windows.window_size`    | `1 minute` |
   | `windows.watermark`      | `3 minutes` |
   | `sessions.gap`           | `30 minutes` |
   | `sessions.watermark`     | `1 hour` |

7. **Save** → **Start**.

## Option B — define the pipeline in `databricks.yml`

Add this under `resources.pipelines` in the bundle config so the
pipeline is provisioned together with the notebooks:

```yaml
resources:
  pipelines:
    logistics_dlt:
      name: logistics_dlt
      catalog: workspace
      schema: dlt_logistics
      serverless: true
      photon: true
      continuous: false              # = Triggered
      libraries:
        - file: { path: ../databricks/notebooks/sw14_lakeflow_pipelines/bronze.py }
        - file: { path: ../databricks/notebooks/sw14_lakeflow_pipelines/silver.py }
        - file: { path: ../databricks/notebooks/sw14_lakeflow_pipelines/gold.py }
      configuration:
        kafka.secret_scope: secret_scope
        kafka.topic: logistics_data_gen
        kafka.truststore_path: /Volumes/workspace/landing/files/aiven/ca.pem
        kafka.sasl_mechanism: SCRAM-SHA-256
        kafka.starting_offsets: earliest
        windows.window_size: 1 minute
        windows.watermark: 3 minutes
        sessions.gap: 30 minutes
        sessions.watermark: 1 hour
```

Then `databricks bundle deploy -p free` and trigger from the UI.

## Running the pipeline

- **Start** — incremental run. Picks up where each table's checkpoint
  left off. This is what you click 99% of the time.
- **Full refresh** — drops state and reprocesses every table from the
  beginning. Useful when you change the Silver parser. Bronze is
  protected by `pipelines.reset.allowed=false` in its table properties,
  so it survives even a full refresh.

## How to interpret the run

After a successful run:

| Where | What you see |
|---|---|
| **Pipeline graph** | Four boxes, edges drawn from the `dlt.read_stream` calls |
| **Tables** in Catalog Explorer | `workspace.dlt_logistics.{bronze_kafka_logistics, silver_logistics, gold_carrier_kpi_per_min, gold_shipment_journey}` |
| **Data quality** tab | Per-expectation pass/fail counts (3 rows for Silver) |
| **Event log** | Structured events: `flow_progress`, `table_metrics`, expectations… queryable via `SELECT * FROM event_log(<pipeline_id>)` |

## Trade-offs vs sw13

**Wins**
- No `checkpointLocation` plumbing. No `availableNow` decoration. No
  schema-create boilerplate. Less code, less to get wrong.
- Data-quality is first-class. The Silver `@dlt.expect_or_drop` lines
  replace foreachBatch-DLQ patterns for the common case.
- The DAG is explicit, visible, click-through.
- Full-refresh is one button.

**Trade-offs**
- Custom sinks → you'd still need `foreachBatch` (you can do it via
  `dlt.create_streaming_table` + an `@dlt.append_flow`, but it's less
  ergonomic than the imperative version).
- Custom stateful APIs (`applyInPandasWithState`) aren't a first-class
  citizen — see `10_stateful_stuck_alerts.py` in sw13 for the
  imperative equivalent.
- Iteration is slower: you can't run "just this one cell". Every
  change requires a pipeline trigger.

Rule of thumb: use DLT for the well-trodden Bronze → Silver → Gold
path with quality constraints; drop to sw13-style imperative streams
for the long tail.
