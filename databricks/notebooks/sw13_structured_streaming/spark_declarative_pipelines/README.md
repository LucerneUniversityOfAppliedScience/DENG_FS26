# Spark Declarative Pipelines

A teaching module on **Lakeflow Declarative Pipelines** in Databricks, using the new
`from pyspark import pipelines as dp` API. Students learn to build a medallion-architecture
data pipeline (bronze → silver → gold) step by step on the `samples.wanderbricks` dataset.

## Folder Overview

| Folder | Purpose |
|--------|---------|
| [`tutorial/`](tutorial/) | **Start here.** Numbered notebooks with concept explanations in Markdown cells and `raise NotImplementedError` stubs for students to implement. |
| [`solution/`](solution/) | Fully implemented version of every tutorial notebook. Use as a sanity check after you finish a notebook. |
| [`Wanderbricks_Sample_Declarative_Pipeline/`](Wanderbricks_Sample_Declarative_Pipeline/) | A reference pipeline showing the same logic as `solution/`, but organized "one table per file" — the conventional Databricks template layout. |

## Learning Path

The tutorial is a sequence of 9 transformation notebooks plus 2 exploration notebooks. Recommended order:

1. [`tutorial/explorations/00_explore_samples.py`](tutorial/explorations/00_explore_samples.py) — get familiar with `samples.wanderbricks.*`
2. [`tutorial/transformations/01_bronze_users.py`](tutorial/transformations/01_bronze_users.py) — first `@dp.materialized_view`
3. [`tutorial/transformations/02_bronze_remaining.py`](tutorial/transformations/02_bronze_remaining.py) — repeat the pattern for hosts/properties/bookings/reviews
4. [`tutorial/transformations/03_silver_users_cleaned.py`](tutorial/transformations/03_silver_users_cleaned.py) — cleaning + first `@dp.expect_or_drop`
5. [`tutorial/transformations/04_silver_properties_enriched.py`](tutorial/transformations/04_silver_properties_enriched.py) — first multi-table join
6. [`tutorial/transformations/05_silver_bookings_and_reviews.py`](tutorial/transformations/05_silver_bookings_and_reviews.py) — more joins, more expectations
7. [`tutorial/transformations/06_gold_aggregations.py`](tutorial/transformations/06_gold_aggregations.py) — `groupBy().agg()` patterns
8. [`tutorial/transformations/07_gold_windows.py`](tutorial/transformations/07_gold_windows.py) — window functions and rankings
9. [`tutorial/transformations/08_streaming_tables.py`](tutorial/transformations/08_streaming_tables.py) — `@dp.table` + `spark.readStream`
10. [`tutorial/transformations/09_capstone.py`](tutorial/transformations/09_capstone.py) — open-ended exercise on a different sample dataset
11. [`tutorial/explorations/inspect_pipeline_output.py`](tutorial/explorations/inspect_pipeline_output.py) — query the resulting tables

## How to Run

The tutorial is a Lakeflow Declarative Pipeline. In Databricks:

1. Open the workspace and create a new **Lakeflow Declarative Pipeline**.
2. Point the **source code** to `spark_declarative_pipelines/tutorial/` (or just `tutorial/transformations/` if you prefer not to include explorations).
3. Choose a **target catalog** (`workspace` by default in this course) and a **target schema** (e.g. `wanderbricks_tutorial_<your_name>`).
4. Click **Run pipeline**. Initially every table fails with `NotImplementedError` — that's expected. Implement notebooks one by one and re-run.

Exploration notebooks under `explorations/` are **not** part of the pipeline graph. Open them as regular notebooks to inspect inputs/outputs.

## Prerequisites

- Access to a Databricks workspace with Lakeflow Declarative Pipelines enabled
- The `samples.wanderbricks.*` dataset (available by default on Databricks)
- Basic familiarity with PySpark DataFrames (`select`, `filter`, `groupBy`, joins)

## Reference

- [Lakeflow Declarative Pipelines overview](https://docs.databricks.com/ldp)
- [Python developer reference](https://docs.databricks.com/ldp/developer/python-ref)
