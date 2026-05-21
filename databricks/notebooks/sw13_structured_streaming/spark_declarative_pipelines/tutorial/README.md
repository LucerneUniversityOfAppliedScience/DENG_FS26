# Tutorial — Spark Declarative Pipelines

Step-by-step notebooks to learn Lakeflow Declarative Pipelines on `samples.wanderbricks`.

## How to Work Through This

1. Open [`explorations/00_explore_samples.py`](explorations/00_explore_samples.py) as a regular
   Databricks notebook (it is **not** part of the pipeline). Run it to get familiar with the
   source data.
2. Create a new **Lakeflow Declarative Pipeline** in your Databricks workspace and point its
   **source code** location at `spark_declarative_pipelines/tutorial/` (or
   `tutorial/transformations/` if you want to keep explorations separate). Target catalog:
   `workspace`. Target schema: pick something like `wanderbricks_tutorial_<your_name>`.
3. Click **Run pipeline**. Every table will fail with `NotImplementedError` — that's expected.
4. Open `transformations/01_bronze_users.py`. Read the Markdown explanation, then implement the
   function body (replace `raise NotImplementedError(...)` with the actual code). Save.
5. Re-run the pipeline. `bronze_users` should now succeed.
6. Repeat for `02`, `03`, … `09`.
7. When stuck, compare with the corresponding file in [`../solution/`](../solution/).

## Notebook Index

| # | File | Concept |
|---|------|---------|
| 00 | [explorations/00_explore_samples.py](explorations/00_explore_samples.py) | Source data exploration (not in pipeline) |
| 01 | [transformations/01_bronze_users.py](transformations/01_bronze_users.py) | `@dp.materialized_view` — first table |
| 02 | [transformations/02_bronze_remaining.py](transformations/02_bronze_remaining.py) | Repeat pattern for the 4 remaining bronze tables |
| 03 | [transformations/03_silver_users_cleaned.py](transformations/03_silver_users_cleaned.py) | Silver cleaning + `@dp.expect_or_drop` |
| 04 | [transformations/04_silver_properties_enriched.py](transformations/04_silver_properties_enriched.py) | First multi-table join |
| 05 | [transformations/05_silver_bookings_and_reviews.py](transformations/05_silver_bookings_and_reviews.py) | More joins, more expectations |
| 06 | [transformations/06_gold_aggregations.py](transformations/06_gold_aggregations.py) | `groupBy().agg()` |
| 07 | [transformations/07_gold_windows.py](transformations/07_gold_windows.py) | Window functions & ranks |
| 08 | [transformations/08_streaming_tables.py](transformations/08_streaming_tables.py) | `@dp.table` + `spark.readStream` |
| 09 | [transformations/09_capstone.py](transformations/09_capstone.py) | Open-ended exercise on a different sample dataset |
| — | [explorations/inspect_pipeline_output.py](explorations/inspect_pipeline_output.py) | Query the pipeline output |
