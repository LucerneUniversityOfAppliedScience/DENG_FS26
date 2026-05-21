# Solution — Spark Declarative Pipelines Tutorial

Fully implemented version of every notebook in [`tutorial/`](../tutorial/). Use as a sanity check
after you implement a notebook yourself — do not just copy-paste.

## Folder Layout

- `transformations/` — Pipeline source notebooks (`@dp.materialized_view`, `@dp.table`, `@dp.expect_or_drop`)
- `explorations/` — Notebooks outside the pipeline graph (data exploration, output inspection)
- `utilities/` — Shared helpers (e.g. `is_valid_email` UDF)

## How to Run as a Pipeline

Same as the tutorial: in Databricks, create a Lakeflow Declarative Pipeline and point the source
code to `spark_declarative_pipelines/solution/`. Choose a target schema name distinct from the
tutorial one so the two don't overwrite each other.
