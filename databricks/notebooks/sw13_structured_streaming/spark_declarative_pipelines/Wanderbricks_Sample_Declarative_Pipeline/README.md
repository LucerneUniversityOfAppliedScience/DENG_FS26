# Wanderbricks Sample Declarative Pipeline

A Spark Declarative Pipeline built on the `samples.wanderbricks` dataset (vacation rental bookings).
It demonstrates a medallion architecture (bronze → silver → gold) using the new
`from pyspark import pipelines as dp` API.

## Folder Structure

- `transformations/` — All dataset definitions, organized by medallion layer:
  - `bronze/` — Raw ingest from `samples.wanderbricks.*` (users, hosts, properties, bookings, reviews)
  - `silver/` — Cleaned and enriched data with `@dp.expect_or_drop` data quality rules
  - `gold/` — Business-level aggregates (booking analytics, host/property performance, review summary)
- `explorations/` — Ad-hoc notebooks for exploring the pipeline output
- `utilities/` — Shared Python helpers used across transformations

## Conventions

- Every dataset is defined in its own file under `transformations/<layer>/`.
- Bronze and silver tables are `@dp.materialized_view`s; gold tables aggregate the silver layer.
- Data quality is enforced in the silver layer via `@dp.expect_or_drop(...)`.

## Running the Pipeline

- **Run file** — preview a single transformation in the Databricks UI.
- **Run pipeline** — execute all transformations end-to-end.
- **Schedule** — attach a schedule to run the pipeline periodically.

## References

- Spark Declarative Pipelines (Python): https://docs.databricks.com/ldp/developer/python-ref
- Lakeflow Declarative Pipelines overview: https://docs.databricks.com/ldp
