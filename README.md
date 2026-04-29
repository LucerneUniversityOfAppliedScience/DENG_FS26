# DENG_FS26

Repository by Stefan Koch for the DENG module.

The Databricks notebooks are located in the `databricks/` folder.

## NYC Taxi Data on Databricks Free Edition

The NYC taxi exercises rely on the yellow taxi parquet files from the NYC TLC
CloudFront distribution (`https://d37ci6vzurychx.cloudfront.net/...`).

**Databricks Free Edition blocks outbound internet traffic by firewall**, so any
direct download of these files from within a Databricks notebook will fail with
a name-resolution error. On Free Edition you must upload the parquet files
manually:

1. Download the monthly `yellow_tripdata_2025-MM.parquet` files from Ilias
   (or from the NYC TLC site outside Databricks).
2. Upload them into the volume `/Volumes/<catalog>/nyc_taxi/raw_files/`.
3. Read them from that volume in the exercise notebooks.

The static lookup tables (`vendor_list`, `payment_types`, `rate_codes`) do not
require any download — they are created by `copy_sample_data.py`.