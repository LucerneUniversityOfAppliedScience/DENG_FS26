# Databricks Training Environment

This folder contains the Databricks training setup for the DENG module.

The repository is designed for a workspace-first workflow: students run everything directly in Databricks and do not need a local CLI environment.

## Run Asset Bundles Directly in the Workspace (Recommended)

### Step 1: Open the repository in Databricks
1. In Databricks, go to **Workspace**.
2. Click **Create** (or **New**) and select **Git folder**.
3. In **Repository URL**, paste:
	`https://github.com/LucerneUniversityOfAppliedScience/DENG_FS26.git`
4. Choose your provider (GitHub) and authenticate if Databricks asks for it.
5. Select your user workspace path and confirm with **Create Git folder**.
6. Open the cloned repository and navigate to the `databricks/` folder.

### Step 2: Create your `databricks.yml` from the template
`databricks.yml` is git-ignored so everyone can point it at their own workspace. In the `databricks/` folder:
1. Duplicate `databricks.yml.template` and rename the copy to `databricks.yml`.
2. Open `databricks.yml` and replace the `host:` values in the `dev` and `prod` targets with your Databricks workspace URL.
3. Adjust `catalog_name` in the `variables` section if you do not want the default `workspace`.

### Step 3: Deploy the bundle in Databricks Workspace UI
1. Open the Asset Bundle project from the `databricks/` folder (contains `databricks.yml`).
2. Select target environment (default is `prod`).
3. Run **Deploy** in the workspace UI.

The bundle creates all required schemas and volumes automatically.

### Step 4: Run the sample data job
1. Run `copy_sample_data_job` from the bundle UI (or Workflows Jobs view).
2. Wait until the job finishes successfully.

After this step, all sample files are available under `/Volumes/<catalog>/raw/sample_data`.

### Step 5: Run the NYC taxi data job (optional)
1. Upload the pre-staged NYC taxi parquet files (`yellow_tripdata_2025-01.parquet` … `yellow_tripdata_2025-12.parquet`) into the volume `/Volumes/<catalog>/nyc_taxi/raw_files/`. The files are available on **Ilias**.
2. Run `load_nyc_taxi_data_job` from the bundle UI (or Workflows Jobs view).
3. The job reads the parquet files from the volume and writes them to the Delta table `<catalog>.nyc_taxi.trips_2025`.

> **Why manual upload?** The Databricks **Free Edition** firewall blocks outbound HTTPS requests, so the original NYC TLC CloudFront URL (`d37ci6vzurychx.cloudfront.net`) cannot be reached from a notebook. The files must be pre-staged in the volume.

### Step 6: Run training notebooks
You can now run the notebooks in `notebooks/sw10_spark_batch/` directly in the Databricks workspace.

## Important Note

No local setup is required for students:
- no local Databricks CLI installation
- no local shell commands
- no manual SQL setup for schemas and volumes

Everything runs in Databricks Workspace via Asset Bundles.

## Project Structure

```
databricks/
├── databricks.yml.template           # Template for databricks.yml (committed)
├── databricks.yml                    # Main bundle config, created from template (git-ignored)
├── bundles/
│   ├── uc_resources_bundle.yml       # Unity Catalog schemas & volumes
│   ├── copy_sample_data.job.yml      # Job to copy sample data
│   └── load_nyc_taxi_data.job.yml    # Job to load NYC yellow taxi 2025 data
├── notebooks/
│   ├── copy_sample_data.py           # Notebook used by the sample data job
│   ├── load_nyc_taxi_data.py         # Notebook used by the NYC taxi job
│   └── sw10_spark_batch/                         # Training notebooks
└── sample_data/                      # Files copied to Unity Catalog volumes
```

## What Gets Created

### Schemas (Medallion Architecture)
| Schema | Description |
|--------|-------------|
| `bronze` | Raw ingested data |
| `silver` | Cleaned and validated data |
| `gold` | Business-ready aggregated data |
| `raw` | Raw data storage |
| `landing` | Landing zone for incoming data |
| `external` | External data sources |
| `demo` | Demo and exercise schema |
| `nyc_taxi` | NYC taxi trip data and reference tables |

### Volumes
- `workspace.raw.sample_data`
- `workspace.raw.files`
- `workspace.demo.data`
- `workspace.nyc_taxi.raw_files` — pre-staged NYC taxi parquet files

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)
