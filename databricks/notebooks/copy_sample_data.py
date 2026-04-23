# Databricks notebook source
# MAGIC %md
# MAGIC # Copy Sample Data to Volume
# MAGIC
# MAGIC This notebook copies all sample data files from the workspace repository to the Unity Catalog volume.
# MAGIC Uses proper Databricks workspace file access patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Libraries

# COMMAND ----------

import os
import shutil
import sys
from pathlib import Path

# COMMAND ----------

dbutils.widgets.text(name="catalog_name", defaultValue="workspace", label="Catalog")

# COMMAND ----------

try:
    catalog = dbutils.widgets.get("catalog_name")
    if not catalog or catalog.strip() == "":
        raise ValueError("Catalog parameter cannot be empty")
    
    notebook_path = os.getcwd()
    sample_data_path = notebook_path.replace("notebooks", "sample_data")
    
    # Validate that sample data path exists
    if not os.path.exists(sample_data_path):
        raise FileNotFoundError(f"Sample data path does not exist: {sample_data_path}")
    
    target_dir = f"/Volumes/{catalog}/raw/sample_data"

    print(f"✓ Configuration validated successfully:")
    print(f"  catalog: {catalog}")
    print(f"  target_dir: {target_dir}")
    print(f"  sample_data_path: {sample_data_path}")
    
except ValueError as ve:
    print(f"❌ Configuration error: {ve}")
    sys.exit(1)
except FileNotFoundError as fe:
    print(f"❌ File path error: {fe}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error during configuration: {e}")
    sys.exit(1)

# COMMAND ----------

# Initialize counters for reporting
total_files = 0
copied_files = 0
failed_files = 0
skipped_files = 0

print(f"🔄 Starting file copy operation...")
print(f"📂 Source: {sample_data_path}")
print(f"📁 Destination: {target_dir}")
print("-" * 50)

try:
    # Walk through all files in the sample data directory
    for root, dirs, files in os.walk(sample_data_path):
        for file in files:
            total_files += 1
            source_file_path = os.path.join(root, file)
            relative_path = source_file_path.replace(sample_data_path, "")
            target_file_path = f"{target_dir}{relative_path}"
            
            try:
                # Skip hidden files and temporary files
                if file.startswith('.') or file.endswith('.tmp'):
                    print(f"⏭️  Skipping hidden/temporary file: {file}")
                    skipped_files += 1
                    continue
                
                # Create the target directory if it does not exist
                target_file_dir = os.path.dirname(target_file_path)
                
                try:
                    if not os.path.exists(target_file_dir):
                        os.makedirs(target_file_dir, exist_ok=True)
                        print(f"📁 Created directory: {target_file_dir}")
                except OSError as ose:
                    print(f"❌ Failed to create directory {target_file_dir}: {ose}")
                    failed_files += 1
                    continue
                
                # Check if source file exists and is readable
                if not os.path.exists(source_file_path):
                    print(f"❌ Source file not found: {source_file_path}")
                    failed_files += 1
                    continue
                
                if not os.access(source_file_path, os.R_OK):
                    print(f"❌ Source file not readable: {source_file_path}")
                    failed_files += 1
                    continue
                
                # Check if target file already exists
                if os.path.exists(target_file_path):
                    source_size = os.path.getsize(source_file_path)
                    target_size = os.path.getsize(target_file_path)
                    
                    if source_size == target_size:
                        print(f"✅ File already exists with same size, skipping: {file}")
                        skipped_files += 1
                        continue
                    else:
                        print(f"🔄 File exists but different size, overwriting: {file}")
                
                print(f"📋 Copying: {source_file_path}")
                print(f"📋 To:      {target_file_path}")
                
                # Perform the actual file copy
                shutil.copy2(source_file_path, target_file_path)  # copy2 preserves metadata
                
                # Verify the copy was successful
                if os.path.exists(target_file_path):
                    source_size = os.path.getsize(source_file_path)
                    target_size = os.path.getsize(target_file_path)
                    
                    if source_size == target_size:
                        print(f"✅ Successfully copied: {file} ({source_size} bytes)")
                        copied_files += 1
                    else:
                        print(f"⚠️  File copied but size mismatch: {file} (source: {source_size}, target: {target_size})")
                        failed_files += 1
                else:
                    print(f"❌ File copy failed - target file does not exist: {file}")
                    failed_files += 1
                    
            except PermissionError as pe:
                print(f"❌ Permission denied copying {file}: {pe}")
                failed_files += 1
            except OSError as ose:
                print(f"❌ OS error copying {file}: {ose}")
                failed_files += 1
            except Exception as e:
                print(f"❌ Unexpected error copying {file}: {e}")
                failed_files += 1
            
            print()  # Empty line for readability

except Exception as e:
    print(f"❌ Fatal error during file operation: {e}")
    sys.exit(1)

# Print summary report
print("=" * 50)
print("📊 COPY OPERATION SUMMARY")
print("=" * 50)
print(f"📄 Total files found:     {total_files}")
print(f"✅ Files copied:          {copied_files}")
print(f"⏭️  Files skipped:         {skipped_files}")
print(f"❌ Files failed:          {failed_files}")
print("-" * 50)

if failed_files == 0:
    print("🎉 All files processed successfully!")
elif copied_files > 0:
    print(f"⚠️  Operation completed with {failed_files} errors")
else:
    print("❌ Operation failed - no files were copied")
    sys.exit(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download NYC Taxi Data (2025)

# COMMAND ----------

import pandas as pd

nyc_target_dir = f"/Volumes/{catalog}/raw/files/NYC"
os.makedirs(nyc_target_dir, exist_ok=True)

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

for month in range(1, 13):
    month_str = str(month).zfill(2)
    filename = f"yellow_tripdata_2025-{month_str}.parquet"
    url = f"{base_url}/{filename}"
    target_path = f"{nyc_target_dir}/{filename}"

    if os.path.exists(target_path):
        print(f"⏭️  Already exists, skipping: {filename}")
        continue

    try:
        print(f"⬇️  Downloading: {url}")
        pdf = pd.read_parquet(url)
        pdf.to_parquet(target_path, index=False)
        print(f"✅ Saved: {target_path} ({len(pdf):,} rows)")
    except Exception as e:
        print(f"❌ Failed {filename}: {e}")

print("\n🎉 NYC Taxi Data download complete.")
