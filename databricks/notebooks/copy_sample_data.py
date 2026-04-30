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
dbutils.widgets.text(name="nyc_schema", defaultValue="nyc_taxi", label="NYC Taxi Schema")

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
# MAGIC ## NYC Taxi Lookup Tables
# MAGIC
# MAGIC Creates the three static lookup tables (`vendor_list`, `payment_types`, `rate_codes`) in the
# MAGIC `nyc_taxi` schema. These are used by the NYC taxi exercises and do not depend on the
# MAGIC trip data parquet files.

# COMMAND ----------

nyc_schema = dbutils.widgets.get("nyc_schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{nyc_schema}")
print(f"✓ Schema ready: {catalog}.{nyc_schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.vendor_list') (
# MAGIC   VendorID    INT,
# MAGIC   VendorName  STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.vendor_list') VALUES
# MAGIC   (1, 'Creative Mobile Technologies (CMT)'),
# MAGIC   (2, 'Curb Mobility (Curb)'),
# MAGIC   (3, 'Arro'),
# MAGIC   (4, 'Dispatch'),
# MAGIC   (5, 'NYC Taxi'),
# MAGIC   (6, 'Myle Technologies'),
# MAGIC   (7, 'Helix');
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.vendor_list');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.payment_types') (
# MAGIC   payment_type        INT,
# MAGIC   payment_description STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.payment_types') VALUES
# MAGIC   (0, 'Flex Fare trip'),
# MAGIC   (1, 'Credit card'),
# MAGIC   (2, 'Cash'),
# MAGIC   (3, 'No charge'),
# MAGIC   (4, 'Dispute'),
# MAGIC   (5, 'Unknown'),
# MAGIC   (6, 'Voided trip');
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.payment_types');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.rate_codes') (
# MAGIC   RatecodeID          INT,
# MAGIC   RatecodeDescription STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.rate_codes') VALUES
# MAGIC   (1,  'Standard rate'),
# MAGIC   (2,  'JFK flat fare'),
# MAGIC   (3,  'Newark flat fare'),
# MAGIC   (4,  'Nassau or Westchester'),
# MAGIC   (5,  'Negotiated fare'),
# MAGIC   (6,  'Group ride'),
# MAGIC   (99, 'Unknown or null');
# MAGIC
# MAGIC SELECT * FROM IDENTIFIER(:catalog_name || '.' || :nyc_schema || '.rate_codes');

