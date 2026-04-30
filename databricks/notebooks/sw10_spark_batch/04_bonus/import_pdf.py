# Databricks notebook source
# MAGIC %md
# MAGIC # Import PDF Files
# MAGIC
# MAGIC ## About this Notebook
# MAGIC This notebook demonstrates how to extract text and data from PDF files in Databricks.
# MAGIC
# MAGIC ## What we'll do:
# MAGIC - Read PDF files using PyPDF2 library
# MAGIC - Extract text content
# MAGIC - Parse structured data from PDFs
# MAGIC - Save extracted data to Delta tables
# MAGIC
# MAGIC ## Use Cases:
# MAGIC - Extract text from documents
# MAGIC - Parse invoices, receipts, reports
# MAGIC - Document analysis and indexing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries

# COMMAND ----------

# MAGIC %pip install PyPDF2 pdfplumber

# COMMAND ----------

# Restart Python after installing packages
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Import Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import PyPDF2
import pdfplumber
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Paths

# COMMAND ----------

# Path to specific PDF file
pdf_file_path = "/Volumes/workspace/raw/sample_data/pdf/employee_data.pdf"

# Target schema
schema_name = "workspace.bronze"

print(f"PDF file: {pdf_file_path}")
print(f"Target schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check PDF File

# COMMAND ----------

# Check if file exists
try:
    file_info = dbutils.fs.ls(pdf_file_path)
    size_kb = file_info[0].size / 1024
    print(f"✓ PDF file found: employee_data.pdf")
    print(f"  Size: {size_kb:.2f} KB")
except Exception as e:
    print(f"✗ Error: {e}")
    print(f"Please ensure the file exists at: {pdf_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Text from PDF (Method 1: PyPDF2)
# MAGIC
# MAGIC PyPDF2 is a basic PDF library that can extract text from PDF files.
# MAGIC
# MAGIC **Advantages**: Simple, lightweight
# MAGIC **Limitations**: May not handle complex layouts or tables well

# COMMAND ----------

def extract_text_pypdf2(pdf_path):
    """
    Extract text from PDF using PyPDF2
    
    Args:
        pdf_path: Path to PDF file in Volumes
    
    Returns:
        Dictionary with page number and text content
    """
    results = []
    
    try:
        # Read PDF directly from Volumes using open()
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            num_pages = len(pdf_reader.pages)
            
            print(f"PDF has {num_pages} pages")
            
            # Extract text from each page
            for page_num in range(num_pages):
                page = pdf_reader.pages[page_num]
                text = page.extract_text()
                
                results.append({
                    "page_number": page_num + 1,
                    "text": text,
                    "char_count": len(text)
                })
        
        return results
        
    except Exception as e:
        print(f"Error extracting text: {e}")
        return []

# COMMAND ----------

# Extract text from employee_data.pdf
print(f"Extracting text from: employee_data.pdf\n")

text_data = extract_text_pypdf2(pdf_file_path)

# Display sample text from first page
if text_data:
    print(f"\nSample text from page 1:")
    print("="*60)
    print(text_data[0]['text'][:500])  # First 500 characters
    print("="*60)
else:
    print("No text extracted from PDF")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Extract Text from PDF (Method 2: pdfplumber)
# MAGIC
# MAGIC pdfplumber is better for structured data and tables.

# COMMAND ----------

def extract_with_pdfplumber(pdf_path):
    """
    Extract text and tables from PDF using pdfplumber
    
    Args:
        pdf_path: Path to PDF file in Volumes
    
    Returns:
        Dictionary with pages, text, and tables
    """
    results = {
        "pages": [],
        "tables": []
    }
    
    try:
        # Read PDF directly from Volumes
        with pdfplumber.open(pdf_path) as pdf:
            print(f"PDF has {len(pdf.pages)} pages")
            
            for i, page in enumerate(pdf.pages):
                page_num = i + 1
                
                # Extract text
                text = page.extract_text()
                
                # Extract tables (if any)
                tables = page.extract_tables()
                
                results["pages"].append({
                    "page_number": page_num,
                    "text": text,
                    "char_count": len(text) if text else 0,
                    "table_count": len(tables)
                })
                
                # Store tables
                for table_idx, table in enumerate(tables):
                    results["tables"].append({
                        "page_number": page_num,
                        "table_index": table_idx + 1,
                        "table_data": table
                    })
        
        return results
        
    except Exception as e:
        print(f"Error extracting with pdfplumber: {e}")
        return results

# COMMAND ----------

# Extract with pdfplumber
print(f"Extracting with pdfplumber from: employee_data.pdf\n")

pdf_data = extract_with_pdfplumber(pdf_file_path)

print(f"Extracted:")
print(f"  Pages: {len(pdf_data['pages'])}")
print(f"  Tables found: {len(pdf_data['tables'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Extracted Tables

# COMMAND ----------

# Show extracted tables
if pdf_data['tables']:
    print("Extracted Tables:\n")
    
    for table_info in pdf_data['tables']:
        print(f"Page {table_info['page_number']}, Table {table_info['table_index']}:")
        print("-" * 60)
        
        # Display first few rows
        table = table_info['table_data']
        for row in table[:5]:  # First 5 rows
            print(row)
        print()
else:
    print("No tables found in PDF")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Identify Tables by Header
# MAGIC
# MAGIC Find the three specific tables: Employee Directory, Department Statistics, and Projects Overview

# COMMAND ----------

# Extract text from all pages to find table headers
def identify_table_sections(pdf_data):
    """
    Identify which table corresponds to which section based on text content
    """
    table_mapping = {}
    
    for page_info in pdf_data['pages']:
        page_text = page_info['text']
        page_num = page_info['page_number']
        
        # Check which headers appear on this page
        if "Employee Directory" in page_text:
            table_mapping['employee_directory'] = page_num
        if "Department Statistics" in page_text:
            table_mapping['department_statistics'] = page_num
        if "Projects Overview" in page_text:
            table_mapping['projects_overview'] = page_num
    
    return table_mapping

table_sections = identify_table_sections(pdf_data)
print("Identified table sections:")
for section, page in table_sections.items():
    print(f"  {section}: Page {page}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split Combined Table into Separate Tables
# MAGIC
# MAGIC pdfplumber extracts all data as one table. We need to split it by identifying header rows.

# COMMAND ----------

def split_table_by_headers(table_data):
    """
    Split a combined table into separate tables by identifying header rows.
    Returns a dict with table name and their data.
    """
    # Define expected headers for each table
    header_patterns = {
        'employee_directory': ['employee id', 'first name', 'last name', 'department', 'position'],
        'department_statistics': ['department', 'employee count', 'average salary', 'total salary'],
        'projects_overview': ['project id', 'project name', 'team lead', 'start date', 'end date']
    }
    
    split_tables = {}
    current_section = None
    current_headers = None
    current_data = []
    
    for row_idx, row in enumerate(table_data):
        if not row or all(not cell or str(cell).strip() == '' for cell in row):
            continue
        
        # Check if this row is a header row
        row_text = ' '.join([str(cell).lower().strip() for cell in row if cell])
        
        matched_section = None
        for section, patterns in header_patterns.items():
            # Check if at least 3 patterns match - use builtin sum, not PySpark sum
            matches = 0
            for pattern in patterns:
                if pattern in row_text:
                    matches += 1
            
            if matches >= 3:
                matched_section = section
                break
        
        if matched_section:
            # Save previous section if exists
            if current_section and current_data:
                split_tables[current_section] = {
                    'headers': current_headers,
                    'data': current_data
                }
            
            # Start new section
            current_section = matched_section
            current_headers = row
            current_data = []
            print(f"✓ Found header for {matched_section} at row {row_idx}")
        else:
            # Add to current section's data
            if current_section and current_headers:
                current_data.append(row)
    
    # Save last section
    if current_section and current_data:
        split_tables[current_section] = {
            'headers': current_headers,
            'data': current_data
        }
    
    return split_tables

# Organize tables by their section
tables_by_section = {
    'employee_directory': None,
    'department_statistics': None,
    'projects_overview': None
}

if pdf_data['tables']:
    print(f"Processing {len(pdf_data['tables'])} table(s) from PDF...\n")
    
    for table_info in pdf_data['tables']:
        table_data = table_info['table_data']
        
        if not table_data or len(table_data) <= 1:
            print("⚠ Skipping empty table")
            continue
        
        print(f"Table has {len(table_data)} rows")
        
        # Split the combined table
        try:
            split_tables = split_table_by_headers(table_data)
            print(f"Split into {len(split_tables)} separate tables")
        except Exception as e:
            print(f"Error splitting table: {e}")
            import traceback
            traceback.print_exc()
            continue
        
        # Process each split table
        for section_name, table_content in split_tables.items():
            headers = table_content['headers']
            rows = table_content['data']
            
            print(f"\n{'='*60}")
            print(f"Processing: {section_name}")
            print(f"{'='*60}")
            print(f"Rows: {len(rows)}")
            
            # Clean headers
            cleaned_headers = []
            for i, h in enumerate(headers):
                if h is None or str(h).strip() == "":
                    cleaned_headers.append(f"col_{i}")
                else:
                    # Clean header names
                    clean_h = str(h).strip().replace(" ", "_").replace("/", "_").lower()
                    cleaned_headers.append(clean_h)
            
            print(f"Headers: {cleaned_headers}")
            
            # Clean rows and filter out empty/header rows
            cleaned_rows = []
            for row in rows:
                cleaned_row = [str(cell).strip() if cell is not None else "" for cell in row]
                
                # Skip empty rows or rows that look like section headers
                if not any(cell for cell in cleaned_row):
                    continue
                    
                row_text = ' '.join(cleaned_row).lower()
                if any(skip in row_text for skip in ['employee directory', 'department statistics', 'projects overview', 'notes', 'this document contains']):
                    continue
                
                cleaned_rows.append(cleaned_row)
            
            print(f"Data rows after cleaning: {len(cleaned_rows)}")
            
            try:
                # Create DataFrame
                if cleaned_rows:
                    df_table = spark.createDataFrame(cleaned_rows, cleaned_headers)
                    
                    # Remove empty columns (columns that only contain empty strings or are named col_N)
                    # First, identify columns that are either named col_N or contain only empty values
                    columns_to_keep = []
                    for col_name in df_table.columns:
                        # Keep columns that don't match the pattern col_N (generated placeholder names)
                        if not col_name.startswith('col_'):
                            columns_to_keep.append(col_name)
                        else:
                            # For col_N columns, check if they have any non-empty values
                            non_empty_count = df_table.filter(df_table[col_name] != "").count()
                            if non_empty_count > 0:
                                columns_to_keep.append(col_name)
                    
                    # Select only the columns we want to keep
                    if columns_to_keep:
                        df_table = df_table.select(*columns_to_keep)
                        print(f"Kept {len(columns_to_keep)} columns: {columns_to_keep}")
                    
                    tables_by_section[section_name] = df_table
                    
                    print(f"✓ Created DataFrame for {section_name}")
                    display(df_table)
                else:
                    print(f"⚠ No data rows found for {section_name}")
            except Exception as e:
                print(f"Could not create DataFrame for {section_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Tables to Delta

# COMMAND ----------

# Drop any existing tables from a previous run so a stale schema does not block
# the overwrite (Delta refuses schema changes on `overwrite` when Table ACLs are
# enabled, and overwriteSchema=true is rejected on those clusters too).
for section_name, df in tables_by_section.items():
    if df is not None:
        table_name = f"{schema_name}.pdf_employee_data_{section_name}"
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped (if existed): {table_name}")

# COMMAND ----------

# Save each table to its own Delta table
saved_tables = []

for section_name, df in tables_by_section.items():
    if df is not None:
        # Create table name
        table_name = f"{schema_name}.pdf_employee_data_{section_name}"

        try:
            # Save DataFrame with schema overwrite to handle schema changes
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)
            
            saved_tables.append(table_name)
            print(f"✓ Saved {section_name} to: {table_name}")
        except Exception as e:
            print(f"✗ Error saving {section_name}: {e}")

print(f"\n✓ Total tables saved: {len(saved_tables)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Saved Tables

# COMMAND ----------

# Display each saved table
for table_name in saved_tables:
    print(f"\n{'='*60}")
    print(f"Table: {table_name}")
    print(f"{'='*60}")
    
    df_verify = spark.sql(f"SELECT * FROM {table_name}")
    display(df_verify)
