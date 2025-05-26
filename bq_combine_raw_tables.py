from google.cloud import bigquery

# Set these to your project and dataset
PROJECT_ID = "jse-datasphere"
DATASET_ID = "jse_raw_financial_data_dev_elroy"
ANALYTICAL_TABLE = "jse_raw_analytical"

client = bigquery.Client(project=PROJECT_ID)

# 1. List all tables matching jse_raw_*
tables = client.list_tables(DATASET_ID)
raw_tables = [t.table_id for t in tables if t.table_id.startswith("jse_raw_") and t.table_id != ANALYTICAL_TABLE]

if not raw_tables:
    print("No raw tables found!")
    exit(1)

# Get column names from the first table
first_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{raw_tables[0]}"
first_table = client.get_table(first_table_ref)
columns = [field.name for field in first_table.schema]

# Columns to cast
cast_columns = ["report_date", "extraction_timestamp"]

def build_select(table):
    select_cols = []
    table_ref = f"`{PROJECT_ID}.{DATASET_ID}.{table}`"
    bq_table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{table}")
    schema = {field.name: field.field_type for field in bq_table.schema}
    for col in columns:
        if col in cast_columns:
            select_cols.append(f"CAST({col} AS STRING) AS {col}")
        else:
            select_cols.append(col)
    return f"SELECT {', '.join(select_cols)} FROM {table_ref}"

# 2. Build the UNION ALL SQL (no need to add symbol column)
union_sql = "\nUNION ALL\n".join([build_select(table) for table in raw_tables])

full_sql = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{ANALYTICAL_TABLE}` AS
{union_sql}
"""

print("Running query to create analytical table...")
job = client.query(full_sql)
job.result()  # Wait for completion

print(f"Created table `{PROJECT_ID}.{DATASET_ID}.{ANALYTICAL_TABLE}` with data from {len(raw_tables)} tables.") 