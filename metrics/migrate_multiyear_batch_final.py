from google.cloud import bigquery
import pandas as pd
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Set up credentials
# Make sure you have set GOOGLE_APPLICATION_CREDENTIALS environment variable
# export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/galbraithelroy/Documents/jse-data-extractor/credentials.json"

def create_bigquery_table():
    """Create the BigQuery table for multiyear batch financial data"""
    # Initialize BigQuery client
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))

    # Define the dataset and table
    dataset_id = "jse_raw_financial_data_dev_elroy"  # Change this to your dataset ID
    table_id = "multiyear_financial_data"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    # Define the schema based on the CSV structure
    schema = [
        bigquery.SchemaField("company", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("item_name", "STRING"),
        bigquery.SchemaField("item_type", "STRING"),
        bigquery.SchemaField("standard_item", "STRING"),
        bigquery.SchemaField("item", "FLOAT"),
        bigquery.SchemaField("unit_multiplier", "INTEGER"),
        bigquery.SchemaField("confidence", "FLOAT"),
        bigquery.SchemaField("drive_path", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    # Create the table
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Created/verified table {table_ref}")

    return table_ref

def prepare_dataframe(csv_path):
    """Load and prepare the CSV data for BigQuery"""
    df = pd.read_csv(csv_path)
    
    # Clean column names
    df.columns = df.columns.str.strip().str.lower()
    
    # Convert data types
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['year'] = df['year'].fillna(0).astype(int)
    # df['item'] = pd.to_numeric(df['item_value'], errors='coerce')
    df['unit_multiplier'] = pd.to_numeric(df['unit_multiplier'], errors='coerce')
    df['unit_multiplier'] = df['unit_multiplier'].fillna(1).astype(int)
    df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')
    
    # Convert string columns to string type to avoid mixed type issues
    string_columns = ['company', 'symbol', 'item_name', 'item_type', 'standard_item', 'drive_path']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Add timestamps
    current_time = datetime.now()   
    df['created_at'] = current_time
    df['updated_at'] = current_time
    
    # Rename columns
    df = df.rename(columns={
        'item_name_standardized': 'item_name',
        'item_type_filled': 'item_type',
        'canonical_item_name': 'standard_item',
        'item_value': 'item',
    })
    
    # Deduplicate to avoid MERGE errors
    before = len(df)
    df = df.drop_duplicates(subset=['company', 'symbol', 'year', 'item_name'], keep='last')
    after = len(df)
    if before != after:
        print(f"Removed {before - after} duplicate rows based on (company, symbol, year, item_name)")

    print(f"Prepared {len(df)} rows for processing")
    return df

def upsert_to_bigquery(df, table_ref):
    """Perform upsert operation using BigQuery MERGE"""
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))
    
    # Create a temporary table with a unique name
    temp_table_id = f"temp_multiyear_batch_{uuid.uuid4().hex}"
    temp_table_ref = f"{client.project}.{table_ref.split('.')[1]}.{temp_table_id}"
    
    try:
        # Create temporary table schema
        temp_schema = [
            bigquery.SchemaField("company", "STRING"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("item_name", "STRING"),
            bigquery.SchemaField("item_type", "STRING"),
            bigquery.SchemaField("item", "FLOAT"),
            bigquery.SchemaField("standard_item", "STRING"),
            bigquery.SchemaField("unit_multiplier", "INTEGER"),
            bigquery.SchemaField("confidence", "FLOAT"),
            bigquery.SchemaField("drive_path", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        
        # Create temporary table
        temp_table = bigquery.Table(temp_table_ref, schema=temp_schema)
        temp_table = client.create_table(temp_table)
        print(f"Created temporary table {temp_table_ref}")
        
        # Load data into temporary table
        job_config = bigquery.LoadJobConfig(
            schema=temp_schema,
            write_disposition="WRITE_TRUNCATE",
        )
        
        job = client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config)
        job.result()
        print(f"Loaded {len(df)} rows into temporary table")
        
        # Perform MERGE operation
        merge_query = f"""
        MERGE `{table_ref}` AS target
        USING `{temp_table_ref}` AS source
        ON target.company = source.company 
           AND target.symbol = source.symbol 
           AND target.year = source.year 
           AND target.item_name = source.item_name
        WHEN MATCHED THEN
          UPDATE SET
            item_type = source.item_type,
            item = source.item,
            standard_item = source.standard_item,
            unit_multiplier = source.unit_multiplier,
            confidence = source.confidence,
            drive_path = source.drive_path,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN
          INSERT (company, symbol, year, item_name, item_type, item, standard_item, 
                  unit_multiplier, confidence, drive_path, created_at, updated_at)
          VALUES (source.company, source.symbol, source.year, source.item_name, 
                  source.item_type, source.item, source.standard_item, 
                  source.unit_multiplier, source.confidence, source.drive_path, 
                  source.created_at, source.updated_at)
        """
        
        merge_job = client.query(merge_query)
        merge_result = merge_job.result()
        
        # Get statistics about the merge operation
        stats_query = f"""
        SELECT 
          COUNT(*) as total_rows,
          COUNT(DISTINCT CONCAT(company, symbol, CAST(year AS STRING), item_name)) as unique_records
        FROM `{table_ref}`
        """
        
        stats_job = client.query(stats_query)
        stats_result = list(stats_job.result())
        
        print(f"MERGE operation completed successfully")
        print(f"Total rows in target table: {stats_result[0]['total_rows']}")
        print(f"Unique records in target table: {stats_result[0]['unique_records']}")
        
    finally:
        # Clean up temporary table
        try:
            client.delete_table(temp_table_ref)
            print(f"Cleaned up temporary table {temp_table_ref}")
        except Exception as e:
            print(f"Warning: Could not clean up temporary table: {e}")

def main():
    
    # Create the target table once
    print("Creating table...")
    table_ref = create_bigquery_table()
    
    # Prepare the data
    print("Preparing data...")
    df = prepare_dataframe("/Users/galbraithelroy/Documents/jse-data-extractor/csvs/multiyear_batch/cleaned_standardized_items.csv")
    # Perform upsert operation
    print("Upserting data...")
    upsert_to_bigquery(df, table_ref)

if __name__ == "__main__":
    main() 