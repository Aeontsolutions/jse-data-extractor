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
        bigquery.SchemaField("item", "FLOAT"),
        bigquery.SchemaField("standard_item", "STRING"),
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
    df['item'] = pd.to_numeric(df['item'], errors='coerce')
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

def concat_local_csvs(output_path="combined_multiyear_data.csv"):
    """Concatenate all local CSV files, deduplicate, and export as one CSV"""
    folder_path = "/Users/galbraithelroy/Documents/jse-data-extractor/csvs/multiyear_batch"
    
    # Verify folder exists
    if not os.path.exists(folder_path):
        print(f"Error: Folder not found at {folder_path}")
        return None
    
    # Get list of CSV files
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return None
    
    print(f"Found {len(csv_files)} CSV files to concatenate")
    
    all_dataframes = []
    
    for file in csv_files:
        csv_path = os.path.join(folder_path, file)
        print(f"Reading {file}...")
        
        try:
            df = pd.read_csv(csv_path)
            # Clean column names
            df.columns = df.columns.str.strip().str.lower()
            all_dataframes.append(df)
            print(f"  ✓ {len(df)} rows")
        except Exception as e:
            print(f"  ✗ Failed to read {file}: {str(e)}")
    
    if not all_dataframes:
        print("No valid CSV files to process")
        return None
    
    # Concatenate all DataFrames
    print(f"\nConcatenating {len(all_dataframes)} DataFrames...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    # Ensure 'year' is numeric for proper aggregation
    combined_df['year'] = pd.to_numeric(combined_df['year'], errors='coerce')
    initial_count = len(combined_df)
    print(f"Combined dataset: {initial_count} total rows")
    
    # Deduplicate based on key columns
    print("Deduplicating...")
    deduplicated_df = combined_df.drop_duplicates(
        subset=['company', 'symbol', 'year', 'item_name'],
        keep='last'  # Keep the last occurrence
    )
    final_count = len(deduplicated_df)
    
    print(f"Removed {initial_count - final_count} duplicate records")
    print(f"Final dataset: {final_count} unique records")
    
    # Sort the data for consistency
    deduplicated_df = deduplicated_df.sort_values(['company', 'symbol', 'year', 'item_name'])
    
    # Export to CSV
    deduplicated_df.to_csv(output_path, index=False)
    print(f"✓ Successfully exported to {output_path}")
    
    # Print summary statistics
    print(f"\n=== Combined CSV Summary ===")
    print(f"Companies: {deduplicated_df['company'].nunique()}")
    print(f"Symbols: {deduplicated_df['symbol'].nunique()}")
    print(f"Year range: {deduplicated_df['year'].min()} - {deduplicated_df['year'].max()}")
    print(f"Unique line items: {deduplicated_df['item_name'].nunique()}")
    print(f"Item types: {list(deduplicated_df['item_type'].unique())}")
    
    return output_path

def export_table_to_csv(output_path="multiyear_financial_data_export.csv"):
    """Export the BigQuery table to a deduplicated CSV file"""
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))
    
    # Define table reference
    dataset_id = "jse_raw_financial_data_dev_elroy"
    table_id = "multiyear_financial_data"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    print(f"Exporting data from {table_ref}...")
    
    # Query all data from the table
    query = f"""
    SELECT 
        company, symbol, year, item_name, item_type, item, 
        standard_item, unit_multiplier, confidence, drive_path,
        created_at, updated_at
    FROM `{table_ref}`
    ORDER BY company, symbol, year, item_name
    """
    
    try:
        # Execute query and convert to DataFrame
        df = client.query(query).to_dataframe()
        print(f"Retrieved {len(df)} rows from BigQuery")
        
        # Deduplicate based on the same key used for upserts
        initial_count = len(df)
        df_deduplicated = df.drop_duplicates(
            subset=['company', 'symbol', 'year', 'item_name'],
            keep='last'  # Keep the most recent record (latest updated_at)
        )
        final_count = len(df_deduplicated)
        
        print(f"Removed {initial_count - final_count} duplicate records")
        print(f"Final dataset: {final_count} unique records")
        
        # Export to CSV
        df_deduplicated.to_csv(output_path, index=False)
        print(f"✓ Successfully exported to {output_path}")
        
        # Print summary statistics
        print(f"\n=== Export Summary ===")
        print(f"Companies: {df_deduplicated['company'].nunique()}")
        print(f"Symbols: {df_deduplicated['symbol'].nunique()}")
        print(f"Year range: {df_deduplicated['year'].min()} - {df_deduplicated['year'].max()}")
        print(f"Unique line items: {df_deduplicated['item_name'].nunique()}")
        
        return output_path
        
    except Exception as e:
        print(f"✗ Error exporting data: {str(e)}")
        return None

def migrate_files():
    """Migrate CSV files to BigQuery"""
    folder_path = "/Users/galbraithelroy/Documents/jse-data-extractor/csvs/multiyear_batch"
    
    # Verify folder exists
    if not os.path.exists(folder_path):
        print(f"Error: Folder not found at {folder_path}")
        return
    
    # Get list of CSV files
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Create the target table once
    table_ref = create_bigquery_table()
    
    finished_files = []
    failed_files = []
    
    for file in csv_files:
        csv_path = os.path.join(folder_path, file)
        print(f"\nProcessing {file} ({len(finished_files) + 1}/{len(csv_files)})")
        
        try:
            # Prepare the data
            df = prepare_dataframe(csv_path)
            # Perform upsert operation
            upsert_to_bigquery(df, table_ref)
            finished_files.append(file)
            print(f"✓ Successfully processed {file}")
        except Exception as e:
            failed_files.append(file)
            print(f"✗ Failed to process {file}: {str(e)}")
    
    # Final summary
    print(f"\n=== Migration Summary ===")
    print(f"Total CSV files: {len(csv_files)}")
    print(f"Successfully processed: {len(finished_files)}")
    print(f"Failed: {len(failed_files)}")
    
    if failed_files:
        print(f"Failed files: {failed_files}")
    
    print("Migration completed!")

def main():
    """Main function - choose operation"""
    import sys
    
    if len(sys.argv) > 1:
        operation = sys.argv[1].lower()
        if operation == "concat":
            output_file = sys.argv[2] if len(sys.argv) > 2 else "combined_multiyear_data.csv"
            concat_local_csvs(output_file)
            return
        elif operation == "export":
            output_file = sys.argv[2] if len(sys.argv) > 2 else "multiyear_financial_data_export.csv"
            export_table_to_csv(output_file)
            return
        elif operation == "migrate":
            migrate_files()
            return
        else:
            print("Usage: python migrate_multiyear_batch.py [migrate|concat|export] [output_file]")
            return
    
    # Default: show options
    print("Choose operation:")
    print("1. Migrate CSV files to BigQuery")
    print("2. Concatenate local CSV files to one CSV")
    print("3. Export BigQuery table to CSV")
    choice = input("Enter choice (1, 2, or 3): ").strip()
    
    if choice == "1":
        migrate_files()
    elif choice == "2":
        output_file = input("Enter output filename (default: combined_multiyear_data.csv): ").strip()
        if not output_file:
            output_file = "combined_multiyear_data.csv"
        concat_local_csvs(output_file)
    elif choice == "3":
        output_file = input("Enter output filename (default: multiyear_financial_data_export.csv): ").strip()
        if not output_file:
            output_file = "multiyear_financial_data_export.csv"
        export_table_to_csv(output_file)
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main() 