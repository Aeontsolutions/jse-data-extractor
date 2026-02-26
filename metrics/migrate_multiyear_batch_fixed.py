"""
BigQuery Migration Script - Fixed Version
Migrates cleaned and standardized financial data to BigQuery
Uses correct column names from the cleaned dataset
"""

from google.cloud import bigquery
import pandas as pd
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Required keys that must exist in the data (from Overview.svelte)
REQUIRED_KEYS = [
    'revenue', 'gross_profit', 'operating_profit', 'net_profit',
    'eps', 'total_assets', 'total_equity', 'debt_to_equity_ratio',
    'roa', 'roe', 'current_ratio',
    'gross_margin', 'ebitda_margin', 'operating_margin', 'net_margin'
]


def validate_csv_before_upload(csv_path):
    """Validate the CSV has all required keys before uploading."""
    print("\n" + "="*60)
    print("PRE-MIGRATION VALIDATION")
    print("="*60)
    
    df = pd.read_csv(csv_path)
    
    # Check required columns exist
    required_columns = [
        'company', 'symbol', 'year', 'item_name_standardized',
        'item_type_filled', 'canonical_item_name', 'item_value'
    ]
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        print(f"❌ ERROR: Missing required columns: {missing_columns}")
        return False
    
    # Check for required keys
    available_keys = set(df['canonical_item_name'].dropna().unique())
    missing_keys = set(REQUIRED_KEYS) - available_keys
    
    print(f"\nRequired frontend keys: {len(REQUIRED_KEYS)}")
    print(f"Available in data: {len(available_keys & set(REQUIRED_KEYS))}")
    
    if missing_keys:
        print(f"\n⚠️  WARNING: Missing {len(missing_keys)} required keys:")
        for key in sorted(missing_keys):
            print(f"  - {key}")
        print("\nFrontend will show 'undefined' for these fields!")
        
        response = input("\nContinue with migration anyway? (yes/no): ")
        if response.lower() != 'yes':
            print("Migration cancelled.")
            return False
    else:
        print("\n✅ All 15 required keys present!")
    
    # Show data summary
    print(f"\nData summary:")
    print(f"  Total records: {len(df):,}")
    print(f"  Companies: {df['company'].nunique()}")
    print(f"  Symbols: {df['symbol'].nunique()}")
    print(f"  Years: {sorted(df['year'].unique())}")
    print(f"  Unique canonical items: {df['canonical_item_name'].nunique()}")
    
    return True


def create_bigquery_table():
    """Create the BigQuery table for multiyear batch financial data."""
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))

    dataset_id = "jse_raw_financial_data_dev_elroy"
    table_id = "multiyear_financial_data"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    # Updated schema to match cleaned data structure
    schema = [
        bigquery.SchemaField("company", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("item_name", "STRING", mode="REQUIRED", 
                           description="Standardized snake_case item name"),
        bigquery.SchemaField("item_type", "STRING", mode="REQUIRED",
                           description="Either 'line_item' or 'ratio'"),
        bigquery.SchemaField("standard_item", "STRING", mode="REQUIRED",
                           description="Canonical item name for frontend (e.g., 'eps', 'roa', 'gross_margin')"),
        bigquery.SchemaField("item", "FLOAT", mode="REQUIRED",
                           description="The numeric value of the financial metric"),
        bigquery.SchemaField("unit_multiplier", "INTEGER",
                           description="Multiplier for the item value (e.g., 1000 for thousands)"),
        bigquery.SchemaField("confidence", "FLOAT"),
        bigquery.SchemaField("drive_path", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]

    # Create the table
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"\n✓ Created/verified table: {table_ref}")

    return table_ref


def prepare_dataframe(csv_path):
    """Load and prepare the CSV data for BigQuery."""
    print("\nPreparing data for upload...")
    df = pd.read_csv(csv_path)
    
    # Expected columns from clean_and_standardize_items.py
    expected_columns = {
        'company', 'symbol', 'year', 'item_name_standardized',
        'item_type_filled', 'canonical_item_name', 'item_value',
        'unit_multiplier', 'confidence', 'drive_path'
    }
    
    actual_columns = set(df.columns)
    if not expected_columns.issubset(actual_columns):
        missing = expected_columns - actual_columns
        raise ValueError(f"CSV missing required columns: {missing}")
    
    # Data type conversions BEFORE renaming
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['year'] = df['year'].fillna(0).astype(int)
    
    df['item_value'] = pd.to_numeric(df['item_value'], errors='coerce')
    
    # Convert unit_multiplier to integer to match BigQuery table schema
    df['unit_multiplier'] = pd.to_numeric(df['unit_multiplier'], errors='coerce')
    df['unit_multiplier'] = df['unit_multiplier'].fillna(1).astype(int)
    
    # Rename columns to match BigQuery schema AFTER conversions
    df = df.rename(columns={
        'item_name_standardized': 'item_name',
        'item_type_filled': 'item_type',
        'canonical_item_name': 'standard_item',
        'item_value': 'item',  # Rename to match BigQuery schema
    })
    
    df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')
    
    # Convert string columns
    string_columns = ['company', 'symbol', 'item_name', 'item_type', 'standard_item', 'drive_path']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)
    
    # Add timestamps
    current_time = datetime.now()
    df['created_at'] = current_time
    df['updated_at'] = current_time
    
    # Remove rows with missing required fields (use 'item' since we renamed it)
    required_fields = ['company', 'symbol', 'year', 'item_name', 'item_type', 'standard_item', 'item']
    before = len(df)
    for field in required_fields:
        df = df[df[field].notna()]
        if df[field].dtype == 'object':  # Only check for empty strings on string columns
            df = df[df[field] != '']
    after = len(df)
    
    if before != after:
        print(f"  Removed {before - after} rows with missing required fields")
    
    # Calculate ebitda_margin (ebitda / revenue) and add as new rows
    print("\n  Calculating ebitda_margin...")
    pivot = df[df['standard_item'].isin(['ebitda', 'revenue'])].pivot_table(
        index=['company', 'symbol', 'year'],
        columns='standard_item',
        values='item',
        aggfunc='first'
    ).reset_index()
    
    if 'ebitda' in pivot.columns and 'revenue' in pivot.columns:
        pivot = pivot.dropna(subset=['ebitda', 'revenue'])
        pivot = pivot[pivot['revenue'] != 0]
        
        ebitda_margin_rows = pd.DataFrame({
            'company': pivot['company'],
            'symbol': pivot['symbol'],
            'year': pivot['year'],
            'item_name': 'ebitda_margin',
            'item_type': 'ratio',
            'standard_item': 'ebitda_margin',
            'item': pivot['ebitda'] / pivot['revenue'],
            'unit_multiplier': 1,
            'confidence': None,
            'drive_path': '',
            'created_at': current_time,
            'updated_at': current_time
        })
        
        df = pd.concat([df, ebitda_margin_rows], ignore_index=True)
        print(f"  ✓ Added {len(ebitda_margin_rows)} ebitda_margin rows")
    else:
        print(f"  ⚠️  Warning: Could not calculate ebitda_margin (missing ebitda or revenue data)")
    
    # Calculate operating_margin (operating_profit / revenue) and add as new rows
    print("  Calculating operating_margin...")
    pivot = df[df['standard_item'].isin(['operating_profit', 'revenue'])].pivot_table(
        index=['company', 'symbol', 'year'],
        columns='standard_item',
        values='item',
        aggfunc='first'
    ).reset_index()
    
    if 'operating_profit' in pivot.columns and 'revenue' in pivot.columns:
        pivot = pivot.dropna(subset=['operating_profit', 'revenue'])
        pivot = pivot[pivot['revenue'] != 0]
        
        operating_margin_rows = pd.DataFrame({
            'company': pivot['company'],
            'symbol': pivot['symbol'],
            'year': pivot['year'],
            'item_name': 'operating_margin',
            'item_type': 'ratio',
            'standard_item': 'operating_margin',
            'item': pivot['operating_profit'] / pivot['revenue'],
            'unit_multiplier': 1,
            'confidence': None,
            'drive_path': '',
            'created_at': current_time,
            'updated_at': current_time
        })
        
        df = pd.concat([df, operating_margin_rows], ignore_index=True)
        print(f"  ✓ Added {len(operating_margin_rows)} operating_margin rows")
    else:
        print(f"  ⚠️  Warning: Could not calculate operating_margin (missing operating_profit or revenue data)")
    
    # Calculate roa (net_profit / total_assets) and add as new rows
    print("  Calculating roa...")
    pivot = df[df['standard_item'].isin(['net_profit', 'total_assets'])].pivot_table(
        index=['company', 'symbol', 'year'],
        columns='standard_item',
        values='item',
        aggfunc='first'
    ).reset_index()
    
    if 'net_profit' in pivot.columns and 'total_assets' in pivot.columns:
        pivot = pivot.dropna(subset=['net_profit', 'total_assets'])
        pivot = pivot[pivot['total_assets'] != 0]
        
        roa_rows = pd.DataFrame({
            'company': pivot['company'],
            'symbol': pivot['symbol'],
            'year': pivot['year'],
            'item_name': 'roa',
            'item_type': 'ratio',
            'standard_item': 'roa',
            'item': pivot['net_profit'] / pivot['total_assets'],
            'unit_multiplier': 1,
            'confidence': None,
            'drive_path': '',
            'created_at': current_time,
            'updated_at': current_time
        })
        
        df = pd.concat([df, roa_rows], ignore_index=True)
        print(f"  ✓ Added {len(roa_rows)} roa rows")
    else:
        print(f"  ⚠️  Warning: Could not calculate roa (missing net_profit or total_assets data)")
    
    # Calculate roe (net_profit / total_equity) and add as new rows
    print("  Calculating roe...")
    pivot = df[df['standard_item'].isin(['net_profit', 'total_equity'])].pivot_table(
        index=['company', 'symbol', 'year'],
        columns='standard_item',
        values='item',
        aggfunc='first'
    ).reset_index()
    
    if 'net_profit' in pivot.columns and 'total_equity' in pivot.columns:
        pivot = pivot.dropna(subset=['net_profit', 'total_equity'])
        pivot = pivot[pivot['total_equity'] != 0]
        
        roe_rows = pd.DataFrame({
            'company': pivot['company'],
            'symbol': pivot['symbol'],
            'year': pivot['year'],
            'item_name': 'roe',
            'item_type': 'ratio',
            'standard_item': 'roe',
            'item': pivot['net_profit'] / pivot['total_equity'],
            'unit_multiplier': 1,
            'confidence': None,
            'drive_path': '',
            'created_at': current_time,
            'updated_at': current_time
        })
        
        df = pd.concat([df, roe_rows], ignore_index=True)
        print(f"  ✓ Added {len(roe_rows)} roe rows")
    else:
        print(f"  ⚠️  Warning: Could not calculate roe (missing net_profit or total_equity data)")
    
    # Deduplicate after all calculations
    # Sort by confidence (nulls last) to prefer high-confidence rows, then drop duplicates
    # Use standard_item as dedup key since that's what's used in production queries
    print("\n  Deduplicating final dataset...")
    before = len(df)
    
    # Sort by confidence descending (NaN values will be last)
    df = df.sort_values('confidence', ascending=False, na_position='last')
    
    # Drop duplicates on the key used in production: symbol, year, standard_item
    # Keep first (highest confidence after sorting)
    df = df.drop_duplicates(subset=['symbol', 'year', 'standard_item'], keep='first')
    after = len(df)
    
    if before != after:
        print(f"  ✓ Removed {before - after} duplicate rows (kept highest confidence)")
    
    # Select only columns needed for BigQuery
    final_columns = [
        'company', 'symbol', 'year', 'item_name', 'item_type',
        'standard_item', 'item', 'unit_multiplier', 'confidence',
        'drive_path', 'created_at', 'updated_at'
    ]
    df = df[final_columns]
    
    print(f"  ✓ Prepared {len(df):,} rows for upload")
    
    return df


def upsert_to_bigquery(df, table_ref):
    """Perform upsert operation using BigQuery MERGE."""
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))
    
    # Create a temporary table with a unique name
    temp_table_id = f"temp_multiyear_batch_{uuid.uuid4().hex}"
    dataset_id = table_ref.split('.')[1]
    temp_table_ref = f"{client.project}.{dataset_id}.{temp_table_id}"
    
    print(f"\nUploading to BigQuery...")
    
    try:
        # Create temporary table schema (matches main table)
        temp_schema = [
            bigquery.SchemaField("company", "STRING"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("item_name", "STRING"),
            bigquery.SchemaField("item_type", "STRING"),
            bigquery.SchemaField("standard_item", "STRING"),
            bigquery.SchemaField("item", "FLOAT"),
            bigquery.SchemaField("unit_multiplier", "INTEGER"),  # Match existing table schema
            bigquery.SchemaField("confidence", "FLOAT"),
            bigquery.SchemaField("drive_path", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        
        # Create temporary table
        temp_table = bigquery.Table(temp_table_ref, schema=temp_schema)
        temp_table = client.create_table(temp_table)
        print(f"  ✓ Created temporary table: {temp_table_id}")
        
        # Load data into temporary table
        job_config = bigquery.LoadJobConfig(
            schema=temp_schema,
            write_disposition="WRITE_TRUNCATE",
        )
        
        job = client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config)
        job.result()
        print(f"  ✓ Loaded {len(df):,} rows into temporary table")
        
        # Perform MERGE operation
        print("  Running MERGE operation...")
        merge_query = f"""
        MERGE `{table_ref}` AS target
        USING `{temp_table_ref}` AS source
        ON target.symbol = source.symbol 
           AND target.year = source.year 
           AND target.standard_item = source.standard_item
        WHEN MATCHED THEN
          UPDATE SET
            company = source.company,
            item_name = source.item_name,
            item_type = source.item_type,
            item = source.item,
            unit_multiplier = source.unit_multiplier,
            confidence = source.confidence,
            drive_path = source.drive_path,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN
          INSERT (company, symbol, year, item_name, item_type, standard_item, 
                  item, unit_multiplier, confidence, drive_path, created_at, updated_at)
          VALUES (source.company, source.symbol, source.year, source.item_name, 
                  source.item_type, source.standard_item, source.item,
                  source.unit_multiplier, source.confidence, source.drive_path, 
                  source.created_at, source.updated_at)
        """
        
        merge_job = client.query(merge_query)
        merge_job.result()
        print(f"  ✓ MERGE operation completed")
        
        # Get statistics
        stats_query = f"""
        SELECT 
          COUNT(*) as total_rows,
          COUNT(DISTINCT CONCAT(symbol, CAST(year AS STRING), standard_item)) as unique_records,
          COUNT(DISTINCT symbol) as unique_symbols,
          COUNT(DISTINCT standard_item) as unique_items
        FROM `{table_ref}`
        """
        
        stats_job = client.query(stats_query)
        stats_result = list(stats_job.result())
        
        print("\n" + "="*60)
        print("MIGRATION COMPLETE")
        print("="*60)
        print(f"Total rows in table: {stats_result[0]['total_rows']:,}")
        print(f"Unique records: {stats_result[0]['unique_records']:,}")
        print(f"Unique symbols: {stats_result[0]['unique_symbols']}")
        print(f"Unique canonical items: {stats_result[0]['unique_items']}")
        
        # Verify required keys are in BigQuery
        print("\nVerifying required keys in BigQuery...")
        verify_query = f"""
        SELECT DISTINCT standard_item
        FROM `{table_ref}`
        WHERE standard_item IN UNNEST(@required_keys)
        ORDER BY standard_item
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("required_keys", "STRING", REQUIRED_KEYS)
            ]
        )
        
        verify_job = client.query(verify_query, job_config=job_config)
        found_keys = [row['standard_item'] for row in verify_job.result()]
        missing_keys = set(REQUIRED_KEYS) - set(found_keys)
        
        print(f"  Found {len(found_keys)}/15 required keys")
        if missing_keys:
            print(f"  ⚠️  Missing keys: {sorted(missing_keys)}")
        else:
            print(f"  ✅ All required keys present!")
        
    finally:
        # Clean up temporary table
        try:
            client.delete_table(temp_table_ref)
            print(f"\n✓ Cleaned up temporary table")
        except Exception as e:
            print(f"\n⚠️  Warning: Could not clean up temporary table: {e}")


def main():
    """Main execution function."""
    print("="*60)
    print("BigQuery Migration - Multiyear Financial Data")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Path to cleaned CSV
    csv_path = "/Users/galbraithelroy/Documents/jse-data-extractor/metrics/cleaned_standardized_items_fixed.csv"
    
    # Check if file exists
    if not os.path.exists(csv_path):
        print(f"❌ ERROR: File not found: {csv_path}")
        print("\nPlease run clean_and_standardize_items.py first!")
        return
    
    # Step 1: Validate CSV
    if not validate_csv_before_upload(csv_path):
        return
    
    # Step 2: Create table
    print("\n" + "-"*60)
    print("Creating BigQuery table...")
    print("-"*60)
    table_ref = create_bigquery_table()
    
    # Step 3: Prepare data
    print("\n" + "-"*60)
    print("Preparing data...")
    print("-"*60)
    df = prepare_dataframe(csv_path)
    
    # Step 4: Upsert to BigQuery
    print("\n" + "-"*60)
    print("Uploading to BigQuery...")
    print("-"*60)
    upsert_to_bigquery(df, table_ref)
    
    print("\n" + "="*60)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


if __name__ == "__main__":
    main()

