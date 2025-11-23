from google.cloud import bigquery
import pandas as pd
import os
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


# Set up credentials
# Make sure you have set GOOGLE_APPLICATION_CREDENTIALS environment variable
# export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"

def extract_date_from_period_detail(period_detail):
    """
    Extract and convert dates from period_detail field to ISO format.
    
    Examples:
    - "Q1 (31-Dec-14)" -> "2014-12-31"
    - "30-Sept-15" -> "2015-09-30"
    - "Q4 (30-Sept-16)" -> "2016-09-30"
    """
    if pd.isna(period_detail) or period_detail == 'nan':
        return None
    
    period_detail = str(period_detail).strip()
    
    # First, try to extract date from parentheses (e.g., "Q1 (31-Dec-14)")
    parentheses_match = re.search(r'\(([^)]+)\)', period_detail)
    if parentheses_match:
        date_str = parentheses_match.group(1)
    else:
        # If no parentheses, use the whole string (e.g., "30-Sept-15")
        date_str = period_detail
    
    # Clean up the date string
    date_str = date_str.strip()
    
    # Handle empty or invalid dates
    if date_str == '-' or date_str == '' or len(date_str) < 3:
        return None
    
    # Normalize month abbreviations that don't match Python's standard
    month_replacements = {
        'Sept': 'Sep',  # September
        'June': 'Jun',  # June (sometimes written as June instead of Jun)
        'July': 'Jul',  # July (sometimes written as July instead of Jul)
    }
    
    for old_month, new_month in month_replacements.items():
        date_str = date_str.replace(old_month, new_month)
    
    # Try to parse different date formats
    date_formats = [
        '%d-%b-%y',  # 31-Dec-14, 30-Sep-15
        '%d-%B-%y',  # 31-December-14
        '%d-%b-%Y',  # 31-Dec-2014
        '%d-%B-%Y',  # 31-December-2014
        '%b-%d-%y',  # Dec-31-14
        '%B-%d-%y',  # December-31-14
        '%b-%d-%Y',  # Dec-31-2014
        '%B-%d-%Y',  # December-31-2014
        '%Y-%m-%d',  # 2014-12-31
        '%d/%m/%y',  # 31/12/14
        '%d/%m/%Y',  # 31/12/2014
        '%m/%d/%y',  # 12/31/14
        '%m/%d/%Y',  # 12/31/2014
    ]
    
    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, date_format)
            # Convert to ISO format (YYYY-MM-DD)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If none of the formats work, return None
    print(f"Warning: Could not parse date from '{period_detail}' (extracted: '{date_str}')")
    return None

def create_bigquery_table():
    # Initialize BigQuery client
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))

    # Define the dataset and table
    dataset_id = "jse_raw_financial_data_dev_elroy"  # Change this to your dataset ID
    table_id = "financial_statements_metadata"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    # Define the schema
    # symbol,statement_type,period,period_detail,report_type,consolidation_type,status,s3_path,pdf_folder_path,period_quarter
    schema = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("statement_type", "STRING"),
        bigquery.SchemaField("period", "STRING"),
        bigquery.SchemaField("period_detail", "STRING"),
        bigquery.SchemaField("period_end_date", "DATE"),
        bigquery.SchemaField("period_quarter", "STRING"),  # New field from lu_period_mapping
        bigquery.SchemaField("report_type", "STRING"),
        bigquery.SchemaField("consolidation_type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("s3_path", "STRING"),
        bigquery.SchemaField("pdf_folder_path", "STRING"),
    ]

    # Create the table
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table_ref}")

    return table_ref

def load_csv_to_bigquery(csv_path, table_ref):
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    print(f"Original data shape: {df.shape}")
    
    # Perform join with lu_period_mapping to get period_quarter data
    dataset_id = "jse_raw_financial_data_dev_elroy"
    period_mapping_table = f"{client.project}.{dataset_id}.lu_period_mapping"
    
    print("Fetching period mapping data from BigQuery...")
    period_mapping_query = f"""
    SELECT 
        symbol,
        period as period_quarter,
        PARSE_DATE('%Y-%m-%d', report_date) as report_date,
        PARSE_DATE('%Y-%m-%d', year_end) as year_end
    FROM `{period_mapping_table}`
    """
    
    try:
        period_df = client.query(period_mapping_query).to_dataframe()
        print(f"Period mapping data shape: {period_df.shape}")
        
        # Debug: Show sample data from both dataframes
        print("\nSample from main dataframe:")
        print(df[['symbol', 'period_end_date']].head())
        print(f"period_end_date dtype: {df['period_end_date'].dtype}")
        
        print("\nSample from period mapping dataframe:")
        print(period_df[['symbol', 'report_date']].head())
        print(f"report_date dtype: {period_df['report_date'].dtype}")
        
        # Ensure both date columns are datetime objects for proper joining
        df['period_end_date'] = pd.to_datetime(df['period_end_date'], errors='coerce')
        period_df['report_date'] = pd.to_datetime(period_df['report_date'], errors='coerce')
        
        print(f"\nAfter conversion - period_end_date dtype: {df['period_end_date'].dtype}")
        print(f"After conversion - report_date dtype: {period_df['report_date'].dtype}")
        
        # Prepare the main dataframe for joining
        # We'll join on symbol and period_end_date (from main df) = report_date (from period mapping)
        print("Performing left join with period mapping...")
        df_with_period = df.merge(
            period_df[['symbol', 'period_quarter', 'report_date']], 
            left_on=['symbol', 'period_end_date'], 
            right_on=['symbol', 'report_date'], 
            how='left'
        )
        
        # Drop the duplicate report_date column from the join
        if 'report_date' in df_with_period.columns:
            df_with_period = df_with_period.drop(columns=['report_date'])
            
        print(f"After join data shape: {df_with_period.shape}")
        print(f"Records with period_quarter data: {df_with_period['period_quarter'].notna().sum()}")
        print(f"Records without period_quarter data: {df_with_period['period_quarter'].isna().sum()}")
        
        # Update df to use the joined version
        df = df_with_period
        
    except Exception as e:
        print(f"Warning: Could not join with period mapping data: {e}")
        print("Proceeding without period_quarter data...")
        # Add empty period_quarter column if join fails
        df['period_quarter'] = None
    
    # Rename columns to match schema
    df.rename(columns={
        'symbol': 'symbol',
        'statement_type': 'statement_type',
        'period': 'period',
        'period_detail': 'period_detail',
        'period_end_date': 'period_end_date',
        'report_type': 'report_type',
        'consolidation_type': 'consolidation_type',
        'status': 'status',
        's3_path': 's3_path',
        'pdf_folder_path': 'pdf_folder_path'
    }, inplace=True)
    
    # Deduplicate records: keep unique records, prefer status=1 over blank for duplicates
    print(f"Original records: {len(df)}")
    
    # Define grouping columns for duplicate detection
    grouping_cols = ['symbol', 'period_end_date', 'report_type', 'consolidation_type']
    
    # Sort by grouping columns and status (putting status=1 first, blank/NaN last)
    df_deduplicated = (df
        .sort_values(grouping_cols + ['status'], na_position='last')
        .drop_duplicates(subset=grouping_cols, keep='first')
    )
    
    print(f"After deduplication: {len(df_deduplicated)}")
    print(f"Removed {len(df) - len(df_deduplicated)} duplicate records")
    
    # Update df to use the deduplicated version
    df = df_deduplicated
    
    # Convert all string columns to string type to avoid mixed type issues
    string_columns = ['symbol', 'statement_type', 'period', 'period_detail', 'report_type', 'consolidation_type', 'status', 's3_path', 'pdf_folder_path']
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # NEW: Derive period_end_date from a 'date' column when present
    if 'date' in df.columns:
        print("Converting 'date' column to period_end_date in ISO format...")
        df['period_end_date'] = pd.to_datetime(df['date'], errors='coerce')
        sample_date = df[['date', 'period_end_date']].dropna().head(5)
        print("Sample date conversions:")
        print(sample_date.to_string(index=False))
        # Drop the original 'date' column so it doesn't cause schema mismatch
        df.drop(columns=['date'], inplace=True)
    elif 'period_detail' in df.columns:
        # Extract period_end_date from period_detail field
        print("Extracting dates from period_detail field...")
        df['period_end_date'] = df['period_detail'].apply(extract_date_from_period_detail)
        # Convert to datetime for BigQuery
        df['period_end_date'] = pd.to_datetime(df['period_end_date'], errors='coerce')
        
        # Show some examples of the extraction
        sample_data = df[['period_detail', 'period_end_date']].dropna().head(5)
        print("Sample date extractions:")
        print(sample_data.to_string(index=False))
    else:
        print("Warning: neither 'date' nor 'period_detail' columns found in CSV; setting period_end_date to None")
        df['period_end_date'] = None

    # NEW: keep only the columns that exist in the target schema to avoid schema mismatch errors
    schema_cols = [
        "symbol",
        "statement_type",
        "period",
        "period_detail",
        "period_end_date",
        "period_quarter",  # New field from lu_period_mapping join
        "report_type",
        "consolidation_type",
        "status",
        "s3_path",
        "pdf_folder_path",
    ]
    # Reindex will drop any extra columns (e.g., unintended 'date') and add missing ones with NaN
    df = df.reindex(columns=schema_cols)

    # Use the correct schema that matches the table definition
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Ensure overwrite
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("statement_type", "STRING"),
            bigquery.SchemaField("period", "STRING"),
            bigquery.SchemaField("period_detail", "STRING"),
            bigquery.SchemaField("period_end_date", "DATE"),
            bigquery.SchemaField("period_quarter", "STRING"),  # New field from lu_period_mapping join
            bigquery.SchemaField("report_type", "STRING"),
            bigquery.SchemaField("consolidation_type", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("s3_path", "STRING"),
            bigquery.SchemaField("pdf_folder_path", "STRING"),
        ]
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_ref}")
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_ref}")



def main():
    csv_path = "/Users/galbraithelroy/Documents/jse-data-extractor/fin_stat_meta/financial_statements_metadata_3_09_2025_with_ID - financial_statements_metadata_3_09_2025 - financial_statements_metadata_3_09_2025 (1).csv"
    
    # Create the table
    table_ref = create_bigquery_table()

    # Load the CSV data
    load_csv_to_bigquery(csv_path, table_ref)

if __name__ == "__main__":
    main() 