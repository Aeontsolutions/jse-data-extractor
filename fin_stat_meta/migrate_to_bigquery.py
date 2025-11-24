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

def extract_quarter_from_period_detail(row):
    """
    Extract quarter information from period_detail based on statement_type.
    
    Rules:
    - If statement_type is 'audited', return 'FY' (Fiscal Year)
    - If statement_type is 'unaudited', extract Q1, Q2, Q3, or Q4 from period_detail
    
    Examples:
    - audited, "30-Sept-15" -> "FY"
    - unaudited, "Q1 (31-Dec-14)" -> "Q1"
    - unaudited, "Q2 (31-Mar-15)" -> "Q2"
    """
    statement_type = row.get('statement_type', '')
    period_detail = row.get('period_detail', '')
    
    # Handle missing or invalid values
    if pd.isna(statement_type) or pd.isna(period_detail):
        return None
    
    statement_type = str(statement_type).strip().lower()
    period_detail = str(period_detail).strip()
    
    # If audited, return FY
    if statement_type == 'audited':
        return 'FY'
    
    # If unaudited, extract quarter (Q1, Q2, Q3, Q4)
    if statement_type == 'unaudited':
        # Search for Q1, Q2, Q3, or Q4 in the period_detail
        quarter_match = re.search(r'Q([1-4])', period_detail, re.IGNORECASE)
        if quarter_match:
            return f'Q{quarter_match.group(1)}'
        else:
            print(f"Warning: Unaudited statement without quarter info in period_detail: '{period_detail}'")
            return None
    
    # For any other statement type, return None
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
    
    # Extract quarter information from period_detail based on statement_type
    print("Extracting quarter information from period_detail...")
    df['period_quarter'] = df.apply(extract_quarter_from_period_detail, axis=1)
    
    # Show statistics
    print(f"Records with period_quarter data: {df['period_quarter'].notna().sum()}")
    print(f"Records without period_quarter data: {df['period_quarter'].isna().sum()}")
    
    # Show value counts for period_quarter
    print("\nPeriod quarter distribution:")
    print(df['period_quarter'].value_counts(dropna=False))
    
    # Show sample extractions
    print("\nSample quarter extractions:")
    sample_data = df[['statement_type', 'period_detail', 'period_quarter']].head(10)
    print(sample_data.to_string(index=False))
    
    # Validation: Check business rules
    print("\n=== VALIDATION ===")
    
    # Rule 1: If statement_type is audited, period_quarter should be FY
    audited_df = df[df['statement_type'].str.lower() == 'audited']
    audited_with_fy = audited_df[audited_df['period_quarter'] == 'FY']
    audited_without_fy = audited_df[audited_df['period_quarter'] != 'FY']
    
    print(f"\nAudited statements: {len(audited_df)}")
    print(f"  - With period_quarter='FY': {len(audited_with_fy)}")
    print(f"  - Without period_quarter='FY': {len(audited_without_fy)}")
    
    if len(audited_without_fy) > 0:
        print("  WARNING: Some audited statements don't have period_quarter='FY':")
        print(audited_without_fy[['statement_type', 'period_detail', 'period_quarter']].head())
    
    # Rule 2: If statement_type is unaudited, period_detail should have Q1, Q2, Q3, or Q4
    unaudited_df = df[df['statement_type'].str.lower() == 'unaudited']
    unaudited_with_quarter = unaudited_df[unaudited_df['period_quarter'].isin(['Q1', 'Q2', 'Q3', 'Q4'])]
    unaudited_without_quarter = unaudited_df[~unaudited_df['period_quarter'].isin(['Q1', 'Q2', 'Q3', 'Q4'])]
    
    print(f"\nUnaudited statements: {len(unaudited_df)}")
    print(f"  - With period_quarter (Q1-Q4): {len(unaudited_with_quarter)}")
    print(f"  - Without period_quarter (Q1-Q4): {len(unaudited_without_quarter)}")
    
    if len(unaudited_without_quarter) > 0:
        print("  WARNING: Some unaudited statements don't have a valid quarter (Q1-Q4):")
        print(unaudited_without_quarter[['statement_type', 'period_detail', 'period_quarter']].head())
    
    # Rule 3: If statement_type is audited, period_detail should NOT contain Q1, Q2, Q3, Q4
    audited_with_q_in_detail = audited_df[audited_df['period_detail'].str.contains(r'Q[1-4]', na=False, regex=True)]
    
    print(f"\nAudited statements with Q1-Q4 in period_detail: {len(audited_with_q_in_detail)}")
    
    if len(audited_with_q_in_detail) > 0:
        print("  WARNING: Some audited statements have Q1-Q4 in period_detail:")
        print(audited_with_q_in_detail[['statement_type', 'period_detail', 'period_quarter']].head())
    
    print("\n=== END VALIDATION ===\n")
    
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