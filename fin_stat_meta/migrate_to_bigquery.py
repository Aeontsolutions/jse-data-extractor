from google.cloud import bigquery
import pandas as pd
import os
import re
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()


# Set up credentials
# Make sure you have set GOOGLE_APPLICATION_CREDENTIALS environment variable
# export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"

def fetch_csv_from_google_sheets(sheet_url, gid=None):
    """
    Fetch CSV data directly from a Google Sheets URL using Google Cloud credentials.

    Args:
        sheet_url: The Google Sheets URL (can be edit or view URL)
        gid: The sheet ID (gid parameter). If None, will try to extract from URL or use first sheet

    Returns:
        pandas DataFrame with the sheet data

    Note:
        Uses the same Google Cloud credentials as BigQuery (GOOGLE_APPLICATION_CREDENTIALS)
    """
    # Extract the spreadsheet ID from the URL
    if '/d/' in sheet_url:
        sheet_id = sheet_url.split('/d/')[1].split('/')[0]
    else:
        raise ValueError("Invalid Google Sheets URL format")

    # Extract gid from URL if not provided
    sheet_name = None
    if gid is None:
        if 'gid=' in sheet_url:
            gid = sheet_url.split('gid=')[1].split('&')[0].split('#')[0]
        else:
            gid = '0'  # Default to first sheet

    print(f"Fetching data from Google Sheets...")
    print(f"Sheet ID: {sheet_id}")
    print(f"GID: {gid}")

    try:
        # Set up credentials using the same credentials as BigQuery
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not credentials_path:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")

        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )

        # Build the Sheets API service
        service = build('sheets', 'v4', credentials=credentials)

        # Get spreadsheet metadata to find sheet name from gid
        spreadsheet = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheets = spreadsheet.get('sheets', [])

        # Find the sheet with matching gid
        target_sheet = None
        for sheet in sheets:
            sheet_properties = sheet.get('properties', {})
            if str(sheet_properties.get('sheetId')) == str(gid):
                target_sheet = sheet_properties.get('title')
                break

        if not target_sheet:
            # If gid not found, use the first sheet
            target_sheet = sheets[0]['properties']['title'] if sheets else 'Sheet1'
            print(f"Warning: Sheet with gid={gid} not found, using first sheet: {target_sheet}")

        print(f"Sheet name: {target_sheet}")

        # Read the data
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=target_sheet
        ).execute()

        values = result.get('values', [])

        if not values:
            print("Warning: No data found in sheet")
            return pd.DataFrame()

        # Get header row
        headers = values[0]
        num_columns = len(headers)

        # Normalize data rows to have same number of columns as header
        # (Google Sheets API omits trailing empty cells)
        normalized_rows = []
        for row in values[1:]:
            # Pad row with empty strings if it has fewer columns than header
            if len(row) < num_columns:
                row = row + [''] * (num_columns - len(row))
            # Truncate row if it has more columns than header (shouldn't happen, but just in case)
            elif len(row) > num_columns:
                row = row[:num_columns]
            normalized_rows.append(row)

        # Convert to DataFrame
        df = pd.DataFrame(normalized_rows, columns=headers)

        print(f"✓ Successfully fetched {len(df)} rows from Google Sheets")
        return df

    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        raise

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


def build_fiscal_year_lookup(df):
    """
    Build a time-aware fiscal year lookup table from audited statements.

    For each company, creates date ranges where:
    - Start Range = Previous Audited Date + 1 day (or min date for first year)
    - End Range = Current Audited Date (the fiscal year end)

    A statement belongs to a fiscal year if:
    reference_date > start_range AND reference_date <= end_range

    Args:
        df: DataFrame with 'symbol', 'statement_type', and 'reference_date' columns

    Returns:
        DataFrame with columns: symbol, fiscal_year, start_range, end_range
    """
    # Filter to audited statements only
    audited_df = df[df['statement_type'].str.lower() == 'audited'].copy()

    # Get unique audited dates per symbol (deduplicate by symbol + reference_date)
    fy_dates = audited_df.groupby(['symbol', 'reference_date']).size().reset_index()[['symbol', 'reference_date']]
    fy_dates = fy_dates.sort_values(['symbol', 'reference_date']).reset_index(drop=True)

    # For each symbol, calculate the start range using lag (previous audited date + 1 day)
    fy_dates['prev_audited_date'] = fy_dates.groupby('symbol')['reference_date'].shift(1)

    # Start range is previous audited date + 1 day
    # For the first year of each company, use a very early date
    fy_dates['start_range'] = fy_dates['prev_audited_date'] + pd.Timedelta(days=1)

    # Fill NaT (first year for each company) with a default early date
    min_date = pd.Timestamp('1900-01-01')
    fy_dates['start_range'] = fy_dates['start_range'].fillna(min_date)

    # End range is the audited date itself (this becomes period_end_date for backward compatibility)
    fy_dates['end_range'] = fy_dates['reference_date']

    # Fiscal year is the calendar year of the audited date
    fy_dates['fiscal_year'] = fy_dates['reference_date'].dt.year

    # Select final columns
    lookup_table = fy_dates[['symbol', 'fiscal_year', 'start_range', 'end_range']].copy()

    return lookup_table


def assign_fiscal_year(df, lookup_table):
    """
    Assign fiscal year and fiscal year end date to each record based on the lookup table.

    A record belongs to fiscal year X if:
    reference_date > start_range AND reference_date <= end_range

    For each matching record, assigns:
    - fiscal_year: The calendar year of the fiscal year end (INTEGER)
    - period_end_date: The actual fiscal year end date (DATE) - only if not already set

    Args:
        df: DataFrame with all records (must have 'symbol' and 'reference_date')
        lookup_table: DataFrame from build_fiscal_year_lookup()

    Returns:
        DataFrame with 'fiscal_year' and 'period_end_date' columns added
    """
    df = df.copy()

    # Initialize fiscal_year column
    df['fiscal_year'] = None

    # Initialize period_end_date only if it doesn't exist or is all null
    # This preserves existing period_end_date values from the CSV
    if 'period_end_date' not in df.columns:
        df['period_end_date'] = pd.NaT
    else:
        # Convert to datetime if it's not already
        df['period_end_date'] = pd.to_datetime(df['period_end_date'], dayfirst=True, errors='coerce')

    # Get unique symbols
    symbols = df['symbol'].unique()

    for symbol in symbols:
        # Get rows for this symbol
        symbol_mask = df['symbol'] == symbol
        symbol_lookup = lookup_table[lookup_table['symbol'] == symbol]

        if symbol_lookup.empty:
            continue

        # For each row of this symbol, find matching fiscal year
        for idx in df[symbol_mask].index:
            reference_date = df.at[idx, 'reference_date']

            if pd.isna(reference_date):
                continue

            # Find the fiscal year where reference_date falls within the range
            match = symbol_lookup[
                (reference_date > symbol_lookup['start_range']) &
                (reference_date <= symbol_lookup['end_range'])
            ]

            if not match.empty:
                df.at[idx, 'fiscal_year'] = int(match.iloc[0]['fiscal_year'])

                # Only set period_end_date if it's currently null
                # This preserves the actual statement dates from the CSV
                if pd.isna(df.at[idx, 'period_end_date']):
                    df.at[idx, 'period_end_date'] = match.iloc[0]['end_range']  # Fiscal year end date

    return df


def create_bigquery_table():
    # Initialize BigQuery client
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))

    # Define the dataset and table
    dataset_id = "jse_raw_financial_data_dev_elroy"  # Change this to your dataset ID
    table_id = "financial_statements_metadata"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    # Define the schema
    # symbol,statement_type,period,period_detail,reference_date,period_end_date,period_quarter,fiscal_year,report_type,consolidation_type,status,s3_path,pdf_folder_path
    schema = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("statement_type", "STRING"),
        bigquery.SchemaField("period", "STRING"),
        bigquery.SchemaField("period_detail", "STRING"),
        bigquery.SchemaField("reference_date", "DATE"),  # Actual statement date (e.g., Q1 Dec 31)
        bigquery.SchemaField("period_end_date", "DATE"),  # Fiscal year end date (for backward compatibility)
        bigquery.SchemaField("period_quarter", "STRING"),  # Q1, Q2, Q3, Q4, or FY
        bigquery.SchemaField("fiscal_year", "INTEGER"),  # Calendar year of the fiscal year end
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

def clean_duplicate_s3_paths(df):
    """
    Clean duplicate s3_path entries step-by-step with detailed logging.
    
    Steps:
    1. Identify duplicates
    2. Identify duplicates where all have blank status
    3. Resolve blank status duplicates using statement_type inference from s3_path
    4. Check for remaining duplicates
    5. Resolve remaining duplicates by keeping status=1.0
    6. Check for remaining duplicates again
    7. Apply manual mapping for report_type misclassifications
    8. Handle true duplicates (identical in all fields) by keeping first occurrence
    """
    print("\n" + "="*80)
    print("CLEANING DUPLICATE S3_PATHS - STEP BY STEP")
    print("="*80)
    
    # Manual mapping: s3_path -> correct report_type (for same file misclassified as different statement types)
    correct_report_type_mapping = {
        # BIL files - verified
        's3://jse-renamed-docs-copy/CSV-Copy/BIL/audited_financial_statements/2022/bil-barita_investments_limited_consolidated_statement_of_comprehensive_income-30-september-2022.csv': 'cashflow',
        's3://jse-renamed-docs-copy/CSV-Copy/BIL/audited_financial_statements/2024/bil-barita_investments_limited_consolidated_statement_of_comprehensive_income-30-september-2024.csv': 'income_statement',
        's3://jse-renamed-docs-copy/CSV-Copy/BIL/unaudited_financial_statements/2020/bil-bartita_investments_limited_consolidated_statement_of_cash_flows-june-30-2020.csv': 'cashflow',
        
        # Other companies - verified
        's3://jse-renamed-docs-copy/CSV-Copy/CAC/audited_financial_statements/2022/cac-cac_2000_limited_statement_of_comprehensive_income-31-october-2022.csv': 'income_statement',
        's3://jse-renamed-docs-copy/CSV-Copy/DTL/audited_financial_statements/2018/dtl-derrimon_trading_company_limited_group_statement_of_comprehensive_income-31-december-2018.csv': 'income_statement',
        's3://jse-renamed-docs-copy/CSV-Copy/JBG/unaudited_financial_statements/2023/jbg-jamaica_broilers_group_limited_group_statement_of_comprehensive_income-october-28-2023.csv': 'income_statement',
        's3://jse-renamed-docs-copy/CSV-Copy/KREMI/unaudited_financial_statements/2020/kremi-caribbean_cream_ltd_unaudited_income_statement-august-31-2020.csv': 'income_statement',
        's3://jse-renamed-docs-copy/CSV-Copy/MFS/unaudited_financial_statements/2023/mfs-MFS_capital_partners_limited_unaudited_consolidated_statement_of_financial_position-june-30-2023.csv': 'balance_sheet',
    }
    
    df_working = df.copy()
    print(f"Starting rows: {len(df_working)}")
    print()
    
    # ========================================================================
    # STEP 1: Identify duplicates
    # ========================================================================
    print("STEP 1: Identifying duplicates")
    print("-" * 80)
    duplicate_s3_paths = df_working['s3_path'].value_counts()
    duplicate_s3_paths = duplicate_s3_paths[duplicate_s3_paths > 1].index
    print(f"Found {len(duplicate_s3_paths)} s3_paths with duplicates")
    
    if len(duplicate_s3_paths) == 0:
        print("✓ No duplicates found, skipping cleaning")
        print("="*80 + "\n")
        return df_working
    
    total_duplicate_rows = df_working[df_working['s3_path'].isin(duplicate_s3_paths)]
    print(f"Total rows with duplicate s3_paths: {len(total_duplicate_rows)}")
    print()
    
    # ========================================================================
    # STEP 2: Identify duplicates with all blank status
    # ========================================================================
    print("STEP 2: Identifying duplicates with all blank status")
    print("-" * 80)
    
    blank_status_duplicates = []
    for s3_path in duplicate_s3_paths:
        dup_rows = df_working[df_working['s3_path'] == s3_path]
        # Check if all duplicates have blank/null status
        if dup_rows['status'].isna().all():
            blank_status_duplicates.append(s3_path)
    
    print(f"Found {len(blank_status_duplicates)} s3_paths where ALL duplicates have blank status")
    print(f"These represent {df_working['s3_path'].isin(blank_status_duplicates).sum()} total rows")
    print()
    
    # ========================================================================
    # STEP 3: Resolve blank status duplicates using statement_type inference
    # ========================================================================
    print("STEP 3: Resolving blank status duplicates using statement_type from s3_path")
    print("-" * 80)
    
    rows_to_drop = []
    for s3_path in blank_status_duplicates:
        dup_rows = df_working[df_working['s3_path'] == s3_path]
        s3_path_lower = str(s3_path).lower()
        
        # Determine correct statement_type from path
        if 'unaudited' in s3_path_lower:
            correct_statement_type = 'unaudited'
        else:
            correct_statement_type = 'audited'
        
        # Mark incorrect rows for dropping
        for idx, row in dup_rows.iterrows():
            if str(row['statement_type']).lower() != correct_statement_type:
                rows_to_drop.append(idx)
    
    print(f"Marking {len(rows_to_drop)} rows for removal (wrong statement_type for blank status duplicates)")
    df_working = df_working.drop(rows_to_drop)
    print(f"Rows after blank status resolution: {len(df_working)}")
    print()
    
    # ========================================================================
    # STEP 4: Check for remaining duplicates
    # ========================================================================
    print("STEP 4: Checking for remaining duplicates")
    print("-" * 80)
    
    remaining_duplicates = df_working['s3_path'].value_counts()
    remaining_duplicates = remaining_duplicates[remaining_duplicates > 1].index
    print(f"Remaining duplicate s3_paths: {len(remaining_duplicates)}")
    
    if len(remaining_duplicates) > 0:
        print(f"Total rows with remaining duplicates: {df_working['s3_path'].isin(remaining_duplicates).sum()}")
    print()
    
    # ========================================================================
    # STEP 5: Keep rows where status=1.0 for remaining duplicates
    # ========================================================================
    print("STEP 5: Resolving remaining duplicates by keeping status=1.0")
    print("-" * 80)
    
    rows_to_drop = []
    for s3_path in remaining_duplicates:
        dup_rows = df_working[df_working['s3_path'] == s3_path]
        has_status_1 = (dup_rows['status'] == 1.0).any()
        
        if has_status_1:
            # Drop rows without status=1.0
            for idx, row in dup_rows.iterrows():
                if row['status'] != 1.0:
                    rows_to_drop.append(idx)
    
    print(f"Marking {len(rows_to_drop)} rows for removal (don't have status=1.0)")
    df_working = df_working.drop(rows_to_drop)
    print(f"Rows after status=1.0 resolution: {len(df_working)}")
    print()
    
    # ========================================================================
    # STEP 6: Check for remaining duplicates again
    # ========================================================================
    print("STEP 6: Checking for remaining duplicates after status filter")
    print("-" * 80)
    
    remaining_duplicates = df_working['s3_path'].value_counts()
    remaining_duplicates = remaining_duplicates[remaining_duplicates > 1].index
    print(f"Remaining duplicate s3_paths: {len(remaining_duplicates)}")
    
    if len(remaining_duplicates) > 0:
        print(f"Total rows with remaining duplicates: {df_working['s3_path'].isin(remaining_duplicates).sum()}")
        print("\nSample of remaining duplicates:")
        for s3_path in list(remaining_duplicates)[:3]:
            dup_rows = df_working[df_working['s3_path'] == s3_path]
            print(f"  {s3_path}")
            print(f"    Statement types: {dup_rows['statement_type'].unique()}")
            print(f"    Report types: {dup_rows['report_type'].unique()}")
            print(f"    Statuses: {dup_rows['status'].unique()}")
    print()
    
    # ========================================================================
    # STEP 7: Apply manual mapping for report_type
    # ========================================================================
    print("STEP 7: Applying manual report_type mapping")
    print("-" * 80)
    print(f"Manual mapping covers {len(correct_report_type_mapping)} s3_paths")
    
    rows_to_drop = []
    for s3_path in remaining_duplicates:
        if s3_path in correct_report_type_mapping:
            correct_report_type = correct_report_type_mapping[s3_path]
            dup_rows = df_working[df_working['s3_path'] == s3_path]
            
            for idx, row in dup_rows.iterrows():
                if row['report_type'] != correct_report_type:
                    rows_to_drop.append(idx)
    
    print(f"Marking {len(rows_to_drop)} rows for removal (wrong report_type per manual mapping)")
    df_working = df_working.drop(rows_to_drop)
    print(f"Rows after report_type mapping: {len(df_working)}")
    print()
    
    # ========================================================================
    # STEP 8: Handle true duplicates (identical in all checked fields)
    # ========================================================================
    print("STEP 8: Handling true duplicates (identical rows)")
    print("-" * 80)
    
    remaining_duplicates = df_working['s3_path'].value_counts()
    remaining_duplicates = remaining_duplicates[remaining_duplicates > 1]
    
    if len(remaining_duplicates) > 0:
        print(f"Found {len(remaining_duplicates)} s3_paths that still have duplicates")
        print(f"These are true duplicates (identical in all checked fields)")
        print(f"Keeping first occurrence of each duplicate s3_path")
        
        # Count how many rows will be dropped
        before_count = len(df_working)
        df_working = df_working.drop_duplicates(subset='s3_path', keep='first')
        after_count = len(df_working)
        
        print(f"Dropped {before_count - after_count} true duplicate rows")
        print(f"Rows after removing true duplicates: {after_count}")
    else:
        print("No true duplicates found")
    
    print()
    
    # ========================================================================
    # FINAL CHECK: Verify no duplicates remain
    # ========================================================================
    print("FINAL CHECK: Verifying no duplicates remain")
    print("-" * 80)
    
    final_duplicates = df_working['s3_path'].value_counts()
    final_duplicates = final_duplicates[final_duplicates > 1]
    
    if len(final_duplicates) > 0:
        print(f"⚠️  WARNING: {len(final_duplicates)} s3_paths still have duplicates!")
        print("\nThese need manual review:")
        for s3_path in list(final_duplicates.index)[:5]:
            dup_rows = df_working[df_working['s3_path'] == s3_path]
            print(f"\n  {s3_path}")
            print(f"    Statement types: {dup_rows['statement_type'].unique()}")
            print(f"    Report types: {dup_rows['report_type'].unique()}")
            print(f"    Statuses: {dup_rows['status'].unique()}")
    else:
        print("✓ SUCCESS: No duplicate s3_paths remain!")
    
    print()
    print("="*80)
    print(f"CLEANING COMPLETE")
    print(f"Starting rows: {len(df)}")
    print(f"Final rows: {len(df_working)}")
    print(f"Rows dropped: {len(df) - len(df_working)}")
    print("="*80 + "\n")
    
    return df_working

def transform_symbols(df):
    """
    Transform specific symbol values to their correct representations.

    Transformations:
    - 'KYNTR' -> 'KNTYR'
    - 'MTL' -> 'MTLJA'
    """
    print("\n" + "="*80)
    print("TRANSFORMING SYMBOLS")
    print("="*80)

    symbol_mapping = {
        'KYNTR': 'KNTYR',
        'MTL': 'MTLJA'
    }

    df_working = df.copy()

    # Count records before transformation
    records_to_transform = df_working['symbol'].isin(symbol_mapping.keys()).sum()
    print(f"Found {records_to_transform} records to transform")

    # Show before state
    if records_to_transform > 0:
        print("\nBefore transformation:")
        for old_symbol, new_symbol in symbol_mapping.items():
            count = (df_working['symbol'] == old_symbol).sum()
            if count > 0:
                print(f"  {old_symbol}: {count} records")

    # Apply transformation
    df_working['symbol'] = df_working['symbol'].replace(symbol_mapping)

    # Show after state
    if records_to_transform > 0:
        print("\nAfter transformation:")
        for old_symbol, new_symbol in symbol_mapping.items():
            count = (df_working['symbol'] == new_symbol).sum()
            if count > 0:
                print(f"  {new_symbol}: {count} records (was {old_symbol})")

    print("\n✓ Symbol transformation complete")
    print("="*80 + "\n")

    return df_working


def validate_quarter_chronological_order(df):
    """
    Validate that fiscal quarters are in proper chronological order by reference_date.

    For each (symbol, fiscal_year) group, checks that earlier quarters have earlier
    dates than later quarters. Also checks that FY date >= latest Q-quarter date.

    Returns a DataFrame of violations with columns:
        symbol, fiscal_year, earlier_quarter, earlier_date, later_quarter, later_date, violation_type
    """
    # Filter to rows where fiscal_year, period_quarter, and reference_date are all non-null
    mask = (
        df['fiscal_year'].notna() &
        df['period_quarter'].notna() &
        df['reference_date'].notna()
    )
    filtered = df[mask].copy()

    if filtered.empty:
        return pd.DataFrame(columns=[
            'symbol', 'fiscal_year', 'earlier_quarter', 'earlier_date',
            'later_quarter', 'later_date', 'violation_type'
        ])

    # Ensure reference_date is datetime
    filtered['reference_date'] = pd.to_datetime(filtered['reference_date'], errors='coerce')

    # Group by (symbol, fiscal_year, period_quarter), take min(reference_date)
    # This collapses multiple report types (IS, BS, CF) per quarter into one date
    grouped = (
        filtered
        .groupby(['symbol', 'fiscal_year', 'period_quarter'])['reference_date']
        .min()
        .reset_index()
    )

    # Canonical quarter ordering
    quarter_order = {'Q1': 1, 'Q2': 2, 'Q3': 3, 'Q4': 4, 'FY': 5}

    violations = []

    # For each (symbol, fiscal_year) group, check pairwise ordering
    for (symbol, fiscal_year), group in grouped.groupby(['symbol', 'fiscal_year']):
        # Only keep quarters we know about
        group = group[group['period_quarter'].isin(quarter_order)].copy()
        if len(group) < 2:
            continue

        # Sort by canonical order
        group['sort_key'] = group['period_quarter'].map(quarter_order)
        group = group.sort_values('sort_key')

        quarters = group['period_quarter'].tolist()
        dates = group['reference_date'].tolist()

        # Check all consecutive pairs
        for i in range(len(quarters) - 1):
            q_earlier = quarters[i]
            d_earlier = dates[i]
            q_later = quarters[i + 1]
            d_later = dates[i + 1]

            if q_later == 'FY':
                # FY date must be >= latest Q-quarter date
                if d_later < d_earlier:
                    violations.append({
                        'symbol': symbol,
                        'fiscal_year': int(fiscal_year),
                        'earlier_quarter': q_earlier,
                        'earlier_date': d_earlier,
                        'later_quarter': q_later,
                        'later_date': d_later,
                        'violation_type': 'FY_BEFORE_QUARTER'
                    })
            else:
                # Earlier quarter's date must be strictly less than later quarter's date
                if d_earlier >= d_later:
                    violations.append({
                        'symbol': symbol,
                        'fiscal_year': int(fiscal_year),
                        'earlier_quarter': q_earlier,
                        'earlier_date': d_earlier,
                        'later_quarter': q_later,
                        'later_date': d_later,
                        'violation_type': 'QUARTER_ORDER'
                    })

    return pd.DataFrame(violations)

def load_csv_to_bigquery(csv_source, table_ref):
    """
    Load CSV data to BigQuery.

    Args:
        csv_source: Either a file path (str) or a pandas DataFrame
        table_ref: BigQuery table reference
    """
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))

    # Handle both DataFrame and file path inputs
    if isinstance(csv_source, pd.DataFrame):
        df = csv_source.copy()
    elif isinstance(csv_source, str):
        df = pd.read_csv(csv_source)
    else:
        raise ValueError("csv_source must be either a file path (str) or pandas DataFrame")

    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    print(f"Original data shape: {df.shape}")
    
    # Clean duplicate s3_paths (NEW STEP)
    df = clean_duplicate_s3_paths(df)

    # Transform symbols
    df = transform_symbols(df)

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
    
    # Note: Deduplication already handled in clean_duplicate_s3_paths()
    print(f"Records to load: {len(df)}")
    
    # Convert all string columns to string type to avoid mixed type issues
    string_columns = ['symbol', 'statement_type', 'period', 'period_detail', 'report_type', 'consolidation_type', 'status', 's3_path', 'pdf_folder_path']
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # NEW: Derive reference_date from a 'date' or 'period_end_date' column when present
    # reference_date is the actual statement date (e.g., Q1 Dec 31, 2015)
    # We'll preserve the original period_end_date and use fiscal year assignment to fill in missing values
    if 'date' in df.columns:
        print("Converting 'date' column to reference_date in ISO format...")
        df['reference_date'] = pd.to_datetime(df['date'], errors='coerce')
        df['reference_date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        sample_date = df[['date', 'reference_date']].dropna().head(5)
        print("Sample date conversions:")
        print(sample_date.to_string(index=False))
        # Drop the original 'date' column so it doesn't cause schema mismatch
        df.drop(columns=['date'], inplace=True)
    elif 'period_end_date' in df.columns and df['period_end_date'].notna().any():
        # IMPORTANT: Use existing period_end_date column as reference_date
        # This preserves the actual statement dates from the CSV (31/3/2024, etc.)
        print("Using existing 'period_end_date' column for reference_date...")
        df['reference_date'] = pd.to_datetime(df['period_end_date'], dayfirst=True, errors='coerce')

        # Show some examples
        sample_data = df[['period_detail', 'period_end_date', 'reference_date']].dropna().head(5)
        print("Sample date usage:")
        print(sample_data.to_string(index=False))
    elif 'period_detail' in df.columns:
        # Extract reference_date from period_detail field
        print("Extracting reference_date from period_detail field...")
        df['reference_date'] = df['period_detail'].apply(extract_date_from_period_detail)
        # Convert to datetime for BigQuery
        df['reference_date'] = pd.to_datetime(df['reference_date'], errors='coerce')

        # Show some examples of the extraction
        sample_data = df[['period_detail', 'reference_date']].dropna().head(5)
        print("Sample date extractions:")
        print(sample_data.to_string(index=False))
    else:
        print("Warning: no date columns found in CSV; setting reference_date to None")
        df['reference_date'] = None

    # ========================================================================
    # FISCAL YEAR ASSIGNMENT
    # ========================================================================
    print("\n" + "="*80)
    print("FISCAL YEAR ASSIGNMENT")
    print("="*80)

    # Build fiscal year lookup table from audited statements
    print("\nBuilding fiscal year lookup table from audited statements...")
    fiscal_year_lookup = build_fiscal_year_lookup(df)
    print(f"Lookup table rows: {len(fiscal_year_lookup)}")
    print(f"Unique symbols in lookup: {fiscal_year_lookup['symbol'].nunique()}")

    # Show sample lookup entries
    print("\nSample lookup entries (first 5 symbols):")
    sample_symbols = fiscal_year_lookup['symbol'].unique()[:5]
    for symbol in sample_symbols:
        symbol_rows = fiscal_year_lookup[fiscal_year_lookup['symbol'] == symbol].head(2)
        for _, row in symbol_rows.iterrows():
            print(f"  {symbol}: FY{row['fiscal_year']} ({row['start_range'].strftime('%Y-%m-%d')} to {row['end_range'].strftime('%Y-%m-%d')})")

    # Assign fiscal year to all records
    print("\nAssigning fiscal year to all records...")
    df = assign_fiscal_year(df, fiscal_year_lookup)

    # Show statistics
    records_with_fy = df['fiscal_year'].notna().sum()
    records_without_fy = df['fiscal_year'].isna().sum()
    print(f"Records with fiscal_year assigned: {records_with_fy}")
    print(f"Records without fiscal_year: {records_without_fy}")

    # Show fiscal year distribution
    print("\nFiscal year distribution:")
    fy_dist = df['fiscal_year'].value_counts().sort_index()
    print(fy_dist.head(10).to_string())

    # Validate: Q1 in previous calendar year should have fiscal_year = next year
    print("\n--- Validation: Cross-year Q1 assignments ---")
    q1_records = df[df['period_quarter'] == 'Q1'].copy()
    if not q1_records.empty:
        q1_records['calendar_year'] = q1_records['reference_date'].dt.year
        cross_year_q1 = q1_records[q1_records['calendar_year'] != q1_records['fiscal_year']]
        print(f"Q1 records where calendar year differs from fiscal year: {len(cross_year_q1)}")
        if not cross_year_q1.empty:
            sample = cross_year_q1[['symbol', 'period_detail', 'reference_date', 'period_end_date', 'fiscal_year']].head(5)
            print(sample.to_string(index=False))

    # Show sample of new column structure
    print("\n--- Sample: reference_date vs period_end_date ---")
    sample_cols = df[['symbol', 'period_quarter', 'reference_date', 'period_end_date', 'fiscal_year']].dropna().head(10)
    print(sample_cols.to_string(index=False))

    # Validate: Quarter chronological ordering
    print("\n--- Validation: Quarter Chronological Ordering ---")
    quarter_violations = validate_quarter_chronological_order(df)
    if len(quarter_violations) > 0:
        print(f"WARNING: Found {len(quarter_violations)} quarter ordering violations:")
        print(quarter_violations.to_string(index=False))
    else:
        print("All quarters are in proper chronological order.")

    print("="*80 + "\n")

    # NEW: keep only the columns that exist in the target schema to avoid schema mismatch errors
    schema_cols = [
        "symbol",
        "statement_type",
        "period",
        "period_detail",
        "reference_date",  # Actual statement date (e.g., Q1 Dec 31)
        "period_end_date",  # Fiscal year end date (for backward compatibility)
        "period_quarter",  # Q1, Q2, Q3, Q4, or FY
        "fiscal_year",  # Calendar year of the fiscal year end
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
            bigquery.SchemaField("reference_date", "DATE"),  # Actual statement date (e.g., Q1 Dec 31)
            bigquery.SchemaField("period_end_date", "DATE"),  # Fiscal year end date (for backward compatibility)
            bigquery.SchemaField("period_quarter", "STRING"),  # Q1, Q2, Q3, Q4, or FY
            bigquery.SchemaField("fiscal_year", "INTEGER"),  # Calendar year of the fiscal year end
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
    # Google Sheets URL for the financial statements metadata
    # This is the first worksheet (gid=1598524624)
    google_sheets_url = "https://docs.google.com/spreadsheets/d/1KRZS4EAo7Rq-7ISmkG1QrdmgnAOGY5PCWoa5ApRLQ8g/edit?gid=1598524624#gid=1598524624"

    # Fallback to local CSV if needed (uncomment to use local file instead)
    # csv_path = "/Users/galbraithelroy/Documents/jse-data-extractor/fin_stat_meta/financial_statements_metadata_3_09_2025_with_ID - financial_statements_metadata_3_09_2025 - financial_statements_metadata_3_09_2025 (1).csv"

    # Create the table
    table_ref = create_bigquery_table()

    # Fetch data from Google Sheets
    try:
        df = fetch_csv_from_google_sheets(google_sheets_url)
        # Load the data to BigQuery
        load_csv_to_bigquery(df, table_ref)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nTo use a local CSV file instead, uncomment the csv_path line in main() and pass it to load_csv_to_bigquery()")
        raise

if __name__ == "__main__":
    main() 