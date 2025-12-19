"""
Exploration script to analyze fiscal year-end patterns across companies
and build a time-aware fiscal year lookup table.

This will help us understand:
1. How many unique symbols have audited statements
2. What are the fiscal year-end months across companies
3. Are there companies with multiple/changing fiscal year-ends
4. Build a lookup table with date ranges for fiscal year assignment
"""

from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv
from migrate_to_bigquery import fetch_csv_from_google_sheets, extract_date_from_period_detail

load_dotenv()


def build_fiscal_year_lookup(audited_df):
    """
    Build a time-aware fiscal year lookup table from audited statements.

    For each company, creates date ranges where:
    - Start Range = Previous Audited Date + 1 day (or min date for first year)
    - End Range = Current Audited Date (the fiscal year end)

    A statement belongs to a fiscal year if:
    period_end_date > start_range AND period_end_date <= end_range

    Returns:
        DataFrame with columns: symbol, fiscal_year, start_range, end_range
    """
    # Get unique audited dates per symbol (deduplicate by symbol + period_end_date)
    # We group by symbol and period_end_date to handle multiple report_types for same FY
    fy_dates = audited_df.groupby(['symbol', 'period_end_date']).size().reset_index()[['symbol', 'period_end_date']]
    fy_dates = fy_dates.sort_values(['symbol', 'period_end_date']).reset_index(drop=True)

    # For each symbol, calculate the start range using lag (previous audited date + 1 day)
    fy_dates['prev_audited_date'] = fy_dates.groupby('symbol')['period_end_date'].shift(1)

    # Start range is previous audited date + 1 day
    # For the first year of each company, use a very early date
    fy_dates['start_range'] = fy_dates['prev_audited_date'] + pd.Timedelta(days=1)

    # Fill NaT (first year for each company) with a default early date
    min_date = pd.Timestamp('1900-01-01')
    fy_dates['start_range'] = fy_dates['start_range'].fillna(min_date)

    # End range is the audited date itself
    fy_dates['end_range'] = fy_dates['period_end_date']

    # Fiscal year is the calendar year of the audited date
    fy_dates['fiscal_year'] = fy_dates['period_end_date'].dt.year

    # Select final columns
    lookup_table = fy_dates[['symbol', 'fiscal_year', 'start_range', 'end_range']].copy()

    return lookup_table


def assign_fiscal_year(df, lookup_table):
    """
    Assign fiscal year to each record based on the lookup table.

    A record belongs to fiscal year X if:
    period_end_date > start_range AND period_end_date <= end_range

    Args:
        df: DataFrame with all records (must have 'symbol' and 'period_end_date')
        lookup_table: DataFrame from build_fiscal_year_lookup()

    Returns:
        DataFrame with 'fiscal_year' column added
    """
    df = df.copy()
    df['fiscal_year'] = None

    # For each record, find the matching fiscal year range
    for idx, row in df.iterrows():
        symbol = row['symbol']
        period_end = row['period_end_date']

        if pd.isna(period_end):
            continue

        # Get lookup entries for this symbol
        symbol_lookup = lookup_table[lookup_table['symbol'] == symbol]

        if symbol_lookup.empty:
            continue

        # Find the fiscal year where period_end falls within the range
        # period_end_date > start_range AND period_end_date <= end_range
        match = symbol_lookup[
            (period_end > symbol_lookup['start_range']) &
            (period_end <= symbol_lookup['end_range'])
        ]

        if not match.empty:
            df.at[idx, 'fiscal_year'] = match.iloc[0]['fiscal_year']

    return df


def explore_fiscal_year_ends():
    # Fetch data from Google Sheets
    google_sheets_url = "https://docs.google.com/spreadsheets/d/1KRZS4EAo7Rq-7ISmkG1QrdmgnAOGY5PCWoa5ApRLQ8g/edit?gid=1598524624#gid=1598524624"

    print("Fetching data from Google Sheets...")
    df = fetch_csv_from_google_sheets(google_sheets_url)
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    print(f"\nTotal records: {len(df)}")
    print(f"Unique symbols: {df['symbol'].nunique()}")

    # Extract period_end_date
    print("\nExtracting period_end_date from period_detail...")
    df['period_end_date'] = df['period_detail'].apply(extract_date_from_period_detail)
    df['period_end_date'] = pd.to_datetime(df['period_end_date'], errors='coerce')

    # Filter to audited statements only
    audited_df = df[df['statement_type'].str.lower() == 'audited'].copy()
    print(f"\nAudited records: {len(audited_df)}")
    print(f"Symbols with audited statements: {audited_df['symbol'].nunique()}")

    # Extract month from period_end_date
    audited_df['fy_end_month'] = audited_df['period_end_date'].dt.month
    audited_df['fy_end_day'] = audited_df['period_end_date'].dt.day

    # Get unique fiscal year-end patterns per symbol
    print("\n" + "="*80)
    print("FISCAL YEAR-END ANALYSIS")
    print("="*80)

    # Group by symbol and find unique fiscal year-end months
    fy_patterns = audited_df.groupby('symbol').agg({
        'fy_end_month': lambda x: sorted(x.dropna().unique().tolist()),
        'period_end_date': 'count'
    }).rename(columns={'period_end_date': 'num_audited_records'})

    fy_patterns['num_unique_months'] = fy_patterns['fy_end_month'].apply(len)

    # Companies with consistent fiscal year-end (1 unique month)
    consistent = fy_patterns[fy_patterns['num_unique_months'] == 1]
    print(f"\n1. Companies with CONSISTENT fiscal year-end: {len(consistent)}")

    # Companies with multiple fiscal year-ends (changed over time)
    changing = fy_patterns[fy_patterns['num_unique_months'] > 1]
    print(f"2. Companies with CHANGING fiscal year-end: {len(changing)}")

    # Distribution of fiscal year-end months
    print("\n" + "-"*80)
    print("FISCAL YEAR-END MONTH DISTRIBUTION (for consistent companies)")
    print("-"*80)

    # Flatten the consistent companies' months
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }

    consistent_months = consistent['fy_end_month'].apply(lambda x: x[0] if x else None)
    month_dist = consistent_months.value_counts().sort_index()

    print("\nMonth | Count | Companies")
    print("-" * 60)
    for month, count in month_dist.items():
        if pd.notna(month):
            month_int = int(month)
            companies = consistent[consistent['fy_end_month'].apply(lambda x: x[0] if x else None) == month].index.tolist()
            # Show first 5 companies as examples
            company_examples = ', '.join(companies[:5])
            if len(companies) > 5:
                company_examples += f", ... (+{len(companies)-5} more)"
            print(f"{month_names.get(month_int, month):12} | {count:5} | {company_examples}")

    # Details on companies with changing fiscal year-ends
    if len(changing) > 0:
        print("\n" + "-"*80)
        print("COMPANIES WITH CHANGING FISCAL YEAR-END")
        print("-"*80)

        for symbol in changing.index:
            symbol_data = audited_df[audited_df['symbol'] == symbol][['period_detail', 'period_end_date', 'fy_end_month']].drop_duplicates()
            symbol_data = symbol_data.sort_values('period_end_date')

            print(f"\n{symbol}:")
            print(f"  Unique FY-end months: {changing.loc[symbol, 'fy_end_month']}")
            print(f"  Records:")
            for _, row in symbol_data.iterrows():
                print(f"    - {row['period_detail']} -> Month {int(row['fy_end_month']) if pd.notna(row['fy_end_month']) else 'N/A'}")

    # Check for any symbols without audited statements
    all_symbols = set(df['symbol'].unique())
    audited_symbols = set(audited_df['symbol'].unique())
    missing_audited = all_symbols - audited_symbols

    if missing_audited:
        print("\n" + "-"*80)
        print(f"SYMBOLS WITHOUT AUDITED STATEMENTS: {len(missing_audited)}")
        print("-"*80)
        print(', '.join(sorted(missing_audited)))

    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total symbols: {len(all_symbols)}")
    print(f"Symbols with audited statements: {len(audited_symbols)}")
    print(f"Symbols with consistent FY-end: {len(consistent)}")
    print(f"Symbols with changing FY-end: {len(changing)}")
    print(f"Symbols without audited statements: {len(missing_audited)}")

    # ========================================================================
    # BUILD AND TEST THE LOOKUP TABLE
    # ========================================================================
    print("\n" + "="*80)
    print("BUILDING FISCAL YEAR LOOKUP TABLE")
    print("="*80)

    lookup_table = build_fiscal_year_lookup(audited_df)
    print(f"\nLookup table rows: {len(lookup_table)}")
    print(f"Unique symbols in lookup: {lookup_table['symbol'].nunique()}")

    # Show sample for 138SL (September year-end)
    print("\n" + "-"*80)
    print("SAMPLE: 138SL (September year-end)")
    print("-"*80)
    sample_138sl = lookup_table[lookup_table['symbol'] == '138SL']
    print(sample_138sl.to_string(index=False))

    # Show sample for BRG (changed from March to December)
    print("\n" + "-"*80)
    print("SAMPLE: BRG (changed from March to December in 2017)")
    print("-"*80)
    sample_brg = lookup_table[lookup_table['symbol'] == 'BRG']
    print(sample_brg.to_string(index=False))

    # ========================================================================
    # TEST FISCAL YEAR ASSIGNMENT
    # ========================================================================
    print("\n" + "="*80)
    print("TESTING FISCAL YEAR ASSIGNMENT")
    print("="*80)

    # Test with 138SL records
    test_138sl = df[df['symbol'] == '138SL'].copy()
    test_138sl = assign_fiscal_year(test_138sl, lookup_table)

    print("\n138SL - All records with assigned fiscal years:")
    print("-"*80)
    test_display = test_138sl[['symbol', 'statement_type', 'period_detail', 'period_end_date', 'fiscal_year']].drop_duplicates()
    test_display = test_display.sort_values('period_end_date')
    print(test_display.to_string(index=False))

    # Validate: Q1 (31-Dec-15) should be FY 2016
    q1_dec_15 = test_138sl[test_138sl['period_detail'].str.contains('Q1.*31-Dec-15', na=False, regex=True)]
    if not q1_dec_15.empty:
        assigned_fy = q1_dec_15.iloc[0]['fiscal_year']
        print(f"\n✓ Validation: Q1 (31-Dec-15) assigned to FY {assigned_fy}")
        if assigned_fy == 2016:
            print("  CORRECT! Q1 Dec-2015 belongs to FY2016 (ending Sept 2016)")
        else:
            print(f"  WARNING: Expected FY2016, got FY{assigned_fy}")

    # Test with BRG (company that changed fiscal year-end)
    print("\n" + "-"*80)
    print("BRG - Testing fiscal year transition (Mar -> Dec in 2017)")
    print("-"*80)
    test_brg = df[df['symbol'] == 'BRG'].copy()
    test_brg = assign_fiscal_year(test_brg, lookup_table)
    test_brg_display = test_brg[['symbol', 'statement_type', 'period_detail', 'period_end_date', 'fiscal_year']].drop_duplicates()
    test_brg_display = test_brg_display.sort_values('period_end_date')
    print(test_brg_display.head(20).to_string(index=False))

    # Return dataframes for further exploration
    return {
        'full_data': df,
        'audited_data': audited_df,
        'fy_patterns': fy_patterns,
        'consistent': consistent,
        'changing': changing,
        'missing_audited': missing_audited,
        'lookup_table': lookup_table
    }


if __name__ == "__main__":
    results = explore_fiscal_year_ends()
