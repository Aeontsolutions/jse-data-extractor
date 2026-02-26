"""
Data Cleaning and Standardization Script
Fixes issues with canonical item name mapping and ensures all 15 required keys exist
"""

import pandas as pd
import re
from datetime import datetime

# Required keys for the frontend (from Overview.svelte)
REQUIRED_KEYS = [
    # Profitability
    'revenue',
    'gross_profit',
    'operating_profit',
    'net_profit',
    # Market
    'eps',
    # Financial Position
    'total_assets',
    'total_equity',
    'debt_to_equity_ratio',
    'roa',
    'roe',
    'current_ratio',
    # Margins
    'gross_margin',
    'ebitda_margin',
    'operating_margin',
    'net_margin'
]


def to_snake_case(text):
    """
    Convert text to snake_case format.
    """
    if pd.isna(text):
        return text
    
    # Convert to string if not already
    text = str(text).strip()
    
    # Handle special cases and abbreviations
    special_mappings = {
        'CAPEX': 'capital_expenditure',
        'EBITDA': 'ebitda',
        'EPS': 'earnings_per_share',
        'P/E RATIO': 'price_earnings_ratio',
        'EARNINGS PER STOCK UNIT': 'earnings_per_stock_unit',
        'OPERATING PROFIT': 'operating_profit',
        'PROFIT ATTRIBUTABLE TO STOCKHOLDERS': 'profit_attributable_to_stockholders',
        'PROFIT BEFORE TAXATION': 'profit_before_taxation',
        "STOCKHOLDERS' RETURN ON EQUITY": 'stockholders_return_on_equity',
        'Net profit(loss) ': 'net_profit_loss',
        'Profit / (Loss) before tax': 'profit_loss_before_tax',
        'Profit/(loss) before taxation': 'profit_loss_before_taxation',
        'Operating revenue net of interest expense': 'operating_revenue_net_of_interest_expense'
    }
    
    # Check if it's a special case first
    if text in special_mappings:
        return special_mappings[text]
    
    # Replace special characters and normalize spaces
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    # Convert to lowercase and replace spaces with underscores
    text = text.lower().replace(' ', '_')
    
    # Remove any remaining special characters
    text = re.sub(r'[^a-z0-9_]', '', text)
    
    # Remove leading/trailing underscores
    text = text.strip('_')
    
    return text


def create_canonical_mapping():
    """
    Create comprehensive canonical item mapping dictionary.
    Maps all variations to the exact keys required by the frontend.
    """
    canonical_item_mapping = {
        # Core financial metrics
        'operating_profit': 'operating_profit',
        'operating_earnings': 'operating_profit',
        
        'net_profit': 'net_profit',
        'net_income': 'net_profit',
        'net_profit_loss': 'net_profit',
        'net_profit_attributable_to_owners': 'net_profit',
        'net_profit_attributable_to_stockholders': 'net_profit',
        'profit_attributable_to_stockholders': 'net_profit',
        'profit_after_tax': 'net_profit',
        'after_tax_profit': 'net_profit',
        'net_loss': 'net_profit',

        'total_assets': 'total_assets',
        'assets': 'total_assets',
        
        'total_liabilities': 'total_liabilities',
        'total_liability': 'total_liabilities',
        
        'shareholders_equity': 'total_equity',
        'stockholders_equity': 'total_equity',
        'equity': 'total_equity',
        'total_shareholders_equity': 'total_equity',
        'total_equity': 'total_equity',

        'revenue': 'revenue',
        'total_revenue': 'revenue',
        'revenues': 'revenue',
        'operating_revenue': 'revenue',
        'net_operating_revenue': 'revenue',
        'total_revenues': 'revenue',
        'operating_revenue_net_of_interest_expense': 'revenue',
        'insurance_revenue': 'revenue',
        'gross_written_premiums': 'revenue',
        'total_operating_income': 'revenue',
        'rental_income': 'revenue',
        
        'gross_profit': 'gross_profit',
        
        # EBITDA
        'ebitda': 'ebitda',

        # Profit before tax
        'profit_before_tax': 'profit_before_tax',
        'profit_before_taxation': 'profit_before_tax',
        'profit_loss_before_tax': 'profit_before_tax',
        'profit_loss_before_taxation': 'profit_before_tax',
        'pre_tax_profit_or_loss': 'profit_before_tax',
        'pretax_profit': 'profit_before_tax',

        # *** CRITICAL: MARGIN MAPPINGS FOR FRONTEND ***
        # Frontend expects: gross_margin, operating_margin, net_margin, ebitda_margin
        
        'gross_profit_margin': 'gross_margin',
        'ratio_gross_margin': 'gross_margin',
        'ratio_gross_profit_margin': 'gross_margin',
        'gross_margin': 'gross_margin',

        'operating_profit_margin': 'operating_margin',
        'operating_profit_to_revenue': 'operating_margin',
        'ratio_operating_margin': 'operating_margin',
        'ratio_operating_profit_margin': 'operating_margin',
        'operating_margin': 'operating_margin',

        'net_profit_margin': 'net_margin',
        'ratio_net_margin': 'net_margin',
        'net_margin': 'net_margin',
        
        'ratio_profit_before_tax_margin': 'profit_before_tax_margin',
        
        'ratio_ebitda_margin': 'ebitda_margin',
        'ebitda_margin': 'ebitda_margin',

        # Liquidity ratios
        'current_ratio': 'current_ratio',
        'ratio_current_ratio': 'current_ratio',
        'current_asset_ratio': 'current_ratio',
        'liquidity_ratio': 'current_ratio',

        'quick_ratio': 'quick_ratio',
        'ratio_quick_ratio': 'quick_ratio',

        # Leverage ratios
        'debt_to_equity_ratio': 'debt_to_equity_ratio',
        'ratio_debt_to_equity': 'debt_to_equity_ratio',
        'debt_to_equity': 'debt_to_equity_ratio',
        'debt-to-equity': 'debt_to_equity_ratio',

        'debt_assets_ratio': 'debt_to_assets_ratio',
        'ratio_debt_to_assets': 'debt_to_assets_ratio',

        # Return ratios - Frontend expects 'roe' and 'roa'
        'return_on_equity': 'roe',
        'return_on_average_equity': 'roe',
        'ratio_return_on_equity': 'roe',
        'stockholders_return_on_equity': 'roe',
        'roe': 'roe',

        'return_on_assets': 'roa',
        'return_on_average_assets': 'roa',
        'return_on_total_asset': 'roa',
        'return_on_average_total_asset': 'roa',
        'return_on_asset': 'roa',
        'ratio_return_on_assets': 'roa',
        'roa': 'roa',

        'return_on_sales': 'return_on_sales',
        'ratio_return_on_sales': 'return_on_sales',

        'return_on_capital_employed': 'roce',
        'ratio_return_on_capital_employed': 'roce',

        'interest_coverage_ratio': 'interest_coverage_ratio',
        
        'ratio_overheads_to_revenue': 'overhead_to_revenue',
        'overhead_to_revenue': 'overhead_to_revenue',
        
        'ratio_revenue_growth': 'revenue_growth',
        'revenue_growth': 'revenue_growth',

        # Market ratios
        'price_earnings_ratio': 'p/e',
        'ratio_pe_ratio': 'p/e',
        'p/e': 'p/e',

        # Frontend expects 'eps'
        'earnings_per_share': 'eps',
        'eps': 'eps',
        'ratio_eps': 'eps',
        'ratio_earnings_per_share': 'eps',
        'earnings_per_stock_unit': 'eps',
        'earnings_per_stock_unit_cents': 'eps',

        'dividend_per_share': 'dividend_per_share',
        'dividends_per_stock_unit_cents': 'dividend_per_share',
        'ratio_dividend_per_share': 'dividend_per_share',

        'dividend_payout_ratio': 'dividend_payout_ratio',
        'ratio_dividend_payout_ratio': 'dividend_payout_ratio',
        'ratio_dividend_cover': 'dividend_payout_ratio',

        'effective_tax_rate': 'effective_tax_rate',
        'ratio_effective_tax_rate': 'effective_tax_rate',

        # Cash flow metrics
        'operating_cash_flow': 'operating_cash_flow',
        'investing_cash_flow': 'investing_cash_flow',
        'financing_cash_flow': 'financing_cash_flow',
        'net_cash_flow': 'net_cash_flow',
        'free_cash_flow': 'free_cash_flow',

        # Interest income
        'interest_income': 'interest_income',
        'net_interest_income': 'net_interest_income',

        'capital_expenditure': 'capital_expenditure',
        'capex': 'capital_expenditure',

        # Items marked as 'none' - not used as canonical KPIs
        'non_current_assets': 'none',
        'current_assets': 'none',
        'non_current_liabilities': 'none',
        'current_liabilities': 'none',
        'net_working_capital': 'none',
        'retained_earnings': 'none',
        'retained_earnings_opening_balance': 'none',
        'retained_earnings_closing_balance': 'none',
        'dividends': 'none',
        'income_tax_expense': 'none',
        'payroll_expenses': 'none',
        'administrative_expenses': 'none',
        'admin_expenses': 'none',
        'admin_expenses_change': 'none',
        'selling_expenses': 'none',
        'selling_expenses_change': 'none',
        'operating_expenses': 'none',
        'operating_expense': 'none',
        'other_operating_expenses': 'none',
        'visitor_count': 'none',
        'visitors': 'none',
        'number_of_associates': 'none',
        'associate_count': 'none',
        'associates_count': 'none',
        'associates_change': 'none',
        'borrowing': 'none',
        'cash': 'none',
        'cash_and_cash_equivalents': 'none',
        'cash_and_deposits': 'none',
        'net_loans': 'none',
        'total_income': 'none',
        'profit': 'none',
        'profit_growth': 'none',
    }
    
    return canonical_item_mapping


def create_item_name_to_standard_mapping():
    """
    Create mapping from standardized item names to standard items.
    Used to fill missing standard_item values.
    """
    return {
        # Revenue items
        'operating_revenue': 'revenue',
        'revenue': 'revenue',
        
        # Profit items  
        'net_profit': 'net_profit',
        'operating_profit': 'operating_profit',
        'gross_profit': 'gross_profit',
        'profit_after_tax': 'net_profit',
        'income_tax_expense': 'none',
        
        # Balance sheet items
        'total_assets': 'total_assets', 
        'shareholders_equity': 'total_equity',
        'total_equity': 'total_equity',
        'total_liabilities': 'total_liabilities',
        'cash': 'none',
        'cash_and_cash_equivalents': 'none',
        'cash_and_deposits': 'none',
        
        # Ratios and margins
        'gross_profit_margin': 'gross_profit_margin',
        'operating_profit_margin': 'operating_profit_margin', 
        'net_profit_margin': 'net_profit_margin',
        'earnings_per_share': 'eps',
        'current_ratio': 'current_ratio',
        'interest_coverage_ratio': 'interest_coverage_ratio',
        'debt_to_equity_ratio': 'debt_to_equity_ratio',
        'ebitda': 'ebitda',
        'return_on_equity': 'roe',
        'return_on_assets': 'roa',
        
        # Working capital and other items
        'net_working_capital': 'none',
        'retained_earnings_opening_balance': 'none',
        'retained_earnings_closing_balance': 'none',
        'dividends': 'none',
        'capital_expenditure': 'capital_expenditure',
        'capex': 'capital_expenditure',
        
        # Cash flow items
        'operating_cash_flow': 'operating_cash_flow',
        
        # Expense items
        'payroll_expenses': 'none',
        'administrative_expenses': 'none', 
        'admin_expenses': 'none',
        'admin_expenses_change': 'none',
        'selling_expenses': 'none',
        'selling_expenses_change': 'none',
        'operating_expenses': 'none',
        'other_operating_expenses': 'none',
        
        # Operational metrics
        'visitors': 'none',
        'visitor_count': 'none',
        'number_of_associates': 'none',
        'associate_count': 'none',
        'associates_count': 'none',
        'associates_change': 'none',
        
        # Growth metrics
        'revenue_growth': 'revenue_growth',
        'profit_growth': 'none',
    }


def load_and_clean_data(csv_path):
    """Load the CSV and perform initial cleaning."""
    print("Loading data...")
    df = pd.read_csv(csv_path)
    df_cleaned = df.copy()
    
    # Rename 'Unnamed: 0' to 'company' if it exists
    if 'Unnamed: 0' in df_cleaned.columns:
        df_cleaned = df_cleaned.rename(columns={'Unnamed: 0': 'company'})
    
    # Fix company name
    df_cleaned.loc[df_cleaned['symbol'] == 'BIL', 'company'] = "Barita Investments Limited"
    
    # Standardize company names
    company_name_mapping = {
        'Caribbean Flavours & Fragrances Limited': 'Caribbean Flavours and Fragrances Limited'
    }
    df_cleaned['company'] = df_cleaned['company'].replace(company_name_mapping)
    
    print(f"Loaded {len(df_cleaned)} rows")
    print(f"Columns: {list(df_cleaned.columns)}")
    return df_cleaned


def standardize_item_names(df):
    """Apply snake_case standardization to item names."""
    print("Standardizing item names to snake_case...")
    df['item_name_standardized'] = df['item_name'].apply(to_snake_case)
    
    print(f"Original unique items: {len(df['item_name'].unique())}")
    print(f"Standardized unique items: {len(df['item_name_standardized'].unique())}")
    
    return df


def standardize_item_types(df):
    """Standardize item_type column."""
    print("Standardizing item types...")
    
    # Standardize existing values
    item_type_standardization = {
        'line item': 'line_item',
        'line_item': 'line_item',
        'ratio': 'ratio',
    }
    df['item_type'] = df['item_type'].replace(item_type_standardization)
    
    # Create mapping from existing data
    non_null_types = df[df['item_type'].notna()][['item_name_standardized', 'item_type']].drop_duplicates()
    item_type_mapping = {}
    
    for _, row in non_null_types.iterrows():
        standardized_name = row['item_name_standardized']
        item_type = row['item_type']
        
        if standardized_name in item_type_mapping:
            if item_type_mapping[standardized_name] != item_type:
                print(f"  WARNING: Inconsistent classification for '{standardized_name}'")
        else:
            item_type_mapping[standardized_name] = item_type
    
    # Fill missing values
    missing_before = df['item_type'].isna().sum()
    df['item_type_filled'] = df['item_type']
    
    for idx, row in df.iterrows():
        if pd.isna(row['item_type']) and row['item_name_standardized'] in item_type_mapping:
            df.at[idx, 'item_type_filled'] = item_type_mapping[row['item_name_standardized']]
    
    # Fill any remaining NaN with original item_type
    df['item_type_filled'] = df['item_type_filled'].fillna(df['item_type'])
    
    missing_after = df['item_type_filled'].isna().sum()
    print(f"  Filled {missing_before - missing_after} missing item_type values")
    
    return df


def fix_standard_item_column(df):
    """Fix missing standard_item values using item_name_standardized."""
    print("Fixing missing standard_item values...")
    
    item_name_to_standard = create_item_name_to_standard_mapping()
    
    missing_before = df['standard_item'].isna().sum()
    missing_mask = df['standard_item'].isna()
    
    # Fill missing values using the mapping
    df.loc[missing_mask, 'standard_item'] = (
        df.loc[missing_mask, 'item_name_standardized'].map(item_name_to_standard)
    )
    
    missing_after = df['standard_item'].isna().sum()
    filled_count = missing_before - missing_after
    
    print(f"  Missing before: {missing_before}")
    print(f"  Missing after: {missing_after}")
    print(f"  Successfully filled: {filled_count}")
    
    return df


def apply_canonical_mapping(df):
    """Apply canonical mapping to create final canonical_item_name column."""
    print("\nApplying canonical mapping...")
    
    canonical_mapping = create_canonical_mapping()
    
    # Lowercase and strip standard_item first
    df['standard_item_lower'] = df['standard_item'].str.lower().str.strip()
    
    # Apply the canonical mapping
    df['canonical_item_name'] = df['standard_item_lower'].map(canonical_mapping)
    
    # For unmapped values, use the lowercase standard_item as fallback
    df['canonical_item_name'] = df['canonical_item_name'].fillna(df['standard_item_lower'])
    
    # Drop temporary column
    df = df.drop(columns=['standard_item_lower'])
    
    unique_canonical = df['canonical_item_name'].dropna().unique()
    print(f"  Unique canonical items: {len(unique_canonical)}")
    
    return df


def convert_item_values(df):
    """Convert item values to proper float format."""
    print("Converting item values to float...")
    
    # Rename column if needed
    if 'item' in df.columns and 'item_value' not in df.columns:
        df = df.rename(columns={'item': 'item_value'})
    
    # Ensure item_value exists
    if 'item_value' not in df.columns:
        raise ValueError("Neither 'item' nor 'item_value' column found in CSV!")
    
    # Convert string to float (handle commas if present)
    df['item_value'] = df['item_value'].astype(str).str.replace(',', '')
    df['item_value'] = pd.to_numeric(df['item_value'], errors='coerce')
    
    return df


def validate_required_keys(df):
    """Validate that all 15 required keys exist in the data."""
    print("\n" + "="*60)
    print("VALIDATION: Checking for required frontend keys")
    print("="*60)
    
    available_keys = set(df['canonical_item_name'].dropna().unique())
    missing_keys = set(REQUIRED_KEYS) - available_keys
    present_keys = set(REQUIRED_KEYS) & available_keys
    
    print(f"\n✓ Present keys ({len(present_keys)}/15):")
    for key in sorted(present_keys):
        count = len(df[df['canonical_item_name'] == key])
        print(f"  ✓ {key:25s} ({count} records)")
    
    if missing_keys:
        print(f"\n✗ Missing keys ({len(missing_keys)}/15):")
        for key in sorted(missing_keys):
            print(f"  ✗ {key}")
        print("\n⚠️  WARNING: Frontend will show 'undefined' for missing keys!")
        return False
    else:
        print("\n✅ SUCCESS: All 15 required keys are present!")
        return True


def show_sample_company_data(df, symbol='DCOVE', year=2024):
    """Show sample data for a specific company and year."""
    print(f"\n{'='*60}")
    print(f"Sample data for {symbol} {year}")
    print(f"{'='*60}")
    
    sample = df[(df['symbol'] == symbol) & (df['year'] == year)]
    
    if len(sample) == 0:
        print(f"No data found for {symbol} {year}")
        return
    
    # Show key metrics
    key_metrics = sample[sample['canonical_item_name'].isin(REQUIRED_KEYS)]
    
    print(f"\nFound {len(key_metrics)} required metrics:")
    for _, row in key_metrics.iterrows():
        print(f"  {row['canonical_item_name']:25s} = {row['item_value']:,.2f}")


def prepare_final_dataset(df):
    """Prepare the final cleaned dataset."""
    print("\nPreparing final dataset...")
    
    cols_to_keep = [
        "company",
        "symbol",
        "year",
        "item_name_standardized",
        "item_type_filled",
        "canonical_item_name",
        "item_value",
        "unit_multiplier",
        "confidence",
        "drive_path"
    ]
    
    df_final = df[cols_to_keep].copy()
    
    # Remove rows with no canonical_item_name
    before = len(df_final)
    df_final = df_final[df_final['canonical_item_name'].notna()]
    after = len(df_final)
    
    if before != after:
        print(f"  Removed {before - after} rows with no canonical_item_name")
    
    print(f"  Final dataset: {len(df_final)} rows")
    
    return df_final


def main():
    """Main execution function."""
    print("="*60)
    print("Data Cleaning and Standardization Pipeline")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Input and output paths
    input_csv = '/Users/galbraithelroy/Documents/jse-data-extractor/metrics/metrics_consolidated - Sheet1.csv'
    output_csv = '/Users/galbraithelroy/Documents/jse-data-extractor/metrics/cleaned_standardized_items_fixed.csv'
    mapping_csv = '/Users/galbraithelroy/Documents/jse-data-extractor/metrics/canonical_item_mapping.csv'
    
    # Step 1: Load and clean (handles column renaming internally)
    df = load_and_clean_data(input_csv)
    
    # Step 2: Standardize item names
    df = standardize_item_names(df)
    
    # Step 3: Standardize item types
    df = standardize_item_types(df)
    
    # Step 4: Convert item values
    df = convert_item_values(df)
    
    # Step 5: Fix missing standard_item values
    df = fix_standard_item_column(df)
    
    # Step 6: Apply canonical mapping (THE CRITICAL FIX!)
    df = apply_canonical_mapping(df)
    
    # Step 7: Validate required keys
    validation_passed = validate_required_keys(df)
    
    # Step 8: Show sample data
    show_sample_company_data(df, 'DCOVE', 2024)
    
    # Step 9: Prepare final dataset
    df_final = prepare_final_dataset(df)
    
    # Step 10: Save outputs
    print("\nSaving outputs...")
    df_final.to_csv(output_csv, index=False)
    print(f"  ✓ Saved cleaned data to: {output_csv}")
    
    # Save mapping reference
    mapping_df = df_final[['item_name_standardized', 'canonical_item_name']].drop_duplicates()
    mapping_df.to_csv(mapping_csv, index=False)
    print(f"  ✓ Saved mapping reference to: {mapping_csv}")
    
    # Final summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total records processed: {len(df_final):,}")
    print(f"Companies: {df_final['company'].nunique()}")
    print(f"Symbols: {df_final['symbol'].nunique()}")
    print(f"Years: {sorted(df_final['year'].unique())}")
    print(f"Unique canonical items: {df_final['canonical_item_name'].nunique()}")
    
    if validation_passed:
        print(f"\n✅ Data is ready for BigQuery migration!")
    else:
        print(f"\n⚠️  Some required keys are missing. Review validation output above.")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


if __name__ == "__main__":
    main()

