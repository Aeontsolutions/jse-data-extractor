import csv
import datetime
import re

def format_date(date_str):
    """
    Convert date from DD-MMM-YYYY to YYYY-MM-DD format.
    Returns empty string if conversion fails.
    """
    if not date_str:
        return ""
    
    try:
        # Parse date in format like "31-Dec-2014"
        match = re.match(r'(\d{1,2})-([A-Za-z]{3})-(\d{4})', date_str)
        if match:
            day, month_str, year = match.groups()
            month_dict = {
                'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
            }
            month = month_dict.get(month_str)
            if month:
                return f"{year}-{month:02d}-{int(day):02d}"
        return ""
    except Exception:
        return ""

def main():
    # Input and output files
    input_file = 'jse_period_currency_mappings.csv'
    currency_output_file = 'lu_currency_mapping.csv'
    period_output_file = 'lu_period_mapping.csv'
    
    # Read the source CSV file
    print(f"Reading {input_file}...")
    source_data = []
    with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        source_data = list(reader)
    
    print(f"Found {len(source_data)} rows in source file")
    
    # Create lu_currency_mapping
    currency_mapping = []
    for row in source_data:
        if row['Currency'] and row['Reporting Period Year End'] and row['Symbol']:
            formatted_date = format_date(row['Reporting Period Year End'])
            currency_mapping.append({
                'symbol': row['Symbol'],
                'currency': row['Currency'],
                'date': formatted_date
            })
    
    # Create lu_period_mapping
    period_mapping = []
    for row in source_data:
        symbol = row['Symbol']
        year_end = format_date(row['Reporting Period Year End'])
        
        if not symbol or not year_end:
            continue
            
        quarters = [
            ('Q1', row.get('Q1', '')),
            ('Q2', row.get('Q2', '')),
            ('Q3', row.get('Q3', '')),
            ('Q4', row.get('Q4', ''))
        ]
        
        for period, date_str in quarters:
            if date_str:
                formatted_date = format_date(date_str)
                if formatted_date:  # Only add if date conversion was successful
                    period_mapping.append({
                        'symbol': symbol,
                        'period': period if period != 'Q4' else 'FY',
                        'report_date': formatted_date,
                        'year_end': year_end
                    })
    
    print(f"Created {len(currency_mapping)} rows for currency mapping")
    print(f"Created {len(period_mapping)} rows for period mapping")
    
    # Write to files
    with open(currency_output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['symbol', 'currency', 'date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(currency_mapping)
    
    with open(period_output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['symbol', 'period', 'report_date', 'year_end']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(period_mapping)
    
    print('CSV files created successfully:')
    print(f'- {currency_output_file}')
    print(f'- {period_output_file}')

if __name__ == "__main__":
    main()