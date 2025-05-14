"""
Utility functions for the JSE Data Extractor.
"""

import re
import io
import logging
from datetime import datetime
from typing import Optional, Dict, List, Union

def parse_date_from_filename(filename: str) -> Optional[datetime.date]:
    """
    Parse YYYY-MM-DD date from the end of the filename string.
    
    Args:
        filename: The filename to parse
        
    Returns:
        datetime.date object if successful, None otherwise
    """
    match = re.search(r'-([a-zA-Z]+)-(\d{1,2})-(\d{4})\.csv$', filename, re.IGNORECASE)
    if match:
        month_str, day_str, year_str = match.groups()
        try:
            day = int(day_str)
            year = int(year_str)
            month_map = {
                'january': 1, 'february': 2, 'march': 3, 'april': 4,
                'may': 5, 'june': 6, 'july': 7, 'august': 8,
                'september': 9, 'october': 10, 'november': 11, 'december': 12
            }
            month = month_map.get(month_str.lower())
            if month:
                return datetime(year, month, day).date()
            else:
                logging.warning(f"Month parse fail '{month_str}' in {filename}")
                return None
        except ValueError as e:
            logging.warning(f"Date component error {year_str}-{month_str}-{day_str} in {filename}: {e}")
            return None
    else:
        logging.warning(f"Date pattern '-Month-DD-YYYY.csv' not found at end of {filename}")
        return None

def clean_value(value_raw: Union[str, int, float, None]) -> Optional[float]:
    """
    Clean financial string/numeric value and convert to float.
    
    Args:
        value_raw: The raw value to clean
        
    Returns:
        float value if successful, None otherwise
    """
    if value_raw is None:
        return None
    if isinstance(value_raw, (int, float)):
        return float(value_raw)
    if not isinstance(value_raw, str):
        return None
        
    value_str = re.sub(r'\[[a-zA-Z0-9]+\]$', '', value_raw.strip())
    cleaned = value_str.replace(',', '').replace('$', '').replace(' ', '')
    is_negative = False
    
    if cleaned.startswith('(') and cleaned.endswith(')'):
        is_negative = True
        cleaned = cleaned[1:-1]
    if cleaned.startswith('-'):
        is_negative = True
        
    try:
        if not cleaned:
            return None
        value_float = float(cleaned)
        if is_negative and value_float > 0:
            value_float *= -1
        elif is_negative and value_float == 0:
            value_float = 0.0
        return value_float
    except ValueError:
        return None

def load_statement_mapping(csv_content_str: str) -> Dict[str, List[Dict[str, str]]]:
    """
    Load and process financial statement mapping data.
    
    Args:
        csv_content_str: The CSV content as a string
        
    Returns:
        Dictionary mapping symbols to their statement configurations
    """
    mapping_data = {}
    
    try:
        csv_io = io.StringIO(csv_content_str)
        import csv
        reader = csv.DictReader(csv_io)
        
        for row in reader:
            symbol = row.get('Symbol')
            if not symbol:
                continue
                
            if symbol not in mapping_data:
                mapping_data[symbol] = []
                
            keywords = row.get('Associated Title Key Words', '')
            
            mapping_data[symbol].append({
                'company': row.get('Company', ''),
                'report_type': row.get('Report Type', ''),
                'statement_type': row.get('Statement Type', ''),
                'keywords': keywords,
                'annual_period_start': row.get('Annual Period Start', ''),
                'annual_period_end': row.get('Annual Period End', ''),
                'note': row.get('Note', '')
            })
            
        return mapping_data
        
    except Exception as e:
        logging.error(f"Error processing statement mapping CSV: {e}")
        return {} 