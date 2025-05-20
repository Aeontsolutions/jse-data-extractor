#!/usr/bin/env python3
"""
validate_pipeline.py - Validate that all S3 files for a symbol are present in BigQuery.
"""

import boto3
import argparse
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd

# Load environment variables
load_dotenv()

# Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "jse-renamed-docs"
S3_BASE_PREFIX = "CSV/"
PROJECT = "jse-datasphere"
DATASET = "jse_raw_financial_data_dev_elroy"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_s3_files(s3_client, symbol):
    """Get list of CSV files from S3 for a symbol."""
    prefix = f"{S3_BASE_PREFIX}{symbol}/"
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        s3_files = set()
        
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith('.csv'):
                    s3_files.add(key)
        
        return s3_files
    
    except Exception as e:
        logging.error(f"Error listing S3 files for symbol {symbol}: {e}")
        return set()

def get_bq_files(client, symbol):
    """Get list of files from BigQuery table for a symbol."""
    table_id = f"{PROJECT}.{DATASET}.jse_raw_{symbol}"
    
    try:
        query = f"""
        SELECT DISTINCT csv_path
        FROM `{table_id}`
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        bq_files = {row.csv_path for row in results}
        return bq_files
    
    except Exception as e:
        logging.error(f"Error querying BigQuery for symbol {symbol}: {e}")
        return set()

def main():
    parser = argparse.ArgumentParser(description="Validate S3 files against BigQuery table")
    parser.add_argument("-s", "--symbol", required=True, help="Symbol to validate (e.g., JPS)")
    args = parser.parse_args()
    
    symbol = args.symbol.upper()
    
    # Initialize clients
    s3_client = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    ).client('s3')
    
    bq_client = bigquery.Client(project=PROJECT)
    
    # Get files from both sources
    s3_files = get_s3_files(s3_client, symbol)
    bq_files = get_bq_files(bq_client, symbol)
    
    if not s3_files:
        logging.error(f"No files found in S3 for symbol {symbol}")
        return
    
    if not bq_files:
        logging.error(f"No files found in BigQuery for symbol {symbol}")
        return
    
    # Compare the sets
    missing_in_bq = s3_files - bq_files
    extra_in_bq = bq_files - s3_files
    
    # Print results
    print(f"\nValidation Results for {symbol}:")
    print("-" * 80)
    print(f"Total files in S3: {len(s3_files)}")
    print(f"Total files in BigQuery: {len(bq_files)}")
    
    if not missing_in_bq and not extra_in_bq:
        print("\n✅ All files match between S3 and BigQuery!")
    else:
        if missing_in_bq:
            print(f"\n❌ Files in S3 but missing from BigQuery ({len(missing_in_bq)}):")
            for file in sorted(missing_in_bq):
                print(f"  - {file}")
        
        if extra_in_bq:
            print(f"\n⚠️ Files in BigQuery but not in S3 ({len(extra_in_bq)}):")
            for file in sorted(extra_in_bq):
                print(f"  - {file}")

if __name__ == "__main__":
    main() 