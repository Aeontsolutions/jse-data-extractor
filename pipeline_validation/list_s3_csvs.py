#!/usr/bin/env python3
"""
list_s3_csvs.py - List all CSV files in S3 bucket for a specific symbol.
"""

import boto3
import argparse
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "jse-renamed-docs"
S3_BASE_PREFIX = "CSV/"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def list_csv_files(s3_client, symbol):
    """List all CSV files for a specific symbol in the S3 bucket."""
    prefix = f"{S3_BASE_PREFIX}{symbol}/"
    
    try:
        # List objects with the symbol prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        csv_files = []
        
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.lower().endswith('.csv'):
                    # Get last modified time
                    last_modified = obj['LastModified']
                    size_bytes = obj['Size']
                    size_mb = size_bytes / (1024 * 1024)  # Convert to MB
                    
                    csv_files.append({
                        'filename': os.path.basename(key),
                        'path': key,
                        'last_modified': last_modified,
                        'size_mb': round(size_mb, 2)
                    })
        
        return csv_files
    
    except Exception as e:
        logging.error(f"Error listing files for symbol {symbol}: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description="List CSV files in S3 for a specific symbol")
    parser.add_argument("-s", "--symbol", required=True, help="Symbol to list files for (e.g., JPS)")
    args = parser.parse_args()
    
    # Initialize S3 client
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3_client = session.client('s3')
    
    # List files
    csv_files = list_csv_files(s3_client, args.symbol.upper())
    
    if not csv_files:
        logging.info(f"No CSV files found for symbol {args.symbol}")
        return
    
    # Print results
    print(f"\nFound {len(csv_files)} CSV files for {args.symbol}:")
    print("-" * 100)
    print(f"{'Filename':<60} {'Last Modified':<25} {'Size (MB)':<10}")
    print("-" * 100)
    
    for file in sorted(csv_files, key=lambda x: x['last_modified'], reverse=True):
        print(f"{file['filename']:<60} {file['last_modified'].strftime('%Y-%m-%d %H:%M:%S'):<25} {file['size_mb']:<10.2f}")
    
    print("-" * 100)
    print(f"Total files: {len(csv_files)}")

if __name__ == "__main__":
    main() 