#!/usr/bin/env python3
"""
delete_raw_tables.py - Delete all tables matching jse_raw_* pattern in the specified BigQuery dataset.
"""

import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# BigQuery configuration
PROJECT_ID = "jse-datasphere"
DATASET_ID = "jse_raw_financial_data_dev_elroy"

def main():
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    # List all tables in the dataset
    tables = client.list_tables(DATASET_ID)
    
    # Filter for tables matching jse_raw_* pattern
    raw_tables = [t.table_id for t in tables if t.table_id.startswith("jse_raw_")]
    
    if not raw_tables:
        logging.info("No tables matching jse_raw_* pattern found in dataset %s.%s", PROJECT_ID, DATASET_ID)
        return
    
    logging.info("Found %d tables matching jse_raw_* pattern:", len(raw_tables))
    for table in raw_tables:
        logging.info("  - %s", table)
    
    # Confirm deletion
    confirm = input("\nAre you sure you want to delete these tables? (yes/no): ")
    if confirm.lower() != "yes":
        logging.info("Operation cancelled by user")
        return
    
    # Delete each table
    for table in raw_tables:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table}"
        try:
            client.delete_table(table_ref)
            logging.info("Deleted table: %s", table_ref)
        except Exception as e:
            logging.error("Error deleting table %s: %s", table_ref, e)
    
    logging.info("Operation completed")

if __name__ == "__main__":
    main() 