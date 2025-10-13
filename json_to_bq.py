#!/usr/bin/env python3
"""
json_to_bq.py - Load JSON financial data into BigQuery.
Creates table if it doesn't exist and appends new rows.
Handles deduplication based on unique identifiers.
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Set

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tqdm import tqdm

# Configuration
PROJECT = "jse-datasphere"
DATASET = "jse_raw_financial_data_dev_elroy"
TABLE_NAME = "standardized_financial_data"
LOGLEVEL = logging.INFO

# Set up logging
logging.basicConfig(
    level=LOGLEVEL,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)

def flatten_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten the nested JSON structure into a single level dictionary."""
    flattened = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                # Standardize column names by replacing spaces and hyphens with underscores
                standardized_key = inner_key.replace(" ", "_").replace("-", "_")
                flattened[standardized_key] = inner_value
    return flattened

def create_table_if_not_exists(client: bigquery.Client, dataset_ref: bigquery.DatasetReference) -> None:
    """Create the BigQuery table if it doesn't exist."""
    table_ref = dataset_ref.table(TABLE_NAME)
    
    schema = [
        bigquery.SchemaField("revenue", "FLOAT"),
        bigquery.SchemaField("revenue_units", "FLOAT"),
        bigquery.SchemaField("gross_profit", "FLOAT"),
        bigquery.SchemaField("gross_profit_units", "FLOAT"),
        bigquery.SchemaField("Operating_Profit", "FLOAT"),
        bigquery.SchemaField("Operating_Profit_units", "FLOAT"),
        bigquery.SchemaField("Net_Profit", "FLOAT"),
        bigquery.SchemaField("Net_Profit_units", "FLOAT"),
        bigquery.SchemaField("Current_Assets", "FLOAT"),
        bigquery.SchemaField("Current_Assets_units", "FLOAT"),
        bigquery.SchemaField("Non_Current", "FLOAT"),
        bigquery.SchemaField("Non_Current_units", "FLOAT"),
        bigquery.SchemaField("Total_Assets", "FLOAT"),
        bigquery.SchemaField("Total_Assets_units", "FLOAT"),
        bigquery.SchemaField("Total_Equity_Attributable_to_Shareholders", "FLOAT"),
        bigquery.SchemaField("Total_Equity_Attributable_to_Shareholders_units", "FLOAT"),
        bigquery.SchemaField("reporting_period", "STRING"),
        bigquery.SchemaField("report_date", "DATE"),
        bigquery.SchemaField("data_source", "STRING"),
        bigquery.SchemaField("company_symbol", "STRING"),
        bigquery.SchemaField("statement_type", "STRING"),
        bigquery.SchemaField("document_year", "STRING"),
        bigquery.SchemaField("filename", "STRING"),
        bigquery.SchemaField("s3_path", "STRING"),
        bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("chunks_processed", "INTEGER"),
        bigquery.SchemaField("extraction_method", "STRING"),
    ]

    try:
        client.get_table(table_ref)
        logging.info(f"Table {TABLE_NAME} already exists")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        logging.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def get_existing_records(client: bigquery.Client, dataset_ref: bigquery.DatasetReference) -> Set[tuple]:
    """Get set of existing records based on unique identifiers."""
    query = f"""
    SELECT 
        company_symbol,
        statement_type,
        report_date,
        filename
    FROM `{PROJECT}.{DATASET}.{TABLE_NAME}`
    """
    query_job = client.query(query)
    results = query_job.result()
    
    # Create a set of tuples containing the unique identifiers
    existing_records = set()
    for row in results:
        existing_records.add((
            row.company_symbol,
            row.statement_type,
            row.report_date,
            row.filename
        ))
    return existing_records

def load_json_to_bq(json_file: str) -> None:
    """Load JSON data into BigQuery with deduplication."""
    t0 = time.time()
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT)
    dataset_ref = client.dataset(DATASET)
    
    # Create table if it doesn't exist
    create_table_if_not_exists(client, dataset_ref)
    
    # Get existing records
    existing_records = get_existing_records(client, dataset_ref)
    logging.info(f"Found {len(existing_records)} existing records in BigQuery")
    
    # Read and process JSON data
    logging.info(f"Reading JSON file: {json_file}")
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Flatten and convert to DataFrame
    flattened_data = [flatten_json({k: v}) for k, v in data.items()]
    df = pd.DataFrame(flattened_data)
    
    # Convert date columns
    df['report_date'] = pd.to_datetime(df['report_date']).dt.date
    df['extraction_timestamp'] = pd.to_datetime(df['extraction_timestamp'])
    
    # Filter out existing records
    new_records = []
    for _, row in df.iterrows():
        record_key = (
            row['company_symbol'],
            row['statement_type'],
            row['report_date'],
            row['filename']
        )
        if record_key not in existing_records:
            new_records.append(row)
    
    if not new_records:
        logging.info("No new records to insert")
        return
    
    # Convert new records to DataFrame
    new_df = pd.DataFrame(new_records)
    logging.info(f"Found {len(new_df)} new records to insert")
    
    # Load to BigQuery
    table_ref = dataset_ref.table(TABLE_NAME)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )
    
    job = client.load_table_from_dataframe(
        new_df, table_ref, job_config=job_config
    )
    job.result()  # Wait for the job to complete
    
    logging.info(f"✅ Successfully loaded {len(new_df)} new records in {time.time() - t0:.1f}s")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load JSON financial data into BigQuery")
    parser.add_argument("json_file", help="Path to the JSON file containing financial data")
    args = parser.parse_args()
    
    load_json_to_bq(args.json_file)

if __name__ == "__main__":
    main() 