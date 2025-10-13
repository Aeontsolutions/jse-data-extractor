from google.cloud import bigquery
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


# Set up credentials
# Make sure you have set GOOGLE_APPLICATION_CREDENTIALS environment variable
# export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"

def create_bigquery_table():
    # Initialize BigQuery client
    client = bigquery.Client(project=os.getenv("GOOGLE_PROJECT_ID"))

    # Define the dataset and table
    dataset_id = "jse_raw_financial_data_dev_elroy"  # Change this to your dataset ID
    table_id = "company_or_group_data"
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    # Define the schema
    # We only want to store a subset of columns in BigQuery
    # csv_s3_path, organized_folder_path, pdf_file, page_num, report_type,
    # company_symbol, statement_type, year, statement_category
    schema = [
        bigquery.SchemaField("csv_s3_path", "STRING"),
        bigquery.SchemaField("organized_folder_path", "STRING"),
        bigquery.SchemaField("pdf_file", "STRING"),
        bigquery.SchemaField("page_num", "INTEGER"),
        bigquery.SchemaField("report_type", "STRING"),
        bigquery.SchemaField("company_symbol", "STRING"),
        bigquery.SchemaField("statement_type", "STRING"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("statement_category", "STRING"),
    ]

    # Create the table
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table_ref}")

    return table_ref

def load_csv_to_bigquery(csv_path, table_ref):
    client = bigquery.Client(project=os.getenv("PROJECT_ID"))
    df = pd.read_csv(csv_path)
    # Normalize column names to lower-case for consistency
    df.columns = df.columns.str.lower()

    # Keep only the columns we care about
    desired_cols = [
        "csv_s3_path",
        "organized_folder_path",
        "pdf_file",
        "page_num",
        "report_type",
        "company_symbol",
        "statement_type",
        "year",
        "statement_category",
    ]
    missing_cols = set(desired_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"The following required columns are missing from the CSV: {missing_cols}")

    df = df[desired_cols]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=[
            bigquery.SchemaField("csv_s3_path", "STRING"),
            bigquery.SchemaField("organized_folder_path", "STRING"),
            bigquery.SchemaField("pdf_file", "STRING"),
            bigquery.SchemaField("page_num", "INTEGER"),
            bigquery.SchemaField("report_type", "STRING"),
            bigquery.SchemaField("company_symbol", "STRING"),
            bigquery.SchemaField("statement_type", "STRING"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("statement_category", "STRING"),
        ]
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_ref}")

def main():
    # Create the table
    table_ref = create_bigquery_table()

    # Load the CSV data
    csv_path = "/Users/galbraithelroy/Documents/jse-data-extractor/CSV_Tagged_Final_1_filtered (1)_processed (6)_Final - CSV_Tagged_Final_1_filtered (1)_processed (6)_Final.csv"
    load_csv_to_bigquery(csv_path, table_ref)

if __name__ == "__main__":
    main() 