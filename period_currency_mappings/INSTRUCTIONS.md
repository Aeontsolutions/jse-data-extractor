# BigQuery JSE Currency and Period Mapping Setup Instructions

This document provides step-by-step instructions for downloading JSE period and currency mapping data and loading it into BigQuery.

## Prerequisites

- Google Cloud SDK installed
- Python 3.6 or higher
- Access to the JSE-Datasphere BigQuery project

## Step 1: Download the Source Data

1. Go to the Google Sheet at this URL:
   ```
   https://docs.google.com/spreadsheets/u/2/d/1MV55DeVFsCeQrB9epO8pNTkLdmiG7vibUKGZ2wfvIv0/edit?gid=0#gid=0
   ```

2. Download the sheet as a CSV file:
   - Click "File"
   - Select "Download"
   - Choose "Comma-separated values (.csv)"

3. Save the file as `jse_period_currency_mappings.csv` in your working directory

## Step 2: Authenticate with Google Cloud

1. Open a terminal or command prompt

2. Authenticate with your Google Cloud account:
   ```bash
   gcloud auth login
   ```

3. Follow the prompts to complete the authentication process

4. Set your project:
   ```bash
   gcloud config set project jse-datasphere
   ```

## Step 3: Prepare and Run the CSV Export Script

1. Create a new file named `export_csv.py` using the provided Python script

2. Run the script to generate the formatted CSV files:
   ```bash
   python export_csv.py
   ```

3. This will create two files:
   - `lu_currency_mapping.csv`
   - `lu_period_mapping.csv`

4. The script converts dates to the YYYY-MM-DD format, and skips any dates that can't be properly converted.

## Step 4: Load the Data into BigQuery

### Option A: Using the Shell Script

1. Create a file named `load_to_bq.sh` with the provided bash script

2. Make the script executable:
   ```bash
   chmod +x load_to_bq.sh
   ```

3. Run the script:
   ```bash
   ./load_to_bq.sh
   ```

### Option B: Using the Python BigQuery Client

1. Ensure you have the required Python package:
   ```bash
   pip install google-cloud-bigquery
   ```

2. Create a file named `load_to_bq.py` with the provided Python script

3. Run the script:
   ```bash
   python load_to_bq.py
   ```

## Step 5: Verify the Data in BigQuery

1. Go to the BigQuery Console:
   ```
   https://console.cloud.google.com/bigquery
   ```

2. Navigate to the `jse-datasphere` project and the `jse_raw_financial_data` dataset

3. Run the following queries to verify the data was loaded:

   ```sql
   -- Check the currency mapping table
   SELECT COUNT(*) as row_count 
   FROM `jse-datasphere.jse_raw_financial_data.lu_currency_mapping`;

   -- Sample data from currency mapping table
   SELECT * 
   FROM `jse-datasphere.jse_raw_financial_data.lu_currency_mapping` 
   LIMIT 10;

   -- Check the period mapping table
   SELECT COUNT(*) as row_count 
   FROM `jse-datasphere.jse_raw_financial_data.lu_period_mapping`;

   -- Sample data from period mapping table
   SELECT * 
   FROM `jse-datasphere.jse_raw_financial_data.lu_period_mapping` 
   LIMIT 10;
   ```

## Troubleshooting

- If you encounter authentication errors, make sure you've run `gcloud auth login` and set the correct project
- If the CSV parsing fails, check that the file is correctly named and in the same directory as your script
- If the BigQuery load fails, check that you have appropriate permissions on the project and dataset

For additional help, contact your BigQuery administrator or refer to the Google Cloud documentation.
