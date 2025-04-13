#!/bin/bash
# This script loads the CSV files into BigQuery

# Set your project ID
PROJECT_ID="jse-datasphere"
DATASET_ID="jse_raw_financial_data"

echo "Creating tables in BigQuery..."

# Create lu_currency_mapping table
bq query --use_legacy_sql=false \
"CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_ID}.lu_currency_mapping\` (
  symbol STRING,
  currency STRING,
  date STRING
)"

# Create lu_period_mapping table
bq query --use_legacy_sql=false \
"CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_ID}.lu_period_mapping\` (
  symbol STRING,
  period STRING,
  report_date STRING,
  year_end STRING
)"

echo "Loading data into BigQuery..."

# Load lu_currency_mapping table
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  ${PROJECT_ID}:${DATASET_ID}.lu_currency_mapping \
  lu_currency_mapping.csv \
  symbol:STRING,currency:STRING,date:STRING

# Load lu_period_mapping table
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  ${PROJECT_ID}:${DATASET_ID}.lu_period_mapping \
  lu_period_mapping.csv \
  symbol:STRING,period:STRING,report_date:STRING,year_end:STRING

echo "Data loaded successfully into BigQuery!"

# Verify row counts
echo "Verifying data..."
bq query --use_legacy_sql=false \
"SELECT 'lu_currency_mapping' as table_name, COUNT(*) as row_count 
FROM \`${PROJECT_ID}.${DATASET_ID}.lu_currency_mapping\`
UNION ALL
SELECT 'lu_period_mapping' as table_name, COUNT(*) as row_count 
FROM \`${PROJECT_ID}.${DATASET_ID}.lu_period_mapping\`"