# Pipeline Validation Scripts

This directory contains scripts for validating and inspecting the JSE data pipeline.

## Scripts

### 1. list_s3_csvs.py

Lists all CSV files in the S3 bucket for a specific symbol, showing:
- Filename
- Last modified date/time
- File size in MB

**Usage:**
```bash
python list_s3_csvs.py -s SYMBOL
```

**Example:**
```bash
python list_s3_csvs.py -s JPS
```

**Use this script when you want to:**
- Quickly check what files exist in S3 for a symbol
- See when files were last modified
- Check file sizes
- Debug S3 access issues

### 2. validate_pipeline.py

Validates that all S3 files for a symbol have been properly processed and loaded into BigQuery. The script:
- Gets all CSV files from S3 for the specified symbol
- Gets all files referenced in the corresponding BigQuery table
- Compares the two sets to find:
  - Files that are in S3 but missing from BigQuery (not processed)
  - Files that are in BigQuery but not in S3 (potential duplicates or errors)

**Usage:**
```bash
python validate_pipeline.py -s SYMBOL
```

**Example:**
```bash
python validate_pipeline.py -s JPS
```

**Use this script when you want to:**
- Verify that your data pipeline is working correctly
- Ensure data integrity between S3 and BigQuery
- Find missing or extra files in the processed data
- Perform data quality assurance

## Requirements

Both scripts require:
- AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
- Google Cloud credentials for BigQuery access
- Python packages:
  - boto3
  - google-cloud-bigquery
  - pandas
  - python-dotenv

## Configuration

The scripts use the following configuration:
- S3 Bucket: `jse-renamed-docs`
- S3 Base Prefix: `CSV/`
- BigQuery Project: `jse-datasphere`
- BigQuery Dataset: `jse_raw_financial_data_dev_elroy` 