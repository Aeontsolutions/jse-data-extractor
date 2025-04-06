import boto3
# --- Import google.generativeai ---
from google import genai
# Removed Vertex AI specific imports
# import google.cloud.aiplatform as aiplatform
# from vertexai.generative_models import GenerativeModel, GenerationConfig
import sqlite3
import asyncio
import os
import logging
import re
from datetime import datetime
import json
from dotenv import load_dotenv
import argparse
import io
import pandas as pd
from typing import List, Union, Literal # Keep for type hints in functions

# --- Configuration ---
load_dotenv()

# --- Environment Variable Name for GenAI Key ---
# Use GOOGLE_API_KEY by default, change if your .env uses something different
API_KEY_NAME = "GOOGLE_VERTEX_API_KEY"

# AWS Config (Unchanged)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "jse-renamed-docs"
S3_BASE_PREFIX = "CSV/"

# Google Cloud Config (Model Name Only) - Project/Location not needed for genai client
# Use the appropriate model name for the GenAI API (might differ from Vertex)
# Sticking with 1.5 flash for now, adjust if needed e.g., "gemini-1.5-flash-latest"
MODEL_NAME = "gemini-2.0-flash" # Adjusted model name potentially needed

# DB Config (Unchanged)
DB_NAME = "jse_financial_data.db"

# Logging Config (Unchanged)
LOG_FILE = "jse_extraction.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# --- Schema as Dictionary (Unchanged) ---
RESPONSE_SCHEMA_DICT = {
    "type": "OBJECT",
    "properties": {
        "metadata_predictions": {
            "type": "OBJECT",
            "description": "Metadata derived from the filename.",
            "properties": {
                "statement_type": {
                    "type": "STRING",
                    "description": "The type of financial statement derived from filename.",
                    "enum": ["Balance Sheet", "Income Statement", "Cash Flow Statement", "Comprehensive Income Statement"]
                },
                "period": {
                    "type": "STRING",
                    "description": "The reporting period ending quarter or fiscal year derived from filename.",
                    "enum": ["Q1", "Q2", "Q3", "FY"]
                },
                "group_or_company": {
                    "type": "STRING",
                    "description": "Whether the statement is for the Group or Company level derived from filename.",
                    "enum": ["group", "company"]
                },
                "trailing_zeros": {
                    "type": "STRING",
                    "description": "From the column headings, determine if trailing zeros should be added to the values. This can be Y or N.",
                    "enum": ["Y", "N"]
                },
                "report_date": {
                    "type": "STRING",
                    "description": "Taken from the filename but conformed to the format %Y-%MM-%D eg. 2024-11-30"
                }
            },
            "required": ["statement_type", "period", "group_or_company"]
        },
        "line_items": {
            "type": "ARRAY",
            "description": "A list of all extracted financial line items and their values for relevant periods.",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "line_item": {
                        "type": "STRING",
                        "description": "The descriptive label for the financial line item (e.g., 'Revenue', 'Total Assets')."
                    },
                    "value": {
                        "type": "Number",
                        "description": "The extracted value for the line item. Pay attention to any parentheses that would indicate negative values."
                    },
                    "period_length": {
                        "type": "STRING",
                        "description": "The length of the period this value covers, based on column header.",
                        "enum": ["3mo", "6mo", "9mo", "1y"]
                    }
                },
                "required": ["line_item", "value", "period_length"]
            }
        }
    },
    "required": ["metadata_predictions", "line_items"]
}


# --- Helper Functions (parse_date_from_filename, clean_value - Unchanged) ---
def parse_date_from_filename(filename):
    """
    Parses YYYY-MM-DD date from the end of the filename string.
    Expects the format -[MonthName]-[DD]-[YYYY].csv at the very end.
    """
    match = re.search(r'-([a-zA-Z]+)-(\d{1,2})-(\d{4})\.csv$', filename, re.IGNORECASE)
    if match:
        month_str, day_str, year_str = match.groups()
        try:
            day = int(day_str); year = int(year_str)
            month_map = {'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12}
            month = month_map.get(month_str.lower())
            if month: return datetime(year, month, day).date()
            else: logging.warning(f"Month parse fail '{month_str}' in {filename}"); return None
        except ValueError as e: logging.warning(f"Date component error {year_str}-{month_str}-{day_str} in {filename}: {e}"); return None
    else: logging.warning(f"Date pattern '-Month-DD-YYYY.csv' not found at end of {filename}"); return None

def clean_value(value_raw: Union[str, int, float, None]) -> Union[float, None]:
    """Cleans financial string/numeric value and converts to float."""
    if value_raw is None: return None
    if isinstance(value_raw, (int, float)): return float(value_raw)
    if not isinstance(value_raw, str): return None
    value_str = re.sub(r'\[[a-zA-Z0-9]+\]$', '', value_raw.strip())
    cleaned = value_str.replace(',', '').replace('$', '').replace(' ', '')
    is_negative = False
    if cleaned.startswith('(') and cleaned.endswith(')'): is_negative = True; cleaned = cleaned[1:-1]
    if cleaned.startswith('-'): is_negative = True
    try:
        if not cleaned: return None
        value_float = float(cleaned)
        if is_negative and value_float > 0: value_float *= -1
        elif is_negative and value_float == 0: value_float = 0.0
        return value_float
    except ValueError: return None

# --- list_csv_files async function (Unchanged) ---
async def list_csv_files(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    logging.info(f"Listing CSV files in s3://{bucket}/{prefix}")
    keys = []
    try:
        async def _paginate():
             pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
             for page in pages:
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.lower().endswith('.csv'): keys.append(key)
        await _paginate()
    except Exception as e: logging.error(f"S3 listing error {bucket}/{prefix}: {e}")
    logging.info(f"Found {len(keys)} CSV files in {prefix}.")
    return keys

# --- LLM Extraction Function (Using genai client + Detailed Prompt + Dict Schema) ---

# Takes the genai client instance now
async def extract_data_with_llm(genai_client: genai.Client, filename: str, csv_content: str):
    """Uses Gemini LLM (via genai client) with detailed prompt and response schema enforcement."""

    # *** Detailed prompt remains unchanged ***
    prompt = f"""
    You are an expert financial analyst AI tasked with extracting structured data from CSV financial statements from the Jamaica Stock Exchange (JSE).

    Analyze the provided CSV data and filename to extract metadata and financial line items according to the specified rules.
    Structure your response according to the provided schema configuration.

    **Filename:**
    `{filename}`

    **CSV Content:**
    ```csv
    {csv_content}
    ```

    **Instructions:**

    1.  **Metadata Extraction (Based ONLY on the filename):**
        *   `statement_type`: Determine the type of financial statement. Choose EXACTLY ONE from: ["Balance Sheet", "Income Statement", "Cash Flow Statement", "Comprehensive Income Statement"].
            *   Hints: 'financial_position' -> "Balance Sheet", 'income_statement' -> "Income Statement", 'comprehensive_income' -> "Comprehensive Income Statement", 'cash_flow' -> "Cash Flow Statement". If unsure, analyze headers in CSV content for keywords like Assets, Liabilities, Equity, Revenue, Expenses, Cash Flow from Operations.
        *   `period`: Determine the reporting period ending quarter or fiscal year. Choose EXACTLY ONE from: ["Q1", "Q2", "Q3", "FY"].
            *   Hints: 'three_months' -> "Q1", 'six_months' -> "Q2", 'nine_months' -> "Q3". If the file is from an 'audited_financial_statements' directory OR indicates 'year_ended', choose "FY". Assume standard quarter ends unless filename strongly implies otherwise (e.g., a non-standard year-end).
        *   `group_or_company`: Determine if the statement is for the Group or Company level. Choose EXACTLY ONE from: ["group", "company"].
            *   Hints: 'group' or 'consolidated' in filename -> "group". 'company' explicitly in filename (and not 'group'/'consolidated') -> "company". Default to "group" if ambiguous or neither term is present.
        * `trailing_zeros`: Determines if trailing zeros should be added to the line item value in downstream processing.
            * Hints: `Y` should be entered when there are trailing zeros, `N` otherwise. You should use the column headings as a hint. YOU SHOULD NOT ADD TRAILING ZEROS TO THE LINE ITEM VALUES ON YOUR OWN.
        * `report_date`: This is the date for the given report in the format %Y-%MM-%D
            * Hints: Use the file name to extract the report date. eg. 2024-11-30
    2.  **Line Item Extraction (Based on CSV Content):**
        *   Identify the primary reporting date implied by the filename.
        *   Focus ONLY on data columns corresponding to periods ending on or close to this date. These might be labeled like '3 months ended [date]', '9 months ended [date]', '[date]', etc.
        *   **IGNORE columns representing data from PRIOR YEARS or comparative periods significantly different from the main reporting date.**
        *   For each meaningful row in the CSV representing a financial line item:
            *   Extract the `line_item` description (the label, e.g., "Revenue", "Total Assets").
            *   For *each relevant data column* identified above:
                *   Extract the corresponding `value` as a string, preserving format like parentheses. The schema expects a STRING type for value.
                *   Determine the `period_length` covered by that specific value based on its column header. Choose EXACTLY ONE from: ["3mo", "6mo", "9mo", "1y"].
                    *   Hints: "3 months ended..." -> "3mo", "six months ended..." -> "6mo", "nine months ended..." -> "9mo". For annual/audited reports or columns simply labeled with the year/date -> "1y".
            *   Often there are headings that group together a bundle of line items. These headings, for example Current Assets, Current Liabilities, etc will include several line items under them alongside a sum at the end. 
                * * You should include all of the sub-line-items as well as the heading value (the heading value being the sum) in your extraction.
                * * Often these "sums" can be easily identified because they are left "dangling", meaning there is no corresponding line item on the same row. You will need to use your intuition to determine this.
                * * The headings can correspondingly be easily identified as well because they too will not include a line item value in the same row. In large, this will be a matching exercise.
    3. **Simple Rules to follows:**
        * Line item values should never be NULL. If you see a dash where a value should be you should enter 0 for the value.
        * You should pay attention to whether there are indications of whether or not a value is negative. This can be indicated by the use of parentheses for instance.
        * Use some common sense here and there - if the date indicated by the filename is 2024 for example, you should really only be paying attention to columns for 2024.
        * Whenever a column just says `Quarter Ended` without specifying a length of time you can assume that it is for a length of `3mo`. These data points should certainly be included.

    **Example Line Item Logic:**
    If a row 'Revenue' has values in columns '3 Months Ended Sep 2023' and '9 Months Ended Sep 2023', you should generate two entries in the `line_items` list for that row (one for "3mo", one for "9mo").
    """
    response = None # Initialize response to handle potential errors during API call
    try:
        # *** Define configuration dictionary for genai client ***
        config_dict = {
            "response_mime_type": "application/json",
            "response_schema": RESPONSE_SCHEMA_DICT, # Pass the dictionary schema
            # Add other config like temperature, top_p if needed
            # "temperature": 0.2,
        }

        # --- Run the synchronous genai call in a separate thread ---
        def sync_generate():
            # Use the passed genai_client instance
            return genai_client.models.generate_content(
                model=MODEL_NAME, # Use configured model name
                contents=[prompt], # Pass the detailed prompt
                config=config_dict # Pass the config dictionary
                # safety_settings=... # Add safety settings if needed
            )

        # Execute the synchronous call in asyncio's default executor (a thread pool)
        response = await asyncio.to_thread(sync_generate)
        # --- End of threaded execution ---

        # Access response text (should be JSON conforming to schema)
        # Check response structure (might need adjustments based on actual genai response)
        # Assuming response.text exists based on genai examples
        if not hasattr(response, 'text') or not response.text:
             # Handle cases where response might be blocked or empty
             logging.error(f"LLM response missing text content for {filename}. Blocked? {getattr(response, 'prompt_feedback', 'N/A')}")
             return None

        json_text = response.text
        data = json.loads(json_text) # Parse JSON

        return data # Return the parsed dictionary

    except json.JSONDecodeError as json_err:
         logging.error(f"LLM response not valid JSON for {filename}: {json_err}")
         logging.error(f"LLM Response Text: {getattr(response, 'text', 'N/A')}")
         return None
    except Exception as e:
        logging.error(f"LLM extraction or parsing failed for {filename}: {e}", exc_info=True)
        logging.error(f"LLM Response (if available): {getattr(response, 'text', 'N/A')}")
        # Log candidate/feedback details if available for better debugging genai issues
        try:
             if response:
                 logging.error(f"LLM Resp Candidates: {getattr(response, 'candidates', 'N/A')}")
                 logging.error(f"LLM Resp Feedback: {getattr(response, 'prompt_feedback', 'N/A')}")
        except Exception: pass
        return None


# --- CSV Processing Function (Passes genai_client now) ---
# Takes genai_client instead of vertex llm_client
async def process_csv(s3_key: str, s3_client, genai_client: genai.Client):
    """Processes a single CSV file: download, parse metadata, extract via LLM, structure."""
    filename = os.path.basename(s3_key)
    logging.info(f"Processing: {s3_key}")

    # 1. Parse Initial Metadata (Unchanged)
    try:
        parts = s3_key.split('/'); symbol = parts[1] if len(parts) > 2 and parts[0].upper() == 'CSV' else None
        if not symbol: logging.error(f"No symbol from path: {s3_key}"); return None
        period_type = 'annual' if 'audited_financial_statements' in s3_key else 'quarterly'
        # report_dt_obj = parse_date_from_filename(filename)
        # if not report_dt_obj: logging.error(f"Date parse error: {filename}"); return None
        # report_date_str = report_dt_obj.strftime('%Y-%m-%d'); report_year = report_dt_obj.year
    except Exception as e: logging.error(f"Metadata parse error {s3_key}: {e}"); return None

    # 2. Download and Prepare CSV Content (Unchanged)
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        csv_content_bytes = response['Body'].read()
        try: 
            csv_content_str = csv_content_bytes.decode('utf-8')
            simplified_csv_content = csv_content_str
        except UnicodeDecodeError:
            try: 
                csv_content_str = csv_content_bytes.decode('latin-1')
                simplified_csv_content = csv_content_str
            except Exception as decode_err: logging.error(f"Decode CSV failed {s3_key}: {decode_err}"); return None
        # try:
        #     df = pd.read_csv(io.StringIO(csv_content_str), header=None, skip_blank_lines=True)
        #     df.dropna(how='all', axis=0, inplace=True); df.dropna(how='all', axis=1, inplace=True)
        #     simplified_csv_content = df.to_csv(index=False, header=False)
        #     if not simplified_csv_content.strip(): logging.warning(f"CSV empty post-clean {s3_key}"); return None
        # except pd.errors.EmptyDataError: logging.warning(f"Pandas: Empty CSV {s3_key}"); return None
        # except Exception as pd_err: logging.error(f"Pandas error {s3_key}: {pd_err}. Using raw."); simplified_csv_content = csv_content_str
    except Exception as e: logging.error(f"S3/Pandas error {s3_key}: {e}"); return None

    # 3. Extract Data using LLM (Calls updated function with genai_client)
    llm_result = await extract_data_with_llm(genai_client, filename, simplified_csv_content) # Pass genai_client

    if not llm_result: return None # Error logged within extract func

    # 4. Structure Data for DB (Unchanged logic)
    records_for_db = []
    metadata = llm_result.get("metadata_predictions", {})
    line_items = llm_result.get("line_items", [])
    statement = metadata.get("statement_type")
    period = metadata.get("period")
    group_or_company = metadata.get("group_or_company")
    trailing_zeros = metadata.get("trailing_zeros")
    report_date = metadata.get("report_date")
    try:
        report_year = datetime.strptime(report_date, "%Y-%m-%d").strftime("%Y")
    except Exception as e:
        report_year = None
        logging.warning(f"Incorrect date format extracted {report_date} from {filename}")

    if not all([statement, period, group_or_company]):
         logging.warning(f"Incomplete metadata {filename}: {metadata}")
    
    print("==============")
    print(filename, statement, line_items)
    print("==============")
    
    for item in line_items:
        li_name = item.get("line_item"); li_value_raw = item.get("value"); li_period_length = item.get("period_length")
        if not li_name or li_value_raw is None or not li_period_length:
            logging.warning(f"Incomplete line item {filename}: {item}"); continue
        # li_value_float = clean_value(li_value_raw)
        # if li_value_float is None and li_value_raw not in ('', None):
        #      logging.debug(f"Value clean failed '{li_name}'='{li_value_raw}' in {filename}.")
        try:
            li_value_float = float(li_value_raw)
            li_value_float = li_value_float * 1000 if trailing_zeros else li_value_float
        except Exception as e:
            logging.debug(f"Value clean failed '{li_name}'='{li_value_raw}' in {filename}.")
        records_for_db.append({
            "symbol": symbol, "csv_path": s3_key, "statement": statement,
            "report_date": report_date, "year": report_year, "period": period,
            "period_type": period_type, "group_or_company_level": group_or_company,
            "line_item": str(li_name).strip(),
            "line_item_value": li_value_float,
            "period_length": li_period_length,
        })

    logging.info(f"Processed {filename}. Found {len(records_for_db)} records.")
    return records_for_db


# --- Database Saving (save_to_db - Unchanged) ---
def save_to_db(records: list, db_path: str):
    if not records: logging.info("No records to save."); return
    records_by_symbol = {}
    for r in records: records_by_symbol.setdefault(r['symbol'], []).append(r)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for symbol, symbol_records in records_by_symbol.items():
            table_name = f"jse_raw_{symbol}"
            logging.info(f"Saving {len(symbol_records)} records for '{symbol}' to '{table_name}'")
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, csv_path TEXT, statement TEXT, report_date DATE, year INTEGER, period TEXT, period_type TEXT, group_or_company_level TEXT, line_item TEXT, line_item_value REAL, period_length TEXT, extraction_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, UNIQUE(csv_path, line_item, period_length))""")
            insert_data = [(r['symbol'], r['csv_path'], r['statement'], r['report_date'], r['year'], r['period'], r['period_type'], r['group_or_company_level'], r['line_item'], r['line_item_value'], r['period_length']) for r in symbol_records]
            cursor.executemany(f"""INSERT INTO {table_name} (symbol, csv_path, statement, report_date, year, period, period_type, group_or_company_level, line_item, line_item_value, period_length) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(csv_path, line_item, period_length) DO UPDATE SET statement=excluded.statement, report_date=excluded.report_date, year=excluded.year, period=excluded.period, period_type=excluded.period_type, group_or_company_level=excluded.group_or_company_level, line_item_value=excluded.line_item_value, extraction_timestamp=CURRENT_TIMESTAMP""", insert_data)
        conn.commit()
        logging.info(f"Saved data for {len(records_by_symbol)} symbols.")
    except sqlite3.Error as e: logging.error(f"DB error: {e}"); conn and conn.rollback()
    finally: conn and conn.close()

# --- Worker Function (Passes genai_client now) ---
# Takes genai_client instead of vertex llm_client
async def worker(semaphore, key, s3_client, genai_client: genai.Client):
    """Worker wrapper to acquire semaphore before processing."""
    async with semaphore:
        # Call the actual processing function, passing the genai_client
        return await process_csv(key, s3_client, genai_client)


# --- Main Orchestration (Initializes and passes genai_client) ---
async def main(symbol_arg=None):
    logging.info("Starting JSE Data Extraction Process with GenAI Client...")
    # --- Initialize S3 Client (Unchanged) ---
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
    s3_client = session.client('s3')

    # --- Initialize GenAI Client ---
    try:
        api_key = os.getenv(API_KEY_NAME)
        if not api_key:
            raise ValueError(f"Environment variable {API_KEY_NAME} not set.")
        # Use genai.configure or genai.Client
        # genai.configure(api_key=api_key)
        genai_client = genai.Client(api_key=api_key)
        # Optional: List models to verify connection/key
        # for m in genai.list_models(): print(m.name)
        logging.info(f"Google GenAI Client initialized for model {MODEL_NAME}")
    except Exception as e: logging.error(f"Google GenAI Client init failed: {e}"); return

    # --- Symbol Discovery (Unchanged) ---
    symbols_to_process = []
    if symbol_arg: symbols_to_process.append(symbol_arg.upper()); logging.info(f"Processing symbol: {symbol_arg}")
    else:
        logging.info(f"Listing symbols in s3://{S3_BUCKET}/{S3_BASE_PREFIX}")
        paginator = s3_client.get_paginator('list_objects_v2')
        try:
            response_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_BASE_PREFIX, Delimiter='/')
            for page in response_iterator:
                 for prefix_data in page.get('CommonPrefixes', []):
                    prefix = prefix_data.get('Prefix'); symbol = prefix.replace(S3_BASE_PREFIX, '', 1).strip('/') if prefix else None; symbol and symbols_to_process.append(symbol.upper())
            logging.info(f"Found symbols: {symbols_to_process}")
            if not symbols_to_process: logging.warning("No symbols found."); return
        except Exception as e: logging.error(f"S3 symbol listing error: {e}"); return

    # --- CSV Key Gathering (Unchanged) ---
    all_csv_keys = []; list_tasks = [list_csv_files(s3_client, S3_BUCKET, f"{S3_BASE_PREFIX}{symbol}/") for symbol in symbols_to_process]
    results = await asyncio.gather(*list_tasks); [all_csv_keys.extend(keys) for keys in results]
    if not all_csv_keys: logging.warning("No CSV files found."); return
    logging.info(f"Found {len(all_csv_keys)} total CSV files.")

    # --- Semaphore Controlled Processing (Passes genai_client) ---
    CONCURRENCY_LIMIT = 20
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    logging.info(f"Processing CSV files with concurrency limit: {CONCURRENCY_LIMIT}")

    # Create worker tasks, passing the genai_client
    processing_tasks = [
        worker(semaphore, key, s3_client, genai_client) # Pass genai_client here
        for key in all_csv_keys
    ]

    # Run tasks concurrently (Unchanged)
    task_results = await asyncio.gather(*processing_tasks)

    # --- Result Aggregation (Unchanged) ---
    all_records = []; actual_failed_count = task_results.count(None); actual_success_count = len(task_results) - actual_failed_count
    for result in task_results: result is not None and all_records.extend(result)
    logging.info(f"Finished. Success: {actual_success_count} files. Failed/Skipped: {actual_failed_count} files.")
    logging.info(f"Total extracted records: {len(all_records)}")

    # --- Database Saving (Unchanged) ---
    save_to_db(all_records, DB_NAME)
    logging.info("JSE Data Extraction Process Finished.")


# --- Entry Point (Unchanged) ---
if __name__ == "__main__":
    # Ensure google-generativeai is installed: pip install google-generativeai
    parser = argparse.ArgumentParser(description="Extract JSE financial data from S3 CSVs using the Google GenAI API.")
    parser.add_argument("-s", "--symbol", help="Specify a single equity symbol to process.", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(main(symbol_arg=args.symbol))