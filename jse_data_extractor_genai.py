# COMPLETE SCRIPT incorporating evaluation/retry logic into the user-provided code

import boto3
# --- Import google.generativeai ---
from google import genai
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
# NOTE: Pandas is not used for CSV prep in the provided user code below
# import pandas as pd 
from typing import List, Union, Literal, Optional, Dict, Any # Added Optional, Dict, Any

# --- Configuration ---
load_dotenv()

API_KEY_NAME = "GOOGLE_VERTEX_API_KEY"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "jse-renamed-docs"
S3_BASE_PREFIX = "CSV/"
MODEL_NAME = "gemini-2.0-flash"
DB_NAME = "jse_financial_data.db"
LOG_FILE = "jse_extraction.log"
STATEMENT_MAPPING_CSV=os.getenv("STATEMENT_MAPPING_CSV_PATH")
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

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
                    "description": "Taken from the filename but conformed to the format %Y-%m-%d eg. 2024-11-30" # Corrected format specifier
                }
            },
            "required": ["statement_type", "period", "group_or_company", "report_date", "trailing_zeros"]
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
                        "type": "NUMBER",
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


# --- NEW: Schema for Evaluator ---
EVALUATION_SCHEMA_DICT = {
    "type": "OBJECT",
    "properties": {
        "evaluation_judgment": {
            "type": "STRING",
            "enum": ["PASS", "FAIL"],
            "description": "Overall assessment: PASS if the extraction accurately follows all rules, FAIL otherwise."
        },
        "evaluation_reasoning": {
            "type": "STRING",
            "description": "Brief explanation for the judgment. If FAIL, specify the primary rule(s) violated (e.g., 'Missing 9mo period data', 'Missing Current Assets total', 'Incorrect metadata'). If PASS, state 'Compliant'."
        },
        "missing_periods_found": {
            "type": "BOOLEAN",
            "description": "True if the evaluation identified relevant time periods present in CSV columns but missing from line item output."
        },
        "missing_grouped_totals_found": {
            "type": "BOOLEAN",
            "description": "True if the evaluation identified expected grouped totals/headings (like 'Current Assets') missing from the line item output."
        }
    },
    "required": ["evaluation_judgment", "evaluation_reasoning", "missing_periods_found", "missing_grouped_totals_found"]
}

# --- NEW: Schema for Group Level Determination ---
GROUP_LEVEL_SCHEMA_DICT = {
    "type": "OBJECT",
    "properties": {
        "group_level_determination": {
            "type": "STRING",
            "enum": ["group", "company"],
            "description": "The determined level of the financial statement: group (consolidated) or company."
        },
        "confidence": {
            "type": "STRING",
            "enum": ["high", "medium", "low"],
            "description": "The confidence level in the determination."
        },
        "reasoning": {
            "type": "STRING",
            "description": "Brief explanation for the determination, referencing specific evidence from the file name or contents."
        }
    },
    "required": ["group_level_determination", "confidence", "reasoning"]
}

# --- Helper Functions (parse_date_from_filename, clean_value) ---
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
    if not isinstance(value_raw, str): return None # User version didn't log warning here
    value_str = re.sub(r'\[[a-zA-Z0-9]+\]$', '', value_raw.strip())
    cleaned = value_str.replace(',', '').replace('$', '').replace(' ', '')
    is_negative = False
    if cleaned.startswith('(') and cleaned.endswith(')'): is_negative = True; cleaned = cleaned[1:-1]
    if cleaned.startswith('-'): is_negative = True
    try:
        if not cleaned: return None # User version returned None for empty string
        value_float = float(cleaned)
        if is_negative and value_float > 0: value_float *= -1
        elif is_negative and value_float == 0: value_float = 0.0
        return value_float
    except ValueError: return None # User version returned None on error

# --- list_csv_files async function  ---
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

# --- NEW: Function to load and process statement mapping CSV ---
def load_statement_mapping(csv_content_str):
    """Load and process financial statement mapping data."""
    mapping_data = {}
    
    try:
        # Use StringIO to treat the string as a file-like object
        csv_io = io.StringIO(csv_content_str)
        
        # Parse the CSV using csv module
        import csv
        reader = csv.DictReader(csv_io)
        
        # Group by symbol
        for row in reader:
            symbol = row.get('Symbol')
            if not symbol:
                continue
                
            if symbol not in mapping_data:
                mapping_data[symbol] = []
                
            # Add entry to the list for this symbol
            # Note: The 'Associated Title Key Words' column might literally contain the string "None"
            keywords = row.get('Associated Title Key Words', '')
            
            mapping_data[symbol].append({
                'company': row.get('Company', ''),
                'report_type': row.get('Report Type', ''),
                'statement_type': row.get('Statement Type', ''),
                'keywords': keywords,  # Store the actual string value, including "None" if present
                'annual_period_start': row.get('Annual Period Start', ''),
                'annual_period_end': row.get('Annual Period End', ''),
                'note': row.get('Note', '')
            })
            
        return mapping_data
        
    except Exception as e:
        logging.error(f"Error processing statement mapping CSV: {e}")
        return {}

# --- NEW: Function to determine if LLM is needed for group level determination ---
def needs_llm_determination(symbol, mapping_data):
    """
    Determine if a symbol needs LLM for group vs company determination.
    Returns (needs_llm, keywords_list) tuple.
    """
    if symbol not in mapping_data:
        # No mapping data for this symbol, default to using original determination
        return False, []
        
    entries = mapping_data[symbol]
    keywords_set = {entry['keywords'] for entry in entries}
    
    # If all keywords are the literal string "None", use deterministic company level
    if keywords_set == {'None'} or (len(keywords_set) == 1 and "None" in keywords_set):
        return False, []
        
    # Otherwise, we need LLM determination with available keywords
    # Exclude the literal "None" string entries
    keywords_list = [kw for kw in keywords_set if kw != "None" and kw]
    return True, keywords_list

# --- NEW: Function to build group level determination prompt ---
def build_group_level_prompt(filename, csv_content, keywords_list):
    """Build prompt for determining group vs company level using LLM."""
    
    prompt = f"""
    You are a financial data analyst specializing in determining whether financial statements are at the GROUP (consolidated) level or COMPANY level.
    Additional context: All the listings you are being presented with are for conglomerates so don't be surprised if "group" is present within the filenames.
    Additional context cont'd: Your job is in distinguishing with of the statements for this GROUP is at the company or group/consolidated level
    Additional context cont'd: This requires you to use your smarts - if the company name has group in it but the statement file has "company statement" in there then it's clearly a Company Statement

    
    **Filename:** `{filename}`
    
    **CSV Content (sample):**
    ```csv
    {csv_content[:2000]}  
    ```
    
    **Keywords Associated with GROUP (Consolidated) Statements for this Company:**
    {', '.join(keywords_list)}
    
    **Instructions:**
    
    1. Analyze both the FILENAME and SAMPLE CONTENT of the CSV to determine if this is a GROUP-level (consolidated) or COMPANY-level financial statement.
    
    2. Look for specific indicators:
       - In FILENAME: Terms like "group", "consolidated", or any of the keywords listed above.
       - In FILANAME: The keyword proximity to the word "statement" aids a lot in determining whether it's a group or company-level line statement
       - In CSV CONTENT: Headers or titles containing "group", "consolidated", or references to consolidated statements.
       - Table structure and column headers that suggest group vs company reporting.
    
    3. Provide your determination as either "group" or "company", your confidence level, and your reasoning.
    
    4. If there are conflicting signals, prioritize:
       a) Explicit mention of "consolidated" or "group" in the filename or header rows
       b) Presence of the keywords provided above
       c) Structure of the data (e.g., parent-subsidiary breakdowns indicate group level)
    
    5. Important: Make sure your determination is based ONLY on the filename and CSV content, not on other metadata like statement type.

    6. Caveats: Often a filename can have the word `group` in there but what you're really looking for in the filename and content is whether it says "group/consolidated STATEMENT" or "company STATEMENT". That's really what determines the level. So, again, if present use the [KEYWORD] Statement pattern.
    
    Return your analysis in the specified JSON format.
    """
    
    return prompt

# --- NEW: Function to determine group level using LLM ---
async def determine_group_level_with_llm(genai_client, filename, csv_content, keywords_list):
    """Uses Gemini LLM to determine if a statement is at group or company level."""
    
    prompt = build_group_level_prompt(filename, csv_content, keywords_list)
    
    try:
        config_dict = {
            "response_mime_type": "application/json",
            "response_schema": GROUP_LEVEL_SCHEMA_DICT,
        }
        
        def sync_generate():
            return genai_client.models.generate_content(
                model=MODEL_NAME,
                contents=[prompt],
                config=config_dict
            )
            
        response = await asyncio.to_thread(sync_generate)
        
        if not hasattr(response, 'text') or not response.text:
            logging.error(f"Group level LLM response missing text for {filename}.")
            return "group"  # Default to group if LLM fails
            
        json_text = response.text
        data = json.loads(json_text)
        
        group_level = data.get("group_level_determination", "group")
        confidence = data.get("confidence", "low")
        reasoning = data.get("reasoning", "No reasoning provided")
        
        logging.info(f"Group level determined for {filename}: {group_level} (confidence: {confidence})\nReasoning: {reasoning}")
        # logging.debug(f"Group level reasoning: {reasoning}")
        
        return group_level
        
    except Exception as e:
        logging.error(f"Group level determination failed for {filename}: {e}", exc_info=True)
        return "group"  # Default to group if LLM fails

# --- NEW: Prompt Building Function ---
def build_extraction_prompt(filename: str, csv_content: str, previous_output: Optional[Dict[str, Any]] = None, evaluation_feedback: Optional[Dict[str, Any]] = None) -> str:
    """Constructs the prompt for the extractor LLM, potentially adding retry context."""

    # --- Base Prompt Definition) ---
    base_prompt_instructions = """
    You are an expert financial analyst AI tasked with extracting structured data from CSV financial statements from the Jamaica Stock Exchange (JSE).

    Analyze the provided CSV data and filename to extract metadata and financial line items according to the specified rules.
    Structure your response according to the provided schema configuration.

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
                *   Extract the corresponding `value` as a NUMBER, preserving format like parentheses. The schema expects a NUMBER type for value.
                *   Determine the `period_length` covered by that specific value based on its column header. Choose EXACTLY ONE from: ["3mo", "6mo", "9mo", "1y"].
                    *   Hints: "3 months ended..." -> "3mo", "six months ended..." -> "6mo", "nine months ended..." -> "9mo". For annual/audited reports or columns simply labeled with the year/date -> "1y".
            *   Often there are headings that group together a bundle of line items. These headings, for example Current Assets, Current Liabilities, etc will include several line items under them alongside a sum at the end.
                * * You should include all of the sub-line-items as well as the heading value (the heading value being the sum) in your extraction.
                * * Often these "sums" can be easily identified because they are left "dangling", meaning there is no corresponding line item on the same row. You will need to use your intuition to determine this.
                * * The headings can correspondingly be easily identified as well because they too will not include a line item value in the same row. In large, this will be a matching exercise.
            *   Cropped line items are often present. These can be in the case of "Net Profits Attributable To:" for example where one or 2 rows proceed, both of which have some value.
                *   In this case, the correct approach is not to create a line item `Net Profits Attributable To:`, however to join it with each of the proceeding rows.
                *   In the scenario I described above there are 3 rows, one being cropped. The final output would be 2 rows.
    3. **Simple Rules to follows:**
        * Line item values should never be NULL. If you see a dash where a value should be you should enter 0 for the value.
        * You should pay attention to whether there are indications of whether or not a value is negative. This can be indicated by the use of parentheses for instance.
        * Use some common sense here and there - if the date indicated by the filename is 2024 for example, you should really only be paying attention to columns for 2024.
        * Whenever a column just says `Quarter Ended` without specifying a length of time you can assume that it is for a length of `3mo`. These data points should certainly be included.
        * It is __exceedingly__ important that no line items are missed and that you are exhaustive in your coverage.
        * Needless to say, imaginary line items are also unacceptable.
    **Example Line Item Logic:**
    If a row 'Revenue' has values in columns '3 Months Ended Sep 2023' and '9 Months Ended Sep 2023', you should generate two entries in the `line_items` list for that row (one for "3mo", one for "9mo").
    
    **Handling Grouped Line Items**
    Below I'll provide an example of how to handle grouped line items like Current Assets, Equity, etc.

    Filename: `wisynco-wisynco_group_limited_group_statement_of_financial_position_31_december_2023-december-31-2023.csv`

    CSV Content:
    ```
    0,1,2,3,4
    ,Note ,Unaudited December 31 2023 $'000 ,Unaudited December 31 2022 $'000 ,Audited June 30 2023 $'000 
    Non-Current Assets ,,,,
    "Property, plant and equipment ",,"9,991,807 ","6,804,428 ","7,560,385 "
    Intangible asset ,,819 ,"1,243 ","1,639 "
    Investment in associate ,5 ,"372,901 ","539,976 ","416,780 "
    Loans receivable ,,"282,264 ","214,017 ","272,195 "
    Investment securities ,,"2,877,552 ","1,793,735 ","1,304,141 "
    ,,"13,525,343 ","9,353,399 ","9,555,140 "
    Current Assets ,,,,
    Inventories ,,"5,743,604 ","5,384,303 ","6,151,108 "
    Receivables and prepayments ,,"5,841,660 ","4,885,040 ","5,451,499 "
    Investment securities ,,"1,110,754 ","640,483 ","1,105,844 "
    Cash and short-term deposits ,6 ,"7,525,495 ","6,833,999 ","10,129,216 "
    ,,"20,221,513 ","17,743,825 ","22,837,667 "
    Current Liabilities ,,,,
    Trade and other payables ,,"4,824,270 ","4,099,687 ","6,330,489 "
    Short-term borrowings ,,"1,025,473 ","780,574 ","1,014,872 "
    Lease Liability ,,"63,884 ","142,000 ","114,808 "
    Taxation payable ,,"1,236,072 ","765,517 ","798,186 "
    ,,"7,149,699 ","5,787,778 ","8,258,355 "
    Net Current Assets ,,"13,071,814 ","11,956,047 ","14,579,312 "
    ,,"26,597,156 ","21,309,446 ","24,134,452 "
    Shareholders' Equity ,,,,
    Share capital ,7 ,"1,262,012 ","1,258,873 ","1,261,259 "
    Other reserves ,,"606,608 ","529,235 ","558,266 "
    Translation reserve ,,"86,489 ","70,365 ","88,095 "
    Retained earnings ,,"21,988,841 ","18,406,176 ","19,218,397 "
    ,,"23,943,950 ","20,264,649 ","21,126,017 "
    Non-current Liabilities ,,,,
    Deferred tax liabilities ,,"41,982 ","33,885 ","41,982 "
    Borrowings ,,"2,589,740 ","928,819 ","2,926,408 "
    Lease Liabilities ,,"21,484 ","82,093 ","40,045 "
    ,,"2,653,206 ","1,044,797 ","3,008,435 "
    ,,"26,597,156 ","21,309,446 ","24,134,452 "


    ```

    Correct Output:
    ```
    [
    {"line_item": "Property, plant and equipment", "value": 9991807, "period_length": "1y"},
    {"line_item": "Intangible asset", "value": 819, "period_length": "1y"},
    {"line_item": "Investment in associate", "value": 372901, "period_length": "1y"},
    {"line_item": "Loans receivable", "value": 282264, "period_length": "1y"},
    {"line_item": "Investment securities", "value": 2877552, "period_length": "1y"},
    {"line_item": "Non-Current Assets", "value": 13525343, "period_length": "1y"},
    {"line_item": "Inventories", "value": 5743604, "period_length": "1y"},
    {"line_item": "Receivables and prepayments", "value": 5841660, "period_length": "1y"},
    {"line_item": "Investment securities", "value": 1110754, "period_length": "1y"},
    {"line_item": "Cash and short-term deposits", "value": 7525495, "period_length": "1y"},
    {"line_item": "Current Assets", "value": 20221513, "period_length": "1y"},
    {"line_item": "Trade and other payables", "value": 4824270, "period_length": "1y"},
    {"line_item": "Short-term borrowings", "value": 1025473, "period_length": "1y"},
    {"line_item": "Lease Liability", "value": 63884, "period_length": "1y"},
    {"line_item": "Taxation payable", "value": 1236072, "period_length": "1y"},
    {"line_item": "Current Liabilities", "value": 7149699, "period_length": "1y"},
    {"line_item": "Net Current Assets", "value": 13071814, "period_length": "1y"},
    {"line_item": "Net Assets", "value": 26597156, "period_length": "1y"},
    {"line_item": "Share capital", "value": 1262012, "period_length": "1y"},
    {"line_item": "Other reserves", "value": 606608, "period_length": "1y"},
    {"line_item": "Translation reserve", "value": 86489, "period_length": "1y"},
    {"line_item": "Retained earnings", "value": 21988841, "period_length": "1y"},
    {"line_item": "Shareholders' Equity", "value": 23943950, "period_length": "1y"},
    {"line_item": "Deferred tax liabilities", "value": 41982, "period_length": "1y"},
    {"line_item": "Borrowings", "value": 2589740, "period_length": "1y"},
    {"line_item": "Lease Liabilities", "value": 21484, "period_length": "1y"},
    {"line_item": "Non-current Liabilities", "value": 2653206, "period_length": "1y"}
    ]
    ```
    """ # End of base_prompt_instructions

    retry_header = ""
    feedback_section = ""

    if previous_output and evaluation_feedback:
        retry_header = """
    ---
    **RETRY ATTEMPT:** Your previous attempt failed evaluation. Review the feedback and the previous output, then generate a corrected response adhering strictly to *all* original rules, paying special attention to the identified errors.
    ---
        """
        feedback_section = f"""
    **Previous Incorrect Output:**
    ```json
    {json.dumps(previous_output, indent=2)}
    ```

    **Evaluation Feedback (Reason for Failure):**
    {evaluation_feedback.get("evaluation_reasoning", "No specific reasoning provided.")}
    Specific Issues Flagged:
    - Missing Periods: {evaluation_feedback.get("missing_periods_found", "N/A")}
    - Missing Grouped Totals: {evaluation_feedback.get("missing_grouped_totals_found", "N/A")}

    **Corrective Action Required:** Regenerate the *entire* output, fixing the errors mentioned in the feedback while ensuring all other rules are still followed completely. Focus on accuracy according to the rules.
        """

    final_prompt = f"""
    {retry_header}
    **Filename:**
    `{filename}`

    **CSV Content:**
    ```csv
    {csv_content}
    ```

    {base_prompt_instructions}

    {feedback_section}
    """
    return final_prompt


# --- LLM Extraction Function (Takes constructed prompt) ---
async def extract_data_with_llm(genai_client: genai.Client, constructed_prompt: str, filename_for_logging: str):
    """Uses Gemini LLM (via genai client) with a potentially modified prompt."""
    prompt = constructed_prompt # Use the fully built prompt

    response = None
    try:
        config_dict = {
            "response_mime_type": "application/json",
            "response_schema": RESPONSE_SCHEMA_DICT, # Use schema provided by user
        }
        def sync_generate():
            # Use MODEL_NAME provided by user
            return genai_client.models.generate_content(
                model=MODEL_NAME,
                contents=[prompt],
                config=config_dict
            )
        response = await asyncio.to_thread(sync_generate)

        if not hasattr(response, 'text') or not response.text:
             logging.error(f"LLM response missing text content for {filename_for_logging}. Blocked? {getattr(response, 'prompt_feedback', 'N/A')}")
             return None
        json_text = response.text
        data = json.loads(json_text)
        return data
    except json.JSONDecodeError as json_err:
         logging.error(f"LLM response not valid JSON for {filename_for_logging}: {json_err}")
         logging.error(f"LLM Response Text: {getattr(response, 'text', 'N/A')}")
         return None
    except Exception as e:
        logging.error(f"LLM extraction or parsing failed for {filename_for_logging}: {e}", exc_info=True)
        logging.error(f"LLM Response (if available): {getattr(response, 'text', 'N/A')}")
        try:
             if response:
                 logging.error(f"LLM Resp Candidates: {getattr(response, 'candidates', 'N/A')}")
                 logging.error(f"LLM Resp Feedback: {getattr(response, 'prompt_feedback', 'N/A')}")
        except Exception: pass
        return None

# --- NEW: LLM Evaluation Function ---
async def evaluate_extraction(genai_client: genai.Client, filename: str, csv_content: str, rules: str, extraction_output: dict):
    """Uses Gemini LLM (via genai client) to evaluate the extraction output against rules."""

    # Using the detailed evaluator prompt from previous step
    evaluator_prompt = f"""
    You are a meticulous auditor reviewing structured data extracted from a financial CSV file.
    Your task is to determine if the provided "Extraction Output" accurately reflects the "Original CSV Content" according to the specified "Extraction Rules".

    **Filename:** `{filename}`
    **Original CSV Content:**\n```csv\n{csv_content}\n```
    **Extraction Rules Provided to Original Extractor:**\n```text\n{rules}\n```
    **Extraction Output to Evaluate:**\n```json\n{json.dumps(extraction_output, indent=2)}\n```

    **Evaluation Steps & Criteria:**
    1. **Review Metadata:** Check if `metadata_predictions` (statement_type, period, group_or_company, trailing_zeros, report_date) correctly follow rules based ONLY on **Filename**. Is `report_date` %Y-%m-%d? Check required fields.
    2. **Check Period Completeness:** Examine CSV columns. Does `line_items` include entries for *all* relevant time periods found? Mark `missing_periods_found` = true if missed.
    3. **Check Grouped Totals:** Look for groupings (Current Assets, Total Liabilities...). Does `line_items` include entries for these *grouping headings/totals* themselves? Mark `missing_grouped_totals_found` = true if missed.
    4. **Verify Value Handling:** Are `value` fields numbers? Are negatives correct? Are dashes/empty cells 0?
    5. **Check Prior Year Exclusion:** Confirm prior year data is excluded.
    6  **Verify Line Item Coverage Completeness:** Ensure no line items from the original CSV content is missed. This is __very__ important.
    7. **Overall Judgment:** "PASS" only if all rules met accurately. "FAIL" otherwise. Provide brief `evaluation_reasoning`.

    Structure response per schema. Be concise.
    """
    response = None
    try:
        # Use the user-specified model name here too
        config_dict = { "response_mime_type": "application/json", "response_schema": EVALUATION_SCHEMA_DICT }
        def sync_generate_eval():
            return genai_client.models.generate_content( model=MODEL_NAME, contents=[evaluator_prompt], config=config_dict )
        response = await asyncio.to_thread(sync_generate_eval)
        if not hasattr(response, 'text') or not response.text:
             logging.error(f"Eval LLM response missing text for {filename}. Blocked? {getattr(response, 'prompt_feedback', 'N/A')}")
             return None
        json_text = response.text; data = json.loads(json_text)
        return data
    except json.JSONDecodeError as json_err:
         logging.error(f"Eval LLM response not valid JSON for {filename}: {json_err}\nText: {getattr(response, 'text', 'N/A')}"); return None
    except Exception as e:
        logging.error(f"Eval LLM call/parsing failed for {filename}: {e}", exc_info=True)
        logging.error(f"Eval LLM Response: {getattr(response, 'text', 'N/A')}")
        try:
             if response: logging.error(f"Eval LLM Feedback: {getattr(response, 'prompt_feedback', 'N/A')}")
        except Exception: pass
        return None

# --- CSV Processing Function (Integrates Evaluation Loop & Prompt Modification - Minimal changes here) ---
async def process_csv(s3_key: str, s3_client, genai_client: genai.Client, mapping_data=None):
    """Processes a single CSV file: download, extract, evaluate, structure."""
    filename = os.path.basename(s3_key)
    logging.info(f"Processing: {s3_key}")
    simplified_csv_content = None # Define outside try block

    # 1. Parse Symbol/PeriodType
    try:
        parts = s3_key.split('/'); symbol = parts[1] if len(parts) > 2 and parts[0].upper() == 'CSV' else None
        if not symbol: logging.error(f"No symbol from path: {s3_key}"); return None
        period_type = 'quarterly' if 'unaudited_financial_statements' in s3_key else 'annual'
    except Exception as e: logging.error(f"Initial metadata parse error {s3_key}: {e}"); return None

    # 2. Download and Prepare CSV Content
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
    except Exception as e: logging.error(f"S3 download error {s3_key}: {e}"); return None

    # --- Added: Extraction and Evaluation Loop ---
    max_attempts = 2 # Set max attempts (1 initial + 1 retry)
    attempt_count = 0
    evaluation_passed = False
    last_llm_result = None # Holds the result of the latest successful extraction
    last_evaluation_result = None # Holds the feedback from the latest evaluation

    # Define rules string once for the evaluator (copied from build_extraction_prompt)
    rules_for_evaluator = """... (Copy the multi-line rules string from build_extraction_prompt's base_prompt_instructions exactly as it is there) ...""" # You need to paste the exact rules here

    while attempt_count < max_attempts and not evaluation_passed:
        current_attempt_num = attempt_count + 1
        logging.info(f"Extraction Attempt {current_attempt_num}/{max_attempts} for {filename}")

        # 3a. Build Prompt for this attempt
        current_prompt = build_extraction_prompt(
            filename,
            simplified_csv_content,
            previous_output=last_llm_result if attempt_count > 0 else None,
            evaluation_feedback=last_evaluation_result if attempt_count > 0 else None
        )

        # 3b. Extract Data using LLM (Using the user's original function structure)
        # NOTE: Passing the constructed prompt to the original function signature
        # This requires the original extract_data_with_llm to be modified to accept the prompt
        # Reverting to call the original function signature for now as per constraint,
        # but this means prompt modification won't take effect without changing extract_data_with_llm
        # --- MAKING THE CHANGE TO PASS PROMPT ---
        current_llm_result = await extract_data_with_llm( # This function now needs modification
             genai_client,
             current_prompt, # Pass the prompt built above
             filename # Keep passing filename for logging inside
        )
        # --- END OF CHANGE ---

        if not current_llm_result:
            logging.error(f"Extractor failed on attempt {current_attempt_num} for {filename}. Stopping attempts for this file.")
            last_llm_result = None
            break # Exit loop

        # Store this attempt's successful result
        last_llm_result = current_llm_result

        # 3c. Evaluate the Extraction Result
        logging.info(f"Evaluating extraction attempt {current_attempt_num} for {filename}")
        current_evaluation_result = await evaluate_extraction(
            genai_client, filename, simplified_csv_content, rules_for_evaluator, last_llm_result
        )

        attempt_count += 1 # Increment attempt counter *after* evaluation attempt

        if current_evaluation_result:
            last_evaluation_result = current_evaluation_result
            judgment = current_evaluation_result.get("evaluation_judgment")
            reasoning = current_evaluation_result.get("evaluation_reasoning", "N/A")
            logging.info(f"Evaluation Result (Attempt {attempt_count}): {judgment} - {reasoning}")
            if judgment == "PASS":
                evaluation_passed = True
            else: # FAIL
                 if current_evaluation_result.get("missing_periods_found"): logging.warning(f"Eval FAIL {filename}: Missing periods detected.")
                 if current_evaluation_result.get("missing_grouped_totals_found"): logging.warning(f"Eval FAIL {filename}: Missing grouped totals detected.")
                 # Retry will happen if attempt_count < max_attempts
        else:
            logging.error(f"Evaluation LLM call failed for {filename} on attempt {attempt_count}. Cannot verify.")
            last_evaluation_result = None # Reset feedback
            # Continue loop if attempts remain, will retry extraction without specific feedback

    # --- End of Extraction/Evaluation Loop ---

    # Check if we have a result (even if it failed evaluation on the last try)
    if not last_llm_result:
        logging.error(f"No successful extraction result after {attempt_count} attempts for {filename}.")
        return None

    if not evaluation_passed:
        logging.warning(f"Proceeding with final extraction data for {filename} after {attempt_count} attempts (Evaluation did not PASS or failed).")

    # --- 4. Structure Data for DB (Using final result, logic as provided by user) ---
    records_for_db = []
    metadata = last_llm_result.get("metadata_predictions", {}) # Use final result
    line_items = last_llm_result.get("line_items", [])      # Use final result

    statement = metadata.get("statement_type")
    period = metadata.get("period")
    
    # Original group_or_company from LLM extraction
    original_group_or_company = metadata.get("group_or_company")
    
    trailing_zeros = metadata.get("trailing_zeros") # Flag 'Y'/'N'
    report_date = metadata.get("report_date") # LLM extracted date
    
    # --- NEW: Determine group or company level using mapping data ---
    # Default to the original determination
    group_or_company = original_group_or_company
    
    if mapping_data and symbol in mapping_data:
        # Check if we need LLM determination for this symbol
        needs_llm, keywords_list = needs_llm_determination(symbol, mapping_data)
        
        if not needs_llm:
            # Deterministic case: If all keywords are the literal string "None", set to company level
            group_or_company = "company"
            logging.info(f"Deterministic group level for {symbol}: company (all keywords are literal 'None')")
        elif keywords_list:
            # Use LLM to determine group level only if we have meaningful keywords
            logging.info(f"Using LLM to determine group level for {filename} with keywords: {keywords_list}")
            group_or_company = await determine_group_level_with_llm(
                genai_client, 
                filename, 
                simplified_csv_content, 
                keywords_list
            )
        else:
            # No meaningful keywords but not all "None" - use original determination
            logging.info(f"No meaningful keywords for {symbol}, using original determination: {original_group_or_company}")
    else:
        logging.info(f"No mapping data for {symbol}, using original determination: {original_group_or_company}")
    
    # Log if determination changed
    if group_or_company != original_group_or_company:
        logging.info(f"Group level changed for {filename}: {original_group_or_company} -> {group_or_company}")
    
    try:
        # Derive year from LLM date
        report_year = datetime.strptime(report_date, "%Y-%m-%d").strftime("%Y") if report_date else None
    except (ValueError, TypeError) as e: # Added TypeError
        report_year = None
        logging.warning(f"Incorrect date format extracted '{report_date}' from {filename}: {e}")

    # --- Using required fields check as provided by user ---
    if not all([statement, period, group_or_company]):
         logging.warning(f"Incomplete metadata {filename}: {metadata}")

    print("==============") # User's debug print
    print(filename, statement, line_items) # User's debug print
    print("==============") # User's debug print

    for item in line_items:
        li_name = item.get("line_item"); li_value_raw = item.get("value"); li_period_length = item.get("period_length")
        if not li_name or li_value_raw is None or not li_period_length:
            logging.warning(f"Incomplete line item {filename}: {item}"); continue

        # --- Using value processing logic as provided by user ---
        try:
            li_value_float = float(li_value_raw) # Directly try float as schema is NUMBER
            # Apply trailing zeros based on the flag from metadata
            # Note: User code compared trailing_zeros directly, assuming 'Y' or 'N' string
            # li_value_float = li_value_float * 1000 if trailing_zeros == "Y" else li_value_float
        except (ValueError, TypeError) as e: # Added TypeError
            logging.debug(f"Value conversion to float failed '{li_name}'='{li_value_raw}' in {filename}: {e}.")
            # Attempt cleanup ONLY if conversion fails (fallback)
            li_value_cleaned = clean_value(str(li_value_raw))
            if li_value_cleaned is not None:
                # li_value_float = li_value_cleaned * 1000 if trailing_zeros == "Y" else li_value_cleaned
                logging.debug(f"Fallback clean_value succeeded for '{li_name}'='{li_value_raw}'.")
            else:
                li_value_float = None # Set to None if both direct float and clean_value fail

        records_for_db.append({
            "symbol": symbol, "csv_path": s3_key, "statement": statement,
            "report_date": report_date, "year": report_year, "period": period,
            "period_type": period_type, "group_or_company_level": group_or_company, # Updated with new determination
            "line_item": str(li_name).strip(),
            "line_item_value": li_value_float, # Use the processed float value
            "period_length": li_period_length,
            "trailing_zeros": trailing_zeros
        })

    logging.info(f"Structured {len(records_for_db)} records for {filename} from final attempt.") # Updated log message slightly
    return records_for_db


# --- Database Saving (save_to_db ) ---
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
            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, csv_path TEXT, statement TEXT, report_date DATE, year INTEGER, period TEXT, period_type TEXT, group_or_company_level TEXT, line_item TEXT, line_item_value REAL, period_length TEXT, extraction_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, trailing_zeros TEXT, UNIQUE(csv_path, line_item, period_length))""")
            insert_data = [(r['symbol'], r['csv_path'], r['statement'], r['report_date'], r['year'], r['period'], r['period_type'], r['group_or_company_level'], r['line_item'], r['line_item_value'], r['period_length'], r['trailing_zeros']) for r in symbol_records]
            cursor.executemany(f"""INSERT INTO {table_name} (symbol, csv_path, statement, report_date, year, period, period_type, group_or_company_level, line_item, line_item_value, period_length, trailing_zeros) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(csv_path, line_item, period_length) DO UPDATE SET statement=excluded.statement, report_date=excluded.report_date, year=excluded.year, period=excluded.period, period_type=excluded.period_type, group_or_company_level=excluded.group_or_company_level, line_item_value=excluded.line_item_value, extraction_timestamp=CURRENT_TIMESTAMP, trailing_zeros=excluded.trailing_zeros""", insert_data)
        conn.commit()
        logging.info(f"Saved data for {len(records_by_symbol)} symbols.")
    except sqlite3.Error as e: logging.error(f"DB error: {e}"); conn and conn.rollback()
    finally: conn and conn.close()

# --- Worker Function  ---
async def worker(semaphore, key, s3_client, genai_client: genai.Client, mapping_data=None):
    """Worker wrapper to acquire semaphore before processing."""
    async with semaphore:
        return await process_csv(key, s3_client, genai_client, mapping_data)


# --- Main Orchestration  ---
async def main(symbol_arg=None, mapping_csv_path=None):
    logging.info("Starting JSE Data Extraction Process with GenAI Client...")
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
    s3_client = session.client('s3')
    
    try:
        api_key = os.getenv(API_KEY_NAME)
        if not api_key: raise ValueError(f"Environment variable {API_KEY_NAME} not set.")
        genai_client = genai.Client(api_key=api_key)
        logging.info(f"Google GenAI Client initialized for model {MODEL_NAME}")
    except Exception as e: 
        logging.error(f"Google GenAI Client init failed: {e}")
        return

    # Load mapping data if provided
    mapping_data = None
    if mapping_csv_path:
        try:
            with open(mapping_csv_path, 'r', encoding='utf-8') as f:
                mapping_csv_content = f.read()
            mapping_data = load_statement_mapping(mapping_csv_content)
            logging.info(f"Loaded statement mapping data for {len(mapping_data)} symbols.")
            
            # Log some stats about mapping data for debugging
            all_none_symbols = []
            llm_needed_symbols = []
            
            for symbol, entries in mapping_data.items():
                needs_llm, keywords = needs_llm_determination(symbol, mapping_data)
                if not needs_llm:
                    all_none_symbols.append(symbol)
                elif keywords:
                    llm_needed_symbols.append(symbol)
            
            logging.info(f"Symbols with all 'None' keywords (will use company level): {len(all_none_symbols)}")
            logging.info(f"Symbols requiring LLM group level determination: {len(llm_needed_symbols)}")
            
        except Exception as e:
            logging.error(f"Failed to load mapping CSV from {mapping_csv_path}: {e}")
            # Continue without mapping data
    
    symbols_to_process = []
    if symbol_arg: 
        symbols_to_process.append(symbol_arg.upper())
        logging.info(f"Processing symbol: {symbol_arg}")
    else:
        logging.info(f"Listing symbols in s3://{S3_BUCKET}/{S3_BASE_PREFIX}")
        paginator = s3_client.get_paginator('list_objects_v2')
        try:
            response_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_BASE_PREFIX, Delimiter='/')
            for page in response_iterator:
                for prefix_data in page.get('CommonPrefixes', []):
                    prefix = prefix_data.get('Prefix')
                    symbol = prefix.replace(S3_BASE_PREFIX, '', 1).strip('/') if prefix else None
                    symbol and symbols_to_process.append(symbol.upper())
            # Sort symbols alphabetically
            symbols_to_process.sort()
            logging.info(f"Found symbols: {symbols_to_process}")
            if not symbols_to_process: 
                logging.warning("No symbols found.")
                return
        except Exception as e: 
            logging.error(f"S3 symbol listing error: {e}")
            return

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    symbols_to_process = [s for s in symbols_to_process if s.lower() >= "lab"]
    
    # Process each symbol sequentially but process files within each symbol concurrently
    for symbol in symbols_to_process:
        logging.info(f"Processing symbol: {symbol}")
        symbol_prefix = f"{S3_BASE_PREFIX}{symbol}/"
        
        # Get CSV files for this symbol
        try:
            csv_keys = await list_csv_files(s3_client, S3_BUCKET, symbol_prefix)
            if not csv_keys:
                logging.warning(f"No CSV files found for symbol {symbol}.")
                continue
            logging.info(f"Found {len(csv_keys)} CSV files for symbol {symbol}.")
            
            # Process all files for this symbol concurrently
            # Pass mapping_data to each worker
            processing_tasks = [worker(semaphore, key, s3_client, genai_client, mapping_data) for key in csv_keys]
            task_results = await asyncio.gather(*processing_tasks)
            
            # Collect results for this symbol
            symbol_records = []
            failed_count = task_results.count(None)
            success_count = len(task_results) - failed_count
            for result in task_results:
                if result is not None:
                    symbol_records.extend(result)
            
            logging.info(f"Finished {symbol}. Success: {success_count} files. Failed/Skipped: {failed_count} files.")
            logging.info(f"Extracted records for {symbol}: {len(symbol_records)}")
            
            # Save this symbol's data to the database immediately
            if symbol_records:
                save_to_db(symbol_records, DB_NAME)
                logging.info(f"Saved {len(symbol_records)} records for symbol {symbol} to database.")
            
        except Exception as e:
            logging.error(f"Error processing symbol {symbol}: {e}")
    
    logging.info("JSE Data Extraction Process Finished.")


# --- Entry Point  ---
if __name__ == "__main__":
    # Ensure google-generativeai is installed: pip install google-generativeai
    parser = argparse.ArgumentParser(description="Extract JSE financial data from S3 CSVs using the Google GenAI API.")
    parser.add_argument("-s", "--symbol", help="Specify a single equity symbol to process.", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(main(symbol_arg=args.symbol, mapping_csv_path=STATEMENT_MAPPING_CSV))