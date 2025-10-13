import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import logging
from dotenv import load_dotenv
from google import genai
from google.genai import types
import json
import asyncio
import random
from typing import Dict, Any, Optional, List, Set
import time
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

MODEL_NAME = "gemini-2.5-flash-preview-05-20"

# Constants for retry mechanism
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 32  # seconds
MAX_CONCURRENT_REQUESTS = 10  # Adjust based on your API limits

class StandardizedName(str, Enum):
    REVENUE = "Revenue"
    GROSS_PROFIT = "Gross Profit"
    OPERATING_PROFIT = "Operating Profit"
    NET_PROFIT = "Net Profit"
    CURRENT_ASSETS = "Current Assets"
    NON_CURRENT_ASSETS = "Non-Current Assets"
    TOTAL_ASSETS = "Total Assets"
    TOTAL_EQUITY = "Total Equity"
    OPERATING_CASH_FLOW = "Operating Cash Flow"
    INVESTING_CASH_FLOW = "Investing Cash Flow"
    FINANCING_CASH_FLOW = "Financing Cash Flow"
    NET_CASH_FLOW = "Net Cash Flow"

class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class Standardization(BaseModel):
    standardized_name: StandardizedName = Field(description="The canonical form of the line item")
    confidence: Confidence = Field(description="Confidence level in the standardization")
    reasoning: str = Field(description="Explanation for the standardization")

    model_config = ConfigDict(
        json_schema_extra={
            "propertyOrdering": ["standardized_name", "confidence", "reasoning"]
        }
    )

# Define the schema for standardization
STANDARDIZATION_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "standardizations": {
            "type": "ARRAY",
            "description": "List of standardizations for each original item",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "original_item": {
                        "type": "STRING",
                        "description": "The original line item name"
                    },
                    "standardized_name": {
                        "type": "STRING",
                        "enum": [e.value for e in StandardizedName]
                    },
                    "confidence": {
                        "type": "STRING",
                        "enum": [e.value for e in Confidence]
                    },
                    "reasoning": {
                        "type": "STRING"
                    }
                },
                "required": ["original_item", "standardized_name", "confidence", "reasoning"]
            }
        }
    },
    "required": ["standardizations"]
}

# System prompt for standardization
SYSTEM_PROMPT = """You are a financial data standardization expert. Your task is to identify and standardize variations of financial statement line items to their canonical forms, taking into account the type of financial statement they appear in.

**Statement Types in the Dataset:**
1. Income Statement
2. Comprehensive Income Statement
3. Balance Sheet
4. Cash Flow Statement

**Target Categories and Their Variations by Statement Type:**

1. Income Statement & Comprehensive Income Statement Items:
   - Revenue
     * Variations: "Revenue", "Sales", "Turnover", "Revenues from contracts with customers", "Revenue (Note X)", etc.
     * Canonical form: "Revenue"
   - Gross Profit
     * Variations: "Gross Profit", "Gross Income", "Gross Earnings", etc.
     * Canonical form: "Gross Profit"
   - Operating Profit
     * Variations: "Operating Profit", "Operating Income", "Operating Earnings", etc.
     * Canonical form: "Operating Profit"
   - Net Profit
     * Variations: "Net Profit", "Net Income", "Net Earnings", "Profit for the period", etc.
     * Canonical form: "Net Profit"

2. Balance Sheet Items:
   - Current Assets
     * Variations: "Current Assets", "Total Current Assets", "Current Assets - Total", etc.
     * Canonical form: "Current Assets"
   - Non-Current Assets
     * Variations: "Non-Current Assets", "Long-term Assets", "Fixed Assets", etc.
     * Canonical form: "Non-Current Assets"
   - Total Assets
     * Variations: "Total Assets", "Assets", "Total Assets and Liabilities", etc.
     * Canonical form: "Total Assets"
   - Total Equity
     * Variations: "Total Equity", "Shareholders' Equity", "Stockholders' Equity", "Equity", etc.
     * Canonical form: "Total Equity"

3. Cash Flow Statement Items:
   - Operating Cash Flow
     * Variations: "Cash generated from operations", "Net cash from operating activities", etc.
     * Canonical form: "Operating Cash Flow"
   - Investing Cash Flow
     * Variations: "Cash used in investing activities", "Net cash from investing activities", etc.
     * Canonical form: "Investing Cash Flow"
   - Financing Cash Flow
     * Variations: "Cash from financing activities", "Net cash from financing activities", etc.
     * Canonical form: "Financing Cash Flow"
   - Net Cash Flow
     * Variations: "Net increase in cash", "Net change in cash", etc.
     * Canonical form: "Net Cash Flow"

**Instructions:**
1. For each item, first identify which statement type it appears in.
2. Then, within that statement type's context, identify which canonical category it belongs to.
3. Consider common variations and patterns in financial terminology within each statement type.
4. Ignore notes, parenthetical references, and minor formatting differences.
5. If an item doesn't clearly match any category for its statement type, mark it as "Other".
6. For each item, provide:
   - The canonical form it should be standardized to
   - Your confidence level in the standardization
   - Brief reasoning that MUST include:
     * The statement type it was found in
     * Why this statement type influenced the standardization
     * Any specific patterns or variations that helped identify the canonical form"""

def normalize_line_item(item: str) -> str:
    """Normalize a single line item string."""
    return (item.lower()
            .strip()
            .replace(' ', '_')
            .replace('-', '_')
            .replace('/', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('.', '')
            .replace(',', ''))

async def standardize_item_variations(
    items_with_context: List[Dict[str, str]],
    genai_client: genai.Client,
    semaphore: asyncio.Semaphore
) -> Dict[str, Dict[str, Any]]:
    """Standardize variations of line items to their canonical forms using statement context."""
    
    # Group items by statement type for better context
    items_by_statement = {}
    for item in items_with_context:
        statement = item['statement']
        if statement not in items_by_statement:
            items_by_statement[statement] = []
        items_by_statement[statement].append(item['line_item'])

    # Create the user prompt with the items to standardize
    user_prompt = f"""
    Please standardize the following items by statement type. Note that these items are from the same report (same symbol and report date):

    {json.dumps(items_by_statement, indent=2)}

    Return your analysis as a JSON array where each object contains:
    - original_item: The original line item name
    - standardized_name: The canonical form
    - confidence: "high", "medium", or "low"
    - reasoning: Brief explanation for the standardization, including statement type context
    """

    retry_count = 0
    backoff = INITIAL_BACKOFF

    while retry_count < MAX_RETRIES:
        try:
            async with semaphore:
                def sync_generate():
                    return genai_client.models.generate_content(
                        model=MODEL_NAME,
                        contents=[user_prompt],
                        config=types.GenerateContentConfig(
                            system_instruction=[SYSTEM_PROMPT],
                            response_mime_type="application/json",
                            response_schema=STANDARDIZATION_SCHEMA,
                            thinking_config=types.ThinkingConfig(
                                thinking_budget=1024
                            )
                        )
                    )

                response = await asyncio.to_thread(sync_generate)

                if hasattr(response, 'text') and response.text:
                    try:
                        result = json.loads(response.text)
                        standardizations_list = result.get('standardizations', [])
                        
                        # Convert array to dictionary for compatibility
                        standardizations = {}
                        for item in standardizations_list:
                            original_item = item.get('original_item')
                            if original_item:
                                standardizations[original_item] = {
                                    'standardized_name': item.get('standardized_name'),
                                    'confidence': item.get('confidence'),
                                    'reasoning': item.get('reasoning')
                                }
                        
                        # Log summary of standardizations by statement type
                        statement_stats = {}
                        for item, std in standardizations.items():
                            # Find the statement type for this item
                            for statement, items in items_by_statement.items():
                                if item in items:
                                    if statement not in statement_stats:
                                        statement_stats[statement] = {}
                                    cat = std.get('standardized_name', 'Other')
                                    statement_stats[statement][cat] = statement_stats[statement].get(cat, 0) + 1
                                    break
                        
                        logging.info("\nStandardization Statistics by Statement Type:")
                        for statement, stats in statement_stats.items():
                            logging.info(f"\n{statement}:")
                            for category, count in stats.items():
                                logging.info(f"  {category}: {count} items")
                        
                        return standardizations
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse LLM response as JSON: {str(e)}")
                        logging.error(f"Raw response: {response.text}")
                        raise ValueError("Invalid JSON response from LLM")
                else:
                    raise ValueError("Empty response from LLM")

        except Exception as e:
            retry_count += 1
            if retry_count == MAX_RETRIES:
                logging.error(f"Failed to standardize items after {MAX_RETRIES} attempts: {str(e)}")
                return {item['line_item']: {
                    "standardized_name": "Other",
                    "confidence": "low",
                    "reasoning": f"Error during standardization after {MAX_RETRIES} attempts: {str(e)}"
                } for item in items_with_context}

            # Calculate backoff with jitter
            jitter = random.uniform(0, 0.1 * backoff)
            sleep_time = min(backoff + jitter, MAX_BACKOFF)
            
            logging.warning(f"Retry {retry_count}/{MAX_RETRIES} after {sleep_time:.2f}s: {str(e)}")
            await asyncio.sleep(sleep_time)
            
            # Exponential backoff
            backoff = min(backoff * 2, MAX_BACKOFF)

def select_best_standardization(standardizations: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Select the best standardization for each category within a report context."""
    # Group standardizations by their standardized name
    by_category = {}
    for original_item, std in standardizations.items():
        category = std.get('standardized_name')
        if category not in by_category:
            by_category[category] = []
        by_category[category].append((original_item, std))
    
    # For each category, select the best standardization
    best_standardizations = {}
    for category, items in by_category.items():
        # Sort by confidence (high > medium > low)
        confidence_order = {'high': 3, 'medium': 2, 'low': 1}
        sorted_items = sorted(
            items,
            key=lambda x: (
                confidence_order.get(x[1].get('confidence', 'low'), 0),
                len(x[0])  # Use length as tiebreaker (prefer shorter names)
            ),
            reverse=True
        )
        
        # Take the best one
        best_original, best_std = sorted_items[0]
        best_standardizations[best_original] = best_std
        
        # Log the alternatives that were not chosen
        if len(sorted_items) > 1:
            logging.info(f"\nFor category {category}, selected '{best_original}' over alternatives:")
            for alt_original, alt_std in sorted_items[1:]:
                logging.info(f"  - '{alt_original}' (confidence: {alt_std.get('confidence')})")
    
    return best_standardizations

async def create_standardized_table_if_not_exists():
    """Create the standardized line items table if it doesn't exist."""
    try:
        client = bigquery.Client()
        table_id = "jse-datasphere.jse_raw_financial_data_dev_elroy.standardized_line_items"
        
        # Define the schema
        schema = [
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("statement", "STRING"),
            bigquery.SchemaField("line_item", "STRING"),
            bigquery.SchemaField("report_date", "DATE"),
            bigquery.SchemaField("period", "STRING"),
            bigquery.SchemaField("period_type", "STRING"),
            bigquery.SchemaField("group_or_company_level", "STRING"),
            bigquery.SchemaField("standardized_line_item", "STRING"),
            bigquery.SchemaField("standardization_confidence", "STRING"),
            bigquery.SchemaField("standardization_reasoning", "STRING"),
            bigquery.SchemaField("processed_at", "TIMESTAMP")
        ]
        
        # Create the table if it doesn't exist
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        logging.info(f"Table {table_id} is ready")
        
    except Exception as e:
        logging.error(f"Error creating table: {str(e)}")
        raise

async def append_to_standardized_table(df: pd.DataFrame):
    """Append standardized results to BigQuery table."""
    try:
        client = bigquery.Client()
        table_id = "jse-datasphere.jse_raw_financial_data_dev_elroy.standardized_line_items"
        
        # Add processed_at timestamp
        df['processed_at'] = pd.Timestamp.now()
        
        # Convert report_date to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['report_date']):
            df['report_date'] = pd.to_datetime(df['report_date'])
        
        # Load the data
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete
        
        logging.info(f"Appended {len(df)} rows to {table_id}")
        
    except Exception as e:
        logging.error(f"Error appending to table: {str(e)}")
        raise

async def standardize_line_items(df: pd.DataFrame, genai_client: genai.Client) -> pd.DataFrame:
    """Standardize line items using Gemini LLM with async processing."""
    
    # Get unique items with their statement context and report date
    unique_items = []
    seen_items = set()
    for _, row in df.iterrows():
        # Create a unique key for each symbol+statement+report_date+period+period_type+group_or_company_level combination
        context_key = f"{row['symbol']}_{row['statement']}_{row['report_date']}_{row['period']}_{row['period_type']}_{row['group_or_company_level']}"
        normalized_item = normalize_line_item(row['line_item'])
        item_key = f"{context_key}_{normalized_item}"
        
        if item_key not in seen_items:
            seen_items.add(item_key)
            unique_items.append({
                'line_item': row['line_item'],
                'statement': row['statement'],
                'symbol': row['symbol'],
                'report_date': row['report_date'],
                'period': row['period'],
                'period_type': row['period_type'],
                'group_or_company_level': row['group_or_company_level'],
                'context_key': context_key
            })
    
    logging.info(f"Found {len(unique_items)} unique line items to standardize")
    
    # Create semaphore for rate limiting
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # Group items by context (symbol+statement+report_date+period+period_type+group_or_company_level)
    items_by_context = {}
    for item in unique_items:
        context_key = item['context_key']
        if context_key not in items_by_context:
            items_by_context[context_key] = []
        items_by_context[context_key].append(item)
    
    # Process items in batches by context
    all_standardizations = {}
    processed_contexts = []
    
    for context_key, context_items in items_by_context.items():
        logging.info(f"Processing context: {context_key} with {len(context_items)} items")
        
        # Create tasks for the context
        tasks = [
            standardize_item_variations(context_items, genai_client, semaphore)
        ]
        
        # Wait for all tasks in the context to complete
        results = await asyncio.gather(*tasks)
        
        # For each context, select the best standardization for each category
        for result in results:
            best_standardizations = select_best_standardization(result)
            all_standardizations.update(best_standardizations)
        
        # Create a DataFrame for this context's results
        context_df = pd.DataFrame([
            {
                'symbol': item['symbol'],
                'statement': item['statement'],
                'line_item': item['line_item'],
                'report_date': item['report_date'],
                'period': item['period'],
                'period_type': item['period_type'],
                'group_or_company_level': item['group_or_company_level'],
                'standardized_line_item': best_standardizations.get(item['line_item'], {}).get('standardized_name', 'Other'),
                'standardization_confidence': best_standardizations.get(item['line_item'], {}).get('confidence', 'low'),
                'standardization_reasoning': best_standardizations.get(item['line_item'], {}).get('reasoning', 'No reasoning provided')
            }
            for item in context_items
        ])
        
        # Append this context's results to BigQuery
        await append_to_standardized_table(context_df)
        processed_contexts.append(context_key)
        
        # Small delay between contexts to prevent rate limiting
        await asyncio.sleep(1)

    # Apply standardization to the full DataFrame
    df['standardized_line_item'] = df['line_item'].map(
        lambda x: all_standardizations.get(x, {}).get('standardized_name', 'Other')
    )
    df['standardization_confidence'] = df['line_item'].map(
        lambda x: all_standardizations.get(x, {}).get('confidence', 'low')
    )
    df['standardization_reasoning'] = df['line_item'].map(
        lambda x: all_standardizations.get(x, {}).get('reasoning', 'No reasoning provided')
    )

    # Log standardization statistics by context
    logging.info("\nStandardization Statistics by Context:")
    for context_key in items_by_context.keys():
        context_df = df[
            (df['symbol'] + '_' + df['statement'] + '_' + df['report_date'].astype(str) + '_' + 
             df['period'] + '_' + df['period_type'] + '_' + df['group_or_company_level']) == context_key
        ]
        standardization_stats = context_df['standardized_line_item'].value_counts()
        logging.info(f"\nContext: {context_key}")
        for category, count in standardization_stats.items():
            logging.info(f"  {category}: {count} items")

    return df

async def query_bigquery_table():
    """Query the JSE raw analytical data from BigQuery."""
    try:
        # Initialize the BigQuery client
        client = bigquery.Client()
        
        # Define the query
        query = """
        SELECT
            distinct symbol, statement, line_item, report_date,
            period, period_type, group_or_company_level
        FROM `jse-datasphere.jse_raw_financial_data_dev_elroy.jse_raw_analytical`
        """
        
        # Execute the query
        query_job = client.query(query)
        
        # Convert results to pandas DataFrame
        results = query_job.result().to_dataframe()
        
        logging.info(f"Successfully queried {len(results)} rows from BigQuery")
        return results
        
    except Exception as e:
        logging.error(f"Error querying BigQuery: {str(e)}")
        raise

async def main():
    # Initialize Google GenAI client
    try:
        api_key = os.getenv("GOOGLE_VERTEX_API_KEY")
        if not api_key:
            raise ValueError("Environment variable GOOGLE_VERTEX_API_KEY not set.")
        genai_client = genai.Client(api_key=api_key)
        logging.info(f"Google GenAI Client initialized for model {MODEL_NAME}")
    except Exception as e:
        logging.error(f"Google GenAI Client initialization failed: {e}")
        exit(1)

    # Create the standardized table if it doesn't exist
    await create_standardized_table_if_not_exists()

    # Query the data
    df = await query_bigquery_table()
    
    # Get the number of distinct line_items
    num_distinct_line_items = df['line_item'].nunique()
    logging.info(f"Number of distinct line_items: {num_distinct_line_items}")
    
    # Get the maximum number of rows per symbol
    max_rows_per_symbol = df.groupby('symbol').size().max()
    logging.info(f"Maximum number of rows per symbol: {max_rows_per_symbol}")
    
    # Get the minimum number of rows per symbol
    min_rows_per_symbol = df.groupby('symbol').size().min()
    logging.info(f"Minimum number of rows per symbol: {min_rows_per_symbol}")
    
    # Standardize the line items
    df = await standardize_line_items(df, genai_client)
    
    # Save the standardized results locally as well
    output_file = "standardized_line_items_2.csv"
    df.to_csv(output_file, index=False)
    logging.info(f"Saved standardized results to {output_file}")

if __name__ == "__main__":
    asyncio.run(main())