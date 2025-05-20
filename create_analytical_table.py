import sqlite3
import logging
import os
from datetime import datetime
from google.cloud import bigquery
from google.api_core import retry
import pandas as pd
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analytical_table_creation.log'),
        logging.StreamHandler()
    ]
)

def create_new_database(source_db, target_db):
    """Create a new database by copying the source database."""
    try:
        # Check if source database exists
        if not os.path.exists(source_db):
            logging.error(f"Source database {source_db} not found")
            return False
            
        # Remove target database if it exists
        if os.path.exists(target_db):
            os.remove(target_db)
            logging.info(f"Removed existing {target_db}")
            
        # Copy the source database to create the new one
        shutil.copy2(source_db, target_db)
        logging.info(f"Created new database {target_db} from {source_db}")
        return True
        
    except Exception as e:
        logging.error(f"Error creating new database: {e}")
        return False

def get_raw_tables(cursor):
    """Get all jse_raw_* tables from the database."""
    cursor.execute("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' 
        AND name LIKE 'jse_raw_%'
    """)
    return [row[0] for row in cursor.fetchall()]

def create_analytical_table(cursor):
    """Create the analytical table with appropriate indexes."""
    # Create the main table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jse_analytical (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            csv_path TEXT,
            statement TEXT,
            report_date DATE,
            year INTEGER,
            period TEXT,
            period_type TEXT,
            group_or_company_level TEXT,
            line_item TEXT,
            line_item_value REAL,
            period_length TEXT,
            extraction_timestamp DATETIME,
            trailing_zeros TEXT,
            UNIQUE(symbol, report_date, statement, line_item, period_length)
        )
    """)
    
    # Create indexes for common query patterns
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytical_symbol ON jse_analytical(symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytical_report_date ON jse_analytical(report_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytical_statement ON jse_analytical(statement)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytical_line_item ON jse_analytical(line_item)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytical_year ON jse_analytical(year)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analytical_period ON jse_analytical(period)")

def combine_raw_tables(cursor, raw_tables):
    """Combine data from all raw tables into the analytical table."""
    total_processed = 0
    for table in raw_tables:
        symbol = table.replace('jse_raw_', '')
        logging.info(f"Processing table: {table} for symbol: {symbol}")
        
        try:
            # Insert data from raw table into analytical table
            cursor.execute(f"""
                INSERT OR REPLACE INTO jse_analytical (
                    symbol, csv_path, statement, report_date, year, 
                    period, period_type, group_or_company_level, 
                    line_item, line_item_value, period_length, 
                    extraction_timestamp, trailing_zeros
                )
                SELECT 
                    '{symbol}', csv_path, statement, report_date, year,
                    period, period_type, group_or_company_level,
                    line_item, line_item_value, period_length,
                    extraction_timestamp, trailing_zeros
                FROM {table}
            """)
            
            # Log the number of records processed
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_processed += count
            logging.info(f"Processed {count} records from {table}")
            
        except sqlite3.Error as e:
            logging.error(f"Error processing table {table}: {e}")
            continue

    return total_processed

def export_to_bigquery(cursor, project_id, dataset_id, table_id):
    """Export the analytical table to BigQuery."""
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Get the full table ID
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Read data from SQLite into a pandas DataFrame
        cursor.execute("SELECT * FROM jse_analytical")
        columns = [description[0] for description in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        
        # Convert date columns to proper format, handling invalid dates
        def safe_date_convert(date_str):
            try:
                if pd.isna(date_str) or date_str == '0000-00-00' or date_str == '2016-00-na':
                    return None
                return pd.to_datetime(date_str).date()
            except:
                return None

        def safe_timestamp_convert(ts_str):
            try:
                if pd.isna(ts_str):
                    return None
                return pd.to_datetime(ts_str)
            except:
                return None

        df['report_date'] = df['report_date'].apply(safe_date_convert)
        df['extraction_timestamp'] = df['extraction_timestamp'].apply(safe_timestamp_convert)
        
        # Define the schema for BigQuery
        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("csv_path", "STRING"),
            bigquery.SchemaField("statement", "STRING"),
            bigquery.SchemaField("report_date", "DATE"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("period", "STRING"),
            bigquery.SchemaField("period_type", "STRING"),
            bigquery.SchemaField("group_or_company_level", "STRING"),
            bigquery.SchemaField("line_item", "STRING"),
            bigquery.SchemaField("line_item_value", "FLOAT"),
            bigquery.SchemaField("period_length", "STRING"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("trailing_zeros", "STRING")
        ]
        
        # Create or replace the table
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        # Load the data
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        
        # Wait for the job to complete
        job.result()
        
        # Get the number of rows loaded
        table = client.get_table(table_ref)
        logging.info(f"Loaded {table.num_rows} rows into {table_ref}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error exporting to BigQuery: {e}")
        return False

def main():
    source_db = "jse_financial_data.db"
    target_db = "jse_raw_financial_data_dev_elroy.db"
    
    # BigQuery configuration
    project_id = "jse-data-pipeline"
    dataset_id = "jse_raw_financial_data_dev_elroy"
    table_id = "jse_raw_analytical"
    
    # Create new database from source
    if not create_new_database(source_db, target_db):
        logging.error("Failed to create new database. Exiting.")
        return
    
    try:
        conn = sqlite3.connect(target_db)
        cursor = conn.cursor()
        
        # Get all raw tables
        raw_tables = get_raw_tables(cursor)
        if not raw_tables:
            logging.error("No jse_raw_* tables found in the database")
            return
        
        logging.info(f"Found {len(raw_tables)} raw tables to process in {target_db}")
        
        # Create analytical table
        create_analytical_table(cursor)
        logging.info("Created analytical table with indexes")
        
        # Combine data from raw tables
        total_processed = combine_raw_tables(cursor, raw_tables)
        
        # Commit changes
        conn.commit()
        logging.info(f"Successfully combined {total_processed} records into analytical table")
        
        # Log final record count
        cursor.execute("SELECT COUNT(*) FROM jse_analytical")
        total_records = cursor.fetchone()[0]
        logging.info(f"Total records in analytical table: {total_records}")
        
        # Log some sample data
        cursor.execute("""
            SELECT symbol, statement, COUNT(*) as count 
            FROM jse_analytical 
            GROUP BY symbol, statement 
            ORDER BY symbol, statement 
            LIMIT 5
        """)
        sample_data = cursor.fetchall()
        logging.info("Sample data distribution:")
        for symbol, statement, count in sample_data:
            logging.info(f"  {symbol} - {statement}: {count} records")
        
        # Export to BigQuery
        logging.info("Starting export to BigQuery...")
        if export_to_bigquery(cursor, project_id, dataset_id, table_id):
            logging.info("Successfully exported data to BigQuery")
        else:
            logging.error("Failed to export data to BigQuery")
        
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 