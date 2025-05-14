"""
Configuration settings for the JSE Data Extractor.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API and AWS Configuration
API_KEY_NAME = "GOOGLE_VERTEX_API_KEY"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "jse-renamed-docs"
S3_BASE_PREFIX = "CSV/"

# Model and Database Configuration
MODEL_NAME = "gemini-2.5-flash-preview-04-17"
DB_NAME = "jse_financial_data.db"
LOG_FILE = "jse_extraction.log"
STATEMENT_MAPPING_CSV = os.getenv("STATEMENT_MAPPING_CSV_PATH")
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT")) 