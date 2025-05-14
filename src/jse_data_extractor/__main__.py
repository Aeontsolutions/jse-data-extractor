"""
Main entry point for the JSE Data Extractor.
"""

import os
import sys
import logging
import argparse
import asyncio
from pathlib import Path
from typing import Optional

from .processors.extractor import JSEExtractor
from .config.settings import STATEMENT_MAPPING_CSV

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='JSE Data Extractor')
    parser.add_argument(
        '--symbol',
        type=str,
        help='Company symbol to process'
    )
    parser.add_argument(
        '--directory',
        type=str,
        help='Directory containing statement files'
    )
    parser.add_argument(
        '--mapping-csv',
        type=str,
        default=STATEMENT_MAPPING_CSV,
        help='Path to statement mapping CSV'
    )
    return parser.parse_args()

async def main(symbol_arg: Optional[str] = None, directory_arg: Optional[str] = None, mapping_csv_path: str = STATEMENT_MAPPING_CSV):
    """
    Main entry point for the JSE Data Extractor.
    
    Args:
        symbol_arg: Optional company symbol to process
        directory_arg: Optional directory containing statement files
        mapping_csv_path: Path to statement mapping CSV
    """
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize extractor
        extractor = JSEExtractor()
        
        # Process single symbol if specified
        if symbol_arg:
            logger.info(f"Processing symbol: {symbol_arg}")
            # TODO: Implement single symbol processing
            pass
            
        # Process directory if specified
        elif directory_arg:
            directory = Path(directory_arg)
            if not directory.exists():
                logger.error(f"Directory not found: {directory}")
                return
                
            logger.info(f"Processing directory: {directory}")
            results = await extractor.process_directory(directory, Path(mapping_csv_path))
            
            # Log results
            for symbol, statements in results.items():
                logger.info(f"Processed {len(statements)} statements for {symbol}")
                
        else:
            logger.error("Either --symbol or --directory must be specified")
            return
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return

if __name__ == '__main__':
    args = parse_args()
    asyncio.run(main(
        symbol_arg=args.symbol,
        directory_arg=args.directory,
        mapping_csv_path=args.mapping_csv
    )) 