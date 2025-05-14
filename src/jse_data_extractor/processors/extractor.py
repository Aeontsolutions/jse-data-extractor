"""
Main processor module for the JSE Data Extractor.
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any
from pathlib import Path

from ..services.llm_service import LLMService
from ..services.db_service import DatabaseService
from ..utils.helpers import parse_date_from_filename, clean_value, load_statement_mapping
from ..models.schemas import (
    RESPONSE_SCHEMA_DICT,
    EVALUATION_SCHEMA_DICT,
    GROUP_LEVEL_SCHEMA_DICT
)

class JSEExtractor:
    """Main processor class for JSE data extraction."""
    
    def __init__(self):
        """Initialize the extractor with required services."""
        self.llm_service = LLMService()
        self.db_service = DatabaseService()
        
    async def process_statement(self, symbol: str, statement_type: str, content: str) -> Optional[Dict]:
        """
        Process a financial statement.
        
        Args:
            symbol: Company symbol
            statement_type: Type of financial statement
            content: Statement content to process
            
        Returns:
            Processed data dictionary or None if failed
        """
        try:
            # Process content through LLM
            extracted_data = self.llm_service.process_content(statement_type, content)
            if not extracted_data:
                logging.error(f"Failed to process content for {symbol} {statement_type}")
                return None
                
            # Validate against schema
            if not self._validate_schema(extracted_data, RESPONSE_SCHEMA_DICT):
                logging.error(f"Invalid schema for {symbol} {statement_type}")
                return None
                
            # Evaluate extraction
            evaluation = self.llm_service.evaluate_extraction(content, extracted_data)
            if not evaluation:
                logging.error(f"Failed to evaluate extraction for {symbol} {statement_type}")
                return None
                
            # Validate evaluation schema
            if not self._validate_schema(evaluation, EVALUATION_SCHEMA_DICT):
                logging.error(f"Invalid evaluation schema for {symbol} {statement_type}")
                return None
                
            # Determine group level
            group_level = self.llm_service.determine_group_level(content)
            if not group_level:
                logging.error(f"Failed to determine group level for {symbol} {statement_type}")
                return None
                
            # Validate group level schema
            if not self._validate_schema(group_level, GROUP_LEVEL_SCHEMA_DICT):
                logging.error(f"Invalid group level schema for {symbol} {statement_type}")
                return None
                
            # Combine results
            result = {
                'extracted_data': extracted_data,
                'evaluation': evaluation,
                'group_level': group_level
            }
            
            # Save to database
            if not self.db_service.save_extraction(symbol, statement_type, result):
                logging.error(f"Failed to save extraction for {symbol} {statement_type}")
                return None
                
            return result
            
        except Exception as e:
            logging.error(f"Error processing statement for {symbol} {statement_type}: {e}")
            return None
            
    def _validate_schema(self, data: Dict, schema: Dict) -> bool:
        """
        Validate data against a JSON schema.
        
        Args:
            data: Data to validate
            schema: Schema to validate against
            
        Returns:
            bool indicating if validation passed
        """
        try:
            # Basic schema validation
            required_keys = set(schema.get('required', []))
            if not required_keys.issubset(data.keys()):
                return False
                
            # Type validation
            for key, value in data.items():
                if key not in schema['properties']:
                    continue
                    
                expected_type = schema['properties'][key]['type']
                if expected_type == 'array':
                    if not isinstance(value, list):
                        return False
                elif expected_type == 'object':
                    if not isinstance(value, dict):
                        return False
                elif expected_type == 'string':
                    if not isinstance(value, str):
                        return False
                elif expected_type == 'number':
                    if not isinstance(value, (int, float)):
                        return False
                elif expected_type == 'boolean':
                    if not isinstance(value, bool):
                        return False
                        
            return True
            
        except Exception as e:
            logging.error(f"Schema validation error: {e}")
            return False
            
    async def process_file(self, file_path: Path, symbol: str, statement_type: str) -> Optional[Dict]:
        """
        Process a financial statement file.
        
        Args:
            file_path: Path to the statement file
            symbol: Company symbol
            statement_type: Type of financial statement
            
        Returns:
            Processed data dictionary or None if failed
        """
        try:
            # Read file content
            content = file_path.read_text()
            
            # Process statement
            return await self.process_statement(symbol, statement_type, content)
            
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return None
            
    async def process_directory(self, directory: Path, mapping_csv_path: Path) -> Dict[str, List[Dict]]:
        """
        Process all statements in a directory.
        
        Args:
            directory: Directory containing statement files
            mapping_csv_path: Path to statement mapping CSV
            
        Returns:
            Dictionary mapping symbols to their processed statements
        """
        try:
            # Load statement mapping
            mapping_data = load_statement_mapping(mapping_csv_path.read_text())
            if not mapping_data:
                logging.error("Failed to load statement mapping")
                return {}
                
            results = {}
            
            # Process each file
            for file_path in directory.glob('*.csv'):
                filename = file_path.name
                date = parse_date_from_filename(filename)
                if not date:
                    logging.warning(f"Could not parse date from filename: {filename}")
                    continue
                    
                # Find matching symbol and statement type
                for symbol, mappings in mapping_data.items():
                    for mapping in mappings:
                        if any(keyword in filename.lower() for keyword in mapping['keywords'].lower().split(',')):
                            result = await self.process_file(
                                file_path,
                                symbol,
                                mapping['statement_type']
                            )
                            if result:
                                if symbol not in results:
                                    results[symbol] = []
                                results[symbol].append(result)
                                
            return results
            
        except Exception as e:
            logging.error(f"Error processing directory {directory}: {e}")
            return {} 