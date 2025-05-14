"""
Database service module for the JSE Data Extractor.
"""

import logging
from typing import Dict, List, Optional, Any
import boto3
from botocore.exceptions import ClientError

from ..config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    DYNAMODB_TABLE_NAME
)

class DatabaseService:
    """Service class for interacting with DynamoDB."""
    
    def __init__(self):
        """Initialize the database service with DynamoDB client."""
        self.dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        self.table = self.dynamodb.Table(DYNAMODB_TABLE_NAME)
        
    def save_extraction(self, symbol: str, statement_type: str, data: Dict) -> bool:
        """
        Save extraction results to DynamoDB.
        
        Args:
            symbol: Company symbol
            statement_type: Type of financial statement
            data: Extracted data to save
            
        Returns:
            bool indicating success
        """
        try:
            item = {
                'symbol': symbol,
                'statement_type': statement_type,
                'data': data
            }
            
            self.table.put_item(Item=item)
            return True
            
        except ClientError as e:
            logging.error(f"Error saving extraction to DynamoDB: {e}")
            return False
            
    def get_extraction(self, symbol: str, statement_type: str) -> Optional[Dict]:
        """
        Retrieve extraction results from DynamoDB.
        
        Args:
            symbol: Company symbol
            statement_type: Type of financial statement
            
        Returns:
            Extracted data dictionary or None if not found
        """
        try:
            response = self.table.get_item(
                Key={
                    'symbol': symbol,
                    'statement_type': statement_type
                }
            )
            
            return response.get('Item', {}).get('data')
            
        except ClientError as e:
            logging.error(f"Error retrieving extraction from DynamoDB: {e}")
            return None
            
    def delete_extraction(self, symbol: str, statement_type: str) -> bool:
        """
        Delete extraction results from DynamoDB.
        
        Args:
            symbol: Company symbol
            statement_type: Type of financial statement
            
        Returns:
            bool indicating success
        """
        try:
            self.table.delete_item(
                Key={
                    'symbol': symbol,
                    'statement_type': statement_type
                }
            )
            return True
            
        except ClientError as e:
            logging.error(f"Error deleting extraction from DynamoDB: {e}")
            return False
            
    def list_extractions(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        List all extractions or extractions for a specific symbol.
        
        Args:
            symbol: Optional company symbol to filter by
            
        Returns:
            List of extraction items
        """
        try:
            if symbol:
                response = self.table.query(
                    KeyConditionExpression='symbol = :s',
                    ExpressionAttributeValues={
                        ':s': symbol
                    }
                )
            else:
                response = self.table.scan()
                
            return response.get('Items', [])
            
        except ClientError as e:
            logging.error(f"Error listing extractions from DynamoDB: {e}")
            return [] 