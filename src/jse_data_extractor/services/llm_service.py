"""
LLM service module for the JSE Data Extractor.
"""

import json
import logging
from typing import Dict, List, Optional, Any
import boto3
from botocore.exceptions import ClientError

from ..config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    MODEL_ID,
    MAX_TOKENS,
    TEMPERATURE,
    TOP_P
)

class LLMService:
    """Service class for interacting with AWS Bedrock LLM."""
    
    def __init__(self):
        """Initialize the LLM service with AWS Bedrock client."""
        self.bedrock = boto3.client(
            service_name='bedrock-runtime',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
    def _create_prompt(self, statement_type: str, content: str) -> str:
        """
        Create the prompt for the LLM.
        
        Args:
            statement_type: Type of financial statement
            content: The content to process
            
        Returns:
            Formatted prompt string
        """
        return f"""You are a financial data extraction assistant. Extract financial data from the following {statement_type} statement.
        Return the data in a structured JSON format with the following schema:
        {{
            "metadata_predictions": {{
                "statement_type": "string",
                "period": "string",
                "group_or_company": "string",
                "trailing_zeros": boolean,
                "report_date": "string"
            }},
            "line_items": [
                {{
                    "line_item": "string",
                    "value": number,
                    "period_length": "string"
                }}
            ]
        }}
        
        Content to process:
        {content}
        """
        
    def _create_evaluation_prompt(self, content: str, extracted_data: Dict) -> str:
        """
        Create the evaluation prompt for the LLM.
        
        Args:
            content: Original content
            extracted_data: Extracted data to evaluate
            
        Returns:
            Formatted evaluation prompt
        """
        return f"""Evaluate the following financial data extraction:
        
        Original Content:
        {content}
        
        Extracted Data:
        {json.dumps(extracted_data, indent=2)}
        
        Return your evaluation in this JSON format:
        {{
            "evaluation_judgment": "string",
            "evaluation_reasoning": "string",
            "missing_periods_found": boolean,
            "missing_grouped_totals_found": boolean
        }}
        """
        
    def _create_group_level_prompt(self, content: str) -> str:
        """
        Create the group level determination prompt.
        
        Args:
            content: Content to analyze
            
        Returns:
            Formatted group level prompt
        """
        return f"""Determine if this financial statement is at the group level:
        
        {content}
        
        Return your analysis in this JSON format:
        {{
            "group_level_determination": "string",
            "confidence": number,
            "reasoning": "string"
        }}
        """
        
    def process_content(self, statement_type: str, content: str) -> Optional[Dict]:
        """
        Process content through the LLM.
        
        Args:
            statement_type: Type of financial statement
            content: Content to process
            
        Returns:
            Processed data dictionary or None if failed
        """
        try:
            prompt = self._create_prompt(statement_type, content)
            
            response = self.bedrock.invoke_model(
                modelId=MODEL_ID,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens": MAX_TOKENS,
                    "temperature": TEMPERATURE,
                    "top_p": TOP_P
                })
            )
            
            response_body = json.loads(response['body'].read())
            return json.loads(response_body['completion'])
            
        except (ClientError, json.JSONDecodeError) as e:
            logging.error(f"Error processing content: {e}")
            return None
            
    def evaluate_extraction(self, content: str, extracted_data: Dict) -> Optional[Dict]:
        """
        Evaluate the extraction results.
        
        Args:
            content: Original content
            extracted_data: Extracted data to evaluate
            
        Returns:
            Evaluation results dictionary or None if failed
        """
        try:
            prompt = self._create_evaluation_prompt(content, extracted_data)
            
            response = self.bedrock.invoke_model(
                modelId=MODEL_ID,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens": MAX_TOKENS,
                    "temperature": TEMPERATURE,
                    "top_p": TOP_P
                })
            )
            
            response_body = json.loads(response['body'].read())
            return json.loads(response_body['completion'])
            
        except (ClientError, json.JSONDecodeError) as e:
            logging.error(f"Error evaluating extraction: {e}")
            return None
            
    def determine_group_level(self, content: str) -> Optional[Dict]:
        """
        Determine if content is at group level.
        
        Args:
            content: Content to analyze
            
        Returns:
            Group level analysis dictionary or None if failed
        """
        try:
            prompt = self._create_group_level_prompt(content)
            
            response = self.bedrock.invoke_model(
                modelId=MODEL_ID,
                body=json.dumps({
                    "prompt": prompt,
                    "max_tokens": MAX_TOKENS,
                    "temperature": TEMPERATURE,
                    "top_p": TOP_P
                })
            )
            
            response_body = json.loads(response['body'].read())
            return json.loads(response_body['completion'])
            
        except (ClientError, json.JSONDecodeError) as e:
            logging.error(f"Error determining group level: {e}")
            return None 