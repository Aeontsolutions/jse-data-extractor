"""
Schema definitions for the JSE Data Extractor.
"""

# Response schema for data extraction
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
                    "description": "Taken from the filename but conformed to the format %Y-%m-%d eg. 2024-11-30"
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

# Evaluation schema for extraction validation
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
            "description": "Brief explanation for the judgment. If FAIL, specify the primary rule(s) violated."
        },
        "missing_periods_found": {
            "type": "BOOLEAN",
            "description": "True if the evaluation identified relevant time periods present in CSV columns but missing from line item output."
        },
        "missing_grouped_totals_found": {
            "type": "BOOLEAN",
            "description": "True if the evaluation identified expected grouped totals/headings missing from the line item output."
        }
    },
    "required": ["evaluation_judgment", "evaluation_reasoning", "missing_periods_found", "missing_grouped_totals_found"]
}

# Group level determination schema
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