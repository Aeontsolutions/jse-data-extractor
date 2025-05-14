# JSE Data Extractor

A Python tool for extracting financial data from Johannesburg Stock Exchange (JSE) statements using AWS Bedrock LLM.

## Features

- Extracts financial data from JSE statements
- Validates extracted data against JSON schemas
- Evaluates extraction quality
- Determines statement group level
- Stores results in DynamoDB
- Supports batch processing of multiple statements

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/jse-data-extractor.git
cd jse-data-extractor
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the package:
```bash
pip install -e .
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your AWS credentials and other settings
```

## Usage

### Process a Single Symbol

```bash
jse-extract --symbol ABC
```

### Process a Directory of Statements

```bash
jse-extract --directory /path/to/statements
```

### Specify a Custom Mapping CSV

```bash
jse-extract --directory /path/to/statements --mapping-csv /path/to/mapping.csv
```

## Project Structure

```
jse-data-extractor/
├── src/
│   └── jse_data_extractor/
│       ├── config/
│       │   └── settings.py
│       ├── models/
│       │   └── schemas.py
│       ├── processors/
│       │   └── extractor.py
│       ├── services/
│       │   ├── db_service.py
│       │   └── llm_service.py
│       ├── utils/
│       │   └── helpers.py
│       └── __main__.py
├── setup.py
└── README.md
```

## Configuration

The following environment variables are required:

- `AWS_ACCESS_KEY_ID`: Your AWS access key
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret key
- `AWS_REGION`: AWS region (e.g., us-east-1)
- `MODEL_ID`: AWS Bedrock model ID
- `DYNAMODB_TABLE_NAME`: Name of the DynamoDB table

## Development

1. Install development dependencies:
```bash
pip install -e ".[dev]"
```

2. Run tests:
```bash
pytest
```

3. Run linting:
```bash
flake8
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 