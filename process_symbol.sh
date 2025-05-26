#!/bin/bash

# Check if symbols were provided
if [ -z "$1" ]; then
    echo "Error: Please provide symbol(s) as an argument"
    echo "Usage: ./process_symbol.sh SYMBOL1,SYMBOL2,SYMBOL3"
    echo "Example: ./process_symbol.sh JPS,NCBJ,SCJ"
    exit 1
fi

# Convert comma-separated list to array (no spaces allowed)
IFS=',' read -ra SYMBOLS <<< "${1// /}"

echo "Starting processing for ${#SYMBOLS[@]} symbol(s): ${SYMBOLS[*]}"
echo "----------------------------------------"

# Process each symbol
for SYMBOL in "${SYMBOLS[@]}"; do
    echo "Processing symbol: $SYMBOL"
    echo "----------------------------------------"

    # Step 1: Run the data extraction script
    echo "Step 1: Running data extraction..."
    python3 jse_data_extractor_genai.py -s "$SYMBOL"

    # Check if the extraction was successful
    if [ $? -ne 0 ]; then
        echo "Error: Data extraction failed for symbol $SYMBOL"
        echo "Skipping BigQuery loading for $SYMBOL"
        continue
    fi

    echo "Data extraction completed successfully for $SYMBOL"
    echo "----------------------------------------"

    # Step 2: Run the BigQuery loading script
    echo "Step 2: Loading data to BigQuery..."
    python3 sqlite_to_bq.py -s "$SYMBOL"

    # Check if the BigQuery loading was successful
    if [ $? -ne 0 ]; then
        echo "Error: BigQuery loading failed for symbol $SYMBOL"
        continue
    fi

    echo "BigQuery loading completed successfully for $SYMBOL"
    echo "----------------------------------------"
done

echo "Processing completed for all symbols" 