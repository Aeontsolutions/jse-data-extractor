#!/bin/bash

# Export a list of tables, each on its own line
sqlite3 jse_financial_data.db ".tables" | tr ' ' '\n' | grep -v '^$' > table_list.txt

# Process each table
while read table; do
  # Skip empty lines
  if [ -z "$table" ]; then continue; fi
  
  echo "Exporting $table"
  # Use quotes to handle table names with special characters
  sqlite3 jse_financial_data.db ".mode csv" ".headers on" ".output csv_export/${table}.csv" "SELECT * FROM \"$table\";"
done < table_list.txt