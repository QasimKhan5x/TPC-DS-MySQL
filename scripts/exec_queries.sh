#!/bin/bash

# Database credentials
USER="root"
PASS="password"
DB="tpcds"

# Folder containing SQL files
SQL_DIR="mysql_queries_qualified"
OUTPUT_FILE="results/query_debug.txt"
# Error log file
ERROR_LOG="errors.txt"

# Remove existing error log file if it exists
if [ -f $ERROR_LOG ]; then
    rm $ERROR_LOG
fi

# Run each SQL file
for sql_file in $SQL_DIR/*.sql; do
    # Skip if the file is query_0.sql
    if [ "$(basename $sql_file)" == "query_0.sql" ]; then
        echo "Skipping query_0.sql..."
        continue
    fi
    echo "Executing $sql_file..."
    # Run the SQL file using the MySQL client
    start_time=$(date +%s.%N)
    mysql $DB < $sql_file
    end_time=$(date +%s.%N)
    elapsed_time=$(echo "$end_time - $start_time" | bc)
    # Check the exit status of the MySQL command
    if [ $? -ne 0 ]; then
        echo "Error executing $sql_file"
        echo $sql_file >> $ERROR_LOG
    else
        echo "Time taken to execute $sql_file: $elapsed_time seconds." | tee -a $OUTPUT_FILE
    fi
done

# Check if errors were logged
if [ -f $ERROR_LOG ]; then
    echo "Some SQL files failed. See $ERROR_LOG for details."
else
    echo "All SQL files executed successfully."
fi
