#!/bin/bash

# Determine UID based on date and time
exp_id=$(date +"%m-%d_%H-%M-%S")
OUTPUT_FILE="results/load_test_$exp_id.txt"
# Database credentials
USER="root"
PASS="password"
DB="tpcds"

# File paths
SQL_FILE="/home/tools/tpcds.sql"
DATA_DIR="/home/data/1"
RI_FILE="/home/tools/tpcds_ri.sql"

# 1. Drop all existing tables
echo "Dropping all tables..."
mysql -h mysql -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;" 

# 2. Create tables
echo "Creating tables..."
mysql -h mysql $DB < $SQL_FILE

# 3. & 4. Start timer and begin data loading
declare -A TABLE_TIMES

echo "Loading data..."
# monitor system performance
dstat -t --all 2 > /home/results/dstat_log_$exp_id.txt &

# Change IFS to iterate over lines
IFS=$'\n'
for file in $(ls $DATA_DIR/*.csv); do
    table_name=$(basename $file .csv)
    echo "Loading $table_name..."
    
    start_time=$(date +%s.%N)  # start time for this table
    
    # Load the data from CSV to MySQL
    mysql -h mysql --local-infile=1 $DB -e "
        LOAD DATA LOCAL INFILE '$file' 
        INTO TABLE $table_name 
        FIELDS TERMINATED BY ',' 
        LINES TERMINATED BY '\n';
    "
    
    end_time=$(date +%s.%N)  # end time for this table
    elapsed_time=$(echo "$end_time - $start_time" | bc)
    
    TABLE_TIMES[$table_name]=$elapsed_time
done

# 5. Implement referential integrity
echo "Implementing referential integrity..."
mysql -h mysql $DB < $RI_FILE

# stop system performance reporting
pkill dstat

# 6. Report the time taken
total_time=0
for table in "${!TABLE_TIMES[@]}"; do
    echo "Time taken to load $table: ${TABLE_TIMES[$table]} seconds." | tee -a $OUTPUT_FILE
    total_time=$(echo "$total_time + ${TABLE_TIMES[$table]}" | bc)
done

echo "Total time taken for data insertion: $total_time seconds." | tee -a $OUTPUT_FILE
