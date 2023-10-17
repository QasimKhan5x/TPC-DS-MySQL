#!/bin/bash

export LC_ALL=C
export LANG=C

# Determine UID based on date and time
exp_id=$(date +"%m-%d_%H-%M-%S")
OUTPUT_FILE="results/load_test_conc_$exp_id.txt"
# Database credentials
DB="tpcds10"

export OUTPUT_FILE
export DB

# File paths
DATA_DIR="../data/1_nulls/"
SQL_FILE="tools/tpcds.sql"

# 1. Drop all existing tables
echo "Dropping all tables..."
mysql -e "DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB;" 

# 2. Create tables
echo "Creating tables..."
start_time=$(date +%s.%N) # start time before tables created
mysql $DB < $SQL_FILE
end_time=$(date +%s.%N)  # end time for creating all tables
elapsed_time=$(echo "$end_time - $start_time" | bc)
echo "Total time taken for table creation: $elapsed_time seconds." | tee -a $OUTPUT_FILE

# 3. & 4. Start timer and begin data loading
echo "Loading data..."
mysql -e "SET AUTOCOMMIT=0;"
# Change IFS to iterate over lines
IFS=$'\n'
# monitor system performance
dstat -t --all 2 > results/dstat_log_$exp_id.txt &

load_data() {
    local file="$1"
    local table_name=$(basename $file .csv)
    
    start_time=$(date +%s.%N)  # start time for this table
    
    # Load the data from CSV to MySQL
    mysql --local-infile=1 $DB -e "
        LOAD DATA LOCAL INFILE '$file' 
        INTO TABLE $table_name 
        FIELDS TERMINATED BY '|' 
        LINES TERMINATED BY '\n';
    "

    end_time=$(date +%s.%N)  # end time for this table
    elapsed_time=$(echo "$end_time - $start_time" | bc)
    echo "Time taken to load $table_name: $elapsed_time seconds."
}

export -f load_data

total_start_time=$(date +%s.%N)  # Total start time before data loading
parallel -j 4 load_data ::: $DATA_DIR/*.csv > $OUTPUT_FILE
total_end_time=$(date +%s.%N)  # Total end time after data loading
# stop system performance reporting
pkill dstat
total_elapsed_time=$(echo "$total_end_time - $total_start_time" | bc)
echo "Total time taken for data insertion: $total_elapsed_time seconds." | tee -a $OUTPUT_FILE

# # Implement referential integrity after load test
# echo "Implementing referential integrity..."

# start_time=$(date +%s.%N)

# RI_FILE="tools/tpcds_ri.sql"
# mysql $DB < $RI_FILE

# end_time=$(date +%s.%N)  # Total end time after data loading
# elapsed_time=$(echo "$end_time - $start_time" | bc)
# echo "Total time taken for referential integrity: $elapsed_time seconds."