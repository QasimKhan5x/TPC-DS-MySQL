#!/bin/bash

# 1_nulls is a copy of the original folder represented by the SF=1
input_dir="../data/1_nulls"
# Loop over each CSV file in the directory
for csv_file in "$input_dir"/*.csv; do
  echo "Processing $csv_file..."
  sed -i ':a; s/||/|\\N|/g; ta' "$csv_file"
done