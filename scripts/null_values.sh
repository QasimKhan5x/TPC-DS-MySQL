#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <leaf_directory>"
    exit 1
fi

# Take the leaf directory as input
input_dir="../data/$1"

# Check if directory exists
if [ ! -d "$input_dir" ]; then
    echo "Directory $input_dir does not exist!"
    exit 1
fi

# Loop over each CSV file in the directory
for csv_file in "$input_dir"/*.csv; do
  echo "Processing $csv_file..."
  
  # Replace occurrences of "||" with "|\\N|"
  sed -i ':a; s/||/|\\N|/g; ta' "$csv_file"
  
  # If a line ends with "|", replace it with "|\N"
  sed -i 's/|$/|\\N/' "$csv_file"
done
