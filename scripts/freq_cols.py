import os
import re
from collections import defaultdict
from pprint import pprint

def extract_column_names(sql_file):
    column_names = defaultdict(list)
    current_table = None
    inside_create_table = False
    table = ''

    with open(sql_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('create table'):
                table = line.split(' ')[-1]
            elif line.endswith(","):
                col = line.split(" ")[0]
                column_names[table].append(col)
    return column_names

def search_and_count_columns(directory, column_names):
    column_counts = {}

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    content = f.read().lower()  # Read and convert to lowercase for case-insensitive matching
                    for table, columns in column_names.items():
                        if table in ["date_dim", "inventory", "catalog_sales", 
                                    "web_sales", "store_sales", "store_returns",
                                    "customer_demographics"]:
                            for column in columns:
                                # Use regex to find whole word matches
                                pattern = r'\b{}\b'.format(re.escape(column.lower()))
                                matches = len(re.findall(pattern, content))
                                column_key = f"{table}.{column}"
                                column_counts[column_key] = column_counts.get(column_key, 0) + matches
                                if column_counts[column_key] < 10:
                                    del column_counts[column_key]

    return column_counts

def main(directory):
    sql_file = "tools\\tpcds.sql"
    column_names = extract_column_names(sql_file)
    
    column_counts = search_and_count_columns(directory, column_names)
    sorted_column_counts = list(sorted(column_counts.items(), key=lambda item: item[1], reverse=True))

    return sorted_column_counts

result = main("queries\\1\\qmod")
pprint(result)
