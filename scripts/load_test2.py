from mysqlsh import mysql
import mysqlsh
import time
import os
from datetime import datetime

base_dir = "E:/Documents/BDMA/ULB/Data Warehouses/project1/DSGen-software-code-3.2.0rc1"
os.chdir(base_dir)

uid = datetime.now().strftime("%m-%d_%H-%M-%S")
log_file = f"TPC-DS-MySQL/results/load_test_{uid}.txt"

# Function to log time measurements
def log_time(log_file, label, elapsed_time):
    with open(log_file, "a") as f:
        f.write(f"{label}: {elapsed_time}\n")

# Connect to the MySQL server
session = mysql.get_classic_session('mysql://root:password@localhost:3306')

# 1. Drop database if it exists and then create it
db_name = "tpcds"  # example database name
session.run_sql(f"DROP DATABASE IF EXISTS {db_name};")
session.run_sql(f"CREATE DATABASE {db_name};")
session.run_sql(f"USE {db_name};")


# 2. Create tables using an SQL file

initial_time = time.time()

path = "TPC-DS-MySQL/tools/tpcds.sql"
with open(path, "r") as f:
    sql_commands = f.read().split(';')
    for command in sql_commands:
        if command.strip():
            session.run_sql(command)
            
log_time(log_file, "Table Creation", time.time() - initial_time)

# 3. Load data into tables from CSV files using util.import_table

csv_dir = "data/1"
start_time = time.time()

for csv_file in os.listdir(csv_dir):
    table_name = os.path.splitext(csv_file)[0]  # Assuming table name is same as CSV file name without extension
    full_path = os.path.join(csv_dir, csv_file)
    options = {
        "schema": db_name,
        "table": table_name,
        "linesTerminatedBy": "\n",
        "fieldsTerminatedBy": "|",
        "characterSet": "latin1"
    }
    mysqlsh.globals.util.import_table(full_path, options)

log_time(log_file, "Data Insertion", time.time() - start_time)


# 4. Execute another SQL file for ALTER commands

path = "TPC-DS-MySQL/tools/tpcds_ri.sql"
start_time = time.time()

# path = "tools/tpcds_ri.sql"
with open(path, "r") as f:
    sql_commands = f.read().split(';')
    for command in sql_commands:
        if command.strip():
            try:
                session.run_sql(command)
            except:
                print("Failed:", command)
            else:
                print("Success:", command)
log_time(log_file, "Referential Integrity", time.time() - start_time)

# 5. Run ANALYZE on all the tables
tables = session.run_sql("SHOW TABLES;").fetch_all()
start_time = time.time()

for table in tables:
    table_name = table[0]
    session.run_sql(f"ANALYZE TABLE {table_name};")
    
end_time = time.time()
log_time(log_file, "Analyze Tables", end_time - start_time)

elapsed_time = end_time - initial_time
# Measure the end time and calculate total time taken
print("Total time taken:", elapsed_time, "seconds")
