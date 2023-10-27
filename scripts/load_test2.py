# \source "E:\Documents\BDMA\ULB\Data Warehouses\project1\DSGen-software-code-3.2.0rc1\TPC-DS-MySQL\scripts\load_test2.py" 1

import argparse
from mysqlsh import mysql
import mysqlsh
import time
import os
from datetime import datetime

base_dir = "E:/Documents/BDMA/ULB/Data Warehouses/project1/DSGen-software-code-3.2.0rc1"
os.chdir(base_dir)

parser = argparse.ArgumentParser(description="load test")
parser.add_argument("sf", type=int, help="An integer input for SF")
args = parser.parse_args() 

sf = args.sf
uid = datetime.now().strftime("%m-%d_%H-%M-%S")
log_file = f"TPC-DS-MySQL/results/load_test_sf={sf}_{uid}.txt"

# Function to log time measurements
def log_time(log_file, label, elapsed_time):
    with open(log_file, "a") as f:
        f.write(f"{label}: {elapsed_time}\n")

# Connect to the MySQL server
session = mysql.get_classic_session('mysql://root:password@localhost:3306')

# 1. Drop database if it exists and then create it

db_name = "tpcds"  if sf == 1 else f"tpcds{sf}"
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

csv_dir = f"data/{sf}"
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
session.run_sql("SET FOREIGN_KEY_CHECKS=0;")
start_time = time.time()

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
session.run_sql("SET FOREIGN_KEY_CHECKS=1;")

# 5. Run ANALYZE on all the tables
tables = session.run_sql("SHOW TABLES;").fetch_all()
start_time = time.time()

for table in tables:
    table_name = table[0]
    session.run_sql(f"ANALYZE TABLE {table_name};")
    
end_time = time.time()
log_time(log_file, "Analyze Tables", end_time - start_time)



# 6. Create EADS
q39_ct = """CREATE TABLE MV_INV (
    W_WAREHOUSE_NAME varchar(20),
    W_WAREHOUSE_SK integer,
    I_ITEM_SK integer,
    D_MOY integer,
    D_YEAR integer,
    STDEV float,
    MEAN float
);"""
q39_data = """INSERT INTO MV_INV
    SELECT
        W_WAREHOUSE_NAME,
        W_WAREHOUSE_SK,
        I_ITEM_SK,
        D_MOY,
        D_YEAR,
        STDDEV_SAMP(INV_QUANTITY_ON_HAND) AS STDEV,
        AVG(INV_QUANTITY_ON_HAND) AS MEAN
    FROM DATE_DIM
        JOIN INVENTORY ON INV_DATE_SK = D_DATE_SK
        JOIN ITEM ON INV_ITEM_SK = I_ITEM_SK
        JOIN WAREHOUSE ON INV_WAREHOUSE_SK = W_WAREHOUSE_SK
    GROUP BY W_WAREHOUSE_NAME, W_WAREHOUSE_SK, I_ITEM_SK, D_MOY, D_YEAR;"""
session.run_sql(q39_ct)
session.run_sql(q39_data)

# Measure the end time and calculate total time taken
elapsed_time = time.time() - initial_time
log_time(log_file, "Total", elapsed_time)