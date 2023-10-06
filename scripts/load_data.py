import mysql.connector
from mysql.connector import pooling
import os
import time
import glob
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor


# Function to log time measurements
def log_time(filename, label, elapsed_time):
    with open(filename, "a") as log_file:
        log_file.write(f"{label}: {elapsed_time}\n")


uid = datetime.now().strftime("%m-%d_%H-%M-%S")
log_file = f"results/load_test_{uid}.txt"

# Initialize connection
conn = mysql.connector.connect(host="localhost", user="root", password="password")
cursor = conn.cursor()

# 1. Database Creation
# cursor.execute("DROP DATABASE IF EXISTS tpcds;")
# cursor.execute("CREATE DATABASE tpcds;")
cursor.execute("USE tpcds;")

# 2. Table Creation
# start_time = time.time()
# with open("tools/tpcds.sql", "r") as f:
#     table_creation_sql = f.read()
# for result in cursor.execute(table_creation_sql, multi=True):
#     if result.with_rows:
#         result.fetchall()
# table_creation_time = time.time() - start_time
# log_time(log_file, "Table Creation Time", table_creation_time)

# conn.commit()
# cursor.close()
# conn.close()

# Initialize Connection Pool
dbconfig = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "tpcds",
    "allow_local_infile": True,
}
cnxpool = pooling.MySQLConnectionPool(
    pool_name="insertion_pool", pool_size=8, **dbconfig
)

# 3. Data Insertion

csv_files = glob.glob("../data/1_nulls/*.csv")
# replace / with \ for Windows
csv_files = list(map(os.path.abspath, csv_files))
# replace \ with \\ for SQL
csv_files = list(map(lambda x: x.replace("\\", "\\\\"), csv_files))


# def load_data_into_table(csv_file):
#     # Acquire a connection from the pool
#     conn = cnxpool.get_connection()
#     cursor = conn.cursor()
#     table_name = os.path.basename(csv_file).replace(".csv", "")
#     cursor.execute("SET autocommit=0;")
#     start_time = time.time()
#     cursor.execute(
#         f"LOAD DATA LOCAL INFILE '{csv_file}' INTO TABLE {table_name} FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n';"
#     )
#     elapsed_time = time.time() - start_time
#     log_time(log_file, f"Data Insert Time for {table_name}", elapsed_time)
#     # Release the connection back to the pool
#     conn.commit()
#     cursor.close()
#     conn.close()
#     return elapsed_time


# max_thread_time = 0
# with ThreadPoolExecutor(max_workers=8) as executor:
#     start_time = time.time()
#     results = executor.map(load_data_into_table, csv_files)

#     for result in results:
#         if result > max_thread_time:
#             max_thread_time = result

# log_time(log_file, "Total Data Insert Time", max_thread_time)

# 4. Foreign Key Constraints


def execute_sql_statement(sql):
    start_time = time.time()
    # Acquire a connection from the pool
    conn = cnxpool.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except mysql.connector.errors.IntegrityError as e:
        print(e)
    else:
        print(f"Executed {sql}")
    elapsed_time = time.time() - start_time
    conn.commit()
    # Release the connection back to the pool
    cursor.close()
    conn.close()
    return elapsed_time


# start_time = time.time()
# with open("tools/tpcds_ri.sql", "r") as f:
#     lines = f.readlines()
# # Filter out comments and empty lines
# lines = [line.strip() for line in lines if not line.startswith("--") and line.strip()]
# # Combine lines into a single string and split into individual statements
# fk_sql_statements = ";".join(lines).split(";")
# # Remove any empty statements or whitespace
# fk_sql_statements = [stmt.strip() for stmt in fk_sql_statements if stmt.strip()]

# max_thread_time = 0
# with ThreadPoolExecutor(max_workers=8) as executor:
#     results = executor.map(execute_sql_statement, fk_sql_statements)

#     for result in results:
#         if result > max_thread_time:
#             max_thread_time = result

# log_time(log_file, "Total FK Time", max_thread_time)

# # 5. Analyze Tables
start_time = time.time()
for csv_file in csv_files:
    table_name = os.path.basename(csv_file).replace(".csv", "")
    cursor.execute(f"ANALYZE TABLE {table_name};")
    cursor.fetchall()
analyze_time = time.time() - start_time
log_time(log_file, "Analyze Time", analyze_time)