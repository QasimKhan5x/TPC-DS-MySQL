import os
import re
import random
import time
import threading

import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

# Initialize Connection Pool
dbconfig = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "tpcds",
    "charset": "utf8",
}
cnxpool = MySQLConnectionPool(pool_name="throughput_pool", pool_size=4, **dbconfig)


def read_sql_files(directory):
    queries = {}
    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".sql"):
            with open(os.path.join(directory, filename), "r") as f:
                sql_content = f.read()
                sql_content = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
                sql_content = " ".join(sql_content.split())
                # remove .sql from filename
                queries[filename[:-4]] = sql_content
    return queries


# Function to execute queries for a single thread
def execute_queries_thread(queries, thread_id):
    conn = cnxpool.get_connection()
    cursor = conn.cursor()

    shuffled_queries = random.sample(list(queries.values()), len(queries))
    print(f"Thread-{thread_id} is starting")

    for i, query in enumerate(shuffled_queries):
        try:
            cursor.execute(query)
            cursor.fetchall()
            while cursor.nextset():
                pass
        except mysql.connector.errors.DatabaseError as e:
            print(f"Thread-{thread_id} Query-{i+1} failed: {e}")
    cursor.close()
    conn.close()


# Function to perform the throughput test
def perform_throughput_test(directory):
    queries = read_sql_files(directory)

    # Create all threads
    threads = []
    for i in range(4):
        t = threading.Thread(target=execute_queries_thread, args=(queries, i))
        threads.append(t)

    start_time = time.time()
    # Start all threads
    for t in threads:
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # End time
    end_time = time.time()

    print(f"Total Throughput Time: {end_time - start_time} seconds")


# After the power test is done, run the throughput test
directory = "mysql_queries_qualified"
perform_throughput_test(directory)
