import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from mysql.connector.pooling import MySQLConnectionPool


def read_sql_files(directory):
    queries = []
    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".sql"):
            with open(os.path.join(directory, filename)) as f:
                sql_content = f.read()
            queries.append(sql_content)
    return queries


# Function to execute queries for a single thread
def execute_query_stream(qstream, cnxpool):
    conn = cnxpool.get_connection()
    cursor = conn.cursor()
    for result in cursor.execute(qstream, multi=True):
        if result.with_rows:
            result.fetchall()
    conn.commit()
    cursor.close()
    conn.close()


# Function to perform the throughput test
def throughput_test(sf, directory, uid):
    # Initialize Connection Pool
    dbconfig = {
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": f"tpcds" if sf == 1 else f"tpcds{sf}",
        "charset": "utf8",
        "consume_results": True,
    }
    cnxpool = MySQLConnectionPool(pool_name="throughput_pool", pool_size=4, **dbconfig)
    queries = read_sql_files(directory)
    start_time = time.time()
    # Create 4 threads and execute the function for each SQL string
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Using a lambda to pass cnxpool to the execute_sql_statements function
        executor.map(lambda sql: execute_query_stream(sql, cnxpool), queries)
    # End time
    end_time = time.time()
    elapsed_time = end_time - start_time
    with open(f"results/tp_sf={sf}_{uid}.txt", "w") as f:
        f.write(f"Total Throughput Time: {elapsed_time} seconds\n")
        f.write(f"Scale Factor: {sf}\n")
        f.write(f"Number of Queries: {len(queries)}\n")
        f.write(f"Queries Directory: {directory}\n")
    return elapsed_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-sf",
        "--scale-factor",
        type=int,
        default=1,
        help="Scale factor to use for the throughput test",
    )
    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        default="queries",
        help="Directory containing the SQL files",
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        default=1,
        help="Throughput test 1 or 2",
    )
    args = parser.parse_args()

    sf = args.scale_factor
    directory = f"{args.directory}/{sf}/qmod"
    uid = f"n={args.n}_" + datetime.now().strftime("%m-%d_%H-%M-%S")
    throughput_test(sf, directory, uid)
