import glob
import os
import time
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import mysql.connector
from mysql.connector import pooling

from scripts.utils import log_time, reset_pool


def load_data_into_table(cnxpool, csv_file):
    # Acquire a connection from the pool
    conn = cnxpool.get_connection()
    cursor = conn.cursor()
    table_name = os.path.basename(csv_file)[:-4]
    # turn off autocommit, unique_checks, foreign_key_checks, sql_log_bin
    cursor.execute("SET autocommit=0;")
    cursor.execute("SET unique_checks=0;")
    cursor.execute("SET foreign_key_checks=0;")
    cursor.execute("SET sql_log_bin=0;")
    start_time = time.time()
    cursor.execute(
        (
            f"LOAD DATA LOCAL INFILE '{csv_file}' INTO TABLE {table_name} character set latin1 "
            "FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n';"
        )
    )
    elapsed_time = time.time() - start_time
    # Release the connection back to the pool
    conn.commit()
    cursor.close()
    conn.close()
    return elapsed_time


def load_test(sf: int, uid: str):
    log_file = f"results/load_test_sf={sf}_{uid}.txt"
    # Initialize connection
    conn = mysql.connector.connect(
        host="localhost", user="root", password="password", consume_results=True
    )
    cursor = conn.cursor()

    # 1. Database Creation
    db = "tpcds" if sf == 1 else f"tpcds{sf}"
    cursor.execute(f"DROP DATABASE IF EXISTS {db};")
    cursor.execute(f"CREATE DATABASE {db};")
    cursor.execute(f"USE {db};")
    # turn off autocommit, unique_checks, foreign_key_checks, sql_log_bin
    cursor.execute("SET autocommit=0;")
    cursor.execute("SET unique_checks=0;")
    cursor.execute("SET foreign_key_checks=0;")
    cursor.execute("SET sql_log_bin=0;")

    # 2. Table Creation
    with open("tools/tpcds.sql", "r") as f:
        table_creation_sql = f.read()
    initial_time = time.time()
    for result in cursor.execute(table_creation_sql, multi=True):
        if result.with_rows:
            result.fetchall()
    table_creation_time = time.time() - initial_time
    log_time(log_file, "Table Creation", table_creation_time)
    # 3. Data insertion
    # Initialize Connection Pool
    dbconfig = {
        "host": "localhost",
        "user": "root",
        "password": "password",
        "database": db,
        "allow_local_infile": True,
        "consume_results": True,
    }
    cnxpool = pooling.MySQLConnectionPool(
        pool_name="insertion_pool", pool_size=8, **dbconfig
    )
    csv_files = glob.glob(f"../data/{sf}/*.csv")
    # replace / with \ for Windows
    csv_files = list(map(os.path.abspath, csv_files))
    # replace \ with \\ for SQL
    csv_files = list(map(lambda x: x.replace("\\", "\\\\"), csv_files))
    start_time = time.time()
    max_thread_time = 0
    th_args = [(cnxpool, csv_file) for csv_file in csv_files]
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = executor.map(lambda p: load_data_into_table(*p), th_args)
        for result in results:
            if result > max_thread_time:
                max_thread_time = result
    elapsed_time = time.time() - start_time
    print("max time for a table is", max_thread_time)
    log_time(log_file, "Insertion", elapsed_time)
    # Reset the connection settings for each connection in the pool
    for _ in range(8):
        reset_pool(cnxpool)

    # 4. Referential Integrity
    with open("tools/tpcds_ri.sql", "r") as f:
        tpcds_ri_sql = f.read().strip()
    start_time = time.time()
    for result in cursor.execute(tpcds_ri_sql, multi=True):
        if result.with_rows:
            result.fetchall()
    elapsed_time = time.time() - start_time
    log_time(log_file, "Referential Integrity", elapsed_time)

    # 5. Analyze Tables
    start_time = time.time()
    for csv_file in csv_files:
        table_name = os.path.basename(csv_file)[:-4]
        cursor.execute(f"ANALYZE TABLE {table_name};")
    analyze_time = time.time() - start_time
    log_time(log_file, "Analyze Time", analyze_time)

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
    start_time = time.time()
    cursor.execute(q39_ct)
    cursor.execute(q39_data)
    elapsed_time = time.time() - start_time
    log_time(log_file, "EADS", elapsed_time)
    # finish
    cursor.execute("SET autocommit=1;")
    cursor.execute("SET unique_checks=1;")
    cursor.execute("SET foreign_key_checks=1;")
    cursor.execute("SET sql_log_bin=1;")
    end_time = time.time()
    total_time = end_time - initial_time
    log_time(log_file, "Total", total_time)
    conn.commit()
    cursor.close()
    conn.close()
    return total_time


if __name__ == "__main__":
    parser = ArgumentParser("load_test.py", description="Run load test")
    parser.add_argument("--sf", type=int, default=1, help="Scale factor")
    args = parser.parse_args()
    uid = datetime.now().strftime("%m-%d_%H-%M-%S")

    load_test(args.sf, uid)
