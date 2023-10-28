import argparse
import os
import time
from datetime import datetime
from glob import glob
from os.path import join as pjoin

import mysql.connector


# Function to log time measurements
def log_time(filename, label, elapsed_time):
    with open(filename, "a") as log_file:
        log_file.write(f"{label}: {elapsed_time}\n")


parser = argparse.ArgumentParser(description="load test")
parser.add_argument("sf", type=int, help="An integer input for SF")
parser.add_argument("n", type=int, help="DM Test 1 or 2")
args = parser.parse_args()

uid = datetime.now().strftime("%m-%d_%H-%M-%S")
log_file = f"results/dm_test_sf={args.sf}_n={args.n}_{uid}.txt"

db_name = "tpcds_dm" if args.sf == 1 else f"tpcds{args.sf}"
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database=db_name,
    allow_local_infile=True,
)
cursor = conn.cursor()


def run_query(query):
    print("executing query:", query)
    cursor.execute(query)
    print(cursor.rowcount, "rows affected.")
    cursor.fetchall()
    while cursor.nextset():
        pass


# S_q = 4
if args.n == 1:
    path_1 = pjoin("..", "data-maintenance", str(args.sf), "1")
    path_2 = pjoin("..", "data-maintenance", str(args.sf), "2")
else:
    path_1 = pjoin("..", "data-maintenance", str(args.sf), "3")
    path_2 = pjoin("..", "data-maintenance", str(args.sf), "4")
all_files_1 = glob(pjoin(path_1, "*.csv"))
all_files_2 = glob(pjoin(path_2, "*.csv"))


def relax_connection():
    """Relax the connection properties to improve performance"""
    cursor.execute("SET autocommit=0;")
    cursor.execute("SET unique_checks=0;")
    cursor.execute("SET foreign_key_checks=0;")
    cursor.execute("SET sql_log_bin=0;")


def reset_connection_settings():
    """Reset the connection properties to their defaults"""
    cursor.execute("SET autocommit=1;")
    cursor.execute("SET unique_checks=1;")
    cursor.execute("SET foreign_key_checks=1;")
    cursor.execute("SET sql_log_bin=1;")


def fact_delete_data_maintenance(dates, prefix):
    start, end = dates
    sales = f"{prefix}_sales"
    returns = f"{prefix}_returns"
    if prefix == "store":
        returns_col = "sr_ticket_number"
        sales_col = "ss_ticket_number"
        date_col = "ss_sold_date_sk"
    elif prefix == "web":
        returns_col = "wr_order_number"
        sales_col = "ws_order_number"
        date_col = "ws_sold_date_sk"
    else:
        returns_col = "cr_order_number"
        sales_col = "cs_order_number"
        date_col = "cs_sold_date_sk"

    deletion_query_1 = f"""DELETE FROM {returns}
    WHERE {returns_col} IN (
        SELECT {sales_col} FROM {sales}
        WHERE {date_col} IN (
            SELECT d_date_sk FROM date_dim
            WHERE d_date between '{start}' and '{end}'
        )
    );"""
    deletion_query_2 = f"""DELETE FROM {sales}
    WHERE {date_col} IN (
        SELECT d_date_sk FROM date_dim
        WHERE d_date between '{start}' and '{end}'
    );"""
    run_query(deletion_query_1)
    run_query(deletion_query_2)


def inventory_delete_data_maintenance(dates):
    start, end = dates
    deletion_query = f"""DELETE FROM INVENTORY
    WHERE inv_date_sk IN (
        SELECT d_date_sk FROM date_dim
        WHERE d_date between '{start}' and '{end}' 
    );"""
    run_query(deletion_query)


def data_maintenance(all_files: list[str]):
    relax_connection()
    # 1. data deletion
    deletion_files = list(filter(lambda x: "delete" in x, all_files))
    facts_file, inventory_file = deletion_files
    with open(facts_file) as f:
        fact_dates = f.readlines()
    fact_dates = [date.strip().split("|") for date in fact_dates]
    with open(inventory_file) as f:
        inventory_dates = f.readlines()
    inventory_dates = [date.strip().split("|") for date in inventory_dates]
    start_time = time.time()
    for fact in ["catalog", "store", "web"]:
        for date in fact_dates:
            pass
            # fact_delete_data_maintenance(date, fact)
    for date in inventory_dates:
        pass
        # inventory_delete_data_maintenance(date)
    end_time = time.time()
    deletion_elapsed_time = end_time - start_time
    log_time(log_file, "Deletion", deletion_elapsed_time)
    # 2. create staging area
    with open("tools/tpcds_source.sql", "r") as f:
        table_creation_sql = f.read()
    start_time = time.time()
    for result in cursor.execute(table_creation_sql, multi=True):
        if result.with_rows:
            result.fetchall()
    table_creation_time = time.time() - start_time
    log_time(log_file, "Table Creation", table_creation_time)
    # 3. load data into staging area
    required_flat_files = [
        "s_catalog_order",
        "s_catalog_order_lineitem",
        "s_catalog_returns",
        "s_inventory",
        "s_purchase",
        "s_purchase_lineitem",
        "s_store_returns",
        "s_web_order",
        "s_web_order_lineitem",
        "s_web_returns",
    ]
    data_files = list(
        filter(
            lambda x: os.path.basename(x).split(".")[0][:-2] in required_flat_files,
            all_files,
        )
    )
    # replace / with \ for Windows
    data_files = list(map(os.path.abspath, data_files))
    # replace \ with \\ for SQL
    data_files = list(map(lambda x: x.replace("\\", "\\\\"), data_files))
    start_time = time.time()
    for data_file in data_files:
        query = (
            f"LOAD DATA LOCAL INFILE '{data_file}' "
            f"INTO TABLE {os.path.basename(data_file).split('.')[0][:-2]} "
            "FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n';"
        )
        cursor.execute(query)
        print(cursor.rowcount, "rows affected.")
    end_time = time.time()
    load_data_elapsed_time = end_time - start_time
    log_time(log_file, "Data Insertion", load_data_elapsed_time)
    # 4. Create views
    with open("scripts/dm_views.sql", "r") as f:
        view_creation_sql = f.read().strip()
    start_time = time.time()
    for result in cursor.execute(view_creation_sql, multi=True):
        if result.with_rows:
            result.fetchall()
    view_creation_time = time.time() - start_time
    log_time(log_file, "View Creation", view_creation_time)
    views = ["crv", "csv", "iv", "srv", "ssv", "wrv", "wsv"]
    # 5. load data into fact tables
    tables = [
        "catalog_returns",
        "catalog_sales",
        "inventory",
        "store_returns",
        "store_sales",
        "web_returns",
        "web_sales",
    ]
    start_time = time.time()
    for view, table in zip(views, tables):
        query = f"INSERT INTO {table} SELECT * FROM {view};"
        run_query(query)
    end_time = time.time()
    insertion_elapsed_time = end_time - start_time
    log_time(log_file, "Insertion", insertion_elapsed_time)
    # 6. recreate EADS
    q39_drop = "DROP TABLE IF EXISTS MV_INV;"
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
    run_query(q39_drop)
    run_query(q39_ct)
    run_query(q39_data)
    end_time = time.time()
    view_updation_time = end_time - start_time
    log_time(log_file, "View Updation", view_updation_time)
    reset_connection_settings()

    total_time = (
        deletion_elapsed_time
        + table_creation_time
        + load_data_elapsed_time
        + view_creation_time
        + insertion_elapsed_time
        + view_updation_time
    )
    return total_time


t1 = data_maintenance(all_files_1)
conn.commit()
t2 = data_maintenance(all_files_2)
conn.commit()
cursor.close()
conn.close()
log_time(log_file, f"Total = {t1} + {t2}", t1 + t2)
