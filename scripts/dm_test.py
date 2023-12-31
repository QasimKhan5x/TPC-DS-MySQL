import argparse
import os
import time
from datetime import datetime
from glob import glob
from os.path import join as pjoin

import mysql.connector

from scripts.utils import log_time, relax_connection, reset_connection_settings


def run_query(cursor, query):
    cursor.execute(query)
    print(cursor.rowcount, "rows affected.")
    cursor.fetchall()
    while cursor.nextset():
        pass


def fact_delete_data_maintenance(cursor, dates, prefix):
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
    run_query(cursor, deletion_query_1)
    run_query(cursor, deletion_query_2)


def inventory_delete_data_maintenance(cursor, dates):
    start, end = dates
    deletion_query = f"""DELETE FROM INVENTORY
    WHERE inv_date_sk IN (
        SELECT d_date_sk FROM date_dim
        WHERE d_date between '{start}' and '{end}' 
    );"""
    run_query(cursor, deletion_query)


def data_maintenance(cursor, all_files: list[str], log_file: str):
    relax_connection(cursor)

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
            fact_delete_data_maintenance(cursor, date, fact)
    for date in inventory_dates:
        inventory_delete_data_maintenance(cursor, date)
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
    # get absolute path of file for mysqld
    data_files = list(map(os.path.abspath, data_files))
    if os.name == "nt":
        # replace \ with \\ for windows-based system
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
    log_time(log_file, "Data Loading", load_data_elapsed_time)

    # 4. Create views
    with open("scripts/dm_views.sql", "r") as f:
        view_creation_sql = f.read().strip()
    start_time = time.time()
    for result in cursor.execute(view_creation_sql, multi=True):
        if result.with_rows:
            result.fetchall()
    view_creation_time = time.time() - start_time
    log_time(log_file, "View Creation", view_creation_time)

    # 5. load data into fact tables
    views = ["crv", "csv", "srv", "ssv", "wrv", "wsv", "iv"]
    view2cols = {}
    for view_name in views:
        cursor.execute(f"DESCRIBE {view_name};")
        columns_description = cursor.fetchall()
        column_names = [column[0] for column in columns_description]
        view2cols[view_name] = column_names
    tables = [
        "catalog_returns",
        "catalog_sales",
        "store_returns",
        "store_sales",
        "web_returns",
        "web_sales",
        "inventory",
    ]
    table2cols = {}
    for table_name in tables:
        cursor.execute(f"DESCRIBE {table_name};")
        columns_description = cursor.fetchall()
        column_names = [column[0] for column in columns_description]
        table2cols[table_name] = column_names
    primary_key_idx = [[2, 16], [15, 17], [2, 9], [2, 9], [2, 13], [3, 17], [0, 1, 2]]
    pk_not_null_clause = []
    for indxs, table_name in zip(primary_key_idx, table2cols):
        clause = []
        for ind in indxs:
            clause.append(f"{table2cols[table_name][ind]} IS NOT NULL")
        clause = " AND ".join(clause)
        pk_not_null_clause.append(clause)
    start_time = time.time()
    for view, table, pk_idx, pk_not_null in zip(view2cols, table2cols, primary_key_idx, pk_not_null_clause):
        # SELECT sub.col1, sub.col2, ..., sub.colN
        selection_clause = ", ".join(f"sub.{col}" for col in table2cols[table])
        # view_name.col1 AS target_col1, view_name.col2 AS target_col2, ..., view_name.colN AS target_colN
        rename_clause = ", ".join(
            f"{view_col} AS {table_col}"
            for view_col, table_col in zip(view2cols[view], table2cols[table])
        )
        # col1 = VALUES(col1), col2 = VALUES(col2), ..., colN = VALUES(colN)
        update_clause = ", ".join(f"{col} = VALUES({col})" for col in table2cols[table])
        if table == "inventory":
            query = f"""INSERT INTO inventory
            SELECT {selection_clause} FROM (
                SELECT {rename_clause}, 
                ROW_NUMBER() OVER (
                    PARTITION BY {view2cols[view][pk_idx[0]]}, 
                    {view2cols[view][pk_idx[1]]}, {view2cols[view][pk_idx[2]]}
                    ORDER BY {view2cols[view][0]} DESC
                ) AS rn
                FROM {view}
            ) AS sub
            WHERE sub.rn = 1 AND {pk_not_null}
            ON DUPLICATE KEY UPDATE {update_clause};"""
        else:
            query = f"""INSERT INTO {table}
            SELECT {selection_clause} FROM (
                SELECT {rename_clause}, 
                ROW_NUMBER() OVER (PARTITION BY {view2cols[view][pk_idx[0]]}, 
                    {view2cols[view][pk_idx[1]]} ORDER BY {view2cols[view][0]} DESC, 
                    {view2cols[view][1]} DESC) AS rn
                FROM {view}
            ) AS sub
            WHERE sub.rn = 1 AND {pk_not_null}
            ON DUPLICATE KEY UPDATE {update_clause};"""
        run_query(cursor, query)
    end_time = time.time()
    insertion_elapsed_time = end_time - start_time
    log_time(log_file, "Data Insertion", insertion_elapsed_time)
    # 6. recreate EADS
    q39_delete = """TRUNCATE TABLE MV_INV;"""
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
    run_query(cursor, q39_delete)
    run_query(cursor, q39_data)
    end_time = time.time()
    view_updation_time = end_time - start_time
    log_time(log_file, "View Updation", view_updation_time)
    reset_connection_settings(cursor)

    total_time = (
        deletion_elapsed_time
        + table_creation_time
        + load_data_elapsed_time
        + view_creation_time
        + insertion_elapsed_time
        + view_updation_time
    )
    return total_time


def data_maintenance_test(test_num, scale_factor, uid):
    log_file = f"results/dm_test_sf={scale_factor}_n={test_num}_{uid}.txt"

    db_name = "tpcds" if scale_factor == 1 else f"tpcds{scale_factor}"
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database=db_name,
        allow_local_infile=True,
        consume_results=True,
    )
    cursor = conn.cursor()
    # S_q = 4
    if test_num == 1:
        path_1 = pjoin("..", "data-maintenance", str(scale_factor), "1")
        path_2 = pjoin("..", "data-maintenance", str(scale_factor), "2")
    else:
        path_1 = pjoin("..", "data-maintenance", str(scale_factor), "3")
        path_2 = pjoin("..", "data-maintenance", str(scale_factor), "4")
    all_files_1 = glob(pjoin(path_1, "*.csv"))
    all_files_2 = glob(pjoin(path_2, "*.csv"))
    t1 = data_maintenance(cursor, all_files_1, log_file)
    conn.commit()
    t2 = data_maintenance(cursor, all_files_2, log_file)
    conn.commit()
    cursor.close()
    conn.close()
    log_time(log_file, f"Total = {t1} + {t2}", t1 + t2)
    return t1, t2, t1 + t2


def main(args):
    uid = datetime.now().strftime("%m-%d_%H-%M-%S")
    data_maintenance_test(args.test_num, args.sf, uid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load test")
    parser.add_argument("--sf", type=int, default=1, help="An integer input for SF")
    parser.add_argument("--test_num", type=int, default=1, help="DM Test 1 or 2")
    args = parser.parse_args()
    main(args)
