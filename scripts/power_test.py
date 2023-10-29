import argparse
import os
import re
import time
from datetime import datetime
from threading import Event

import matplotlib.pyplot as plt
import mysql.connector
import pandas as pd
import seaborn as sns

from utils import extract_number
from system_stats import stats_thread


def read_sql_files(directory):
    queries = {}
    for filename in sorted(os.listdir(directory), key=extract_number):
        if filename.endswith(".sql"):
            with open(os.path.join(directory, filename), "r") as f:
                sql_content = f.read()
                sql_content = re.sub(r"--.*$", "", sql_content, flags=re.MULTILINE)
                sql_content = " ".join(sql_content.split())
                # only capture query number
                queries[extract_number(filename)] = sql_content
    return queries


# Function to execute queries and measure time
def execute_queries(queries, cursor, monitor_resource_utils=False, 
                    cold_results_dir=None, warm_results_dir=None) -> dict:
    query_times = {}
    for filename, query in queries.items():
        print("Executing", filename, end="...\n")
        total_time = 0
        query_lst = query.strip().split(";")
        query_lst = list(filter(lambda x: len(x.strip()) > 0, query_lst))
        for q in query_lst:
            q = q.strip()
            # warm up the cache
            print("executing warmup query")
            # Select process & start thread for recording metrics
            if monitor_resource_utils:
                condition = Event()
                stats_thread(condition, cold_results_dir, filename)
            start_time = time.time()
            cursor.execute(query)
            cursor.fetchall()
            while cursor.nextset():
                pass
            if monitor_resource_utils:
                # stop thread
                condition.set()
                # otherwise the thread writes the next print statement to the log as well
                time.sleep(0.5) 
            print(
                "executed warmup query. took",
                time.time() - start_time,
                "seconds",
            )
            # Select process & start thread for recording metrics
            if monitor_resource_utils:
                condition = Event()
                stats_thread(condition, warm_results_dir, filename)

            # execute query again
            start_time = time.time()
            # session.run_sql(q)
            cursor.execute(query)
            cursor.fetchall()
            while cursor.nextset():
                pass
            end_time = time.time()
            elapsed_time = end_time - start_time
            total_time += elapsed_time

            if monitor_resource_utils:
                # stop thread
                condition.set()
                # otherwise the thread writes the next print statement to the log as well
                time.sleep(0.5) 

        print(f"{filename} took {total_time} seconds")
        query_times[filename] = total_time
    return query_times


# Function to save results to a file
def save_results_to_file(query_times, scale_factor, uid):
    filename = f"results/{scale_factor}_{uid}/power_test_sf={scale_factor}.txt"
    with open(filename, "w") as f:
        for time in query_times.values():
            f.write(f"{time}\n")
        f.write(f"Total Time: {sum(query_times.values())}\n")


# Function to plot horizontal histogram
def plot_histogram(query_times, scale_factor, uid):
    filename = f"results/{scale_factor}_{uid}/power_test_sf={scale_factor}.png"
    # Create the horizontal bar chart
    filenames, times = zip(*query_times.items())
    query_times_df = pd.DataFrame({"Query Number": filenames, "Time (seconds)": times})
    # Create the Seaborn plot
    plt.figure(figsize=(10, 30))  # Increase the vertical size
    ax = sns.barplot(
        x="Time (seconds)", y="Query Number", data=query_times_df, orient="h"
    )
    # Label the axes
    plt.xlabel("Time (seconds)", fontsize=12)
    plt.ylabel("Query Number", fontsize=12)
    # Increase y-tick font size
    ax.set_yticklabels(ax.get_yticklabels(), size=5)
    # Add title
    plt.title(f"Time Taken for Each Query. SF={scale_factor}", fontsize=14)
    # Show the plot
    plt.savefig(filename)
    # plt.show()


def power_test(scale_factor, queries_directory, uid, monitor_resource_utils=False):
    total_time = 0
    cold_results_directory_path = f"results/{scale_factor}_{uid}_cold"
    warm_results_directory_path = f"results/{scale_factor}_{uid}_warm"
    if monitor_resource_utils:
        if not os.path.exists(cold_results_directory_path):
            os.makedirs(cold_results_directory_path)
        if not os.path.exists(warm_results_directory_path):
            os.makedirs(warm_results_directory_path)
    try:
        database = "tpcds" if scale_factor == 1 else f"tpcds{scale_factor}"
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database=database,
            charset="utf8",
            consume_results=True,
        )
        cursor = conn.cursor()
        queries = read_sql_files(queries_directory)
        query_times = execute_queries(queries, cursor, monitor_resource_utils, cold_results_directory_path, warm_results_directory_path)
        total_time = sum(query_times.values())
    except mysql.connector.Error as e:
        print(f"Connection failed: {e}")
    except KeyboardInterrupt:
        print("User interrupted the process.")
    else:
        save_results_to_file(query_times, scale_factor, uid)
        plot_histogram(query_times, scale_factor, uid)
    finally:
        cursor.close()
        conn.close()
        return total_time


def main(args):
    uid = datetime.now().strftime("%m-%d_%H-%M-%S")
    # Run the power test
    power_test(args.sf, args.qdir, uid, args.mru)


if __name__ == "__main__":
    # Specify the directory containing the SQL files
    parser = argparse.ArgumentParser(
        description="Perform power test (provide SF and location of queries)"
    )
    parser.add_argument(
        "--sf", type=int, default=1, help="An integer input for SF. Default is 1."
    )
    parser.add_argument(
        "--qdir",
        type=str,
        default="queries/1/qmod",
        help="Directory for queries to execute",
    )
    parser.add_argument(
        "--mru", action='store_true', default=False, help="Whether to monitor resource utils"
    )

    args = parser.parse_args()
    main(args)
