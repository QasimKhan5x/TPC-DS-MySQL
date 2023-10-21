import argparse
import mysql.connector
from datetime import datetime
import os
import re
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


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


# Function to execute queries and measure time
def execute_queries(queries, cursor) -> dict:
    query_times = {}
    print("Executing queries...")
    for filename, query in queries.items():
        print("Executing query", filename, end="... ")
        start_time = datetime.now()
        try:
            cursor.execute(query)
            cursor.fetchall()
            while cursor.nextset():
                pass
            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()
            print(f"Query {filename} took {elapsed_time} seconds")
            query_times[filename] = elapsed_time
        except mysql.connector.errors.DatabaseError as e:
            with open("errors.txt", "a") as error_file:
                error_file.write(f"Query {filename} failed: {e}\n")
                print(f"Query {filename} failed: {e}")
    return query_times


# Function to save results to a file
def save_results_to_file(query_times, scale_factor):
    uid = datetime.now().strftime("%m-%d_%H-%M-%S")
    filename = f"results\\power_test_sf={scale_factor}_{uid}.txt"
    with open(filename, "w") as f:
        for time in query_times.values():
            f.write(f"{time}\n")
        f.write(f"Total Time: {sum(query_times.values())}\n")


# Function to plot horizontal histogram
def plot_histogram(query_times, scale_factor):
    uid = datetime.now().strftime("%m-%d_%H-%M-%S")
    filename = f"results/power_test_sf={scale_factor}_{uid}.png"
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
    plt.title("Time Taken for Each Query", fontsize=14)
    # Show the plot
    plt.savefig(filename)
    plt.show()


def perform_power_test(scale_factor, queries_directory):
    try:
        database = "tpcds" if scale_factor == 1 else f"tpcds{scale_factor}"
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database=database,
            charset="utf8",
        )
        cursor = conn.cursor()
        queries = read_sql_files(queries_directory)
        query_times = execute_queries(queries, cursor)
    except mysql.connector.Error as e:
        print(f"Connection failed: {e}")
    except KeyboardInterrupt:
        print("User interrupted the process.")
    else:
        save_results_to_file(query_times, scale_factor)
        plot_histogram(query_times, scale_factor)
    finally:
        cursor.close()
        conn.close()


def main():
    # Specify the directory containing the SQL files
    parser = argparse.ArgumentParser(description="Perform power test (provide SF and location of queries)")
    parser.add_argument('--sf', type=int, default=1, help='An integer input for SF. Default is 1.')
    parser.add_argument('--qdir', type=str, default="mysql_queries_qualified", help='Directory for queries to execute')
    args = parser.parse_args()
    # Run the power test
    perform_power_test(args.sf, args.qdir)

if __name__ == "__main__":
    main()