import os
import re
from argparse import ArgumentParser
from glob import glob

table_name = "t0"


def find_all_derived_tables(sql_query):
    # List to store start and end indices of all derived tables
    derived_table_indices = []

    # Initial pattern to find a potential starting point for a derived table
    pattern = r"FROM\s*(?:[^,\(]*?,\s*)?\("

    # Current position in the SQL query
    curr_pos = 0

    while curr_pos < len(sql_query):
        # Find the next match of the pattern in the SQL query from the current position
        match = re.search(
            pattern, sql_query[curr_pos:], flags=re.IGNORECASE | re.DOTALL
        )

        if not match:
            break

        # Start index of the derived table relative to the current position
        start_idx_rel = match.end()

        # Actual start index of the derived table
        start_idx = curr_pos + start_idx_rel

        # Track the number of open parentheses
        open_parentheses = 1
        idx = start_idx

        # Traverse the query to find the matching closing parenthesis
        while idx < len(sql_query) and open_parentheses > 0:
            if sql_query[idx] == "(":
                open_parentheses += 1
            elif sql_query[idx] == ")":
                open_parentheses -= 1
            idx += 1

        # If all open parentheses have matching closing parentheses
        if open_parentheses == 0:
            derived_table_indices.append((start_idx - 1, idx))

        # Update the current position to search for the next derived table
        curr_pos = start_idx + 1

    return derived_table_indices


def derived_table_has_alias(query, end_ind):
    return re.match(r" {0,3}[a-zA-Z_]+", query[end_ind : end_ind + 4]) is not None


def add_alias_if_missing(query):
    global table_name

    # Find all derived table indices in the given query
    derived_tables = find_all_derived_tables(query)

    # Initialize a variable to hold the modified query
    modified_query = query

    # Variable to track the offset caused by adding aliases
    offset = 0

    for start, end in derived_tables:
        # Check if the derived table has an alias
        if not derived_table_has_alias(modified_query, end + offset):
            # If no alias, add one
            alias = " " + table_name
            modified_query = (
                modified_query[: end + offset] + alias + modified_query[end + offset :]
            )

            # Increment the offset by the length of the added alias
            offset += len(alias)

            # Update the global table_name for the next usage
            table_name = "t" + str(
                int(table_name[1:]) + 1
            )  # Increment the numeric part of the alias

    return modified_query


def add_aliases_to_all_derived_tables(query):
    global table_name
    table_name = "t0"
    # Starting position to search for 'FROM' in the query
    start_pos = 0

    # Variable to hold the progressively modified query
    modified_query = query

    while start_pos < len(modified_query):
        # Find the next occurrence of 'FROM' from the current position
        from_pos = modified_query.find("from", start_pos)

        # If 'FROM' not found, break out of the loop
        if from_pos == -1:
            break

        # Extract a segment of the query starting from the found 'FROM' position
        segment = modified_query[from_pos:]
        # Apply the 'add_alias_if_missing' function to this segment
        modified_segment = add_alias_if_missing(segment)

        # Replace the original segment with the modified segment in the main query
        modified_query = modified_query[:from_pos] + modified_segment

        # Update the starting position for the next iteration to the position after the current 'FROM'
        start_pos = from_pos + 1

    return modified_query


def correct_date_interval(sql_query):
    # Correct the date interval
    corrected_query = re.sub(
        r"(\s*\d+\s*)days", r" INTERVAL\1DAY", sql_query, flags=re.IGNORECASE
    )

    # Correct double BETWEEN
    corrected_query = re.sub(
        r"BETWEEN\s+BETWEEN", "BETWEEN", corrected_query, flags=re.IGNORECASE
    )

    return corrected_query


def rem_spaces_bw_func(sql_query, functions):
    """
    Find all the spaces between function names and parenthesis and remove them.
    e.g. sum ( -> sum(
         cast ( -> cast(
    Names of functions are in the list functions.
    """
    for func in functions:
        # Pattern to find 'function_name ' (with space(s) after it) followed by '('
        pattern = r"{} +\(".format(func)

        # Replacement pattern is the function name immediately followed by '('
        replacement = r"{}(".format(func)

        # Replace using regular expressions
        sql_query = re.sub(pattern, replacement, sql_query, flags=re.IGNORECASE)

    return sql_query


def convert_rollup_syntax(sql_query):
    # Regular expression to match 'group by rollup (column1, column2)'
    pattern = r"group\s+by\s+rollup\s*\(([^)]+)\)"

    # Replace with 'group by column1, column2 with rollup'
    converted_query = re.sub(
        pattern, r"group by \1 with rollup", sql_query, flags=re.IGNORECASE
    )

    return converted_query


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--sf", type=int, default=1)
    args = parser.parse_args()

    original_queries = glob(f"qstreams/{args.sf}/*.sql")
    tgt_dir = f"qstreams/{args.sf}/qmod"
    if not os.path.exists(tgt_dir):
        os.makedirs(tgt_dir)

    for filepath in original_queries:
        with open(filepath) as f:
            query = f.read().strip()

        modified_query = query
        modified_query = add_aliases_to_all_derived_tables(query)
        # for fn in ["2.sql", "14.sql", "23.sql", "49.sql"]:
        #     if filepath.endswith(fn):
        #         modified_query = add_aliases_to_all_derived_tables(query)
        #         break
        modified_query = correct_date_interval(modified_query)
        modified_query = rem_spaces_bw_func(modified_query, ["sum", "cast"])
        modified_query = convert_rollup_syntax(modified_query)
        if "c_last_review_date_sk" in modified_query:
            modified_query = modified_query.replace(
                "c_last_review_date_sk", "c_last_review_date"
            )
        if "cast((revenue/50) as int)" in modified_query:
            modified_query = modified_query.replace(
                "cast((revenue/50) as int)", "cast((revenue/50) as unsigned)"
            )
        with open(os.path.join(tgt_dir, os.path.basename(filepath)), "w") as f:
            f.write(modified_query)
        # still remaining
        # full outer join
        # issue with q49
