import re


# Function to log time measurements
def log_time(filename, label, elapsed_time):
    with open(filename, "a") as log_file:
        log_file.write(f"{label}: {elapsed_time}\n")


def reset_pool(cnxpool):
    conn = cnxpool.get_connection()
    cursor = conn.cursor()

    # Reset the connection properties to their defaults
    cursor.execute("SET autocommit=1;")
    cursor.execute("SET unique_checks=1;")
    cursor.execute("SET foreign_key_checks=1;")
    cursor.execute("SET sql_log_bin=1;")

    conn.commit()
    cursor.close()
    conn.close()


def extract_number(s):
    # Extract the number from the string using a regex
    match = re.search(r"(\d+)", s)
    if match:
        return int(match.group(1))
    return 0


def relax_connection(cursor):
    """Relax the connection properties to improve performance"""
    cursor.execute("SET autocommit=0;")
    cursor.execute("SET unique_checks=0;")
    cursor.execute("SET foreign_key_checks=0;")
    cursor.execute("SET sql_log_bin=0;")


def reset_connection_settings(cursor):
    """Reset the connection properties to their defaults"""
    cursor.execute("SET autocommit=1;")
    cursor.execute("SET unique_checks=1;")
    cursor.execute("SET foreign_key_checks=1;")
    cursor.execute("SET sql_log_bin=1;")
