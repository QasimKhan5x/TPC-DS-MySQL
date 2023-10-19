import mysql.connector


def drop_foreign_keys(host, user, password, database):
    # Step 1: Connect to the database
    mydb = mysql.connector.connect(
        host=host, user=user, password=password, database=database
    )
    cursor = mydb.cursor()

    # Step 2: Retrieve Table Names
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]

        # Step 3: Identify Foreign Keys
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        create_table_script = cursor.fetchone()[1]
        lines = create_table_script.split("\n")
        foreign_keys = [
            line.strip()
            for line in lines
            if "CONSTRAINT" in line and "FOREIGN KEY" in line
        ]

        # Step 4: Drop Foreign Keys
        for fk in foreign_keys:
            constraint_name = (
                fk.split("CONSTRAINT")[1]
                .split("FOREIGN KEY")[0]
                .strip()
                .replace("`", "")
            )
            drop_fk_query = (
                f"ALTER TABLE {table_name} DROP FOREIGN KEY {constraint_name};"
            )
            cursor.execute(drop_fk_query)
            mydb.commit()

    cursor.close()
    mydb.close()


# Usage
drop_foreign_keys("localhost", "root", "password", "tpcds2")
