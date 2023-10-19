import os
import re

def remove_comments(sql_content):
    # Remove single line comments starting with --
    sql_content = re.sub(r'--.*\n', '', sql_content)
    
    # Remove multi-line comments between /* and */
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
    
    return sql_content

def main():
    directory = "./mysql_queries_qualified/"  # Replace with the directory containing your .SQL files
    output_file = "./q00.sql"  # The file where all content will be appended

    # Check if directory exists
    if not os.path.exists(directory):
        print(f"The directory {directory} does not exist.")
        return

    # Initialize an empty string to hold all SQL content
    all_sql_content = ""

    # Loop through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".sql"):
            filepath = os.path.join(directory, filename)
            
            # Read the SQL file
            with open(filepath, "r") as f:
                sql_content = f.read()
            
            # Remove comments
            sql_content = remove_comments(sql_content)
            
            # Append content to the master string
            all_sql_content += sql_content + "\n\n"

    # Write all content to the new file
    with open(output_file, "w") as f:
        f.write(all_sql_content)

    print(f"All SQL files have been merged into {output_file} without comments.")

if __name__ == "__main__":
    main()