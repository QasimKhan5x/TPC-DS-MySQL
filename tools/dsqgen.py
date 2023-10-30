import os
import subprocess
import argparse
import shlex
from glob import glob
from pprint import pprint
import re
import os

def extract_number(s):
    # Extract the number from the string using a regex
    match = re.search(r'(\d+)', s)
    if match:
        return int(match.group(1))
    return 0

parser = argparse.ArgumentParser(description="Generate queries for scale factor")
parser.add_argument('--sf', type=int, default=1, help='An integer input for SF. Default is 1.')
args = parser.parse_args()

files = glob("../query_templates/query*.tpl")
query_files = sorted(files, key=extract_number)

DIRECTORY = "../query_templates"
OUTPUT_DIR = f"../queries/{args.sf}"
if not os.path.isdir(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

INPUT = "../query_templates/templates.lst"

for file in query_files:
    query_name = file.split("/")[-1][:-4]
    command_string = (
        f"./dsqgen -DIRECTORY {DIRECTORY} -OUTPUT_DIR {OUTPUT_DIR} "
        f"-RNGSEED 42 -DIALECT mysql -SCALE {args.sf} -TEMPLATE {file}"
    )
    cmd = shlex.split(command_string)
    result = subprocess.run(cmd, capture_output=True, text=True)
    os.rename(OUTPUT_DIR + "/query_0.sql", OUTPUT_DIR + f"/{query_name}.sql")
