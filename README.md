# TPC-DS Benchmarking with MySQL and Python

This project demonstrates the implementation of TPC-DS benchmarking on a MySQL database using Python scripts to automate the process. The primary goal is to evaluate the performance and scalability of the MySQL database under different conditions, as defined by the TPC-DS benchmark.

## Getting Started

These instructions will help you set up and run the project on your local machine for development and testing purposes.

### Prerequisites

- Python 3.9
- MySQL Server
- TPC-DS Toolkit
- A bash-based command line tool to execute the bash scripts

### Installation

1. Clone the repository:
```bash
git clone https://github.com/QasimKhan5x/TPC-DS-MySQL
```
2. Navigate to the project directory:
```bash
cd TPC-DS-MySQL
```
3. Install the requisite python packages:
```bash
pip install -r requirements.txt
```

### Executing the benchmark

1. First, generate data for scale factor 1 using the `tools/dsdgen` program. You first need to compile it. It can be done on a Linux distribution using
```bash
cd tools
make
```
For Windows, use Visual Studio to open the `.sln` file and build the project.

2. Create a folder called `../data/1` and `../data-maintenance/1`. The data for SF=1 will go here. These folders should exist outside of the root folder. So, if you are in `tools`, you should make the directories two levels up from the current directory instead.
3. After building it, execute `./dsdgen` to build the data. Precise instructions regarding all the arguments are given in Section 3.3 of our report.
4. Remove empty values in each file by executing the following script:
```bash
chmod +x ./scripts/null_values.sh
./null_values 1
```
This means you give the bash script permission to act as an executable and then preprocess the data in `../data/1`

5. Create four folders in `../data-maintenance/1` called `1`, `2`, `3`, and `4`. The refresh data for each run of the data maintenance will go there.
6. Create the refresh data from `tools` using `./dsdgen`. The command to generate the data is given in Section 3.8. of our report.
7. Preprocess the generated refresh data for each set of refresh data using the following:
```bash
chmod +x ./scripts/null_values_dm.sh
./scripts/null_values_dm.sh 1 <n> # execute for n=1,2,3,4
```
8. `mkdir results` from the root directory. The results will go there.
9. Run the benchmark using the following:
```bash
python -m scripts.main
```
Text files will be generated in the `results` folder containing the results for each test.

Note: you don't need to create the queries as we included them in our repository. However, for other scale factors, you will have to consult our report to generate queries for the power test and throughput test. 

