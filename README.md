# TPC-DS-MySQL

Benchmarking the MySQL DBMS on TPC-DS Benchmark

1. Generate data using ./dsdgen
2. Replace empty columns with '\N' using `scripts/null_values.sh`
3. Run `scripts/load_test.py`
4. Run `scripts/power_test.py`
5. Run `scripts/throughput_test.py`
