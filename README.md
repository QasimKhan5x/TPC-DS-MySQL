# TPC-DS-MySQL

Benchmarking the MySQL DBMS on TPC-DS Benchmark

1. Generate data using ./dsdgen
2. Replace empty columns with '\N' using `scripts/null_values.sh`
3. Run `scripts/load_test.py` using [MySQL Shell](https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-features.html)
5. Run `scripts/power_test.py`
6. Run `scripts/throughput_test.py`
