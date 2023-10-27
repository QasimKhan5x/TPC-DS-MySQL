import os
import time
from contextlib import redirect_stdout

import psutil


def disk_usage_calculation(p, sleep_time=0):
    """
    Calculate the process & system disk usage and derive disk percentage
    process_disk_percent = disk_usage_calculation(p) where p is the process.

    https://stackoverflow.com/questions/63774242/get-usage-disk-of-a-windows-process-by-name-in-python
    https://groups.google.com/g/psutil/c/Yepd8QNTKF0
    """
    time.sleep(sleep_time)
    disk_io_counter = psutil.disk_io_counters()
    disk_total = disk_io_counter[2] + disk_io_counter[3]  # read_bytes + write_bytes
    io_counters = p.io_counters()
    process_disk_usage = (
        io_counters[2] + io_counters[3]
    )  # read_bytes + write_bytes for the process
    percent_disk_usage = (process_disk_usage / disk_total) * 100
    path = "stats/disk_stat.txt"
    with open(path, "a") as file:
        with redirect_stdout(file):
            print(percent_disk_usage)


def check_process_id():
    for proc in psutil.process_iter():
        if "mysqld.exe" in proc.name():
            print(proc.pid)  # I get 2 Processes, relevant process for me is 5624


def cpu_usage_calculation(p, interval=0.1):
    path = "stats/cpu_stat.txt"
    with open(path, "a") as file:
        with redirect_stdout(file):
            cpu_usage = p.cpu_percent(interval)
            print(cpu_usage)


def memory_usage_calculation(p, type, sleep_time=0):
    # p.memory_info() --> gives all memory information values
    # Note : memory_full_info() gave access denied problems on Windows
    time.sleep(sleep_time)
    path = f"stats/memory_stat_{type}.txt"
    with open(path, "a") as file:
        with redirect_stdout(file):
            memory_usage = p.memory_percent(type)
            print(memory_usage)


def main():
    check_process_id()
    if not os.path.exists("stats"):
        os.makedirs("stats")
    p = psutil.Process(18044)  # process ID for mysqld
    while True:
        disk_usage_calculation(p, sleep_time=0.5)
        cpu_usage_calculation(p, interval=0.5)
        memory_usage_calculation(
            p, "rss", sleep_time=0.5
        )  # m “Mem Usage” column of taskmgr.exe in Windows; matches “top“‘s RES column in Unix.
        memory_usage_calculation(
            p, "vms", sleep_time=0.5
        )  # matches “Mem Usage” “VM Size” column of taskmgr.exe in Windows; matches “top“‘s VIRT column in Unix.


main()


# I found that you can only retrieve thread information from processes running as yourself.
