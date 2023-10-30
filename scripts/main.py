import argparse
import math
from datetime import datetime

from scripts.dm_test import data_maintenance_test
from scripts.load_test import load_test
from scripts.power_test import power_test
from scripts.throughput_test import throughput_test


def main(args):
    uid = datetime.now().strftime("%m-%d_%H-%M-%S")
    T_load = load_test(args.sf, uid)
    T_power = power_test(args.sf, args.qdir, uid)
    T_tt1 = throughput_test(
        args.sf, f"{args.streams_dir}/{args.sf}/qmod", f"n={1}_" + uid
    )
    _, _, T_dm1 = data_maintenance_test(1, args.sf, uid)
    T_tt2 = throughput_test(
        args.sf, f"{args.streams_dir}/{args.sf}/qmod", f"n={2}_" + uid
    )
    _, _, T_dm2 = data_maintenance_test(2, args.sf, uid)

    SF = args.sf
    S_q = 4
    Q = S_q * 99
    T_pt = T_power * S_q
    T_tt = T_tt1 + T_tt2
    T_dm = T_dm1 + T_dm2
    T_ld = 0.01 * S_q * T_load
    QphDS_SF = math.floor((SF * Q) / ((T_pt * T_tt * T_dm * T_ld) / 3600) ** (1 / 4))
    results = f"""Scale Factor: {SF}
    Number of Queries: {Q}
    Load Time: {T_load} seconds
    Power Test Time: {T_power} seconds
    Throughput Test 1 Time: {T_tt1} seconds
    Throughput Test 2 Time: {T_tt2} seconds
    Data Maintenance Time: {T_dm} seconds
    TPC-DS query throughput (QphDS) is {QphDS_SF}"""
    print(results)
    with open(f"results/benchmark_{uid}.txt", "w") as f:
        f.write(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run TPC-DS tests")
    parser.add_argument(
        "--sf", type=int, default=1, help="An integer input for SF. Default is 1."
    )
    parser.add_argument(
        "--qdir",
        type=str,
        default="queries/1/qmod",
        help="Directory for queries to execute",
    )
    parser.add_argument(
        "--streams_dir",
        type=str,
        default="qstreams",
        help="Directory for query streams (throughput test)",
    )
    main(parser.parse_args())
