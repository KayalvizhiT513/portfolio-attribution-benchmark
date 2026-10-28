"""Fine-grained, security-day effect tasks from independently shuffled CSVs.

Every allocation and selection effect for every (date, security_id) is a separately
schedulable task. Uses hardware-selected workers and a bounded task queue.
"""
import argparse
import csv
import json
import os
import queue
import threading
import time
from pathlib import Path

def load_csv(path):
    records = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != ["date", "security_id", "weight", "return"]:
            raise ValueError(f"{path}: expected date,security_id,weight,return")
        for row in reader:
            key = (row["date"], row["security_id"])
            if not all(key):
                raise ValueError(f"{path}: empty key")
            if key in records:
                raise ValueError(f"{path}: duplicate key {key}")
            records[key] = (float(row["weight"]), float(row["return"]))
    return records

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/csv-15k")
    args = parser.parse_args()
    begin = time.perf_counter()
    folder = Path(args.data)
    # CPython threads are not CPU parallel for this scalar Python work because
    # of the GIL. ProcessPoolExecutor instead provides independent CPU processes.
    from concurrent.futures import ProcessPoolExecutor
    portfolio = load_csv(folder / "portfolio.csv")
    benchmark = load_csv(folder / "benchmark.csv")
    if portfolio.keys() != benchmark.keys():
        raise ValueError("portfolio and benchmark differ in date/security keys")
    workers = max(1, (os.process_cpu_count() if hasattr(os, "process_cpu_count") else os.cpu_count()) or 1)

    # Individual effect calculations are independent. Submit in bounded groups to
    # avoid materializing tens of millions of Future objects.
    def task_iter():
        for key, (wp, rp) in portfolio.items():
            wb, rb = benchmark[key]
            yield (0, wp, wb, rp, rb)
            yield (1, wp, wb, rp, rb)

    # Executor.map batches the transport of independent scalar tasks, but each
    # task remains logically separate; worker processes evaluate effects in any order.
    # A single CSV join is the only prerequisite for each effect.
    allocation = selection = 0.0
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for kind, value in pool.map(compute_task, task_iter(), chunksize=1024):
            if kind == 0:
                allocation += value
            else:
                selection += value
    elapsed = time.perf_counter() - begin
    print(json.dumps({
        "language": "python-stdlib", "mode": "csv-security-effect-tasks",
        "workers": workers, "security_day_records": len(portfolio),
        "independent_effect_tasks": len(portfolio) * 2,
        "total_allocation": allocation, "total_selection": selection,
        "elapsed_seconds": elapsed,
        "timing_scope": "CSV parsing, keyed join, task dispatch and independent effects"
    }, indent=2))

def compute_task(t):
    kind, wp, wb, rp, rb = t
    if kind == 0:
        return 0, (wp - wb) * rb
    return 1, wp * (rp - rb)

if __name__ == "__main__":
    main()
