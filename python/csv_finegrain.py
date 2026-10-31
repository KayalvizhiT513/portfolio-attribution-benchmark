"""One independently schedulable task per security-day per effect.

CPU workers are chosen from detected CPU availability; bounded multiprocessing
queues prevent millions of pending futures. Intentionally a fine-grained
scheduling stress test, not an efficient batch/vectorized kernel.
"""
import argparse
import csv
import json
import multiprocessing as mp
import os
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

def run_worker(tasks, output):
    allocation = selection = 0.0
    while True:
        item = tasks.get()
        if item is None:
            break
        kind, wp, wb, rp, rb = item
        if kind == 0:
            allocation += (wp - wb) * rb
        else:
            selection += wp * (rp - rb)
    output.put((allocation, selection))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data/csv-15k")
    args = p.parse_args()
    started = time.perf_counter()
    root = Path(args.data)
    pf = load_csv(root/"portfolio.csv")
    bm = load_csv(root/"benchmark.csv")
    if pf.keys() != bm.keys():
        raise ValueError("portfolio and benchmark have different date/security keys")
    detected = os.process_cpu_count() if hasattr(os, "process_cpu_count") else os.cpu_count()
    worker_count = max(1, detected or 1)
    ctx = mp.get_context("spawn")
    tasks = ctx.Queue(maxsize=worker_count * 4)
    results = ctx.Queue()
    workers = [ctx.Process(target=run_worker, args=(tasks, results)) for _ in range(worker_count)]
    for proc in workers:
        proc.start()
    try:
        for key, (wp, rp) in pf.items():
            wb, rb = bm[key]
            tasks.put((0, wp, wb, rp, rb))
            tasks.put((1, wp, wb, rp, rb))
        for _ in workers:
            tasks.put(None)
        allocation = selection = 0.0
        for _ in workers:
            a, s = results.get()
            allocation += a
            selection += s
    finally:
        for proc in workers:
            proc.join()
        tasks.close()
        results.close()
    for proc in workers:
        if proc.exitcode != 0:
            raise RuntimeError(f"worker exited abnormally: {proc.exitcode}")
    print(json.dumps({
        "language": "python-stdlib", "mode": "csv-security-effect-tasks",
        "workers": worker_count, "cpu_detected": detected,
        "security_day_records": len(pf), "independent_effect_tasks": 2*len(pf),
        "total_allocation": allocation, "total_selection": selection,
        "elapsed_seconds": time.perf_counter()-started,
        "timing_scope": "CSV parse, key join, process startup, dispatch and compute"
    }, indent=2))

if __name__=="__main__":
    main()
