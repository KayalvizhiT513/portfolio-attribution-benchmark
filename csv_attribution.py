"""Streaming, keyed CSV attribution with hardware-detected threaded NumPy work.

Input files must be grouped by date; rows *within* each date may be in any order.
The wall-clock timer includes CSV reading, parsing, joining, dispatch, and compute.
"""
import argparse
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path
import numpy as np

def day_groups(path):
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["date", "security_id", "weight", "return"]:
            raise ValueError(f"{path}: expected date,security_id,weight,return")
        day, records, seen = None, {}, set()
        for row in reader:
            date, security = row["date"], row["security_id"]
            if not date or not security:
                raise ValueError(f"{path}: missing date/security ID")
            if day is not None and date != day:
                if date in seen:
                    raise ValueError(f"{path}: date {date} is not grouped")
                seen.add(day)
                yield day, records
                records = {}
            day = date
            if security in records:
                raise ValueError(f"{path}: duplicate {(date, security)}")
            records[security] = (float(row["weight"]), float(row["return"]))
        if day is not None:
            yield day, records

def effect(kind, wp, wb, rp, rb):
    # Each task owns its own independent array and returns one partial sum.
    if kind == "allocation":
        return kind, float(np.sum((wp - wb) * rb, dtype=np.float64))
    return kind, float(np.sum(wp * (rp - rb), dtype=np.float64))

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data/csv-15k")
    args = p.parse_args()
    start = time.perf_counter()
    folder = Path(args.data)
    cpu = os.process_cpu_count() if hasattr(os, "process_cpu_count") else os.cpu_count()
    workers = max(1, cpu or 1)
    pending = set()
    allocation = selection = 0.0
    days = securities_seen = 0
    # Keep the pipeline bounded so input parsing cannot run too far ahead.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        from itertools import zip_longest
        for p_group, b_group in zip_longest(day_groups(folder/"portfolio.csv"), day_groups(folder/"benchmark.csv")):
            if p_group is None or b_group is None:
                raise ValueError("portfolio and benchmark have different dates")
            pd, pf = p_group
            bd, bm = b_group
            if pd != bd:
                raise ValueError(f"date mismatch: portfolio={pd}, benchmark={bd}")
            if pf.keys() != bm.keys():
                raise ValueError(f"security mismatch on {pd}: missing in benchmark={len(pf.keys()-bm.keys())}, missing in portfolio={len(bm.keys()-pf.keys())}")
            keys = list(pf)
            wp = np.fromiter((pf[k][0] for k in keys), dtype=np.float64, count=len(keys))
            rp = np.fromiter((pf[k][1] for k in keys), dtype=np.float64, count=len(keys))
            wb = np.fromiter((bm[k][0] for k in keys), dtype=np.float64, count=len(keys))
            rb = np.fromiter((bm[k][1] for k in keys), dtype=np.float64, count=len(keys))
            pending.add(pool.submit(effect, "allocation", wp, wb, rp, rb))
            pending.add(pool.submit(effect, "selection", wp, wb, rp, rb))
            days += 1
            securities_seen += len(keys)
            if len(pending) >= workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for f in done:
                    kind, value = f.result()
                    if kind == "allocation":
                        allocation += value
                    else:
                        selection += value
        for f in pending:
            kind, value = f.result()
            if kind == "allocation":
                allocation += value
            else:
                selection += value
    elapsed = time.perf_counter() - start
    print(json.dumps({"language": "python-numpy", "mode": "csv-keyed-effect-tasks",
                      "workers": workers, "cpu_detected": cpu, "days": days,
                      "security_day_records": securities_seen,
                      "total_allocation": allocation, "total_selection": selection,
                      "elapsed_seconds": elapsed, "timing_scope": "CSV read + parse + keyed match + compute + dispatch"},
                     indent=2))
if __name__ == "__main__":
    main()
