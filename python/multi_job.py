"""Concurrent independent attribution jobs with one shared read-only input fixture.

Benchmark scope: time to complete N CPU-heavy jobs, with at most C jobs active.
Input loading, JIT compilation and output allocation occur before timing.
"""
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from numba import njit


@njit(nogil=True)
def compute(wp, wb, rp, rb, allocation, selection):
    for d in range(wp.shape[0]):
        for i in range(wp.shape[1]):
            allocation[d, i] = (wp[d, i] - wb[d, i]) * rb[d, i]
            selection[d, i] = wp[d, i] * (rp[d, i] - rb[d, i])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data/100k")
    p.add_argument("--jobs", type=int, default=10)
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--runs", type=int, default=5)
    args = p.parse_args()
    if min(args.jobs, args.concurrency, args.runs) < 1:
        p.error("jobs, concurrency and runs must be positive")
    concurrency = min(args.jobs, args.concurrency)
    folder = Path(args.data)
    meta = json.loads((folder / "metadata.json").read_text())
    shape = (meta["days"], meta["securities"])
    arrays = tuple(np.memmap(folder / (key + ".bin"), dtype="<f8",
                             mode="r", shape=shape)
                   for key in ("wp", "wb", "rp", "rb"))
    # Allocate at most concurrency output pairs rather than jobs output pairs.
    slots = [(np.empty(shape, dtype=np.float64),
              np.empty(shape, dtype=np.float64))
             for _ in range(concurrency)]
    compute(*arrays, *slots[0])  # compile and pre-warm before measuring
    expected_a = float(slots[0][0].sum())
    expected_s = float(slots[0][1].sum())

    def worker(slot, count):
        a, s = slots[slot]
        for _ in range(count):
            compute(*arrays, a, s)
        return float(a.sum()), float(s.sum())

    # Deterministic number of jobs per long-lived worker; no queue duplication.
    counts = [args.jobs // concurrency + (i < args.jobs % concurrency)
              for i in range(concurrency)]
    measurements = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for _ in range(args.runs):
            start = time.perf_counter()
            futures = [pool.submit(worker, idx, counts[idx])
                       for idx in range(concurrency)]
            results = [future.result() for future in futures]
            elapsed = time.perf_counter() - start
            for aa, ss in results:
                if not (np.isclose(aa, expected_a, rtol=1e-9, atol=1e-11)
                        and np.isclose(ss, expected_s, rtol=1e-9, atol=1e-11)):
                    raise RuntimeError("concurrent job results disagree")
            measurements.append(elapsed)
    median = float(np.median(measurements))
    print(json.dumps({
        "language": "python-numba",
        "mode": "concurrent-jobs",
        "jobs": args.jobs, "concurrency": concurrency,
        "days": meta["days"], "securities": meta["securities"],
        "runs_seconds": measurements, "median_seconds": median,
        "throughput_jobs_per_second": args.jobs / median,
        "output_buffers": concurrency,
        "input_sharing": "read-only shared memmap",
        "timing_scope": "all jobs plus dispatch and completion, no input loading/JIT/output allocation"
    }, indent=2))


if __name__ == "__main__":
    main()
