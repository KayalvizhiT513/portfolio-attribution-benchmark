"""Security-level synthetic attribution: NumPy vs Numba serial/parallel."""
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import numpy as np

def numpy_chunk(wp, wb, rp, rb, a, s, start, stop):
    # Each thread owns disjoint, contiguous day slices; NumPy ufuncs release the GIL.
    sl = slice(start, stop)
    np.subtract(wp[sl], wb[sl], out=a[sl])
    np.multiply(a[sl], rb[sl], out=a[sl])
    np.subtract(rp[sl], rb[sl], out=s[sl])
    np.multiply(s[sl], wp[sl], out=s[sl])


def load(folder):
    folder = Path(folder)
    meta = json.loads((folder / "metadata.json").read_text())
    shape = meta["days"], meta["securities"]
    arrays = [np.memmap(folder / f"{k}.bin", dtype="<f8", mode="r", shape=shape)
              for k in ("wp", "wb", "rp", "rb")]
    return arrays, meta

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--mode", choices=("numpy-threaded", "numba", "numba-parallel", "numba-steps"), default="numba-parallel")
    args = p.parse_args()
    if args.workers < 1 or args.runs < 1:
        p.error("workers and runs must be positive")
    arrays, meta = load(args.data)
    wp, wb, rp, rb = arrays
    a, s = np.empty(wp.shape, dtype=np.float64), np.empty(wp.shape, dtype=np.float64)
    executor = None
    if args.mode == "numpy-threaded":
        executor = ThreadPoolExecutor(max_workers=args.workers)
        days = wp.shape[0]
        chunks = [(i * days // args.workers, (i + 1) * days // args.workers)
                  for i in range(args.workers)]
        chunks = [(start, stop) for start, stop in chunks if start < stop]

        def kernel(wp, wb, rp, rb, a, s):
            futures = [executor.submit(numpy_chunk, wp, wb, rp, rb, a, s, start, stop)
                       for start, stop in chunks]
            for future in futures:
                future.result()
    else:
        from numba import njit, prange, set_num_threads
        if args.mode == "numba":
            @njit
            def kernel(wp, wb, rp, rb, a, s):
                for d in range(wp.shape[0]):
                    for i in range(wp.shape[1]):
                        a[d, i] = (wp[d, i] - wb[d, i]) * rb[d, i]
                        s[d, i] = wp[d, i] * (rp[d, i] - rb[d, i])
        elif args.mode == "numba-parallel":
            set_num_threads(args.workers)
            @njit(parallel=True)
            def kernel(wp, wb, rp, rb, a, s):
                for d in prange(wp.shape[0]):
                    for i in range(wp.shape[1]):
                        a[d, i] = (wp[d, i] - wb[d, i]) * rb[d, i]
                        s[d, i] = wp[d, i] * (rp[d, i] - rb[d, i])
        else:
            # Independent allocation and selection tasks for each day.
            # Task scheduling includes both effects; neither waits for the other.
            set_num_threads(args.workers)
            @njit(parallel=True)
            def kernel(wp, wb, rp, rb, a, s):
                days, securities = wp.shape
                for task in prange(days * 2):
                    d = task // 2
                    if task % 2 == 0:
                        for i in range(securities):
                            a[d, i] = (wp[d, i] - wb[d, i]) * rb[d, i]
                    else:
                        for i in range(securities):
                            s[d, i] = wp[d, i] * (rp[d, i] - rb[d, i])
    kernel(*arrays, a, s)  # Compile/warm up outside timed region.
    times = []
    for _ in range(args.runs):
        start = time.perf_counter()
        kernel(*arrays, a, s)
        times.append(time.perf_counter() - start)
    if executor is not None:
        executor.shutdown()
    error = float(np.max(np.abs((a + s).sum(axis=1) -
                                (wp * rp - wb * rb).sum(axis=1))))
    if error > 1e-10:
        raise RuntimeError(f"attribution reconciliation failed: {error}")
    print(json.dumps({
        "language": "python", "mode": args.mode,
        "workers": args.workers if args.mode in ("numba-parallel", "numba-steps", "numpy-threaded") else 1,
        "days": meta["days"], "securities": meta["securities"],
        "runs_seconds": times, "median_seconds": float(np.median(times)),
        "max_reconciliation_error": error,
        "total_allocation": float(a.sum()), "total_selection": float(s.sum())
    }, indent=2))

if __name__ == "__main__":
    main()
