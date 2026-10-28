"""Security-level synthetic attribution: NumPy vs Numba serial/parallel."""
import argparse
import json
import time
from pathlib import Path
import numpy as np

def numpy_kernel(wp, wb, rp, rb):
    return (wp - wb) * rb, wp * (rp - rb)

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
    p.add_argument("--mode", choices=("numpy", "numba", "numba-parallel"), default="numba-parallel")
    args = p.parse_args()
    if args.workers < 1 or args.runs < 1:
        p.error("workers and runs must be positive")
    arrays, meta = load(args.data)
    wp, wb, rp, rb = arrays
    if args.mode == "numpy":
        kernel = numpy_kernel
    else:
        from numba import njit, prange, set_num_threads
        if args.mode == "numba":
            @njit
            def kernel(wp, wb, rp, rb):
                a, s = np.empty_like(wp), np.empty_like(wp)
                for d in range(wp.shape[0]):
                    for i in range(wp.shape[1]):
                        a[d, i] = (wp[d, i] - wb[d, i]) * rb[d, i]
                        s[d, i] = wp[d, i] * (rp[d, i] - rb[d, i])
                return a, s
        else:
            set_num_threads(args.workers)
            @njit(parallel=True)
            def kernel(wp, wb, rp, rb):
                a, s = np.empty_like(wp), np.empty_like(wp)
                for d in prange(wp.shape[0]):
                    for i in range(wp.shape[1]):
                        a[d, i] = (wp[d, i] - wb[d, i]) * rb[d, i]
                        s[d, i] = wp[d, i] * (rp[d, i] - rb[d, i])
                return a, s
    kernel(*arrays)  # Compile/warm up outside timed region.
    times = []
    for _ in range(args.runs):
        start = time.perf_counter()
        a, s = kernel(*arrays)
        times.append(time.perf_counter() - start)
    error = float(np.max(np.abs((a + s).sum(axis=1) -
                                (wp * rp - wb * rb).sum(axis=1))))
    if error > 1e-10:
        raise RuntimeError(f"attribution reconciliation failed: {error}")
    print(json.dumps({
        "language": "python", "mode": args.mode,
        "workers": args.workers if args.mode == "numba-parallel" else 1,
        "days": meta["days"], "securities": meta["securities"],
        "runs_seconds": times, "median_seconds": float(np.median(times)),
        "max_reconciliation_error": error,
        "total_allocation": float(a.sum()), "total_selection": float(s.sum())
    }, indent=2))

if __name__ == "__main__":
    main()
