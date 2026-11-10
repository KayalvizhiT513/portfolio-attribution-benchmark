"""CPU-intensive multi-scenario compounded-return stress test, Numba version."""
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import numpy as np
from numba import njit

@njit(nogil=True)
def simulate(wp, wb, rp, rb, results, job, scenarios, steps):
    days, securities = wp.shape
    for d in range(days):
        for scenario in range(scenarios):
            shock = (scenario - (scenarios - 1) * 0.5) * 0.001 + job * 0.0001
            total = 0.0
            for i in range(securities):
                # Two compounded hypothetical paths. Results are risk-style
                # stress scores, NOT a production financial risk methodology.
                p = 1.0
                b = 1.0
                base_p = rp[d, i] + shock
                base_b = rb[d, i] + shock * 0.6
                for k in range(steps):
                    drift = (k % 5 - 2) * 0.00012
                    p *= 1.0 + base_p + drift
                    b *= 1.0 + base_b - drift
                total += wp[d, i] * (p - 1.0) - wb[d, i] * (b - 1.0)
            results[d, scenario] = total

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data/100k")
    p.add_argument("--jobs", type=int, default=10)
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--scenarios", type=int, default=8)
    p.add_argument("--steps", type=int, default=12)
    p.add_argument("--runs", type=int, default=3)
    args = p.parse_args()
    if min(args.jobs, args.concurrency, args.scenarios, args.steps, args.runs) < 1:
        p.error("all arguments must be positive")
    folder = Path(args.data)
    meta = json.loads((folder / "metadata.json").read_text())
    shape = (meta["days"], meta["securities"])
    arrays = tuple(np.memmap(folder / (k + ".bin"), dtype="<f8",
                             mode="r", shape=shape)
                   for k in ("wp", "wb", "rp", "rb"))
    concurrency = min(args.concurrency, args.jobs)
    outputs = [np.empty((meta["days"], args.scenarios), dtype=np.float64)
               for _ in range(concurrency)]
    simulate(*arrays, outputs[0], 0, args.scenarios, args.steps)

    def run_slot(slot):
        checksum = 0.0
        for job in range(slot, args.jobs, concurrency):
            simulate(*arrays, outputs[slot], job, args.scenarios, args.steps)
            checksum += float(outputs[slot].sum())
        return checksum

    durations = []
    checksum = None
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for _ in range(args.runs):
            start = time.perf_counter()
            values = list(pool.map(run_slot, range(concurrency)))
            elapsed = time.perf_counter() - start
            current = float(sum(values))
            if not np.isfinite(current):
                raise RuntimeError("nonfinite checksum")
            if checksum is not None and not np.isclose(current, checksum, rtol=1e-10, atol=1e-10):
                raise RuntimeError("inconsistent checksum")
            checksum = current
            durations.append(elapsed)
    median = float(np.median(durations))
    print(json.dumps({
        "language": "python-numba", "mode": "risk-scenarios",
        "jobs": args.jobs, "concurrency": concurrency,
        "days": meta["days"], "securities": meta["securities"],
        "scenarios": args.scenarios, "steps": args.steps,
        "path_updates": args.jobs * meta["days"] * meta["securities"] * args.scenarios * args.steps * 2,
        "runs_seconds": durations, "median_seconds": median,
        "jobs_per_second": args.jobs / median, "checksum": checksum,
        "timing_scope": "all stress computations; excludes input loading/JIT warmup/output allocation"
    }, indent=2))

if __name__ == "__main__":
    main()
