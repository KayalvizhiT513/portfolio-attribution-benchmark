# Portfolio attribution benchmark: Python vs Go

Reproducible synthetic security-level attribution benchmark comparing threaded NumPy, Numba (serial/parallel), and Go goroutines.

**For benchmarking only:** formulas are not confirmed as the team's production attribution methodology.

## Benchmark report

See [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) for the measured Python/Numba and Go results, multicore scaling, execution architecture, interpretation, and limitations.

## Input
15,000 securities in each of portfolio and benchmark, 252 days, and daily weights and returns (15,120,000 float64 values). Daily weights in each universe sum to 1. Benchmark security returns are uniformly distributed between -3% and +3%; portfolio returns are perturbed by up to 5 basis points and clipped to the same range. Seed 42. Python generates binary files which both languages read.

## Synthetic two-effect formulas
- Allocation: `(wp - wb) * rb`
- Selection: `wp * (rp - rb)`
- Sum reconciles to `wp * rp - wb * rb` for each security and day.

## Commands
```bash
python -m pip install -r requirements.txt
python generate.py --securities 15000 --days 252 --seed 42
python benchmark.py --mode numpy-threaded --workers 4 --runs 5
python benchmark.py --mode numba --runs 5
python benchmark.py --mode numba-parallel --workers 4 --runs 5
go build -o benchmark-go .
./benchmark-go --workers 4 --runs 5
```

## Larger stress test

The original 15,000-security fixture is a lightweight baseline. Increase the **security count** rather than trading days to keep the intended one-year horizon. Each security is present in both portfolio and benchmark.

| Securities per universe | Input values | Four input arrays | Two output arrays | Total array footprint |
|---:|---:|---:|---:|---:|
| 15,000 | 15.12 million | 121 MB | 60 MB | 181 MB |
| 100,000 | 100.8 million | 806 MB | 403 MB | 1.21 GB |
| 250,000 | 252 million | 2.02 GB | 1.01 GB | 3.02 GB |
| 500,000 | 504 million | 4.03 GB | 2.02 GB | 6.05 GB |
| 1,000,000 | 1.008 billion | 8.06 GB | 4.03 GB | 12.10 GB |

Sizes are decimal GB; actual peak RAM, file-system cache, temporary arrays and application overhead increase resource demands. Go eagerly loads inputs; Python memory-maps them. The generator writes day by day to limit peak generation memory.

**Recommended first stress test: 100,000 securities per universe, then 250,000** if memory and disk are sufficient. Keep a separate directory for each test:

```bash
python generate.py --securities 100000 --days 252 --seed 42 --output data/100k
python benchmark.py --data data/100k --mode numpy-threaded --workers 4 --runs 20
python benchmark.py --data data/100k --mode numba-parallel --workers 4 --runs 20
./benchmark-go --data data/100k --workers 4 --runs 20

# Repeat across 1, 2, 4, 8 workers for each parallel runtime.
# If this fits comfortably, replace 100000 with 250000 and output data/250k.
```

The new generator uses a **day-by-day random-number sequence**, so its seed-42 fixture is not byte-identical to the original generator's historical dataset. Re-run **all** implementations with the newly generated files; don't compare new timings to old fixtures as if the inputs were unchanged.

At these larger sizes, pay attention to **peak memory, disk space, warm/cold page cache and sustained bandwidth**. A bigger dataset stresses memory and I/O; it does not make the two financial formulas computationally more complex.

## Independent-effect scheduling experiment

The existing **fused** calculation remains the performance baseline. A second mode tests whether scheduling allocation and selection as **independent tasks** on each trading day improves speed. These effects have no dependency on each other, but splitting them may increase reads and scheduling overhead. There is no guarantee it will improve performance.

- **Numba `--mode numba-steps`**: `prange(days * 2)` schedules 504 independent (day, effect) tasks; allocation and selection write disjoint arrays.
- **Go `--mode steps`**: 504 day/effect tasks in the goroutine worker channel. The original default `--mode fused` runs 252 day tasks.
- Both preserve preallocated outputs, share read-only input buffers in one process, and run the same mathematical formulas. Their per-day reconciliation check remains active.
- `--workers` sets the **total** worker budget, not separate workers per formula. Thus this tests **task-level inter-step parallelism** without doubling CPU resources.

Run the paired benchmark against the same existing fixture (do not regenerate it):

```bash
git pull origin main
go build -o benchmark-go .

# 100k dataset, 4 workers, 20 runs
python benchmark.py --data data/100k --mode numba-parallel --workers 4 --runs 20
python benchmark.py --data data/100k --mode numba-steps --workers 4 --runs 20
./benchmark-go --data data/100k --mode fused --workers 4 --runs 20
./benchmark-go --data data/100k --mode steps --workers 4 --runs 20
```

Repeat with 1, 2 and 8 workers to test whether effects-as-tasks helps at different concurrency levels. Timings are kernel-only, just like the earlier report. The [benchmark report](BENCHMARK_REPORT.md) contains **fused-mode measured results only**; no results for the new steps modes have yet been recorded.

## Go static-range optimization experiment

The original Go `--mode fused` sends one day at a time through a job channel (252 tasks), and `--mode steps` sends separate allocation/selection tasks (504 tasks). A new **`--mode static`** gives each worker one contiguous range of days, removes the job channel, and computes both effects together in one loop. This reduces scheduling/coordination work and may improve locality; whether it improves performance on the Mac must be measured.

```bash
git pull origin main
go build -o benchmark-go .
./benchmark-go --data data/100k --mode fused --workers 4 --runs 20
./benchmark-go --data data/100k --mode static --workers 4 --runs 20
./benchmark-go --data data/100k --mode steps --workers 4 --runs 20
python benchmark.py --data data/100k --mode numba-parallel --workers 4 --runs 20
python benchmark.py --data data/100k --mode numba-steps --workers 4 --runs 20
```

Repeat at 1, 2, 4 and 8 workers, ideally alternating the order of modes across trials. The three Go modes all preserve the same formulas, output shapes, shared input arrays, and reconciliation checks. The new mode is not yet measured; it is not evidence that Go is faster. This remains a kernel-only comparison.

## Concurrent heavy-job stress test (Numba versus Go)

The original benchmarks measure **one attribution job** with parallel processing of days. This additional experiment measures **10 independent attribution jobs**, allowing up to 4 jobs to compute simultaneously (4 total compute workers, each job using one native compute thread). The same 100k-security × 252-day dataset is reused as **shared read-only input**. Each active job gets separate, preallocated output arrays. This compares concurrent-job throughput; it does **not** model 10 unique user portfolios or independent data-loading overheads.

Run from the repository root after `git pull origin main`:

```bash
go build -o benchmark-multijob ./cmd/multijob
python multi_job.py --data data/100k --jobs 10 --concurrency 4 --runs 5
./benchmark-multijob --data data/100k --jobs 10 --concurrency 4 --runs 5
```

To stress more jobs without increasing simultaneous output-memory allocations, try `--jobs 20` and `--jobs 50`, keeping `--concurrency 4`. Compare **median wall-clock duration for all jobs** and **completed jobs/second**, not the single-job kernel times. The tests exclude input reading, Numba compilation, and output allocation, but include job scheduling and completion. Jobs share read-only inputs and recycle their assigned output buffers; this measures repeated independent jobs against the *same fixture*, not ten distinct gigabyte-scale datasets.

**Memory warning:** Each 100k job's output pair is ~403 MB. Four simultaneously active jobs need about **1.61 GB of output arrays**, in addition to ~806 MB of shared inputs and runtime/OS overhead. Go holds all four input arrays in memory; Python memory-maps them, but still depends on system cache and available memory. Start at `--concurrency 2` if the machine is memory-constrained. Increasing `--jobs` alone does not increase output buffer count.

Results for this concurrency experiment are **not measured yet**.

## Historical results

[BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) documents the initial run, when the NumPy baseline was non-threaded and the earlier generator was in use. Those measured results remain historically valid, but the new threaded NumPy mode needs a fresh comparison. Do not present the historical NumPy timing as the performance of threaded NumPy.

## Reproduce the reported benchmark runs

Run these commands from the repository root after installing dependencies and generating the default fixture. The same dataset is reused by all runs.

```bash
# Updated comparison (five measured runs each; historical report used unthreaded NumPy)
python benchmark.py --mode numpy-threaded --workers 4
python benchmark.py --mode numba
python benchmark.py --mode numba-parallel --workers 4
go build -o benchmark-go .
./benchmark-go --workers 4

# Multicore scaling comparison (20 measured runs per configuration)
python benchmark.py --mode numba-parallel --workers 1 --runs 20
python benchmark.py --mode numba-parallel --workers 2 --runs 20
python benchmark.py --mode numba-parallel --workers 4 --runs 20
python benchmark.py --mode numba-parallel --workers 8 --runs 20

./benchmark-go --workers 1 --runs 20
./benchmark-go --workers 2 --runs 20
./benchmark-go --workers 4 --runs 20
./benchmark-go --workers 8 --runs 20
```

The measured outputs report median kernel runtime, individual runtimes, attribution totals, and maximum reconciliation error. Worker counts should not exceed practical CPU availability; results can vary with machine load and cache state.

Use a smaller dataset first (`--securities 100 --days 5`). Benchmark 1, 2, 4, and 8 workers as supported by the machine.

**Timing scope:** CPU calculation kernel only, excluding input generation and loading, JIT compilation warm-up, output writes, and validation. Both Python and Go preallocate and reuse output buffers outside the timed kernel. Threaded NumPy uses a persistent thread pool and ufunc `out=` buffers to avoid large intermediate arrays. Runtime scheduling overheads still differ. Both include reconciliation checks. This project does not yet implement HTTP, storage adapters, or an event-triggered processing service.
