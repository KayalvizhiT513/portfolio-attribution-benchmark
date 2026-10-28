# Portfolio attribution benchmark: Python vs Go

Reproducible synthetic security-level attribution benchmark comparing NumPy, Numba (serial/parallel), and Go goroutines.

**For benchmarking only:** formulas are not confirmed as the team's production attribution methodology.

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
python benchmark.py --mode numpy --runs 5
python benchmark.py --mode numba --runs 5
python benchmark.py --mode numba-parallel --workers 4 --runs 5
go build -o benchmark-go .
./benchmark-go --workers 4 --runs 5
```

## Reproduce the reported benchmark runs

Run these commands from the repository root after installing dependencies and generating the default fixture. The same dataset is reused by all runs.

```bash
# Initial comparison (five measured runs each)
python benchmark.py --mode numpy
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

**Timing scope:** CPU calculation kernel only, excluding input generation and loading, JIT compilation warm-up, output writes, and validation. Both Python and Go preallocate and reuse output buffers outside the timed kernel. NumPy uses ufunc `out=` buffers to avoid large intermediate arrays. Runtime scheduling overheads still differ. Both include reconciliation checks. This project does not yet implement HTTP, storage adapters, or an event-triggered processing service.
