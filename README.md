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

## Heavy multi-scenario computation (separate from attribution)

The earlier concurrent-jobs test repeats the existing simple attribution formulas. For a genuinely heavier CPU workload, `risk_stress.py` and `cmd/riskstress/main.go` implement matching **hypothetical compounded-return stress simulations**:

- Every job processes the 252 days and 100k securities, across **8 shock scenarios**.
- For every security, scenario and day, calculate **12 successive compounded updates** for the portfolio and benchmark paths, then weight and aggregate their results.
- Jobs use slightly different deterministic shocks so they do not simply produce identical outputs.
- **10 jobs × 252 days × 100k securities × 8 scenarios × 12 steps × 2 paths = 4.8384 billion path updates.**
- Four jobs can run at a time, using at most four total CPU computation workers. Both implementations share the same read-only numerical inputs and allocate one small result array per active worker. Input read, Numba compilation and output allocation are excluded from the timer.
- This is a **synthetic CPU stress test**, not a validated portfolio-risk model. It does not measure file I/O or backend API latency.

Start with a smaller sanity run (these will take longer than earlier millisecond benchmarks):

```bash
git pull origin main
go build -o benchmark-riskstress ./cmd/riskstress
python risk_stress.py --data data/100k --jobs 2 --concurrency 2 --scenarios 2 --steps 4 --runs 1
./benchmark-riskstress --data data/100k --jobs 2 --concurrency 2 --scenarios 2 --steps 4 --runs 1
```

Then run the full workload, **one process at a time**:

```bash
python risk_stress.py --data data/100k --jobs 10 --concurrency 4 --scenarios 8 --steps 12 --runs 3
./benchmark-riskstress --data data/100k --jobs 10 --concurrency 4 --scenarios 8 --steps 12 --runs 3
```

Compare median time and completed jobs per second, and check that the aggregate checksums agree within ordinary floating-point tolerances. The simulation updates are intentionally serial within each scenario and independent between jobs, making it a different CPU-load profile from the earlier simple attribution formulas. These new variants have **not yet been run or validated on the target Mac**.

## Dependent four-stage computation benchmark (new; results pending)

This addresses the **multi-step** requirement, unlike the earlier repeated compounding test. Each of ten concurrent jobs runs four **different, dependent** calculations in order:

1. **Rolling risk-adjusted relative return:** For every security and day, calculate mean and volatility of the portfolio-minus-benchmark return over the preceding 20 trading days.
2. **Weighted signal calculation:** Consume stage 1 values, apply a job-specific threshold and multiply by portfolio-minus-benchmark weights.
3. **Daily portfolio aggregation:** Reduce stage 2 security-level signals to one portfolio value per day.
4. **Sequential portfolio path/drawdown:** Compound daily aggregate values and compute accumulated drawdown; each day depends on the preceding day.

Four active jobs at a time; each job runs its four stages sequentially. Different jobs can run concurrently. All jobs reuse the **same read-only dataset**, with a different threshold per job, and keep independent scratch arrays. The complete workload measures all four compute stages but excludes loading, Numba compilation, and output allocation. This is a **synthetic numerical pipeline**, not validated financial advice or production methodology.

```bash
git pull origin main
go build -o benchmark-multistage ./cmd/multistage
python multistage.py --data data/100k --jobs 2 --concurrency 2 --window 20 --runs 1
./benchmark-multistage --data data/100k --jobs 2 --concurrency 2 --window 20 --runs 1
```

If the small test completes and checksums agree, run the full test:

```bash
python multistage.py --data data/100k --jobs 10 --concurrency 4 --window 20 --runs 3
./benchmark-multistage --data data/100k --jobs 10 --concurrency 4 --window 20 --runs 3
```

Four active jobs each allocate a 201.6 MB scratch array (about 806 MB total); Go also loads the shared 806 MB inputs into RAM. Peak process memory and page cache need monitoring, and high worker counts may cause swapping. **No performance result is yet claimed for this new benchmark.**

## Shuffled two-file CSV attribution benchmark (hardware-selected concurrency)

This is a **new end-to-end CSV experiment**, separate from the older binary-input kernel tests. Generate **portfolio.csv** and **benchmark.csv**, each with `date,security_id,weight,return` columns. Rows for securities are independently shuffled **within each date**; days remain grouped and in the same order in both files to support streaming. Joining is by **(date, security_id)**, never row position.

The Python implementation uses standard NumPy with a thread pool; it **does not require Numba**. Go uses its standard CSV parser and goroutines. Both determine worker concurrency from available CPU resources instead of accepting a fixed worker count. The two independent per-day tasks are allocation `(wp-wb)*rb` and selection `wp*(rp-rb)`. Each task can be scheduled in any order, with only its input records as prerequisites. Both implementations reject mismatched dates, duplicate security keys and missing matches, rather than silently imputing zeros. The CSV reader, parse, keyed match, task dispatch, computation and collection **all fall inside the measured wall time**, unlike the earlier kernel-only benchmark.

```bash
git pull origin main
python generate_csv.py --securities 15000 --days 252 --output data/csv-15k
go build -o benchmark-csv ./cmd/csvattribution
python csv_attribution.py --data data/csv-15k
./benchmark-csv --data data/csv-15k
```

The outputs include detected workers, processed security-day records, elapsed time, total allocation and selection. Compare the effect totals with floating-point tolerance, not exact string equality (Go map iteration changes summation order).

**Design boundaries:** The streaming reader requires the dates to appear in identical contiguous groups in both files; only *security row ordering within a date* can differ. The dataset has matching securities in both files; unmatched holdings or benchmark names are rejected, not treated as zero. One pair of effect tasks per trading day prevents millions of tiny queued tasks, so **security-level work inside each effect is still processed as a batch**. Worker selection is CPU-based, not a measured memory-bandwidth optimum. For an entirely order-independent cross-day ingestion pipeline or dynamic memory-aware scheduling, more work is required. **No measured CSV timings have been recorded.**

## Fully independent security-day effect tasks (new experiment)

The earlier `csv_attribution.py` and `cmd/csvattribution` dispatched **one allocation task and one selection task per day**. That grouped all securities together. The newer `csv_finegrain.py` and `cmd/csvfinegrain` dispatch **separate allocation and selection tasks for every (date, security_id)** after joining the two shuffled CSV inputs on those keys. There is no ordering constraint across days, securities or effects. CPU worker availability is auto-detected; neither runner requires a manually chosen worker count. Python uses `multiprocessing` (not GIL-bound Python threads) and Go uses goroutines. Both report total tasks, worker count and elapsed time. The Python queue is bounded to avoid accumulating millions of futures. This benchmark **deliberately tests the scheduling cost of the smallest independently runnable arithmetic units**; it is likely substantially slower than batching because each individual effect only performs a few operations.

```bash
git pull origin main
python generate_csv.py --securities 100 --days 5 --output data/csv-small
go build -o benchmark-csv-finegrain ./cmd/csvfinegrain
python csv_finegrain.py --data data/csv-small
./benchmark-csv-finegrain --data data/csv-small
```

The implementation loads the CSVs into keyed maps first. At 100k securities × 252 days, each file contains 25.2 million rows and the maps can exhaust laptop RAM; **do not run that scale before a streaming/partitioned join is built**. It is also intentionally not an optimized performance comparison: millions of tiny messages cost time. All results remain unmeasured pending local execution. Dates and securities may occur in arbitrary row orders in the two input files; the join is keyed, not positional. Both sides must contain matching keys, and duplicate keys are errors.

## Brinson–Fachler three-effect CSV experiment (new; unmeasured)

This is a separate, corrected **segment-level** benchmark, leaving historical synthetic two-effect tests intact. Both `portfolio.csv` and `benchmark.csv` now contain `date,security_id,segment_id,weight,return`. Within each trading date, rows are independently shuffled. The implementations match on date/security ID, confirm that segment IDs agree, aggregate weighted returns to the segment level, then compute:

- Allocation: `(wp_segment - wb_segment) * (Rb_segment - Rb_total)`
- Selection: `wb_segment * (Rp_segment - Rb_segment)`
- Interaction: `(wp_segment - wb_segment) * (Rp_segment - Rb_segment)`

The sum of all segment effects must reconcile to the difference between total portfolio and benchmark returns on each day. Workers are selected automatically from detected CPU availability. **Selection and interaction have no mathematical dependency on whole-benchmark return**; allocation does. **Current implementation limitation:** both implementations finish each day's matching and segment aggregation, including calculation of whole-benchmark return, *before* submitting any effect. Thus the mathematical dependency is correctly modeled, but the scheduler does **not yet realize the earliest-start advantage of selection**. Effects are individually scheduled per **segment-day**, not per security-day, because these are segment-level attribution formulas. Truly early selection/interaction dispatch requires incremental segment completion notifications while benchmark totals are still being reduced.

```bash
git pull origin main
python generate_brinson_csv.py --securities 15000 --days 252 --segments 20 --output data/brinson-15k
go build -o benchmark-brinson ./cmd/brinsoncsv
python brinson_csv.py --data data/brinson-15k
./benchmark-brinson --data data/brinson-15k
```

These runs include CSV parse, security key matching, segment aggregation, effect scheduling and reconciliation in the elapsed time; they are **not** comparable directly with historical kernel-only numbers. The files can be large: start at this size, not 100k. Results are pending local validation.

## Brinson CSV output files and inclusive I/O timing

Both Brinson–Fachler benchmark runners now write **one CSV row per (date, segment_id)** with columns `date,segment_id,allocation,selection,interaction,total`. This includes all three effects and their sum; individual security rows are not produced because Brinson attribution is calculated at segment level. Rows are sorted by segment identifier within each day for easier comparison.

The measured `elapsed_seconds` now includes both input CSV reading and writing the result CSV, plus flushing and `fsync` of the output before stopping the timer. Output directory creation and process/thread setup are likewise inside the timer. File-system caching can still influence timings; `fsync` does not guarantee identical physical-storage behavior across OS and hardware. Dataset generation is excluded.

```bash
git pull origin main
go build -o benchmark-brinson ./cmd/brinsoncsv
python brinson_csv.py --data data/brinson-15k --output results/brinson-python.csv
./benchmark-brinson --data data/brinson-15k --output results/brinson-go.csv
```

Compare CSV output by matching `(date,segment_id)` and applying floating-point tolerance to numerical columns. Historical Python 8.604s and Go 1.986s results were collected **before** CSV output writing was added, so they cannot be used as updated end-to-end timings. The new results remain unmeasured.

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

### Separate CSV I/O durations in JSON

Both Brinson runners now include an `io_timing` object with `input_read_parse_seconds` and `output_write_sync_seconds`, alongside unchanged `elapsed_seconds`. Input read/parse time measures the wall time spent advancing the CSV readers and decoding rows (not pure disk access). Output time includes row formatting and CSV writing plus flush and `fsync`. File opens, matching, aggregation, validation, task execution and runtime overhead remain counted in total elapsed time but are not attributed to either I/O subtotal. Thus the two subtotals do **not** necessarily add to total time. Timings should be obtained by rerunning both executables; the earlier results did not emit this breakdown.

### Actual CPU utilization (not configured worker count)

The Brinson runners now emit `cpu_usage.process_cpu_seconds` and `cpu_usage.average_effective_cores` using process user+system CPU time divided by wall-clock elapsed time. For example, a value around 1.0 means approximately one logical CPU's worth of processing **on average** for the complete run. This is more meaningful than the configured `workers` count, but does **not** establish how many cores were running at the same instant or which physical cores ran the workload. Precise peak concurrency requires OS sampling/profiling (e.g. macOS Instruments). Python's GIL-bound threaded effect code may not achieve multicore CPU compute despite detecting multiple workers. Go CPU accounting here uses the platform's `syscall.Getrusage` interface; test on macOS before treating the build as validated.


### Streaming, dependency-aware Brinson schedule (latest)

Python and Go now keep a **portfolio security map for one day** and consume the corresponding **benchmark CSV one row at a time**. The portfolio records provide the expected number of securities in each segment. Each matching benchmark row updates that segment's benchmark weight and return. As soon as all securities in a segment have matched, **selection and interaction are submitted immediately** to the worker pool. These two effects can execute while later benchmark rows are being parsed. Only after all benchmark securities for the day have been matched and the overall benchmark return is final are the **allocation** tasks submitted. Daily reconciliation and CSV writing still form a day boundary.

The new `pipeline.segments_released_before_full_benchmark` reports the count of segment-days whose selection/interaction tasks were dispatched **before the last benchmark record of the day was processed**. This counts dispatch readiness, not proven simultaneous CPU execution. This approach presumes **date-grouped CSV inputs** and complete per-date membership across both files; security rows within a date can be shuffled. It also has a limitation: portfolio input parsing occurs before benchmark streaming each day, and per-segment completion is constrained by when the last member's benchmark row arrives.

Rebuild and rerun both implementations to measure the new architecture:
```bash
git pull origin main
go build -o benchmark-brinson ./cmd/brinsoncsv
python brinson_csv.py --data data/brinson-15k --output results/brinson-python.csv
./benchmark-brinson --data data/brinson-15k --output results/brinson-go.csv
```

**Validation status:** This code has been committed but has not yet been executed on the user's Mac; compare both output CSVs and reported totals against earlier measured runs. Historical timings belong to the previous day-barrier architecture.
