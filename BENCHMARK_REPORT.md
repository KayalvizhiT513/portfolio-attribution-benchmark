# Portfolio Attribution Compute Benchmark
### Python / NumPy / Numba vs Go / Goroutines

**Experiment:** security-level, daily synthetic allocation and selection effects  
**Run date:** 28 October 2026 (benchmarks executed locally on the user's MacBook Pro)  
**Status:** measured kernel results; architectural hypotheses are not yet profiled  
**Repository:** [portfolio-attribution-benchmark](../README.md)

## Executive summary

**Numba with 8 threads had the lowest observed median kernel runtime: 0.955 ms**, versus **Go with 8 workers at 1.131 ms** (about **15.6% less elapsed time**, or 1.18x as fast). The best single-worker Numba configuration in the scaling experiment took 1.784 ms versus 4.714 ms for Go.

Both implementations gain from multicore execution, but **Go displays larger relative speedups from its slower single-worker baseline**. These measurements **do not establish the best language for the full service**: only two lightweight arithmetic formulas are timed, no file-transfer/storage work is included, and CPU model, system load, memory use, and physical memory traffic were not measured.

**Decision for the current kernel:** retain Python + Numba as the leading candidate; treat Go as a competitive alternative. Benchmark representative multi-stage domain calculations and end-to-end I/O before making a system-wide language decision.

## 1. Dataset and measurement contract

| Dimension | Value |
|---|---:|
| Portfolio securities | 15,000 |
| Benchmark securities | 15,000 |
| Trading days | 252 |
| Security-day observations across both universes | 7,560,000 |
| Numerical inputs: weights and returns | 15,120,000 float64 values |
| Per-effect outputs | 3,780,000 float64 values |
| Two effect outputs combined | 7,560,000 float64 values |
| Four input arrays | ~120.96 MB |
| Two output arrays | ~60.48 MB |
| All six arrays | ~181.44 MB (~173.0 MiB) |

Input files are four little-endian binary float64 arrays, in **day-major** order. The generator uses seed **42**, benchmark daily returns within **-3% to +3%**, and portfolio return perturbations up to **5 basis points**, clipped to the same range. Portfolio and benchmark weights are normalized independently each day.

These inputs are **synthetic**. The two-effect decomposition is a benchmarking convention, **not yet confirmed as a production performance-attribution standard**:

```text
allocation[i,t] = (portfolio_weight[i,t] - benchmark_weight[i,t]) * benchmark_return[i,t]
selection[i,t]  = portfolio_weight[i,t] * (portfolio_return[i,t] - benchmark_return[i,t])
allocation + selection = portfolio_weight*portfolio_return - benchmark_weight*benchmark_return
```

## 2. Execution architecture

```text
                        Python fixture generator
                                  |
                    four binary float64 files
                     wp.bin wb.bin rp.bin rb.bin
                                  |
                    +-------------+--------------+
                    |                            |
               Python runtime                Go runtime
            np.memmap input arrays       binary.Read into []float64
                    |                            |
           NumPy ufunc / Numba JIT        Go worker goroutines
                    |                            |
          Numba prange over days          channel of day indices
             15,000 securities               15,000 securities
                per day                         per day
                    |                            |
             preallocated a,s             preallocated a,s
                    |                            |
             reconcile totals             reconcile totals
                    +-------------+--------------+
                                  |
                      JSON timing summary
```

**The timed portion is the computation kernel only.** Input generation, file loading, Numba compilation/warm-up, output persistence, and reconciliation are outside the timer. Both languages preallocate and reuse output arrays. Go initializes its goroutine/channel scheduling inside the measured compute call; Numba invokes its compiled parallel runtime. The implementations are comparable in arithmetic and output allocation policy, but their runtime overheads are not identical.

### Data sharing and true parallelism

- **Python:** all four input arrays use read-only NumPy memory maps. Numba's `@njit(parallel=True)` with `prange(days)` assigns independent day iterations to native threads. Each thread walks the 15,000 securities in its day sequentially, calculating both effects together and writing disjoint result locations. This compiled work can use multiple CPU cores without ordinary CPython bytecode/GIL serialization.
- **Go:** the four inputs are loaded into process-local float64 slices. Goroutines receive **day indices** from a channel, not copies of array payloads. Each goroutine reads shared immutable input slices and writes its assigned, non-overlapping output ranges. `GOMAXPROCS(workers)` controls the maximum number of logical CPUs executing Go code simultaneously.
- **Why days, not one task per security?** Each day is a contiguous block in the chosen memory layout; **252 independent tasks** provide enough scheduling granularity for the tested 1–8 worker counts. A task per security-day would mean 3.78 million tiny scheduling units. Security-level calculations are parallel **across distinct days**, but there is no separately scheduled thread/goroutine for every security.
- **Why fuse allocation and selection?** Both effects reuse nearby per-security inputs; computing them in one loop avoids redundant memory traversal and extra synchronization.

## 3. Measured results

### Initial five-run comparison

| Mode | Workers | Median (ms) |
|---|---:|---:|
| NumPy (previous non-threaded baseline; superseded) | reported as 1 | 2.870 |
| Numba serial | 1 | 1.588 |
| Numba parallel | 4 | 1.206 |
| Go goroutines | 4 | 1.576 |

The original NumPy result used the earlier non-threaded implementation. I will replace that comparison with my threaded NumPy 15k measurement once available. The more extensive 20-run results below should be preferred for worker scaling. NumPy's reported worker count does not establish that its underlying numerical libraries use exactly one OS thread.

### Multicore scaling: 20 measured runs per configuration

| Workers | Numba median (ms) | Go median (ms) | Numba speedup vs Numba-1 | Go speedup vs Go-1 |
|---:|---:|---:|---:|---:|
| 1 | 1.784 | 4.714 | 1.00x | 1.00x |
| 2 | 1.380 | 1.909 | 1.29x | 2.47x |
| 4 | 1.206 | 1.297 | 1.48x | 3.64x |
| 8 | **0.955** | **1.131** | **1.87x** | **4.17x** |

```text
Median compute time (ms; lower is better)
                1 worker        2 workers      4 workers    8 workers
Numba           1.784           1.380          1.206        0.955
Go              4.714           1.909          1.297        1.131
```

**At eight workers**, Numba is about **0.176 ms faster** than Go, or **15.6% lower elapsed time** than Go for this kernel. Relative scaling is different from absolute performance: Go improves more from its one-worker case, but still finishes after Numba at each tested worker count.

### Correctness checks

| Check | Observed outcome |
|---|---|
| Maximum Numba daily reconciliation error | ~1.12 × 10⁻¹⁹ |
| Maximum Go daily reconciliation error | ~3.25 × 10⁻¹⁹ |
| Total allocation effect | ~0.000854959330534 in both |
| Total selection effect | ~0.0000211005101234 in both |

The numbers agree to floating-point precision under the benchmark's reconciliation identity. This verifies consistency for the chosen synthetic formulas, **not** their financial appropriateness.

## 4. Architectural interpretation

**Observation: compiled Numba is fast even on one worker.** In the 20-run scaling comparison it took 1.784 ms versus Go's 4.714 ms. Numba uses a compiled numerical loop that avoids Python-object and interpreter overhead per security. It is plausible that compiler vectorization and/or generated machine instructions contribute, but those details have **not** been inspected.

**Observation: both scale, but not linearly.** Numba reaches 1.87x at eight threads; Go reaches 4.17x at eight workers. Neither is close to ideal 8x scaling. The difference may reflect scheduling costs, cache/DRAM behavior, compiler choices, CPU frequency, or the hardware's mix of performance and efficiency cores. The benchmark does **not** isolate which is dominant.

**Observation: Go's 2-worker speedup exceeds 2x** relative to its 1-worker result (2.47x). That should **not** be interpreted as more than two cores' worth of compute. It indicates the trials do not form a clean fixed-frequency, fixed-cache, fixed-scheduling scaling experiment. Repeat runs in interleaved/randomized worker-count order with system-load and CPU metadata before attributing the result to an optimization.

**Likely limit to investigate: memory movement.** Per security-day the formulas read four float64 numbers and write two outputs, around 48 bytes of logical access. Across 3.78 million security-days this is about 181.44 MB of logical reads/writes per pass. Dividing by the fastest runtime gives roughly **190 GB/s of effective logical array traffic**, **not** measured physical memory bandwidth, because OS caches, CPU caches, prefetching and actual store traffic change the picture. The arithmetic is very small relative to the data moved.

**Python GIL:** not an obstacle for the compiled parallel Numba calculation tested here. It would remain relevant for ordinary threaded, CPU-bound Python code executed as Python bytecode. **IPC:** not used inside either single-process implementation. Go goroutines share slices; Numba threads share memory-mapped inputs and ordinary output arrays.

## 5. Scope and threats to validity

1. **Only two fused arithmetic formulas.** The experiment is not representative yet of irregular, branch-heavy or multi-stage custom financial logic.
2. **Kernel-only timing.** File download/upload, storage adapters, event triggers, API polling, exports, and serialization are excluded.
3. **Different input residency:** Python uses `np.memmap`; Go eagerly loads slices. Files are read before timing, but residency/cache state can still differ.
4. **Different runtime setup:** Go creates a worker pool/channel each timed call; Numba uses its parallel execution runtime.
5. **No measured peak RAM, CPU utilization, hardware counters or sustained throughput.** The timings alone cannot prove a memory-bandwidth bottleneck.
6. **Potential OS thermal/cache noise:** one machine and sequential groups of runs; the specific Mac CPU, RAM, Go version and Python/Numba versions were not recorded in the supplied output.
7. **No cross-language elementwise comparison** beyond reconciliation and aggregate totals. Add direct output comparison against a reference implementation for a stronger correctness guarantee.
8. **Short durations:** around 1 ms at optimum. More batches and repeated sessions would increase confidence in relative ranking.

## 6. Next engineering decision

**Keep the current day-partitioned, fused-security-loop architecture for the baseline.** It correctly exploits independent daily security calculations and avoids per-security scheduling overhead. Do not yet add a task per security or switch languages purely on these results.

Before selecting a production engine, benchmark the real multistage custom formulas and capture: end-to-end wall time, stage timings, peak RSS, input/output time, correctness per security-day, hardware/runtime versions, and scaling at larger datasets. It is reasonable to keep Python + Numba as the leading candidate until those results show otherwise.

## 8. Larger dataset: 100,000 securities per universe (4 workers)

I increased the dataset from 15,000 to **100,000 securities per universe**, retaining 252 trading days. The updated day-streaming generator produced **100,800,000 float64 inputs** across four arrays (portfolio and benchmark weights and returns); the kernels produce **50,400,000 float64 effect outputs**. The combined logical input/output array footprint is approximately **1.21 GB** (decimal). I ran each mode for 20 repetitions with four workers on my MacBook Pro.

| Mode | Workers | Median (ms) | Relative runtime vs Numba |
|---|---:|---:|---:|
| **Numba parallel** | 4 | **6.031854** | **1.00x** |
| Go goroutines | 4 | 7.465354 | 1.24x |
| NumPy threaded | 4 | 11.045771 | 1.83x |

Numba took **19.2% less time than Go** and **45.4% less time than threaded NumPy** for this larger kernel measurement. All three passed the daily reconciliation check: threaded NumPy and Numba reported maximum absolute errors of approximately **4.07 × 10⁻²⁰**; Go reported approximately **3.79 × 10⁻¹⁹**. Their total allocation and selection effects matched to floating-point precision (approximately **−0.0000718324413562** and **0.0000139546228697**, respectively).

### What the larger test tells me

- Numba still has the lowest measured latency in this **four-worker** comparison.
- Threaded NumPy now participates in multicore execution; it uses multiple separate ufunc passes on each assigned day slice, while Numba and Go calculate both effects in one fused loop. More memory passes are a plausible contributor to its higher latency, **not a proven isolated cause**.
- At 100k securities, the data size is **6.67x** the earlier 15k dataset. Four-worker Numba median grows from **1.206 ms to 6.032 ms** (about **5.00x**); Go grows from **1.297 ms to 7.465 ms** (about **5.76x**). These use separately generated datasets and are scaling observations rather than controlled microarchitectural explanations.
- A new 15k **threaded NumPy** result is still **pending**. I should not use the historical 2.870 ms unthreaded NumPy timing as if it were a threaded result.
- These are still **kernel-only** timings. I have not yet measured input/output wall time, peak RSS, CPU hardware counters, or 100k worker-count scaling.

### Reproduce the 100k test

```bash
python generate.py --securities 100000 --days 252 --seed 42 --output data/100k
python benchmark.py --data data/100k --mode numpy-threaded --workers 4 --runs 20
python benchmark.py --data data/100k --mode numba-parallel --workers 4 --runs 20
go build -o benchmark-go .
./benchmark-go --data data/100k --workers 4 --runs 20
```

### Complete the 15k threaded NumPy comparison

Use the **original** `data/` fixture without regenerating it, so it remains comparable to the historical 15k Numba and Go measurements:

```bash
python benchmark.py --data data --mode numpy-threaded --workers 4 --runs 20
```

Once that output is available, I will add the measured median to the 15k comparison. No threaded NumPy 15k result is assumed in this report.

## 7. Reproduction

```bash
python -m pip install -r requirements.txt
python generate.py --securities 15000 --days 252 --seed 42

# Initial five-run comparison
python benchmark.py --mode numpy-threaded --workers 4 --runs 20  # new measurement pending
python benchmark.py --mode numba
python benchmark.py --mode numba-parallel --workers 4
go build -o benchmark-go .
./benchmark-go --workers 4

# Twenty-run worker scaling
python benchmark.py --mode numba-parallel --workers 1 --runs 20
python benchmark.py --mode numba-parallel --workers 2 --runs 20
python benchmark.py --mode numba-parallel --workers 4 --runs 20
python benchmark.py --mode numba-parallel --workers 8 --runs 20

./benchmark-go --workers 1 --runs 20
./benchmark-go --workers 2 --runs 20
./benchmark-go --workers 4 --runs 20
./benchmark-go --workers 8 --runs 20
```

**Benchmark provenance:** I executed the Python and Go benchmarks locally on my MacBook Pro and captured the terminal results. The architectural interpretations reflect my analysis of those measurements; I have not completed additional hardware profiling.
