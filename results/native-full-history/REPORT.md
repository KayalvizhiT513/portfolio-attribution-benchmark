# Native attribution benchmark

Scope: all effective dates, all supplied portfolios, 2 repetitions. OS-managed filesystem cache; language order alternates. First run is retained. Results describe these implementations and this workload, not universal language performance.

Machine: macOS-27.0.1-arm64-arm-64bit-Mach-O; 12 logical CPUs. Rust: 4 portfolio-worker limit; Go: GOMAXPROCS=12 with concurrent I/O goroutines. Separate settings use each runtime's concurrency model; no shared thread cap is imposed.

## Overall results

| Scenario | Rust seconds (min–max) | Go seconds (min–max) | Go/Rust time | Rust/Go peak MiB | Rust/Go CPU seconds | Rust/Go average cores |
|---|---:|---:|---:|---:|---:|---:|
| country | 13.038 (13.008–13.068) | 22.175 (22.045–22.305) | 1.70× | 45.4 / 74.9 | 23.17 / 37.85 | 1.78 / 1.71 |
| subset | 13.104 (12.874–13.334) | 22.171 (22.117–22.224) | 1.69× | 41.8 / 78.9 | 24.26 / 39.31 | 1.85 / 1.77 |

Go/Rust time > 1 means Rust was faster. Peak RSS is the median per-process maximum resident set, not total OS cache usage. CPU seconds include all native process threads.

## Stage efficiency

Stage times are summed elapsed time inside each stage across reader/worker threads. They include scheduling pauses and can overlap; they are not CPU time and do not add up to overall wall time. Rates below divide work by this accumulated stage time. Read includes CSV decoding, numeric parsing, key lookup and input validation. Aggregate includes normalization, dense-group allocation, and weighted contributions. Write includes formatting, buffering, flushing and fsync. Master loading, channel waits and setup are only in overall time. Filter is N/A except for the standalone-subset scenario.

| Scenario | Language | Read s; M rows/s; MiB/s | Filter s; M rows/s | Aggregate s; M retained rows/s | Compute s; M effect values/s | Write s; M rows/s; MiB/s |
|---|---|---:|---:|---:|---:|---:|
| country | rust | 22.9447; 7.636; 460.6 | N/A | 0.2059; 850.884 | 0.0004; 70.144 | 0.0544; 0.322; 71.9 |
| country | go | 35.0243; 5.002; 301.7 | N/A | 0.3269; 535.943 | 0.0008; 35.022 | 0.0464; 0.378; 83.2 |
| subset | rust | 23.0838; 7.590; 457.8 | 0.0730; 2400.946 | 0.0073; 120.173 | 0.0122; 861.654 | 1.3393; 2.623; 540.7 |
| subset | go | 35.4777; 4.938; 297.9 | 0.1653; 1060.103 | 0.0100; 87.570 | 0.0334; 315.052 | 1.4495; 2.423; 487.4 |

## Correctness

| Scenario | Rows compared | Maximum absolute numeric difference | Status |
|---|---:|---:|---|
| country | 17,520 | 0 | PASS |
| subset | 3,512,760 | 0 | PASS |

Every date and period also reconciles within each implementation. The independent synthetic oracle in `python/test_attribution.py` covers nine scenario/period combinations and invalid inputs. Numeric comparisons allow absolute tolerance 1e-10 or relative tolerance 1e-8. Output is decimal round-trip precision; Rust and Go may use different exponent notation.

Input byte throughput uses parsed return-file bytes, including one-record lookahead at the date limit; it excludes security-master/header bytes. Full input file sizes are recorded separately and are not used as the byte-throughput numerator for truncated runs. Filtering is computational filtering after CSV parsing, so a small subset does not avoid reading source rows.

The subset is the union of the first 100 instruments found in each portfolio. Each side is independently normalized every date. Period labels use observed first/last effective dates, including partial weeks/months. These runs do not establish full-history output-write performance. Use `--days 0` with sufficient disk space to measure it.

Raw repetitions: `runs.csv`; medians, ranges and throughput: `summary.csv`; configuration and correctness: `environment.json`.
