# Native attribution benchmark

Scope: first 3 effective dates, all supplied portfolios, 3 repetitions. OS-managed filesystem cache; language order alternates. First run is retained. Results describe these implementations and this workload, not universal language performance.

Machine: macOS-27.0.1-arm64-arm-64bit-Mach-O; 12 logical CPUs. Rust: 4 portfolio-worker limit; Go: GOMAXPROCS=12 with concurrent I/O goroutines. Separate settings use each runtime's concurrency model; no shared thread cap is imposed.

## Overall results

| Scenario | Rust seconds (min–max) | Go seconds (min–max) | Go/Rust time | Rust/Go peak MiB | Rust/Go CPU seconds | Rust/Go average cores |
|---|---:|---:|---:|---:|---:|---:|
| security | 0.283 (0.274–0.293) | 0.325 (0.324–0.329) | 1.15× | 117.2 / 207.8 | 0.85 / 1.06 | 3.00 / 3.24 |
| subset | 0.086 (0.086–0.089) | 0.103 (0.102–0.103) | 1.20× | 42.2 / 64.0 | 0.10 / 0.16 | 1.18 / 1.57 |
| country | 0.087 (0.082–0.090) | 0.107 (0.102–0.117) | 1.22× | 37.1 / 59.8 | 0.10 / 0.16 | 1.17 / 1.54 |
| weekly | 0.221 (0.219–0.223) | 0.247 (0.245–0.253) | 1.12× | 117.5 / 185.2 | 0.63 / 0.77 | 2.86 / 3.15 |
| monthly | 0.169 (0.167–0.175) | 0.193 (0.186–0.193) | 1.14× | 112.1 / 175.2 | 0.38 / 0.49 | 2.22 / 2.55 |

Go/Rust time > 1 means Rust was faster. Peak RSS is the median per-process maximum resident set, not total OS cache usage. CPU seconds include all native process threads.

## Stage efficiency

Stage times are summed elapsed time inside each stage across reader/worker threads. They include scheduling pauses and can overlap; they are not CPU time and do not add up to overall wall time. Rates below divide work by this accumulated stage time. Read includes CSV decoding, numeric parsing, key lookup and input validation. Aggregate includes normalization, dense-group allocation, and weighted contributions. Write includes formatting, buffering, flushing and fsync. Master loading, channel waits and setup are only in overall time. Filter is N/A except for the standalone-subset scenario.

| Scenario | Language | Read s; M rows/s; MiB/s | Filter s; M rows/s | Aggregate s; M retained rows/s | Compute s; M effect values/s | Write s; M rows/s; MiB/s |
|---|---|---:|---:|---:|---:|---:|
| security | rust | 0.0718; 6.681; 403.0 | N/A | 0.0046; 105.183 | 0.0135; 534.543 | 0.7514; 3.194; 644.1 |
| security | go | 0.1076; 4.461; 269.0 | N/A | 0.0057; 84.078 | 0.0416; 173.046 | 0.9048; 2.653; 527.6 |
| subset | rust | 0.0627; 7.653; 461.6 | 0.0002; 2352.445 | 0.0000; 84.581 | 0.0000; 862.921 | 0.0369; 0.261; 53.8 |
| subset | go | 0.1040; 4.618; 278.5 | 0.0005; 999.915 | 0.0000; 70.669 | 0.0001; 320.004 | 0.0356; 0.271; 54.5 |
| country | rust | 0.0605; 7.930; 478.3 | N/A | 0.0005; 1018.025 | 0.0000; 36.000 | 0.0329; 0.001; 0.3 |
| country | go | 0.1069; 4.490; 270.8 | N/A | 0.0009; 540.820 | 0.0000; 19.645 | 0.0296; 0.002; 0.4 |
| weekly | rust | 0.0734; 6.540; 394.5 | N/A | 0.0044; 108.557 | 0.0115; 626.382 | 0.5293; 3.023; 605.0 |
| weekly | go | 0.1101; 4.359; 262.9 | N/A | 0.0067; 71.433 | 0.0372; 193.518 | 0.6092; 2.626; 517.7 |
| monthly | rust | 0.0695; 6.908; 416.6 | N/A | 0.0040; 120.864 | 0.0105; 684.652 | 0.3040; 2.631; 525.2 |
| monthly | go | 0.1068; 4.496; 271.2 | N/A | 0.0082; 58.644 | 0.0302; 238.026 | 0.3345; 2.392; 471.6 |

## Correctness

| Scenario | Rows compared | Maximum absolute numeric difference | Status |
|---|---:|---:|---|
| security | 2,400,024 | 0 | PASS |
| subset | 9,624 | 0 | PASS |
| country | 48 | 0 | PASS |
| weekly | 1,600,016 | 1.73e-18 | PASS |
| monthly | 800,008 | 1.73e-18 | PASS |

Every date and period also reconciles within each implementation. The independent synthetic oracle in `python/test_attribution.py` covers nine scenario/period combinations and invalid inputs. Numeric comparisons allow absolute tolerance 1e-10 or relative tolerance 1e-8. Output is decimal round-trip precision; Rust and Go may use different exponent notation.

Input byte throughput uses parsed return-file bytes, including one-record lookahead at the date limit; it excludes security-master/header bytes. Full input file sizes are recorded separately and are not used as the byte-throughput numerator for truncated runs. Filtering is computational filtering after CSV parsing, so a small subset does not avoid reading source rows.

The subset is the union of the first 100 instruments found in each portfolio. Each side is independently normalized every date. Period labels use observed first/last effective dates, including partial weeks/months. These runs do not establish full-history output-write performance. Use `--days 0` with sufficient disk space to measure it.

Raw repetitions: `runs.csv`; medians, ranges and throughput: `summary.csv`; configuration and correctness: `environment.json`.
