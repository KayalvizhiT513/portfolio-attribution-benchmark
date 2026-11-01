# CSV vs Parquet: Go and Rust

Complete: 10 CSV runs, followed by 10 Parquet runs.

Parquet/CSV total wall-time ratio across the five scenarios: Rust **1.52×**, Go **0.97×** (below 1 means Parquet was faster in this single pass).

One full-history run per cell; 1,095 dates, one 60,000-security portfolio, a 100,000-security index, and 1,000 shared DE IDs. Four threads configured for both engines. BF effects and contribution columns included. CSV input writes CSV; Parquet input writes typed Zstandard Parquet.

## Total runtime (seconds)

| Scenario | Output rows | CSV Rust | CSV Go | Parquet Rust | Parquet Go |
|---|---:|---:|---:|---:|---:|
| Security | 318,000 | 14.58 | 23.50 | 24.76 | 23.78 |
| Country | 2,190 | 13.88 | 23.88 | 21.17 | 22.69 |
| Weekly | 316 | 14.09 | 23.48 | 21.62 | 23.31 |
| Monthly | 72 | 15.82 | 25.30 | 22.02 | 23.63 |
| Subset | 602 | 14.58 | 23.61 | 21.48 | 23.02 |

## Resources across all five scenarios

| Engine | CSV CPU s¹ | Parquet CPU s¹ | CSV max memory MiB² | Parquet max memory MiB² |
|---|---:|---:|---:|---:|
| Rust | 125.21 | 178.65 | 70.75 | 141.91 |
| Go | 218.16 | 233.81 | 135.22 | 253.34 |

¹ Total CPU time across the five scenarios. ² Highest memory use seen in any scenario.

Validation: 15/15 file comparisons passed (5 CSV Go/Rust, 5 Parquet Go/Rust, 5 CSV/Parquet). Every output field is compared; engines also enforce BF and contribution reconciliation.

Subset: 400 IDs—200 portfolio-only US, 100 index-only US, 100 shared DE; 300 held IDs × 2 + 2 residual rows = 602. Country: US-filtered, separately normalized daily attribution.

CSV ran first, then Parquet. The starting language alternates by scenario. We used the normal filesystem cache and measured each configuration once. Total runtime includes setup, reading, calculation and saving the finished output.

[Raw runs and stage metrics](results/csv-parquet-native-output/runs.csv) · [Validation](results/csv-parquet-native-output/checks.json) · [Environment](results/csv-parquet-native-output/environment.json)

<details>
<summary>How the CSV and Parquet runs work</summary>

| Layer | CSV | Parquet / Arrow |
|---|---|---|
| Storage / decode | Buffered text records; parse numeric strings | Project Parquet columns; decompress/decode into typed Arrow RecordBatches |
| Daily adapter | Parsed values → reusable raw buffers | Typed Arrow access → reusable raw buffers; date fragments assembled across batches |
| Calculation | Dense ID/country slots; existing BF/contribution engine | Same dense engine after Arrow decoding |
| Filtering | After full record validation | After full record validation; no instrument predicate pushdown |
| Output | Text CSV formatting and buffering | Typed Arrow output batches → Zstandard Parquet row groups and footer |

**These runs measure Phase 2: native Parquet-to-Arrow readers feeding the existing calculation buffers. They do not measure an Arrow-native/vectorized BF engine.** Each scenario diagram shows the CSV and Arrow/Parquet paths converging at the shared calculation buffers. Output paths split again: CSV formatting for CSV inputs; typed Arrow batches, Parquet encoding/compression and footer finalization for Parquet inputs.

### Concurrency scope

The index producer and portfolio worker execute concurrently. With this merged single-portfolio fixture, there is one portfolio worker; a capacity-1 channel provides bounded index prefetch. Within that worker, portfolio reading, filtering, aggregation, BF/contribution calculation and output writing execute sequentially. There are no separate application-level stage workers or writer queue.

Go `pqarrow` uses `Parallel=false` only for decoding columns within each individual reader. It does not disable the concurrent index producer and portfolio goroutine. `GOMAXPROCS=4` permits four logical CPUs but does not create four portfolio workers or parallelize the security loop. Enabling per-column decoding would be a separate configuration experiment requiring new measurements.

</details>

## Time by stage

Each scenario has the same layout: a timing table, its supporting numbers, then the next input format. Read down to follow the steps; compare Rust and Go across the row.

**Why stage times can exceed the whole run:** the index and portfolio workers can run at the same time. Stage figures add their time together; “Whole run” is how long you actually wait. Do not add the stage rows to estimate runtime.

One measurement per configuration. Small timing differences need repeat runs to confirm.

### Security

#### CSV input → CSV output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and check CSV records. | 24.480 | 38.789 |
| Filter | Keep all holdings; total the weights on each side. | 0.092 | 0.294 |
| Prepare / group | Scale each side’s weights to 100%; line up matching security IDs. | 0.316 | 0.481 |
| Calculate | Calculate BF effects and contributions for every security over the full period. | 0.978 | 2.677 |
| Write | Save the CSV report to disk. | 0.108 | 0.133 |
| **Whole run** | Elapsed time from start to finish. | 14.577 | 23.502 |

<details>
<summary>Counts, throughput, and memory — Security, CSV</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.157 | 4.517 |
| Read | CSV bytes parsed per second (MiB) | 491.707 | 310.323 |
| Read | Stored file size / read time (MiB/sec) | 491.838 | 310.406 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 555,045,359.466 | 364,124,490.730 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,068,095,072.504 | 390,169,902.684 |
| Calculate | Final BF effect values / calculation time | 975,429.290 | 356,319.546 |
| Calculate | Report rows | 318,000 | 318,000 |
| Write | Report rows written per second | 2,957,160.646 | 2,394,868.475 |
| Write | Output written per second (MiB) | 738.826 | 583.220 |
| Write | Report rows written | 318,000 | 318,000 |
| Write | Output file size (MB) | 83.309447 | 81.204076 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 70.750 | 135.219 |
| Whole run | Total CPU time (seconds) | 25.874797 | 43.835052 |
| Whole run | Average CPU cores used | 1.775 | 1.865 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

#### PARQUET input → PARQUET output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and decode Parquet batches. | 39.191 | 37.312 |
| Filter | Keep all holdings; total the weights on each side. | 0.096 | 0.302 |
| Prepare / group | Scale each side’s weights to 100%; line up matching security IDs. | 0.329 | 0.531 |
| Calculate | Calculate BF effects and contributions for every security over the full period. | 1.018 | 2.711 |
| Write | Save the PARQUET report to disk. | 0.099 | 0.289 |
| **Whole run** | Elapsed time from start to finish. | 24.761 | 23.783 |

<details>
<summary>Counts, throughput, and memory — Security, PARQUET</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 4.470 | 4.696 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 129.320 | 135.833 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 532,145,156.065 | 329,932,655.434 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,025,933,029.192 | 385,361,284.931 |
| Calculate | Final BF effect values / calculation time | 936,925.141 | 351,928.114 |
| Calculate | Report rows | 318,000 | 318,000 |
| Write | Report rows written per second | 3,196,537.671 | 1,100,223.215 |
| Write | Output written per second (MiB) | 139.904 | 50.269 |
| Write | Report rows written | 318,000 | 318,000 |
| Write | Output file size (MB) | 14.594064 | 15.235043 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 141.906 | 253.344 |
| Whole run | Total CPU time (seconds) | 38.095214 | 47.847328 |
| Whole run | Average CPU cores used | 1.539 | 2.012 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

<details>
<summary>View the security flow diagram</summary>

[![Security — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/security.svg)](docs/flows/csv-parquet/security.svg)

</details>

### Country

#### CSV input → CSV output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and check CSV records. | 22.965 | 39.955 |
| Filter | Keep US holdings in the portfolio and index. | 0.230 | 0.357 |
| Prepare / group | Scale US weights to 100% on each side; add weighted returns into one US group. | 0.174 | 0.314 |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | 0.000743 | 0.000974 |
| Write | Save the CSV report to disk. | 0.011 | 0.011 |
| **Whole run** | Elapsed time from start to finish. | 13.878 | 23.884 |

<details>
<summary>Counts, throughput, and memory — Country, CSV</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.629 | 4.385 |
| Read | CSV bytes parsed per second (MiB) | 524.150 | 301.269 |
| Read | Stored file size / read time (MiB/sec) | 524.290 | 301.349 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 173,010,000 | 173,010,000 |
| Prepare / group | Rows prepared per second | 996,094,510.387 | 550,982,288.268 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 2,190 | 2,190 |
| Calculate | Daily BF effect values calculated | 6,570 | 6,570 |
| Calculate | Daily BF effect values per second | 8,848,318.012 | 6,747,146.327 |
| Calculate | Final BF effect values / calculation time | 8,848,318.012 | 6,747,146.327 |
| Calculate | Report rows | 2,190 | 2,190 |
| Write | Report rows written per second | 197,508.364 | 203,144.548 |
| Write | Output written per second (MiB) | 61.053 | 62.358 |
| Write | Report rows written | 2,190 | 2,190 |
| Write | Output file size (MB) | 0.709851 | 0.704911 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 36.406 | 74.594 |
| Whole run | Total CPU time (seconds) | 23.226444 | 42.037703 |
| Whole run | Average CPU cores used | 1.674 | 1.760 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

#### PARQUET input → PARQUET output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and decode Parquet batches. | 33.689 | 36.605 |
| Filter | Keep US holdings in the portfolio and index. | 0.230 | 0.355 |
| Prepare / group | Scale US weights to 100% on each side; add weighted returns into one US group. | 0.164 | 0.316 |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | 0.000861 | 0.000881 |
| Write | Save the PARQUET report to disk. | 0.007 | 0.012 |
| **Whole run** | Elapsed time from start to finish. | 21.170 | 22.694 |

<details>
<summary>Counts, throughput, and memory — Country, PARQUET</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 5.200 | 4.786 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 150.440 | 138.456 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 173,010,000 | 173,010,000 |
| Prepare / group | Rows prepared per second | 1,055,502,102.582 | 547,900,248.067 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 2,190 | 2,190 |
| Calculate | Daily BF effect values calculated | 6,570 | 6,570 |
| Calculate | Daily BF effect values per second | 7,626,436.922 | 7,459,839.494 |
| Calculate | Final BF effect values / calculation time | 7,626,436.922 | 7,459,839.494 |
| Calculate | Report rows | 2,190 | 2,190 |
| Write | Report rows written per second | 322,011.090 | 186,604.919 |
| Write | Output written per second (MiB) | 29.098 | 17.229 |
| Write | Report rows written | 2,190 | 2,190 |
| Write | Output file size (MB) | 0.207507 | 0.212026 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 101.688 | 195.312 |
| Whole run | Total CPU time (seconds) | 34.082801 | 44.616701 |
| Whole run | Average CPU cores used | 1.610 | 1.966 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

<details>
<summary>View the country flow diagram</summary>

[![Country — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/country.svg)](docs/flows/csv-parquet/country.svg)

</details>

### Weekly

#### CSV input → CSV output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and check CSV records. | 23.203 | 39.099 |
| Filter | Keep all holdings; total the weights on each side. | 0.095 | 0.305 |
| Prepare / group | Prepare weights and matching security IDs for each day. | 0.349 | 0.548 |
| Calculate | Add security results to portfolio totals, then combine them by week. | 0.928 | 1.733 |
| Write | Save the CSV report to disk. | 0.006 | 0.007 |
| **Whole run** | Elapsed time from start to finish. | 14.090 | 23.477 |

<details>
<summary>Counts, throughput, and memory — Weekly, CSV</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.551 | 4.481 |
| Read | CSV bytes parsed per second (MiB) | 518.776 | 307.860 |
| Read | Stored file size / read time (MiB/sec) | 518.914 | 307.942 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 501,569,421.312 | 319,600,562.925 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,126,249,222.597 | 602,916,840.372 |
| Calculate | Final BF effect values / calculation time | 1,022.069 | 547.146 |
| Calculate | Report rows | 316 | 316 |
| Write | Report rows written per second | 53,306.370 | 48,057.489 |
| Write | Output written per second (MiB) | 14.911 | 13.185 |
| Write | Report rows written | 316 | 316 |
| Write | Output file size (MB) | 0.092687 | 0.090911 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 48.891 | 98.344 |
| Whole run | Total CPU time (seconds) | 24.415678 | 43.787871 |
| Whole run | Average CPU cores used | 1.733 | 1.865 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

#### PARQUET input → PARQUET output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and decode Parquet batches. | 34.302 | 37.284 |
| Filter | Keep all holdings; total the weights on each side. | 0.096 | 0.306 |
| Prepare / group | Prepare weights and matching security IDs for each day. | 0.351 | 0.563 |
| Calculate | Add security results to portfolio totals, then combine them by week. | 0.933 | 1.738 |
| Write | Save the PARQUET report to disk. | 0.005 | 0.007 |
| **Whole run** | Elapsed time from start to finish. | 21.625 | 23.306 |

<details>
<summary>Counts, throughput, and memory — Weekly, PARQUET</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 5.108 | 4.699 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 147.752 | 135.936 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 499,839,345.301 | 311,292,393.287 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,120,073,217.017 | 601,123,425.986 |
| Calculate | Final BF effect values / calculation time | 1,016.465 | 545.519 |
| Calculate | Report rows | 316 | 316 |
| Write | Report rows written per second | 60,570.257 | 47,681.941 |
| Write | Output written per second (MiB) | 6.467 | 5.132 |
| Write | Report rows written | 316 | 316 |
| Write | Output file size (MB) | 0.035379 | 0.035660 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 113.672 | 214.406 |
| Whole run | Total CPU time (seconds) | 35.679190 | 47.770195 |
| Whole run | Average CPU cores used | 1.650 | 2.050 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

<details>
<summary>View the weekly flow diagram</summary>

[![Weekly — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/weekly.svg)](docs/flows/csv-parquet/weekly.svg)

</details>

### Monthly

#### CSV input → CSV output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and check CSV records. | 26.281 | 42.099 |
| Filter | Keep all holdings; total the weights on each side. | 0.096 | 0.312 |
| Prepare / group | Prepare weights and matching security IDs for each day. | 0.353 | 0.563 |
| Calculate | Add security results to portfolio totals, then combine them by month. | 0.925 | 1.739 |
| Write | Save the CSV report to disk. | 0.006 | 0.009 |
| **Whole run** | Elapsed time from start to finish. | 15.822 | 25.298 |

<details>
<summary>Counts, throughput, and memory — Monthly, CSV</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 6.667 | 4.162 |
| Read | CSV bytes parsed per second (MiB) | 458.024 | 285.924 |
| Read | Stored file size / read time (MiB/sec) | 458.146 | 286.000 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 496,773,012.697 | 311,143,011.557 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,129,550,969.078 | 600,562,191.129 |
| Calculate | Final BF effect values / calculation time | 233.559 | 124.179 |
| Calculate | Report rows | 72 | 72 |
| Write | Report rows written per second | 11,899.023 | 8,130.921 |
| Write | Output written per second (MiB) | 3.355 | 2.250 |
| Write | Report rows written | 72 | 72 |
| Write | Output file size (MB) | 0.021288 | 0.020895 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 48.703 | 102.516 |
| Whole run | Total CPU time (seconds) | 27.420683 | 46.953158 |
| Whole run | Average CPU cores used | 1.733 | 1.856 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

#### PARQUET input → PARQUET output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and decode Parquet batches. | 34.962 | 37.785 |
| Filter | Keep all holdings; total the weights on each side. | 0.096 | 0.310 |
| Prepare / group | Prepare weights and matching security IDs for each day. | 0.351 | 0.588 |
| Calculate | Add security results to portfolio totals, then combine them by month. | 0.933 | 1.741 |
| Write | Save the PARQUET report to disk. | 0.007 | 0.007 |
| **Whole run** | Elapsed time from start to finish. | 22.023 | 23.627 |

<details>
<summary>Counts, throughput, and memory — Monthly, PARQUET</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 5.011 | 4.637 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 144.966 | 134.133 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 499,185,974.980 | 298,085,100.295 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,120,043,133.653 | 600,093,793.586 |
| Calculate | Final BF effect values / calculation time | 231.593 | 124.082 |
| Calculate | Report rows | 72 | 72 |
| Write | Report rows written per second | 10,580.065 | 11,044.984 |
| Write | Output written per second (MiB) | 1.804 | 1.875 |
| Write | Report rows written | 72 | 72 |
| Write | Output file size (MB) | 0.012874 | 0.012819 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 110.531 | 216.812 |
| Whole run | Total CPU time (seconds) | 36.313088 | 48.398475 |
| Whole run | Average CPU cores used | 1.649 | 2.048 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

<details>
<summary>View the monthly flow diagram</summary>

[![Monthly — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/monthly.svg)](docs/flows/csv-parquet/monthly.svg)

</details>

### Subset

#### CSV input → CSV output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and check CSV records. | 24.373 | 39.979 |
| Filter | Keep the 400 selected IDs on both sides. | 0.080 | 0.205 |
| Prepare / group | Scale selected weights to 100% on each side; line up selected IDs. | 0.004 | 0.004 |
| Calculate | Calculate selected-security results; combine index-only results into one summary. | 0.003 | 0.008 |
| Write | Save the CSV report to disk. | 0.012 | 0.004 |
| **Whole run** | Elapsed time from start to finish. | 14.584 | 23.609 |

<details>
<summary>Counts, throughput, and memory — Subset, CSV</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.188 | 4.382 |
| Read | CSV bytes parsed per second (MiB) | 493.868 | 301.087 |
| Read | Stored file size / read time (MiB/sec) | 493.999 | 301.167 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 547,500 | 547,500 |
| Prepare / group | Rows prepared per second | 136,594,912.133 | 128,909,307.181 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 547,500 | 547,500 |
| Calculate | Daily BF effect values calculated | 2,628,000 | 2,628,000 |
| Calculate | Daily BF effect values per second | 824,093,121.268 | 338,270,259.917 |
| Calculate | Final BF effect values / calculation time | 566,328.834 | 232,464.265 |
| Calculate | Report rows | 602 | 602 |
| Write | Report rows written per second | 51,172.888 | 134,547.569 |
| Write | Output written per second (MiB) | 13.556 | 34.929 |
| Write | Report rows written | 602 | 602 |
| Write | Output file size (MB) | 0.167226 | 0.163874 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 36.984 | 76.641 |
| Whole run | Total CPU time (seconds) | 24.268410 | 41.544603 |
| Whole run | Average CPU cores used | 1.664 | 1.760 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

#### PARQUET input → PARQUET output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and decode Parquet batches. | 34.452 | 37.644 |
| Filter | Keep the 400 selected IDs on both sides. | 0.084 | 0.195 |
| Prepare / group | Scale selected weights to 100% on each side; line up selected IDs. | 0.003 | 0.005 |
| Calculate | Calculate selected-security results; combine index-only results into one summary. | 0.003 | 0.008 |
| Write | Save the PARQUET report to disk. | 0.007 | 0.007 |
| **Whole run** | Elapsed time from start to finish. | 21.483 | 23.023 |

<details>
<summary>Counts, throughput, and memory — Subset, PARQUET</summary>

Steps appear in the same order as the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 5.085 | 4.654 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 147.108 | 134.635 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 547,500 | 547,500 |
| Prepare / group | Rows prepared per second | 171,420,037.327 | 118,175,984.438 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 547,500 | 547,500 |
| Calculate | Daily BF effect values calculated | 2,628,000 | 2,628,000 |
| Calculate | Daily BF effect values per second | 868,446,828.451 | 314,167,483.068 |
| Calculate | Final BF effect values / calculation time | 596,809.350 | 215,900.485 |
| Calculate | Report rows | 602 | 602 |
| Write | Report rows written per second | 85,756.049 | 86,474.647 |
| Write | Output written per second (MiB) | 5.202 | 5.340 |
| Write | Report rows written | 602 | 602 |
| Write | Output file size (MB) | 0.038292 | 0.038980 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 98.797 | 200.969 |
| Whole run | Total CPU time (seconds) | 34.481872 | 45.175742 |
| Whole run | Average CPU cores used | 1.605 | 1.962 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude the additional contribution columns. A runtime range is unavailable because there was one run.

</details>

<details>
<summary>View the subset flow diagram</summary>

[![Subset — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/subset.svg)](docs/flows/csv-parquet/subset.svg)

</details>
