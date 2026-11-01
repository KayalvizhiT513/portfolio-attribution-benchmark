# Country attribution: US only

Keep US holdings in the portfolio and index. Scale each side’s US weights to 100%, then calculate daily Brinson–Fachler effects and return contributions.

**Scope:** 1,095 dates (2023-01-01 to 2025-12-30), 59,000 US portfolio holdings and 99,000 US index holdings per date. DE holdings are excluded.

**Output:** 2,190 rows per run—one US group × 1,095 dates × base/local returns. CSV input writes CSV; Parquet input writes Parquet. All reports include the same 20 columns.

## Total runtime

| Input → output | Rust seconds | Go seconds | Output rows |
|---|---:|---:|---:|
| CSV → CSV | 13.878 | 23.884 | 2,190 |
| PARQUET → PARQUET | 21.170 | 22.694 | 2,190 |

## Time by stage

The index and portfolio workers can run at the same time. Stage figures add their time together; **Whole run** is how long you actually wait. Do not add the stage rows to estimate runtime.

### CSV input → CSV output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and check CSV records. | 22.965 | 39.955 |
| Filter | Keep US holdings in the portfolio and index. | 0.230 | 0.357 |
| Prepare / group | Scale US weights to 100% on each side; add weighted returns into one US group. | 0.174 | 0.314 |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | 0.000743 | 0.000974 |
| Write | Save the CSV report to disk. | 0.011 | 0.011 |
| **Whole run** | Elapsed time from start to finish. | 13.878 | 23.884 |

<details>
<summary>Counts, throughput, and memory — CSV</summary>

Steps follow the timing table. Rates use the time spent in that step.

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

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. Repeat-run ranges are unavailable because each configuration was measured once.

</details>

### PARQUET input → PARQUET output

| Step | Work done | Rust seconds | Go seconds |
|---|---|---:|---:|
| Read | Read and decode Parquet batches. | 33.689 | 36.605 |
| Filter | Keep US holdings in the portfolio and index. | 0.230 | 0.355 |
| Prepare / group | Scale US weights to 100% on each side; add weighted returns into one US group. | 0.164 | 0.316 |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | 0.000861 | 0.000881 |
| Write | Save the PARQUET report to disk. | 0.007 | 0.012 |
| **Whole run** | Elapsed time from start to finish. | 21.170 | 22.694 |

<details>
<summary>Counts, throughput, and memory — PARQUET</summary>

Steps follow the timing table. Rates use the time spent in that step.

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

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. Repeat-run ranges are unavailable because each configuration was measured once.

</details>

<details>
<summary>View the country flow diagram</summary>

[![Country: CSV/Parquet paths and Rust/Go stages](docs/flows/csv-parquet/country.svg)](docs/flows/csv-parquet/country.svg)

</details>

<details>
<summary>Calculation and worker details</summary>

US weights and weighted base/local returns are added into one US group separately for the portfolio and index on each date. There is no linking across dates in this scenario. With one normalized country group, allocation and interaction are zero apart from rounding; selection equals the difference between US portfolio and US index returns.

Daily portfolio and benchmark contributions equal the corresponding US weighted returns. Active contribution is their difference. The index producer runs concurrently with one portfolio worker; stages inside the portfolio worker run in sequence. Parquet uses Arrow for reading and writing, while the calculations use the shared daily buffers.

</details>

## Checks and measurement notes

**3/3 comparisons passed:** CSV Rust/Go, Parquet Rust/Go, and CSV/Parquet. Every output value is compared; each engine checks BF and contribution totals.

These are the existing full-history measurements, not a new run. One run per language/format, four threads configured, CSV first, normal filesystem caching. Small timing differences need repeat runs to confirm.

[Full comparison](CSV_PARQUET_REPORT.md) · [Raw measurements](results/csv-parquet-native-output/runs.csv) · [Checks](results/csv-parquet-native-output/checks.json) · [Calculation definitions](docs/CONTRIBUTIONS.md)
