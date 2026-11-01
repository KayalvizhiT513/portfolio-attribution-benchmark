# Country attribution: US only

Both engines use their date-cache follow-up measurements: Country is the median of three CSV/Parquet pairs per engine; each other scenario uses one pair. Rust and Go were measured in separate sessions.

Keep US holdings in the portfolio and index. Scale each side’s US weights to 100%, then calculate daily Brinson–Fachler effects and return contributions.

**Scope:** 1,095 dates (2023-01-01 to 2025-12-30), 59,000 US portfolio holdings and 99,000 US index holdings per date. DE holdings are excluded.

**Output:** 2,190 rows per run—one US group × 1,095 dates × base/local returns. CSV input writes CSV; Parquet input writes Parquet. All reports include the same 20 columns.

## Total runtime

| Input → output | Rust seconds | Go seconds | Output rows |
|---|---:|---:|---:|
| CSV → CSV | 13.067 | 23.218 | 2,190 |
| PARQUET → PARQUET | 14.776 | 16.772 | 2,190 |

## Country Parquet: read-time split

| Part of reading | What is actually measured | Rust seconds | Go seconds |
|---|---|---:|---:|
| Fetch decoded Arrow batches | File reading + decompression + decoding + Arrow array creation; timed around each batch fetch. | 6.859 | 16.004 |
| Read decoded rows and prepare daily calculation data | Read typed Arrow values, reuse cached date text, look up security positions, validate rows, assemble each date and fill reusable calculation arrays. Calculated as total read time minus batch-fetch time. | 14.973 | 11.307 |
| Total read | Both parts above; excludes filtering, grouping, Brinson calculations and output writing. | 21.832 | 27.311 |

**Decompression is included, not measured separately from file reading and decoding. No decompression optimization was made.** The implemented optimization caches date strings instead of formatting a date for every row. Preparing calculation data still handles rows individually; it is more than copying an Arrow buffer.

These are separate profiling runs, not the normal runtime runs. For each engine, all three country runs cover all 1,095 dates; the table uses the run with the middle total read time and takes both parts from that same run. Times add the concurrent index and portfolio readers, so they can exceed whole-run runtime. Rust and Go were measured in separate sessions; Go diagnostics also enabled CPU/allocation profiling.

Both engines passed all six country Parquet profiling-output comparisons against their CSV and Parquet reference outputs. This checks decoded results; it does not isolate decompression performance.

[Rust profiling measurements](results/rust-date-cache/profile/runs.json) · [Go profiling measurements](results/go-date-cache/profile/runs.json)

<details>
<summary>Go: decoding, row handling, allocations and garbage collection</summary>

Country diagnostic runs only: three runs per format, separate from the normal timing runs above. These measure the whole process, including setup and output.

| Memory / garbage collection | CSV | Parquet with cached dates |
|---|---:|---:|
| Total allocated GiB | 11.577 | 80.389 |
| Allocated objects (millions) | 175.698 | 4.243 |
| Garbage collections | 467.000 | 1173.000 |
| Estimated garbage-collection CPU seconds | 1.273 | 6.969 |
| Pause seconds | 0.026 | 0.110 |

Values above are medians. Allocated GiB is the total allocated over the run, **not peak memory**. Garbage-collection CPU is a Go-runtime estimate that may overestimate CPU use and is not directly comparable with OS CPU time. It overlaps the stage measurements; do not add it to runtime.

| Work sampled on CPU | CSV CPU seconds | Parquet CPU seconds |
|---|---:|---:|
| File reads | 17.267 | 7.683 |
| Text parsing / Parquet decoding | 10.380 | 2.707 |
| Row handling and ID lookup | 2.850 | 4.460 |
| Allocate / resize memory | 0.543 | 0.733 |
| Garbage collection | 0.780 | 2.903 |
| Release unused heap memory | 0.037 | 1.800 |
| Go runtime / profiling support | 4.513 | 10.207 |

CPU rows are means from 10 ms samples, not elapsed stage times. Each sample is counted once; parsing/decoding and row handling exclude samples assigned to file reads, allocation or garbage collection. Runtime support includes scheduling, waits and profiling support. Remaining calculation, output and setup samples are in the full Go report.

CSV spends most sampled CPU on file reads and text parsing. Parquet allocates large Arrow and dictionary-decoding buffers. A separate single-run control with the date cache disabled creates about 175.2 million more objects than cached Parquet; the cache removes repeated date-string allocations.

[Full Go analysis](GO_DATE_READ_REPORT.md) · [Diagnostic measurements](results/go-date-cache/profile/runs.json) · [Diagnostic checks](results/go-date-cache/profile/checks.json)

</details>


## Elapsed time and processing steps

All timing columns below use elapsed clock seconds. Only whole-run elapsed time was recorded; accumulated worker times are kept in the expandable details.

### CSV input → CSV output

| Step | Work done | Rust elapsed seconds | Go elapsed seconds |
|---|---|---:|---:|
| Read | Read and check CSV records. | — | — |
| Filter | Keep US holdings in the portfolio and index. | — | — |
| Prepare / group | Scale US weights to 100% on each side; add weighted returns into one US group. | — | — |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | — | — |
| Write | Save the CSV report to disk. | — | — |
| **Whole run** | Elapsed time from start to finish. | 13.067 | 23.218 |

— = elapsed stage time was not recorded. Existing stage measurements add worker times and are available in the expandable details; they cannot be converted into elapsed stage times.

<details>
<summary>Worker times, counts, throughput, and memory — CSV</summary>

Steps follow the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 8.059 | 4.513 |
| Read | CSV bytes parsed per second (MiB) | 553.706 | 310.065 |
| Read | Stored file size / read time (MiB/sec) | 553.854 | 310.148 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 173,010,000 | 173,010,000 |
| Prepare / group | Rows prepared per second | 1,026,188,197.043 | 559,628,419.409 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 2,190 | 2,190 |
| Calculate | Daily BF effect values calculated | 6,570 | 6,570 |
| Calculate | Daily BF effect values per second | 7,049,325.969 | 8,023,741.482 |
| Calculate | Final BF effect values / calculation time | 7,049,325.969 | 8,023,741.482 |
| Calculate | Report rows | 2,190 | 2,190 |
| Write | Report rows written per second | 195,566.306 | 242,016.628 |
| Write | Output written per second (MiB) | 60.453 | 74.291 |
| Write | Report rows written | 2,190 | 2,190 |
| Write | Output file size (MB) | 0.709851 | 0.704911 |
| Whole run | Runtime range across repeat runs | 12.982–13.283 | 22.114–23.246 |
| Whole run | Highest memory use (MiB) | 40.250 | 76.344 |
| Whole run | Total CPU time (seconds) | 22.089002 | 40.954650 |
| Whole run | Average CPU cores used | 1.677 | 1.770 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. Both engines use three-run country medians.

</details>

### PARQUET input → PARQUET output

| Step | Work done | Rust elapsed seconds | Go elapsed seconds |
|---|---|---:|---:|
| Read | Read and decode Parquet batches. | — | — |
| Filter | Keep US holdings in the portfolio and index. | — | — |
| Prepare / group | Scale US weights to 100% on each side; add weighted returns into one US group. | — | — |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | — | — |
| Write | Save the PARQUET report to disk. | — | — |
| **Whole run** | Elapsed time from start to finish. | 14.776 | 16.772 |

— = elapsed stage time was not recorded. Existing stage measurements add worker times and are available in the expandable details; they cannot be converted into elapsed stage times.

<details>
<summary>Worker times, counts, throughput, and memory — PARQUET</summary>

Steps follow the timing table. Rates use the time spent in that step.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.461 | 6.507 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 215.844 | 188.235 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 173,010,000 | 173,010,000 |
| Prepare / group | Rows prepared per second | 1,073,595,961.924 | 552,975,327.200 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 2,190 | 2,190 |
| Calculate | Daily BF effect values calculated | 6,570 | 6,570 |
| Calculate | Daily BF effect values per second | 8,170,656.028 | 8,093,186.202 |
| Calculate | Final BF effect values / calculation time | 8,170,656.028 | 8,093,186.202 |
| Calculate | Report rows | 2,190 | 2,190 |
| Write | Report rows written per second | 293,929.589 | 191,744.867 |
| Write | Output written per second (MiB) | 26.560 | 17.704 |
| Write | Report rows written | 2,190 | 2,190 |
| Write | Output file size (MB) | 0.207507 | 0.212026 |
| Whole run | Runtime range across repeat runs | 12.868–15.090 | 16.637–17.046 |
| Whole run | Highest memory use (MiB) | 110.000 | 206.281 |
| Whole run | Total CPU time (seconds) | 21.285223 | 34.304759 |
| Whole run | Average CPU cores used | 1.447 | 2.043 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. Both engines use three-run country medians.

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

**Latest checks:** Rust country has 15 output comparisons and 6 profiling comparisons; Go country has 15 output comparisons and 14 diagnostic/control comparisons. All passed; original baseline checks are retained. Every output value is compared; each engine checks BF and contribution totals.

Rust and Go each use three fresh CSV/Parquet country pairs with alternating format order, measured in separate sessions. All use four configured threads and normal filesystem caching; this report reuses those completed measurements.

[Full comparison](CSV_PARQUET_REPORT.md) · [Latest Rust measurements](results/rust-date-cache/runs.csv) · [Latest Go measurements](results/go-date-cache/runs.csv) · [Original measurements](results/csv-parquet-native-output/runs.csv) · [Checks](results/csv-parquet-native-output/checks.json) · [Calculation definitions](docs/CONTRIBUTIONS.md)
