# Historical CSV and Parquet benchmarks: five attribution scenarios in Rust and Go

Archived experiment with a different dataset or execution path. See the [current country attribution performance report](COUNTRY_REPORT_EFFECTIVE_DATED_SMF.md) for optimized CSV and Parquet results.

Country uses the latest rerun: three runs per engine/format. Its elapsed-time breakdown uses one complete run per column, selected by the middle whole-run time; supporting metrics use medians. Other scenarios retain their date-cache follow-up runs. The latest country runs alternate format and engine order and execute sequentially.
Updated with date caches in both Rust and Go.

Parquet/CSV total wall-time ratio across the five scenarios: Rust **1.15×**, Go **0.73×** (below 1 means Parquet was faster for the displayed results).

Country cells are three-run medians for both engines; other cells are single runs; 1,095 dates, one 60,000-security portfolio, a 100,000-security index, and 1,000 shared DE IDs. Four threads configured for both engines. BF effects and contribution columns included. CSV input writes CSV; Parquet input writes typed Zstandard Parquet.

## Total runtime (seconds)

| Scenario | Output rows | CSV Rust | CSV Go | Parquet Rust | Parquet Go |
|---|---:|---:|---:|---:|---:|
| Security | 318,000 | 14.22 | 23.91 | 16.77 | 17.47 |
| Country | 2,190 | 13.43 | 22.98 | 14.94 | 16.64 |
| Weekly | 316 | 14.45 | 24.16 | 16.74 | 17.42 |
| Monthly | 72 | 14.10 | 23.75 | 16.37 | 17.46 |
| Subset | 602 | 13.88 | 23.06 | 15.62 | 16.79 |

## Resources across all five scenarios

| Engine | CSV CPU s¹ | Parquet CPU s¹ | CSV max memory MiB² | Parquet max memory MiB² |
|---|---:|---:|---:|---:|
| Rust | 120.34 | 118.71 | 70.81 | 146.95 |
| Go | 214.91 | 179.68 | 163.36 | 261.39 |

¹ Sum of the displayed scenario CPU values; Country uses three-run medians for both engines. ² Highest memory use observed, including repeats.

Checks: the updated Rust and Go outputs each passed 35 comparisons against the original results and fresh CSV/Parquet outputs. Rust has 14 passed profiling checks; Go has 14 passed diagnostic/control checks. Each engine checks BF and contribution totals.

Subset: 400 IDs—200 portfolio-only US, 100 index-only US, 100 shared DE; 300 held IDs × 2 + 2 residual rows = 602. Country: US-filtered, separately normalized daily attribution.

Country uses the latest three-run-per-format worker-timing rerun, alternating format and engine order. Other scenarios retain one date-cache follow-up pair per engine. All used normal filesystem caching. Total runtime includes setup, reading, calculation and saving the finished output.

[Raw runs and stage metrics](results/csv-parquet-native-output/runs.csv) · [Validation](results/csv-parquet-native-output/checks.json) · [Environment](results/csv-parquet-native-output/environment.json)

Latest country worker-timing rerun: 12 complete runs and 24 passed reference-output comparisons. [Measurements](results/country-worker-stages/runs.csv) · [Checks](results/country-worker-stages/checks.json).

Latest measurements: [Rust](results/rust-date-cache/runs.csv) · [Go](results/go-date-cache/runs.csv). Detailed diagnostics: [Go read report](GO_DATE_READ_REPORT.md).
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

## Rust Parquet: where read time goes

These separate profiling runs split the read step. Times combine both readers, so they can exceed total runtime. Country uses the run with the middle read time out of three; other scenarios use one.

| Scenario | Read + decompress/decode seconds | Read decoded rows + prepare calculation data seconds | Total read seconds |
|---|---:|---:|---:|
| Security | 7.077 | 16.147 | 23.224 |
| Country | 6.859 | 14.973 | 21.832 |
| Weekly | 7.014 | 15.710 | 22.724 |
| Monthly | 7.064 | 15.414 | 22.478 |
| Subset | 7.008 | 14.641 | 21.649 |

The first part includes file I/O, decompression, decoding and Arrow batch creation. The second includes row handling, date caching, ID lookup, validation and building daily buffers. It is the remaining read time after subtracting batch time.

[Full date-cache comparison and profiling evidence](RUST_DATE_READ_REPORT.md)

## Go Parquet: where read time goes

These separate profiling runs split the read step. Times combine both readers, so they can exceed total runtime. Country uses the run with the middle read time out of three; other scenarios use one.

| Scenario | Read + decompress/decode seconds | Read decoded rows + prepare calculation data seconds | Total read seconds |
|---|---:|---:|---:|
| Country | 16.004 | 11.307 | 27.311 |

The first part includes file I/O, decompression, decoding and Arrow batch creation. The second includes row handling, date caching, ID lookup, validation and building daily buffers. It is the remaining read time after subtracting batch time.

[Full date-cache comparison and profiling evidence](GO_DATE_READ_REPORT.md)

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

## Elapsed time and processing steps

Each scenario has the same layout: a timing table, its supporting numbers, then the next input format. Read down to follow the steps; compare Rust and Go across the row.

Country shows an elapsed-time breakdown that sums to whole-run time, including setup, waiting and remaining work. Other scenarios retain processing descriptions until their worker timings are rerun. Combined worker measurements remain in expandable details.

Country uses the latest rerun: three runs per engine/format. Its elapsed-time breakdown uses one complete run per column, selected by the middle whole-run time; supporting metrics use medians. Other scenarios retain their date-cache follow-up runs. The latest country runs alternate format and engine order and execute sequentially.

### Security

#### CSV input → CSV output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 14.222 | 23.915 |

| Step | Work done |
|---|---|
| Read | Read and check CSV records. |
| Filter | Keep all holdings; total the weights on each side. |
| Prepare / group | Scale each side’s weights to 100%; line up matching security IDs. |
| Calculate | Calculate BF effects and contributions for every security over the full period. |
| Write | Save the CSV report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Security, CSV</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.467 | 4.449 |
| Read | CSV bytes parsed per second (MiB) | 513.048 | 305.665 |
| Read | Stored file size / read time (MiB/sec) | 513.184 | 305.747 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 537,188,744.310 | 340,956,545.649 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,035,704,459.502 | 397,879,816.254 |
| Calculate | Final BF effect values / calculation time | 945,848.821 | 363,360.563 |
| Calculate | Report rows | 318,000 | 318,000 |
| Write | Report rows written per second | 2,799,156.310 | 2,380,015.343 |
| Write | Output written per second (MiB) | 699.350 | 579.603 |
| Write | Report rows written | 318,000 | 318,000 |
| Write | Output file size (MB) | 83.309447 | 81.204076 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 70.812 | 163.359 |
| Whole run | Total CPU time (seconds) | 24.898769 | 44.279602 |
| Whole run | Average CPU cores used | 1.751 | 1.852 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

#### PARQUET input → PARQUET output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 16.773 | 17.468 |

| Step | Work done |
|---|---|
| Read | Read and decode Parquet batches. |
| Filter | Keep all holdings; total the weights on each side. |
| Prepare / group | Scale each side’s weights to 100%; line up matching security IDs. |
| Calculate | Calculate BF effects and contributions for every security over the full period. |
| Write | Save the PARQUET report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Security, PARQUET</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 6.639 | 6.556 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 192.065 | 189.643 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 520,539,816.309 | 305,938,148.935 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,008,749,772.288 | 394,202,305.563 |
| Calculate | Final BF effect values / calculation time | 921,232.669 | 360,002.106 |
| Calculate | Report rows | 318,000 | 318,000 |
| Write | Report rows written per second | 3,161,336.701 | 1,130,814.764 |
| Write | Output written per second (MiB) | 138.363 | 51.666 |
| Write | Report rows written | 318,000 | 318,000 |
| Write | Output file size (MB) | 14.594064 | 15.235043 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 146.953 | 261.391 |
| Whole run | Total CPU time (seconds) | 24.825229 | 36.671846 |
| Whole run | Average CPU cores used | 1.480 | 2.099 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

<details>
<summary>View the security flow diagram</summary>

[![Security — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/security.svg)](docs/flows/csv-parquet/security.svg)

</details>

### Country

#### CSV input → CSV output

| Step | Work done | Rust elapsed seconds | Go elapsed seconds |
|---|---|---:|---:|
| Read | Read and check portfolio CSV records. | 9.245057 | 15.927452 |
| Filter | Keep US portfolio holdings. | 0.103318 | 0.138032 |
| Prepare / group | Scale portfolio US weights to 100% and combine their weighted returns into one US group. | 0.070083 | 0.116835 |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | 0.000766 | 0.000832 |
| Write | Save the CSV report to disk. | 0.011830 | 0.010097 |
| Setup, waiting, and remaining work | Everything outside the timed portfolio stages, including waiting for index data, setup and shutdown; calculated as whole run minus the five stages. | 3.998659 | 6.786590 |
| **Whole run** | **Sum of the six rows above.** | 13.429713 | 22.979838 |

Each column sums to whole-run elapsed time. All values in a column come from the same run: the middle whole-run time of three repetitions. The five timed stages follow the portfolio worker; index preparation runs concurrently and is not added again. The remaining-time row is a subtraction, not a separately measured index-reading or waiting timer. Other supporting metrics retain three-run medians.

[Latest country measurements](results/country-worker-stages/runs.csv) · [Output checks](results/country-worker-stages/checks.json)

<details>
<summary>Counts, throughput, and memory — Country, CSV</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 8.024 | 4.561 |
| Read | CSV bytes parsed per second (MiB) | 551.294 | 313.387 |
| Read | Stored file size / read time (MiB/sec) | 551.441 | 313.471 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 173,010,000 | 173,010,000 |
| Prepare / group | Rows prepared per second | 1,064,181,171.822 | 553,287,476.298 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 2,190 | 2,190 |
| Calculate | Daily BF effect values calculated | 6,570 | 6,570 |
| Calculate | Daily BF effect values per second | 8,577,706.580 | 7,843,989.379 |
| Calculate | Final BF effect values / calculation time | 8,577,706.580 | 7,843,989.379 |
| Calculate | Report rows | 2,190 | 2,190 |
| Write | Report rows written per second | 181,655.408 | 216,898.063 |
| Write | Output written per second (MiB) | 56.153 | 66.580 |
| Write | Report rows written | 2,190 | 2,190 |
| Write | Output file size (MB) | 0.709851 | 0.704911 |
| Whole run | Runtime range across repeat runs | 13.393–15.862 | 22.903–23.526 |
| Whole run | Highest memory use (MiB) | 42.797 | 76.672 |
| Whole run | Total CPU time (seconds) | 22.394922 | 40.567501 |
| Whole run | Average CPU cores used | 1.672 | 1.761 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

#### PARQUET input → PARQUET output

| Step | Work done | Rust elapsed seconds | Go elapsed seconds |
|---|---|---:|---:|
| Read | Read and decode portfolio Parquet batches. | 9.055367 | 10.362680 |
| Filter | Keep US portfolio holdings. | 0.102618 | 0.137314 |
| Prepare / group | Scale portfolio US weights to 100% and combine their weighted returns into one US group. | 0.061585 | 0.119112 |
| Calculate | Calculate the US group’s BF effects and contributions for each date. | 0.000734 | 0.000815 |
| Write | Save the PARQUET report to disk. | 0.010223 | 0.011689 |
| Setup, waiting, and remaining work | Everything outside the timed portfolio stages, including waiting for index data, setup and shutdown; calculated as whole run minus the five stages. | 5.714021 | 6.004804 |
| **Whole run** | **Sum of the six rows above.** | 14.944548 | 16.636414 |

Each column sums to whole-run elapsed time. All values in a column come from the same run: the middle whole-run time of three repetitions. The five timed stages follow the portfolio worker; index preparation runs concurrently and is not added again. The remaining-time row is a subtraction, not a separately measured index-reading or waiting timer. Other supporting metrics retain three-run medians.

[Latest country measurements](results/country-worker-stages/runs.csv) · [Output checks](results/country-worker-stages/checks.json)

<details>
<summary>Counts, throughput, and memory — Country, PARQUET</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.392 | 6.602 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 213.841 | 190.975 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 173,010,000 | 173,010,000 |
| Prepare / group | Rows prepared per second | 938,202,462.215 | 549,213,398.071 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 2,190 | 2,190 |
| Calculate | Daily BF effect values calculated | 6,570 | 6,570 |
| Calculate | Daily BF effect values per second | 8,955,699.955 | 7,878,434.674 |
| Calculate | Final BF effect values / calculation time | 8,955,699.955 | 7,878,434.674 |
| Calculate | Report rows | 2,190 | 2,190 |
| Write | Report rows written per second | 242,054.318 | 175,071.316 |
| Write | Output written per second (MiB) | 21.873 | 16.164 |
| Write | Report rows written | 2,190 | 2,190 |
| Write | Output file size (MB) | 0.207507 | 0.212026 |
| Whole run | Runtime range across repeat runs | 13.181–15.069 | 16.475–16.750 |
| Whole run | Highest memory use (MiB) | 105.812 | 205.578 |
| Whole run | Total CPU time (seconds) | 21.528375 | 33.832283 |
| Whole run | Average CPU cores used | 1.446 | 2.034 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

<details>
<summary>View the country flow diagram</summary>

[![Country — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/country.svg)](docs/flows/csv-parquet/country.svg)

</details>

### Weekly

#### CSV input → CSV output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 14.446 | 24.163 |

| Step | Work done |
|---|---|
| Read | Read and check CSV records. |
| Filter | Keep all holdings; total the weights on each side. |
| Prepare / group | Prepare weights and matching security IDs for each day. |
| Calculate | Add security results to portfolio totals, then combine them by week. |
| Write | Save the CSV report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Weekly, CSV</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.266 | 4.357 |
| Read | CSV bytes parsed per second (MiB) | 499.192 | 299.325 |
| Read | Stored file size / read time (MiB/sec) | 499.325 | 299.405 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 501,169,158.708 | 306,017,267.307 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,135,468,646.491 | 632,094,889.678 |
| Calculate | Final BF effect values / calculation time | 1,030.436 | 573.625 |
| Calculate | Report rows | 316 | 316 |
| Write | Report rows written per second | 53,987.793 | 49,377.785 |
| Write | Output written per second (MiB) | 15.102 | 13.548 |
| Write | Report rows written | 316 | 316 |
| Write | Output file size (MB) | 0.092687 | 0.090911 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 50.453 | 98.109 |
| Whole run | Total CPU time (seconds) | 25.424049 | 45.031998 |
| Whole run | Average CPU cores used | 1.760 | 1.864 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

#### PARQUET input → PARQUET output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 16.744 | 17.420 |

| Step | Work done |
|---|---|
| Read | Read and decode Parquet batches. |
| Filter | Keep all holdings; total the weights on each side. |
| Prepare / group | Prepare weights and matching security IDs for each day. |
| Calculate | Add security results to portfolio totals, then combine them by week. |
| Write | Save the PARQUET report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Weekly, PARQUET</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 6.613 | 6.335 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 191.290 | 183.255 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 500,822,762.898 | 304,107,812.302 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,139,463,684.899 | 627,686,733.846 |
| Calculate | Final BF effect values / calculation time | 1,034.061 | 569.625 |
| Calculate | Report rows | 316 | 316 |
| Write | Report rows written per second | 46,470.568 | 50,299.441 |
| Write | Output written per second (MiB) | 4.962 | 5.413 |
| Write | Report rows written | 316 | 316 |
| Write | Output file size (MB) | 0.035379 | 0.035660 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 126.500 | 223.547 |
| Whole run | Total CPU time (seconds) | 25.186351 | 37.510574 |
| Whole run | Average CPU cores used | 1.504 | 2.153 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

<details>
<summary>View the weekly flow diagram</summary>

[![Weekly — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/weekly.svg)](docs/flows/csv-parquet/weekly.svg)

</details>

### Monthly

#### CSV input → CSV output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 14.096 | 23.751 |

| Step | Work done |
|---|---|
| Read | Read and check CSV records. |
| Filter | Keep all holdings; total the weights on each side. |
| Prepare / group | Prepare weights and matching security IDs for each day. |
| Calculate | Add security results to portfolio totals, then combine them by month. |
| Write | Save the CSV report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Monthly, CSV</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.560 | 4.431 |
| Read | CSV bytes parsed per second (MiB) | 519.437 | 304.411 |
| Read | Stored file size / read time (MiB/sec) | 519.576 | 304.492 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 496,642,168.847 | 282,691,820.103 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,129,402,962.660 | 633,611,287.375 |
| Calculate | Final BF effect values / calculation time | 233.529 | 131.013 |
| Calculate | Report rows | 72 | 72 |
| Write | Report rows written per second | 11,765.592 | 13,264.855 |
| Write | Output written per second (MiB) | 3.318 | 3.671 |
| Write | Report rows written | 72 | 72 |
| Write | Output file size (MB) | 0.021288 | 0.020895 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 48.203 | 102.938 |
| Whole run | Total CPU time (seconds) | 24.406038 | 44.377285 |
| Whole run | Average CPU cores used | 1.731 | 1.868 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

#### PARQUET input → PARQUET output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 16.366 | 17.464 |

| Step | Work done |
|---|---|
| Read | Read and decode Parquet batches. |
| Filter | Keep all holdings; total the weights on each side. |
| Prepare / group | Prepare weights and matching security IDs for each day. |
| Calculate | Add security results to portfolio totals, then combine them by month. |
| Write | Save the PARQUET report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Monthly, PARQUET</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 6.766 | 6.313 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 195.724 | 182.634 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 175,200,000 | 175,200,000 |
| Prepare / group | Rows prepared per second | 501,889,950.831 | 288,340,258.094 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 175,200,000 | 175,200,000 |
| Calculate | Daily BF effect values calculated | 1,044,630,000 | 1,044,630,000 |
| Calculate | Daily BF effect values per second | 1,131,359,257.367 | 628,063,529.980 |
| Calculate | Final BF effect values / calculation time | 233.933 | 129.866 |
| Calculate | Report rows | 72 | 72 |
| Write | Report rows written per second | 13,494.409 | 8,615.401 |
| Write | Output written per second (MiB) | 2.301 | 1.463 |
| Write | Report rows written | 72 | 72 |
| Write | Output file size (MB) | 0.012874 | 0.012819 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 126.656 | 223.703 |
| Whole run | Total CPU time (seconds) | 24.672630 | 37.537981 |
| Whole run | Average CPU cores used | 1.508 | 2.149 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

<details>
<summary>View the monthly flow diagram</summary>

[![Monthly — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/monthly.svg)](docs/flows/csv-parquet/monthly.svg)

</details>

### Subset

#### CSV input → CSV output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 13.883 | 23.058 |

| Step | Work done |
|---|---|
| Read | Read and check CSV records. |
| Filter | Keep the 400 selected IDs on both sides. |
| Prepare / group | Scale selected weights to 100% on each side; line up selected IDs. |
| Calculate | Calculate selected-security results; combine index-only results into one summary. |
| Write | Save the CSV report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Subset, CSV</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 7.545 | 4.497 |
| Read | CSV bytes parsed per second (MiB) | 518.381 | 308.972 |
| Read | Stored file size / read time (MiB/sec) | 518.519 | 309.054 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 547,500 | 547,500 |
| Prepare / group | Rows prepared per second | 140,441,115.926 | 153,096,491.895 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 547,500 | 547,500 |
| Calculate | Daily BF effect values calculated | 2,628,000 | 2,628,000 |
| Calculate | Daily BF effect values per second | 888,726,263.856 | 341,044,645.184 |
| Calculate | Final BF effect values / calculation time | 610,745.674 | 234,370.863 |
| Calculate | Report rows | 602 | 602 |
| Write | Report rows written per second | 88,109.518 | 169,973.575 |
| Write | Output written per second (MiB) | 23.342 | 44.126 |
| Write | Report rows written | 602 | 602 |
| Write | Output file size (MB) | 0.167226 | 0.163874 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 40.359 | 76.344 |
| Whole run | Total CPU time (seconds) | 23.218193 | 40.652325 |
| Whole run | Average CPU cores used | 1.672 | 1.763 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

#### PARQUET input → PARQUET output

| Measurement | Rust elapsed seconds | Go elapsed seconds |
|---|---:|---:|
| Whole run | 15.618 | 16.787 |

| Step | Work done |
|---|---|
| Read | Read and decode Parquet batches. |
| Filter | Keep the 400 selected IDs on both sides. |
| Prepare / group | Scale selected weights to 100% on each side; line up selected IDs. |
| Calculate | Calculate selected-security results; combine index-only results into one summary. |
| Write | Save the PARQUET report to disk. |

Elapsed times for individual stages were not recorded. Accumulated worker measurements are in the expandable details.

<details>
<summary>Counts, throughput, and memory — Subset, PARQUET</summary>

Steps follow the timing table. Throughput rates use combined index and portfolio stage time; this denominator counts overlapping worker time.

| Step | Supporting metric | Rust | Go |
|---|---|---:|---:|
| Read | Million rows read per second | 6.982 | 6.420 |
| Read | CSV bytes parsed per second (MiB) | N/A (no parsed-byte counter) | N/A (no parsed-byte counter) |
| Read | Stored file size / read time (MiB/sec) | 201.990 | 185.719 |
| Filter | Rows checked | 175,200,000 | 175,200,000 |
| Filter | Rows kept | 547,500 | 547,500 |
| Prepare / group | Rows prepared per second | 192,415,964.304 | 112,471,612.473 |
| Prepare / group | Groups prepared across dates, counting portfolio and index separately | 547,500 | 547,500 |
| Calculate | Daily BF effect values calculated | 2,628,000 | 2,628,000 |
| Calculate | Daily BF effect values per second | 864,373,314.546 | 335,855,401.214 |
| Calculate | Final BF effect values / calculation time | 594,009.972 | 230,804.739 |
| Calculate | Report rows | 602 | 602 |
| Write | Report rows written per second | 94,059.399 | 101,032.020 |
| Write | Output written per second (MiB) | 5.706 | 6.239 |
| Write | Report rows written | 602 | 602 |
| Write | Output file size (MB) | 0.038292 | 0.038980 |
| Whole run | Runtime range across repeat runs | N/A (one run) | N/A (one run) |
| Whole run | Highest memory use (MiB) | 112.031 | 202.453 |
| Whole run | Total CPU time (seconds) | 22.498215 | 34.127748 |
| Whole run | Average CPU cores used | 1.441 | 2.033 |

MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Both engines report country medians and an observed runtime range from three runs; other scenarios use one run. Memory shows the highest observed peak.

</details>

<details>
<summary>View the subset flow diagram</summary>

[![Subset — CSV/Parquet and Rust/Go flow](docs/flows/csv-parquet/subset.svg)](docs/flows/csv-parquet/subset.svg)

</details>
