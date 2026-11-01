# Country runtime breakdown

One complete profiled release run per engine/format, executed sequentially with automatic CPU detection and normal filesystem caching. These are individual observations, not repeated-run medians. CSV input writes CSV; Parquet input writes Parquet. Both formats use the same effective-dated master; the prepared Parquet master is verified field-for-field against CSV. Holdings use the existing Parquet files through read-only symlinks.

## Whole-run elapsed phases (seconds)

**Each column below adds to its whole-run elapsed time.** A concurrent pipeline is one measured wall interval: its overlapping read/filter/aggregate stages cannot be separated into additive wall durations without an attribution convention. These measurements use no artificial allocation of shared wall time.

| Step | Rust PARQUET | Go PARQUET |
|---|---:|---:|
| Configuration, paths and output setup | 0.000226 | 0.000233 |
| Load, validate and index security master | 1.085545 | 1.423565 |
| Discover and plan whole-date input ranges | 0.004032 | 0.011990 |
| Concurrent range reading / filtering / aggregation | 6.796207 | 9.130459 |
| Streaming pipeline (overlapping index and portfolio stages) | 0.000000 | 0.000000 |
| Ordered daily BF computation, reconciliation and writing | 0.008015 | 0.008835 |
| Finalize reports, file sizes and metrics setup | 0.000205 | 0.000217 |
| Remaining pipeline coordination | 0.000107 | 0.000001 |
| **Whole run** | 7.894336 | 10.575299 |

## Read, filter, aggregate, compute and write (accumulated worker seconds)

**These rows do not add to whole-run elapsed time.** They sum timed intervals across concurrent workers and include time spent blocked or descheduled within each stage; they are not OS CPU seconds. Range planning is shown separately and subtracted from the read row here. Master loading is measured separately in the whole-run table.

| Step | Rust PARQUET | Go PARQUET |
|---|---:|---:|
| Range planning: CSV scanning / Parquet footer inspection | 0.004029 | 0.011982 |
| Read / parse / validate holdings and assemble daily rows | 52.526882 | 81.588201 |
| Effective-date lookup, US filtering and selected-weight sum | 25.385906 | 24.148151 |
| Normalize weights and aggregate country returns | 0.255178 | 0.422578 |
| BF effects, contributions and reconciliation | 0.000136 | 0.000243 |
| Format / encode report, flush, footer and sync | 0.007739 | 0.008460 |

## Parquet read detail (accumulated worker seconds)

These two rows are subsets of the holdings read stage above. Batch fetch combines file reads, decompression, decoding and Arrow construction; decompression is not isolated. Row preparation is the remaining read time, including typed access, ID lookup, validation and daily-buffer construction. CSV columns have zero Arrow batch work.

| Step | Rust PARQUET | Go PARQUET |
|---|---:|---:|
| Fetch / decompress / decode Arrow batches | 12.826591 | 40.293811 |
| Prepare daily data from decoded Arrow rows | 39.700291 | 41.294389 |

## Scope and resource checks

| Step | Rust PARQUET | Go PARQUET |
|---|---:|---:|
| Input dates | 1,095 | 1,095 |
| Holdings rows read | 175,200,000 | 175,200,000 |
| US rows retained | 173,010,000 | 173,010,000 |
| Output rows | 2,190 | 2,190 |
| Detected parallelism | 12 | 12 |
| Date-range workers (zero means streaming) | 12 | 12 |
| Date-range tasks | 24 | 24 |
| OS CPU seconds | 66.837671 | 95.275923 |
| Average CPU cores | 8.392073 | 8.976517 |
| Peak RSS (MiB) | 761.187500 | 1917.625000 |

Read timing includes numeric validation, instrument mapping and assembly; it does not isolate physical disk I/O. Filter timing includes effective-dated country selection. Write includes buffered report serialization and final flush/sync; final publication/rename appears under finalization. Input file sizes describe stored bytes, not measured disk traffic.

The internal whole-run timer starts in `run()` and ends at the metrics snapshot; process startup/exit and serialization of the metrics CSV itself are outside it. `process_wall_seconds` records the full child-process duration. Detailed read profiling is enabled for every run.

The date-range worker count identifies parallel versus streaming execution. CSV discovers byte boundaries; Parquet uses footer statistics to group complete dates. Dates spanning adjacent single-date row groups stay together. Missing statistics or mixed-date groups retain streaming execution. Both formats write daily results in input date order.

Artifacts: `runs.csv`, `runs.json`, `checks.json`, `environment.json`, and per-run `metrics.csv`, `usage.json`, and `run.log`. Each run is checked against the first run and the requested original baseline.
