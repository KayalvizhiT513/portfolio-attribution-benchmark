# Country runtime breakdown

One complete profiled release run per engine/format, executed sequentially with automatic CPU detection and normal filesystem caching. These are individual observations, not repeated-run medians. CSV input writes CSV; Parquet input writes Parquet. Both formats use the same effective-dated master; the prepared Parquet master is verified field-for-field against CSV. Holdings use the existing Parquet files through read-only symlinks.

## Whole-run elapsed phases (seconds)

**Each column below adds to its whole-run elapsed time.** A concurrent pipeline is one measured wall interval: its overlapping read/filter/aggregate stages cannot be separated into additive wall durations without an attribution convention. These measurements use no artificial allocation of shared wall time.

| Step | Rust CSV | Go CSV | Rust PARQUET | Go PARQUET |
|---|---:|---:|---:|---:|
| Configuration, paths and output setup | 0.000230 | 0.000208 | 0.000383 | 0.000210 |
| Load, validate and index security master | 0.637504 | 1.243391 | 1.070481 | 1.424447 |
| Discover and plan whole-date CSV ranges | 8.624785 | 9.298910 | 0.000000 | 0.000000 |
| Concurrent range reading / filtering / aggregation | 7.304019 | 10.296463 | 0.000000 | 0.000000 |
| Streaming pipeline (overlapping index and portfolio stages) | 0.000000 | 0.000000 | 35.312858 | 40.076122 |
| Ordered daily BF computation, reconciliation and writing | 0.014891 | 0.006665 | 0.000000 | 0.000000 |
| Finalize reports, file sizes and metrics setup | 0.000195 | 0.000218 | 0.000222 | 0.000274 |
| Remaining pipeline coordination | 0.000092 | 0.000001 | 0.000000 | 0.000000 |
| **Whole run** | 16.581717 | 20.845857 | 36.383944 | 41.501054 |

## Read, filter, aggregate, compute and write (accumulated worker seconds)

**These rows do not add to whole-run elapsed time.** They sum timed intervals across concurrent workers and include time spent blocked or descheduled within each stage; they are not OS CPU seconds. Boundary discovery is shown separately and subtracted from the read row here. Master loading is measured separately in the whole-run table.

| Step | Rust CSV | Go CSV | Rust PARQUET | Go PARQUET |
|---|---:|---:|---:|---:|
| CSV boundary scanning | 8.624758 | 9.298808 | 0.000000 | 0.000000 |
| Read / parse / validate holdings and assemble daily rows | 64.742497 | 98.079565 | 43.318323 | 49.511679 |
| Effective-date lookup, US filtering and selected-weight sum | 19.127010 | 21.674480 | 13.029025 | 15.569154 |
| Normalize weights and aggregate country returns | 0.219081 | 0.646402 | 0.160336 | 0.488930 |
| BF effects, contributions and reconciliation | 0.000163 | 0.000363 | 0.000999 | 0.000822 |
| Format / encode report, flush, footer and sync | 0.014546 | 0.006161 | 0.010680 | 0.015131 |

## Parquet read detail (accumulated worker seconds)

These two rows are subsets of the holdings read stage above. Batch fetch combines file reads, decompression, decoding and Arrow construction; decompression is not isolated. Row preparation is the remaining read time, including typed access, ID lookup, validation and daily-buffer construction. CSV columns have zero Arrow batch work.

| Step | Rust CSV | Go CSV | Rust PARQUET | Go PARQUET |
|---|---:|---:|---:|---:|
| Fetch / decompress / decode Arrow batches | 0.000000 | 0.000000 | 7.342352 | 15.894374 |
| Prepare daily data from decoded Arrow rows | 0.000000 | 0.000000 | 35.975971 | 33.617305 |

## Scope and resource checks

| Step | Rust CSV | Go CSV | Rust PARQUET | Go PARQUET |
|---|---:|---:|---:|---:|
| Input dates | 1,095 | 1,095 | 1,095 | 1,095 |
| Holdings rows read | 175,200,000 | 175,200,000 | 175,200,000 | 175,200,000 |
| US rows retained | 173,010,000 | 173,010,000 | 173,010,000 | 173,010,000 |
| Output rows | 2,190 | 2,190 | 2,190 | 2,190 |
| Detected parallelism | 12 | 12 | 12 | 12 |
| Date-range workers (zero means streaming) | 12 | 12 | 0 | 0 |
| Date-range tasks | 24 | 24 | 0 | 0 |
| OS CPU seconds | 80.838809 | 114.557222 | 57.588003 | 96.375140 |
| Average CPU cores | 4.849471 | 5.481978 | 1.579622 | 2.321137 |
| Peak RSS (MiB) | 542.781250 | 840.562500 | 503.578125 | 701.234375 |

Read timing includes numeric validation, instrument mapping and assembly; it does not isolate physical disk I/O. Filter timing includes effective-dated country selection. Write includes buffered report serialization and final flush/sync; final publication/rename appears under finalization. Input file sizes describe stored bytes, not measured disk traffic.

The internal whole-run timer starts in `run()` and ends at the metrics snapshot; process startup/exit and serialization of the metrics CSV itself are outside it. `process_wall_seconds` records the full child-process duration. Detailed read profiling is enabled for all four runs.

CSV uses the new date-range pool. Parquet retains the current streaming index/portfolio pipeline; this is a comparison of the current implementations, not an isolated format-only benchmark.

Artifacts: `runs.csv`, `runs.json`, `checks.json`, `environment.json`, and per-run `metrics.csv`, `usage.json`, and `run.log`. Each run is checked against the first run and the requested original baseline.
