# Archived Rust profiling: date caching and Parquet reads

Archived experiment with a different dataset or execution path. See the [current country attribution performance report](COUNTRY_REPORT.md) for optimized CSV and Parquet results.

Completed 14/14 full-history runs. Only the Rust input date adapter changed; calculation, native output, batch size and worker settings are unchanged.

## Fresh CSV vs Parquet

Times below are medians. Country has three runs per format; other scenarios have one. Each run processes all 1,095 dates and 175,200,000 return records.

| Scenario | Runs per format | CSV read s¹ | Parquet read s¹ | CSV whole run s | Parquet whole run s |
|---|---:|---:|---:|---:|---:|
| Security | 1 | 23.462 | 26.388 | 14.222 | 16.773 |
| Country | 3 | 21.739 | 23.481 | 13.067 | 14.776 |
| Weekly | 1 | 24.113 | 26.495 | 14.446 | 16.744 |
| Monthly | 1 | 23.173 | 25.895 | 14.096 | 16.366 |
| Subset | 1 | 23.221 | 25.091 | 13.883 | 15.618 |

¹ Read time adds time from the index and portfolio workers. The workers can overlap, so read time can exceed whole-run time.

## Country: check the remaining slowdown

Country pairs alternate CSV→Parquet, Parquet→CSV, CSV→Parquet. All runs use four configured threads and normal filesystem caching. The trigger for adding deeper timers is Parquet median read time or whole-run time at least 10% above CSV after three pairs. This is a practical threshold, not a statistical significance test.

Parquet/CSV ratio: read **1.080×**; whole run **1.131×**. Below 1 means Parquet is faster.

**Follow-up:** The slowdown remains above the trigger; separate decoding and adapter measurements are shown below.

Whole-run ranges: CSV 12.982–13.283s; Parquet 12.868–15.090s.

## What changed

Before: every Parquet return record converted its date32 value into a new date string. Now: compare the integer date and keep the formatted string until the date changes. The cache survives batch boundaries. CSV and Parquet string-date handling are unchanged.

For these date-sorted files, date formatting falls from roughly 175.2 million calls to 2,190 per run: 1,095 dates in each of two return streams. This count follows from the input layout and cache logic; the unit test verifies cache reuse.

Output checks passed: **35**. Every output field is checked against the original Rust result and the unchanged Go result; fresh CSV and Parquet outputs are also compared.

[Raw measurements](results/rust-date-cache/runs.csv) · [Checks](results/rust-date-cache/checks.json) · [Settings and binary hash](results/rust-date-cache/environment.json) · [Original baseline](CSV_PARQUET_REPORT.md)

## Where the remaining Parquet read time goes

Profiling runs completed: 7/7. Profiling output comparisons passed: 14/14.

Optional timers are enabled only with `ATTRIBUTION_PROFILE_READ=1`. One clock surrounds each batch fetch; the rest of the read stage covers typed Arrow access, date caching, ID lookup, validation, and building daily buffers. No clocks are added to individual rows.

**Batch read/decode includes file I/O, decompression, decoding and Arrow batch creation. It does not isolate decompression from disk I/O.** The daily-buffer figure is the read-stage time minus batch time: it includes loop overhead, typed access, lookup/validation and copying values. Both columns are combined worker time, not process wall time.

| Scenario | Batch read + decode s | Arrow → daily buffers s | Total read s |
|---|---:|---:|---:|
| Security | 7.077 | 16.147 | 23.224 |
| Country | 6.859 | 14.973 | 21.832 |
| Weekly | 7.014 | 15.710 | 22.724 |
| Monthly | 7.064 | 15.414 | 22.478 |
| Subset | 7.008 | 14.641 | 21.649 |

Country uses the profiling run with the middle read time out of three; the other scenarios use one run. Each row’s components come from the same run and sum to its read time. Profiled times are kept separate from the uninstrumented comparison above.

[Profile measurements](results/rust-date-cache/profile/runs.csv) · [Profile checks](results/rust-date-cache/profile/checks.json)

In the representative country run, about **69%** of reader time is in row handling and daily-buffer preparation, including lookup/validation. The remaining time is batch reading/decoding. This identifies where to investigate next; it does not separately measure hashing, validation or buffer copies.
