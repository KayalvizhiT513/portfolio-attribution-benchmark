# Parallel Parquet country attribution

## Baseline recorded on 2026-11-02

The preceding fully instrumented run used the effective-dated master with
5,760,000 records, 175,200,000 holdings rows and 1,095 dates. Native runtime
parallelism was automatic (12 detected). Parquet input wrote Parquet output.

| Engine | Streaming whole-run time | Average CPU cores | Peak RSS |
|---|---:|---:|---:|
| Rust | 36.383944 s | 1.58 | 503.6 MiB |
| Go | 41.501054 s | 2.32 | 701.2 MiB |

Baseline evidence: `results/country-runtime-2026-11-02/parquet/`.

## Implementation

[End-to-end SVG flowchart: physical reads, filtering, grouping, BF formulas and output](docs/flows/country-parallel-end-to-end.svg).

Both country engines now share their bounded date-range worker pool between CSV
and Parquet. Each task owns an independent input reader; the effective-dated
security master remains immutable and shared.

For Parquet, the planner reads footer statistics for `EffectiveDate`. A row group
is eligible when its minimum and maximum establish a single valid date. Adjacent
row groups with the same date are coalesced so an entire date stays in one task.
The current input files each have 1,095 row groups, one per date. Groups are
partitioned among the runtime-sized worker pool and each reader decodes only its
assigned groups, without rereading any group in another task. Instrument order
within a date is unchanged. The existing daily weight, duplicate, numeric and
reconciliation checks still run.

Worker results are collected into their original input positions. Before output,
the engine checks date alignment between index and portfolios, entity consistency
across task boundaries, and exhaustion of every selected range. Daily BF effects,
contributions, and reports are then computed/written in date order. Output format
continues to follow portfolio input format; mixed CSV/Parquet holdings are supported.

`--max-days` selects complete dates, including every row group for the last date.
`--threads 1` retains the streaming reference path. Missing statistics, empty groups,
and groups containing multiple dates retain streaming execution for the entire
pipeline; these layouts remain correct but are not parallelized by this change.
Statistics guide planning, while decoded rows are still validated against the plan.

The metric `boundary_scan_seconds` is retained for compatibility and now measures
range discovery for either format: a full CSV boundary scan or Parquet footer
inspection. It is included in `read_parse_seconds`. Parquet batch decode plus row
preparation therefore equals holdings read time *minus* this planning time.
Whole-run phase timers remain additive; accumulated worker stage durations overlap.

## Validation

- Native Go tests exercise dates spanning adjacent row groups, full range coverage,
  maximum-date limits, and entity changes across workers, including race detection.
- Python parity checks compare serial and parallel paths at 2/4/12 workers, typed
  and string dates, nested extra columns and reordered schemas, multiple portfolios,
  mixed CSV/Parquet I/O, effective-date country changes, maximum-date limits,
  missing statistics and mixed-date-group fallback.
- Invalid fixtures check duplicate instruments across groups, entity changes,
  unordered dates, null numeric fields, unknown instruments and invalid master
  coverage. No final report is published on failure.

Full-dataset measurements and exact baseline comparisons follow.

## Full-dataset measurement

Release binaries, automatic parallelism (12 detected), instrumented read timers,
normal filesystem caching, and engines run sequentially. Both baseline and new
runs use the verified effective-dated Parquet master and the same holdings files.
These are individual measurements, not repeated-run medians.

| Engine | Streaming baseline | Parallel ranges | Speedup | Runtime reduction | Peak RSS before → after | Average CPU cores before → after |
|---|---:|---:|---:|---:|---:|---:|
| Rust | 36.384 s | 7.894 s | 4.61× | 78.3% | 503.6 → 761.2 MiB | 1.58 → 8.39 |
| Go | 41.501 s | 10.575 s | 3.92× | 74.5% | 701.2 → 1917.6 MiB | 2.32 → 8.98 |

All 1,095 dates and 175,200,000 input rows were processed. The engines retain
173,010,000 US rows and write 2,190 result rows. Every one of the 26,280 numeric
output values matches each engine's prior streaming output and the other engine
exactly; report keys and date ordering also match.

Footer planning takes about 0.004 s in Rust and 0.012 s in Go. Concurrent range
processing takes about 6.796 s and 9.130 s, respectively; the full additive phase
and accumulated stage tables are in the [runtime report](results/country-parquet-parallel-2026-11-02/REPORT.md).

The speedup uses more concurrent Arrow decoding buffers. Peak RSS increases from
about 504 to 761 MiB in Rust and from 701 to 1,918 MiB in Go. These are whole-process
peaks, not per-worker values. The CPU-sized pool remains bounded; an explicit
`--threads` override can reduce concurrency when memory is constrained.

Final checks passed: 156 exact Parquet serial/parallel comparisons, 78 CSV
comparisons, the independent nine-case attribution oracle, CSV/Parquet/mixed-format
compatibility checks, native Rust tests, and native Go race detection including
parallel Parquet execution.

Evidence: `results/country-parquet-parallel-2026-11-02/summary.json`, `checks.json`,
`runs.csv`, and per-engine `metrics.csv` and `usage.json`. The prepared inputs and
generated attribution files remain local; the original source inputs are unchanged.
