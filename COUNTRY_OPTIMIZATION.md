# Country attribution optimization

This records the initial CSV optimization. Subsequent Parquet range processing
and measurements are documented in [the Parquet report](PARQUET_PARALLEL_REPORT.md).

## Baseline: 2026-11-02

Release builds, CSV input in `attribution_data`, effective-dated security master,
1,095 dates, 175,200,000 holdings rows, 173,010,000 retained US rows, and 2,190
output rows per engine. Runs executed sequentially with normal filesystem caching.
These are individual measurements, not repeated-run medians.

| Engine | Explicit four-thread setting | Automatic setting (12 detected) |
|---|---:|---:|
| Rust | 25.862961042 s | 26.571013834 s |
| Go | 50.761098791 s | 50.101715459 s |

All numeric outputs match exactly across engines and settings.

Baseline artifacts:
- `results/country-effective-dates-2026-11-02/checks.json`
- `results/country-auto-2026-11-02/checks.json`

## Initial CSV implementation

For daily country attribution on single-line CSV holdings, discover complete date
boundaries and dispatch independent byte ranges to a bounded pool sized from
runtime CPU availability. Keep every date intact so instrument order, floating
point accumulation, weight checks, and duplicate checks retain their existing
semantics. Share the immutable effective-dated master across workers. Collect
small daily country aggregates and reconcile/write portfolio and index results
in their original date order. Keep the existing stream path for Parquet, multiline
CSV, and other scenarios. Include boundary discovery and ordered writing in
whole-run runtime; stage timings sum worker durations and may exceed wall time.

## First optimized measurement

Release builds, the same CSV dataset and automatic CPU detection, with engines
run sequentially and no `--threads` override. Boundary discovery and final
ordered writing are included in the internal whole-run timer.

| Engine | Baseline | Optimized | Speedup | Runtime reduction | Peak RSS | Average CPU cores |
|---|---:|---:|---:|---:|---:|---:|
| Rust | 26.571 s | 14.342 s | 1.85× | 46.0% | 538.8 MiB | 4.63 |
| Go | 50.102 s | 20.248 s | 2.47× | 59.6% | 822.5 MiB | 5.56 |

Both engines use 12 range workers and 24 whole-date tasks across index and
portfolio. They process 175,200,000 rows, retain 173,010,000 US rows, and produce
2,190 rows covering all 1,095 dates. All 26,280 numeric values match exactly
between engines and each engine's original automatic-setting baseline.

Resource measurements use child-process CPU time and peak RSS from `wait4`;
average cores is CPU time divided by process elapsed time. This is whole-process
peak RSS, not additional memory over baseline. The baseline did not collect
resource counters. More parallel CSV buffers increase memory needs.

Rust boundary discovery: 7.713 s (included in read and whole-run time).

Go boundary discovery: 8.923 s (included in read and whole-run time).

These are individual measurements with normal filesystem caching. They show
the result of this change on this dataset, not a general scaling guarantee.
Range discovery adds a full read pass over the selected CSV dates.
`parsed_input_bytes` counts parser input once; it excludes the discovery pass
and does not measure physical disk traffic. Multiline
records and Parquet retain streaming execution; no Parquet speedup is claimed.
Daily aggregates are retained until all ranges succeed and are then written in
input date order. Holdings buffers are limited to one day per active worker.
Only complete successful reports are renamed from `.partial` to final output.

Artifacts: `results/country-parallel-v2-2026-11-02/checks.json`, per-engine
`metrics.csv`, `usage.json`, and `run.log`.

Validation:
- `python3 python/test_country_parallel.py`: 78 exact output comparisons, including
  effective-date transitions, leading blank lines, CRLF, missing final newline, quoted fields and
  multiline fallback, multiple portfolios, worker counts 2/4/12, and `--max-days`.
  Serial and parallel paths reject the same ten malformed-input fixtures.
- `python3 python/test_attribution.py`: independent oracle across nine
  scenario/period combinations and invalid-input checks.
- `go test -race ./cmd/attribution`: passes, including range-worker execution.
- `cargo test --locked`: passes.
- `.venv/bin/python python/test_parquet_input.py`: CSV/Parquet/mixed-format
  parity across nine scenario/period cases, subset formats, maximum-date limits,
  column projection, more than 65,000 daily holdings, and invalid-input checks pass.

## Final binary confirmation

After the final boundary guards and mixed-format selection checks, both release
binaries were rebuilt and measured sequentially without `--threads`. These
measurements include the additional discovery pass; the saved usage files
record the command and SHA-256 of each binary. Baseline remains the original
single automatic-setting run, so no repeated-run median is claimed.

| Engine | Original baseline | Final optimized binary | Speedup | Runtime reduction | Peak RSS |
|---|---:|---:|---:|---:|---:|
| Rust | 26.571 s | 15.638 s | 1.70× | 41.1% | 542.4 MiB |
| Go | 50.102 s | 20.654 s | 2.43× | 58.8% | 815.0 MiB |

Both final runs use 12 workers and 24 whole-date tasks, process every input date
and holding, and reproduce the baseline and each other with zero numeric
difference across all 26,280 values. Final confirmation artifacts:
`results/country-parallel-v2-2026-11-02/confirmation/checks.json`.
