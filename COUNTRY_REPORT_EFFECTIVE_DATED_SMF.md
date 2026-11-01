# Country attribution with an effective-dated SMF: optimized CSV and Parquet in Rust and Go

**Current report for the daily US-only country scenario, updated 2 November 2026.** This file contains the results, optimization history, timing definitions, calculation flow and evidence links. Per-run folders retain raw metrics and validation records, not separate narrative reports.

## Latest recorded optimized runtimes

| Input → output | Rust elapsed seconds | Go elapsed seconds | Output rows per engine |
|---|---:|---:|---:|
| CSV → CSV | 16.582 | 20.846 | 2,190 |
| PARQUET → PARQUET | 7.894 | 10.575 | 2,190 |

CSV values are the latest fully instrumented CSV range runs; Parquet values are the later row-group range runs. The formats were measured in separate batches. Each value is one complete release-build run with normal filesystem caching, not a repeated-run median or a controlled cold-cache measurement. All selected runs omit `--threads`; detected parallelism, worker counts and task counts are recorded below.

## Main optimization after adding the effective-dated SMF

**SMF means security master file.** This report measures the workload after adding `EffectiveDate` to `security_master.csv`: 36 monthly records per instrument, with the latest record effective on or before each holding date used for classification. The master is loaded once and shared read-only; SMF loading itself has not been parallelized.

**The largest demonstrated optimization in this workload is concurrent processing of independent complete-date ranges.** Merely allowing more CPU cores did not create more parallel work in the original index/portfolio pipeline. The date-range pool parallelizes holdings reading/decoding, effective-date country lookup, filtering and aggregation. Ordered collection and writing preserve daily calculation results.

The confirmed CSV range runs improved Rust by **1.70×** and Go by **2.43×** against their automatic-setting streaming baselines. The fully instrumented Parquet range runs improved Rust by **4.61×** and Go by **3.92×**. CSV confirmation timings and later instrumented timings are recorded separately below. These are combined pipeline changes, not isolated measurements of concurrency alone or of the cost of adding the SMF.

Parquet also benefits from metadata-only range planning; CSV still scans holdings to discover date boundaries. Concurrency uses more buffers and raises peak memory. These conclusions apply to the recorded workload; other possible optimizations have not all been evaluated.

## Dataset and calculation scope

- 1,095 dates: 1 January 2023 through 30 December 2025.
- One 60,000-security portfolio and a 100,000-security index per date: 175,200,000 total holdings rows.
- Keep 59,000 US portfolio holdings and 99,000 US index holdings per date; 173,010,000 rows remain after filtering.
- Effective-dated master: 160,000 instruments × 36 monthly versions = 5,760,000 records.
- Separately normalize retained weights on each side to one; group by date, entity and country; calculate base and local returns independently.
- Output: 2,190 rows per engine, with the same 20 fields. CSV input writes CSV; Parquet input writes typed Zstd Parquet.

The benchmark prepares a separate Parquet master from the updated CSV and verifies every typed field and row. Existing Parquet holdings are linked without modification. The original `attribution_data_parquet` master remains the older undated source; it is not the master used for these comparable measurements.

## Whole-run elapsed phases, in seconds

**Each column adds to its whole-run runtime, subject to displayed rounding.** Concurrent reading, filtering and aggregation form one measured wall interval. Their overlapping worker stages cannot be split into additive wall durations without an attribution convention. No such allocation is estimated here.

| Step | Rust CSV → CSV | Go CSV → CSV | Rust Parquet → Parquet | Go Parquet → Parquet |
|---|---:|---:|---:|---:|
| Configuration, paths and output setup | 0.000230 | 0.000208 | 0.000226 | 0.000233 |
| Load, validate and index security master | 0.637504 | 1.243391 | 1.085545 | 1.423565 |
| Discover and plan complete-date input ranges | 8.624785 | 9.298910 | 0.004032 | 0.011990 |
| Concurrent range reading, filtering and aggregation | 7.304019 | 10.296463 | 6.796207 | 9.130459 |
| Streaming pipeline, when the range path is unavailable | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| Ordered daily BF computation, reconciliation and writing | 0.014891 | 0.006665 | 0.008015 | 0.008835 |
| Finalize reports, sizes and metrics setup | 0.000195 | 0.000218 | 0.000205 | 0.000217 |
| Remaining pipeline coordination | 0.000092 | 0.000001 | 0.000107 | 0.000001 |
| **Whole run** | 16.581717 | 20.845857 | 7.894336 | 10.575299 |

## Read, filter, aggregate, compute and write, in worker seconds

**These rows do not add to whole-run runtime.** They accumulate elapsed intervals across workers, including descheduling or blocking inside a stage; they are not OS CPU seconds. Master loading is separate. Range-planning time is shown separately and subtracted from the holdings-read row.

| Step | Rust CSV → CSV | Go CSV → CSV | Rust Parquet → Parquet | Go Parquet → Parquet |
|---|---:|---:|---:|---:|
| Range planning: CSV scan or Parquet footer inspection | 8.624758 | 9.298808 | 0.004029 | 0.011982 |
| Read, parse, validate holdings and assemble daily rows | 64.742497 | 98.079565 | 52.526882 | 81.588201 |
| Effective-date lookup, US filtering and selected-weight sum | 19.127010 | 21.674480 | 25.385906 | 24.148151 |
| Normalize weights and aggregate country contributions | 0.219081 | 0.646402 | 0.255178 | 0.422578 |
| BF effects, contributions and reconciliation | 0.000163 | 0.000363 | 0.000136 | 0.000243 |
| Format/encode reports, flush, footer and sync | 0.014546 | 0.006161 | 0.007739 | 0.008460 |

### Parquet holdings-read detail

The two Parquet rows below are subsets of the holdings-read stage above. Batch fetch combines file reading, decompression, decoding and Arrow array creation; decompression is not isolated. Daily preparation includes typed access, date reuse, ID lookup, validation and daily-buffer construction. CSV has no Arrow batch work.

| Step | Rust CSV → CSV | Go CSV → CSV | Rust Parquet → Parquet | Go Parquet → Parquet |
|---|---:|---:|---:|---:|
| Fetch, decompress and decode Arrow batches | 0.000000 | 0.000000 | 12.826591 | 40.293811 |
| Prepare daily rows from decoded Arrow data | 0.000000 | 0.000000 | 39.700291 | 41.294389 |

## CPU use and memory

| Step | Rust CSV → CSV | Go CSV → CSV | Rust Parquet → Parquet | Go Parquet → Parquet |
|---|---:|---:|---:|---:|
| Available parallelism | 12 | 12 | 12 | 12 |
| Range workers | 12 | 12 | 12 | 12 |
| Complete-date tasks | 24 | 24 | 24 | 24 |
| Process CPU seconds | 80.838809 | 114.557222 | 66.837671 | 95.275923 |
| Average CPU cores used | 4.849471 | 5.481978 | 8.392073 | 8.976517 |
| Peak process RSS, MiB | 542.781250 | 840.562500 | 761.187500 | 1917.625000 |

CPU time and peak RSS come from child-process `wait4` counters. Average cores means CPU time divided by full process elapsed time. Peak RSS is whole-process memory, not per-worker memory. Parallel Parquet decoding increases concurrent Arrow buffers; its current peaks are about 761 MiB in Rust and 1,918 MiB in Go.

## Optimization history and baseline comparisons

All measurements in this section use the effective-dated master. They are separate individual runs; differences between confirmation and later instrumented runs are visible rather than presented as medians.

| Measurement | I/O | Rust seconds | Go seconds | Evidence |
|---|---|---:|---:|---|
| Streaming with explicit four-thread setting | CSV | 25.863 | 50.761 | [Recorded metrics and checks](<results/country-effective-dates-2026-11-02/checks.json>) |
| Streaming with automatic parallelism | CSV | 26.571 | 50.102 | [Recorded metrics and checks](<results/country-auto-2026-11-02/checks.json>) |
| First complete-date parallel run | CSV | 14.342 | 20.248 | [Recorded metrics and checks](<results/country-parallel-v2-2026-11-02/checks.json>) |
| Parallel binary confirmation | CSV | 15.638 | 20.654 | [Recorded metrics and checks](<results/country-parallel-v2-2026-11-02/confirmation/checks.json>) |
| Instrumented streaming baseline | Parquet | 36.384 | 41.501 | [Recorded metrics and checks](<results/country-runtime-2026-11-02/checks.json>) |

### Parquet row-group optimization

| Engine | Streaming baseline | Latest parallel run | Speedup | Runtime reduction | Peak RSS before → after |
|---|---:|---:|---:|---:|---:|
| Rust | 36.384 s | 7.894 s | 4.61× | 78.3% | 503.6 → 761.2 MiB |
| Go | 41.501 s | 10.575 s | 3.92× | 74.5% | 701.2 → 1917.6 MiB |

The earlier CSV stage used one index producer and one portfolio worker; raising the thread limit alone did not split either reader. Complete-date processing exposed more independent work. Parquet initially retained that streaming path; row-group processing then removed that concurrency limitation. These comparisons measure the implemented pipelines, not an intrinsic universal format or language ranking.

## Implemented processing flow

[Open the end-to-end SVG: physical reads, effective-date filtering, grouping, BF formulas, ordered merging and output](docs/flows/country-parallel-end-to-end.svg).

1. Load and validate the master once; sort versions by effective date and share the immutable index.
2. CSV scans physical records to discover whole-date byte ranges. Parquet reads footer statistics to establish complete-date group ranges; adjacent groups for the same date stay together.
3. A bounded CPU-sized pool opens an independent reader per task. Each worker buffers one logical date; Parquet decodes projected Arrow batches of at most 65,536 rows. No row group is assigned to two tasks.
4. Validate every holding, then resolve the latest master version effective on or before its date. Keep `Country == US` and sum retained original weights independently for portfolio and benchmark.
5. Divide retained weights and weighted returns by that side’s selected weight. Aggregate daily country weights and contributions, preserving instrument summation order.
6. Collect small daily aggregates into their original positions. Check range exhaustion, entity consistency and portfolio/index date alignment. This merge reorders results; it does not average or sum different dates.
7. After all ranges succeed, calculate and reconcile each date in order, then append base/local output rows. Flush buffers, finish Parquet footers, sync/close partial files and publish completed reports.

### Benchmark return used in allocation and selection

For one date and one return type, `W` is normalized country weight and `C` is weighted country contribution. Group returns are `rp,g = Cp,g / Wp,g` and `rb,g = Cb,g / Wb,g`; total returns are `RP = Σg Cp,g` and `RB = Σg Cb,g`.

```text
Allocation_g  = (Wp,g − Wb,g) × (rb,g − RB)
Selection_g   = Wb,g × (rp,g − rb,g)
Interaction_g = (Wp,g − Wb,g) × (rp,g − rb,g)
Total_g       = Allocation_g + Selection_g + Interaction_g
```

**Allocation uses total benchmark return `RB` after US filtering and renormalization, not the original full-index return. Selection uses benchmark group return `rb,g`, not `RB`.** With one retained US group, both group weights are approximately one and `RB ≈ rb,US`; allocation and interaction are approximately zero, while selection carries `RP − RB`. Contributions are `Cp,g`, `Cb,g` and their difference. The country period is one day, so returns are not linked across dates.

### Fallbacks and measurement limits

- `--threads 1` retains streaming execution. Multiline CSV, missing Parquet statistics, empty groups or mixed-date row groups also use streaming for the entire pipeline. Those layouts remain correct but are not parallelized here.
- `--max-days` selects complete dates, including every row group belonging to the last selected date.
- `boundary_scan_seconds` includes CSV scanning or Parquet footer inspection and is included in `read_parse_seconds`. For Parquet, batch decode plus daily preparation equals holdings read time minus planning time.
- Read stages include lookup and validation rather than isolated disk I/O. Stored file sizes and parsed byte counters do not measure physical disk traffic.
- Whole-run timing starts in native `run()` and ends at the metrics snapshot. Process startup/exit and metrics CSV serialization fall outside that timer; `process_wall_seconds` includes the entire child process.
- Concurrent buffers increase memory needs; `--threads N` remains an optional concurrency override.

## Correctness checks

All four selected results cover every input date and holding and reproduce the same 2,190 report rows. Every one of the 26,280 numeric values matches exactly across engines, formats and the original streaming outputs. The Parquet checks compare against the preceding verified results, which already match CSV; report keys and order also match.

- 156 exact Parquet serial/parallel comparisons cover split-date groups, typed/string dates, projected/reordered schemas, mixed I/O, multiple portfolios, maximum-date limits, layout fallback and six invalid fixtures.
- 78 exact CSV comparisons cover effective-date transitions, leading blank lines, CRLF, missing final newline, quoted records, multiline fallback, worker counts 2/4/12, limits and ten invalid fixtures.
- The independent attribution oracle and CSV/Parquet compatibility checks pass across nine scenario/period cases.
- `cargo test --locked` and `go test -race ./cmd/attribution` pass, including native parallel Parquet execution.
- Daily attribution and contribution reconciliation remain enforced; reports are finalized only after successful processing.

## Recorded evidence for the selected results

| Selected run | Native metrics | Process resources | Output comparisons |
|---|---|---|---|
| Rust CSV | [Metrics CSV](<results/country-runtime-2026-11-02/csv/rust/metrics.csv>) | [Usage JSON](<results/country-runtime-2026-11-02/csv/rust/usage.json>) | [Checks JSON](<results/country-runtime-2026-11-02/checks.json>) |
| Go CSV | [Metrics CSV](<results/country-runtime-2026-11-02/csv/go/metrics.csv>) | [Usage JSON](<results/country-runtime-2026-11-02/csv/go/usage.json>) | [Checks JSON](<results/country-runtime-2026-11-02/checks.json>) |
| Rust PARQUET | [Metrics CSV](<results/country-parquet-parallel-2026-11-02/parquet/rust/metrics.csv>) | [Usage JSON](<results/country-parquet-parallel-2026-11-02/parquet/rust/usage.json>) | [Checks JSON](<results/country-parquet-parallel-2026-11-02/checks.json>) |
| Go PARQUET | [Metrics CSV](<results/country-parquet-parallel-2026-11-02/parquet/go/metrics.csv>) | [Usage JSON](<results/country-parquet-parallel-2026-11-02/parquet/go/usage.json>) | [Checks JSON](<results/country-parquet-parallel-2026-11-02/checks.json>) |

## Reproduce or refresh this report

```sh
cargo build --release --manifest-path rust/Cargo.toml
go -C go build -trimpath -ldflags='-s -w' -o ../benchmark-go-attribution ./cmd/attribution
.venv/bin/python python/benchmark_country_runtime.py --output-dir results/new-country-run
```

Choose a new raw-results directory. The benchmark records metrics/usage/checks, then updates **this existing `COUNTRY_REPORT_EFFECTIVE_DATED_SMF.md` only after both engines complete and validate for every requested format**. A Parquet-only run (`--formats parquet`) replaces the two Parquet selections and retains the recorded CSV selections. It does not generate another Markdown report.

To rebuild the report from its selected evidence without rerunning benchmarks:

```sh
python3 python/render_country_report.py
```

## Archived experiments with different scope

Older undated-master and date-cache experiments are historical and are not the optimization baselines above. Their timings, four-thread settings and memory results should not be mixed with the current effective-dated dataset.

- [Historical five-scenario CSV/Parquet benchmark](CSV_PARQUET_REPORT.md).
- [Archived Go date-cache, Arrow and memory profiling](GO_DATE_READ_REPORT.md).
- [Archived Rust date-cache and Parquet reader profiling](RUST_DATE_READ_REPORT.md).
- [Earlier country worker-stage evidence](results/country-worker-stages/runs.csv).
- [Contribution calculation definitions](docs/CONTRIBUTIONS.md).

<!-- country-report-sources: {"csv/go": "results/country-runtime-2026-11-02/csv/go/metrics.csv", "csv/rust": "results/country-runtime-2026-11-02/csv/rust/metrics.csv", "parquet/go": "results/country-parquet-parallel-2026-11-02/parquet/go/metrics.csv", "parquet/rust": "results/country-parquet-parallel-2026-11-02/parquet/rust/metrics.csv"} -->
