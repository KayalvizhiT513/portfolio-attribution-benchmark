# Attribution: Rust and Go

All five scenarios process all **1,095 effective dates**, all four portfolios, and separate base/local returns. Calculation granularity is independent of report granularity. The report contains one section per scenario, comparing Rust and Go.

Each language/scenario has **2 fresh complete repetitions**. Stage times, OS CPU, peak RSS and process wall times are medians; overall ranges are also shown. All displayed measurements use the compact reporting contracts defined below.

## Output contract and benchmark-only reconciliation

Calculation and output granularity are independent. Every scenario reads daily inputs, validates and normalizes them, and calculates the required daily effects. Only the requested linked summaries are emitted.

| Scenario | Calculation state | Final output contract | Rows for this dataset |
|---|---|---|---:|
| Security | Daily full security union; full-history linked accumulator per instrument | Portfolio-held IDs plus explicit benchmark-only reconciliation bucket | 120,000 + 8 |
| Standalone subset | Independently normalized selected sides; compact selected-ID slots; full-history linking | Selected portfolio-held IDs plus benchmark-only reconciliation bucket | 800 + 8 |
| Country | Daily weighted country grouping; full-history linking | One country/portfolio/return-type row | 8 |
| Weekly | Sum daily security effects to portfolio components; link within Monday–Sunday buckets | One portfolio/week/return-type row | 1,264 |
| Monthly | Sum daily security effects to portfolio components; link within calendar months | One portfolio/month/return-type row | 288 |

All counts include separate base/local rows. Security/subset counts include eight reconciliation rows: one per portfolio and return type. No duplicate total rows are appended. All five use the complete 1,095-date history.

Portfolio-held is presence-based: the ID appears in a portfolio holding record on at least one date in the reporting range. A benchmark-only bucket covers instruments never held by that portfolio in that range. It is part of the mathematical report, not an optional discarded residual. It preserves benchmark weights and linked effects that would otherwise be lost by suppressing benchmark-only detail. Security/subset detail plus this bucket reconcile to complete active return.

## Overall results and output sizes

| Scenario | Rust seconds | Go seconds | Report rows/language | Rust output MB | Go output MB | Combined MB |
|---|---:|---:|---:|---:|---:|---:|
| security | 14.501 | 23.830 | 120,008 | 31.684299 | 31.367751 | 63.052050 |
| subset | 13.023 | 22.143 | 808 | 0.202497 | 0.202451 | 0.404948 |
| country | 13.016 | 22.167 | 8 | 0.002600 | 0.002582 | 0.005182 |
| weekly | 14.720 | 24.671 | 1,264 | 0.274796 | 0.268777 | 0.543573 |
| monthly | 14.356 | 24.305 | 288 | 0.063190 | 0.061947 | 0.125137 |

MB uses 1,000,000 bytes. Compact sizes include report headers and all four portfolio CSVs, including security/subset reconciliation rows, but exclude metrics/logs. Output files are small and are retained as the final compact reports; no large daily/detail CSV is produced.

## Measurement method

The fixture has four disjoint portfolios of 15,000 fixed instruments each, a shared 100,000-instrument index, and 1,095 calendar dates. All portfolio holdings are in the index. Each execution reads 175,200,000 return records; weights vary daily and are normalized per entity/date. All held instruments map to US/USD.

Machine: macOS-27.0.1-arm64-arm-64bit-Mach-O, 12 logical CPUs. Rust uses 4 native portfolio workers; Go uses GOMAXPROCS=12 with concurrent portfolio goroutines. Each uses its own optimized parser/buffers/formatter. Language order alternates between repetitions; filesystem cache is OS-managed and first runs are retained. These are workload-specific observations, not controlled cold-cache or universal language claims.

**Stage seconds are accumulated worker elapsed time, include scheduling pauses and overlap across threads. They are not stage CPU time and cannot be added to obtain wall time.** Read includes decoding, numeric parsing, lookup and validation. Filter includes selection and denominator scan; only subset applies an instrument predicate. Aggregate includes normalization, dense slot construction and weighted contributions. Compute includes daily effects, linking, reconciliation, compact report planning, residual reduction and checksums. Write includes only attribution file creation/header, numeric/CSV formatting, buffering, flush and fsync. Metrics-file writing, setup, channel waits and publication contribute to external overall time, not the attribution write stage.

Per-stage rates divide work by the corresponding median stage duration. Groups are input entity/group/date combinations, not output row counts. Logical output effect components count three components per compact report row. Daily effect components count the daily engine’s values before linking/reduction. Average cores is process CPU time divided by process wall time; it is descriptive, not a metric where larger is automatically better.

## Five scenarios

### 1. Security

![Compact security flow](docs/flows/01-security.svg)

Compute on the full portfolio/index union, tracking whether each ID was ever held by this portfolio during the reporting range. At EOF, emit one linked detail row for each ever-held security and aggregate never-held benchmark IDs into `RowType=benchmark_only`, `Segment=BENCHMARK_ONLY`. The residual bucket preserves the complete active result. Do not renormalize the displayed held-only index weights: the full index remains the benchmark.

**Scope:** all 1,095 dates, all four portfolios, both return types; complete read, calculation and CSV-writing executions.

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read/parse | Seconds | 25.194951 | 37.398302 |
|  | Million rows/sec | 6.954 | 4.685 |
|  | Input MiB/sec | 419.451 | 282.581 |
| Filter | Seconds | 0.095541 | 0.307224 |
|  | Rows examined | 175,200,000 | 175,200,000 |
|  | Rows retained | 175,200,000 | 175,200,000 |
| Aggregate | Seconds | 0.854045 | 1.008709 |
|  | Records processed/sec | 205,141,402.53 | 173,687,287.50 |
|  | Input entity/group/date groups produced | 175,200,000 | 175,200,000 |
| Compute/link | Seconds | 3.982126 | 9.562240 |
|  | Daily effect components computed | 2,628,000,000 | 2,628,000,000 |
|  | Daily effect components/sec | 659,948,915.32 | 274,830,990.43 |
|  | Logical output effect components/sec | 90,409.99 | 37,650.59 |
|  | Logical report rows | 120,008 | 120,008 |
| Write CSV | Seconds | 0.107084 | 0.117280 |
|  | Rows/sec | 1,120,694.48 | 1,023,264.27 |
|  | Output MiB/sec | 282.177 | 255.071 |
|  | Attribution rows written | 120,008 | 120,008 |
|  | Attribution output MB | 31.684299 | 31.367751 |
| Overall | Median wall seconds | 14.501281 | 23.830096 |
|  | Wall min–max seconds | 14.149–14.854 | 23.563–24.097 |
|  | Median peak RSS MiB | 112.430 | 230.633 |
|  | Median CPU seconds | 30.099794 | 50.728190 |
|  | Median average cores | 2.075 | 2.129 |

### 2. Standalone subset

![Compact standalone subset flow](docs/flows/02-subset.svg)

Predicate: `InstrumentId IN selected_ids`. The 400-ID set comprises the inclusive ranges `S000001–S000100`, `S015001–S015100`, `S030001–S030100`, and `S045001–S045100`; see [subset.csv](results/compact-full-history/subset.csv). Each portfolio holds 100 selected IDs; the index holds 400. Normalize each selected side independently every day; fail if either denominator is zero. Link across the full history. Emit 800 held-detail rows plus eight benchmark-only residual rows. Filtered-out input is still parsed and validated, but does not enter attribution.

**Scope:** all 1,095 dates, all four portfolios, both return types; complete read, calculation and CSV-writing executions.

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read/parse | Seconds | 23.925582 | 35.502112 |
|  | Million rows/sec | 7.323 | 4.935 |
|  | Input MiB/sec | 441.705 | 297.674 |
| Filter | Seconds | 0.070383 | 0.165726 |
|  | Rows examined | 175,200,000 | 175,200,000 |
|  | Rows retained | 876,000 | 876,000 |
| Aggregate | Seconds | 0.007270 | 0.010179 |
|  | Records processed/sec | 120,493,702.31 | 86,057,885.72 |
|  | Input entity/group/date groups produced | 876,000 | 876,000 |
| Compute/link | Seconds | 0.010824 | 0.026857 |
|  | Daily effect components computed | 10,512,000 | 10,512,000 |
|  | Daily effect components/sec | 971,216,396.34 | 391,399,385.73 |
|  | Logical output effect components/sec | 223,956.29 | 90,254.20 |
|  | Logical report rows | 808 | 808 |
| Write CSV | Seconds | 0.036136 | 0.037216 |
|  | Rows/sec | 22,360.18 | 21,711.15 |
|  | Output MiB/sec | 5.344 | 5.188 |
|  | Attribution rows written | 808 | 808 |
|  | Attribution output MB | 0.202497 | 0.202451 |
| Overall | Median wall seconds | 13.022669 | 22.142611 |
|  | Wall min–max seconds | 12.965–13.080 | 22.058–22.227 |
|  | Median peak RSS MiB | 40.156 | 74.953 |
|  | Median CPU seconds | 23.912335 | 37.891954 |
|  | Median average cores | 1.836 | 1.711 |

### 3. Country

![Compact country flow](docs/flows/03-country.svg)

No country filter applies. Resolve InstrumentId to Country, aggregate each side’s normalized weights and weighted returns by country, calculate daily country effects, then link across all dates. All held instruments are US/USD, so the report has one country per portfolio and return type: eight rows. This fixture is a weak test of realistic multi-country attribution.

**Scope:** all 1,095 dates, all four portfolios, both return types; complete read, calculation and CSV-writing executions.

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read/parse | Seconds | 22.928799 | 34.916485 |
|  | Million rows/sec | 7.641 | 5.018 |
|  | Input MiB/sec | 460.907 | 302.667 |
| Filter | Seconds | 0.096108 | 0.312492 |
|  | Rows examined | 175,200,000 | 175,200,000 |
|  | Rows retained | 175,200,000 | 175,200,000 |
| Aggregate | Seconds | 0.213288 | 0.335391 |
|  | Records processed/sec | 821,426,412.87 | 522,374,836.34 |
|  | Input entity/group/date groups produced | 5,475 | 5,475 |
| Compute/link | Seconds | 0.001039 | 0.000635 |
|  | Daily effect components computed | 26,280 | 26,280 |
|  | Daily effect components/sec | 25,286,688.31 | 41,371,428.21 |
|  | Logical output effect components/sec | 23,092.87 | 37,782.13 |
|  | Logical report rows | 8 | 8 |
| Write CSV | Seconds | 0.033001 | 0.034793 |
|  | Rows/sec | 242.42 | 229.93 |
|  | Output MiB/sec | 0.075 | 0.071 |
|  | Attribution rows written | 8 | 8 |
|  | Attribution output MB | 0.002600 | 0.002582 |
| Overall | Median wall seconds | 13.015891 | 22.166817 |
|  | Wall min–max seconds | 12.663–13.369 | 22.049–22.285 |
|  | Median peak RSS MiB | 35.484 | 70.344 |
|  | Median CPU seconds | 23.130959 | 37.919525 |
|  | Median average cores | 1.777 | 1.711 |

### 4. Weekly

![Compact weekly flow](docs/flows/04-weekly.svg)

Calculate daily security effects, reduce them to three portfolio-level components for each return type, then link these six values within a Monday–Sunday bucket. The linked accumulator has one portfolio entry, not 100,000 security entries. At a boundary/EOF, emit two rows per portfolio. There are 158 observed week buckets, including partial boundary weeks: 158 × 4 × 2 = 1,264 rows.

**Scope:** all 1,095 dates, all four portfolios, both return types; complete read, calculation and CSV-writing executions.

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read/parse | Seconds | 25.907244 | 38.680824 |
|  | Million rows/sec | 6.763 | 4.529 |
|  | Input MiB/sec | 407.919 | 273.212 |
| Filter | Seconds | 0.097007 | 0.329595 |
|  | Rows examined | 175,200,000 | 175,200,000 |
|  | Rows retained | 175,200,000 | 175,200,000 |
| Aggregate | Seconds | 0.870800 | 1.688835 |
|  | Records processed/sec | 201,194,317.37 | 103,740,144.31 |
|  | Input entity/group/date groups produced | 175,200,000 | 175,200,000 |
| Compute/link | Seconds | 3.778659 | 6.886732 |
|  | Daily effect components computed | 2,628,000,000 | 2,628,000,000 |
|  | Daily effect components/sec | 695,484,762.52 | 381,603,361.52 |
|  | Logical output effect components/sec | 1,003.53 | 550.62 |
|  | Logical report rows | 1,264 | 1,264 |
| Write CSV | Seconds | 0.041283 | 0.049564 |
|  | Rows/sec | 30,618.25 | 25,502.35 |
|  | Output MiB/sec | 6.348 | 5.172 |
|  | Attribution rows written | 1,264 | 1,264 |
|  | Attribution output MB | 0.274796 | 0.268777 |
| Overall | Median wall seconds | 14.720450 | 24.671069 |
|  | Wall min–max seconds | 14.611–14.830 | 24.438–24.904 |
|  | Median peak RSS MiB | 62.328 | 136.703 |
|  | Median CPU seconds | 30.533341 | 52.819079 |
|  | Median average cores | 2.074 | 2.141 |

### 5. Monthly

![Compact monthly flow](docs/flows/05-monthly.svg)

Use the same portfolio-level reduction and linking engine with calendar-month keys. Emit two rows per portfolio/month, not security/month rows. The full range has 36 month buckets: 36 × 4 × 2 = 288 rows. Quarter/year overrides use the same engine with a different bucket key; boundary periods can be partial.

**Scope:** all 1,095 dates, all four portfolios, both return types; complete read, calculation and CSV-writing executions.

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read/parse | Seconds | 25.609576 | 38.072932 |
|  | Million rows/sec | 6.841 | 4.602 |
|  | Input MiB/sec | 412.660 | 277.574 |
| Filter | Seconds | 0.096679 | 0.318752 |
|  | Rows examined | 175,200,000 | 175,200,000 |
|  | Rows retained | 175,200,000 | 175,200,000 |
| Aggregate | Seconds | 0.867075 | 1.672868 |
|  | Records processed/sec | 202,058,613.52 | 104,730,316.74 |
|  | Input entity/group/date groups produced | 175,200,000 | 175,200,000 |
| Compute/link | Seconds | 3.767712 | 6.802985 |
|  | Daily effect components computed | 2,628,000,000 | 2,628,000,000 |
|  | Daily effect components/sec | 697,505,611.80 | 386,301,018.36 |
|  | Logical output effect components/sec | 229.32 | 127.00 |
|  | Logical report rows | 288 | 288 |
| Write CSV | Seconds | 0.040021 | 0.043221 |
|  | Rows/sec | 7,196.20 | 6,663.45 |
|  | Output MiB/sec | 1.506 | 1.367 |
|  | Attribution rows written | 288 | 288 |
|  | Attribution output MB | 0.063190 | 0.061947 |
| Overall | Median wall seconds | 14.355851 | 24.305471 |
|  | Wall min–max seconds | 14.319–14.392 | 24.239–24.372 |
|  | Median peak RSS MiB | 61.742 | 139.602 |
|  | Median CPU seconds | 30.263100 | 52.042584 |
|  | Median average cores | 2.108 | 2.141 |

## Linking and reconciliation

For each component and return type, update `E_new = E_previous × (1 + index_return_today) + daily_effect × portfolio_growth_before_today`, then update portfolio/index compounded growth. Security/subset/country link across the full requested range by default; weekly/monthly reset at calendar boundaries.

The same date’s portfolio and index weights are each normalized to one. Missing-side group returns use the held side’s return, assigning that missing-side effect to allocation. Daily effects must reconcile within 1e-9 and linked period effects within 1e-8. Security/subset displayed held detail plus benchmark-only rows reconcile to the complete active return. Country rows sum to active return. Weekly/monthly rows directly contain the portfolio’s linked effects.

Weights on full-history group rows are mean daily weights; missing groups contribute zero on absent days. They are descriptive averages, not inputs to a new one-period attribution formula. PortfolioReturn, BenchmarkReturn and ActiveReturn are whole-period portfolio quantities repeated on group rows; do not sum those fields. Only effect components should be summed across detail and benchmark-only rows. Weekly/monthly portfolio weights are 1.

## Execution and output

| Concern | Rust | Go |
|---|---|---|
| Input | Native csv parser, reusable record, day buffers and duplicate flags | encoding/csv ReuseRecord, reusable day slices and duplicate flags |
| Shared index | Arc immutable days; bounded channel capacity one | Shared immutable day pointers; buffered channel capacity one |
| Concurrency | Native portfolio workers; four workers for this dataset | Portfolio goroutines; GOMAXPROCS=12 on this machine |
| Group arithmetic | Dense slots; compact slots for selected IDs | Dense slices; compact slots for selected IDs |
| Formatting | Reused ByteRecord with Ryū float conversion | Reused escaped byte buffer with strconv.AppendFloat |
| Output | Emit only the requested final rows; flush/fsync and publish after validation | Emit only the requested final rows; flush/fsync and publish after validation |

No year-scale observations or daily effect table are retained. Working memory holds the master, bounded current/in-flight days, and required active accumulators. Security/subset require per-security full-history accumulators; country requires country accumulators; weekly/monthly require one portfolio accumulator per active period. Rust with fewer workers than portfolios rereads the index per worker batch; the benchmark uses four workers and one index pass.

Each execution writes the contracted attribution CSV report and a small metrics CSV. The checksum components and row counts validate the emitted report.

## Interpretation limits

The input is synthetic. The generator samples returns independently by entity, so a shared InstrumentId can have different portfolio/index returns. These benchmarks establish computational behavior, not validated investment-selection insight. We assume supplied weights apply to that day’s returns; no transaction/cash-flow or beginning/end-of-day timing model has been established. Base/local are separate analyses, not a dedicated currency attribution model. The same pricing/model limitations exist in both languages.

See [the implementation guide](rust/README.md) for CLI options, output columns and validation.

## Correctness evidence

| Scenario | Compact rows compared | Maximum absolute numeric difference | Repetition checksum agreement | Result |
|---|---:|---:|---|---|
| security | 120,008 | 1.5e-17 | PASS | PASS |
| subset | 808 | 1.3e-16 | PASS | PASS |
| country | 8 | 1.63e-16 | PASS | PASS |
| weekly | 1,264 | 3.47e-18 | PASS | PASS |
| monthly | 288 | 1.04e-17 | PASS | PASS |

Every numeric field of the final compact Rust and Go CSVs is compared (absolute tolerance 1e-10 or relative tolerance 1e-8), not a sample. Every execution validates daily and linked reconciliation. Logical row counts and all three checksum components must agree across languages and repetitions. An independent Python closed-form linking oracle covers nine small scenario/period combinations, changing holdings, full-history held membership, benchmark-only reconciliation and invalid inputs. Checksum equality and reconciliation are consistency evidence, not independent proof of an economically realistic model.

## Who wins, and where?

Lower median accumulated stage time, process wall time, memory and CPU determine each winner. Filter is N/A without an instrument predicate. Small stages and two-repeat medians are noisy and do not establish statistical significance.

| Scenario | Read/parse | Filter | Aggregate | Compute/link | Write | Overall | Peak memory | CPU time | Overall speed advantage |
|---|---|---|---|---|---|---|---|---|---|
| security | Rust | N/A | Rust | Rust | Rust | Rust | Rust | Rust | Rust 1.64× |
| subset | Rust | Rust | Rust | Rust | Rust | Rust | Rust | Rust | Rust 1.70× |
| country | Rust | N/A | Rust | Go | Rust | Rust | Rust | Rust | Rust 1.70× |
| weekly | Rust | N/A | Rust | Rust | Rust | Rust | Rust | Rust | Rust 1.68× |
| monthly | Rust | N/A | Rust | Rust | Rust | Rust | Rust | Rust | Rust 1.69× |

## Recorded sources and reproduction

- [All raw executions](results/compact-full-history/runs.json), [CSV version](results/compact-full-history/runs.csv), and [median/range summaries](results/compact-full-history/summary.json).
- [Correctness checks](results/compact-full-history/checks.json), [environment, input metadata and executable hashes](results/compact-full-history/environment.json).
- [Contracts, formulas, CLI and limitations](rust/README.md).

```sh
python3 python/test_attribution.py
python3 python/benchmark_compact.py --runs 2
python3 python/render_stage_report.py
```

The benchmark requires a new output directory if one already exists; use `--output-dir` for a fresh execution. The report, metrics and diagrams use the output contracts defined above.
