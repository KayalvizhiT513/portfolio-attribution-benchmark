# Attribution flows and stage comparison: Rust and Go

All five scenarios were compared. This document records every requested stage metric for all five scenarios, followed by the additional full-history measurements.

Each scenario below combines its SVG flowchart, efficiency explanation and measured stage metrics. Shared linking and Rust/Go execution details follow the five scenario comparisons.

## Coverage and method

| Comparison | Scenarios | Effective dates | Repetitions |
|---|---|---|---:|
| Five-scenario comparison | Security, standalone subset, country, weekly, monthly | 2023-01-01 through 2023-01-03 | 3 per language/scenario |
| Additional full-history comparison | Country, standalone subset | All 1,095 dates | 2 per language/scenario |

All runs include all four portfolios and their index. Rust uses four native portfolio workers; Go uses GOMAXPROCS=12 and concurrent I/O goroutines. Each implementation uses its own optimized parser, buffering, formatting and runtime approach. Runs alternate language order and use the OS-managed filesystem cache; they are not controlled cold-cache measurements. Times, memory and CPU values are medians; ranges are included for overall wall time. Rates divide the recorded work by the median corresponding stage duration.

**Stage seconds are accumulated elapsed time inside each stage across threads. They can overlap, include scheduling pauses, and must not be added together or interpreted as stage CPU time.** Overall wall time is measured externally for the complete process. Peak memory is the per-process maximum resident set; CPU time includes all process threads; average cores is CPU time divided by process wall time.

Read includes CSV decoding, numeric parsing, security lookup and validation. Its byte rate uses consumed return-file bytes, not the full dataset size for truncated runs. Filter timings include the selection/weight-denominator scan: only the subset scenario applies an instrument predicate; all other scenarios retain all rows. Aggregation includes independent weight normalization, dense-group construction and weighted contributions. Group counts include the index and portfolio input groups on every date, rather than just distinct labels. Compute includes attribution, linking and reconciliation. Write includes numeric formatting, CSV escaping, buffering, flush and fsync. Master loading, setup and channel waits remain in overall time.

**Output effects** below count the three attribution components (allocation, selection, interaction) on every written row, including portfolio-total rows. The derived `Total` field is not counted as a fourth component. Output effects/sec divides this count by compute-stage seconds; it excludes CSV formatting, which belongs to the write stage. Daily detail effect counts are shown separately because linking reduces the number of period output rows.

The first-three-date weekly sample contains two partial weeks; the monthly sample contains one partial month. These are linked results for observed dates, not completed calendar periods. Full-history daily security output would contain approximately 876 million detail rows, around 180 GiB per language at the observed CSV width, exceeding the available space for both languages. Full-history write performance for security/weekly/monthly has therefore **not** been measured.

## Scenario flow overview

These diagrams describe the implemented Rust and Go logic, rather than a proposed redesign. Each SVG is standalone and can be opened in a browser, enlarged, or embedded in a document.

## Five scenarios at a glance

| Scenario | Efficient read/filter | Efficient compute | Efficient write |
|---|---|---|---|
| Full security | Stream one date at a time; reuse CSV/day buffers; read index once per active portfolio batch | Dense instrument slots; union of portfolio/index holdings; independent base/local effects | Stream daily security details and total rows to each portfolio’s buffered CSV |
| Standalone subset | Parse and validate all rows; apply a precomputed selection mask on both sides | Independently normalize selected weights; use compact selected-ID slots so aggregation/effect loops avoid the full master | Write selected-universe details and totals; excluded instruments produce no output |
| Country | Resolve instrument-to-country mapping once; stream current-day holdings | Aggregate weight and weighted contributions by country before calculating effects | Write country details and totals rather than security rows |
| Weekly | Stream daily holdings; assign dates to Monday–Sunday buckets | Calculate daily security effects, then update one linked weekly accumulator | Write once per observed week at the next bucket boundary or EOF; include partial weeks |
| Monthly | Stream daily holdings; assign dates to calendar-month buckets | Calculate daily security effects, then update one linked monthly accumulator | Write once per observed month at the next bucket boundary or EOF; quarter/year use the same path |

The diagrams use different structures to expose the actual transformation in each scenario: security union, filter/reject branches, a many-to-one country funnel, a weekly state machine, and a calendar-month progression. Holdings and dates shown inside nodes are illustrative; the country diagram explicitly distinguishes its US/DE example from the US-only supplied data. January and February are successive active periods, not parallel accumulators.

The shared execution engine remains the same: one index producer broadcasts immutable daily arrays to portfolio workers through bounded channels. Each portfolio worker reads its own input and owns its accumulator and CSV writer. The diagrams focus on scenario logic; the Rust/Go execution table below documents the shared concurrency layer.

## Five-scenario comparison — first three effective dates

### 1. Full security-level attribution

[Open standalone SVG](docs/flows/01-security.svg)

![Full security-level attribution flow](docs/flows/01-security.svg)

Dense numeric instrument slots remove string-key joins from the effect loop. Presence flags preserve instruments held on only one side. The daily effects reconcile to portfolio minus index return. Full daily output still has a large inherent write cost: approximately 876 million detail rows for the supplied full history.

#### Stage metrics

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read and parse CSV | Seconds | 0.071842 | 0.107602 |
|  | Million rows/sec | 6.681 | 4.461 |
|  | Input MiB/sec | 402.968 | 269.047 |
|  | Rows read | 480,000 | 480,000 |
| Filter | Seconds | 0.000256 | 0.000855 |
|  | Rows examined | 480,000 | 480,000 |
|  | Rows retained | 480,000 | 480,000 |
|  | Rows excluded | 0 | 0 |
| Aggregate | Seconds | 0.004563 | 0.005709 |
|  | Records processed/sec | 105,183,439.92 | 84,078,405.21 |
|  | Records processed | 480,000 | 480,000 |
|  | Groups produced (input entity/group/date combinations) | 480,000 | 480,000 |
| Compute attribution and linking | Seconds | 0.013469 | 0.041607 |
|  | Output effects/sec (3 components × CSV rows) | 534,548,091.62 | 173,047,463.70 |
|  | Output effect components (includes total rows) | 7,200,072 | 7,200,072 |
|  | Daily detail effect components computed | 7,200,000 | 7,200,000 |
| Write CSV | Seconds | 0.751416 | 0.904783 |
|  | Rows/sec | 3,194,000.31 | 2,652,597.60 |
|  | Output MiB/sec | 644.082 | 527.568 |
|  | Rows written (detail + total) | 2,400,024 | 2,400,024 |
|  | Output MiB | 483.974 | 477.335 |
| Overall | Wall time (seconds) | 0.282638 | 0.325493 |
|  | Wall time min–max (seconds) | 0.274462–0.293105 | 0.323800–0.328624 |
|  | Peak memory (MiB) | 117.172 | 207.828 |
|  | CPU time (seconds) | 0.847354 | 1.056990 |
|  | Average cores used | 2.998 | 3.240 |

### 2. Standalone instrument subset

[Open standalone SVG](docs/flows/02-subset.svg)

![Standalone instrument subset flow](docs/flows/02-subset.svg)

Selected IDs are mapped once to a mask and compact output slots. The filter is applied after CSV parsing and validation: it reduces aggregation, computation and output work, but does not avoid reading the source CSVs. Each side’s selected weights are normalized independently on every date. A zero selected-weight denominator fails explicitly. The selected index supplies the benchmark return; full-index weights and returns are not used for this scenario’s attribution.

#### Stage metrics

The selected universe is the union of the first 100 instruments in each portfolio (400 IDs). Each portfolio and the index are independently normalized to 100% on each date.

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read and parse CSV | Seconds | 0.062719 | 0.103951 |
|  | Million rows/sec | 7.653 | 4.618 |
|  | Input MiB/sec | 461.585 | 278.496 |
|  | Rows read | 480,000 | 480,000 |
| Filter | Seconds | 0.000204 | 0.000480 |
|  | Rows examined | 480,000 | 480,000 |
|  | Rows retained | 2,400 | 2,400 |
|  | Rows excluded | 477,600 | 477,600 |
| Aggregate | Seconds | 0.000028 | 0.000034 |
|  | Records processed/sec | 84,581,497.80 | 70,669,297.13 |
|  | Records processed | 2,400 | 2,400 |
|  | Groups produced (input entity/group/date combinations) | 2,400 | 2,400 |
| Compute attribution and linking | Seconds | 0.000033 | 0.000090 |
|  | Output effects/sec (3 components × CSV rows) | 865,078,651.69 | 320,803,564.48 |
|  | Output effect components (includes total rows) | 28,872 | 28,872 |
|  | Daily detail effect components computed | 28,800 | 28,800 |
| Write CSV | Seconds | 0.036916 | 0.035553 |
|  | Rows/sec | 260,697.61 | 270,693.51 |
|  | Output MiB/sec | 53.827 | 54.502 |
|  | Rows written (detail + total) | 9,624 | 9,624 |
|  | Output MiB | 1.987 | 1.938 |
| Overall | Wall time (seconds) | 0.086201 | 0.103150 |
|  | Wall time min–max (seconds) | 0.085503–0.089450 | 0.101943–0.103267 |
|  | Peak memory (MiB) | 42.188 | 64.000 |
|  | CPU time (seconds) | 0.103028 | 0.159812 |
|  | Average cores used | 1.177 | 1.568 |

### 3. Country attribution

[Open standalone SVG](docs/flows/03-country.svg)

![Country attribution flow](docs/flows/03-country.svg)

For each country, accumulate `sum(weight)`, `sum(weight × BaseReturn)` and `sum(weight × LocalReturn)`. Divide contribution by group weight to obtain group return, then calculate country-level effects. This is attribution calculated at country level, rather than a sum of previously calculated security effects. The supplied holdings produce one country group, US.

#### Stage metrics

The supplied holdings are all US/USD, so each input entity has one country group per date.

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read and parse CSV | Seconds | 0.060531 | 0.106906 |
|  | Million rows/sec | 7.930 | 4.490 |
|  | Input MiB/sec | 478.265 | 270.798 |
|  | Rows read | 480,000 | 480,000 |
| Filter | Seconds | 0.000258 | 0.000866 |
|  | Rows examined | 480,000 | 480,000 |
|  | Rows retained | 480,000 | 480,000 |
|  | Rows excluded | 0 | 0 |
| Aggregate | Seconds | 0.000472 | 0.000888 |
|  | Records processed/sec | 1,018,025,412.46 | 540,820,086.06 |
|  | Records processed | 480,000 | 480,000 |
|  | Groups produced (input entity/group/date combinations) | 15 | 15 |
| Compute attribution and linking | Seconds | 0.000002 | 0.000004 |
|  | Output effects/sec (3 components × CSV rows) | 72,000,000.00 | 39,290,586.63 |
|  | Output effect components (includes total rows) | 144 | 144 |
|  | Daily detail effect components computed | 72 | 72 |
| Write CSV | Seconds | 0.032863 | 0.029613 |
|  | Rows/sec | 1,460.62 | 1,620.92 |
|  | Output MiB/sec | 0.347 | 0.382 |
|  | Rows written (detail + total) | 48 | 48 |
|  | Output MiB | 0.011 | 0.011 |
| Overall | Wall time (seconds) | 0.087380 | 0.106897 |
|  | Wall time min–max (seconds) | 0.082452–0.090186 | 0.101578–0.116841 |
|  | Peak memory (MiB) | 37.109 | 59.828 |
|  | CPU time (seconds) | 0.097695 | 0.158841 |
|  | Average cores used | 1.168 | 1.536 |

### 4. Weekly linked security attribution

[Open standalone SVG](docs/flows/04-weekly.svg)

![Weekly linked security attribution flow](docs/flows/04-weekly.svg)

Each worker retains current-day inputs and a single active week accumulator. A Monday starts a new bucket; the previous bucket is emitted when the next date reveals the boundary. EOF also closes the active bucket. No complete list of daily effects is retained for later linking.

#### Stage metrics

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read and parse CSV | Seconds | 0.073391 | 0.110114 |
|  | Million rows/sec | 6.540 | 4.359 |
|  | Input MiB/sec | 394.461 | 262.909 |
|  | Rows read | 480,000 | 480,000 |
| Filter | Seconds | 0.000260 | 0.000843 |
|  | Rows examined | 480,000 | 480,000 |
|  | Rows retained | 480,000 | 480,000 |
|  | Rows excluded | 0 | 0 |
| Aggregate | Seconds | 0.004422 | 0.006720 |
|  | Records processed/sec | 108,557,398.82 | 71,433,450.59 |
|  | Records processed | 480,000 | 480,000 |
|  | Groups produced (input entity/group/date combinations) | 480,000 | 480,000 |
| Compute attribution and linking | Seconds | 0.011495 | 0.037206 |
|  | Output effects/sec (3 components × CSV rows) | 417,592,109.68 | 129,013,031.15 |
|  | Output effect components (includes total rows) | 4,800,048 | 4,800,048 |
|  | Daily detail effect components computed | 7,200,000 | 7,200,000 |
| Write CSV | Seconds | 0.529335 | 0.609199 |
|  | Rows/sec | 3,022,691.45 | 2,626,427.82 |
|  | Output MiB/sec | 605.046 | 517.727 |
|  | Rows written (detail + total) | 1,600,016 | 1,600,016 |
|  | Output MiB | 320.272 | 315.398 |
| Overall | Wall time (seconds) | 0.221104 | 0.246682 |
|  | Wall time min–max (seconds) | 0.218665–0.222896 | 0.244845–0.252634 |
|  | Peak memory (MiB) | 117.516 | 185.156 |
|  | CPU time (seconds) | 0.626130 | 0.773937 |
|  | Average cores used | 2.858 | 3.147 |

### 5. Monthly linked security attribution

[Open standalone SVG](docs/flows/05-monthly.svg)

![Monthly linked security attribution flow](docs/flows/05-monthly.svg)

The calendar-month key replaces the weekly key; the streaming, attribution and linking logic is otherwise the same. `--period quarter` or `--period year` changes bucket assignment without introducing a separate processing engine. Observed first/last dates label partial periods.

#### Stage metrics

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read and parse CSV | Seconds | 0.069486 | 0.106760 |
|  | Million rows/sec | 6.908 | 4.496 |
|  | Input MiB/sec | 416.630 | 271.170 |
|  | Rows read | 480,000 | 480,000 |
| Filter | Seconds | 0.000261 | 0.000839 |
|  | Rows examined | 480,000 | 480,000 |
|  | Rows retained | 480,000 | 480,000 |
|  | Rows excluded | 0 | 0 |
| Aggregate | Seconds | 0.003971 | 0.008185 |
|  | Records processed/sec | 120,863,691.94 | 58,643,867.89 |
|  | Records processed | 480,000 | 480,000 |
|  | Groups produced (input entity/group/date combinations) | 480,000 | 480,000 |
| Compute attribution and linking | Seconds | 0.010516 | 0.030249 |
|  | Output effects/sec (3 components × CSV rows) | 228,219,604.40 | 79,342,702.53 |
|  | Output effect components (includes total rows) | 2,400,024 | 2,400,024 |
|  | Daily detail effect components computed | 7,200,000 | 7,200,000 |
| Write CSV | Seconds | 0.304043 | 0.334482 |
|  | Rows/sec | 2,631,232.36 | 2,391,779.23 |
|  | Output MiB/sec | 525.195 | 471.620 |
|  | Rows written (detail + total) | 800,008 | 800,008 |
|  | Output MiB | 159.682 | 157.749 |
| Overall | Wall time (seconds) | 0.169208 | 0.192609 |
|  | Wall time min–max (seconds) | 0.167033–0.175300 | 0.185790–0.193352 |
|  | Peak memory (MiB) | 112.062 | 175.203 |
|  | CPU time (seconds) | 0.375615 | 0.489211 |
|  | Average cores used | 2.220 | 2.549 |

## Linking without retaining daily history

For each instrument, each attribution component, and each return type:

```text
E_new = E_previous × (1 + index_return_today)
        + daily_effect × portfolio_growth_before_today

portfolio_growth_new = portfolio_growth_before_today × (1 + portfolio_return_today)
index_growth_new     = index_growth_before_today × (1 + index_return_today)
```

`E` is an accumulated allocation, selection, or interaction effect. Daily effects must reconcile before linking; linked period effects must sum to compounded portfolio return minus compounded index return. The recurrence avoids division by the difference between portfolio/index returns, so equal returns do not require a special division case. Period weights in output are arithmetic mean daily weights, not summed weights.

## Language-specific execution

| Concern | Rust implementation | Go implementation |
|---|---|---|
| CSV input | Native `csv` reader with a reusable record and 1 MiB input buffer | `encoding/csv`, `ReuseRecord`, and a 1 MiB buffered reader |
| Day storage | Reusable raw-day vector and duplicate-check storage | Reusable raw-day slice and duplicate-check storage |
| Index sharing | `Arc<Day>` references sent over bounded synchronous channels | Shared immutable `*day` pointers sent over buffered channels |
| Backpressure | Channel capacity one per worker | Channel capacity one per worker |
| Parallelism | Native portfolio worker threads; `--threads` limits the active batch | Goroutine per portfolio; `--threads` sets GOMAXPROCS |
| Index passes | Once per worker batch; four supplied portfolios share one pass with four workers | Once across all portfolio goroutines |
| Effect storage | Dense arrays; compact selected-ID arrays for subsets | Dense slices; compact selected-ID slices for subsets |
| CSV formatting | Reusable `ByteRecord` and Ryū round-trip float formatting | Reusable escaped byte buffer and `strconv.AppendFloat` |
| Write coordination | Each worker owns its output; coordinator publishes files after worker success | Each goroutine owns its output; coordinator publishes files after worker success |

With fewer Rust workers than portfolios, the index is reread once per batch. This bounds native worker concurrency at the cost of additional input I/O; benchmark counters include those passes. Go keeps concurrent I/O goroutines while its runtime caps concurrently executing Go code with GOMAXPROCS. These are independently chosen execution models.

The one-item channel buffers and immutable index sharing bound queued input. Memory holds reusable reader buffers, current/queued/in-flight index days, portfolio current-day arrays, and one active period accumulator per worker. It does not retain a full-history security table or a full-history output table. Slow workers apply backpressure to the producer instead of allowing unbounded queued days. Output files grow with history even though working memory does not.

## Stage timing locations

| Stage | What the diagrams measure |
|---|---|
| Read and parse CSV | CSV decode, numeric parse, mapping lookup, duplicate/date/value checks and reusable day-buffer fill |
| Filter | Selection predicate when applicable and selected/full weight-denominator scan |
| Aggregate | Normalization, dense-slot allocation, weighted contributions and group construction |
| Compute attribution and linking | Daily effects, compounded growth, period recurrence and reconciliation |
| Write CSV | Numeric formatting, escaping, buffered row writes, final flush and fsync |
| Overall | Entire process including master setup, channel waits, coordination and file publication |

Stage durations are accumulated elapsed time across threads and can overlap. They do not add up to overall process wall time. See [the scenario metrics above](#five-scenario-comparison--first-three-effective-dates).

## Implementation references

- [Rust stream, attribution, linking, workers and coordinator](rust/src/main.rs).
- [Go stream, attribution, linking, workers and coordinator](go/cmd/attribution/main.go).
- [CLI options and formulas](rust/README.md).
- [SVG generation script](python/render_attribution_flows.py).

Regenerate the SVGs using `python3 python/render_attribution_flows.py`.

## Additional full-history comparison — all 1,095 dates

These measurements exercise all 175.2 million input rows per run. They supplement the five-scenario comparison above; they do not replace its three-date measurements.

### Country attribution

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read and parse CSV | Seconds | 22.944748 | 35.024303 |
|  | Million rows/sec | 7.636 | 5.002 |
|  | Input MiB/sec | 460.587 | 301.735 |
|  | Rows read | 175,200,000 | 175,200,000 |
| Filter | Seconds | 0.094911 | 0.312284 |
|  | Rows examined | 175,200,000 | 175,200,000 |
|  | Rows retained | 175,200,000 | 175,200,000 |
|  | Rows excluded | 0 | 0 |
| Aggregate | Seconds | 0.205904 | 0.326901 |
|  | Records processed/sec | 850,883,774.41 | 535,942,798.03 |
|  | Records processed | 175,200,000 | 175,200,000 |
|  | Groups produced (input entity/group/date combinations) | 5,475 | 5,475 |
| Compute attribution and linking | Seconds | 0.000375 | 0.000750 |
|  | Output effects/sec (3 components × CSV rows) | 140,287,006.50 | 70,043,764.03 |
|  | Output effect components (includes total rows) | 52,560 | 52,560 |
|  | Daily detail effect components computed | 26,280 | 26,280 |
| Write CSV | Seconds | 0.054366 | 0.046367 |
|  | Rows/sec | 322,257.86 | 377,852.19 |
|  | Output MiB/sec | 71.863 | 83.161 |
|  | Rows written (detail + total) | 17,520 | 17,520 |
|  | Output MiB | 3.907 | 3.856 |
| Overall | Wall time (seconds) | 13.037831 | 22.175153 |
|  | Wall time min–max (seconds) | 13.008139–13.067523 | 22.044841–22.305465 |
|  | Peak memory (MiB) | 45.391 | 74.945 |
|  | CPU time (seconds) | 23.168737 | 37.849452 |
|  | Average cores used | 1.777 | 1.707 |

### Standalone instrument subset

| Stage | Metric | Rust | Go |
|---|---|---:|---:|
| Read and parse CSV | Seconds | 23.083838 | 35.477697 |
|  | Million rows/sec | 7.590 | 4.938 |
|  | Input MiB/sec | 457.812 | 297.879 |
|  | Rows read | 175,200,000 | 175,200,000 |
| Filter | Seconds | 0.072971 | 0.165267 |
|  | Rows examined | 175,200,000 | 175,200,000 |
|  | Rows retained | 876,000 | 876,000 |
|  | Rows excluded | 174,324,000 | 174,324,000 |
| Aggregate | Seconds | 0.007290 | 0.010003 |
|  | Records processed/sec | 120,172,612.32 | 87,569,643.98 |
|  | Records processed | 876,000 | 876,000 |
|  | Groups produced (input entity/group/date combinations) | 876,000 | 876,000 |
| Compute attribution and linking | Seconds | 0.012200 | 0.033366 |
|  | Output effects/sec (3 components × CSV rows) | 863,807,922.03 | 315,839,497.92 |
|  | Output effect components (includes total rows) | 10,538,280 | 10,538,280 |
|  | Daily detail effect components computed | 10,512,000 | 10,512,000 |
| Write CSV | Seconds | 1.339314 | 1.449491 |
|  | Rows/sec | 2,622,805.88 | 2,423,443.77 |
|  | Output MiB/sec | 540.737 | 487.436 |
|  | Rows written (detail + total) | 3,512,760 | 3,512,760 |
|  | Output MiB | 724.217 | 706.534 |
| Overall | Wall time (seconds) | 13.104010 | 22.170793 |
|  | Wall time min–max (seconds) | 12.873905–13.334116 | 22.117497–22.224089 |
|  | Peak memory (MiB) | 41.797 | 78.867 |
|  | CPU time (seconds) | 24.263123 | 39.313862 |
|  | Average cores used | 1.852 | 1.773 |

## Numerical verification

| Scope | Scenario | Rows compared | Maximum absolute numeric difference | Result |
|---|---|---:|---:|---|
| First three dates | security | 2,400,024 | 0 | PASS |
| First three dates | subset | 9,624 | 0 | PASS |
| First three dates | country | 48 | 0 | PASS |
| First three dates | weekly | 1,600,016 | 1.73e-18 | PASS |
| First three dates | monthly | 800,008 | 1.73e-18 | PASS |
| Full history | country | 17,520 | 0 | PASS |
| Full history | subset | 3,512,760 | 0 | PASS |

Every date/period also passed native reconciliation. The independent Python oracle covers nine scenario/period combinations, including quarter/year boundaries, changing holdings, missing-side securities, standalone normalization and invalid inputs.

## Recorded sources and reproduction

- [Five-scenario raw runs](results/native-benchmark/runs.csv) and [summary metrics](results/native-benchmark/summary.csv).
- [Full-history raw runs](results/native-full-history/runs.csv) and [summary metrics](results/native-full-history/summary.csv).
- [Five-scenario environment and executable hashes](results/native-benchmark/environment.json).
- [Full-history environment and executable hashes](results/native-full-history/environment.json).
- [Build commands, CLI options and attribution formulas](rust/README.md).

Regenerate this Markdown from the recorded metrics:

```sh
python3 python/render_stage_report.py
```
