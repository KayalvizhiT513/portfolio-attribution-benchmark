"""Flowchart sections embedded by render_stage_report.py in the combined report."""

OVERVIEW = """These diagrams describe the implemented Rust and Go logic, rather than a proposed redesign. Each SVG is standalone and can be opened in a browser, enlarged, or embedded in a document.

## Five scenarios at a glance

| Scenario | Efficient read/filter | Efficient compute | Efficient write |
|---|---|---|---|
| Full security | Stream one date at a time; reuse CSV/day buffers; read index once per active portfolio batch | Dense instrument slots; union of portfolio/index holdings; independent base/local effects | Stream daily security details and total rows to each portfolio’s buffered CSV |
| Standalone subset | Parse and validate all rows; apply a precomputed selection mask on both sides | Independently normalize selected weights; use compact selected-ID slots so aggregation/effect loops avoid the full master | Write selected-universe details and totals; excluded instruments produce no output |
| Country | Resolve instrument-to-country mapping once; stream current-day holdings | Aggregate weight and weighted contributions by country before calculating effects | Write country details and totals rather than security rows |
| Weekly | Stream daily holdings; assign dates to Monday–Sunday buckets | Calculate daily security effects, then update one linked weekly accumulator | Write once per observed week at the next bucket boundary or EOF; include partial weeks |
| Monthly | Stream daily holdings; assign dates to calendar-month buckets | Calculate daily security effects, then update one linked monthly accumulator | Write once per observed month at the next bucket boundary or EOF; quarter/year use the same path |

The diagrams use different structures to expose the actual transformation in each scenario: security union, filter/reject branches, a many-to-one country funnel, a weekly state machine, and a calendar-month progression. Holdings and dates shown inside nodes are illustrative; the country diagram explicitly distinguishes its US/DE example from the US-only supplied data. January and February are successive active periods, not parallel accumulators.

The shared execution engine remains the same: one index producer broadcasts immutable daily arrays to portfolio workers through bounded channels. Each portfolio worker reads its own input and owns its accumulator and CSV writer. The diagrams focus on scenario logic; the Rust/Go execution table below documents the shared concurrency layer."""

SCENARIO_FLOWS = {
    "security": """[Open standalone SVG](docs/flows/01-security.svg)

![Full security-level attribution flow](docs/flows/01-security.svg)

Dense numeric instrument slots remove string-key joins from the effect loop. Presence flags preserve instruments held on only one side. The daily effects reconcile to portfolio minus index return. Full daily output still has a large inherent write cost: approximately 876 million detail rows for the supplied full history.""",
    "subset": """[Open standalone SVG](docs/flows/02-subset.svg)

![Standalone instrument subset flow](docs/flows/02-subset.svg)

Selected IDs are mapped once to a mask and compact output slots. The filter is applied after CSV parsing and validation: it reduces aggregation, computation and output work, but does not avoid reading the source CSVs. Each side’s selected weights are normalized independently on every date. A zero selected-weight denominator fails explicitly. The selected index supplies the benchmark return; full-index weights and returns are not used for this scenario’s attribution.""",
    "country": """[Open standalone SVG](docs/flows/03-country.svg)

![Country attribution flow](docs/flows/03-country.svg)

For each country, accumulate `sum(weight)`, `sum(weight × BaseReturn)` and `sum(weight × LocalReturn)`. Divide contribution by group weight to obtain group return, then calculate country-level effects. This is attribution calculated at country level, rather than a sum of previously calculated security effects. The supplied holdings produce one country group, US.""",
    "weekly": """[Open standalone SVG](docs/flows/04-weekly.svg)

![Weekly linked security attribution flow](docs/flows/04-weekly.svg)

Each worker retains current-day inputs and a single active week accumulator. A Monday starts a new bucket; the previous bucket is emitted when the next date reveals the boundary. EOF also closes the active bucket. No complete list of daily effects is retained for later linking.""",
    "monthly": """[Open standalone SVG](docs/flows/05-monthly.svg)

![Monthly linked security attribution flow](docs/flows/05-monthly.svg)

The calendar-month key replaces the weekly key; the streaming, attribution and linking logic is otherwise the same. `--period quarter` or `--period year` changes bucket assignment without introducing a separate processing engine. Observed first/last dates label partial periods.""",
}

SHARED_LOGIC = """## Linking without retaining daily history

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

Regenerate the SVGs using `python3 python/render_attribution_flows.py`."""
