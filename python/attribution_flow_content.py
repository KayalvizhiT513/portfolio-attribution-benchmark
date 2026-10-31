"""Compact-contract explanations embedded in the current benchmark report."""
OVERVIEW = """Calculation and output granularity are independent. Every scenario reads daily inputs, validates and normalizes them, and calculates the required daily effects. Only the requested linked summaries are emitted.

| Scenario | Calculation state | Final output contract | Rows for this dataset |
|---|---|---|---:|
| Security | Daily full security union; full-history linked accumulator per instrument | Portfolio-held IDs plus explicit benchmark-only reconciliation bucket | 120,000 + 8 |
| Standalone subset | Independently normalized selected sides; compact selected-ID slots; full-history linking | Selected portfolio-held IDs plus benchmark-only reconciliation bucket | 800 + 8 |
| Country | Daily weighted country grouping; full-history linking | One country/portfolio/return-type row | 8 |
| Weekly | Sum daily security effects to portfolio components; link within Monday–Sunday buckets | One portfolio/week/return-type row | 1,264 |
| Monthly | Sum daily security effects to portfolio components; link within calendar months | One portfolio/month/return-type row | 288 |

All counts include separate base/local rows. Security/subset counts include eight reconciliation rows: one per portfolio and return type. No duplicate total rows are appended. All five use the complete 1,095-date history."""

SCENARIO_FLOWS = {
    'security': """![Compact security flow](docs/flows/01-security.svg)

Compute on the full portfolio/index union, tracking whether each ID was ever held by this portfolio during the reporting range. At EOF, emit one linked detail row for each ever-held security and aggregate never-held benchmark IDs into `RowType=benchmark_only`, `Segment=BENCHMARK_ONLY`. The residual bucket preserves the complete active result. Do not renormalize the displayed held-only index weights: the full index remains the benchmark.""",
    'subset': """![Compact standalone subset flow](docs/flows/02-subset.svg)

Predicate: `InstrumentId IN selected_ids`. The 400-ID set comprises the inclusive ranges `S000001–S000100`, `S015001–S015100`, `S030001–S030100`, and `S045001–S045100`; see [subset.csv](results/compact-full-history/subset.csv). Each portfolio holds 100 selected IDs; the index holds 400. Normalize each selected side independently every day; fail if either denominator is zero. Link across the full history. Emit 800 held-detail rows plus eight benchmark-only residual rows. Filtered-out input is still parsed and validated, but does not enter attribution.""",
    'country': """![Compact country flow](docs/flows/03-country.svg)

No country filter applies. Resolve InstrumentId to Country, aggregate each side’s normalized weights and weighted returns by country, calculate daily country effects, then link across all dates. All held instruments are US/USD, so the report has one country per portfolio and return type: eight rows. This fixture is a weak test of realistic multi-country attribution.""",
    'weekly': """![Compact weekly flow](docs/flows/04-weekly.svg)

Calculate daily security effects, reduce them to three portfolio-level components for each return type, then link these six values within a Monday–Sunday bucket. The linked accumulator has one portfolio entry, not 100,000 security entries. At a boundary/EOF, emit two rows per portfolio. There are 158 observed week buckets, including partial boundary weeks: 158 × 4 × 2 = 1,264 rows.""",
    'monthly': """![Compact monthly flow](docs/flows/05-monthly.svg)

Use the same portfolio-level reduction and linking engine with calendar-month keys. Emit two rows per portfolio/month, not security/month rows. The full range has 36 month buckets: 36 × 4 × 2 = 288 rows. Quarter/year overrides use the same engine with a different bucket key; boundary periods can be partial.""",
}

SHARED_LOGIC = """## Linking and reconciliation

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

See [the implementation guide](rust/README.md) for CLI options, output columns and validation."""
