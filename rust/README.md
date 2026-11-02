# Native attribution engines: CLI, output contracts and formulas

Rust and Go implement the same attribution contracts with native CSV or Arrow/Parquet streaming, bounded immutable index broadcasts and independent portfolio workers. Daily country attribution uses a complete-date pool for eligible CSV and Parquet inputs; other paths retain streaming execution. Python provides independent correctness checks and benchmark orchestration. See the [current country performance report](../COUNTRY_REPORT_EFFECTIVE_DATED_SMF.md).

## Output contracts

| Scenario | Default period | Final report grain | Rows in current merged data |
|---|---|---|---:|
| security | all dates | Full portfolio/index instrument union / portfolio / return type | 318,000 detail |
| subset | all dates | Selected portfolio-held instrument / portfolio / return type, plus benchmark-only bucket | 2 × selected held IDs + 2 if selected benchmark-only IDs exist |
| country | day | US country / date / return type, US-only normalized | 2,190 |
| weekly | Monday–Sunday | Portfolio / week / return type | 316 |
| monthly | calendar month | Portfolio / month / return type | 72 |

Calculation granularity and output granularity are independent. Security,
subset and weekly/monthly calculations include the full applicable portfolio/
index union through aligned master-ID slots (full outer-join semantics), with
zero weight on a missing side. Country also aligns the filtered, normalized US security union, computes
security effects, then sums effects, weights and contributions into daily country rows.

Security writes an individual `RowType=detail` row for every instrument present
on either side during the reporting period, including portfolio-only and
index-only instruments. There is no security summary bucket. Subset keeps
selected portfolio-held detail and combines selected never-held index IDs into
one `RowType=benchmark_only`, `Segment=BENCHMARK_ONLY` row per return type.
Subset detail plus this bucket reconcile to the complete selected active return.
There are no duplicate portfolio-total CSV rows for security/subset/country.


Weekly/monthly calculate daily security effects, **sum each component to the portfolio level first**, then link the portfolio components inside the current period. Their period accumulator has a single portfolio entry, and their CSVs contain `RowType=total`, `Segment=ALL`. Daily/security observations are not saved as output history.

Both base and local returns are calculated separately. Periods include all supplied effective dates, including weekends and partial calendar boundaries. `PeriodStart/End` are observed dates. On group rows, weights are arithmetic mean daily weights (zero on absent days); period returns are whole-portfolio/index compounded returns repeated across rows. Sum effect fields, not repeated portfolio-return fields.

## Build and run

From the repository root, with stable Rust and Go 1.25+:

```sh
cargo build --release --manifest-path rust/Cargo.toml
go -C go build -trimpath -ldflags='-s -w' -o ../benchmark-go-attribution ./cmd/attribution

./rust/target/release/portfolio-attribution --scenario security \
  --data attribution_data --output-dir results/rust-security --metrics results/rust-security-metrics.csv

./benchmark-go-attribution --scenario monthly \
  --data attribution_data --output-dir results/go-monthly-compute --metrics results/go-monthly-compute-metrics.csv
```

The temporary validation toolchain is under `/tmp`: use `CARGO_HOME=/tmp/attribution-cargo RUSTUP_HOME=/tmp/attribution-rustup /tmp/attribution-cargo/bin/cargo` if Rust is not on PATH. This installation did not change the shell profile.

Every execution calculates, links, validates and writes its attribution report in the portfolio input format, plus a metrics CSV. There is no execution-mode option.

The default is all input dates (`--max-days 0`). `--max-days N` is available for explicit tests. `--period all|day|week|month|quarter|year` overrides the reporting period; defaults follow the table above. Country requires daily output (`--period day`); other scenarios control output grain independently of the period. Weekly/monthly produce portfolio-level rows; security/subset/country retain their groups.

For subset, pass `--instruments-file path.csv` with exact header `InstrumentId`, one ID per record, or a Parquet file with the string column `InstrumentId`. Filter both sides independently and normalize their selected weights separately every day. Zero selected weight on either side fails. The historical four-portfolio benchmark selected the union of the first 100 instruments in each portfolio: `S000001–S000100`, `S015001–S015100`, `S030001–S030100`, `S045001–S045100`. Those historical subset IDs do not describe the current remapped dataset; select held IDs from the current master/inputs instead.

`--data` accepts CSV, Parquet or mixed-format directories. Files are resolved by
stem: `security_master`, `index` and `portfolio_*`. Both extensions for the same
stem are an error, preventing double counting or silent input selection. Parquet
projects required columns by name, accepts strings/date32 for dates and float64
for numeric fields, and rejects null required values. Decoded batches contain at
most 65,536 rows; daily grouping spans batch and row-group boundaries. The daily
dense arrays, formulas and concurrency are retained. Country now filters both sides to US, normalizes their US weights independently and emits daily results. Zero US weight on either side fails. See [current output counts](../docs/OUTPUT_ROW_COUNTS.md). See
[input architecture and validation](../docs/PARQUET_INPUT.md).

## Calculation and validation

For each day/security and return type, normalize weights and compute weighted return contributions. Country reports sum the resulting security effects after this calculation. Divide each group contribution by its weight to obtain its return. With portfolio/index group weights Wp/Wb, group returns Rp/Rb, and whole applicable index return B:

```
allocation  = (Wp - Wb) * (Rb - B)
selection   = Wb * (Rp - Rb)
interaction = (Wp - Wb) * (Rp - Rb)
```

For a zero-weight/missing side, its group return equals the held side's return. This is an explicit convention assigning the missing-side effect through allocation. Daily sums must reconcile to portfolio minus index return within 1e-9.

Forward arithmetic linking applies independently to all three components and both return types:

```
E_new = E_previous * (1 + index_return_today)
        + daily_effect * portfolio_growth_before_today
```

Then update portfolio/index compounded growth. At report boundaries, summed linked effects must reconcile to compounded portfolio return minus compounded index return within 1e-8. Weekly/monthly link sums of daily security effects; linearity makes this equal to summing the corresponding linked security effects, without retaining per-security period state.

CSV input headers must exactly match the generator; Parquet required columns are matched by name. Validate one entity per file, valid increasing grouped ISO dates, mapped unique instrument IDs per date, finite nonnegative weights, finite returns greater than −1, daily full weights within 1e-6 of one, and matched portfolio/index dates. All full weights are normalized again to remove source rounding drift. Compact files are first written as `.partial`; final .csv/.parquet names are published only after all workers validate. A failed run can leave partial files; files already present in the chosen directory are not cleared. Use fresh benchmark directories.

Output columns (the three contribution columns are appended):

```
Scenario,Entity,PeriodStart,PeriodEnd,Days,RowType,Segment,ReturnType,
PortfolioWeight,BenchmarkWeight,Allocation,Selection,Interaction,Total,
PortfolioReturn,BenchmarkReturn,ActiveReturn,
PortfolioContribution,BenchmarkContribution,ActiveContribution
```

Contribution columns use normalized daily weight × return. For multi-date
periods, each side links its daily contributions using its own growth before
that date, so sums reconcile to the side's compounded period return.
ActiveContribution is the difference between the two side contributions;
Brinson effects retain their original definitions. See
[contribution definitions and checks](../docs/CONTRIBUTIONS.md).

## Efficient implementation

Both retain the master, reusable one-day raw/duplicate buffers, dense daily arrays, bounded current/queued index days, and required active accumulators. They do not retain year-scale return records or output tables.

Rust uses `csv` reusable records, `Arc` index sharing, native portfolio workers, synchronous channels of capacity one, reusable output `ByteRecord`, and Ryū formatting. `--threads` limits worker batches; fewer workers than portfolios reread the index per batch. Four workers share one index pass for this dataset.

Go uses `encoding/csv` with `ReuseRecord`, buffered readers, dense slices, immutable shared day pointers and goroutines. `--threads` sets GOMAXPROCS, not the number of I/O goroutines. Output reuses an escaped byte buffer and `strconv.AppendFloat`.

Subset arrays use compact selected-ID slots. Country daily arrays use master-ID slots until security effects are computed;
only completed country accumulators use country slots. Full-history security/subset keep linked per-ID accumulators; weekly/monthly keep only portfolio period accumulators. The optimization reduces output and period state without discarding benchmark-only effects or changing the daily model.

## Correctness and full-history comparison

```sh
cargo test --manifest-path rust/Cargo.toml
go -C go test ./...
python3 python/test_attribution.py
python3 python/test_parquet_input.py
python3 python/benchmark_compact.py --runs 2
python3 python/render_stage_report.py
```

The independent oracle uses closed-form forward linking instead of the native recurrence. Nine scenario/period combinations cover mixed countries, changing holdings, missing-side and never-held benchmark instruments, standalone normalization, invalid inputs and zero selected weights.

The new benchmark uses two fresh complete repetitions per language/scenario, alternating language order. It compares **every field of every CSV row** across Rust/Go and compares logical counts and all three checksum components across repetitions. Use `--output-dir` with a fresh directory for a new benchmark; default output paths are not overwritten.

Metrics distinguish logical `report_rows` from physical `output_rows`; include checksum components, rows read/retained, input groups, daily effect values, parsed input bytes, attribution bytes, five stage durations, OS process wall/CPU time, peak RSS and average cores. Worker-stage elapsed times overlap and are not CPU times or additive wall time. Read includes parse/lookup/validation; aggregate includes normalization/grouping; compute includes linking/report planning/checksums; write includes attribution formatting, flush and fsync. Metrics file I/O, setup and channel waits are in overall process time. Throughput uses matching work counters.

For Parquet, read time includes batch decoding/decompression and typed value
access. `parsed_input_bytes` is zero for Parquet streams (CSV offsets have no
equivalent); in mixed runs it counts only CSV streams. `input_file_bytes` still
reports physical input sizes. No performance improvement is asserted by adding
the reader; compare identical holdings and comparable cache conditions first.

## Model limitations

The original synthetic generator used fixed all-US holdings; the current merged fixture contains US and DE securities. Both use calendar dates including weekends, and independent random returns for each entity. Thus the same instrument can have different portfolio/index returns. It does not model common security prices, transactions/cash flows or weight timing. These are implementation/performance tests, not validated investment-selection insights. Base/local separation is not a dedicated currency-attribution model. Real production attribution needs agreed pricing, timing and model conventions.

See [the five-scenario comparison](../ATTRIBUTION_STAGE_COMPARISON.md) for stage timings, output sizes and language winners.

## Output format

Each portfolio report uses its input extension: CSV in → CSV out, Parquet in →
Parquet out. This also applies per portfolio in mixed-format input directories.
The 20 columns, row counts, BF effects and contributions match across formats.
Metrics remain CSV. Native Parquet reports use typed Arrow builders and
Zstandard compression, with at most 65,536 rows per output batch/row group;
string labels, date32 period dates, uint32 Days and float64 numeric results.
The write stage includes encoding/compression, footer finalization and fsync.
