# Streaming attribution with compact final reports

Rust and Go implement the same attribution contracts with native CSV streaming, bounded immutable index broadcasts and independent portfolio workers. Python provides independent correctness checks and benchmark orchestration.

## Output contracts

| Scenario | Default period | Final CSV grain | Rows in supplied data |
|---|---|---|---:|
| security | all dates | Portfolio-held instrument / portfolio / return type, plus benchmark-only bucket | 120,000 detail + 8 reconciliation |
| subset | all dates | Selected portfolio-held instrument / portfolio / return type, plus benchmark-only bucket | 800 detail + 8 reconciliation |
| country | all dates | Country / portfolio / return type | 8 |
| weekly | Monday–Sunday | Portfolio / week / return type | 1,264 |
| monthly | calendar month | Portfolio / month / return type | 288 |

Calculation granularity and output granularity are independent. Security/subset compute using the full applicable portfolio/index union. IDs appearing in a portfolio holding record on any date in the reporting range get detail rows. IDs never appearing in that portfolio are combined into one `RowType=benchmark_only`, `Segment=BENCHMARK_ONLY` row per return type. These rows contain their linked effects and average index weights; they are required for complete reconciliation. Do not renormalize the displayed held-only benchmark weights. There are no duplicate portfolio-total CSV rows for security/subset/country.

Weekly/monthly calculate daily security effects, **sum each component to the portfolio level first**, then link the portfolio components inside the current period. Their period accumulator has a single portfolio entry, and their CSVs contain `RowType=total`, `Segment=ALL`. Daily/security observations are not saved as output history.

Both base and local returns are calculated separately. Periods include all supplied effective dates, including weekends and partial calendar boundaries. `PeriodStart/End` are observed dates. On group rows, weights are arithmetic mean daily weights (zero on absent days); period returns are whole-portfolio/index compounded returns repeated across rows. Sum effect fields, not repeated portfolio-return fields.

## Build and run

From the repository root, with stable Rust and Go 1.22+:

```sh
cargo build --release --manifest-path rust/Cargo.toml
go -C go build -trimpath -ldflags='-s -w' -o ../benchmark-go-attribution ./cmd/attribution

./rust/target/release/portfolio-attribution --scenario security \
  --data attribution_data --output-dir results/rust-security --metrics results/rust-security-metrics.csv

./benchmark-go-attribution --scenario monthly \
  --data attribution_data --output-dir results/go-monthly-compute --metrics results/go-monthly-compute-metrics.csv
```

The temporary validation toolchain is under `/tmp`: use `CARGO_HOME=/tmp/attribution-cargo RUSTUP_HOME=/tmp/attribution-rustup /tmp/attribution-cargo/bin/cargo` if Rust is not on PATH. This installation did not change the shell profile.

Every execution calculates, links, validates and writes its compact attribution CSV report, plus a metrics CSV. There is no execution-mode option.

The default is all input dates (`--max-days 0`). `--max-days N` is available for explicit tests. `--period all|day|week|month|quarter|year` overrides the reporting period; defaults follow the table above. Scenario controls output grain independently of the period. Weekly/monthly produce portfolio-level rows; security/subset/country retain their groups.

For subset, pass `--instruments-file path.csv` with exact header `InstrumentId`, one ID per record. Filter both sides independently and normalize their selected weights separately every day. Zero selected weight on either side fails. The benchmark selects the union of the first 100 instruments in each portfolio: `S000001–S000100`, `S015001–S015100`, `S030001–S030100`, `S045001–S045100`. Each portfolio has 100 selected holdings; the selected index has 400.

## Calculation and validation

For each day/group and return type, normalize weights and compute weighted return contributions. Divide each group contribution by its weight to obtain its return. With portfolio/index group weights Wp/Wb, group returns Rp/Rb, and whole applicable index return B:

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

Input headers must exactly match the generator. Validate one entity per file, valid increasing grouped ISO dates, mapped unique instrument IDs per date, finite nonnegative weights, finite returns greater than −1, daily full weights within 1e-6 of one, and matched portfolio/index dates. All full weights are normalized again to remove CSV rounding drift. Compact files are first written as `.partial`; final names are published only after all workers validate. A failed run can leave partial files; files already present in the chosen directory are not cleared. Use fresh benchmark directories.

Output columns remain:

```
Scenario,Entity,PeriodStart,PeriodEnd,Days,RowType,Segment,ReturnType,
PortfolioWeight,BenchmarkWeight,Allocation,Selection,Interaction,Total,
PortfolioReturn,BenchmarkReturn,ActiveReturn
```

## Efficient implementation

Both retain the master, reusable one-day raw/duplicate buffers, dense daily arrays, bounded current/queued index days, and required active accumulators. They do not retain year-scale return records or output tables.

Rust uses `csv` reusable records, `Arc` index sharing, native portfolio workers, synchronous channels of capacity one, reusable output `ByteRecord`, and Ryū formatting. `--threads` limits worker batches; fewer workers than portfolios reread the index per batch. Four workers share one index pass for this dataset.

Go uses `encoding/csv` with `ReuseRecord`, buffered readers, dense slices, immutable shared day pointers and goroutines. `--threads` sets GOMAXPROCS, not the number of I/O goroutines. Output reuses an escaped byte buffer and `strconv.AppendFloat`.

Subset arrays use compact selected-ID slots. Country arrays use country slots. Full-history security/subset keep linked per-ID accumulators; weekly/monthly keep only portfolio period accumulators. The optimization reduces output and period state without discarding benchmark-only effects or changing the daily model.

## Correctness and full-history comparison

```sh
cargo test --manifest-path rust/Cargo.toml
go -C go test ./...
python3 python/test_attribution.py
python3 python/benchmark_compact.py --runs 2
python3 python/render_stage_report.py
```

The independent oracle uses closed-form forward linking instead of the native recurrence. Nine scenario/period combinations cover mixed countries, changing holdings, missing-side and never-held benchmark instruments, standalone normalization, invalid inputs and zero selected weights.

The new benchmark uses two fresh complete repetitions per language/scenario, alternating language order. It compares **every field of every compact CSV row** across Rust/Go and compares logical counts and all three checksum components across repetitions. Use `--output-dir` with a fresh directory for a new benchmark; default output paths are not overwritten.

Metrics distinguish logical `report_rows` from physical `output_rows`; include checksum components, rows read/retained, input groups, daily effect values, parsed input bytes, attribution bytes, five stage durations, OS process wall/CPU time, peak RSS and average cores. Worker-stage elapsed times overlap and are not CPU times or additive wall time. Read includes parse/lookup/validation; aggregate includes normalization/grouping; compute includes linking/report planning/checksums; write includes attribution formatting, flush and fsync. Metrics file I/O, setup and channel waits are in overall process time. Throughput uses matching work counters.

## Model limitations

The synthetic generator uses fixed holdings, one all-US country, calendar dates including weekends, and independent random returns for each entity. Thus the same instrument can have different portfolio/index returns. It does not model common security prices, transactions/cash flows or weight timing. These are implementation/performance tests, not validated investment-selection insights. Base/local separation is not a dedicated currency-attribution model. Real production attribution needs agreed pricing, timing and model conventions.

See [the five-scenario comparison](../ATTRIBUTION_STAGE_COMPARISON.md) for stage timings, output sizes and language winners.
