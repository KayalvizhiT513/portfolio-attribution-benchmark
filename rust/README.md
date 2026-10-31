# Native attribution: Rust and Go

Both implementations stream CSV inputs from `attribution_data/` and write per-portfolio CSVs. Existing Python and older Go benchmarks remain in `python/` and `go/`.

## Build and run

From the repository root, with stable Rust and Go 1.22+ installed:

```sh
cargo build --release --manifest-path rust/Cargo.toml
go -C go build -trimpath -ldflags='-s -w' -o ../benchmark-go-attribution ./cmd/attribution

./rust/target/release/portfolio-attribution --scenario country \
  --data attribution_data --output-dir results/rust-country \
  --metrics results/rust-country-metrics.csv --threads 4

./benchmark-go-attribution --scenario country \
  --data attribution_data --output-dir results/go-country \
  --metrics results/go-country-metrics.csv
```

The temporary Rust installation used for validation in this workspace is at `/tmp/attribution-cargo`, with toolchains at `/tmp/attribution-rustup`; it has not modified your shell profile. For this installation, prefix Cargo commands with `CARGO_HOME=/tmp/attribution-cargo RUSTUP_HOME=/tmp/attribution-rustup` and use `/tmp/attribution-cargo/bin/cargo`.

## Five scenarios

| `--scenario` | Grouping | Default period | Meaning |
|---|---|---|---|
| `security` | InstrumentId | day | Full union of portfolio and index securities |
| `subset` | InstrumentId | day | Standalone selected instruments, independently normalized on each side |
| `country` | Country | day | Daily country-level attribution |
| `weekly` | InstrumentId | week | Linked security effects for Monday–Sunday weeks |
| `monthly` | InstrumentId | month | Linked security effects for calendar months |

`--period day|week|month|quarter|year` overrides the default for any scenario, including country/subset. Calendar quarters and years are used. Partial periods are included; `PeriodStart` and `PeriodEnd` identify the observed first/last dates, not artificial calendar endpoints. `Days` counts observed effective dates, including weekends when present.

For a standalone subset, supply `--instruments-file path/to/subset.csv`. Its header is `InstrumentId`, followed by one exact instrument ID per row. The filter applies to portfolio and index independently, and their selected weights are each normalized to one on every date. Zero selected weight on either side fails explicitly. Instruments are not silently intersected: securities held on only one side remain in the union.

```sh
./rust/target/release/portfolio-attribution --scenario subset \
  --instruments-file results/native-benchmark/subset.csv \
  --output-dir results/rust-subset --metrics results/rust-subset-metrics.csv

./benchmark-go-attribution --scenario monthly --period quarter \
  --output-dir results/go-quarter --metrics results/go-quarter-metrics.csv
```

`--max-days N` reads the first N dates; zero (default) processes all dates. The new CLI replaces the earlier Rust `--group`/single `--output` interface. All five scenarios run with both base and local returns, separately; this is not a separate currency-attribution model.

## Attribution and linking

Within each group, return is weighted contribution divided by group weight. Brinson–Fachler daily effects use portfolio weight Wp, index weight Wb, group returns Rp/Rb, and total index return B:

- Allocation = (Wp − Wb) × (Rb − B)
- Selection = Wb × (Rp − Rb)
- Interaction = (Wp − Wb) × (Rp − Rb)

For a missing or zero-weight side, its group return is set to the held side's return. This assigns that group's effect to allocation without inventing a selection return. Each daily sum must reconcile to portfolio minus index return within 1e-9.

Source daily weights must sum to one within 1e-6. Full weights are then normalized too, removing rounding drift; subset weights use the selected-side denominator instead. This is a small precision change from the initial Rust version, which used rounded full weights directly.

Multi-period effects use forward (Frongello-style) linking. Let E be a group's accumulated component, P be portfolio growth before the current day, e be its current daily component, and b be the current total index return:

```
E_new = E_previous × (1 + b) + e × P
P_new = P × (1 + current portfolio return)
```

The recurrence applies separately to allocation, selection, and interaction, and separately to base/local returns. It handles equal portfolio/index returns without division. Period effects must reconcile to compounded portfolio return minus compounded index return within 1e-8. Returns must be finite and greater than −100%; negative weights are unsupported.

## CSV output

One output file per portfolio: `<scenario>-portfolio_<name>.csv`. Each period contains detail rows followed by `RowType=total`, `Segment=ALL` summary rows, for base and local returns.

Columns: `Scenario,Entity,PeriodStart,PeriodEnd,Days,RowType,Segment,ReturnType,PortfolioWeight,BenchmarkWeight,Allocation,Selection,Interaction,Total,PortfolioReturn,BenchmarkReturn,ActiveReturn`.

Weights are daily weights for daily rows and arithmetic mean daily weights for period rows, with absent groups counted as zero. Returns on detail rows repeat the whole portfolio/index period returns; do not sum them. Total rows also repeat the sum of detail effects; do not sum detail and total rows together. Numeric output preserves round-trip double precision. Native text formatting may differ without numeric disagreement.

Input files require the generator's exact headers. Dates must be valid increasing ISO dates, with rows for a date contiguous. Security IDs must be mapped and unique within each entity/date. Each file must contain one entity, and portfolio dates must match index dates. Output directories must differ from the input directory. Results are written to `.partial` files and published after all input/worker validations pass; failed runs can leave `.partial` files and any final files from earlier successful runs.

## Streaming and language-specific implementation

Both reuse one-day raw buffers and duplicate-check storage, use dense arrays keyed by the security master, aggregate weighted contributions, and broadcast bounded index days. Standalone subsets use compact dense slots for the selected IDs, so aggregation and effect computation scale with the selected universe rather than all master IDs. No year-scale security return table is retained. Period effects use one dense accumulator per portfolio. Memory is proportional to the master size × active portfolios and does not grow with the number of dates.

Rust uses the native `csv` parser with reusable records, `Arc` index sharing, bounded synchronous channels, native portfolio workers, reusable byte-record output, and Ryū float formatting. `--threads` limits concurrent portfolio workers. If fewer workers than portfolios are requested, portfolios run in batches and the index is reread once per batch; that extra input is counted in its metrics. At four workers, the supplied four portfolios share a single index pass.

Go uses `encoding/csv` with `ReuseRecord`, buffered readers, dense slices, shared immutable index days, and goroutines per portfolio. `--threads` sets GOMAXPROCS; concurrent I/O goroutines can outnumber it. Its output writer reuses an RFC 4180 escaped byte buffer and appends floats directly with `strconv.AppendFloat`. Neither implementation creates one task per security or per trivial effect.

## Verification and benchmarks

```sh
cargo test --manifest-path rust/Cargo.toml
go -C go test ./...
python3 python/test_attribution.py
python3 python/benchmark_native.py --days 3 --runs 3

# Full-history read/filter/country benchmark, manageable CSV outputs:
python3 python/benchmark_native.py --days 0 --runs 2 \
  --scenarios country subset --output-dir results/native-full-history
```

The independent oracle covers 105 dates across week/month/quarter/year boundaries, mixed countries, changing holdings, missing-side instruments, independently normalized subsets, and invalid inputs. It uses a closed-form linking calculation instead of copying the native recurrence. Benchmarks compare every numeric CSV field, record per-run stage timings, OS CPU time, peak RSS, and average cores, and report medians/ranges with independently configurable Rust and Go concurrency settings.

See `results/native-benchmark/REPORT.md`, `runs.csv`, `summary.csv`, and `environment.json`. Stage durations are summed worker elapsed time, not CPU time or end-to-end stage latency; stages overlap. Read includes parse/key lookup/validation; filter measures selection; aggregate includes normalization and weighted group construction; compute includes attribution/linking/reconciliation; write includes formatting, flush, and fsync. Master setup and pipeline waits are accounted for in overall process wall time. Per-stage throughput uses matching work counters; source bytes consumed are distinguished from total file sizes.

The supplied holdings are all US/USD, so country grouping has one group. The full daily security view produces approximately 876 million detail rows, around 180 GiB per language at the observed CSV width. The complete weekly/monthly views also produce large files. The default benchmark intentionally measures three dates for all five scenarios, labels that scope, and reuses output paths between repetitions. Only run full security output on a filesystem with sufficient space. Full-history country/subset runs exercise all input rows without that output expansion.
