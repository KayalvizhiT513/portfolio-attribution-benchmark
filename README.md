# Portfolio attribution: Rust and Go

Both implementations stream CSV or Parquet inputs, calculate and link attribution, and write final reports in the portfolio input format (CSV or Parquet). Python provides independent correctness checks and benchmark orchestration. Parquet is decoded into typed Apache Arrow batches before entering the existing daily calculation engine.

[Current concise CSV/Parquet benchmark report](CSV_PARQUET_REPORT.md) — full
history with the 60,000-security portfolio, security outer-join detail, US daily
country attribution and contribution columns. Updated as the measurements finish.

[Country-only report](COUNTRY_REPORT.md) — daily US results, stage times, supporting metrics and flow diagram.

Historical benchmark counts below describe the original four-portfolio fixture.
Current CSV and Parquet inputs both use the merged portfolio described below.
See [current scenario row counts](docs/OUTPUT_ROW_COUNTS.md), including daily
US-only country attribution (2,190 rows). All scenarios also include
[portfolio, benchmark and active contribution columns](docs/CONTRIBUTIONS.md)
alongside Brinson effects.

| Scenario | Final output | Rows across four original portfolios and both return types |
|---|---|---:|
| Security | Full-history portfolio-held security detail + benchmark-only reconciliation | 120,008 |
| Standalone subset | Full-history selected held-security detail + reconciliation | 808 |
| Country | Full-history country summaries | 8 |
| Weekly | Linked portfolio summaries per Monday–Sunday week | 1,264 |
| Monthly | Linked portfolio summaries per calendar month | 288 |

Every execution reads, calculates, validates, and writes the contracted report in its input format. All benchmarks process all 1,095 dates.

- [Five scenarios, SVG flowcharts, stage metrics and winners](ATTRIBUTION_STAGE_COMPARISON.md).
- [Output sizes by scenario and language](ATTRIBUTION_STAGE_COMPARISON.md#overall-results-and-output-sizes).
- [Go vs Python benchmark measurements](GO_VS_PYTHON_BENCHMARK_REPORT.md).
- [Build instructions, CLI, formulas and limitations](rust/README.md).

## Build

Use stable Rust and Go 1.25 or newer.

```sh
cargo build --release --manifest-path rust/Cargo.toml
go -C go build -trimpath -ldflags='-s -w' -o ../benchmark-go-attribution ./cmd/attribution
```

## Validate and benchmark

```sh
cargo test --manifest-path rust/Cargo.toml
go -C go test ./...
python3 python/test_attribution.py
python3 python/test_parquet_input.py
python3 python/benchmark_compact.py --runs 2
python3 python/render_stage_report.py
python3 python/render_attribution_flows.py
```

Use a fresh `--output-dir` for another benchmark execution. Generated attribution CSVs remain local; metrics and reports are retained separately. Sources are in `python/`, `go/` and `rust/`; inputs are in `attribution_data/`.

The current synthetic dataset has one merged portfolio and index, with exactly 1,000 shared DE/EUR securities and no US overlap. Entity returns are independently generated. This measures computational correctness and performance; real investment analysis needs agreed pricing, timing and attribution conventions.

## Parquet inputs

`attribution_data_parquet/` contains the merged portfolio and index described
below. `attribution_data/` contains identical typed values in CSV format.
The benchmark results above describe the earlier four-portfolio fixture.

`python/convert_attribution_parquet.py` created the intermediate four-portfolio
Parquet inputs, replacing 1,000 US IDs per portfolio with the 1,000 DE/EUR IDs.
`python/merge_attribution_portfolios.py` then merged and remapped those inputs.
The existing Parquet folder now contains the final merged dataset.

Both native attribution binaries automatically discover `.csv` and `.parquet`
inputs, including mixed-format directories. For example:

```sh
./rust/target/release/portfolio-attribution --data attribution_data_parquet \
  --scenario country --output-dir results/parquet-rust --metrics results/parquet-rust/metrics.csv
./benchmark-go-attribution --data attribution_data_parquet \
  --scenario country --output-dir results/parquet-go --metrics results/parquet-go/metrics.csv
```

See [Parquet input architecture and validation](docs/PARQUET_INPUT.md). The old
benchmark measurements above remain CSV results, and the EUR-modified Parquet
fixture differs from the historical four-portfolio holdings. Existing benchmark
harnesses use those historical portfolio/count assumptions.

## Merged portfolio with DE-only index overlap

`attribution_data_parquet/` contains one portfolio,
`portfolio_PF.parquet`, plus its index and unchanged security master.
Each source portfolio contributes 25% after daily normalization. Shared EUR
holdings are consolidated by summing weights and taking weight-averaged base
and local returns. The merged portfolio has 60,000 unique securities per date:
59,000 US/USD and 1,000 DE/EUR.

The merged portfolio's US IDs cover `S100001–S159000`, outside the
source index. The index replaces `S099001–S100000` with `S159001–S160000`,
retaining the replaced rows' weights and returns. It therefore has 99,000
US/USD and 1,000 DE/EUR securities. Exactly the 1,000 IDs whose master country
is DE are shared with the merged portfolio on every date. These identity changes preserve synthetic
returns rather than regenerate price or FX data.

`instrument_remapping.parquet` records all changed IDs. `manifest.json` records
counts, merge rules, file sizes and verification. All 1,095 output dates are
reloaded to check holdings, the exact EUR-only intersection, weights and
source-derived weighted returns. The four old Parquet portfolio files were
replaced in this same folder; the extra merged folder was removed. The CSV
folder has also been synchronized in place, replacing its four old portfolio
files with `portfolio_PF.csv`. Every parsed CSV value was checked against
its typed Parquet source. Synchronize CSVs again with:

```sh
.venv/bin/python python/export_attribution_csvs.py
```

Both native attribution binaries can use `--data attribution_data_parquet`.

The portfolio was expanded from 56,000 to 59,000 US IDs by splitting 3,000
existing US holdings equally between their original IDs and new IDs
`S156001–S159000`, keeping the same base/local returns on each pair. This
preserves daily total weight and weighted returns. Split provenance is recorded
in `attribution_data_parquet/expansion_provenance.parquet`. The expansion and
CSV/Parquet verification are implemented in `python/expand_portfolio_us.py`.
