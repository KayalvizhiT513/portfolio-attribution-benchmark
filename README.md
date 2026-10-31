# Portfolio attribution: Rust and Go

Both implementations stream daily input CSVs, calculate and link attribution, and write final reports. Python provides independent correctness checks and benchmark orchestration.

| Scenario | Final output | Rows across four portfolios and both return types |
|---|---|---:|
| Security | Full-history portfolio-held security detail + benchmark-only reconciliation | 120,008 |
| Standalone subset | Full-history selected held-security detail + reconciliation | 808 |
| Country | Full-history country summaries | 8 |
| Weekly | Linked portfolio summaries per Monday–Sunday week | 1,264 |
| Monthly | Linked portfolio summaries per calendar month | 288 |

Every execution reads, calculates, validates, and writes the contracted CSV report. All benchmarks process all 1,095 dates.

- [Five scenarios, SVG flowcharts, stage metrics and winners](ATTRIBUTION_STAGE_COMPARISON.md).
- [Output sizes by scenario and language](ATTRIBUTION_STAGE_COMPARISON.md#overall-results-and-output-sizes).
- [Go vs Python benchmark measurements](GO_VS_PYTHON_BENCHMARK_REPORT.md).
- [Build instructions, CLI, formulas and limitations](rust/README.md).

## Build

```sh
cargo build --release --manifest-path rust/Cargo.toml
go -C go build -trimpath -ldflags='-s -w' -o ../benchmark-go-attribution ./cmd/attribution
```

## Validate and benchmark

```sh
cargo test --manifest-path rust/Cargo.toml
go -C go test ./...
python3 python/test_attribution.py
python3 python/benchmark_compact.py --runs 2
python3 python/render_stage_report.py
python3 python/render_attribution_flows.py
```

Use a fresh `--output-dir` for another benchmark execution. Generated attribution CSVs remain local; metrics and reports are retained separately. Sources are in `python/`, `go/` and `rust/`; inputs are in `attribution_data/`.

The synthetic dataset has fixed, disjoint portfolio holdings, a shared index and US/USD holdings only. Entity returns are independently generated. This measures computational correctness and performance; real investment analysis needs agreed pricing, timing and attribution conventions.
