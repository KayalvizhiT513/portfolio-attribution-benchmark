# Cumulative US Total statistics

The existing Go and Rust runners dispatch `--scenario cumulative` to
`go/cmd/attribution/cumulative.go` and `rust/src/cumulative.rs`. Cumulative uses
only the shared attribution daily reader. The experimental independent reader,
its CLI selector and benchmark reader-selection option have been removed.

## Build and run

```sh
go -C go build -o ../benchmark-go-attribution ./cmd/attribution
cargo build --release --manifest-path rust/Cargo.toml
./benchmark-go-attribution --scenario cumulative --data attribution_data_parquet \
  --output-dir results/cumulative-go --metrics results/cumulative-go/metrics.csv
./rust/target/release/portfolio-attribution --scenario cumulative --data attribution_data_parquet \
  --output-dir results/cumulative-rust --metrics results/cumulative-rust/metrics.csv
```

Both use automatic CPU parallelism. `--threads N` overrides the read-pool size;
`--effective-date-workers N` overrides month-end workers (capped at 36 per
portfolio). Omit both to leave concurrency automatic.

## Pipeline

1. Load and validate the security master once and share its immutable ID mapping
   and effective-dated country history among workers.
2. Discover complete-date CSV byte ranges or Parquet row-group ranges using the
   shared attribution planners. Match the date ranges across all input files.
3. Process paired date-range tasks with the existing `stream.next` / `Stream::next`
   reader, alternating index and portfolio reads per date. This reuses attribution's
   validation, effective-dated US filtering and normalized security buffers.
4. Sum those buffers into daily US Total returns and retain compact daily results.
   No BF effects are computed. If complete-date ranges are unavailable, the same
   reader handles whole files and validates portfolio/index date alignment.
5. Sort daily totals and validate entity/date consistency. Skip return observations
   when neither side has US exposure; reject one-sided US absence.
6. Share the immutable aligned US returns with 36 independent month-end tasks,
   then write rows in effective-date order.

With 12 workers and the supplied single portfolio, there are 24 paired date-range
read tasks and 36 month-end calculation tasks. The read and calculation pools
execute in successive stages. No bounded index broadcasts are required.
The pure calculation functions `cumulativeRows` / `cumulative_rows` are separate
from I/O for use by a future service handler; no HTTP endpoint is added here.

## Calculation contract

Each portfolio produces `cumulative-portfolio_ENTITY.csv`, with 36 rows covering
2023-01-31 through 2025-12-31. Columns include Entity, Level=Total, PeriodStart,
EffectiveDate, Observations, LastObservationDate, and the five statistics for both
Base and Local returns. Output is CSV for either input format.

Input coverage starts at 2023-01-01 and extends into December 2025. All aligned
US daily observations through each month-end are included, including weekends.
The supplied data ends on 2025-12-30, so the final month-end uses 1,095 observations
and records LastObservationDate=2025-12-30. Missing observations are not filled
with zero returns. An empty US window has zero observations, blank last observation
and blank statistics. PeriodStart remains 2023-01-01.

Daily returns use only holdings whose country is US as of the holdings date.
Each side is normalized separately by its selected US weight sum. Security overlap
is not required. With portfolio return p, benchmark return b and active a=p-b:

- Portfolio standard deviation: sample SD(p), denominator n-1.
- Benchmark standard deviation: sample SD(b), denominator n-1.
- Tracking error: sample SD(a), denominator n-1.
- Information ratio: mean(a) / tracking error.
- Beta: sample covariance(p,b) / sample variance(b).

No annualization is applied. Stable centered moments compute variance and
covariance. Undefined ratios and statistics with fewer than two observations
are blank. Returns and standard deviations use decimal units.

## Timing and cold-cache runs

```sh
sudo -v
.venv/bin/python python/benchmark_cumulative.py --cache macos-purge --runs 1 \
  --output-dir results/cumulative-us-cold-new
```

Use a fresh output directory. One repetition runs Go CSV, Rust CSV, Go Parquet
and Rust Parquet sequentially. Each run has its own purge, outside measured time,
and automatic core counts. Matching effective-dated CSV/Parquet masters are
prepared and verified before measurement. The wrapper removes inherited
GOMAXPROCS and ATTRIBUTION diagnostic overrides during measurements.

Reports: `RUNTIME_SPLIT.md`, `summary.json`, `runs.json`, `runs.csv`, `checks.json`
and per-run `metrics.csv`, `usage.json`, `run.log`. Output folders use
`<format>/<language>/<repetition>/`. Repetitions can be increased with `--runs N`.

`read_pipeline_wall_seconds` directly measures elapsed read-pool processing.
It includes reading, validation, classification, US filtering, normalization,
security-buffer reduction into daily totals and result collection. These operations
are not separately measured in elapsed time. `read_filter_aggregate_seconds`
sums parallel task durations and must not be added to elapsed phases.
Master loading, boundary discovery, month-end calculation and writing have separate
timers. The report's other time is the residual covering setup, alignment and
finalization. Cache preparation is excluded; process elapsed includes startup/exit.

The completed purged attribution-reader evidence is under
`results/cumulative-attribution-reader-cold-2026-11-03/`. Its runtime split is
generated from those saved metrics; it is not a new benchmark after code cleanup.
Historical independent-reader results remain as archived evidence.

## Validation

```sh
go -C go test ./cmd/attribution
cargo test --manifest-path rust/Cargo.toml
.venv/bin/python python/test_cumulative.py
.venv/bin/python python/test_attribution.py
```

The cumulative oracle checks 2,880 statistics across both engines, CSV/Parquet,
serial/parallel readers and serial/parallel month-end workers. It covers US country
changes, excluded non-US returns, US weight normalization, empty dates and leap
years, and rejects 28 invalid inputs. Native tests cover zero denominators and
empty US windows. Existing attribution reference tests use the same binaries.
