# Daily-snapshot attribution specification and archived results

Recorded 2026-11-08, Asia/Kolkata.

**Historical contract:** sector measurements in this archive predate the switch
to sector-level Brinson calculation with combined sector/security rows and Count.
Those historical reports sum security effects by sector. See the [current sector
contract](../../DAILY_SMF_SNAPSHOTS.md#sector-aggregation-rust) for the new behavior.
 This archive preserves contracts, run dates,
commands, metrics and rebuild dependencies before generated bulk data is removed.
See [all API results by run date](RESULTS.md), the machine-readable
[results catalog](results.json), [environment](environment.json), and
[retained source fingerprints](canonical-inputs.json). Original run evidence is
under `runs/` (worker profiles and small report rows are gzip-compressed); the
current code snapshot is under `code/`, with `python-packages.txt` for package
versions. Older implementation versions are not reconstructed by that snapshot; input manifests are under `input-manifests/`.

## Input specification

Each worker reads six Parquet files. Required values are non-null. Names and
Arrow types matter; column order does not. Row groups contain a single date,
with min/max statistics, ordered by date. A date may span adjacent groups.

| File | Columns and Arrow types | Key / semantics |
|---|---|---|
| accounts.parquet | AccountId string, EffectiveDate date32, PortfolioEntity string, BaseCurrency string, IsActive bool | Unique account/date and portfolio entity/date; active account required |
| forex.parquet | EffectiveDate date32, FromCurrency string, ToCurrency string, Rate float64 | Unique date/currency pair; positive finite quote in ToCurrency units per FromCurrency unit |
| security_master.parquet | InstrumentId string, EffectiveDate date32, Currency string, Country string, AnalyticsSecId int64; SnapshotDate date32 for daily snapshots; optional Sector string | InstrumentId + SnapshotDate unique in snapshots; EffectiveDate is the original last-change date and cannot exceed SnapshotDate |
| analytics.parquet | SecId int64, **Effective Date** date32, Float01…Float15 float64 | Unique SecId/date; finite values; synthetic analytics aliases may be shared by several instruments |
| portfolio_PF.parquet | Entity string, EffectiveDate date32, InstrumentId string, Weight float64, BaseReturn float64, LocalReturn float64 | Unique entity/date/instrument; daily source weights sum to one |
| index.parquet | Same as portfolio_PF | Benchmark holdings, independently generated |

Full benchmark window: **2023-01-01 through 2025-12-28**, inclusive,
**1,093 consecutive dates**. Physical returns also contain December 29–30,
excluded because analytics ends December 28. Portfolio: 60,000 securities/date.
Benchmark: 100,000/date. SMF universe: 160,000, producing 174,880,000 daily snapshot
rows. Analytics contains 365,000,000 physical rows: 100,000 securities × 3,650
dates from 2016-01-01; requests select only their calculation dates.

Synthetic analytics mapping: `((numeric InstrumentId - 1) % 100000) + 1`.
Accounts and FX are joined and validated. Analytics values are checksummed and
validated. Neither rewrites the supplied BaseReturn/LocalReturn values.

For each date, keep US instruments, renormalize portfolio and benchmark weights
separately, align their security universe, and calculate security-level effects.
The portfolio and benchmark may contain different securities, and membership can
change across dates. Missing snapshot, analytics or FX coverage is an error.
No shared parsed-snapshot cache exists; each worker owns its current snapshot.
Server-wide sharing retains immutable footer/date-plan metadata only.

### SMF variants

- Monthly-staggered history: every security updates once per calendar month,
  on `1 + ((numeric ID - 1) % 28)`. December 2022 seeds initialize January 2023.
  Countries/currencies vary according to `prepare_monthly_smf.py`.
- Daily snapshots: latest full state as of each return date, preserving the
  source EffectiveDate. No earlier snapshot replay is required.
- Fixed synthetic sectors: 11 sector names, assigned by ID modulo 11.
- Effective-dated synthetic sectors: one revision/security/year, on month
  `1 + ((ID - 1) % 12)` and its existing monthly effective day. This produces
  **480,000 changes**, exactly three per security in the three-year window.
  It is a synthetic stress workload, not an industry frequency assumption.

[The independent replay validation](input-manifests/six-input/monthly-staggered-sector-dated-snapshots/dated-sector-validation.json)
checks all 174,880,000 snapshot sectors and effective dates against source history.

## Output specification

The API accepts `POST /attribution` with only `start_date` and `end_date` strings,
formatted YYYY-MM-DD. Worker counts, grouping and return type are server options.
HTTP 200 includes request ID, elapsed seconds, report path, output directory,
metrics, and admission/calculation timing. Validation/calculation failures return
an error; the server uses isolated per-request directories.

`country-portfolio_PF.parquet` is the current report filename for all four modes;
the filename does not imply that aggregate rows are grouped by country.

| Output columns | Arrow type | Meaning |
|---|---|---|
| Scenario, Entity | string | `country` for daily; `arithmetic_sum` for aggregation; portfolio entity |
| PeriodStart, PeriodEnd | date32 | Daily date or aggregate window bounds |
| Days | uint32 | 1 for daily rows; whole calculation-window length for aggregate rows, not security presence count |
| RowType, Segment, ReturnType | string | detail/total; country, ALL, security ID or sector name; base/local |
| PortfolioWeight, BenchmarkWeight | float64 | Daily normalized weights or arithmetic sums of those weights across dates |
| Allocation, Selection, Interaction, Total | float64 | Security-level Brinson effects rolled up; Total equals the three effects' sum |
| PortfolioReturn, BenchmarkReturn, ActiveReturn | float64 | Portfolio-wide daily values, or arithmetic sums over dates of group presence; **not compounded returns** |
| PortfolioContribution, BenchmarkContribution, ActiveContribution | float64 | Daily security contributions or arithmetic sums; active equals portfolio minus benchmark |

| Mode | Retained worker state | Output ordering | Full benchmark rows |
|---|---|---|---:|
| daily | Country report per date | Date | 2,186 (US × base/local × 1,093) |
| total | One directly accumulated total | Return type | 1 for base; 2 for both |
| security | Direct sums keyed by stable security ID | ID, return type | 159,000 for base in the original fixture |
| sector | Direct sums keyed by the sector effective on each date | Sector, return type | 11 for base |

Aggregate defaults to base; `--return-type local|both` selects alternatives. Daily
retains both return types. Aggregation sums effects without cross-date compounding
or linking. Sector mode sums security effects, rather than recalculating Brinson
at the sector level. Effects, contributions and weights reconcile vertically to
total. Portfolio-wide return columns are repeated across groups and must not be
summed vertically. An ID classified in different sectors across dates contributes
to the appropriate sector on each date.

All modes write once to `.partial`, finish the Parquet footer, flush/sync, then
rename to the final path. There is no second data-copy write. Workers finish
before final output; aggregate worker sums are merged by key. The direct
aggregation optimization removes daily security-result arrays and cloned ID
lists. It does not remove temporary-file finalization.

## Metrics specification

| Metric / artifact | Unit / scope | Interpretation |
|---|---|---|
| API response `seconds` in summary | Seconds, client wall time | Complete timed request, including admission, calculation, writing and response |
| `elapsed_seconds` in response body | Seconds, server wall time | Server calculation/request span; see request admission fields |
| `server_ready_seconds` in startup.json | Seconds, outside API timing | Process launch, shared metadata load and readiness polling; no warmup attribution request |
| peak_sampled_rss_bytes | Bytes, entire server | Largest sampled RSS across the run, not exact allocator/live-object peak; convert using 1024^3 for GiB |
| planning_seconds | Seconds, wall | Metadata, window validation and partition planning |
| worker_phase_seconds | Seconds, wall | Workers, input reading, calculation, accumulation and worker-map merging; merge not separately isolated |
| report_write_seconds | Seconds, wall | Output setup/order where applicable, encoding, finishing, sync and rename |
| metrics_write_seconds | Seconds, wall | Write/flush metrics.csv; stage-profile JSON itself is outside this span |
| *_rows_decoded | Rows | Physical input rows decoded; not logical SMF rows served |
| *_read_decode_seconds | Summed elapsed seconds | Input-reader spans; can overlap across workers and inputs; not disk-only time or CPU time |
| smf_snapshot_rows_served | Rows | Logical current-state rows presented to calculations |
| smf_snapshot_loads / smf_cache_hits | Counts, legacy names | In direct daily snapshots every date loads and hits are zero; older history mode reused private state on dates without changes |
| joined_us_securities | Count, summed over dates | Union of retained portfolio/benchmark securities validated for analytics/FX joins |
| analytics_checksum / fx_checksum | Sums | Diagnostic input coverage checks, not return calculations |
| date_timing worker events | Seconds, elapsed per worker/date | Input, lookups, holdings processing, joins, attribution/accumulation, cleanup and residual |
| Summed worker stage times | Summed elapsed seconds | Workers overlap; **do not add these values to wall-clock phases** |

`attribution_seconds` includes direct accumulation in optimized aggregate runs.
Older aggregate profiles charged a separate accumulation pass mostly to
`date_other_seconds`; comparisons across implementation versions need this
qualification. CPU scheduling and I/O waits are included in elapsed spans.
Cold-cache labels mean **the user confirmed purge before the run**; OS cache
state was not independently verified. One run per mode is an observation, not a
statistical performance guarantee. Additional sector-column input costs are part
of sector runs.

## Latest comparable results

All runs below used 12 workers, one request, no warmup, a user-confirmed purge
before each request, and temporary-file finish/sync/rename. Precise run dates and
timestamps are in [the catalog](RESULTS.md). All occurred on **2026-11-08 IST**.

| Mode / SMF | API seconds | Peak RSS GiB | Rows | Validation |
|---|---:|---:|---:|---|
| Daily / no sector | 13.046 | 1.330 | 2,186 | Matches prior daily; base sums match total |
| Total / no sector | 12.859 | 1.331 | 1 | Matches daily additive columns |
| Security / no sector | 13.909 | 2.069 | 159,000 | Each row matches previous; sums match total |
| Sector / fixed sectors | 13.507 | 1.473 | 11 | Each sector matches independent security rollup; sums match total |
| Sector / effective-dated sectors | 14.381 | 1.467 | 11 | Sector distribution changed; sums still match total |

Earlier cache and intermediate-array experiments, including unsuccessful/stopped
runs, remain in results.json with their original commands and statuses. Removed
indexed/shared-cache options are **historical**, not supported by the current CLI.
Reproducing old implementation timings requires the corresponding old source;
this archive does not claim the current binary reproduces those timings.

## Rebuild

Keep `attribution_data_parquet/`: it contains the canonical merged/expanded
portfolio and index, undated master, remapping and expansion provenance. The exact
canonical return series cannot be promised from a new RNG invocation alone.
The small historical SMF is preserved in `source/security_master_history.parquet`.
SHA-256 hashes and schemas are in canonical-inputs.json. Retaining these inputs
is deliberate; a file manifest cannot replace their numerical values.

```sh
python3 -m venv .venv                    # only if the environment is absent
.venv/bin/pip install -r python/requirements.txt
cargo build --release --manifest-path rust/Cargo.toml
.venv/bin/python python/rebuild_daily_benchmark.py \
  --output results/rebuilt-daily --variant all
```

Use a **fresh** output directory. `--variant daily|fixed-sector|dated-sector|all`
chooses what to build. The recipe regenerates analytics with NumPy seed 42,
prepares six inputs, monthly history, then requested snapshots. All variants
share canonical returns and analytics through symlinks. Expect roughly 50 GiB
of regenerated inputs; sufficient disk space is required. Environment versions
can affect encoded bytes and timings; numerical correctness must be revalidated.

After rebuild and validation, purge, then run **one** mode:

```sh
.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/rebuilt-daily/dated-sector-snapshots \
  --output results/api/new-dated-sector-c1 --aggregate sector --return-type base \
  --start-date 2023-01-01 --end-date 2025-12-28 \
  --workers 12 --worker-limit 12 --request-limit 12 \
  --concurrency 1 --repeats 1 --settle-seconds 0 --stage-profile
```

For daily/total/security use `--data results/rebuilt-daily/daily-snapshots` and
select `--aggregate daily|total|security`. For fixed sectors use
`fixed-sector-snapshots`. Purge separately before each cold-cache run. The sampler
retains startup health, RSS, responses and per-request metrics. Small fixtures
are validated by test_daily_smf.py, test_aggregate_smf.py, test_sector_smf.py and
test_dated_sector_smf.py without rebuilding the 42 GiB analytics fixture.

The older daily-heavy source can be recreated from `six-input` with
`prepare_sparse_smf.py`, then `prepare_daily_smf.py`; its original manifests are
archived. Other historical workloads use their existing reports and generator
scripts. Large legacy `data/` source CSVs were retained because their exact
inline-segment schema/seed recipe is not verified by current generators.

Redundant attribution CSVs can be exported from the retained Parquet inputs via
`python/export_attribution_csvs.py`. That exporter emits the undated master;
for the historical CSV scenario, replace its master with a CSV export of the
archived historical SMF instead. This preserves values, not necessarily the
original CSV byte order/format. Fresh output checks are mandatory.

## Cleanup and retained evidence

`cleanup-*.json` journals list each selected file, size, prior mtime, Parquet
schema/row count or CSV header, and deletion status. Bulk generated files were
selected only when untracked and at least 20 MiB under results/; redundant
attribution_data CSVs were also selected. Canonical Parquet inputs, original
supplied inputs, legacy data/ sources, code, Git history, dependencies and small
reports/metrics/output files remain. Symlinks into deleted fixtures may remain;
use the fresh-path rebuild recipe, not those old input directories. Old absolute
paths in archived commands describe original runs, not current file availability.

Full raw historical rows that were deleted cannot be queried without rebuilding
and rerunning. Archived metrics, contracts and small outputs remain usable for
reference. The cleanup is not a claim that every older workload is exactly
regenerable. No tracked data file is deleted by the cleanup script.
