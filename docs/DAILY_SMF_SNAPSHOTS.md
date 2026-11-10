# Direct daily SMF snapshots

Rust's six-input reader supports complete daily snapshots prepared from sparse
change history. It detects snapshot mode explicitly
from the additional typed `SnapshotDate` column; snapshot files must not have
an adjacent security-history index. Go's reader is unchanged.

Snapshot schema:

- InstrumentId: string
- EffectiveDate: date32 — actual last-change date for this security
- Currency: string
- Country: string
- AnalyticsSecId: int64
- SnapshotDate: date32 — calculation/return date resolved by this snapshot

Rows are sorted by SnapshotDate, then InstrumentId. Row groups contain one
SnapshotDate; underlying EffectiveDate values can differ and cannot be later
than SnapshotDate. Workers load exactly their current SnapshotDate, discard the
previous parsed state, and build a private state for that date. They never replay
earlier snapshots. Missing
snapshot dates fail, rather than silently using a previous date. The supplied
prepared snapshots contain all securities known as of that date; they do not
introduce securities whose first source record is in the future.

Preparation performs one chronological pass over source changes, carries forward
unchanged securities, and emits one complete snapshot for each shared portfolio/
index return date inside the source manifest's computation window. The supporting
account, FX and analytics files must cover every selected date. Source files are
preserved; a fresh output directory is required.

```sh
.venv/bin/python python/prepare_daily_smf.py \
  --source results/six-input/security-history-daily-heavy-inputs \
  --output results/six-input/daily-snapshot-inputs
```

Prepared full fixture: 160,000 securities per date, 1,093 dates, 174,880,000 rows,
907,406,144 Parquet bytes. Preparation took 49.316 seconds, separate from any API
latency. `preparation-validation.json` records footer checks; small fixtures
independently verify actual attributes against sparse source history.

Snapshots replace state on every date. Traces emit `snapshot_reset`, optional
`security_applied` records with their actual EffectiveDate and SnapshotDate,
`snapshot_loaded`, and `date_ready` with `snapshot_initialize`/`snapshot_replace`.
Snapshot inserts describe the complete state observed for a date, not newly
created securities in source history. Existing stage timings remain available;
`stage-profile.json` identifies `smf_reading_path: daily_snapshot`. Each computation
date decodes its full snapshot once, so the full single request decodes
174,880,000 SMF rows. Parsed states are never shared between workers or requests.

Tests completed before the requested purge:

```sh
.venv/bin/python python/test_daily_smf.py
.venv/bin/python python/test_security_index.py
.venv/bin/python python/test_six_inputs.py
cargo test --manifest-path rust/Cargo.toml --offline
```

These cover state resolution, real effective dates, new securities, future
exclusion, serial/parallel and late-window output parity, record-level cache
trace replay, and missing-snapshot/future-effective/duplicate-security rejection.
All full-file preparation and metadata checks are finished. No full snapshot
API benchmark has started.

After the user confirms their purge is finished, the next single-request run is:

```sh
.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/six-input/daily-snapshot-inputs \
  --output results/api/daily-snapshot-c1 \
  --workers 12 --worker-limit 12 --worker-trace --smf-trace summary \
  --stage-profile --concurrency 1 --repeats 1 --settle-seconds 3 \
  --timeout 3600 --interval 0.2
```

Do not run preparation, source validation or warm-up requests between that purge
and the timed run. The server's own metadata planning/validation remains part
of measured API latency. Record purge as user-reported, without claiming it was
independently verified. Compare outputs to the prior daily-heavy sparse result
and export the same per-worker initialization/processing/finish splits after
timing. The 20-request run remains stopped.

## Arithmetic aggregation across dates (Rust)

Use `--aggregate total` for one row across all selected dates and securities, or
`--aggregate security` for one row per security across the selected dates:

```sh
rust/target/release/portfolio-attribution --scenario six-input \
  --data /path/to/snapshots --output-dir results/snapshot-total \
  --metrics results/snapshot-total/metrics.csv --aggregate total
rust/target/release/portfolio-attribution --scenario six-input \
  --data /path/to/snapshots --output-dir results/snapshot-security \
  --metrics results/snapshot-security/metrics.csv --aggregate security
```

The existing US filter, daily weight normalization and security attribution
formulas apply. Workers immediately add daily values to private accumulators;
only these accumulators are merged after workers finish. Effects are accumulated
directly during calculation: no daily security result array or cloned security-name
list is built. Security IDs are allocated only on first use in each worker, using
a hash map; final output is sorted by ID. Daily reports are not retained in
aggregation modes. The attribution timing includes this direct accumulation. Memory scales with workers × unique securities
for security aggregation, and workers for total aggregation, in addition to
normal input/snapshot buffers. Final rows are written through the existing
on-disk temporary file and renamed on completion.

Aggregates default to `--return-type base`; use `local` or `both` to select other
output. `both` emits one row per return type for each aggregate key. The default
`--aggregate daily` preserves existing country output and both return types.
These options also apply to Rust API servers when supplied at server startup;
request bodies continue to contain only start/end dates.

`Scenario=arithmetic_sum` distinguishes these outputs from linked reports.
All numeric columns are arithmetic sums, including weights (not averaged).
PortfolioReturn, BenchmarkReturn and ActiveReturn are sums of daily
portfolio-wide values, not compounded returns. In security output those three
portfolio-wide columns are repeated for each security's dates of presence and
must not be summed across securities; effects and contributions do reconcile
across securities to the total row. PeriodStart/PeriodEnd and Days describe the
whole requested calculation window, including dates a given security is absent.
Missing securities contribute zero. There is no cross-date linking or compounding.

Validate with `.venv/bin/python python/test_aggregate_smf.py` after building Rust.

All three output modes write to a temporary report, finish and sync the file,
then rename it to the final report path. Interrupted writes leave an incomplete
temporary file rather than publishing it as the final report. The direct
aggregation optimization removes intermediate daily result arrays in memory;
it does not change this output finalization sequence.

## Sector aggregation (Rust)

SMFs can optionally include `Sector: string`. Values must be non-null and
nonempty. Sector mode requires the column; older inputs continue to work for
daily, total and security modes. Snapshot preparation preserves sectors and their
actual effective dates. Sector changes move subsequent effects to the new sector.

`--aggregate sector` now produces **sector-level Brinson attribution and security
metrics in the same report**. It does not sum security-level attribution effects
to obtain the sector allocation/selection/interaction split.

For each date, after the existing US filter and separate portfolio/benchmark
normalization:

1. Accumulate portfolio and benchmark sector weights and weighted return
   contributions separately using that date's SMF sector.
2. Calculate each sector's return as contribution / sector weight.
3. Calculate Brinson allocation, selection and interaction on those sector
   weights/returns against the date's overall benchmark return.
4. Independently retain existing security-level Brinson metrics, keyed by
   **sector + security ID**. A changing security can have multiple output rows.
5. Accumulate both levels across dates in worker-local state and merge after
   workers finish. Dates remain independent; no multi-period linking is added.

The existing twenty columns are retained. Sector mode appends:

| Column | Type | Meaning |
|---|---|---|
| Sector | string | Classification applicable on each contributing date |
| Security | string | Instrument ID on security rows; empty on sector rows |
| Count | uint32 | Distinct dates the sector or sector/security pair appears on either retained side |

`RowType` is `sector` or `security`; `Scenario` is `sector_attribution`. Sector
rows precede their security rows, sorted by sector and then security ID.
`Segment` repeats the sector name for sector rows and ID for security rows.
`Days` continues to describe the full requested window, while `Count` describes
actual group presence. Counts are union-date counts, not portfolio + benchmark
counts. A sector date counts once regardless of how many members it contains;
child counts must not be summed to obtain the parent count. Returning to an
old sector resumes the existing sector/security row.

Weights, effects, contributions and the group's **own daily returns** are summed
arithmetically across contributing dates. PortfolioReturn/BenchmarkReturn are
the sector's weighted constituent returns on sector rows, and the security's
returns on security rows, not repeated portfolio-wide returns. An absent side
has return zero in output. Effect calculation retains the established empty-side
convention: use the other side's return to avoid artificial selection/interaction.
`--return-type base|local|both` applies to both row types (default base).

Sector effects reconcile to the daily active return; security effects reconcile
separately. Sector allocation, selection and interaction can differ from summed
security effects. Weights, contributions and Total reconcile across levels.
Do not sum both RowTypes together: they are two representations of the same
portfolio/benchmark result. Report checksum metrics count sector effects only;
output-row counts include both levels.

This replaces the former sector rollup output. Archived benchmark measurements
and eleven-row reports describe the **old summed-security-effects version** and
are not measurements of the combined output. Daily, total and standalone security
modes retain their existing schema and behavior.

The benchmark has 11 **synthetic** sectors, assigned by numeric security ID modulo
11. They are benchmark classifications, not verified real-world classifications.
The existing snapshots remain unchanged; the sector-enriched dataset is separate:

```sh
.venv/bin/python python/prepare_sector_smf.py \
  --source results/six-input/monthly-staggered-daily-snapshot-inputs \
  --output results/six-input/monthly-staggered-sector-daily-snapshot-inputs
.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/six-input/monthly-staggered-sector-daily-snapshot-inputs \
  --output results/api/sector-c1 --aggregate sector \
  --workers 12 --worker-limit 12 --request-limit 12 \
  --concurrency 1 --repeats 1 --settle-seconds 0 --stage-profile
```

For cold-cache measurements, purge after preparation and validation, before the
API run. The extra column changes input decoding cost, so comparisons with
sector-free inputs include both the sector input cost and grouping cost.
Validate with `.venv/bin/python python/test_sector_smf.py`.

### Effective-dated sector benchmark

The changing-sector fixture uses the same monthly history and 11 synthetic sector
names, with one sector revision per security per year. The revision month is
`1 + ((numeric ID - 1) % 12)`; the effective day remains the source's monthly
update day, `1 + ((numeric ID - 1) % 28)`. Sectors stay unchanged between revision
events. This is a deterministic test workload, not an industry frequency claim.

Generate sectors on history records **before** resolving daily snapshots:

```sh
.venv/bin/python python/prepare_sector_smf.py \
  --source results/six-input/monthly-staggered-history \
  --output results/six-input/monthly-staggered-sector-dated-history \
  --effective-dated
.venv/bin/python python/prepare_daily_smf.py \
  --source results/six-input/monthly-staggered-sector-dated-history \
  --output results/six-input/monthly-staggered-sector-dated-snapshots
.venv/bin/python python/validate_dated_sector_smf.py \
  --history results/six-input/monthly-staggered-sector-dated-history \
  --snapshots results/six-input/monthly-staggered-sector-dated-snapshots
```

The validator independently replays history chronologically and checks every
snapshot's sector and EffectiveDate against the latest applicable record. It
also records sector-change counts and examples in `dated-sector-validation.json`.
Run `.venv/bin/python python/test_dated_sector_smf.py` for before/on/after event
boundary checks, future-state exclusion, trace replay, and serial/parallel runs.
Purge only after preparation and validation when measuring cold-cache latency.

### Limited-occurrence count fixture

For the next combined sector/security run, rebuild with:

```sh
.venv/bin/python python/rebuild_daily_benchmark.py \
  --output results/count600-rebuild --variant dated-sector --count-days 600
```

This prepares `results/count600-rebuild/dated-sector-snapshots` for the full
1,093-date window, with four selected IDs present on exactly 600 union-holdings
dates: S000001 (first 600), S000013 (last 600), S100001 (evenly spaced 600), and
S100013 (600 deterministic shuffled dates). They are removed from both holdings
sides on all other dates, and remaining weights are normalized separately.
Selected IDs remain US-classified in the SMF so the existing country filter does
not reduce their expected counts further. Their effective-dated sectors remain
unchanged from the dated-sector generator. Other IDs retain their original
attributes and presence.

`count-history/expected-counts.json` records the exact dates, holdings-side counts,
and expected count for every sector/security pair. For each selected security,
**sum Count over its sector rows within one ReturnType = 600**; individual
sector/security rows generally have smaller counts because sectors change.
Sector-parent counts are distinct dates of any member, not child-count sums.
All rows still have `Days = 1093`, the requested window length.

The preparation writes fresh inputs and retains canonical sources. Snapshot
presence alone does not count toward output Count. Use the new snapshot folder
for the next `--aggregate sector` API request; purge only after preparation and
validation. `test_limited_occurrence.py` checks the same logic on small inputs,
including one versus three date workers and source-weight normalization.

### Numeric slots in sector accumulation

The combined sector/security implementation interns sector names and security IDs
into worker-local catalogs, with dense numeric slots for each sector/security
pair. Normal per-date updates index accumulator vectors directly rather than
repeatedly hashing string keys. Daily sector weight/contribution buffers are
reused and reset only for sectors touched on that date.

Snapshot row positions are hints, not permanent identities: each cached slot is
checked against the current security ID and sector. New or reordered entries,
sector changes and returning securities resolve through the catalogs and reuse
or allocate the appropriate pair slot. Absent holdings do not increment counts.
Different workers have different slot numbers; merging translates each worker's
catalog once before merging numeric pairs. The first worker's state is adopted
directly. String identities remain available for deterministic output sorting.

This changes neither formulas nor output schema, counts or ordering.
`test_sector_smf.py` covers reordered and resized snapshots, new and returning
IDs, sector moves and returns, absent holdings, and 1/3-worker oracle parity.
`test_limited_occurrence.py` checks sparse holdings counts. The earlier
`sector-count600-c1` timing is the string-map baseline; numeric-slot performance
requires a new run after a fresh purge.

### Per-worker workload volumes

After a profiled concurrency run with worker traces, run:

```sh
.venv/bin/python python/summarize_worker_volumes.py \
  --run results/api/sector-count600-numeric-slots-c20
.venv/bin/python python/plot_worker_race.py \
  --run results/api/sector-count600-numeric-slots-c20
```

This writes worker-volumes.csv (one row per API/worker), request-volumes.csv,
worker-volumes.json, and VOLUMES.md. The interactive worker Gantt includes date
counts, six-input row counts and total rows in hover details and its table;
static bars show total input rows in millions.

For existing direct-snapshot runs without recorded worker row counters, exact
row counts are reconstructed from Parquet row-group metadata for each worker's
assigned dates, then reconciled against every request's recorded decode counters.
This is not an estimate from average rows/date. Source files must still exist
when reconstructing. The exported CSV/JSON remains usable after bulk-data cleanup.
These are input table rows, counting repeated processing by each API, not unique
securities, physical disk reads or output rows. All six input tables are included.
Per-worker joined US-security counts were not recorded in these old profiles;
request and batch totals are available, but no worker-level estimate is substituted.
