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
only these accumulators are merged after workers finish. Daily reports are not
retained in aggregation modes. Memory scales with workers × unique securities
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
