# SMF workload with uneven security updates

Generate the current workload with:

```sh
.venv/bin/python python/prepare_sparse_smf.py
```

The default source is `results/six-input/inputs`; the generated dataset is
`results/six-input/security-history-daily-heavy-inputs`. Existing directories
are never overwritten. The other five input files are symlinks to the source.
The original snapshot dataset and the earlier 50%-unchanged experiment remain
separate, reproducible inputs.

| Cohort | Share | Securities in the full fixture | Update interval |
|---|---:|---:|---|
| Daily | 50% | 80,000 | Every day |
| Weekly | 9% | 14,400 | Every 7 days |
| 30-day | 20% | 32,000 | Every 30 days |
| 90-day | 20% | 32,000 | Every 90 days |
| Unchanged | 1% | 1,600 | Initial record only |

Intervals are fixed day counts, not calendar months or quarters. Numeric security
IDs select cohorts deterministically; ID-derived phases stagger each non-daily
cohort across dates. The first date initializes all securities. Later dates
contain only securities due for an update. Every update changes country and
currency, alternating between initial and alternative values; analytics aliases
stay fixed. These are synthetic stress-test changes, not observed market events.

Initialization resolves each security's latest source version on or before the
start date and rebases it to that date. Generated updates replace the source's
later history, so attribution is not expected to match the original snapshot
baseline. Compare engines on the same generated data and validate small fixtures
against the independent trace oracle. The manifest records cohort sizes, example
security IDs, per-cohort update totals and row counts for every date.

For the 160,000-security, 1,093-date fixture, the current schedule creates
91,319,200 SMF records including initialization. This is a data-file row count,
not per-API decoded volume: each worker additionally replays earlier history
when initializing its assigned range. The daily-heavy workload therefore also
exposes the cost of the current initialization strategy. No checkpoints exist yet.

Enable `--smf-trace summary` for compact per-worker audit logs or `records` for
before/after details. Record-level logs on this full workload can be very large.
See [SMF cache tracing](SMF_CACHE_TRACING.md).

Verify generation, security-specific update frequencies, unchanged securities,
Rust/Go attribution equality and detailed trace replay using:

```sh
.venv/bin/python python/test_sparse_smf.py
```
