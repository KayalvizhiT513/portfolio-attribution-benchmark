# Daily-snapshot single-request benchmark

The user confirmed purge was done after all preparation and checks. The timed
API run started without subsequent preparation, source validation or warm-up
requests. Purge is user-reported; its OS effects are not independently verified.

One Rust API request covered 2023-01-01 through 2025-12-28, using 12 workers
and 12 shared slots. Summary SMF traces and stage profiling were enabled.

| Approach | API seconds | Peak sampled RSS | SMF records processed |
|---|---:|---:|---:|
| Date-ordered history replay (original run) | 30.604 | 1.558 GiB | 593,536,729 |
| Daily snapshots, after reported purge | 14.098 | 1.513 GiB | 174,880,000 decoded rows |

These are separate observed runs, not a matched cache-state experiment.
Daily snapshot preparation (49.316 seconds) is a reusable cost outside API
latency. Snapshot Parquet occupies 907,406,144 bytes.

The daily-snapshot run returned HTTP 200. All outputs match the original
daily-heavy result. All 12 worker traces validate exact snapshot dates and
row counts, state resets and replacements. All slots were released; FIFO
admission and no slot overlap were verified. Host swap stayed at zero.

The whole request spent 0.0393 seconds planning, 14.0197 seconds in the
worker phase, and 0.0083 seconds writing/finalizing the report. Remaining
metrics/profile/API overhead accounts for the rest.

## Per-worker split

| Worker | Initial snapshot rows | Initialization | Processing afterward | Finished after first worker queued |
|---|---:|---:|---:|---:|
| 1 | 160,000 | 0.069s | 13.803s | 13.880s |
| 2 | 160,000 | 0.102s | 13.644s | 13.759s |
| 3 | 160,000 | 0.074s | 13.758s | 13.847s |
| 4 | 160,000 | 0.079s | 13.928s | 14.019s |
| 5 | 160,000 | 0.121s | 13.587s | 13.719s |
| 6 | 160,000 | 0.103s | 13.698s | 13.814s |
| 7 | 160,000 | 0.104s | 13.860s | 13.981s |
| 8 | 160,000 | 0.093s | 13.905s | 14.011s |
| 9 | 160,000 | 0.111s | 13.583s | 13.716s |
| 10 | 160,000 | 0.065s | 13.837s | 13.920s |
| 11 | 160,000 | 0.100s | 13.586s | 13.709s |
| 12 | 160,000 | 0.105s | 13.862s | 13.989s |

Initialization is measured from each worker's SMF trace start to its first
cache-ready event. Processing afterward includes the first date's calculation
and the remaining dates. Finished-after excludes request planning before the
first queue event. Worker durations overlap; do not add across workers.

Evidence:

- [Validation](../results/api/daily-snapshot-c1/validation.json)
- [Worker CSV](../results/api/daily-snapshot-c1/worker-timing-split.csv)
- [Stage summary](../results/api/daily-snapshot-c1/stage-summary.json)
- [Cache policy](../results/api/daily-snapshot-c1/cache-policy.json)

No 20-request benchmark was started.
