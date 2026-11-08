# Daily-heavy SMF: single-request stage profile

One Rust API request processed 2023-01-01 through 2025-12-28 (1,093 dates),
using 12 date workers and 12 shared slots. The dataset has 160,000 securities,
50% daily changes and 91,319,200 SMF input records. This run used summary SMF
tracing, worker admission tracing and detailed stage profiling. OS caches were
managed naturally; there was no purge. The 20-request run remains stopped.

The profiled response took **30.4068 seconds** server-side (30.4102 seconds for
the driver batch). Every output matched the previous unprofiled single request.
Peak sampled RSS was 1.540 GiB; host swap stayed at zero with zero recorded
Swapins/Swapouts. This is one diagnostic run, not a controlled performance comparison.

| Sequential request phase | Elapsed seconds |
|---|---:|
| Input metadata planning, date coverage validation and setup | 0.047601 |
| Worker phase: spawn, admission, processing, joining and cleanup | 30.349993 |
| Sort reports, aggregate metrics, write/finalize/rename Parquet | 0.007506 |
| Metrics CSV writing | 0.000094 |
| Remaining API/profile overhead | 0.001614 |
| **API elapsed** | **30.406807** |

Worker 12 was the last to release its slot. It handled September 28 through
December 28, 2025 (92 dates). Its sequential work is broken down below; the other
workers ran concurrently and their durations must not be added to this table.

| Worker 12 phase, summed across its dates | Elapsed seconds |
|---|---:|
| Concurrent input loading and SMF advancement, including reader thread waits | 27.635754 |
| Account and FX lookup construction/validation | 0.000277 |
| Analytics lookup construction/validation | 0.626391 |
| Portfolio/index return adapters, validation, US filtering, normalization and aggregation | 1.650018 |
| Account/currency/analytics join validation and checksum accumulation | 0.326974 |
| Attribution/contribution period construction and calculation | 0.082166 |
| Calculate-call cleanup and remaining overhead | 0.008208 |
| Other per-date overhead | 0.000115 |
| Worker setup, teardown, profile writing and remaining overhead inside its slot | 0.007970 |
| **Worker slot occupancy** | **30.337873** |

Inside that worker's input phase (these spans overlap other readers):

| SMF subphase, summed across dates | Elapsed seconds |
|---|---:|
| Parquet reading/decoding and decoded input validation | 3.587105 |
| Applying effective-date changes, including duplicate checks, lookup mutations, allocation and enabled summary tracing | 23.983224 |
| Batch release, traversal, trace flushing and remaining SMF overhead | 0.007877 |
| **SMF task spans** | **27.578206** |

SMF applying/trace time is a broad elapsed span, not CPU time or an isolated
allocator measurement. It includes OS preemption; the profile does not divide
it into hashing, string allocation, mutation or trace serialization costs.
The other five readers run concurrently with SMF, and their durations are
available individually in the CSV without being added to the input phase.

Worker 12's first-date initialization alone read 83,722,600 SMF records:
3.287445 seconds decoding, 22.589989 seconds applying/tracing, and 0.004950
seconds of remaining SMF overhead. The complete first-date input phase took
25.882532 seconds, including the overlapping daily readers. Across all workers,
the request decoded 593,536,729 SMF records because initialization replays earlier
history separately for each assigned range. No checkpoint optimization was applied.

Evidence (generated locally):

- [Stage summary](../results/api/daily-heavy-c1-profile/stage-summary.json)
- [All worker stages CSV](../results/api/daily-heavy-c1-profile/stage-worker-summary.csv)
- [Output, SMF and admission validation](../results/api/daily-heavy-c1-profile/validation.json)
- [Raw experiment and samples](../results/api/daily-heavy-c1-profile/summary.json)

Reproduce in a fresh output directory:

```sh
.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/six-input/security-history-daily-heavy-inputs \
  --output results/api/daily-heavy-c1-profile-new \
  --workers 12 --worker-limit 12 --worker-trace --smf-trace summary \
  --stage-profile --concurrency 1 --repeats 1 --settle-seconds 3 \
  --timeout 3600 --interval 0.2
.venv/bin/python python/summarize_stage_profile.py \
  --run results/api/daily-heavy-c1-profile-new
```

Verification: 37 six-input integration checks and 12 Rust unit tests passed.
The profile test checks unchanged attribution output, timing decomposition,
nonnegative spans, date coverage and nested/concurrent input durations.
