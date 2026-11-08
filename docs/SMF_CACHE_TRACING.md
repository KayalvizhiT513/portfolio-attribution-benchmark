# Inspecting per-worker SMF updates

Both engines accept `--smf-trace off|summary|records` (default `off`). Tracing
works for the six-input CLI and HTTP API. Each worker writes a new file named
`smf-trace-worker-01.jsonl` in its calculation's output directory. API requests
already have separate output directories, so their files cannot mix. Use a fresh
output directory: existing trace files are never overwritten.

Add this to a normal CLI or server command:

```sh
--smf-trace records
```

`summary` writes effective-date totals and one cache-ready event per calculation
date. `records` additionally writes each applied security's before/after country,
currency and analytics ID. Full record tracing can produce large files and adds
serialization/I/O overhead; use it for diagnosis, not comparable performance runs.

Events are ordered by a per-worker `sequence`. `seconds` is elapsed time since
that worker opened its trace, not a shared clock across workers.

| Event | Meaning |
|---|---|
| `worker_start` | Engine, worker ID, assigned first/last calculation dates, trace level |
| `security_applied` | Effective date, calculation date, instrument ID, action, before and after attributes |
| `effective_date_applied` | Rows inserted, updated, unchanged, and total securities now retained |
| `date_ready` | Cache initialization, update or reuse; applied rows/dates; retained security count |
| `worker_complete` | All assigned date calculations completed successfully |

`updated` means existing attributes changed. `unchanged` means a new effective-date
record was applied but its attributes matched the previous state. This differs
from `reuse`, which means no effective-date records were read for that calculation
date. Initial history events have earlier effective dates than their calculation
date. Future records must never appear. A missing `worker_complete` indicates an
incomplete/failed worker; `date_ready` means the cache is ready, not that the
subsequent attribution calculation succeeded. Trace write failures fail the
calculation rather than silently discarding audit events. Rust also records the
server request ID; CLI uses null. In both engines the containing directory identifies
the calculation/request.

Validate record-level traces against the original input and export a compact CSV:

```sh
.venv/bin/python python/inspect_smf_trace.py \
  --data INPUT_DIRECTORY --trace-dir CALCULATION_OUTPUT_DIRECTORY
```

The inspector independently streams SMF history, resolves the latest effective
record per security, and compares it with the replayed trace at every calculation
date. It checks before/after values, action counts, effective-date coverage, daily
continuity, sequence ordering, cache size, and worker completion. It rereads source
history per worker and retains a current state for verification, so its overhead
is separate from the engine. Source files must remain unchanged after the run.

A runnable small example is available:

```sh
.venv/bin/python python/create_smf_trace_demo.py --output results/smf-trace-demo-new
```

It generates one-worker and three-worker runs for Rust and Go, validates all
traces, and creates `smf-trace-summary.csv` in each output directory. Build the
Rust release binary and `results/six-input/benchmark-go` first.

In the generated single-worker example:

| Calculation date | Cache decision | Applied records | Result |
|---|---|---:|---|
| Jan 1 | initialize | 3 | Load S3 from Dec 31; S1/S2 from Jan 1 |
| Jan 2 | update | 3 | Change S1 currency/alias, add S4, apply unchanged S3 record |
| Jan 3 | update | 1 | Change only S2 country/currency |
| Jan 4 | reuse | 0 | Keep all four securities unchanged |

The Jan 5 change is deliberately excluded. The three-worker example also shows
that later workers initialize from all earlier effective dates instead of loading
only the most recent global date.

Rust six-input profiling accepts `--stage-profile true` on the server or CLI;
`python/load_attribution_api.py` passes it through with `--stage-profile`.
It writes `stage-profile.json` (planning, worker phase, report writing and metrics
writing) and `stage-profile-worker-NN.jsonl` (one record per calculation date).
The worker records separate the concurrent input phase from sequential account/FX
lookup building, analytics lookup building, return processing, joins/validation,
attribution and cleanup. SMF reader records separately measure decoding, applying
changes with enabled tracing, and release/other overhead. These SMF subspans occur
inside the concurrent input phase, so they must not be added to it. Timings are
elapsed spans that include OS scheduling and I/O waits, not CPU measurements.
Profile serialization is outside date spans and appears in worker overhead.

For a single-request profiled API run, export per-worker CSV and JSON summaries:

```sh
.venv/bin/python python/summarize_stage_profile.py --run RUN_DIRECTORY
```
