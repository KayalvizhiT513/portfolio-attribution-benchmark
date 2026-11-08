# Attribution API memory experiments

The Go HTTP adapter calls the existing `runSixInput` function in the server
process; the Rust adapter calls `six_input::run_with_pool` with a shared date-worker
limit (12 by default). Both preserve complete-date loading, per-worker SMF caches, calculation
formulas, Arrow release calls, and ordinary garbage collection. It allocates no
synthetic ballast, copies no full request dataset, and does not force GC or flush
application/OS caches. Both engines support the same HTTP request/response contract.
Rust health reports request counters and the engine name; Go also reports heap/GC counters.

Build and serve from the repository root:

```sh
go -C go build -o ../results/api/benchmark-go ./cmd/attribution
results/api/benchmark-go --listen 127.0.0.1:8080 \
  --data results/six-input/inputs --output-dir results/api/manual --threads 12
```

Submit a calculation:

```sh
curl -sS http://127.0.0.1:8080/attribution \
  -H 'Content-Type: application/json' \
  -d '{"start_date":"2023-01-01","end_date":"2025-12-28"}'
curl -sS http://127.0.0.1:8080/health
```

`POST /attribution` returns elapsed time, calculation metrics, and local report
paths. Attribution and contribution fields remain in the original Parquet report.
Each request gets its own output directory. Invalid JSON/dates return 400;
calculation/input failures return 422. Outputs, including failed-request
artifacts, are retained on disk for inspection. Requests cannot override input
paths or worker counts. The server's `--data` fixes the six-file dataset and
`--threads` fixes workers per calculation and, for Go, process-wide GOMAXPROCS once at
startup. Rust's `--worker-limit` sets the shared date-worker limit independently
of per-request partition count. HTTP mode uses the six-input daily scenario regardless of CLI scenario
flags; request dates are required and CLI max-days does not truncate requests.

`GET /health` reports active/completed/failed calculations and process-wide Go
heap allocation, heap reservation, and GC cycles for Go. Rust has no equivalent
heap counters here; process RSS is measured externally for both engines. Heap statistics are not RSS.
Go requests execute without a shared date-worker admission limit. Rust requests
share 12 date-worker slots by default, as described below. This is a local benchmark
adapter, without authentication or request cancellation inside the calculation.
A disconnected client does not stop already-started work. The load driver stops
the server when the experiment ends. No server is left running afterward.

## Reproducible experiments

The driver starts one fresh server, keeps it alive across concurrency levels and
repeated batches, and samples its RSS using `ps` alongside health counters:

```sh
.venv/bin/python python/load_attribution_api.py \
  --data results/six-input/inputs --output results/api/current-full \
  --concurrency 1 2 4 --repeats 2 --settle-seconds 5
```

Use a fresh output directory for each experiment. `summary.json` records the
command, responses, timings, sampled RSS, active request counts, GC/heap counters,
and natural idle behavior. `server.log` retains errors. A failed request/process
makes the driver exit unsuccessfully after saving evidence. Process death alone
is not an OOM diagnosis: retain OS kill/allocation-failure evidence to establish
that. Sampling can miss short-lived peaks, and RSS can include shared pages.
The sampler does not allocate measurement buffers inside the calculation process.

Generate input-size cases independently:

```sh
# Baseline: 10k securities, one account per date, 12 calculation dates.
.venv/bin/python python/prepare_memory_inputs.py \
  --output results/api/fixtures/baseline --securities 10000 --days 12
# Grow portfolio, index, SMF and analytics together to 100k securities/date.
.venv/bin/python python/prepare_memory_inputs.py \
  --output results/api/fixtures/securities-100k --securities 100000 --days 12
# Grow unrelated accounts/date, preserving the requested portfolio and securities.
.venv/bin/python python/prepare_memory_inputs.py \
  --output results/api/fixtures/accounts-100k --securities 10000 --accounts 100000 --days 12
# Grow account history outside the identical calculation window.
.venv/bin/python python/prepare_memory_inputs.py \
  --output results/api/fixtures/history-3650 --securities 10000 --account-history-days 3650 --days 12
```

Run each fixture separately with `--data PATH --end-date 2023-01-12`, keeping
workers and concurrency equal. Optional `--master-securities` and
`--analytics-securities` isolate oversized lookup/input universes; require
analytics >= master >= portfolio securities. Generation writes date-pure row
groups in chunks, while the engine still collects complete dates. Counts,
coverage and dimensions are retained in `memory-manifest.json`. Synthetic
accounts have unique account and portfolio IDs per date, with PF as the requested
portfolio. Both returns streams use normalized weights, shared valid securities,
and matching SMF/analytics/FX coverage.

Compare load-phase RSS peaks, observed active calculations, throughput and idle
heap/RSS across repeats. The number of requests sent is not proof of execution
overlap: use the health samples. An RSS plateau after requests finish is not by
itself a leak; allocators can retain free memory. More history also increases
file metadata/planning memory even though old account rows are not decoded.

The current six-input engine supports one portfolio per invocation. Repeated
requests exercise concurrent calculation instances on the same read-only dataset;
they do not add unsupported portfolio entities to a returns file or claim to
represent different portfolio compositions. Run distinct dataset experiments to
measure different portfolio shapes.

## Validation

```sh
go -C go test ./cmd/attribution
go -C go build -race -o ../results/api/benchmark-go-race ./cmd/attribution
.venv/bin/python python/test_attribution_api.py --binary results/api/benchmark-go-race
```

The integration check validates eight outputs from two four-request batches
against the existing independent Python oracle, including changing SMF snapshots,
all attribution/contribution fields, isolated output paths, unchanged cache
counts, invalid requests, calculation failures, and final request counters.

## Measurements on November 6, 2026

Go, 12 workers per request, existing 1093-date six-input dataset. One server stayed
alive across all six batches; each concurrency level ran twice. Conditions were
uncontrolled (ordinary OS caches, host applications and garbage collection).
These are observations, not capacity guarantees or an established OOM threshold.

| Active requests observed | Sampled peak process RSS across repeats | Batch wall seconds, repeats 1 / 2 | Result |
|---|---:|---:|---|
| 1 | 2.656 GiB | 11.271 / 10.787 | Both passed |
| 2 | 5.262 GiB | 20.004 / 20.084 | All four requests passed |
| 4 | 8.167 GiB | 94.335 / 86.010 | All eight requests passed |

All 14 reports match every attribution/contribution field in the saved CLI
baseline (`results/six-input/cached/go/country-portfolio_PF.parquet`). Four active
requests were observed, rather than merely queued HTTP traffic. At concurrency
four, Go heap allocation reached about 10.05 GiB; heap allocation and resident
memory are different measurements and can diverge under OS memory management.
After the last batch, sampled idle heap allocation was at most 1.902 GiB versus
9.888 GiB during that batch, while sampled idle RSS remained as high as 6.578
GiB. This does not establish a leak, nor rule one out over longer operation.
The latency increase does not identify CPU, I/O, GC, or memory pressure as the
isolated cause. No process death or OOM was observed.

Local raw evidence: [full API run](../results/api/current-full/summary.json) and
[server log](../results/api/current-full/server.log). These generated artifacts
are ignored by Git. Tests do not artificially retain completed calculation inputs.

Separate fresh-server input experiments used 12 calculation dates, 12 workers,
one request at a time, two repeats, and a 20 ms requested sampling interval
(actual interval includes sampler work). These short requests can have missed
peaks. Securities scaling grows portfolio, index, SMF and analytics together.

| Fixture | Daily securities | Daily accounts | Extra account-history dates | Sampled experiment peak RSS |
|---|---:|---:|---:|---:|
| Baseline | 10,000 | 1 | 0 | 251.4 MiB |
| Securities growth | 100,000 | 1 | 0 | 1535.1 MiB |
| Accounts growth | 10,000 | 100,000 | 0 | 493.1 MiB |
| History growth | 10,000 | 1 | 3650 | 294.2 MiB |

All eight requests passed. The accounts-growth fixture decoded 1,200,000 account
rows per request. Both baseline and history-growth fixtures decoded exactly 12
account rows, confirming historical account rows outside the request window were
not loaded into the daily calculation. Every accounts/history output matches
the baseline report. More history still adds metadata/planning overhead.

Local evidence: [baseline](../results/api/baseline-comparison/summary.json),
[securities](../results/api/securities-100k/summary.json),
[accounts](../results/api/accounts-100k/summary.json), and
[history](../results/api/history-3650/summary.json).

The tested implementation remained functional at these sizes. These original
measurements preceded Rust's shared date-worker limit. Neither engine has a
daily-input memory budget: sufficiently large
complete dates or enough overlapping valid calculations can exceed available
memory. These experiments establish scaling behavior; they do not identify the
first failing security count, account count, or concurrency. The Go unit tests,
race-detector unit tests, and concurrent integration test against a
race-instrumented binary passed.

## Rust API and 20 simultaneous requests

Build Rust and use the same load driver:

```sh
cargo build --release --manifest-path rust/Cargo.toml --bin portfolio-attribution
.venv/bin/python python/test_attribution_api.py \
  --binary rust/target/release/portfolio-attribution
.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/six-input/inputs --output results/api/rust-full-20 \
  --workers 12 --worker-limit 240 --concurrency 20 --repeats 1 --settle-seconds 10 \
  --timeout 1800 --interval 0.2
```

The Rust adapter uses `tiny_http` for HTTP and `serde_json` for request/response
encoding. Each request directly invokes the existing engine, with an isolated
output directory and the original per-request date workers. It does not spawn
calculation subprocesses. `--listen` enables API mode; the default CLI remains
unchanged. Rust API mode also fixes the scenario to six-input daily calculation
and requires explicit dates in each request. Ordinary calculation errors return
422; caught calculation panics return 500 and remain in the server log. Native
allocator OOM aborts and OS process kills remain process failures.

The historical run below predates the shared worker limit. Setting
`--worker-limit 240` allows the same degree of date-worker concurrency for comparison.
Twenty simultaneous calculations with 12 workers each can then activate 240 date
workers and up to 1440 input-reader threads. Failures from thread/resource limits
must be distinguished from allocator OOM. Rust and Go need not encounter the
same resource bottleneck even with identical calculation results.

The driver writes samples immediately to `samples.jsonl`, completed request
responses to `responses.jsonl`, and the final experiment to `summary.json`. On
macOS it also captures host swap usage and `vm_stat` approximately every five
seconds; those are host-wide measurements and can include other applications.
The API observer allocates no measurement arrays in the calculation process.

### Recorded Rust 20-request result — November 6, 2026

The full 2023-01-01 through 2025-12-28 window (1093 dates) was submitted 20 times
simultaneously, with 12 existing date workers per request. Health samples confirmed
20 calculations active together. One batch was run; no forced GC, cache flush,
ballast allocation, subprocess-per-request model, or memory limit was introduced.

| Measurement | Observed result |
|---|---:|
| Successful HTTP responses | 20 / 20 |
| Calculation failures / thread errors / observed OOM | 0 |
| Batch wall time | 841.829 s (14 min 1.8 s) |
| Individual request wall times | 841.207–841.823 s |
| Peak sampled process RSS | 9.206 GiB |
| Host swap before load | 0 GiB |
| Peak host swap during experiment | 4.903 GiB |
| Host swap near experiment end | 3.001 GiB |
| Final active / completed / failed counters | 0 / 20 / 0 |

All 20 reports match every attribution/contribution field in the saved Rust CLI
baseline. Each request preserved the expected 12 workers, 1093 dates, 46 SMF
snapshot loads and 1047 SMF cache hits. Ten Rust unit tests, the eight-request
concurrent independent-oracle integration test, formatting and diff checks passed.
The driver stopped the otherwise-live server after the batch and idle samples.

This workload completed with severe latency and host memory pressure, rather
than OOM. Swap and OS compression mean physical RAM alone is not a hard process
allocation limit; process RSS also does not represent all paged/compressed memory.
Host swap is system-wide and cannot all be attributed to Rust. CPU oversubscription,
reader-thread scheduling, I/O and memory management were not isolated as individual
causes of the elapsed time. The result does not establish a safe production
concurrency limit or exclude failure at larger inputs/concurrency.

Raw evidence: [experiment summary](../results/api/rust-full-20/summary.json),
[20-output validation](../results/api/rust-full-20/validation.json),
[streamed memory/host samples](../results/api/rust-full-20/samples.jsonl),
[streamed responses](../results/api/rust-full-20/responses.jsonl), and
[server log](../results/api/rust-full-20/server.log). These generated artifacts
remain local and ignored by Git.

## Shared date-worker slots in Rust

Rust API mode now defaults to **12 date-worker slots across the entire server**.
`--threads 12` still divides each request's date window into 12 consecutive
ranges. `--worker-limit 12` limits how many of those ranges can be processed
across all requests at once. The limit is configurable at server startup and
cannot be overridden by an HTTP request. Standalone CLI execution keeps its
existing per-invocation behavior.

Each worker acquires one FIFO permit **before** opening its six readers or
allocating its SMF cache. It holds the permit while processing every date in its
assigned range, including time waiting for input readers, until its readers and
cache have been dropped. A returned error or panic unwind also releases the
permit. Queued date-worker threads hold their request references but have not
loaded daily buffers or built their worker-local caches. Request metadata planning
and compact completed reports remain outside this slot limit.

FIFO ordering applies to worker admission tickets, not entire API requests.
One request may acquire all slots initially, while later requests can share
slots as ranges finish. No request has to acquire all of its permits to progress,
so a partial allocation cannot deadlock with another request. An admitted worker
is never rotated out of its slot to admit another worker. The operating system
can still preempt its threads; no CPU pinning or real-time scheduling is used.
Each admitted worker retains its existing concurrent input readers: 12 date
workers can still have up to 72 input-reader threads. This is a date-worker
memory/concurrency limit, not a 12-thread limit or a fixed RAM budget.

Health adds `worker_limit`, `running_date_workers`, `waiting_date_workers`, and
`peak_running_date_workers`. `active` continues to count unfinished API requests,
including those whose workers are queued.

```sh
.venv/bin/python python/test_attribution_api.py \
  --binary rust/target/release/portfolio-attribution --worker-limit 2 --requests 20
.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/six-input/inputs --output results/api/rust-shared12-full-20 \
  --workers 12 --worker-limit 12 --concurrency 20 --repeats 1 \
  --settle-seconds 5 --timeout 1800 --interval 0.2
```

New load-driver responses record start/end offsets relative to batch launch, so
future Gantt charts can use measured offsets instead of aligning every start to
zero. Historical run files remain unchanged.

### Shared-slot validation and repeat of the 20-request full-window run

All 20 requests succeeded. Every report matched the saved Rust CLI baseline;
each retained the original 12 partitions, 1093 dates, 46 SMF loads and 1047
cache hits. Health counters recorded a peak of exactly 12 running date workers
and 228 queued workers, ending with zero running/queued workers and zero failures.
The observed request durations include time spent waiting for worker slots.

| Measurement | Earlier run, up to 240 date workers | Shared limit of 12 date workers |
|---|---:|---:|
| Successful requests | 20 / 20 | 20 / 20 |
| Batch wall seconds | 841.829 | 153.147 |
| Peak sampled RSS | 9.206 GiB | 2.148 GiB |
| Individual request seconds | 841.207–841.823 | 8.821–153.141 |
| Host swap at start | 0 GiB | 1.302 GiB |
| Peak host swap | 4.903 GiB | 1.302 GiB |

These are separate runs with uncontrolled cache and host conditions. The shared
limit is verified by its persistent peak counter and all health samples; the
observed time/memory differences are not a controlled statistical performance
claim. Existing host swap remained unchanged in the shared-slot experiment.
This change limits worker input/cache concurrency; it does not bound request
metadata, completed reports, request count, daily input size, or total process RAM.

Validation includes FIFO admission and holding slots while work waits, releasing
slots after returned errors and panic unwinding, and concurrent independent-oracle
API checks with a deliberately smaller two-slot limit. The API check also forces
an invalid-FX calculation after admission, verifies slots return to zero, restores
the data, and successfully processes 40 subsequent requests across two batches.

Local evidence: [shared-limit summary](../results/api/rust-shared12-full-20/summary.json),
[worker-cap and output validation](../results/api/rust-shared12-full-20/validation.json),
and [streamed health/RSS samples](../results/api/rust-shared12-full-20/samples.jsonl).

## Worker-by-worker competition trace

To see how each API's 12 date workers compete, enable `--worker-trace PATH` on
the Rust server, or use the driver's `--worker-trace` flag. Tracing is optional
and disabled by default. The trace file must be fresh; an existing file is not
overwritten. Responses now include a server `request_id`, letting the viewer map
server events back to each client's numbered API request.

Every worker records three events under the admission lock: `queued`, `admitted`,
and `released`. Each contains a monotonic timestamp, server request ID, worker
number (1–12), FIFO ticket, date range, and Rust thread ID. Admission/release
also identify the application slot (1–12). Slot IDs are reusable admission
permits, not physical CPU IDs. A release event occurs after the worker's readers
and cached input state have been dropped, including error or panic paths.
Trace writes introduce some overhead; these are traced observations.

The viewer verifies every worker has all three events, FIFO admission order,
non-overlapping slot holders, matching worker/thread identity, the shared limit,
and complete coverage of all 12 workers for each successful API request. It
exports an interactive HTML viewer, a 12-slot overview in PNG/SVG/PDF, and the
exact worker admission/handoff order as CSV. Hover or select an API to see the
queue wait, full slot occupancy and previous worker that held the same slot.

```sh
# Run this manually immediately before the experiment when desired:
sudo purge

.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/six-input/inputs --output results/api/rust-worker-race-20-purged \
  --workers 12 --worker-limit 12 --worker-trace \
  --concurrency 20 --repeats 1 --settle-seconds 5 --timeout 1800 --interval 0.2
.venv/bin/python python/plot_worker_race.py \
  --run results/api/rust-worker-race-20-purged
```

The viewer uses the first worker's queue event as time zero. Individual event
boundaries are measured; no artificial simultaneous worker starts are assumed.
Gray worker bars mean queued for admission; colored bars mean holding a slot.
Slot occupancy includes I/O waits and OS thread preemption. This is **not a CPU
scheduler trace** and cannot identify which CPU core ran a worker or why macOS
paused it. Rust thread IDs are not OS thread IDs. Trace timing shows who held a
slot and who followed on that slot, not who executed on a CPU before whom.

The initial trace in `results/api/rust-worker-race-20` was made without a pre-run
purge and is retained separately. The user reported running `sudo purge` before
the fresh `rust-worker-race-20-purged` experiment; cache policy is recorded in
that run's `cache-policy.json`. There is no cache flushing between requests;
requests naturally share and populate the OS cache during the run. Pre-run purge
is recorded from user confirmation and is not an independently verified guarantee
that every request sees cold storage.

### Traced run after user-reported macOS purge

The fresh `rust-worker-race-20-purged` batch completed all 20 requests in
147.073 seconds. All 20 outputs matched the saved Rust CLI baseline, including
unchanged worker partitions and SMF cache counts. Peak sampled RSS was 2.341 GiB.
The trace contains exactly 720 events for 240 workers (12 per API), with FIFO
admission verified, every permit released, no overlapping occupants of any slot,
and a maximum of 12 holders. Tracing records slot competition, not CPU scheduling.

The first 12 slots were split across eight client API requests. Worker numbers
were not admitted in numeric order: for example, API 20 workers 1, 2, 8 and 12
were among the initial holders while its other workers entered later. The
numbered APIs in the viewer are client request indices, mapped through each
response's server `request_id` to avoid confusing server arrival order with the
client's numbering.

An observed slot-1 handoff sequence, in seconds from the first queue event:

| Worker | Admitted s | Released s |
|---|---:|---:|
| API 20 / W01 | 0.000234 | 6.399124 |
| API 19 / W03 | 6.411 | 12.984 |
| API 16 / W09 | 12.985 | 20.426 |

The exact unrounded values are in the CSV. A previous slot occupant is not a
CPU scheduling predecessor. The workers were allowed to retain their permits
while doing input work and while paused by the OS.

- [Interactive view: select an API, inspect all its workers, or follow handoffs](../results/api/rust-worker-race-20-purged/worker-race.html)
- [12-slot overview PNG](../results/api/rust-worker-race-20-purged/worker-slots.png)
- [Exact admission and handoff CSV](../results/api/rust-worker-race-20-purged/worker-order.csv)
- [Trace validation](../results/api/rust-worker-race-20-purged/worker-validation.json)
- [All-output validation and cache policy](../results/api/rust-worker-race-20-purged/output-validation.json)

The HTML viewer is standalone with embedded data and no external scripts or
network dependencies. Matplotlib produces the static PNG/SVG/PDF overview.

## Instrumented repeat of the high-concurrency configuration

The Rust health endpoint now includes persistent current/peak counters under
`read_activity`. These counters are independent of the optional worker-event log:

- `reading_workers`: date workers inside the concurrent-input read/join phase.
- `read_decode_calls`: calls currently inside `read_date`, including I/O waits,
  decoding, validation and OS preemption. SMF cache hits do not enter this counter.
- `retained_daily_inputs`: completed non-SMF input reads retained by a date scope,
  from read completion through return of that date's calculation. A calculation
  can consume/drop some buffers earlier internally, so this scope count is not
  an exact instantaneous count of live allocations or bytes.
- `workers_with_smf_cache`: workers retaining an applicable decoded SMF snapshot;
  the counter remains held across dates and drops after worker-local cache cleanup.

The counters do not measure CPU memory-read instructions, disk reads alone, or
per-worker RSS. Their persistent peaks capture short-lived concurrency that
periodic samples can miss. Compare their timelines with sampled process RSS and
host swap counters without interpreting correlated peaks as allocation ownership.
Counter guards release on error paths. Integration checks verify zero final
counts after a calculation failure and 40 successful oracle-checked requests,
and check count peaks against the configured slot limits.

To restore the original potential concurrency for exactly 20 requests, use
`--workers 12 --worker-limit 240`. This allows all 240 date ranges to be admitted,
without changing input size, calculation formulas, caches, or cleanup.

```sh
# User performs the pre-run macOS purge, then the driver is launched:
sudo purge
.venv/bin/python python/load_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --data results/six-input/inputs --output results/api/rust-read-pressure-240-purged \
  --workers 12 --worker-limit 240 --worker-trace \
  --concurrency 20 --repeats 1 --settle-seconds 5 --timeout 1800 --interval 0.2
.venv/bin/python python/plot_read_pressure.py \
  --run results/api/rust-read-pressure-240-purged
```

The high-concurrency rerun was executed after a fresh user-reported purge; results
are recorded below. The default server limit remains 12; the 240 limit is explicit
for this benchmark.

### Instrumented 240-worker result — November 7, 2026

After the user reported completing `sudo purge`, the run admitted all 240 date
workers for 20 full-window API requests. All 20 requests succeeded in 254.883
seconds (4 minutes 15 seconds). Every output matched the saved Rust CLI baseline,
with unchanged 1093 dates, 12 partitions, 46 SMF loads and 1047 cache hits per
request. All 720 worker admission/release events were complete and consistent.

| Measurement | Recorded peak |
|---|---:|
| Admitted date workers | 240 |
| Workers in concurrent-input read phase | 213 |
| Overlapping `read_date` calls | 791 |
| Completed daily inputs tracked by active date scopes | 1200 |
| Workers holding SMF caches | 240 |
| Sampled process RSS | 12.326 GiB |

All activity counters returned to zero when requests finished. The 1200 input
scope count corresponds to five non-SMF daily sources across 240 workers; it is
not a count of equally sized buffers, exact live allocations, or resident bytes.
A worker can retain its SMF cache while outside its read phase. Read/decode spans
include I/O waits and OS preemption, so 791 overlapping calls do not mean 791
threads were executing CPU instructions at once. Each peak is independent; their
maxima are not asserted to occur at the same timestamp.

Host swap usage stayed at zero. Recorded `vm_stat` deltas were **zero Swapins
and zero Swapouts** over the sampled experiment. Unlike the older 14-minute run,
this repeat did not reproduce swap activity or the same runtime. Hardware load,
cache state, tracing and other conditions differ between separate runs; these
measurements do not isolate the causes of that difference. Even without observed
swap activity, this run's worker/input concurrency and RSS were substantially
higher than in the recorded 12-slot run (147.073 seconds, 2.341 GiB peak RSS).

For the 12-slot configuration, source structure bounds read-phase workers at 12,
read/decode calls at 72, daily input scopes at 60, and cache-holding workers at 12.
Those are structural upper bounds, not measurements retroactively added to the
older 12-slot trace, which did not record these new activity counters.

- [Read concurrency and memory chart](../results/api/rust-read-pressure-240-purged/read-pressure.png)
- [Activity peak statistics](../results/api/rust-read-pressure-240-purged/read-pressure-stats.json)
- [Output, worker-event and swap-counter validation](../results/api/rust-read-pressure-240-purged/validation.json)
- [Exact worker admission/release order](../results/api/rust-read-pressure-240-purged/worker-order.csv)
- [Full sampled evidence](../results/api/rust-read-pressure-240-purged/summary.json)
