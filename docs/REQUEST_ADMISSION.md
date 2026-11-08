# Request admission before input planning

Rust API mode now defaults to **12 admitted requests**, configured with
`--request-limit 12`. This is separate from `--worker-limit 12`, which caps date
workers across all admitted requests. The admission permit is acquired before
creating request output directories, planning file metadata, or validating
the shared SMF source. It stays held until calculation and associated cleanup finish.

For 20 simultaneous requests, at most 12 can enter planning/calculation; the
remaining requests wait without loading input metadata or validating the shared source.
Each admitted request can have date workers queued for the 12 shared worker
slots. Request admission does not reserve all date-worker slots, so partially
admitted requests cannot deadlock each other. Both gates use FIFO ticket order.
Errors and panic unwinding release permits automatically.

`GET /health` adds:

- `request_limit`: maximum admitted requests.
- `running_requests`: requests currently holding admission, including planning
  and waiting for their date workers.
- `waiting_requests`: requests waiting for admission before planning.
- `peak_running_requests`: persistent peak admitted count.

The existing `active` field counts outstanding requests, including admission
waiters. It can therefore exceed `running_requests`; this does not mean waiters
have loaded input metadata. Existing worker/read activity counters remain independent.
Health endpoints and malformed-request rejection do not wait for admission.

Successful responses add `request_admission_wait_seconds` and
`admitted_calculation_seconds`. `elapsed_seconds` includes admission waiting,
so queued time is not hidden from API latency. Detailed stage profiles start
inside admission; their planning/worker phases exclude the request admission wait.
Client timings continue to include the complete round trip.

The load driver forwards `--request-limit N` when supplied; Rust defaults to 12
when omitted. This is a Rust API feature, not a change to the Go adapter or CLI
calculation scheduling. The admission gate caps request concurrency. The subsequent server-wide SMF
sharing change loads the immutable SMF plan once at startup; admitted
requests then perform only cheap source fingerprint checks. See
[shared SMF](SHARED_SMF.md).

Validate with daily snapshots:

```sh
.venv/bin/python python/test_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --worker-limit 2 --request-limit 2 --requests 6 --snapshots
.venv/bin/python python/test_attribution_api.py \
  --binary rust/target/release/portfolio-attribution \
  --worker-limit 12 --request-limit 12 --requests 20 --snapshots
```

These checks compare outputs with the independent oracle and verify request and
worker limits, trace replay, isolated outputs and recovery after errors.
