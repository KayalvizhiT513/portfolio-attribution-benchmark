# Server-wide immutable SMF sharing

The Rust API loads one immutable SMF plan at server startup. All workers across
requests reference its Parquet footer metadata and row-group/date mappings through
`Arc`. Admitted requests check file size and modification time before reusing it.
If the source changes, restart the server to load the new dataset version.

Daily snapshots are the supported production approach. Each worker reads and
parses its current snapshot into private state, replacing it on the next date.
Date-grouped history remains available for snapshot preparation and validation;
its mutable calculation state stays private to each worker.

Request admission defaults to 12 requests and the independent worker gate defaults
to 12 active date workers. `GET /health` exposes `shared_smf`:

- `scope`: server.
- `mode`: daily_snapshot or date_grouped.
- `metadata_loads`: 1 for the server lifetime.
- `startup_load_seconds`: time spent opening the shared SMF plan.
- `request_uses`: requests that successfully validated and reused the plan.
- `state_scope`: worker.

Per-request stage profiles include `smf_shared_at_server: true`. The load driver
writes startup timing and health to `startup.json`. Report server startup
separately from request and batch latency. Only source metadata is shared; parsed snapshot states are private to workers.
