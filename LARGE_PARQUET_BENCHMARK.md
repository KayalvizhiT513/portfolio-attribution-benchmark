# 365-million-row Parquet benchmark

Measured November 3, 2026, on this Mac: 24 GiB physical RAM, 12 logical CPUs,
Python with NumPy and PyArrow 23.0.1. All results use the complete dataset.

| Item | Result |
|---|---:|
| Rows | 365,000,000 |
| Securities per date | 100,000 |
| Dates | 3,650 consecutive days starting 2016-01-01 |
| Columns | SecId int64, Effective Date date32, 15 float64 columns |
| Parquet file | 45,365,904,023 bytes (42.25 GiB) |
| Generation and Parquet write | 53.725 seconds |
| Raw typed values | 48,180,000,000 bytes (44.87 GiB) |
| Sum of decoded Arrow batch sizes | 48,910,000,000 bytes (45.55 GiB) |
| Read/decode every row and column, discarding each batch | 12.367 seconds |
| Peak process RSS during scan | 198,459,392 bytes (189.3 MiB) |
| Peak Arrow allocation during scan | 59,484,928 bytes (56.7 MiB) |
| Full table retained in RAM | Not run: exceeds physical RAM |
| Cold-cache full-table load time | Not measured |

The 12.367-second measurement is a streaming scan, **not** the time to load and
retain the complete table in RAM. Its small RSS reflects discarded batches.
The sum of decoded batch sizes includes Arrow validity buffers beyond the raw
values; it is not a measured RSS for a retained table. A full load needs at least
the value buffers plus validity buffers, metadata, read buffers, and runtime
overhead. A machine with 64 GiB or more is a reasonable target for measuring it.

Cache clearing was attempted with `purge` (operation not permitted), then
`sudo -n purge` (password required). Cache state is uncontrolled; this result
must not be described as a cold-cache benchmark. The file was generated shortly
before reading and is larger than physical RAM.

The fixture uses reproducible independent uniform random float64 values (seed
42), SecIds 1–100,000 on each date, Snappy compression, no dictionary encoding,
and one 100,000-row group per date. Float data distribution affects compression
and timing. The requested 365 × 10 convention is used exactly, rather than ten
calendar years with leap days. Generation writes typed batches directly to one
Parquet file, avoiding an additional very large CSV intermediate.

Artifacts (local, ignored by Git):

- `results/large_parquet/securities.parquet`
- `results/large_parquet/generate.json`
- `results/large_parquet/scan.json`

Reproduce generation and scan:

```sh
python3 python/benchmark_large_parquet.py generate
sudo purge
python3 python/benchmark_large_parquet.py scan
```

On a host with sufficient available RAM, measure actual table materialization
in a fresh process after clearing the cache:

```sh
sudo purge
python3 python/benchmark_large_parquet.py load
```

`load` retains the Arrow table through measurement and records its `nbytes`,
process peak RSS, Arrow peak allocated bytes, and elapsed read time. Peak RSS
units in this script are macOS bytes. No full load was attempted on this host
because memory compression or swapping would prevent an all-resident result.
