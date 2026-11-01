# CSV and Parquet input adapters

Rust now caches formatted date32 text until the integer date changes, including
across batches. It no longer allocates a date string for every return record.
See the [follow-up measurements](../RUST_DATE_READ_REPORT.md).

Go now uses the same date cache. [Go measurements](../GO_DATE_READ_REPORT.md)
separate normal timing runs from optional CPU/allocation/GC diagnostics.
`ATTRIBUTION_CPU_PROFILE=FILE` enables CPU sampling,
`ATTRIBUTION_ALLOC_PROFILE=FILE` writes sampled allocation stacks, and
`ATTRIBUTION_RUNTIME_STATS=FILE` writes process-wide allocation/GC statistics;
the GC CPU values are Go-runtime estimates and may overestimate CPU use.
`ATTRIBUTION_PROFILE_READ=1` also adds Go batch read/decode and daily-buffer
timings, with the same counter names as Rust. No per-row clocks are introduced.
For a controlled comparison only, `ATTRIBUTION_DISABLE_DATE_CACHE=1` restores
the old Go date-formatting path; the cache is enabled by default.

For optional Rust read profiling, set `ATTRIBUTION_PROFILE_READ=1`. Metrics then
append `parquet_batch_read_decode_seconds`, `parquet_to_daily_buffer_seconds`
and `parquet_batches`. The first timer wraps batch fetches (file I/O,
decompression, decoding and Arrow materialization). The second is the remainder
of the read stage: typed access, date caching, ID lookup, validation and daily
buffer construction. These are combined reader elapsed times, not CPU times;
their sum matches `read_parse_seconds` for pure Parquet runs. Clocks are sampled
per batch/date, never per input row. Normal metrics remain unchanged when the
environment variable is unset.

Both native attribution CLIs now implement Phase 2 of the supplied migration
plan: replaceable storage readers feeding the existing daily dense arrays. Rust
uses `parquet::arrow::ParquetRecordBatchReaderBuilder`; Go uses Apache Arrow Go's
`pqarrow` record reader. Float64 columns enter the engine directly, without a
Parquet-to-CSV intermediate or numeric text parsing.

Required columns are projected by name into Arrow batches of up to 65,536 rows.
The adapters preserve source row order and keep one-row lookahead at a date
boundary. A complete date can span multiple batches or row groups; a batch can
contain multiple dates. The input adapters preserve normalization and integer master lookup,
subset selection, BF calculations, forward linking,
reconciliation, portfolio workers and report columns/row counts are preserved. Output now follows each portfolio input format.
There is no new DataFusion, async runtime, Rayon or explicit SIMD calculation
path. Subset selection still follows decoding and full-day validation; there is
no claim of instrument predicate pushdown or row-group skipping.

Memory remains bounded by the master, daily calculation state, current Arrow
batches and decoder buffers, bounded index broadcasts and active period
accumulators. Parquet decoders also retain metadata and may buffer compressed
pages/column chunks according to the file's physical layout; batch size alone
is not a hard cap on total RSS. Whole-history tables are not materialized.

Country attribution now filters both inputs to US, normalizes each side
separately and emits daily rows. See [current output counts and country
semantics](OUTPUT_ROW_COUNTS.md). Security now writes every instrument in the
portfolio/index outer-join union individually; other scenario output contracts
are unchanged.

## Files and types

`--data DIR` discovers either extension for each logical input:

| Stem | Required fields |
|---|---|
| `security_master` | InstrumentId, Currency, Country |
| `index` | Entity, EffectiveDate, InstrumentId, Weight, BaseReturn, LocalReturn |
| `portfolio_*` | Entity, EffectiveDate, InstrumentId, Weight, BaseReturn, LocalReturn |

Different stems may use different formats. Both `index.csv` and `index.parquet`
in the same directory are an error; this applies equally to the master and each
portfolio. Report filenames use the input stem, regardless of input extension.
`--instruments-file` also accepts CSV or Parquet.

CSV headers and parsing retain their existing contracts. Parquet required
columns can be reordered; extra columns are ignored. Strings accept Arrow UTF-8,
large UTF-8 and UTF-8 view types. EffectiveDate accepts date32 (days since epoch)
or an ISO `YYYY-MM-DD` string. Weight/BaseReturn/LocalReturn must be float64.
Unsupported types, missing or duplicate required columns, null required values
and decoding errors fail the run. Required columns must be flat scalar fields.
Rust enables uncompressed, Snappy and Zstandard input; the supplied conversion
uses Zstandard. No new final reports are published on input failure; partial
files may remain, consistent with the original engine.

## Build and run

Use stable Rust and Go 1.25+. Python tests require PyArrow.

```sh
cargo build --release --manifest-path rust/Cargo.toml
go -C go build -o ../benchmark-go-attribution ./cmd/attribution

./rust/target/release/portfolio-attribution --data attribution_data_parquet \
  --scenario country --output-dir results/parquet-rust --metrics results/parquet-rust/metrics.csv
./benchmark-go-attribution --data attribution_data_parquet \
  --scenario country --output-dir results/parquet-go --metrics results/parquet-go/metrics.csv
```

Use fresh output directories. Existing scenario and period flags work with both
formats. CSV portfolio inputs produce CSV reports; Parquet portfolio inputs produce native Parquet reports. In mixed directories, each portfolio determines its own output format. Metrics remain CSV.

## Validation performed

```sh
cargo test --manifest-path rust/Cargo.toml
go -C go test ./...
.venv/bin/python python/test_attribution.py
.venv/bin/python python/test_parquet_input.py
```

The independent oracle passes all nine scenario/period combinations for both
languages. The Parquet suite compares every report field and logical work count
between CSV, Parquet and mixed directories for the same fixtures. It covers all
five scenarios, Parquet subset lists, partial runs, reordered fields, ignored
nested extras, small row groups splitting dates, and dates containing 65,539
holdings across a 65,536-row batch boundary. Invalid schemas, nulls, NaN,
returns at -1, unknown IDs, mixed entities, duplicate holdings, unsorted dates
and ambiguous duplicate input formats are rejected.

Before merging the portfolios, both engines also completed all five scenarios
for the first two dates of the then-current `attribution_data_parquet` files. Every output field matched across
languages. Country output contained 16 rows (US and DE, four portfolios, two
return types). This is a smoke test, not a full-history performance benchmark.

## Comparing performance

`read_parse_seconds` includes Parquet decoding, decompression, typed access,
lookup and validation. Reader construction/schema work remains setup overhead,
as CSV reader construction did. Other stage definitions are unchanged.
`parsed_input_bytes` counts CSV parser offsets only and is zero for pure
Parquet runs; do not use it as a Parquet disk-throughput measure.
`input_file_bytes` records physical file sizes, with the existing Rust accounting
for repeated index passes when portfolio worker batches require them.

The current CSV and Parquet folders now contain the same merged portfolio and
index, sharing exactly the 1,000 DE securities and no US securities. CSV export
compares every parsed value against its typed Parquet source. Both formats can
therefore be compared using the same holdings and numeric values. Use comparable
cache conditions and record physical file layouts and conversion cost separately
before attributing a speedup to Parquet. Historical benchmark reports and their
four-portfolio harness assumptions have not been updated for this merged fixture.

Official reader APIs:
[Rust Arrow/Parquet](https://docs.rs/parquet/60.1.0/parquet/arrow/),
[Go pqarrow](https://pkg.go.dev/github.com/apache/arrow-go/v18@v18.8.0/parquet/pqarrow).

## Native Parquet report writer

Both languages build typed Arrow output batches and write Zstandard-compressed
Parquet, using at most 65,536 rows per batch/row group. No intermediate CSV is
created. Dates use date32, Days uses uint32, labels use UTF-8 and result values
use float64. Rust uses ArrowWriter; Go uses pqarrow.FileWriter. Footer completion,
flush/fsync and final publication are part of the successful output lifecycle.
The write-stage clock includes typed builder appends, encoding, compression and
footer/flush work. Failed runs can leave partial files but publish no new final
reports. The latest benchmark is in [CSV_PARQUET_REPORT.md](../CSV_PARQUET_REPORT.md).

### Separate country worker timings

`python/benchmark_country_stages.py` runs the full country scenario three times
per engine and input format, sequentially, alternating engine/format order.
The native metrics append `index_{read,filter,aggregate,compute,write}_seconds`
and `portfolio_{read,filter,aggregate,compute,write}_seconds`.
Each value sums elapsed stage calls within that worker; the index and portfolio
workers overlap. Channel waits and setup are outside these stage timers, so
stage times do not sum to whole-run elapsed time. The index worker prepares
benchmark data; only the portfolio worker calculates attribution and writes it.
For this single-portfolio dataset, the portfolio columns describe one worker;
with multiple portfolio files they sum portfolio workers and must be labeled
accordingly. Existing combined metrics remain for backward compatibility.
The rerun disables diagnostic profiling and checks every output field against
the existing CSV and Parquet reference outputs. Raw evidence is saved under
`results/country-worker-stages/`; earlier experiments are preserved.
