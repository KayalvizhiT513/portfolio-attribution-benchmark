# Archived Go profiling: date caching, Arrow decoding and memory

Archived experiment with a different dataset or execution path. See the [current country attribution performance report](COUNTRY_REPORT.md) for optimized CSV and Parquet results.

Complete: 14/14 normal runs; 7/7 diagnostic runs. All use all 1,095 dates and four configured threads.

Date32 text is now cached until the date changes, including across batches. CSV parsing, calculations, native output formats and batch sizes are unchanged. Normal timing runs do not enable the diagnostic clocks, CPU sampling or memory snapshots.

## Normal runtime after the fix

Country uses the median of three alternating CSV/Parquet pairs; other scenarios use one pair. Read time combines the two input workers and can exceed whole-run time.

| Scenario | CSV read s | Parquet read s | CSV whole run s | Parquet whole run s |
|---|---:|---:|---:|---:|
| Security | 39.380 | 26.725 | 23.915 | 17.468 |
| Country | 38.821 | 26.925 | 23.218 | 16.772 |
| Weekly | 40.214 | 27.657 | 24.163 | 17.420 |
| Monthly | 39.542 | 27.751 | 23.751 | 17.464 |
| Subset | 38.959 | 27.290 | 23.058 | 16.787 |

Country: cached Parquet takes **27.8% less time** than CSV in the three-pair comparison.


## Country diagnostics: read time

These are separate profiled runs. Parquet has one timer around each batch fetch; the remaining read time covers typed access, lookup, validation and daily-buffer construction. CSV decode/row CPU costs are estimated from sampled stacks, avoiding an expensive timer on every CSV row.

| Mode | Batch read/decode s | Row handling + buffers s | Whole run s |
|---|---:|---:|---:|
| csv | CPU sampling below | CPU sampling below | 23.427 |
| parquet | 16.004 | 11.307 | 17.344 |
| parquet-cache-off | 15.680 | 21.406 | 23.206 |

Batch time includes file I/O, decompression, decoding and Arrow batch creation. It is not pure decompression time. Country has three diagnostic runs per format; cache-off is a single same-binary control with the old per-row formatting restored.

## Allocations and garbage collection

These cover the whole process, including setup, reading and output. Allocated bytes are cumulative, not peak memory. Allocation counts/bytes and pause totals come from runtime counters. GC CPU figures are Go-runtime estimates that may overestimate CPU use; they are not directly comparable with OS CPU time. GC work overlaps stage/runtime measurements; do not add it to runtime.

| Metric | CSV | Parquet, cached dates | Parquet, cache off |
|---|---:|---:|---:|
| Allocated GiB | 11.577 | 80.389 | 83.004 |
| Allocated objects (millions) | 175.698 | 4.243 | 179.449 |
| Garbage collections | 467.000 | 1173.000 | 1302.000 |
| Estimated GC CPU seconds | 1.273 | 6.969 | 7.635 |
| Estimated GC assistance CPU seconds | 0.019 | 0.330 | 0.294 |
| Stop-the-world pause seconds | 0.026 | 0.110 | 0.118 |

The same-binary cache-off control creates about **175.2 million more objects** than cached Parquet. The control is one run; cached values are medians of three.


## Where CPU samples land

Mean estimated CPU seconds from 10 ms samples (three runs for CSV/cached Parquet, one cache-off control). Each sample is counted once: GC, allocation, file reads, heap/runtime support, then the remaining decoding/row handling/calculation/output. Decoding therefore excludes samples charged to file I/O, allocation or GC. Runtime support includes scheduler, semaphore and signal/profiler support. These estimates differ from the runtime GC counters; zero means no samples, not zero work.

| Work | CSV CPU s | Parquet CPU s | Cache-off CPU s |
|---|---:|---:|---:|
| File reads | 17.267 | 7.683 | 8.470 |
| Text parsing / Parquet decoding | 10.380 | 2.707 | 2.730 |
| Row handling and ID lookup | 2.850 | 4.460 | 8.480 |
| Allocate / resize memory | 0.543 | 0.733 | 1.400 |
| Garbage collection | 0.780 | 2.903 | 3.080 |
| Release unused heap memory | 0.037 | 1.800 | 2.690 |
| Go runtime / profiling support | 4.513 | 10.207 | 11.830 |
| Calculate BF and contributions | 0.003 | 0.003 | 0.000 |
| Write reports | 0.027 | 0.013 | 0.020 |
| Set up inputs | 0.020 | 0.007 | 0.000 |
| Other | 0.157 | 0.243 | 0.370 |
| Total sampled CPU | 36.577 | 30.760 | 39.070 |
| OS-measured process CPU | 41.187 | 34.607 | 45.327 |

The profiles point to file reads and text parsing as the largest CSV costs. Parquet allocation profiles point to Arrow buffers and dictionary-decoding buffers. These findings separate memory/GC activity from row handling; they do not claim that GC pause time explains the entire runtime.


<details>
<summary>Allocation sites — csv</summary>

Allocation profiles are sampled by bytes; the counters above are exact runtime totals. No forced garbage collection is introduced.

```text
File: benchmark-go-attribution
Type: alloc_space
Time: 2026-11-01 13:47:10 IST
Showing nodes accounting for 11.29GB, 99.59% of 11.34GB total
Dropped 29 nodes (cum <= 0.06GB)
      flat  flat%   sum%        cum   cum%
   11.26GB 99.29% 99.29%    11.26GB 99.29%  encoding/csv.(*Reader).readRecord
    0.03GB   0.3% 99.59%    11.29GB 99.57%  main.(*stream).next
         0     0% 99.59%    11.26GB 99.29%  encoding/csv.(*Reader).Read
         0     0% 99.59%    11.26GB 99.29%  main.(*csvInput).next
         0     0% 99.59%     6.41GB 56.53%  main.main
         0     0% 99.59%     6.41GB 56.51%  main.run
         0     0% 99.59%     4.93GB 43.45%  main.run.func1
         0     0% 99.59%     4.93GB 43.45%  main.worker
         0     0% 99.59%     6.41GB 56.54%  runtime.main
```

</details>

<details>
<summary>Allocation sites — parquet</summary>

Allocation profiles are sampled by bytes; the counters above are exact runtime totals. No forced garbage collection is introduced.

```text
File: benchmark-go-attribution
Type: alloc_space
Time: 2026-11-01 13:46:47 IST
Showing nodes accounting for 81944.93MB, 99.68% of 82208.92MB total
Dropped 190 nodes (cum <= 411.04MB)
Showing top 12 nodes out of 53
      flat  flat%   sum%        cum   cum%
28590.79MB 34.78% 34.78% 28590.79MB 34.78%  github.com/apache/arrow-go/v18/arrow/memory.(*GoAllocator).Allocate (partial-inline)
24594.54MB 29.92% 64.70% 24594.54MB 29.92%  github.com/apache/arrow-go/v18/parquet/internal/encoding.(*dictConverter[go.shape.float64]).ensure
22661.22MB 27.57% 92.26% 22661.22MB 27.57%  github.com/apache/arrow-go/v18/parquet/internal/encoding.(*dictConverter[go.shape.[]uint8]).ensure
 5152.88MB  6.27% 98.53%  5153.38MB  6.27%  github.com/apache/arrow-go/v18/parquet.(*ReaderProperties).GetStream
  675.40MB  0.82% 99.35%   675.40MB  0.82%  github.com/apache/arrow-go/v18/parquet/pqarrow.transferInt
  115.45MB  0.14% 99.49% 24711.99MB 30.06%  github.com/apache/arrow-go/v18/parquet/internal/utils.(*TypedRleDecoder[go.shape.float64]).GetBatchWithDict
  101.90MB  0.12% 99.61% 22764.11MB 27.69%  github.com/apache/arrow-go/v18/parquet/internal/utils.(*TypedRleDecoder[go.shape.[]uint8]).GetBatchWithDict
   35.48MB 0.043% 99.66% 82125.20MB 99.90%  main.(*stream).next
      11MB 0.013% 99.67% 10732.41MB 13.06%  github.com/apache/arrow-go/v18/parquet/file.(*serializedPageReader).Next
    5.50MB 0.0067% 99.68%  5420.26MB  6.59%  github.com/apache/arrow-go/v18/parquet/file.(*RowGroupReader).GetColumnPageReader
    0.77MB 0.00093% 99.68% 29345.99MB 35.70%  github.com/apache/arrow-go/v18/parquet/file.(*byteArrayRecordReader).ReadValuesSpaced
         0     0% 99.68%  6582.13MB  8.01%  github.com/apache/arrow-go/v18/arrow/array.(*BinaryBuilder).Append
```

</details>

<details>
<summary>Allocation sites — parquet-cache-off</summary>

Allocation profiles are sampled by bytes; the counters above are exact runtime totals. No forced garbage collection is introduced.

```text
File: benchmark-go-attribution
Type: alloc_space
Time: 2026-11-01 13:48:15 IST
Showing nodes accounting for 84461.94MB, 99.69% of 84721.10MB total
Dropped 194 nodes (cum <= 423.61MB)
Showing top 12 nodes out of 56
      flat  flat%   sum%        cum   cum%
28458.05MB 33.59% 33.59% 28458.05MB 33.59%  github.com/apache/arrow-go/v18/arrow/memory.(*GoAllocator).Allocate (partial-inline)
24614.73MB 29.05% 62.64% 24614.73MB 29.05%  github.com/apache/arrow-go/v18/parquet/internal/encoding.(*dictConverter[go.shape.float64]).ensure
22705.62MB 26.80% 89.44% 22705.62MB 26.80%  github.com/apache/arrow-go/v18/parquet/internal/encoding.(*dictConverter[go.shape.[]uint8]).ensure
 5066.90MB  5.98% 95.43%  5069.40MB  5.98%  github.com/apache/arrow-go/v18/parquet.(*ReaderProperties).GetStream
 2692.04MB  3.18% 98.60%  2692.04MB  3.18%  time.Time.Format
  649.35MB  0.77% 99.37%   651.35MB  0.77%  github.com/apache/arrow-go/v18/parquet/pqarrow.transferInt
  138.54MB  0.16% 99.53% 24753.27MB 29.22%  github.com/apache/arrow-go/v18/parquet/internal/utils.(*TypedRleDecoder[go.shape.float64]).GetBatchWithDict
   89.35MB  0.11% 99.64% 22798.47MB 26.91%  github.com/apache/arrow-go/v18/parquet/internal/utils.(*TypedRleDecoder[go.shape.[]uint8]).GetBatchWithDict
   33.79MB  0.04% 99.68% 84644.96MB 99.91%  main.(*stream).next
    6.50MB 0.0077% 99.69% 10670.36MB 12.59%  github.com/apache/arrow-go/v18/parquet/file.(*serializedPageReader).Next
       4MB 0.0047% 99.69%  5317.98MB  6.28%  github.com/apache/arrow-go/v18/parquet/file.(*RowGroupReader).GetColumnPageReader
    3.06MB 0.0036% 99.69% 29302.20MB 34.59%  github.com/apache/arrow-go/v18/parquet/file.(*byteArrayRecordReader).ReadValuesSpaced
```

</details>

Checks passed: 35 normal-output comparisons and 14 diagnostic comparisons. Every field is compared; BF/contribution reconciliation remains enabled.

[Normal measurements](results/go-date-cache/runs.csv) · [Normal checks](results/go-date-cache/checks.json) · [Diagnostic measurements](results/go-date-cache/profile/runs.json) · [Diagnostic checks](results/go-date-cache/profile/checks.json) · [Settings](results/go-date-cache/environment.json)
