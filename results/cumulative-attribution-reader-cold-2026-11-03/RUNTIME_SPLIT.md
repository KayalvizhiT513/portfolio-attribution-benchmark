# Cumulative US Total runtime split

Seconds; attribution reader only. Medians when repetitions exceed one.
Read-pipeline time is directly measured elapsed time. Purge is excluded from engine runtime.

| Step | Go CSV | Rust CSV | Go Parquet | Rust Parquet |
|---|---:|---:|---:|---:|
| Load security master | 1.200018 | 0.584592 | 1.402638 | 1.082735 |
| Discover date ranges | 8.699638 | 8.236736 | 0.011839 | 0.005222 |
| Read / US filter / normalize / daily totals | 9.185012 | 5.765433 | 8.912770 | 5.329436 |
| Calculate cumulative statistics for 36 month-ends | 0.000098 | 0.000240 | 0.000327 | 0.000413 |
| Write output | 0.009469 | 0.000180 | 0.005797 | 0.000172 |
| Other setup / alignment / finalization | 0.000636 | 0.001307 | 0.000578 | 0.001629 |
| Total engine runtime | 19.094870 | 14.588489 | 10.333949 | 6.419608 |
| Cache preparation (excluded) | 0.247704 | 0.348002 | 0.307456 | 0.329737 |
| Total process elapsed | 19.388926 | 14.941702 | 10.655515 | 6.705918 |

Reading, filtering, normalization and daily aggregation overlap across workers;
they are included together in the directly measured read-pipeline interval.
Other time is the residual after subtracting the five directly measured phases.
Summed worker durations are not added to elapsed phases.
Per-cell medians may not sum exactly when repetitions exceed one.
Raw evidence: runs.json, per-run metrics.csv and usage.json; output parity: checks.json.
