# Benchmark result catalog — archived 2026-11-08

Times below are observed runs, not guarantees. Dates use Asia/Kolkata. Complete commands, inputs, cache policy, schemas, per-response metrics, and stage timings are in [results.json](results.json). Original evidence is under `runs/`.

| Run date (IST) | Run | Aggregate | API requests / successful | Batch seconds | Peak sampled RSS GiB |
|---|---|---|---:|---:|---:|
| 2026-11-06T04:16:25 | accounts-100k | daily (legacy default) | 2 / 2 | 0.120, 0.104 | 0.482 |
| 2026-11-06T04:26:17 | baseline-comparison | daily (legacy default) | 2 / 2 | 0.051, 0.042 | 0.245 |
| 2026-11-06T04:06:47 | current-full | daily (legacy default) | 14 / 14 | 11.271, 10.787, 20.004, 20.084, 94.335, 86.010 | 8.167 |
| 2026-11-07T07:59:18 | daily-heavy-c1-profile | daily (legacy default) | 1 / 1 | 30.410 | 1.540 |
| 2026-11-07T07:43:08 | daily-heavy-c1-run1 | daily (legacy default) | 1 / 1 | 30.605 | 1.558 |
| 2026-11-07T07:48:45 | daily-heavy-c20-run1 | daily (legacy default) | 0 / 0 |  | 2.666 |
| 2026-11-08T12:08:39 | daily-snapshot-aggregate-security-direct-c1 | security | 1 / 1 | 14.039 | 2.080 |
| 2026-11-08T11:04:20 | daily-snapshot-aggregate-security-no-cache-c1 | security | 1 / 1 | 16.580 | 2.452 |
| 2026-11-08T10:54:21 | daily-snapshot-aggregate-total-c1 | total | 1 / 1 | 31.174 | 1.033 |
| 2026-11-08T12:04:58 | daily-snapshot-aggregate-total-direct-c1 | total | 1 / 1 | 11.583 | 1.361 |
| 2026-11-08T11:02:15 | daily-snapshot-aggregate-total-no-cache-c1 | total | 1 / 1 | 15.612 | 1.723 |
| 2026-11-07T12:18:20 | daily-snapshot-c1 | daily (legacy default) | 1 / 1 | 14.101 | 1.513 |
| 2026-11-08T11:50:13 | daily-snapshot-daily-output-c1 | daily | 1 / 1 | 13.368 | 1.347 |
| 2026-11-08T12:09:59 | daily-snapshot-daily-output-direct-c1 | daily | 1 / 1 | 12.916 | 1.377 |
| 2026-11-08T16:20:46 | daily-snapshot-dated-sector-c1 | sector | 1 / 1 | 14.382 | 1.467 |
| 2026-11-08T15:34:45 | daily-snapshot-final-daily-c1 | daily | 1 / 1 | 13.047 | 1.330 |
| 2026-11-08T16:03:08 | daily-snapshot-final-sector-c1 | sector | 1 / 1 | 13.507 | 1.473 |
| 2026-11-08T15:36:50 | daily-snapshot-final-security-c1 | security | 1 / 1 | 13.910 | 2.069 |
| 2026-11-08T15:35:43 | daily-snapshot-final-total-c1 | total | 1 / 1 | 12.860 | 1.331 |
| 2026-11-06T04:26:07 | history-3650 | daily (legacy default) | 2 / 2 | 0.072, 0.069 | 0.287 |
| 2026-11-07T12:55:28 | monthly-staggered-indexed-c1 | daily (legacy default) | 1 / 1 | 10.648 | 1.619 |
| 2026-11-07T13:11:10 | monthly-staggered-indexed-c20 | daily (legacy default) | 20 / 20 | 333.901 | 5.376 |
| 2026-11-07T12:57:12 | monthly-staggered-snapshot-c1 | daily (legacy default) | 1 / 1 | 13.319 | 1.585 |
| 2026-11-07T19:08:42 | monthly-staggered-snapshot-private-c20 | daily (legacy default) | 20 / 20 | 251.164 | 2.868 |
| 2026-11-06T04:53:20 | rust-full-20 | daily (legacy default) | 20 / 20 | 841.829 | 9.206 |
| 2026-11-07T05:25:13 | rust-read-pressure-240-purged | daily (legacy default) | 20 / 20 | 254.883 | 12.326 |
| 2026-11-06T22:41:31 | rust-shared12-full-20 | daily (legacy default) | 20 / 20 | 153.147 | 2.148 |
| 2026-11-06T22:49:17 | rust-worker-race-20 | daily (legacy default) | 20 / 20 | 158.337 | 2.294 |
| 2026-11-06T22:52:52 | rust-worker-race-20-purged | daily (legacy default) | 20 / 20 | 147.073 | 2.341 |
| 2026-11-06T04:07:56 | securities-100k | daily (legacy default) | 2 / 2 | 0.208, 0.166 | 1.499 |
| 2026-11-07T11:37:58 | security-first-layout-check | daily (legacy default) | 1 / 0 | 0.018 | 0.018 |
| 2026-11-07T11:47:13 | security-indexed-c1 | daily (legacy default) | 1 / 1 | 15.098 | 2.991 |
| 2026-11-06T02:55:27 | smoke-allowed | daily (legacy default) | 14 / 14 | 0.048, 0.033, 0.071, 0.072, 0.115, 0.110 | 0.782 |
