# Full-history output estimates and size-guarded runs

Combined CSV limit per scenario: **5,000,000,000 bytes (5.00 GB)**, across both languages and all four portfolios. Each language receives half the budget. GB is decimal; GiB is binary. All requested native runs use `--max-days 0` (all dates).

## Estimated full-history output

| Scenario | Dates | Output periods | Rows per language | Rust GB | Go GB | Combined GB | Basis |
|---|---:|---:|---:|---:|---:|---:|---|
| security | 1095 | 1095 | 876,008,760 | 185.231 | 182.690 | 367.922 | measured sampled bytes/row × estimated full-history rows |
| subset | 1095 | 1095 | 3,512,760 | 0.759 | 0.741 | 1.500 | previous full-history measurement |
| country | 1095 | 1095 | 17,520 | 0.004 | 0.004 | 0.008 | previous full-history measurement |
| weekly | 1095 | 158 | 126,401,264 | 26.531 | 26.127 | 52.657 | measured sampled bytes/row × estimated full-history rows |
| monthly | 1095 | 36 | 28,800,288 | 6.028 | 5.955 | 11.983 | measured sampled bytes/row × estimated full-history rows |

The synthetic fixture has fixed instrument universes and every calendar date. Daily row cardinalities scale by date count; weekly/monthly row cardinalities scale by calendar bucket count, including partial boundary periods. Variable-width decimal values and date/period labels make extrapolated bytes approximate; allow about ±20% for security/weekly/monthly. This does not generalize to changing holdings without new cardinality sampling. The country/subset sizes were already measured across all dates. Input file names, sizes and modification times are checked against that recorded inventory before estimation.

## Sequential run outcomes

| Scenario | Language | Status | Actual CSV GB | Wall seconds | Full-history result? |
|---|---|---|---:|---:|---|
| security | rust | STOPPED_HARD_FILE_LIMIT | 2.344 | 1.043 | No — partial output |
| security | go | STOPPED_SIZE_GUARD | 2.433 | 1.223 | No — partial output |
| subset | go | COMPLETE | 0.741 | 21.593 | Yes |
| subset | rust | COMPLETE | 0.759 | 13.068 | Yes |
| country | rust | COMPLETE | 0.004 | 13.060 | Yes |
| country | go | COMPLETE | 0.004 | 22.114 | Yes |
| weekly | go | STOPPED_SIZE_GUARD | 2.412 | 2.785 | No — partial output |
| weekly | rust | STOPPED_SIZE_GUARD | 2.410 | 2.106 | No — partial output |
| monthly | rust | STOPPED_SIZE_GUARD | 2.411 | 6.798 | No — partial output |
| monthly | go | STOPPED_SIZE_GUARD | 2.457 | 10.445 | No — partial output |

| Scenario | Combined actual CSV GB | Under 5 GB? | Numerical comparison |
|---|---:|---|---|
| security | 4.776 | Yes | Not compared: at least one run is incomplete |
| subset | 1.500 | Yes | PASS: 3,512,760 rows; max difference 0 |
| country | 0.008 | Yes | PASS: 17,520 rows; max difference 0 |
| weekly | 4.821 | Yes | Not compared: at least one run is incomplete |
| monthly | 4.867 | Yes | Not compared: at least one run is incomplete |

## Check against actual output growth

| Scenario | Initial combined estimate GB | Projection from actual bytes / complete row GB |
|---|---:|---:|
| security | 367.922 | 368.631 |
| subset | 1.500 | 1.500 |
| country | 0.008 | 0.008 |
| weekly | 52.657 | 52.574 |
| monthly | 11.983 | 12.060 |

The second projection uses complete records counted in actual generated CSVs, including guard-stopped files. A truncated last row is not counted; at most one such row per file adds negligible byte-width error. This audits row-width growth for the supplied newline-free synthetic labels; it does not make partial runs complete or remove the fixed-cardinality assumption.


## Guard behavior and interpretation

A monitor samples CSV sizes every 20 ms, including `.partial` files, and terminates a language run near 96% of its allocated half-budget. A hard per-file OS limit divides that allocation among the four portfolio outputs, with reserved space for metrics. The combined guard also checks both languages and stops if free disk falls below 5 GiB. Files remain available for inspection; partial files may end inside a CSV record and must not be treated as valid completed attribution outputs.

The two language runs are sequential, and scenarios are sequential. A stopped oversized scenario does not prevent testing the next scenario. Only COMPLETE runs produce valid full-history stage metrics and are eligible for numerical comparison. Stopped wall/CPU times describe termination under the guard; they are not full-history performance results or evidence of a language winner.

Native metrics and outputs: `results/size-guarded-full-history/<scenario>/<language>/`. Run records and estimates are also saved as JSON. No earlier benchmark outputs are overwritten.

The earlier “180 GiB per language” estimate was imprecise: sampled daily security output projects to about 183–185 **GB**, or 170–173 **GiB**, per language. The estimate is driven by 876 million detail rows, not by input CSV size. This guarded run checks actual size growth rather than relying only on that projection.
