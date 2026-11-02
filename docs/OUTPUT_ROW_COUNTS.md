# Output rows for the current one-portfolio dataset

CSV and Parquet inputs are named `portfolio_PF.csv` and
`portfolio_PF.parquet`. Renaming files does not change the stored Entity value.
The dataset covers 1,095 dates, 2023-01-01 through 2025-12-30, with 60,000
fixed portfolio holdings (59,000 US and 1,000 DE). The index has 100,000
holdings (99,000 US and 1,000 DE). Only the 1,000 DE IDs are shared.

These counts use the default scenario periods and both base and local return
types. They exclude the header and the separate metrics file. CSV and Parquet
inputs produce the same report grain, with CSV or native Parquet output respectively. Adding the three
[contribution columns](CONTRIBUTIONS.md) does not change these counts.

| Scenario | Default output grain | Full-history output rows |
|---|---|---:|
| Security | Every instrument in the portfolio/index union / return type | 318,000 |
| Subset | Selected held instrument / return type, plus selected benchmark-only bucket if needed | 2H + 2B |
| Country | US / date / return type, after US-only normalization | 2,190 |
| Weekly | Portfolio / observed Monday–Sunday week / return type | 316 |
| Monthly | Portfolio / observed calendar month / return type | 72 |

Security has (59,000 portfolio-only US + 1,000 shared DE + 99,000
index-only US) × 2 = 318,000 detail rows. All 159,000 union IDs are emitted
individually after full-history linking; there is no security benchmark-only
summary bucket. Country has 1,095 × 2 rows. There are
158 observed weekly buckets (including partial boundary weeks) and 36 monthly
buckets. Counts were derived from all 1,095 Parquet date row groups and the
verified fixed holdings; these are expected counts, not new full-history
benchmark measurements.

Only the security output contract changed to full-union detail. Subset still
uses its compact report; weekly/monthly still aggregate the daily security
union, and country computes effects on the normalized US security union before summing by country.

For subset, H is the number of selected IDs present in the portfolio. B is
one if at least one selected ID is benchmark-only, otherwise zero. The subset
must have positive selected weight on both sides on every date. For example,
selecting `S100001` (portfolio-only), `S159001` (shared DE) and `S000001`
(index-only) yields H=2 and B=1: six output rows. No subset file was prescribed,
so there is no single fixed row count for this scenario. Explicit period
overrides change counts for scenarios other than country.

## US country scenario

Both engines validate the full input date, then retain only securities whose
master country is US on each side. Portfolio and index US weights are each
normalized separately to one. Weighted base/local returns and BF effects are
calculated within that US universe and emitted daily. `PeriodStart` equals
`PeriodEnd`, `Days=1`, and `Segment=US`. DE holdings do not contribute.
Zero US weight on either side fails. Country defaults to `--period day` and
rejects other periods so an explicit flag cannot silently change daily output.

Country effects are sums of security-level BF effects, calculated before country
aggregation. Include both portfolio-only and benchmark-only securities. For this
fixture the US ID sets are disjoint: 158,000 security pairs per date, with three
effects for each of base/local returns, or 1,038,060,000 effect values across all
dates. Under the existing missing-side-return convention, selection and
interaction are zero and allocation carries active return up to rounding.
Shared-ID fixtures can have nonzero selection and interaction. This is standalone
US attribution within separately normalized US universes.

For example:

```sh
./rust/target/release/portfolio-attribution --data attribution_data_parquet \
  --scenario country --output-dir results/us-daily-rust --metrics results/us-daily-rust/metrics.csv
./benchmark-go-attribution --data attribution_data \
  --scenario country --output-dir results/us-daily-go --metrics results/us-daily-go/metrics.csv
```
