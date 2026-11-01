# Contribution columns alongside Brinson effects

Both native engines append these columns to every scenario's CSV or Parquet output:

| Column | Meaning |
|---|---|
| PortfolioContribution | Contribution of this row to the applicable portfolio return |
| BenchmarkContribution | Contribution of this row to the applicable index return |
| ActiveContribution | PortfolioContribution minus BenchmarkContribution |

All existing columns retain their order and meaning. Allocation, Selection,
Interaction and Total remain Brinson-Fachler effects. Row counts and grains are
unchanged: security writes full outer-join instrument detail, subset retains
its selected held detail and benchmark-only bucket, country writes daily US
groups, and weekly/monthly write portfolio totals. Contributions are decimal
returns, separately for base and local return types.

## Daily contributions

Use the same normalized weights and applicable universe as the attribution:

```
portfolio_daily_contribution(i,t) = portfolio_weight(i,t) * portfolio_return(i,t)
benchmark_daily_contribution(i,t) = benchmark_weight(i,t) * benchmark_return(i,t)
```

The sum on each side equals that side's daily return. A missing side of the
outer join contributes zero. The held-side return fallback used by the BF
formulas does not create a contribution on the missing side.

For country, the applicable universe is US only, with portfolio and benchmark
US weights normalized separately to one. Its daily US contribution therefore
equals the respective normalized US return. For subset, normalize within the
selected universe on each side and include the selected benchmark-only bucket.

## Contributions over multiple dates

Each reporting period starts with side-specific growth of one. Using the
growth accumulated before each date:

```
portfolio_contribution(i) = sum_t(portfolio_growth_before(t) * portfolio_daily_contribution(i,t))
benchmark_contribution(i) = sum_t(benchmark_growth_before(t) * benchmark_daily_contribution(i,t))
active_contribution(i) = portfolio_contribution(i) - benchmark_contribution(i)
```

After adding that date's contributions, update each side's growth by multiplying
by one plus its daily total return. This is a beginning-of-period wealth
contribution convention. Growth and contribution accumulators reset at report
boundaries. It is not the average reported weight multiplied by a compounded
instrument return.

For example, if a security contributes 0.01 on day one and 0.02 on day two,
and the entire portfolio returns 0.05 on day one, its two-day portfolio
contribution is `0.01 + 1.05 * 0.02 = 0.031`.

The engine checks, separately for both return types, that sums across all
output rows in a period reconcile to:

```
sum(PortfolioContribution) = PortfolioReturn
sum(BenchmarkContribution) = BenchmarkReturn
sum(ActiveContribution) = ActiveReturn
```

Security detail and subset detail plus its residual preserve these sums.
Weekly/monthly aggregate daily security contributions before linking, so their
single portfolio row carries the full side contributions. Checks use a 1e-8
period tolerance, alongside the existing BF reconciliation.

An individual row's ActiveContribution need not equal its BF Total. BF effects
include benchmark-relative allocation terms and use the existing BF linking
convention. Both sets of rows independently sum to the same active return.

## Verification

`python/test_attribution.py` checks all three new values against an independent
closed-form calculation across nine scenario/period combinations, including
daily US grouping, changing holdings, missing-side instruments, subset residuals
and week/month/quarter/year boundaries. It also verifies period contribution
sums and zero contribution on missing sides. `python/test_parquet_input.py`
compares the new fields across CSV, Parquet and mixed input formats in both
languages.

## Native output format

Each portfolio report follows that portfolio input's extension: CSV → CSV;
Parquet → Parquet, including in mixed-format directories. Metrics remain CSV.
Parquet uses Zstandard compression and bounded Arrow batches/row groups of at
most 65,536 rows. Labels are UTF-8 strings, PeriodStart/PeriodEnd are date32,
Days is uint32, and all 12 numeric result columns are float64. Values go directly
from calculation results into Arrow builders; they are not converted through CSV.
Final publication follows footer completion and fsync, after all inputs validate.
