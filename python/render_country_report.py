"""Render the country-only report from the existing full-history measurements."""
import json

import benchmark_csv_parquet as benchmark
from render_csv_parquet_report import metrics_table, timing_table

REPORT = benchmark.ROOT / 'COUNTRY_REPORT.md'


def main():
    runs = json.loads((benchmark.OUT / 'runs.json').read_text())
    checks = json.loads((benchmark.OUT / 'checks.json').read_text())
    country = [r for r in runs if r['scenario'] == 'country']
    country_checks = [c for c in checks if c['scenario'] == 'country']
    assert len(country) == 4 and len(country_checks) == 3
    assert all(c['status'] == 'PASS' for c in country_checks)
    assert all(int(r['days']) == 1095 and int(r['output_rows']) == 2190 for r in country)
    rows = {(r['format'], r['scenario'], r['language']): r for r in country}
    flow = 'docs/flows/csv-parquet/country.svg'
    assert (benchmark.ROOT / flow).is_file()
    lines = ['# Country attribution: US only', '',
             'Keep US holdings in the portfolio and index. Scale each side’s US weights to 100%, then calculate daily Brinson–Fachler effects and return contributions.', '',
             '**Scope:** 1,095 dates (2023-01-01 to 2025-12-30), 59,000 US portfolio holdings and 99,000 US index holdings per date. DE holdings are excluded.', '',
             '**Output:** 2,190 rows per run—one US group × 1,095 dates × base/local returns. CSV input writes CSV; Parquet input writes Parquet. All reports include the same 20 columns.', '',
             '## Total runtime', '',
             '| Input → output | Rust seconds | Go seconds | Output rows |',
             '|---|---:|---:|---:|']
    for fmt in ['csv', 'parquet']:
        a, b = rows[fmt, 'country', 'rust'], rows[fmt, 'country', 'go']
        lines.append(f"| {fmt.upper()} → {fmt.upper()} | {a['process_wall_seconds']:.3f} | {b['process_wall_seconds']:.3f} | 2,190 |")
    lines += ['', '## Time by stage', '',
              'The index and portfolio workers can run at the same time. Stage figures add their time together; **Whole run** is how long you actually wait. Do not add the stage rows to estimate runtime.', '']
    for fmt in ['csv', 'parquet']:
        lines += [f'### {fmt.upper()} input → {fmt.upper()} output', '']
        lines += timing_table(rows, fmt, 'country')
        lines += ['', '<details>', f'<summary>Counts, throughput, and memory — {fmt.upper()}</summary>', '',
                  'Steps follow the timing table. Rates use the time spent in that step.', '']
        lines += metrics_table(rows, fmt, 'country')
        lines += ['', 'MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. Repeat-run ranges are unavailable because each configuration was measured once.', '', '</details>', '']
    lines += ['<details>', '<summary>View the country flow diagram</summary>', '',
              f'[![Country: CSV/Parquet paths and Rust/Go stages]({flow})]({flow})', '', '</details>', '',
              '<details>', '<summary>Calculation and worker details</summary>', '',
              'US weights and weighted base/local returns are added into one US group separately for the portfolio and index on each date. There is no linking across dates in this scenario. With one normalized country group, allocation and interaction are zero apart from rounding; selection equals the difference between US portfolio and US index returns.', '',
              'Daily portfolio and benchmark contributions equal the corresponding US weighted returns. Active contribution is their difference. The index producer runs concurrently with one portfolio worker; stages inside the portfolio worker run in sequence. Parquet uses Arrow for reading and writing, while the calculations use the shared daily buffers.', '', '</details>', '',
              '## Checks and measurement notes', '',
              '**3/3 comparisons passed:** CSV Rust/Go, Parquet Rust/Go, and CSV/Parquet. Every output value is compared; each engine checks BF and contribution totals.', '',
              'These are the existing full-history measurements, not a new run. One run per language/format, four threads configured, CSV first, normal filesystem caching. Small timing differences need repeat runs to confirm.', '',
              '[Full comparison](CSV_PARQUET_REPORT.md) · [Raw measurements](results/csv-parquet-native-output/runs.csv) · [Checks](results/csv-parquet-native-output/checks.json) · [Calculation definitions](docs/CONTRIBUTIONS.md)', '']
    REPORT.write_text('\n'.join(lines))
    print(f'Updated {REPORT}')


if __name__ == '__main__':
    main()
