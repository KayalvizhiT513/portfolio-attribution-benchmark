"""Render every requested stage metric from the recorded native benchmarks."""
import csv
import json
from pathlib import Path
from attribution_flow_content import OVERVIEW, SCENARIO_FLOWS, SHARED_LOGIC

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'ATTRIBUTION_STAGE_COMPARISON.md'
SCENARIOS = [
    ('security', 'Full security-level attribution'),
    ('subset', 'Standalone instrument subset'),
    ('country', 'Country attribution'),
    ('weekly', 'Weekly linked security attribution'),
    ('monthly', 'Monthly linked security attribution'),
]


def load(folder):
    with (folder / 'summary.csv').open(newline='') as f:
        rows = {(r['scenario'], r['language']): r for r in csv.DictReader(f)}
    return rows, json.loads((folder / 'environment.json').read_text())


def table(rows, scenario):
    pair = [rows[scenario, language] for language in ['rust', 'go']]
    f = lambda r, field: float(r[field])
    integer = lambda r, field: f"{int(r[field]):,}"
    number = lambda r, field: f"{f(r, field):.6f}"
    rate = lambda r, count, seconds: f"{f(r, count) / f(r, seconds):,.2f}"
    metrics = [
        ('Read and parse CSV', 'Seconds', lambda r: number(r, 'read_parse_seconds')),
        ('', 'Million rows/sec', lambda r: f"{f(r, 'read_parse_million_units_per_worker_second'):.3f}"),
        ('', 'Input MiB/sec', lambda r: f"{f(r, 'read_parse_mib_per_worker_second'):.3f}"),
        ('', 'Rows read', lambda r: integer(r, 'rows_read')),
        ('Filter', 'Seconds', lambda r: number(r, 'filter_seconds')),
        ('', 'Rows examined', lambda r: integer(r, 'rows_read')),
        ('', 'Rows retained', lambda r: integer(r, 'rows_retained')),
        ('', 'Rows excluded', lambda r: f"{int(r['rows_read']) - int(r['rows_retained']):,}"),
        ('Aggregate', 'Seconds', lambda r: number(r, 'aggregate_seconds')),
        ('', 'Records processed/sec', lambda r: rate(r, 'rows_retained', 'aggregate_seconds')),
        ('', 'Records processed', lambda r: integer(r, 'rows_retained')),
        ('', 'Groups produced (input entity/group/date combinations)', lambda r: integer(r, 'groups')),
        ('Compute attribution and linking', 'Seconds', lambda r: number(r, 'compute_seconds')),
        ('', 'Output effects/sec (3 components × CSV rows)', lambda r: f"{3 * f(r, 'output_rows') / f(r, 'compute_seconds'):,.2f}"),
        ('', 'Output effect components (includes total rows)', lambda r: f"{3 * int(r['output_rows']):,}"),
        ('', 'Daily detail effect components computed', lambda r: integer(r, 'effect_values')),
        ('Write CSV', 'Seconds', lambda r: number(r, 'write_seconds')),
        ('', 'Rows/sec', lambda r: rate(r, 'output_rows', 'write_seconds')),
        ('', 'Output MiB/sec', lambda r: f"{f(r, 'write_mib_per_worker_second'):.3f}"),
        ('', 'Rows written (detail + total)', lambda r: integer(r, 'output_rows')),
        ('', 'Output MiB', lambda r: f"{f(r, 'output_bytes') / 1024**2:.3f}"),
        ('Overall', 'Wall time (seconds)', lambda r: number(r, 'process_wall_seconds')),
        ('', 'Wall time min–max (seconds)', lambda r: f"{f(r, 'process_wall_seconds_min'):.6f}–{f(r, 'process_wall_seconds_max'):.6f}"),
        ('', 'Peak memory (MiB)', lambda r: f"{f(r, 'peak_rss_mib'):.3f}"),
        ('', 'CPU time (seconds)', lambda r: number(r, 'cpu_seconds')),
        ('', 'Average cores used', lambda r: f"{f(r, 'average_cores'):.3f}"),
    ]
    lines = ['| Stage | Metric | Rust | Go |', '|---|---|---:|---:|']
    for stage, metric, render in metrics:
        lines.append(f'| {stage} | {metric} | {render(pair[0])} | {render(pair[1])} |')
    return lines


def main():
    sample, sample_meta = load(ROOT / 'results/native-benchmark')
    full, full_meta = load(ROOT / 'results/native-full-history')
    lines = [
        '# Attribution flows and stage comparison: Rust and Go', '',
        'All five scenarios were compared. The previous chat summary showed only country and standalone subset because those two were also benchmarked over the entire 1,095-date history. The security, weekly, and monthly scenarios were benchmarked over the first three effective dates. This document records every requested stage metric for all five scenarios, followed by the additional full-history measurements.', '',
        'Each scenario below combines its SVG flowchart, efficiency explanation and measured stage metrics. Shared linking and Rust/Go execution details follow the five scenario comparisons.', '',
        '## Coverage and method', '',
        '| Comparison | Scenarios | Effective dates | Repetitions |', '|---|---|---|---:|',
        '| Five-scenario comparison | Security, standalone subset, country, weekly, monthly | 2023-01-01 through 2023-01-03 | 3 per language/scenario |',
        '| Additional full-history comparison | Country, standalone subset | All 1,095 dates | 2 per language/scenario |', '',
        'All runs include all four portfolios and their index. Rust uses four native portfolio workers; Go uses GOMAXPROCS=12 and concurrent I/O goroutines. Each implementation uses its own optimized parser, buffering, formatting and runtime approach. Runs alternate language order and use the OS-managed filesystem cache; they are not controlled cold-cache measurements. Times, memory and CPU values are medians; ranges are included for overall wall time. Rates divide the recorded work by the median corresponding stage duration.', '',
        '**Stage seconds are accumulated elapsed time inside each stage across threads. They can overlap, include scheduling pauses, and must not be added together or interpreted as stage CPU time.** Overall wall time is measured externally for the complete process. Peak memory is the per-process maximum resident set; CPU time includes all process threads; average cores is CPU time divided by process wall time.', '',
        'Read includes CSV decoding, numeric parsing, security lookup and validation. Its byte rate uses consumed return-file bytes, not the full dataset size for truncated runs. Filter timings include the selection/weight-denominator scan: only the subset scenario applies an instrument predicate; all other scenarios retain all rows. Aggregation includes independent weight normalization, dense-group construction and weighted contributions. Group counts include the index and portfolio input groups on every date, rather than just distinct labels. Compute includes attribution, linking and reconciliation. Write includes numeric formatting, CSV escaping, buffering, flush and fsync. Master loading, setup and channel waits remain in overall time.', '',
        '**Output effects** below count the three attribution components (allocation, selection, interaction) on every written row, including portfolio-total rows. The derived `Total` field is not counted as a fourth component. Output effects/sec divides this count by compute-stage seconds; it excludes CSV formatting, which belongs to the write stage. Daily detail effect counts are shown separately because linking reduces the number of period output rows.', '',
        'The first-three-date weekly sample contains two partial weeks; the monthly sample contains one partial month. These are linked results for observed dates, not completed calendar periods. Full-history daily security output would contain approximately 876 million detail rows, around 180 GiB per language at the observed CSV width, exceeding the available space for both languages. Full-history write performance for security/weekly/monthly has therefore **not** been measured.', '',
        '## Scenario flow overview', '', OVERVIEW, '',
        '## Five-scenario comparison — first three effective dates', '',
    ]
    for index, (scenario, title) in enumerate(SCENARIOS, 1):
        lines += [f'### {index}. {title}', '', SCENARIO_FLOWS[scenario], '', '#### Stage metrics', '']
        if scenario == 'subset':
            lines += ['The selected universe is the union of the first 100 instruments in each portfolio (400 IDs). Each portfolio and the index are independently normalized to 100% on each date.', '']
        if scenario == 'country':
            lines += ['The supplied holdings are all US/USD, so each input entity has one country group per date.', '']
        lines += table(sample, scenario) + ['']
    lines += [SHARED_LOGIC, '']
    lines += ['## Additional full-history comparison — all 1,095 dates', '', 'These measurements exercise all 175.2 million input rows per run. They supplement the five-scenario comparison above; they do not replace its three-date measurements.', '']
    for scenario, title in [('country', 'Country attribution'), ('subset', 'Standalone instrument subset')]:
        lines += [f'### {title}', ''] + table(full, scenario) + ['']
    lines += ['## Numerical verification', '', '| Scope | Scenario | Rows compared | Maximum absolute numeric difference | Result |', '|---|---|---:|---:|---|']
    for scope, meta in [('First three dates', sample_meta), ('Full history', full_meta)]:
        for check in meta['correctness']:
            lines.append(f"| {scope} | {check['scenario']} | {check['rows_compared']:,} | {check['max_absolute_difference']:.3g} | {check['status']} |")
    lines += ['', 'Every date/period also passed native reconciliation. The independent Python oracle covers nine scenario/period combinations, including quarter/year boundaries, changing holdings, missing-side securities, standalone normalization and invalid inputs.', '',
              '## Recorded sources and reproduction', '',
              '- [Five-scenario raw runs](results/native-benchmark/runs.csv) and [summary metrics](results/native-benchmark/summary.csv).',
              '- [Full-history raw runs](results/native-full-history/runs.csv) and [summary metrics](results/native-full-history/summary.csv).',
              '- [Five-scenario environment and executable hashes](results/native-benchmark/environment.json).',
              '- [Full-history environment and executable hashes](results/native-full-history/environment.json).',
              '- [Build commands, CLI options and attribution formulas](rust/README.md).', '',
              'Regenerate this Markdown from the recorded metrics:', '', '```sh', 'python3 python/render_stage_report.py', '```', '']
    OUTPUT.write_text('\n'.join(lines))
    print(OUTPUT)


if __name__ == '__main__':
    main()
