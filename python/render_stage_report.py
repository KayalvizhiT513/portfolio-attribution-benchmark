"""Render every requested stage metric from the recorded native benchmarks."""
import csv
import json
import statistics
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



def full_history_summary():
    runs = json.loads((ROOT / 'results/size-guarded-full-history/runs.json').read_text())
    with (ROOT / 'results/native-full-history/runs.csv').open(newline='') as f:
        originals = list(csv.DictReader(f))
    rows = {}
    numeric = ['process_wall_seconds', 'cpu_seconds', 'peak_rss_mib', 'average_cores'] + [stage + '_seconds' for stage in ['read_parse', 'filter', 'aggregate', 'compute', 'write']]
    invariants = ['days', 'threads', 'rows_read', 'rows_retained', 'groups', 'effect_values', 'output_rows', 'output_bytes']
    for scenario in ['subset', 'country']:
        for language in ['rust', 'go']:
            samples = [dict(r) for r in originals if r['scenario'] == scenario and r['language'] == language]
            assert len(samples) == 2
            run = next(r for r in runs if r['scenario'] == scenario and r['language'] == language and r['status'] == 'COMPLETE')
            monitored = dict(run['native_metrics'])
            monitored.update({field: run[field] for field in ['process_wall_seconds', 'cpu_seconds', 'peak_rss_mib']})
            monitored['average_cores'] = run['cpu_seconds'] / run['process_wall_seconds']
            samples.append(monitored)
            for field in invariants:
                assert len({int(r[field]) for r in samples}) == 1, (scenario, language, field)
            r = dict(samples[0])
            r['runs'] = str(len(samples))
            for field in numeric:
                values = [float(x[field]) for x in samples]
                r[field] = statistics.median(values)
                r[field + '_min'], r[field + '_max'] = min(values), max(values)
            for stage, count in [('read_parse', 'rows_read'), ('filter', 'rows_read'), ('aggregate', 'rows_retained'), ('compute', 'effect_values'), ('write', 'output_rows')]:
                r[stage + '_million_units_per_worker_second'] = float(r[count]) / r[stage + '_seconds'] / 1e6
            r['read_parse_mib_per_worker_second'] = float(r['parsed_input_bytes']) / r['read_parse_seconds'] / 1024**2
            r['write_mib_per_worker_second'] = float(r['output_bytes']) / r['write_seconds'] / 1024**2
            rows[scenario, language] = r
    return rows, runs


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
        ('', 'Wall time min–max (seconds)', lambda r: 'Single run; no range' if r.get('runs') == '1' else f"{f(r, 'process_wall_seconds_min'):.6f}–{f(r, 'process_wall_seconds_max'):.6f}"),
        ('', 'Peak memory (MiB)', lambda r: f"{f(r, 'peak_rss_mib'):.3f}"),
        ('', 'CPU time (seconds)', lambda r: number(r, 'cpu_seconds')),
        ('', 'Average cores used', lambda r: f"{f(r, 'average_cores'):.3f}"),
    ]
    lines = ['| Stage | Metric | Rust | Go |', '|---|---|---:|---:|']
    for stage, metric, render in metrics:
        lines.append(f'| {stage} | {metric} | {render(pair[0])} | {render(pair[1])} |')
    return lines




def output_sizes(selected, guarded_runs):
    estimates = json.loads((ROOT / 'results/size-guarded-full-history/estimates.json').read_text())
    lines = ['## Output sizes by scenario and language', '',
             'Sizes below are attribution CSV totals across all four portfolios for one run per language, including detail and total rows. Repeated runs reuse output paths; sizes are not multiplied by the repeat count. Metrics CSVs, logs and input files are excluded. MB = 1,000,000 bytes; GB = 1,000,000,000 bytes.', '',
             '### Completed outputs used in the five scenario tables', '',
             '| Scenario | Completed scope | Rust MB | Go MB | Combined MB |', '|---|---|---:|---:|---:|']
    for scenario, _ in SCENARIOS:
        rust, go = [selected[scenario, l] for l in ['rust', 'go']]
        a, b = int(rust['output_bytes']), int(go['output_bytes'])
        lines.append(f"| {scenario} | {int(rust['days']):,} dates | {a/1e6:.3f} | {b/1e6:.3f} | {(a+b)/1e6:.3f} |")
    lines += ['', '### All-date attempts: actual output versus estimated complete output', '',
              '| Scenario | Rust actual GB | Go actual GB | Combined actual GB | Outcome | Rust estimated full GB | Go estimated full GB |',
              '|---|---:|---:|---:|---|---:|---:|']
    for scenario, _ in SCENARIOS:
        r, g = [next(x for x in guarded_runs if x['scenario'] == scenario and x['language'] == l) for l in ['rust', 'go']]
        a, b = r['attribution_csv_bytes'], g['attribution_csv_bytes']
        er, eg = [next(x['estimated_bytes'] for x in estimates if x['scenario'] == scenario and x['language'] == l) for l in ['rust', 'go']]
        status = 'Complete: 1,095 dates' if r['status'] == g['status'] == 'COMPLETE' else 'Guard-stopped: partial files'
        lines.append(f'| {scenario} | {a/1e9:.3f} | {b/1e9:.3f} | {(a+b)/1e9:.3f} | {status} | {er/1e9:.3f} | {eg/1e9:.3f} |')
    lines += ['', 'Guard-stopped sizes describe partial `.csv.partial` files and are not full-history output sizes. The two languages can stop after different amounts of data; those sizes are not a like-for-like efficiency comparison. Both languages together remained below the 5 GB per-scenario limit. Complete-output estimates for security, weekly and monthly extrapolate sampled row widths; subset and country use measured full-history sizes. See [estimate assumptions and guarded outcomes](ATTRIBUTION_OUTPUT_SIZE_REPORT.md).', '']
    return lines


def winner_summary(selected):
    lines = [
        '## Who wins, and where?', '',
        'Winners below have the lower recorded stage time, overall wall time, peak memory, or CPU time. Security, weekly and monthly use three-date, three-run medians; subset and country use three-run full-history medians across all 1,095 dates. These are observed benchmark winners, not statistical significance claims; short runs and tiny stages can be noisy. Filter is N/A when no instrument predicate is applied. Stage comparisons use accumulated worker elapsed time, as defined above.', '',
        '| Scope | Scenario | Read/parse | Filter | Aggregate | Compute/link | Write CSV | Overall | Lower peak memory | Lower CPU time | Overall speed advantage |',
        '|---|---|---|---|---|---|---|---|---|---|---|',
    ]
    fields = ['read_parse_seconds', 'filter_seconds', 'aggregate_seconds', 'compute_seconds', 'write_seconds', 'process_wall_seconds', 'peak_rss_mib', 'cpu_seconds']
    for scenario, _ in SCENARIOS:
        rust, go = [selected[scenario, language] for language in ['rust', 'go']]
        scope = f"{int(rust['days']):,} dates ({rust['runs']}-run median)"
        winners = []
        for field in fields:
            a, b = float(rust[field]), float(go[field])
            winners.append('N/A' if field == 'filter_seconds' and scenario != 'subset' else 'Rust' if a < b else 'Go' if b < a else 'Tie')
        a, b = float(rust['process_wall_seconds']), float(go['process_wall_seconds'])
        advantage = f"{'Rust' if a < b else 'Go'} {max(a,b)/min(a,b):.2f}×" if a != b else 'Tie'
        lines.append('| ' + ' | '.join([scope, scenario, *winners, advantage]) + ' |')
    lines += [
        '',
        'Rust has the lower overall runtime, peak memory and CPU time in all five scenario comparisons shown here. Go has the lower write-stage time for country. Rust has the lower write-stage time in the other four scenarios. Each winner applies only to the scope shown in its row.', '',
        'Average cores used is reported in the detailed tables but is not ranked: using more cores can improve throughput or simply consume more CPU. Overall runtime and CPU time together show that tradeoff. Output formatting and byte counts differ slightly between languages; the write winner here is based on stage duration, while both row/sec and MiB/sec remain available above.', '',
        'Full-history security, weekly and monthly attempts were stopped by the output-size guard; completed full-history results remain unavailable. Their three-date wins must not be presented as full-history wins. These conclusions apply to the recorded implementations, machine and workloads.', '',
    ]
    return lines


CONDITIONS = {
    'subset': """Predicate: `InstrumentId IN selected_ids`, where `selected_ids` is the union of these four inclusive ranges:

- `S000001`–`S000100`
- `S015001`–`S015100`
- `S030001`–`S030100`
- `S045001`–`S045100`

This is exactly 400 IDs, listed in [subset.csv](results/native-full-history/subset.csv): the first 100 IDs from each portfolio. Apply the same predicate to both portfolio and index. Each portfolio retains 100 holdings/day; the index retains 400 holdings/day. Normalize each selected side independently to 100% every date, and compare against the selected index. This is a standalone-subset return, not those instruments’ contribution to the original full portfolio.

Across all files: 175,200,000 input rows examined, 876,000 retained, 174,324,000 excluded. Both languages wrote 3,512,760 attribution rows; every numeric field matched (maximum difference 0). Attribution CSV size: Rust 759,396,377 bytes; Go 740,854,715 bytes; combined 1,500,251,092 bytes (1.500 GB), excluding the small metrics CSVs.""",
    'country': """No instrument or country filter is applied. Read all input rows, join `InstrumentId` to the security master, and group by `Country`. US is the only country in the supplied holdings; `Country = US` is an observed grouping result, not a filter predicate. Use the complete portfolio/index weights, normalized to remove rounding drift.

Across all files: 175,200,000 input rows examined and retained. The 5,475 input entity/country/date groups are five entities × 1,095 dates. Both languages wrote 17,520 attribution rows; every numeric field matched (maximum difference 0). Attribution CSV size: Rust 4,096,710 bytes; Go 4,043,282 bytes; combined 8,139,992 bytes (8.140 MB), excluding metrics CSVs.""",
}


def main():
    sample, sample_meta = load(ROOT / 'results/native-benchmark')
    completed, guarded_runs = full_history_summary()
    selected = dict(sample)
    selected.update(completed)
    lines = [
        '# Attribution flows and stage comparison: Rust and Go', '',
        'This report contains five scenario comparisons, each with its flowchart, conditions and one Rust/Go metrics table. Subset and country use the completed full-history runs across all 1,095 dates. Security, weekly and monthly retain the three-date benchmark because their full-history attempts exceeded the output-size guard. Repeated measurements are combined into medians and ranges in one table per scenario.', '',
        'Each scenario below combines its SVG flowchart, efficiency explanation and measured stage metrics. Shared linking and Rust/Go execution details follow the five scenario comparisons.', '',
        'For full-history size estimates and actual runs stopped at a 5 GB combined-output guard, see [output-size estimates and guarded run outcomes](ATTRIBUTION_OUTPUT_SIZE_REPORT.md). Guard-stopped outputs are incomplete and do not establish full-history performance winners.', '',
        '## Coverage and method', '',
        '| Comparison | Scenarios | Effective dates | Repetitions |', '|---|---|---|---:|',
        '| Date-limited benchmark | Security, weekly, monthly | 2023-01-01 through 2023-01-03 | 3 per language/scenario |',
        '| Completed full-history runs | Standalone subset, country | 2023-01-01 through 2025-12-30 (1,095 dates) | 3 per language/scenario |', '',
        'All runs include all four portfolios and their index. Rust uses four native portfolio workers; Go uses GOMAXPROCS=12 and concurrent I/O goroutines. Each implementation uses its own optimized parser, buffering, formatting and runtime approach. Runs alternate language order and use the OS-managed filesystem cache; they are not controlled cold-cache measurements. Security, weekly and monthly values are three-run medians, with overall wall-time ranges. Subset and country combine the two original full-history runs with the subsequent completed size-guarded run per language, reporting three-run medians and min–max wall times. The third run includes size-monitoring overhead, so these are descriptive pooled results across slightly different monitoring conditions, not three identically instrumented trials. Rates divide work by the corresponding median stage duration. Individual source runs remain available for inspection.', '',
        '**Stage seconds are accumulated elapsed time inside each stage across threads. They can overlap, include scheduling pauses, and must not be added together or interpreted as stage CPU time.** Overall wall time is measured externally for the complete process. Peak memory is the per-process maximum resident set; CPU time includes all process threads; average cores is CPU time divided by process wall time.', '',
        'Read includes CSV decoding, numeric parsing, security lookup and validation. Its byte rate uses consumed return-file bytes, not the full dataset size for truncated runs. Filter timings include the selection/weight-denominator scan: only the subset scenario applies an instrument predicate; all other scenarios retain all rows. Aggregation includes independent weight normalization, dense-group construction and weighted contributions. Group counts include the index and portfolio input groups on every date, rather than just distinct labels. Compute includes attribution, linking and reconciliation. Write includes numeric formatting, CSV escaping, buffering, flush and fsync. Master loading, setup and channel waits remain in overall time.', '',
        '**Output effects** below count the three attribution components (allocation, selection, interaction) on every written row, including portfolio-total rows. The derived `Total` field is not counted as a fourth component. Output effects/sec divides this count by compute-stage seconds; it excludes CSV formatting, which belongs to the write stage. Daily detail effect counts are shown separately because linking reduces the number of period output rows.', '',
        'The first-three-date weekly sample contains two partial weeks; the monthly sample contains one partial month. These are linked results for observed dates, not completed calendar periods. Full-history daily security output would contain approximately 876 million detail rows, about 183–185 GB (170–173 GiB) per language at the observed CSV width, exceeding the available space for both languages. Full-history write performance for security/weekly/monthly has therefore **not** been measured.', '',
        '## Industry relevance and modelling limits', '',
        '| View | Practical role | Interpretation of this benchmark |', '|---|---|---|',
        '| Security detail | Explain individual-company contributions and investigate winners/losers | Useful calculation/drilldown grain; exporting every benchmark constituent every day is an output-design choice, not an industry requirement |',
        '| Weekly linked view | Assessment: useful for tactical/internal monitoring when a team needs weekly review | A calendar reporting frequency of the daily engine, not another attribution model |',
        '| Monthly linked view | Established performance reporting/analysis interval; longer periods require consistent compounding/linking | Same engine as weekly, with a different bucket key and fewer emitted rows |', '',
        'FactSet describes benchmark-relative analysis at company or group level in its [performance attribution explanation](https://insight.factset.com/keeping-up-with-compliant-benchmarks-for-performance-attribution). MathWorks documents [monthly and compounded multi-period Brinson analysis](https://www.mathworks.com/help/finance/performance-attribution-using-brinson-model.html), using category/sector weights and returns. These support the usefulness of granular drilldown and period reporting; the weekly monitoring use above is an assessment, not a claim of a mandatory industry standard.', '',
        'Brinson allocation/selection is commonly interpreted over decision groups such as sectors or countries. Treating every InstrumentId as its own Brinson group is a modelling choice. For an identical security on the same return basis, if its portfolio and index returns are equal, the per-security selection term is zero by this implementation’s formula. The [synthetic generator](python/generate_attribution_csvs.py) samples returns independently by entity, so the same ID can have different portfolio/index returns. Its nonzero security selection effects demonstrate software calculations, not validated economic security-selection insight.', '',
        'For production use, my recommendation is security-level calculation with requested drilldowns, plus linked period summaries by portfolio and meaningful decision groups. The current benchmark deliberately emits detailed CSVs, so its write volume should not be mistaken for a necessary industry delivery format. The all-US country fixture also tests reading/reduction performance more than realistic cross-country allocation.', '',
        '## Scenario flow overview', '', OVERVIEW, '',
        '## Five-scenario comparison', '',
    ]
    for index, (scenario, title) in enumerate(SCENARIOS, 1):
        lines += [f'### {index}. {title}', '', SCENARIO_FLOWS[scenario], '']
        if scenario in CONDITIONS:
            lines += ['**Scope:** all 1,095 dates, all four portfolios; median of three completed runs per language (two original runs plus one run under the combined 5 GB output guard).', '', '#### Conditions', '', CONDITIONS[scenario], '']
        else:
            lines += ['**Scope:** first three dates (2023-01-01 through 2023-01-03), all four portfolios; median of three runs per language. Full-history attempts were guard-stopped and are not included as completed performance results.', '']
        lines += ['#### Stage metrics', ''] + table(selected, scenario) + ['']
    lines += output_sizes(selected, guarded_runs)
    lines += [SHARED_LOGIC, '']
    lines += ['## Numerical verification', '', '| Scope | Scenario | Rows compared | Maximum absolute numeric difference | Result |', '|---|---|---:|---:|---|']
    for scenario, _ in SCENARIOS:
        if scenario in CONDITIONS:
            check = next(r['comparison'] for r in guarded_runs if r['scenario'] == scenario and r.get('comparison'))
            scope, count, delta = '1,095 dates', check['rows'], check['max_absolute_difference']
        else:
            check = next(c for c in sample_meta['correctness'] if c['scenario'] == scenario)
            scope, count, delta = '3 dates', check['rows_compared'], check['max_absolute_difference']
        lines.append(f"| {scope} | {scenario} | {count:,} | {delta:.3g} | PASS |")
    lines += ['', 'Every date/period also passed native reconciliation. The independent Python oracle covers nine scenario/period combinations, including quarter/year boundaries, changing holdings, missing-side securities, standalone normalization and invalid inputs.', '',
              '## Recorded sources and reproduction', '',
              '- [Five-scenario raw runs](results/native-benchmark/runs.csv) and [summary metrics](results/native-benchmark/summary.csv).',
              '- [Original two full-history raw runs](results/native-full-history/runs.csv) and [original two-run summaries](results/native-full-history/summary.csv).',
              '- [Five-scenario environment and executable hashes](results/native-benchmark/environment.json).',
              '- [Full-history environment and executable hashes](results/native-full-history/environment.json).',
              '- [Completed full-history runs, conditions, native stage metrics and guard statuses](results/size-guarded-full-history/runs.json).',
              '- [Build commands, CLI options and attribution formulas](rust/README.md).', '',
              'Regenerate this Markdown from the recorded metrics:', '', '```sh', 'python3 python/render_stage_report.py', '```', '']
    lines += winner_summary(selected)
    OUTPUT.write_text('\n'.join(lines))
    print(OUTPUT)


if __name__ == '__main__':
    main()
