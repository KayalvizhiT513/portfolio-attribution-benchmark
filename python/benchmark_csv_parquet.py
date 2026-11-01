"""Run CSV first, then Parquet; continuously update a concise full-history report."""
import csv
import hashlib
import json
import math
import platform
from pathlib import Path
import sys

from benchmark_native import measured
from test_attribution import compare_files

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results/csv-parquet-native-output'
REPORT = ROOT / 'CSV_PARQUET_REPORT.md'
SCENARIOS = [('security', 318000), ('country', 2190), ('weekly', 316), ('monthly', 72), ('subset', 602)]


def save(runs, checks, environment, persist=True):
    if persist:
        for name, value in [('runs', runs), ('checks', checks), ('environment', environment)]:
            (OUT / f'{name}.json').write_text(json.dumps(value, indent=2) + '\n')
    if runs and persist:
        with (OUT / 'runs.csv').open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(runs[0]))
            writer.writeheader(); writer.writerows(runs)
    def lookup(scenario, fmt, language):
        return next((r for r in runs if r['scenario'] == scenario and r['format'] == fmt and r['language'] == language), None)
    def wall(scenario, fmt, language):
        r = lookup(scenario, fmt, language)
        return f"{r['process_wall_seconds']:.2f}" if r else '—'
    done = {fmt: sum(r['format'] == fmt for r in runs) for fmt in ['csv', 'parquet']}
    lines = ['# Historical CSV and Parquet benchmarks: five attribution scenarios in Rust and Go', '', 'Archived experiment; see [COUNTRY_REPORT.md](COUNTRY_REPORT.md) for the current optimized country measurements.', '',
             f"Progress: CSV {done['csv']}/10; Parquet {done['parquet']}/10. CSV runs finish before Parquet starts.", '',
             'One full-history run per cell; 1,095 dates, one 60,000-security portfolio, a 100,000-security index, and 1,000 shared DE IDs. Four threads configured for both engines. BF effects and contribution columns included. CSV input writes CSV; Parquet input writes typed Zstandard Parquet.', '',
             '## Total runtime (seconds)', '',
             '| Scenario | Output rows | CSV Rust | CSV Go | Parquet Rust | Parquet Go |',
             '|---|---:|---:|---:|---:|---:|']
    followup = ROOT / 'RUST_DATE_READ_REPORT.md'
    if done == {'csv': 10, 'parquet': 10}:
        lines[2] = 'Complete: 10 CSV runs, followed by 10 Parquet runs.'
        ratios = {lang: sum(r['process_wall_seconds'] for r in runs if r['format'] == 'parquet' and r['language'] == lang)
                       / sum(r['process_wall_seconds'] for r in runs if r['format'] == 'csv' and r['language'] == lang)
                  for lang in ['rust', 'go']}
        lines.insert(4, f"Parquet/CSV total wall-time ratio across the five scenarios: Rust **{ratios['rust']:.2f}×**, Go **{ratios['go']:.2f}×** (below 1 means Parquet was faster in this single pass).\n")
    for scenario, count in SCENARIOS:
        values = [wall(scenario, fmt, lang) for fmt in ['csv', 'parquet'] for lang in ['rust', 'go']]
        lines.append(f"| {scenario.capitalize()} | {count:,} | " + ' | '.join(values) + ' |')
    lines += ['', '## Resources across all five scenarios', '',
              '| Engine | CSV CPU s¹ | Parquet CPU s¹ | CSV max memory MiB² | Parquet max memory MiB² |',
              '|---|---:|---:|---:|---:|']
    for lang in ['rust', 'go']:
        cells = []
        for field in ['cpu_seconds', 'peak_rss_mib']:
            for fmt in ['csv', 'parquet']:
                samples = [r[field] for r in runs if r['language'] == lang and r['format'] == fmt]
                cells.append(f'{(sum(samples) if field == "cpu_seconds" else max(samples)):.2f}' if len(samples) == 5 else '—')
        lines.append(f'| {lang.capitalize()} | ' + ' | '.join(cells) + ' |')
    lines += ['', '¹ Total CPU time across the five scenarios. ² Highest memory use seen in any scenario.', '',
              f"Validation: {len(checks)}/15 file comparisons passed (5 CSV Go/Rust, 5 Parquet Go/Rust, 5 CSV/Parquet). Every output field is compared; engines also enforce BF and contribution reconciliation.", '',
              'Subset: 400 IDs—200 portfolio-only US, 100 index-only US, 100 shared DE; 300 held IDs × 2 + 2 residual rows = 602. Country: US-filtered, separately normalized daily attribution.', '',
              'CSV ran first, then Parquet. The starting language alternates by scenario. We used the normal filesystem cache and measured each configuration once. Total runtime includes setup, reading, calculation and saving the finished output.', '',
              '[Raw runs and stage metrics](results/csv-parquet-native-output/runs.csv) · [Validation](results/csv-parquet-native-output/checks.json) · [Environment](results/csv-parquet-native-output/environment.json)', '']
    REPORT.write_text('\n'.join(lines))
    if followup.exists():
        report_text = REPORT.read_text()
        report_text = report_text.replace('# Historical CSV and Parquet benchmarks: five attribution scenarios in Rust and Go\n', '# Historical CSV and Parquet benchmarks: five attribution scenarios in Rust and Go\n\n**Baseline measurements.** See the [Rust date-format fix and fresh comparison](RUST_DATE_READ_REPORT.md) for the follow-up.', 1)
        REPORT.write_text(report_text)


def main():
    if OUT.exists():
        raise FileExistsError(f'Choose a fresh results directory: {OUT}')
    OUT.mkdir(parents=True)
    subset = OUT / 'subset.csv'
    ids = ([f'S{i:06d}' for i in range(100001, 100201)] +
           [f'S{i:06d}' for i in range(1, 101)] + [f'S{i:06d}' for i in range(159001, 159101)])
    subset.write_text('InstrumentId\n' + '\n'.join(ids) + '\n')
    binaries = {'rust': ROOT / 'rust/target/release/portfolio-attribution', 'go': ROOT / 'benchmark-go-attribution'}
    hashes = {lang: hashlib.sha256(path.read_bytes()).hexdigest() for lang, path in binaries.items()}
    environment = {'platform': platform.platform(), 'thread_policy': 'runtime default; resolved threads recorded in each run', 'repetitions': 1,
                   'format_order': ['csv', 'parquet'], 'cache': 'OS-managed, no explicit eviction',
                   'executables': {lang: {'path': str(path), 'sha256': hashes[lang]} for lang, path in binaries.items()},
                   'input_manifests': {fmt: json.loads((ROOT / folder / 'manifest.json').read_text())
                                       for fmt, folder in [('csv', 'attribution_data'), ('parquet', 'attribution_data_parquet')]}}
    runs, checks = [], []
    save(runs, checks, environment)
    for fmt, folder in [('csv', 'attribution_data'), ('parquet', 'attribution_data_parquet')]:
        for index, (scenario, expected) in enumerate(SCENARIOS):
            order = ['rust', 'go'] if index % 2 == 0 else ['go', 'rust']
            for lang in order:
                out = OUT / fmt / scenario / lang
                out.mkdir(parents=True)
                metrics = out / 'metrics.csv'
                command = [str(binaries[lang]), '--data', str(ROOT / folder), '--scenario', scenario,
                           '--output-dir', str(out), '--metrics', str(metrics),
                           '--max-days', '0', '--instruments-file', str(subset)]
                print(f'START {fmt} {scenario} {lang}: all 1,095 dates', flush=True)
                usage = measured(command, out / 'run.log')
                with metrics.open() as f:
                    row = next(csv.DictReader(f))
                assert int(row['days']) == 1095 and int(row['output_rows']) == int(row['report_rows']) == expected, row
                assert int(row['rows_read']) == 175200000, row
                assert all(math.isfinite(float(row[c])) for c in ['checksum_allocation', 'checksum_selection', 'checksum_interaction'])
                row.update({'format': fmt, **usage})
                runs.append(row)
                save(runs, checks, environment)
                print(f"DONE {fmt} {scenario} {lang}: {usage['process_wall_seconds']:.2f}s, {usage['peak_rss_mib']:.1f} MiB, {expected:,} rows", flush=True)
            name = f'{scenario}-portfolio_PF.{fmt}'
            a, b = OUT / fmt / scenario / 'rust' / name, OUT / fmt / scenario / 'go' / name
            count, delta = compare_files(a, b)
            assert count == expected
            checks.append({'format': fmt, 'scenario': scenario, 'comparison': 'Go/Rust',
                           'rows': count, 'maximum_absolute_difference': delta, 'status': 'PASS'})
            if fmt == 'parquet':
                count, delta = compare_files(OUT / 'csv' / scenario / 'rust' / f'{scenario}-portfolio_PF.csv', a)
                assert count == expected
                checks.append({'format': fmt, 'scenario': scenario, 'comparison': 'CSV/Parquet',
                               'rows': count, 'maximum_absolute_difference': delta, 'status': 'PASS'})
            save(runs, checks, environment)
        print(f'{fmt.upper()} COMPLETE: report updated at {REPORT}', flush=True)
    assert len(runs) == 20 and len(checks) == 15
    assert all(hashlib.sha256(path.read_bytes()).hexdigest() == hashes[lang] for lang, path in binaries.items())
    from render_csv_parquet_report import main as render_report
    render_report()
    print(f'COMPLETE: {REPORT}', flush=True)


if __name__ == '__main__':
    main()
