"""Estimate all scenarios, then run full-history jobs sequentially with CSV size limits.

python3 python/run_size_guarded.py --run --limit-gb 5
No date cap is passed: unfinished runs stop only at an output-size/disk guard.
"""
import argparse
import csv
import json
import os
import resource
import signal
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from test_attribution import compare_files, period_key

ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = ['security', 'subset', 'country', 'weekly', 'monthly']
STAGES = ['read_parse_seconds', 'filter_seconds', 'aggregate_seconds', 'compute_seconds', 'write_seconds']


def load_csv(path):
    with path.open(newline='') as f:
        return list(csv.DictReader(f))


def estimates(data):
    sample = load_csv(ROOT / 'results/native-benchmark/summary.csv')
    recorded = ROOT / 'results/native-full-history'
    full = load_csv(recorded / 'summary.csv')
    metadata = json.loads((recorded / 'environment.json').read_text())
    actual = [{'name': p.name, 'bytes': p.stat().st_size, 'mtime_ns': p.stat().st_mtime_ns} for p in sorted(data.glob('*.csv'))]
    if actual != metadata['input_files']:
        raise ValueError('Input inventory changed; refresh the baseline before extrapolating its sizes')
    days = int(full[0]['days'])
    with (data / 'index.csv').open(newline='') as f:
        first = date.fromisoformat(next(csv.DictReader(f))['EffectiveDate'])
    # This fixture was generated with every calendar date and fixed holdings.
    dates = [first + timedelta(days=i) for i in range(days)]
    result = []
    for scenario in SCENARIOS:
        period = {'weekly': 'week', 'monthly': 'month'}.get(scenario, 'day')
        count = len({period_key(str(d), period) for d in dates})
        for language in ['rust', 'go']:
            sampled = next(r for r in sample if r['scenario'] == scenario and r['language'] == language)
            sampled_periods = len({period_key(str(d), period) for d in dates[:int(sampled['days'])]})
            output_rows = int(sampled['output_rows']) * count // sampled_periods
            measured = next((r for r in full if r['scenario'] == scenario and r['language'] == language), None)
            size = int(measured['output_bytes']) if measured else round(int(sampled['output_bytes']) * output_rows / int(sampled['output_rows']))
            result.append({'scenario': scenario, 'language': language, 'dates': days, 'periods': count, 'output_rows': output_rows, 'estimated_bytes': size, 'basis': 'previous full-history measurement' if measured else 'measured sampled bytes/row × estimated full-history rows'})
    return result


def csv_sizes(folder):
    # Include both published and in-progress attribution CSVs plus metrics CSVs.
    total = 0
    for p in folder.rglob('*'):
        if p.name.endswith('.csv') or p.name.endswith('.csv.partial'):
            try:
                total += p.stat().st_size
            except FileNotFoundError:
                pass  # A worker/coordinator may rename a completed file during a poll.
    return total


def guarded_process(command, output, limit, total_folder, combined_limit, file_count):
    # The hard per-file allowance bounds the sum even between monitor polls.
    reserve = 8 * 1024**2
    file_limit = max(1, (limit - reserve) // file_count)
    def set_limit():
        resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, file_limit))
    start = time.perf_counter()
    stopped = None
    with (output / 'process.log').open('w') as log:
        p = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True, preexec_fn=set_limit)
        peak = 0
        while True:
            pid, status, usage = os.wait4(p.pid, os.WNOHANG)
            own = csv_sizes(output)
            combined = csv_sizes(total_folder)
            peak = max(peak, combined)
            if pid:
                p.returncode = os.waitstatus_to_exitcode(status)
                break
            if own >= limit * 0.96 or combined >= combined_limit * 0.98:
                stopped = 'STOPPED_SIZE_GUARD'
            elif __import__('shutil').disk_usage(output).free < 5 * 1024**3:
                stopped = 'STOPPED_FREE_DISK_GUARD'
            if stopped:
                os.killpg(p.pid, signal.SIGTERM)
                _, status, usage = os.wait4(p.pid, 0)
                p.returncode = os.waitstatus_to_exitcode(status)
                break
            time.sleep(0.02)
    elapsed = time.perf_counter() - start
    size = csv_sizes(output)
    if not stopped and p.returncode:
        files = list(output.glob('*.csv.partial'))
        if p.returncode == -signal.SIGXFSZ or any(x.stat().st_size >= file_limit - 1024**2 for x in files):
            stopped = 'STOPPED_HARD_FILE_LIMIT'
        else:
            raise RuntimeError(f'Native job failed ({p.returncode}): {output}/process.log\n{(output / "process.log").read_text()[-4000:]}')
    state = stopped or 'COMPLETE'
    result = {'status': state, 'csv_bytes': size, 'returncode': p.returncode, 'process_wall_seconds': elapsed, 'cpu_seconds': usage.ru_utime + usage.ru_stime, 'peak_rss_mib': usage.ru_maxrss / (1024**2 if sys.platform == 'darwin' else 1024), 'combined_peak_observed_bytes': max(peak, csv_sizes(total_folder))}
    if state == 'COMPLETE':
        result['native_metrics'] = load_csv(output / 'metrics.csv')[0]
    return result



def audit_output_rows(output, scenario):
    # Generated output labels contain no embedded newlines. Count complete records
    # with bounded binary chunks; a guard-truncated final record is not counted.
    rows = 0
    data_bytes = 0
    for path in output.glob(f'{scenario}-portfolio_*.csv*'):
        newlines = 0
        with path.open('rb') as f:
            for chunk in iter(lambda: f.read(4 * 1024**2), b''):
                newlines += chunk.count(b'\n')
                data_bytes += len(chunk)
        rows += max(0, newlines - 1)  # exclude one header per file
    return {'complete_csv_records': rows, 'attribution_csv_bytes': data_bytes}


def write_report(root, estimate, runs, limit):
    lines = ['# Full-history output estimates and size-guarded runs', '',
             f'Combined CSV limit per scenario: **{limit:,} bytes ({limit / 1e9:.2f} GB)**, across both languages and all four portfolios. Each language receives half the budget. GB is decimal; GiB is binary. All requested native runs use `--max-days 0` (all dates).', '',
             '## Estimated full-history output', '',
             '| Scenario | Dates | Output periods | Rows per language | Rust GB | Go GB | Combined GB | Basis |', '|---|---:|---:|---:|---:|---:|---:|---|']
    for scenario in SCENARIOS:
        r, g = [next(x for x in estimate if x['scenario'] == scenario and x['language'] == l) for l in ['rust', 'go']]
        lines.append(f"| {scenario} | {r['dates']} | {r['periods']} | {r['output_rows']:,} | {r['estimated_bytes']/1e9:.3f} | {g['estimated_bytes']/1e9:.3f} | {(r['estimated_bytes']+g['estimated_bytes'])/1e9:.3f} | {r['basis']} |")
    lines += ['', 'The synthetic fixture has fixed instrument universes and every calendar date. Daily row cardinalities scale by date count; weekly/monthly row cardinalities scale by calendar bucket count, including partial boundary periods. Variable-width decimal values and date/period labels make extrapolated bytes approximate; allow about ±20% for security/weekly/monthly. This does not generalize to changing holdings without new cardinality sampling. The country/subset sizes were already measured across all dates. Input file names, sizes and modification times are checked against that recorded inventory before estimation.', '',
              '## Sequential run outcomes', '', '| Scenario | Language | Status | Actual CSV GB | Wall seconds | Full-history result? |', '|---|---|---|---:|---:|---|']
    for r in runs:
        lines.append(f"| {r['scenario']} | {r['language']} | {r['status']} | {r['csv_bytes']/1e9:.3f} | {r['process_wall_seconds']:.3f} | {'Yes' if r['status']=='COMPLETE' else 'No — partial output'} |")
    lines += ['', f'| Scenario | Combined actual CSV GB | Under {limit/1e9:g} GB? | Numerical comparison |', '|---|---:|---|---|']
    for scenario in SCENARIOS:
        entries = [r for r in runs if r['scenario'] == scenario]
        if not entries:
            continue
        size = sum(r['csv_bytes'] for r in entries)
        check = next((r.get('comparison') for r in entries if r.get('comparison')), None)
        comparison = f"PASS: {check['rows']:,} rows; max difference {check['max_absolute_difference']:.3g}" if check else 'Not compared: at least one run is incomplete'
        lines.append(f"| {scenario} | {size/1e9:.3f} | {'Yes' if size <= limit else 'NO'} | {comparison} |")
    lines += ['', '## Check against actual output growth', '',
              '| Scenario | Initial combined estimate GB | Projection from actual bytes / complete row GB |',
              '|---|---:|---:|']
    for scenario in SCENARIOS:
        entries = [r for r in runs if r['scenario'] == scenario]
        if len(entries) != 2 or not all(r.get('complete_csv_records') for r in entries):
            continue
        projected = sum(r['attribution_csv_bytes'] / r['complete_csv_records'] * next(e['output_rows'] for e in estimate if e['scenario'] == scenario and e['language'] == r['language']) for r in entries)
        original = sum(e['estimated_bytes'] for e in estimate if e['scenario'] == scenario)
        lines.append(f'| {scenario} | {original/1e9:.3f} | {projected/1e9:.3f} |')
    lines += ['', 'The second projection uses complete records counted in actual generated CSVs, including guard-stopped files. A truncated last row is not counted; at most one such row per file adds negligible byte-width error. This audits row-width growth for the supplied newline-free synthetic labels; it does not make partial runs complete or remove the fixed-cardinality assumption.', '']
    lines += ['', '## Guard behavior and interpretation', '',
              'A monitor samples CSV sizes every 20 ms, including `.partial` files, and terminates a language run near 96% of its allocated half-budget. A hard per-file OS limit divides that allocation among the four portfolio outputs, with reserved space for metrics. The combined guard also checks both languages and stops if free disk falls below 5 GiB. Files remain available for inspection; partial files may end inside a CSV record and must not be treated as valid completed attribution outputs.', '',
              'The two language runs are sequential, and scenarios are sequential. A stopped oversized scenario does not prevent testing the next scenario. Only COMPLETE runs produce valid full-history stage metrics and are eligible for numerical comparison. Stopped wall/CPU times describe termination under the guard; they are not full-history performance results or evidence of a language winner.', '',
              f'Native metrics and outputs: `{os.path.relpath(root, ROOT)}/<scenario>/<language>/`. Run records and estimates are also saved as JSON. No earlier benchmark outputs are overwritten.', '',
              'The earlier “180 GiB per language” estimate was imprecise: sampled daily security output projects to about 183–185 **GB**, or 170–173 **GiB**, per language. The estimate is driven by 876 million detail rows, not by input CSV size. This guarded run checks actual size growth rather than relying only on that projection.', '']
    target = ROOT / 'ATTRIBUTION_OUTPUT_SIZE_REPORT.md'
    target.write_text('\n'.join(lines))
    (root / 'estimates.json').write_text(json.dumps(estimate, indent=2) + '\n')
    (root / 'runs.json').write_text(json.dumps(runs, indent=2) + '\n')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--data', type=Path, default=ROOT / 'attribution_data')
    ap.add_argument('--output-dir', type=Path, default=ROOT / 'results/size-guarded-full-history')
    ap.add_argument('--limit-gb', type=float, default=5)
    ap.add_argument('--run', action='store_true')
    args = ap.parse_args()
    limit = int(args.limit_gb * 1e9)
    if limit < 64 * 1024**2:
        ap.error('limit must be at least 64 MiB')
    if args.output_dir.exists():
        ap.error('use a new output directory to avoid mixing new and old CSV sizes')
    args.output_dir.mkdir(parents=True)
    estimate = estimates(args.data)
    results = []
    write_report(args.output_dir, estimate, results, limit)
    for scenario in SCENARIOS:
        total = sum(x['estimated_bytes'] for x in estimate if x['scenario'] == scenario)
        print(f'{scenario}: full-history combined estimate {total/1e9:.3f} GB; limit {limit/1e9:.3f} GB', flush=True)
    if not args.run:
        return
    subset = ROOT / 'results/native-full-history/subset.csv'
    portfolios = len(list(args.data.glob('portfolio_*.csv')))
    if portfolios != 4:
        raise ValueError('This inventory-based runner expects the four recorded portfolios')
    for i, scenario in enumerate(SCENARIOS):
        scenario_dir = args.output_dir / scenario
        for language in (['rust', 'go'] if i % 2 == 0 else ['go', 'rust']):
            output = scenario_dir / language
            output.mkdir(parents=True)
            binary = ROOT / ('rust/target/release/portfolio-attribution' if language == 'rust' else 'benchmark-go-attribution')
            command = [str(binary), '--scenario', scenario, '--data', str(args.data), '--output-dir', str(output), '--metrics', str(output / 'metrics.csv'), '--max-days', '0', '--threads', str(4 if language == 'rust' else os.cpu_count() or 1), '--instruments-file', str(subset)]
            print(f'{scenario}: starting {language}, ALL dates, output guard active', flush=True)
            row = {'scenario': scenario, 'language': language, **guarded_process(command, output, limit // 2, scenario_dir, limit, portfolios)}
            row.update(audit_output_rows(output, scenario))
            results.append(row)
            print(f"  {row['status']}: {row['csv_bytes']/1e9:.3f} GB in {row['process_wall_seconds']:.2f}s", flush=True)
            assert csv_sizes(scenario_dir) <= limit, 'combined CSV budget exceeded'
            write_report(args.output_dir, estimate, results, limit)
        entries = [r for r in results if r['scenario'] == scenario]
        if all(r['status'] == 'COMPLETE' for r in entries):
            count = 0
            difference = 0.0
            for left in sorted((scenario_dir / 'rust').glob(f'{scenario}-portfolio_*.csv')):
                n, delta = compare_files(left, scenario_dir / 'go' / left.name)
                count += n
                difference = max(difference, delta)
            entries[-1]['comparison'] = {'rows': count, 'max_absolute_difference': difference}
            print(f'  PASS: {count:,} complete output rows compared', flush=True)
        write_report(args.output_dir, estimate, results, limit)
    print('Report: ATTRIBUTION_OUTPUT_SIZE_REPORT.md', flush=True)


if __name__ == '__main__':
    main()
