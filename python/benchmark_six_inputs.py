"""Run the six-input engines sequentially and verify every output field."""
import argparse
import csv
import datetime as dt
import json
from pathlib import Path
import re
import subprocess
import time

from test_attribution import FIELDS, compare_files, key, records
import math
import pyarrow.parquet as pq


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data', type=Path, default=Path('results/six-input/inputs'))
    p.add_argument('--output', type=Path, default=Path('results/six-input/cached'))
    p.add_argument('--start-date', default='2023-01-01')
    p.add_argument('--end-date', default='2025-12-28')
    p.add_argument('--threads', type=int, default=12)
    p.add_argument('--rust', type=Path, default=Path('rust/target/release/portfolio-attribution'))
    p.add_argument('--go', type=Path, default=Path('results/six-input/benchmark-go'))
    p.add_argument('--baseline', type=Path, default=Path(
        'results/country-security-first-v2-2026-11-02/history/parquet/rust/1/country-portfolio_PF.parquet'))
    args = p.parse_args()
    summary = dict(start_date=args.start_date, end_date=args.end_date, threads=args.threads,
                   cache='uncontrolled; sequential single runs, no cold-cache claim', runs={})
    for language, binary in [('rust', args.rust), ('go', args.go)]:
        out = args.output / language
        out.mkdir(parents=True, exist_ok=True)
        cmd = [str(binary.resolve()), '--scenario', 'six-input', '--data', str(args.data),
               '--output-dir', str(out), '--metrics', str(out / 'metrics.csv'),
               '--start-date', args.start_date, '--end-date', args.end_date,
               '--threads', str(args.threads)]
        print(f'Running {language}: {args.start_date} through {args.end_date}', flush=True)
        started = time.perf_counter()
        # This repository's full-size fixture is on macOS; preserve native time evidence.
        with (out / 'run.log').open('w') as log, (out / 'usage.txt').open('w') as usage:
            subprocess.run(['/usr/bin/time', '-l', *cmd], check=True, stdout=log, stderr=usage)
        usage = (out / 'usage.txt').read_text()
        peak = re.search(r'(\d+)\s+maximum resident set size', usage)
        with (out / 'metrics.csv').open() as f:
            metrics = next(csv.DictReader(f))
        summary['runs'][language] = dict(process_wall_seconds=time.perf_counter() - started,
                                         peak_rss_bytes=int(peak[1]) if peak else None, metrics=metrics)
        print(f'{language}: {metrics["days"]} dates, {metrics["output_rows"]} rows, '
              f'{metrics["wall_seconds"]} native seconds', flush=True)
        (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    left = args.output / 'rust/country-portfolio_PF.parquet'
    right = args.output / 'go/country-portfolio_PF.parquet'
    count, maximum = compare_files(left, right)
    # The pre-existing baseline is independent of six-file loading. Compare only
    # dates selected by this request; do not require source files to cover alike.
    baseline = {key(r): r for r in records(args.baseline)
                if args.start_date <= r['PeriodStart'] <= args.end_date}
    seen = set()
    for row in records(left):
        k = key(row); expected = baseline[k]; seen.add(k)
        assert row['Scenario'] == expected['Scenario']
        for field in FIELDS:
            assert math.isclose(float(row[field]), float(expected[field]), rel_tol=1e-8, abs_tol=1e-10), (k, field)
    assert seen == baseline.keys(), 'baseline row sets differ'
    expected_days = (dt.date.fromisoformat(args.end_date) - dt.date.fromisoformat(args.start_date)).days + 1
    for language, run in summary['runs'].items():
        mt = run['metrics']
        assert int(mt['days']) == expected_days
        assert int(mt['readers_per_worker']) == 6
        assert int(mt['accounts_rows_decoded']) == expected_days
        assert int(mt['forex_rows_decoded']) == 4 * expected_days
        smf = pq.ParquetFile(args.data / 'security_master.parquet')
        ci = smf.schema_arrow.get_field_index('EffectiveDate')
        snapshots = sorted({smf.metadata.row_group(g).column(ci).statistics.min for g in range(smf.num_row_groups)})
        days = [dt.date.fromisoformat(args.start_date) + dt.timedelta(days=n) for n in range(expected_days)]
        workers = min(args.threads, expected_days)
        expected_loads = 0
        for worker in range(workers):
            previous = None
            for day in days[expected_days*worker//workers:expected_days*(worker+1)//workers]:
                selected = max(s for s in snapshots if s <= day)
                if selected != previous:
                    expected_loads += 1
                    previous = selected
        assert int(mt['security_master_rows_decoded']) == 160000 * expected_loads
        assert int(mt['smf_snapshot_loads']) == expected_loads
        assert int(mt['smf_cache_hits']) == expected_days - expected_loads
        assert int(mt['analytics_rows_decoded']) == 100000 * expected_days
        assert int(mt['portfolio_PF_rows_decoded']) == 60000 * expected_days
        assert int(mt['index_rows_decoded']) == 100000 * expected_days
        assert int(mt['joined_us_securities']) == 158000 * expected_days
    a, b = (summary['runs'][n]['metrics'] for n in ('rust', 'go'))
    for name in ('analytics_checksum', 'fx_checksum'):
        assert math.isclose(float(a[name]), float(b[name]), rel_tol=1e-10, abs_tol=1e-8)
    summary['validation'] = dict(output_rows=count, cross_language_max_absolute_difference=maximum,
                                 original_country_baseline='all report fields match within tolerance',
                                 decoded_row_counts='all six inputs verified', joined_checksums='match')
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary['validation'], indent=2), flush=True)


if __name__ == '__main__':
    main()
