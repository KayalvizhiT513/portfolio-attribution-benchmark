"""Rerun country attribution with separate elapsed stage timers for each worker."""
import csv
import hashlib
import json
import math
import os
from pathlib import Path

from benchmark_native import measured
from test_attribution import compare_files

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results/country-worker-stages'
FIELDS = ('read', 'filter', 'aggregate', 'compute', 'write')
TOTAL = dict(read='read_parse_seconds', filter='filter_seconds', aggregate='aggregate_seconds',
             compute='compute_seconds', write='write_seconds')


def main():
    OUT.mkdir(exist_ok=False)
    binaries = {'rust': ROOT / 'rust/target/release/portfolio-attribution',
                'go': ROOT / 'benchmark-go-attribution'}
    environment = {'threads': 4, 'repetitions': 3, 'dates': 1095,
                   'order': 'Alternate format and engine order between repetitions; runs execute sequentially.',
                   'timing': 'Elapsed durations per stage per worker; repeated calls sum only within that worker. Channel waits/setup are outside stage timers. Workers overlap; do not sum workers.',
                   'profiling': 'CPU/allocation/read-batch diagnostics disabled',
                   'binary_sha256': {lang: hashlib.sha256(p.read_bytes()).hexdigest() for lang, p in binaries.items()}}
    (OUT / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')
    runs, checks = [], []
    env = {k: v for k, v in os.environ.items() if not k.startswith('ATTRIBUTION_')}
    for rep in range(1, 4):
        for fmt in (['csv', 'parquet'] if rep % 2 else ['parquet', 'csv']):
            for lang in (['rust', 'go'] if rep % 2 else ['go', 'rust']):
                folder = OUT / f'run-{rep}' / fmt / lang
                folder.mkdir(parents=True)
                command = [str(binaries[lang]), '--data', str(ROOT / ('attribution_data' if fmt == 'csv' else 'attribution_data_parquet')),
                           '--scenario', 'country', '--threads', '4', '--max-days', '0',
                           '--output-dir', str(folder), '--metrics', str(folder / 'metrics.csv')]
                # measured() inherits the process environment.
                os.environ.clear()
                os.environ.update(env)
                print(f'COUNTRY {rep} {fmt} {lang}', flush=True)
                usage = measured(command, folder / 'run.log')
                with (folder / 'metrics.csv').open() as f:
                    row = next(csv.DictReader(f))
                assert int(row['rows_read']) == 175200000
                assert int(row['days']) == 1095 and int(row['output_rows']) == 2190
                for stage in FIELDS:
                    assert math.isclose(float(row[f'index_{stage}_seconds']) + float(row[f'portfolio_{stage}_seconds']),
                                        float(row[TOTAL[stage]]), rel_tol=1e-9, abs_tol=1e-6)
                row.update(format=fmt, repetition=rep, **usage)
                runs.append(row)
                output = folder / f'country-portfolio_PF.{fmt}'
                for reference_fmt in ('csv', 'parquet'):
                    reference = ROOT / f'results/{lang}-date-cache/country/run-1/{reference_fmt}/country-portfolio_PF.{reference_fmt}'
                    count, delta = compare_files(reference, output)
                    assert count == 2190
                    checks.append(dict(language=lang, format=fmt, repetition=rep, reference_format=reference_fmt,
                                       rows=count, max_difference=delta, status='PASS'))
                (OUT / 'runs.json').write_text(json.dumps(runs, indent=2) + '\n')
                (OUT / 'checks.json').write_text(json.dumps(checks, indent=2) + '\n')
                with (OUT / 'runs.csv').open('w', newline='') as f:
                    w = csv.DictWriter(f, fieldnames=list(runs[0]))
                    w.writeheader()
                    w.writerows(runs)
                print(f"  wall {usage['process_wall_seconds']:.3f}s; index read {float(row['index_read_seconds']):.3f}s; portfolio read {float(row['portfolio_read_seconds']):.3f}s", flush=True)
    assert len(runs) == 12 and len(checks) == 24
    print('Complete: 12 country runs; 24 reference comparisons passed.', flush=True)


if __name__ == '__main__':
    main()
