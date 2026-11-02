"""Compare undated and historical SMFs with identical concurrent country execution."""
import csv
import hashlib
import json
import os
from pathlib import Path
import statistics

import pyarrow as pa
import pyarrow.parquet as pq

from benchmark_country_runtime import prepare_parquet, validate_metrics
from benchmark_native import measured
from test_attribution import compare_files

ROOT = Path(__file__).resolve().parents[1]


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--runs', type=int, default=3)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error('--runs must be positive')
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    inputs = root / 'inputs'
    static = inputs / 'static/csv'
    static.mkdir(parents=True)
    # Prove that collapsing history preserves every classification, rather than
    # silently picking a snapshot from a master whose attributes change.
    values = {}
    historical_rows = 0
    source = ROOT / 'attribution_data/security_master.csv'
    with source.open(newline='') as f:
        for row in csv.DictReader(f):
            sid = row['InstrumentId']
            value = (row['Currency'], row['Country'])
            assert values.setdefault(sid, value) == value, f'changing classification: {sid}'
            historical_rows += 1
    with (static / 'security_master.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['InstrumentId', 'Currency', 'Country'])
        writer.writerows((sid, *value) for sid, value in values.items())
    table = pa.table({'InstrumentId': list(values),
                      'Currency': [v[0] for v in values.values()],
                      'Country': [v[1] for v in values.values()]})
    static_pq = inputs / 'static/parquet'
    static_pq.mkdir(parents=True)
    pq.write_table(table, static_pq / 'security_master.parquet', compression='zstd')
    assert pq.read_table(static_pq / 'security_master.parquet').equals(table)
    for fmt, folder, source_folder in [('csv', static, ROOT / 'attribution_data'),
                                        ('parquet', static_pq, ROOT / 'attribution_data_parquet')]:
        for path in [source_folder / f'index.{fmt}', *sorted(source_folder.glob(f'portfolio_*.{fmt}'))]:
            (folder / path.name).symlink_to(path.resolve())
    historical_pq = inputs / 'history/parquet'
    prepared = prepare_parquet(ROOT / 'attribution_data', ROOT / 'attribution_data_parquet', historical_pq)
    binaries = {'rust': ROOT / 'rust/target/release/portfolio-attribution', 'go': ROOT / 'benchmark-go-attribution'}
    environment = {'calculation_contract': 'security-level BF effects before country aggregation',
                   'historical_master_rows': historical_rows, 'static_master_rows': len(values),
                   'classification_constant_across_history': True, 'runs_per_cell': args.runs,
                   'logical_cpus': os.cpu_count(), 'threads': 'automatic', 'profile_read': True,
                   'cache_policy': 'normal filesystem cache; no cache flushing',
                   'historical_parquet': prepared,
                   'binary_sha256': {k: hashlib.sha256(v.read_bytes()).hexdigest() for k, v in binaries.items()}}
    (root / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')
    runs, checks = [], []
    reference = None
    for key in list(os.environ):
        if key.startswith('ATTRIBUTION_'):
            del os.environ[key]
    os.environ['ATTRIBUTION_PROFILE_READ'] = '1'
    # Alternate the history order and reverse engine/format order each round.
    cells = [('csv', 'rust'), ('csv', 'go'), ('parquet', 'go'), ('parquet', 'rust')]
    for rep in range(args.runs):
        for fmt, lang in cells[::1 if rep % 2 == 0 else -1]:
            for mode in (['history', 'static'] if rep % 2 == 0 else ['static', 'history']):
                folder = root / mode / fmt / lang / str(rep + 1)
                folder.mkdir(parents=True)
                data = (ROOT / 'attribution_data' if fmt == 'csv' else historical_pq) if mode == 'history' else (static if fmt == 'csv' else static_pq)
                command = [str(binaries[lang]), '--data', str(data), '--scenario', 'country',
                           '--output-dir', str(folder), '--metrics', str(folder / 'metrics.csv')]
                print(f'RUN {rep+1}/{args.runs} {mode} {lang} {fmt}', flush=True)
                usage = measured(command, folder / 'run.log')
                (folder / 'usage.json').write_text(json.dumps(dict(command=command, **usage), indent=2) + '\n')
                with (folder / 'metrics.csv').open() as f:
                    metrics = next(csv.DictReader(f))
                validate_metrics(metrics)
                assert int(metrics['days']) == 1095
                assert int(metrics['rows_read']) == 175200000
                assert int(metrics['rows_retained']) == 173010000
                assert int(metrics['groups']) == 173010000
                assert int(metrics['effect_values']) == 1038060000
                assert int(metrics['output_rows']) == 2190
                assert int(metrics['date_range_workers']) > 1
                outputs = {p.stem: p for p in folder.glob(f'country-portfolio_*.{fmt}')}
                assert outputs
                if reference is None:
                    reference = outputs
                assert outputs.keys() == reference.keys()
                for name, path in outputs.items():
                    n, delta = compare_files(reference[name], path)
                    assert delta == 0, delta
                    checks.append(dict(path=str(path.relative_to(root)), rows=n, max_absolute_difference=delta, passed=True))
                runs.append({**metrics, **usage, 'mode': mode, 'format': fmt, 'language': lang, 'repeat': rep+1})
                (root / 'runs.json').write_text(json.dumps(runs, indent=2) + '\n')
                (root / 'checks.json').write_text(json.dumps(checks, indent=2) + '\n')
                print(f"PASS {float(metrics['wall_seconds']):.3f}s; master {float(metrics['master_load_seconds']):.3f}s", flush=True)
    summary = []
    for fmt, lang in cells:
        row = dict(format=fmt, language=lang)
        for mode in ['history', 'static']:
            subset = [r for r in runs if (r['mode'], r['format'], r['language']) == (mode, fmt, lang)]
            for metric in ['wall_seconds', 'master_load_seconds', 'peak_rss_mib']:
                row[f'{mode}_{metric}'] = statistics.median(float(r[metric]) for r in subset)
        row['speedup'] = row['history_wall_seconds'] / row['static_wall_seconds']
        row['reduction_percent'] = 100 * (1 - row['static_wall_seconds'] / row['history_wall_seconds'])
        summary.append(row)
    (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2), flush=True)
    from render_security_country_report import render
    print(f'Updated {render(root)}', flush=True)


if __name__ == '__main__':
    main()
