"""Sequential US cumulative benchmarks with optional cache purge before every native run."""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics

from benchmark_country_runtime import prepare_parquet
from benchmark_native import measured
from input_cache import MODES

ROOT = Path(__file__).resolve().parents[1]
META = {'Entity', 'Level', 'PeriodStart', 'EffectiveDate', 'Observations', 'LastObservationDate'}


def compare(left, right):
    with left.open() as f, right.open() as g:
        a, b = list(csv.DictReader(f)), list(csv.DictReader(g))
    assert len(a) == len(b) == 36, (left, right)
    maximum = 0.0
    for x, y in zip(a, b):
        assert x.keys() == y.keys()
        for key in x:
            if key in META or not x[key] or not y[key]:
                assert x[key] == y[key], (key, x[key], y[key])
            else:
                xv, yv = float(x[key]), float(y[key])
                assert math.isclose(xv, yv, rel_tol=1e-10, abs_tol=1e-12), (key, xv, yv)
                maximum = max(maximum, abs(xv-yv))
    return maximum


def render_summary(root, runs):
    phase_fields=['master_load_seconds','boundary_scan_seconds','read_pipeline_wall_seconds','compute_seconds','write_seconds']
    fields=phase_fields+['wall_seconds','other_seconds','cache_prepare_seconds','process_wall_seconds','peak_rss_mib','cpu_seconds','average_cores']
    cells={}
    for r in runs:
        item=dict(r)
        item['other_seconds']=float(r['wall_seconds'])-sum(float(r[k]) for k in phase_fields)
        cells.setdefault((r['format'],r['language']),[]).append(item)
    summary=[]
    for (fmt,lang),items in cells.items():
        summary.append(dict(format=fmt,language=lang,repetitions=len(items),**{k:statistics.median(float(x[k]) for x in items) for k in fields}))
    (root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    by_key={(r['format'],r['language']):r for r in summary}
    order=[('csv','go'),('csv','rust'),('parquet','go'),('parquet','rust')]
    lines=['# Cumulative US Total runtime split','','Seconds; attribution reader only. Medians when repetitions exceed one.',
           'Read-pipeline time is directly measured elapsed time. Purge is excluded from engine runtime.','',
           '| Step | Go CSV | Rust CSV | Go Parquet | Rust Parquet |',
           '|---|---:|---:|---:|---:|']
    labels=[('Load security master','master_load_seconds'),('Discover date ranges','boundary_scan_seconds'),('Read / US filter / normalize / daily totals','read_pipeline_wall_seconds'),('Calculate cumulative statistics for 36 month-ends','compute_seconds'),('Write output','write_seconds'),('Other setup / alignment / finalization','other_seconds'),('Total engine runtime','wall_seconds'),('Cache preparation (excluded)','cache_prepare_seconds'),('Total process elapsed','process_wall_seconds')]
    for label,key in labels:
        values=[format(by_key[k][key],'.6f') if k in by_key else '—' for k in order]
        lines.append('| '+label+' | '+' | '.join(values)+' |')
    lines.extend(['','Reading, filtering, normalization and daily aggregation overlap across workers;',
                  'they are included together in the directly measured read-pipeline interval.',
                  'Other time is the residual after subtracting the five directly measured phases.',
                  'Summed worker durations are not added to elapsed phases.',
                  'Per-cell medians may not sum exactly when repetitions exceed one.',
                  'Raw evidence: runs.json, per-run metrics.csv and usage.json; output parity: checks.json.'])
    (root/'RUNTIME_SPLIT.md').write_text('\n'.join(lines)+'\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--cache', choices=MODES, default=os.environ.get('BENCHMARK_INPUT_CACHE', 'normal'))
    parser.add_argument('--csv-data', type=Path, default=ROOT/'attribution_data')
    parser.add_argument('--parquet-source', type=Path, default=ROOT/'attribution_data_parquet')
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--formats', nargs='+', choices=['csv','parquet'], default=['csv','parquet'])
    args = parser.parse_args()
    if args.runs<1: parser.error('--runs must be positive')
    binaries = {'go': ROOT/'benchmark-go-attribution', 'rust': ROOT/'rust/target/release/portfolio-attribution'}
    for path in binaries.values():
        if not path.is_file():
            raise FileNotFoundError(f'Build the unified engine first: {path}')
    root = args.output_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    print('Preparing matching effective-dated Parquet master before cache preparation...', flush=True)
    parquet = root/'inputs/parquet'
    prepared = prepare_parquet(args.csv_data.resolve(), args.parquet_source.resolve(), parquet)
    base = [(fmt,lang) for fmt in args.formats for lang in ['go','rust']]
    order = []
    for rep in range(args.runs):
        for fmt,lang in (base if rep%2==0 else list(reversed(base))):
            order.append((fmt,lang,rep+1))
    (root/'environment.json').write_text(json.dumps({
        'platform': platform.platform(), 'logical_cpus': os.cpu_count(),
        'thread_policy': 'Automatic native parallelism; no thread/worker caps; GOMAXPROCS override removed',
        'cache_mode': args.cache, 'sequential': True,
        'calculation': 'Cumulative US Total statistics, no annualization',
        'prepared_parquet': prepared, 'order': order,
        'binary_sha256': {lang: hashlib.sha256(path.read_bytes()).hexdigest() for lang, path in binaries.items()},
    }, indent=2)+'\n')
    saved = dict(os.environ)
    runs, checks, reference = [], [], None
    try:
        os.environ['BENCHMARK_INPUT_CACHE'] = args.cache
        os.environ.pop('GOMAXPROCS', None)
        for key in list(os.environ):
            if key.startswith('ATTRIBUTION_'): os.environ.pop(key)
        for fmt, lang, rep in order:
            out = root/fmt/lang/str(rep)
            out.mkdir(parents=True)
            data = args.csv_data.resolve() if fmt == 'csv' else parquet
            command = [str(binaries[lang]), '--scenario', 'cumulative', '--data', str(data),
                       '--output-dir', str(out), '--metrics', str(out/'metrics.csv')]
            print(f'RUN {rep}/{args.runs} {lang} {fmt}: {args.cache}; automatic cores', flush=True)
            # measured() conditionally prepares the cache immediately before Popen,
            # outside measured time. Purge errors prevent the native run from starting.
            usage = measured(command, out/'run.log')
            (out/'usage.json').write_text(json.dumps({'command': command, **usage}, indent=2)+'\n')
            with (out/'metrics.csv').open() as f:
                metrics = next(csv.DictReader(f))
            assert int(metrics['output_rows']) == 36
            outputs = {p.name: p for p in out.glob('cumulative-portfolio_*.csv')}
            assert len(outputs) == 1, outputs
            if reference is None:
                reference = outputs
            assert outputs.keys() == reference.keys()
            maximum = max(compare(reference[name], path) for name, path in outputs.items())
            checks.append({'format': fmt, 'language': lang, 'repetition': rep, 'rows': 36, 'max_absolute_difference': maximum, 'passed': True})
            runs.append({'format': fmt, 'repetition': rep, **metrics, **usage})
            (root/'runs.json').write_text(json.dumps(runs, indent=2)+'\n')
            (root/'checks.json').write_text(json.dumps(checks, indent=2)+'\n')
            with (root/'runs.csv').open('w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=list(runs[0]))
                writer.writeheader(); writer.writerows(runs)
            render_summary(root,runs)
            print(f"PASS {lang} {fmt}: {float(metrics['wall_seconds']):.3f}s; "
                  f"{metrics['threads']} cores configured; 36 rows", flush=True)
    finally:
        os.environ.clear(); os.environ.update(saved)
    print(f'Complete: {root}', flush=True)


if __name__ == '__main__':
    main()
