"""Generate a small, inspectable sparse-SMF trace for both engines."""
import argparse
import datetime as dt
from pathlib import Path
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
from test_six_inputs import fixture, rewrite_dates


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, default=Path('results/smf-trace-demo'))
    args = p.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    inputs = root / 'inputs'
    inputs.mkdir()
    days, _ = fixture(inputs)
    path = inputs / 'security_master.parquet'
    table = pq.read_table(path)
    rows = [r for r in table.to_pylist() if r['EffectiveDate'] == days[0] or r['InstrumentId'] == 'S000002']
    for row in rows:
        if row['InstrumentId'] == 'S000003':
            row['EffectiveDate'] = dt.date(2022, 12, 31)
    rows.extend([
        dict(InstrumentId='S000001', EffectiveDate=days[1], Country='US', Currency='EUR', AnalyticsSecId=4),
        dict(InstrumentId='S000004', EffectiveDate=days[1], Country='FR', Currency='EUR', AnalyticsSecId=4),
        # An unchanged attribute record is still an applied effective-date version.
        dict(InstrumentId='S000003', EffectiveDate=days[1], Country='DE', Currency='EUR', AnalyticsSecId=3),
        dict(InstrumentId='S000001', EffectiveDate=dt.date(2023, 1, 5), Country='ZZ', Currency='USD', AnalyticsSecId=1),
    ])
    rewrite_dates(path, pa.Table.from_pylist(rows, schema=table.schema))
    for engine, binary in [('rust', 'rust/target/release/portfolio-attribution'), ('go', 'results/six-input/benchmark-go')]:
        for workers in (1, 3):
            out = root / f'{engine}-{workers}-workers'
            subprocess.run([str(Path(binary).resolve()), '--scenario', 'six-input', '--data', str(inputs),
                '--output-dir', str(out), '--metrics', str(out / 'metrics.csv'), '--threads', str(workers),
                '--start-date', '2023-01-01', '--end-date', '2023-01-04', '--smf-trace', 'records'], check=True)
            subprocess.run([str(Path('.venv/bin/python').resolve()), 'python/inspect_smf_trace.py',
                '--data', str(inputs), '--trace-dir', str(out)], check=True)
    print(f'Demo inputs and validated traces: {root}')


if __name__ == '__main__':
    main()
