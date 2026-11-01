"""CSV/Parquet parity, batch boundaries, mixed formats and invalid-input checks.

Run after building both native binaries: .venv/bin/python python/test_parquet_input.py
"""
import argparse
import csv
import math
from datetime import date
from pathlib import Path
import shutil
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from test_attribution import compare_files, fixture, records, FIELDS


def convert(source, target):
    types = ({'EffectiveDate': pa.date32(), 'Weight': pa.float64(),
              'BaseReturn': pa.float64(), 'LocalReturn': pa.float64()}
             if source.stem == 'index' or source.stem.startswith('portfolio_') else {})
    table = pacsv.read_csv(source, convert_options=pacsv.ConvertOptions(column_types=types))
    # Exercise projection and name resolution, with an unrelated nested column.
    table = table.append_column('Ignored', pa.array([{'x': 1}] * table.num_rows))
    table = table.select(list(reversed(table.column_names)))
    pq.write_table(table, target, compression='zstd', row_group_size=7)


def run(binary, data, out, scenario='country', period=None, subset=None, limit=0, valid=True):
    args = [str(binary), '--data', str(data), '--output-dir', str(out),
            '--metrics', str(out / 'metrics.csv'), '--scenario', scenario,
            '--threads', '2', '--max-days', str(limit)]
    if period is not None:
        args += ['--period', period]
    if subset:
        args += ['--instruments-file', str(subset)]
    result = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if valid:
        assert result.returncode == 0, (args, result.stderr)
    else:
        assert result.returncode != 0, args
        assert not [p for p in out.glob('*') if p.suffix in ['.csv', '.parquet']], (out, result.stderr)
    return result


def compare_outputs(a, b):
    left = {p.stem: p for p in a.glob('*') if p.suffix in ['.csv', '.parquet'] and p.name != 'metrics.csv'}
    right = {p.stem: p for p in b.glob('*') if p.suffix in ['.csv', '.parquet'] and p.name != 'metrics.csv'}
    assert left.keys() == right.keys() and left
    for name in left:
        compare_files(left[name], right[name])
        for path in [left[name], right[name]]:
            if path.suffix == '.parquet':
                schema = pq.ParquetFile(path).schema_arrow
                assert len(schema) == 20
                assert schema.field('PeriodStart').type == pa.date32()
                assert schema.field('PeriodEnd').type == pa.date32()
                assert schema.field('Days').type == pa.uint32()
                assert all(schema.field(field).type == pa.float64() for field in FIELDS)
    with (a / 'metrics.csv').open() as f, (b / 'metrics.csv').open() as g:
        x, y = next(csv.DictReader(f)), next(csv.DictReader(g))
        for field in ['days', 'rows_read', 'rows_retained', 'groups', 'effect_values', 'output_rows', 'report_rows']:
            assert x[field] == y[field], (field, x[field], y[field])
        for folder, row in [(a, x), (b, y)]:
            if 'parquet_batch_read_decode_seconds' in row and folder.name.endswith('parquet'):
                decode = float(row['parquet_batch_read_decode_seconds'])
                adapter = float(row['parquet_to_daily_buffer_seconds'])
                assert decode > 0 and adapter > 0 and int(row['parquet_batches']) > 0
                assert math.isclose(decode + adapter, float(row['read_parse_seconds'])-float(row.get('boundary_scan_seconds',0)), rel_tol=1e-9, abs_tol=1e-6)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--rust', type=Path, default=Path('rust/target/release/portfolio-attribution'))
    ap.add_argument('--go', type=Path, default=Path('benchmark-go-attribution'))
    args = ap.parse_args()
    binaries = [args.rust.resolve(), args.go.resolve()]
    with tempfile.TemporaryDirectory(prefix='parquet-input-') as tmp:
        root = Path(tmp)
        source, parquet, mixed = [root / x for x in ['csv', 'parquet', 'mixed']]
        source.mkdir(); parquet.mkdir(); mixed.mkdir()
        fixture(source)
        for path in source.glob('*.csv'):
            convert(path, parquet / path.with_suffix('.parquet').name)
        for path in parquet.glob('*.parquet'):
            shutil.copy(path, mixed / path.name)
        (mixed / 'portfolio_PF2.parquet').unlink()
        shutil.copy(source / 'portfolio_PF2.csv', mixed)
        cases = [('security', 'all'), ('subset', 'all'), ('country', 'day'),
                 ('weekly', 'week'), ('monthly', 'month'), ('monthly', 'quarter'),
                 ('monthly', 'year'), ('subset', 'month'), ('security', 'month')]
        for language, binary in zip(['rust', 'go'], binaries):
            for scenario, period in cases:
                outputs = []
                for folder in [source, parquet, mixed]:
                    out = root / language / f'{scenario}-{period}-{folder.name}'
                    subset = folder / ('subset.csv' if folder == source else 'subset.parquet')
                    run(binary, folder, out, scenario, period, subset)
                    if scenario == 'country':
                        for report in out.glob('country-portfolio_*'):
                            if report.suffix not in ['.csv', '.parquet']: continue
                            rows = list(records(report))
                            assert len(rows) == 210  # 105 dates, base + local
                            assert all(r['Segment'] == 'US' and r['Days'] == '1'
                                       and r['PeriodStart'] == r['PeriodEnd']
                                       and abs(float(r['PortfolioWeight']) - 1) < 1e-12
                                       and abs(float(r['BenchmarkWeight']) - 1) < 1e-12 for r in rows)
                    outputs.append(out)
                compare_outputs(outputs[0], outputs[1])
                compare_outputs(outputs[0], outputs[2])
            for folder in [source, parquet]:
                run(binary, folder, root / language / f'limit-{folder.name}', limit=2)
            compare_outputs(root / language / 'limit-csv', root / language / 'limit-parquet')
        # Schema, null, numeric and semantic errors must propagate, without finals.
        path = parquet / 'portfolio_PF1.parquet'
        original = pq.read_table(path).drop(['Ignored'])
        def replace(name, values, dtype=None):
            return original.set_column(original.schema.get_field_index(name), name, pa.array(values, type=dtype))
        count = original.num_rows
        bad_tables = [original.drop(['Weight']),
                      replace('Weight', ['bad'] * count),
                      replace('Weight', [None] + original['Weight'].to_pylist()[1:], pa.float64()),
                      replace('BaseReturn', [float('nan')] + original['BaseReturn'].to_pylist()[1:]),
                      replace('LocalReturn', [-1.] + original['LocalReturn'].to_pylist()[1:]),
                      replace('InstrumentId', ['UNKNOWN'] + original['InstrumentId'].to_pylist()[1:]),
                      replace('Entity', ['OTHER'] + original['Entity'].to_pylist()[1:]),
                      pa.concat_tables([original.slice(0, 1), original]),
                      pa.concat_tables([original.slice(3), original.slice(0, 3)])]
        for i, table in enumerate(bad_tables):
            pq.write_table(table, path, row_group_size=2)
            for language, binary in zip(['rust', 'go'], binaries):
                run(binary, parquet, root / f'bad-{language}-{i}', valid=False)
        convert(source / 'portfolio_PF1.csv', path)
        for stem in ['portfolio_PF1', 'index', 'security_master']:
            duplicate = parquet / f'{stem}.csv'
            shutil.copy(source / duplicate.name, duplicate)
            for language, binary in zip(['rust', 'go'], binaries):
                result = run(binary, parquet, root / f'duplicate-{language}-{stem}', valid=False)
                assert 'ambiguous' in result.stderr
            duplicate.unlink()
        # Filtering is applied to both sides, and a zero-US side must fail.
        master_path = parquet / 'security_master.parquet'
        saved_master = pq.read_table(master_path)
        changed = saved_master['Country'].to_pylist()
        changed = ['CA' if value == 'US' else value for value in changed]
        pq.write_table(saved_master.set_column(saved_master.schema.get_field_index('Country'),
                                               'Country', pa.array(changed)), master_path)
        for language, binary in zip(['rust', 'go'], binaries):
            result = run(binary, parquet, root / f'no-us-{language}', valid=False)
            assert 'zero selected weight' in result.stderr
        pq.write_table(saved_master, master_path)
        for language, binary in zip(['rust', 'go'], binaries):
            result = run(binary, parquet, root / f'country-month-{language}', period='month', valid=False)
            assert 'country requires --period day' in result.stderr
        # A single date must span 65,536-row batches in both adapters.
        large_csv, large_parquet = root / 'large-csv', root / 'large-parquet'
        large_csv.mkdir(); large_parquet.mkdir()
        n = 65539
        ids = [f'L{i:06d}' for i in range(n)]
        master = pa.table({'InstrumentId': ids, 'Currency': ['USD'] * n, 'Country': ['US'] * n})
        pacsv.write_csv(master, large_csv / 'security_master.csv')
        pq.write_table(master, large_parquet / 'security_master.parquet')
        for entity, stem in [('INDEX', 'index'), ('PF1', 'portfolio_PF1')]:
            table = pa.table({'Entity': [entity] * (2*n),
                              'EffectiveDate': pa.array([date(2024, 1, 1)] * n + [date(2024, 1, 2)] * n, type=pa.date32()),
                              'InstrumentId': ids * 2, 'Weight': [1/n] * (2*n),
                              'BaseReturn': [0.001 if entity == 'PF1' else 0.002] * (2*n),
                              'LocalReturn': [0.003] * (2*n)})
            pacsv.write_csv(table, large_csv / f'{stem}.csv')
            pq.write_table(table, large_parquet / f'{stem}.parquet', row_group_size=100001)
        for language, binary in zip(['rust', 'go'], binaries):
            a, b = root / f'large-{language}-csv', root / f'large-{language}-parquet'
            run(binary, large_csv, a, scenario='security')
            run(binary, large_parquet, b, scenario='security')
            metadata = pq.ParquetFile(b / 'security-portfolio_PF1.parquet').metadata
            assert metadata.num_rows == 65539 * 2 and metadata.num_row_groups >= 3
            assert all(metadata.row_group(i).num_rows <= 65536 for i in range(metadata.num_row_groups))
            compare_outputs(a, b)
    print('PASS: CSV/Parquet/mixed parity across 9 scenario/period cases in both languages; '
          'subset formats, max-days, projection, >65k daily holdings, invalid inputs and duplicates checked')


if __name__ == '__main__':
    main()
