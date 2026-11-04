"""End-to-end six-reader checks, including an independent attribution oracle."""
import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path
import subprocess
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from test_attribution import FIELDS, compare_files, key, records, reference


def fixture(folder):
    days = [dt.date(2023, 1, n) for n in (1, 2, 3, 4)]
    master = pa.schema([('InstrumentId', pa.string()), ('EffectiveDate', pa.date32()),
                        ('Currency', pa.string()), ('Country', pa.string()), ('AnalyticsSecId', pa.int64())])
    accounts = pa.schema([('AccountId', pa.string()), ('EffectiveDate', pa.date32()),
                          ('PortfolioEntity', pa.string()), ('BaseCurrency', pa.string()), ('IsActive', pa.bool_())])
    forex = pa.schema([('EffectiveDate', pa.date32()), ('FromCurrency', pa.string()),
                       ('ToCurrency', pa.string()), ('Rate', pa.float64())])
    returns = pa.schema([('Entity', pa.string()), ('EffectiveDate', pa.date32()),
                         ('InstrumentId', pa.string()), ('Weight', pa.float64()),
                         ('BaseReturn', pa.float64()), ('LocalReturn', pa.float64())])
    analytics = pa.schema([('SecId', pa.int64()), ('Effective Date', pa.date32())]
                          + [(f'Float{i:02}', pa.float64()) for i in range(1, 16)])
    with pq.ParquetWriter(folder / 'security_master.parquet', master) as w:
        for day in (days[0], days[2]):
            w.write_table(pa.Table.from_pylist([dict(
                InstrumentId=f'S{i:06}', EffectiveDate=day,
                Currency='EUR' if i == 3 or (i == 2 and day == days[2]) else 'USD',
                Country='DE' if i == 3 or (i == 2 and day == days[2]) else 'US',
                AnalyticsSecId=i) for i in (1, 2, 3)], schema=master), row_group_size=2)
    with pq.ParquetWriter(folder / 'accounts.parquet', accounts) as w:
        for day in days:
            w.write_table(pa.Table.from_pylist([dict(AccountId='A', EffectiveDate=day,
                PortfolioEntity='PF', BaseCurrency='USD', IsActive=True)], schema=accounts))
    with pq.ParquetWriter(folder / 'forex.parquet', forex) as w:
        for day in days:
            w.write_table(pa.Table.from_pylist([dict(EffectiveDate=day, FromCurrency=c,
                ToCurrency='USD', Rate=r) for c, r in [('USD', 1.), ('EUR', 1.1)]], schema=forex))
    with pq.ParquetWriter(folder / 'analytics.parquet', analytics) as w:
        ids = np.arange(1, 65540, dtype=np.int64)
        for day in days:
            arrays = [pa.array(ids), pa.array([day] * len(ids), pa.date32())]
            arrays += [pa.array(ids / 100000 + i / 1000) for i in range(1, 16)]
            w.write_table(pa.Table.from_arrays(arrays, schema=analytics), row_group_size=32768)
    data = {'PF': [], 'INDEX': []}
    for entity, stem in [('PF', 'portfolio_PF'), ('INDEX', 'index')]:
        with pq.ParquetWriter(folder / f'{stem}.parquet', returns) as w:
            # Index-only extra date must not matter for the requested API window.
            for day in days + ([dt.date(2023, 1, 5)] if entity == 'INDEX' else []):
                weights = [.5, .3, .2] if entity == 'PF' else [.2, .5, .3]
                holdings = {f'S{i:06}': (weight, (i - 2) * .01 + (.003 if entity == 'PF' else -.002),
                                           (i - 2) * .015 + (.001 if entity == 'PF' else .002))
                            for i, weight in enumerate(weights, 1)}
                data[entity].append((str(day), holdings))
                w.write_table(pa.Table.from_pylist([dict(Entity=entity, EffectiveDate=day,
                    InstrumentId=s, Weight=v[0], BaseReturn=v[1], LocalReturn=v[2])
                    for s, v in holdings.items()], schema=returns), row_group_size=2)
    return days, data


def rewrite_dates(path, table):
    column = 'Effective Date' if 'Effective Date' in table.column_names else 'EffectiveDate'
    with pq.ParquetWriter(path, table.schema) as w:
        for day in sorted(set(table[column].to_pylist())):
            w.write_table(table.filter(pc.equal(table[column], pa.scalar(day, pa.date32()))), row_group_size=32768)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rust', type=Path, default=Path('rust/target/release/portfolio-attribution'))
    p.add_argument('--go', type=Path, default=Path('results/six-input/benchmark-go'))
    args = p.parse_args()
    checks = []
    with tempfile.TemporaryDirectory(prefix='six-input-') as tmp:
        root = Path(tmp)
        inputs = root / 'inputs'; inputs.mkdir()
        days, data = fixture(inputs)
        for language, binary in [('rust', args.rust.resolve()), ('go', args.go.resolve())]:
            def run(tag, threads=3, start='2023-01-01', end='2023-01-04', fail=None):
                out = root / f'{language}-{tag}'
                cmd = [str(binary), '--scenario', 'six-input', '--data', str(inputs),
                       '--output-dir', str(out), '--metrics', str(out / 'metrics.csv'),
                       '--threads', str(threads), '--start-date', start, '--end-date', end]
                r = subprocess.run(cmd, capture_output=True, text=True)
                if fail:
                    assert r.returncode != 0 and fail.lower() in r.stderr.lower(), (language, tag, r.stderr)
                    assert not (out / 'country-portfolio_PF.parquet').exists()
                else:
                    assert r.returncode == 0, (language, tag, r.stderr)
                checks.append(f'{language}/{tag}')
                return out
            serial = run('serial', threads=1)
            parallel = run('parallel')
            compare_files(serial / 'country-portfolio_PF.parquet', parallel / 'country-portfolio_PF.parquet')
            expected = {}
            for i, day in enumerate(days):
                countries = {'S000001': 'US', 'S000002': 'DE' if i >= 2 else 'US', 'S000003': 'DE'}
                single = {entity: [values[i]] for entity, values in data.items()}
                expected.update(reference(countries, single, 'PF', 'country', 'day'))
            actual = list(records(parallel / 'country-portfolio_PF.parquet'))
            assert len(actual) == len(expected)
            for row in actual:
                for field, target in zip(FIELDS, expected[key(row)]):
                    assert math.isclose(float(row[field]), target, rel_tol=1e-8, abs_tol=1e-10), (language, field, row, target)
            with (parallel / 'metrics.csv').open() as f:
                mt = next(csv.DictReader(f))
            assert int(mt['readers_per_worker']) == 6 and int(mt['workers']) == 3
            assert int(mt['analytics_rows_decoded']) == 4 * 65539  # complete dates span multiple groups and exceed 65536
            assert int(mt['security_master_rows_decoded']) == 9  # Three workers each need an initial snapshot.
            assert int(mt['smf_snapshot_loads']) == 3 and int(mt['smf_cache_hits']) == 1
            with (serial / 'metrics.csv').open() as f:
                serial_metrics = next(csv.DictReader(f))
            assert int(serial_metrics['security_master_rows_decoded']) == 6  # Jan 1 reused on Jan 2; Jan 3 reloads.
            assert int(serial_metrics['smf_snapshot_loads']) == 2
            assert int(serial_metrics['smf_cache_hits']) == 2
            assert int(mt['joined_us_securities']) == 6
            expected_sum = sum(sum(s / 100000 + n / 1000 for n in range(1, 16)) for s in [1, 2, 1, 2, 1, 1])
            assert math.isclose(float(mt['analytics_checksum']), expected_sum, abs_tol=1e-10)
            assert float(mt['fx_checksum']) == 6
            two_workers = run('cache-parallel', threads=2)
            compare_files(serial / 'country-portfolio_PF.parquet', two_workers / 'country-portfolio_PF.parquet')
            with (two_workers / 'metrics.csv').open() as f:
                cached = next(csv.DictReader(f))
            assert int(cached['smf_snapshot_loads']) == 2 and int(cached['smf_cache_hits']) == 2
            assert int(cached['security_master_rows_decoded']) == 6
            window = run('window', start='2023-01-02', end='2023-01-02')
            assert len(list(records(window / 'country-portfolio_PF.parquet'))) == 2
            run('reversed', start='2023-01-03', end='2023-01-01', fail='start-date')
            run('missing-return-coverage', end='2023-01-05', fail='missing date')
            for name, label, transform, error in [
                ('analytics', 'missing-date', lambda t: t.filter(pc.not_equal(t['Effective Date'], pa.scalar(days[1], pa.date32()))), 'missing date'),
                ('analytics', 'missing-security', lambda t: t.filter(pc.not_equal(t['SecId'], 2)), 'missing analytics'),
                ('analytics', 'nan-last-column', lambda t: t.set_column(t.schema.get_field_index('Float15'), 'Float15', pa.array([float('nan')] + t['Float15'].to_pylist()[1:])), 'nonfinite analytics'),
                ('forex', 'missing-fx', lambda t: t.filter(pc.not_equal(t['FromCurrency'], 'USD')), 'missing FX'),
                ('accounts', 'inactive', lambda t: t.set_column(t.schema.get_field_index('IsActive'), 'IsActive', pa.array([False] * len(t))), 'inactive'),
                ('security_master', 'duplicate-master', lambda t: pa.concat_tables([t, t.slice(0, 1)]).sort_by([('EffectiveDate', 'ascending')]), 'duplicate'),
            ]:
                path = inputs / f'{name}.parquet'
                original = path.read_bytes()
                try:
                    rewrite_dates(path, transform(pq.read_table(path)))
                    run(label, fail=error)
                finally:
                    path.write_bytes(original)
        compare_files(root / 'rust-parallel/country-portfolio_PF.parquet', root / 'go-parallel/country-portfolio_PF.parquet')
    print(json.dumps(dict(passed=len(checks), checks=checks), indent=2))


if __name__ == '__main__':
    main()
