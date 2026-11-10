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
from inspect_smf_trace import validate_trace


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
            def run(tag, threads=3, start='2023-01-01', end='2023-01-04', fail=None, trace=None, profile=False):
                out = root / f'{language}-{tag}'
                cmd = [str(binary), '--scenario', 'six-input', '--data', str(inputs),
                       '--output-dir', str(out), '--metrics', str(out / 'metrics.csv'),
                       '--threads', str(threads), '--start-date', start, '--end-date', end]
                if trace:
                    cmd += ['--smf-trace', trace]
                if profile:
                    cmd += ['--stage-profile', 'true']
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
            assert not list(serial.glob('smf-trace-worker-*.jsonl'))
            compare_files(serial / 'country-portfolio_PF.parquet', parallel / 'country-portfolio_PF.parquet')
            if language == 'rust':
                profiled = run('stage-profile', threads=1, trace='summary', profile=True)
                compare_files(serial / 'country-portfolio_PF.parquet', profiled / 'country-portfolio_PF.parquet')
                stage_events = [json.loads(line) for line in (profiled / 'stage-profile-worker-01.jsonl').read_text().splitlines()]
                assert stage_events[0]['event'] == 'worker_start' and stage_events[-1]['event'] == 'worker_complete'
                date_events = [e for e in stage_events if e['event'] == 'date_timing']
                assert len(date_events) == 4 and sum(e['initialization'] for e in date_events) == 1
                for e in date_events:
                    assert all(v >= 0 for k,v in e.items() if k.endswith('_seconds') and isinstance(v, (int,float)))
                    components = ['accounts_fx_lookup_seconds', 'analytics_lookup_seconds', 'returns_processing_seconds',
                                  'join_validation_seconds', 'attribution_seconds', 'calculate_cleanup_and_other_seconds']
                    assert math.isclose(sum(e[k] for k in components), e['calculate_call_seconds'], abs_tol=1e-9)
                    assert math.isclose(e['input_phase_seconds'] + e['calculate_call_seconds'] + e['date_other_seconds'], e['date_seconds'], abs_tol=1e-9)
                    assert e['input_phase_seconds'] >= max(e['input_read_decode_seconds'])
                    assert math.isclose(e['smf_read_decode_seconds'] + e['smf_apply_and_trace_seconds'] + e['smf_release_and_other_seconds'], e['input_read_decode_seconds'][2], abs_tol=1e-9)
                stages = json.loads((profiled / 'stage-profile.json').read_text())
                assert stages['worker_phase_seconds'] >= stage_events[-1]['worker_seconds']
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
            assert int(mt['security_master_rows_decoded']) == 12  # Later workers replay earlier effective dates.
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
            assert int(cached['security_master_rows_decoded']) == 9
            # Delta history is equivalent to the complete snapshots above:
            # S3 has an older effective date; Jan 3 changes only S2. A future
            # change must never leak into the requested calculation window.
            master_path = inputs / 'security_master.parquet'
            original_master = master_path.read_bytes()
            try:
                original_table = pq.read_table(master_path)
                changes = original_table.to_pylist()
                changes = [r for r in changes if r['EffectiveDate'] == days[0] or r['InstrumentId'] == 'S000002']
                for r in changes:
                    if r['InstrumentId'] == 'S000003':
                        r['EffectiveDate'] = dt.date(2022, 12, 31)
                future = dict(changes[0], EffectiveDate=dt.date(2023, 1, 5), Country='ZZ')
                changes.append(future)
                rewrite_dates(master_path, pa.Table.from_pylist(changes, schema=original_table.schema))
                delta_serial = run('delta-serial', threads=1)
                delta_parallel = run('delta-parallel')
                for out in (delta_serial, delta_parallel):
                    compare_files(serial / 'country-portfolio_PF.parquet', out / 'country-portfolio_PF.parquet')
                with (delta_serial / 'metrics.csv').open() as f:
                    delta_metrics = next(csv.DictReader(f))
                assert int(delta_metrics['security_master_rows_decoded']) == 4
                assert int(delta_metrics['smf_cache_hits']) == 2
                late = run('delta-late-start', threads=1, start='2023-01-04', end='2023-01-04')
                late_rows = list(records(late / 'country-portfolio_PF.parquet'))
                expected_late = [r for r in records(serial / 'country-portfolio_PF.parquet') if r['PeriodEnd'] == '2023-01-04']
                assert len(late_rows) == len(expected_late)
                for actual_row, expected_row in zip(sorted(late_rows, key=key), sorted(expected_late, key=key)):
                    assert key(actual_row) == key(expected_row)
                    for field in FIELDS:
                        assert math.isclose(float(actual_row[field]), float(expected_row[field]), rel_tol=1e-8, abs_tol=1e-10)
                # Update alias/currency, introduce a security and a new country.
                # Unchanged instruments must survive all three sparse updates.
                changes.extend([
                    dict(InstrumentId='S000001', EffectiveDate=days[1], Currency='EUR', Country='US', AnalyticsSecId=4),
                    dict(InstrumentId='S000004', EffectiveDate=days[1], Currency='EUR', Country='FR', AnalyticsSecId=4),
                    dict(InstrumentId='S000003', EffectiveDate=days[1], Currency='EUR', Country='FR', AnalyticsSecId=3),
                ])
                rewrite_dates(master_path, pa.Table.from_pylist(changes, schema=original_table.schema))
                updated = run('delta-attributes', threads=1, trace='records')
                updated_parallel = run('delta-attributes-parallel', trace='records')
                for traced in (updated, updated_parallel):
                    for path in traced.glob('smf-trace-worker-*.jsonl'):
                        validate_trace(path, inputs)
                # A plausible-looking but incorrect after value must be caught.
                corrupted = root / f'{language}-corrupted-trace.jsonl'
                trace_lines = (updated / 'smf-trace-worker-01.jsonl').read_text().splitlines()
                bad = [json.loads(line) for line in trace_lines]
                next(e for e in bad if e['event'] == 'security_applied')['after']['country'] = 'INVALID'
                corrupted.write_text(''.join(json.dumps(e) + '\n' for e in bad))
                try:
                    validate_trace(corrupted, inputs)
                except AssertionError:
                    pass
                else:
                    raise AssertionError('corrupted cache trace passed validation')
                summary = run('delta-summary', threads=1, trace='summary')
                events = [json.loads(line) for line in (summary / 'smf-trace-worker-01.jsonl').read_text().splitlines()]
                assert not any(e['event'] == 'security_applied' for e in events)
                assert [e['mode'] for e in events if e['event'] == 'date_ready'] == ['initialize', 'update', 'update', 'reuse']
                compare_files(updated / 'country-portfolio_PF.parquet', summary / 'country-portfolio_PF.parquet')
                compare_files(updated / 'country-portfolio_PF.parquet', updated_parallel / 'country-portfolio_PF.parquet')
                compare_files(serial / 'country-portfolio_PF.parquet', updated / 'country-portfolio_PF.parquet')
                with (updated / 'metrics.csv').open() as f:
                    updated_metrics = next(csv.DictReader(f))
                assert math.isclose(float(updated_metrics['analytics_checksum']), expected_sum + 3 * 15 * 3 / 100000, abs_tol=1e-10)
                assert math.isclose(float(updated_metrics['fx_checksum']), 6.3, abs_tol=1e-10)
            finally:
                master_path.write_bytes(original_master)
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
