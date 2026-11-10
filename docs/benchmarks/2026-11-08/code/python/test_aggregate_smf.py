"""Check arithmetic sums against daily attribution and serial/parallel parity."""
import datetime as dt
import math
import subprocess
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from prepare_daily_smf import prepare
from test_six_inputs import fixture

NUMBERS = ['PortfolioWeight', 'BenchmarkWeight', 'Allocation', 'Selection',
           'Interaction', 'Total', 'PortfolioReturn', 'BenchmarkReturn',
           'ActiveReturn', 'PortfolioContribution', 'BenchmarkContribution', 'ActiveContribution']


def close(a, b):
    assert math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12), (a, b)


def main():
    binary = str(Path('rust/target/release/portfolio-attribution').resolve())
    with tempfile.TemporaryDirectory(prefix='aggregate-smf-') as temp:
        root = Path(temp)
        source = root/'source'; source.mkdir(); fixture(source)
        data = root/'snapshots'; prepare(source, data)

        def run(mode, threads, return_type='base', day=None):
            out = root/f'{mode}-{threads}-{return_type}-{day or "all"}'
            window = ['--start-date', day, '--end-date', day] if day else []
            subprocess.run([binary, '--scenario', 'six-input', '--data', str(data),
                '--output-dir', str(out), '--metrics', str(out/'metrics.csv'),
                '--threads', str(threads), '--aggregate', mode, '--return-type', return_type] + window,
                check=True, capture_output=True, text=True)
            return pq.read_table(out/'country-portfolio_PF.parquet').to_pylist()

        daily = run('daily', 1)
        for return_type in ('base', 'local'):
            total = run('total', 1, return_type)
            parallel = run('total', 3, return_type)
            assert len(total) == len(parallel) == 1
            row = total[0]
            assert row['Days'] == 4 and row['Segment'] == 'ALL'
            assert row['ReturnType'] == return_type and row['Scenario'] == 'arithmetic_sum'
            selected = [r for r in daily if r['ReturnType'] == return_type]
            for col in NUMBERS:
                # Daily country rows repeat portfolio-wide returns by country.
                if col in ('PortfolioReturn', 'BenchmarkReturn', 'ActiveReturn'):
                    by_date = {r['PeriodStart']: r[col] for r in selected}
                    expected = sum(by_date.values())
                else:
                    expected = sum(r[col] for r in selected)
                close(row[col], expected)
                close(row[col], parallel[0][col])
            security = run('security', 1, return_type)
            security_parallel = run('security', 3, return_type)
            assert len(security) == 2  # Two securities pass the US filter over this window.
            assert {r['Segment'] for r in security} == {r['Segment'] for r in security_parallel}
            for a, b in zip(security, security_parallel):
                assert a['Segment'] == b['Segment']
                for col in NUMBERS: close(a[col], b[col])
            for col in NUMBERS:
                if col not in ('PortfolioReturn', 'BenchmarkReturn', 'ActiveReturn'):
                    close(sum(r[col] for r in security), row[col])
            per_day = [r for day in ('2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04')
                       for r in run('security', 1, return_type, day)]
            for security_row in security:
                matching = [r for r in per_day if r['Segment'] == security_row['Segment']]
                for col in NUMBERS: close(security_row[col], sum(r[col] for r in matching))
            # S000002 is US only on the first two dates; it must not inherit later effects.
            s2 = next(r for r in security if r['Segment'] == 'S000002')
            assert s2['Days'] == 4
            assert s2['PortfolioWeight'] < row['PortfolioWeight']
        assert len(run('total', 3, 'both')) == 2
        # Change the snapshot universe and slot order midway through the window.
        before = {r['Segment']: r for r in run('security', 1)}
        expected_total = run('total', 1)[0]
        first_half = [r for day in ('2023-01-01', '2023-01-02')
                      for r in run('security', 1, day=day) if r['Segment'] == 'S000001']
        for name in ('security_master', 'portfolio_PF', 'index'):
            path = data/f'{name}.parquet'
            table = pq.read_table(path)
            date_col = 'SnapshotDate' if name == 'security_master' else 'EffectiveDate'
            values = table.to_pylist()
            for value in values:
                if value[date_col] >= dt.date(2023, 1, 3) and value['InstrumentId'] == 'S000001':
                    value['InstrumentId'] = 'NEW_SECURITY'
            with pq.ParquetWriter(path, table.schema) as writer:
                for day in sorted({v[date_col] for v in values}):
                    group = [v for v in values if v[date_col] == day]
                    if name == 'security_master' and day.day % 2: group.reverse()
                    writer.write_table(pa.Table.from_pylist(group, schema=table.schema), row_group_size=2)
        changed = run('security', 1)
        changed_parallel = run('security', 3)
        assert [r['Segment'] for r in changed] == ['NEW_SECURITY', 'S000001', 'S000002']
        for a, b in zip(changed, changed_parallel):
            assert a['Segment'] == b['Segment']
            for col in NUMBERS: close(a[col], b[col])
        by_id = {r['Segment']: r for r in changed}
        for col in NUMBERS:
            first_sum = sum(r[col] for r in first_half)
            close(by_id['S000001'][col], first_sum)
            close(by_id['NEW_SECURITY'][col], before['S000001'][col]-first_sum)
            close(by_id['S000002'][col], before['S000002'][col])
        changed_total = run('total', 3)[0]
        for col in NUMBERS: close(changed_total[col], expected_total[col])
    print('Passed: arithmetic totals match daily output; per-security sums reconcile; base/local selection; changing security universe and row order; 1/3-worker parity.')


if __name__ == '__main__': main()
