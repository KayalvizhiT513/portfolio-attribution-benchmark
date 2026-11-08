"""Check arithmetic sums against daily attribution and serial/parallel parity."""
import math
import subprocess
import tempfile
from pathlib import Path

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
    print('Passed: arithmetic totals match daily output; per-security sums reconcile; base/local selection; 1/3-worker parity.')


if __name__ == '__main__': main()
