"""Check sectors, as-of regrouping, trace replay, and serial/parallel parity."""
import datetime as dt
import math
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from prepare_daily_smf import prepare as snapshots
from prepare_sector_smf import prepare, SECTORS
from test_six_inputs import fixture, rewrite_dates
from test_aggregate_smf import NUMBERS, close
from inspect_smf_trace import validate_trace


def main():
    binary = str(Path('rust/target/release/portfolio-attribution').resolve())
    with tempfile.TemporaryDirectory(prefix='sector-smf-') as temp:
        root = Path(temp); source = root/'source'; source.mkdir(); fixture(source)
        history = root/'sector-history'; prepare(source, history)
        # Two held securities share a sector on the first two dates.
        hp = history/'security_master.parquet'; ht = pq.read_table(hp)
        hr = ht.to_pylist()
        for value in hr:
            if value['InstrumentId']=='S000002': value['Sector']=SECTORS[0]
        rewrite_dates(hp, pa.Table.from_pylist(hr, schema=ht.schema))
        data = root/'snapshots'; snapshots(history, data)
        raw = root/'raw'; snapshots(source, raw)
        counter = 0

        def run(mode, threads=1, return_type='base', day=None, directory=data, trace=False):
            nonlocal counter
            counter += 1; out = root/f'out-{counter}'
            command = [binary, '--scenario', 'six-input', '--data', str(directory),
                '--output-dir', str(out), '--metrics', str(out/'metrics.csv'), '--aggregate', mode,
                '--threads', str(threads), '--return-type', return_type]
            if day: command += ['--start-date', day, '--end-date', day]
            if trace: command += ['--smf-trace', 'records']
            result = subprocess.run(command, capture_output=True, text=True)
            return result, out

        result, _ = run('sector', directory=raw)
        assert result.returncode and 'requires Sector' in result.stderr, result.stderr
        result, out = run('sector', threads=3, trace=True)
        assert result.returncode == 0, result.stderr
        for trace in out.glob('smf-trace-worker-*.jsonl'): validate_trace(trace, data)
        # Sector values must follow their effective dates, not an ID's first sector.
        path = history/'security_master.parquet'; table = pq.read_table(path)
        values = table.to_pylist()
        for value in values:
            if value['InstrumentId']=='S000001' and value['EffectiveDate'] >= dt.date(2023,1,3):
                value['Sector']='Energy'
        rewrite_dates(path, pa.Table.from_pylist(sorted(values, key=lambda r:r['EffectiveDate']), schema=table.schema))
        updated = root/'updated'; snapshots(history, updated)
        data = updated
        for return_type in ('base', 'local'):
            expected = {}
            for day in ('2023-01-01','2023-01-02','2023-01-03','2023-01-04'):
                result, out = run('security', return_type=return_type, day=day, directory=data)
                assert result.returncode == 0, result.stderr
                seen = set()
                for row in pq.read_table(out/'country-portfolio_PF.parquet').to_pylist():
                    sector = 'Energy' if row['Segment']=='S000001' and day>='2023-01-03' else SECTORS[(int(row['Segment'][1:])-1)%11]
                    if row['Segment']=='S000002': sector=SECTORS[0]
                    dest = expected.setdefault(sector, {k:0.0 for k in NUMBERS})
                    for k in NUMBERS:
                        if k not in ('PortfolioReturn','BenchmarkReturn','ActiveReturn') or sector not in seen:
                            dest[k] += row[k]
                    seen.add(sector)
            for threads in (1,3):
                result, out = run('sector', threads, return_type, directory=data, trace=True)
                assert result.returncode == 0, result.stderr
                for trace in out.glob('smf-trace-worker-*.jsonl'): validate_trace(trace, data)
                rows = pq.read_table(out/'country-portfolio_PF.parquet').to_pylist()
                assert [r['Segment'] for r in rows] == sorted(expected)
                for row in rows:
                    assert row['Days']==4
                    for k in NUMBERS: close(row[k], expected[row['Segment']][k])
                result, out = run('total', threads, return_type, directory=data)
                assert result.returncode == 0, result.stderr
                total = pq.read_table(out/'country-portfolio_PF.parquet').to_pylist()[0]
                for k in NUMBERS:
                    if k not in ('PortfolioReturn','BenchmarkReturn','ActiveReturn'):
                        close(math.fsum(r[k] for r in rows), total[k])
    print('Passed: Sector preservation, effective-date changes, independent security rollup, total reconciliation, base/local, trace replay, 1/3-worker parity, missing-sector rejection.')


if __name__ == '__main__': main()
