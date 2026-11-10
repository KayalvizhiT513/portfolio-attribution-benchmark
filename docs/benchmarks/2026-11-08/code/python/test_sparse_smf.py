"""Verify uneven security schedules, sparse updates and both attribution engines."""
import datetime as dt
import json
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from prepare_sparse_smf import prepare
from test_six_inputs import fixture, rewrite_dates
from test_attribution import compare_files
from inspect_smf_trace import validate_trace


def main():
    with tempfile.TemporaryDirectory(prefix='sparse-smf-') as tmp:
        root = Path(tmp)
        source = root / 'source'; source.mkdir()
        days, _ = fixture(source)
        path = source / 'security_master.parquet'
        schema = pq.read_schema(path)
        rows = [dict(InstrumentId=f'S{i:06}', EffectiveDate=dt.date(2022, 12, 31),
                     Currency='USD', Country='US', AnalyticsSecId=i) for i in range(1, 10001)]
        rows.append(dict(rows[0], EffectiveDate=days[0], Country='DE', Currency='EUR'))
        rewrite_dates(path, pa.Table.from_pylist(rows, schema=schema))
        before = path.read_bytes()
        target = root / 'inputs'
        manifest = prepare(source, target, str(days[0]), str(days[-1]))
        assert path.read_bytes() == before
        records = pq.read_table(target / 'security_master.parquet').to_pylist()
        state, seen, updates = {}, set(), {}
        previous = None
        for row in records:
            identity, day = row['InstrumentId'], row['EffectiveDate']
            pair = identity, day
            assert pair not in seen
            seen.add(pair)
            assert previous is None or day >= previous
            previous = day
            if identity in state:
                old = state[identity]
                assert (old['Country'], old['Currency']) != (row['Country'], row['Currency'])
                updates[identity] = updates.get(identity, 0) + 1
            state[identity] = row
        initial = [r for r in records if r['EffectiveDate'] == days[0]]
        assert len(initial) == 10000
        assert next(r for r in initial if r['InstrumentId'] == 'S000001')['Country'] == 'DE'
        assert updates['S000001'] == 3
        assert all(f'S{i:06}' not in updates for i in range(100, 10001, 100))
        assert {k:v['securities'] for k,v in manifest['smf_cohorts'].items()} == {
            'daily':5000, 'weekly':900, '30-day':2000, '90-day':2000, 'unchanged':100}
        assert manifest['smf_rows'] == len(records)
        assert all(v['update_records'] > 0 for k,v in manifest['smf_cohorts'].items() if k != 'unchanged')
        # Identical source/schedules produce identical rows, regardless of output path.
        other = root / 'repeat'
        prepare(source, other, str(days[0]), str(days[-1]))
        assert pq.read_table(target / 'security_master.parquet').equals(pq.read_table(other / 'security_master.parquet'))
        try:
            prepare(source, target, str(days[0]), str(days[-1]))
        except FileExistsError:
            pass
        else:
            raise AssertionError('existing dataset overwritten')
        outputs = []
        for engine, binary in [('rust','rust/target/release/portfolio-attribution'), ('go','results/six-input/benchmark-go')]:
            out = root / engine
            subprocess.run([str(Path(binary).resolve()), '--scenario','six-input','--data',str(target),
                '--output-dir',str(out),'--metrics',str(out/'metrics.csv'),'--threads','2',
                '--start-date',str(days[0]),'--end-date',str(days[-1]),'--smf-trace','records'], check=True, capture_output=True)
            for trace in out.glob('smf-trace-worker-*.jsonl'):
                validate_trace(trace, target)
            outputs.append(out/'country-portfolio_PF.parquet')
        compare_files(*outputs)
    print('Passed: schedules, bootstrap, true attribute changes, static securities, reproducibility, overwrite protection, Rust/Go output equality and cache trace replay.')


if __name__ == '__main__':
    main()
