"""Verify absent holdings do not count, renormalization, and sector/security counts."""
import json
from pathlib import Path
import subprocess
import tempfile

import pyarrow.parquet as pq
from test_six_inputs import fixture
from prepare_sector_smf import prepare as sectors
from prepare_limited_occurrence import prepare
from prepare_daily_smf import prepare as snapshots
from validate_occurrence_counts import validate


def main():
    with tempfile.TemporaryDirectory(prefix='limited-occurrence-') as tmp:
        root=Path(tmp);source=root/'source';source.mkdir();fixture(source)
        history=root/'history';sectors(source,history)
        limited=root/'limited';spec=prepare(history,limited,count=2,securities=['S000001','S000002','S000003'])
        data=root/'snapshots';snapshots(limited,data)
        reports=[]
        for workers in (1,3):
            out=root/f'out-{workers}'
            r=subprocess.run([str(Path('rust/target/release/portfolio-attribution').resolve()),
                '--scenario','six-input','--data',str(data),'--output-dir',str(out),
                '--metrics',str(out/'metrics.csv'),'--aggregate','sector','--threads',str(workers)],capture_output=True,text=True)
            assert r.returncode==0,r.stderr
            validate(data,out/'country-portfolio_PF.parquet')
            rows=pq.read_table(out/'country-portfolio_PF.parquet').to_pylist();reports.append(rows)
            for sid,expected in spec['securities'].items():
                selected=[r for r in rows if r['RowType']=='security' and r['Security']==sid]
                assert sum(r['Count'] for r in selected)==2
                assert {r['Sector']:r['Count'] for r in selected}==expected['sector_counts']
            assert all(r['Days']==4 for r in rows)
            assert {r['Security'] for r in rows if r['RowType']=='security'}==set(spec['securities'])
        assert [(r['RowType'],r['Sector'],r['Security'],r['Count']) for r in reports[0]]==[(r['RowType'],r['Sector'],r['Security'],r['Count']) for r in reports[1]]
        for stem in ('portfolio_PF','index'):
            values=pq.read_table(limited/f'{stem}.parquet').to_pylist()
            for day in sorted({r['EffectiveDate'] for r in values}):
                assert abs(sum(r['Weight'] for r in values if r['EffectiveDate']==day)-1)<1e-12
    print('Passed: exact sparse presence, US eligibility, no phantom dates from snapshots, normalized holdings, sector Count reconciliation, 1/3-worker parity.')


if __name__=='__main__':main()
