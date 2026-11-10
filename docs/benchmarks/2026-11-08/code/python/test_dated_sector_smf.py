"""Verify annual synthetic sector revisions and latest-effective snapshot sectors."""
import datetime as dt
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from prepare_monthly_smf import prepare as monthly
from prepare_sector_smf import prepare, SECTORS
from prepare_daily_smf import prepare as snapshots
from test_six_inputs import fixture, rewrite_dates
from inspect_smf_trace import validate_trace


def main():
    with tempfile.TemporaryDirectory(prefix='dated-sector-') as temp:
        root=Path(temp);source=root/'source';source.mkdir();fixture(source)
        history=root/'monthly';monthly(source,history,'2023-01-01','2024-12-31')
        enriched=root/'dated';prepare(history,enriched,effective_dated=True)
        records=pq.read_table(enriched/'security_master.parquet').to_pylist()
        latest={};changes=[]
        for row in records:
            number=int(row['InstrumentId'][1:]);date=row['EffectiveDate']
            revision=(date.year*12+date.month-1-2023*12-(number-1)%12)//12+1
            assert row['Sector']==SECTORS[(number-1+revision)%11]
            old=latest.get(row['InstrumentId'])
            if old is not None and old['Sector']!=row['Sector']:
                assert date.month==1+(number-1)%12
                assert date.day==1+(number-1)%28
                changes.append((row['InstrumentId'],date))
            latest[row['InstrumentId']]=row
        assert len(changes)==6 and len(latest)==3
        assert ('S000002',dt.date(2023,2,2)) in changes
        assert ('S000002',dt.date(2024,2,2)) in changes
        # Exercise the day before, the day of and the day after sector changes.
        days=[dt.date(2023,1,1)+dt.timedelta(days=i)
              for i in range((dt.date(2024,2,3)-dt.date(2023,1,1)).days+1)]
        for name in ('accounts','forex','analytics','portfolio_PF','index'):
            path=source/f'{name}.parquet';table=pq.read_table(path)
            col='Effective Date' if name=='analytics' else 'EffectiveDate'
            original=table.to_pylist();first=min(r[col] for r in original)
            initial=[r for r in original if r[col]==first]
            values=[dict(r,**{col:day}) for day in days for r in initial]
            rewrite_dates(path,pa.Table.from_pylist(values,schema=table.schema))
        data=root/'snapshots';snapshots(enriched,data)
        actual=pq.read_table(data/'security_master.parquet').to_pylist()
        for day in days:
            expected={}
            for row in records:
                if row['EffectiveDate']<=day:expected[row['InstrumentId']]=row
            observed={r['InstrumentId']:r for r in actual if r['SnapshotDate']==day}
            assert observed.keys()==expected.keys()
            for sid,row in observed.items():
                assert row['Sector']==expected[sid]['Sector']
                assert row['EffectiveDate']==expected[sid]['EffectiveDate']
        for workers in (1,3):
            out=root/f'out-{workers}'
            result=subprocess.run([str(Path('rust/target/release/portfolio-attribution').resolve()),
                '--scenario','six-input','--data',str(data),'--output-dir',str(out),
                '--metrics',str(out/'metrics.csv'),'--threads',str(workers),
                '--aggregate','sector','--smf-trace','records'],capture_output=True,text=True)
            assert result.returncode==0,result.stderr
            for trace in out.glob('smf-trace-worker-*.jsonl'):validate_trace(trace,data)
        assert pq.read_table(root/'out-1/country-portfolio_PF.parquet').num_rows>0
    print('Passed: annual staggered sector revisions, exact effective boundaries, no future sector leakage, preserved effective dates, sector CLI and trace oracle at 1/3 workers.')


if __name__=='__main__':main()
