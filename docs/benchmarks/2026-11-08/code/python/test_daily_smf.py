"""Validate direct daily snapshots against sparse histories and worker trace oracle."""
import datetime as dt,json,subprocess,tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from prepare_daily_smf import prepare
from test_six_inputs import fixture,rewrite_dates
from inspect_smf_trace import validate_trace
from test_attribution import compare_files


def main():
    with tempfile.TemporaryDirectory(prefix='daily-smf-') as temp:
        root=Path(temp);source=root/'source';source.mkdir();days,_=fixture(source)
        path=source/'security_master.parquet';table=pq.read_table(path)
        rows=[r for r in table.to_pylist() if r['EffectiveDate']==days[0] or r['InstrumentId']=='S000002']
        for r in rows:
            if r['InstrumentId']=='S000003':r['EffectiveDate']=dt.date(2022,12,31)
        rows.extend([dict(InstrumentId='S000004',EffectiveDate=days[1],Country='FR',Currency='EUR',AnalyticsSecId=4),
                     dict(InstrumentId='S000001',EffectiveDate=days[1],Country='US',Currency='EUR',AnalyticsSecId=4),
                     dict(InstrumentId='S000002',EffectiveDate=dt.date(2023,1,5),Country='ZZ',Currency='USD',AnalyticsSecId=2)])
        rewrite_dates(path,pa.Table.from_pylist(rows,schema=table.schema))
        target=root/'snapshots';manifest=prepare(source,target)
        assert manifest['smf_rows']==15
        snapshots=pq.read_table(target/'security_master.parquet').to_pylist()
        history=sorted(rows,key=lambda r:r['EffectiveDate'])
        for day in days:
            expected={}
            for row in history:
                if row['EffectiveDate']<=day:expected[row['InstrumentId']]=row
            actual={r['InstrumentId']:{k:v for k,v in r.items() if k!='SnapshotDate'} for r in snapshots if r['SnapshotDate']==day}
            assert actual==expected
            assert all(r['EffectiveDate']<=r['SnapshotDate'] for r in snapshots)
        binary=str(Path('rust/target/release/portfolio-attribution').resolve())
        def run(data,tag,threads=3,start='2023-01-01',end='2023-01-04',failure=None):
            out=root/tag
            r=subprocess.run([binary,'--scenario','six-input','--data',str(data),'--output-dir',str(out),'--metrics',str(out/'metrics.csv'),'--threads',str(threads),'--start-date',start,'--end-date',end,'--smf-trace','records','--stage-profile','true'],capture_output=True,text=True)
            if failure:
                assert r.returncode and failure in r.stderr,r.stderr
                return out
            assert r.returncode==0,r.stderr
            for trace in out.glob('smf-trace-worker-*.jsonl'):validate_trace(trace,data)
            return out
        baseline=run(source,'sparse')
        for n in (1,3):
            out=run(target,f'snapshot-{n}',threads=n)
            compare_files(baseline/'country-portfolio_PF.parquet',out/'country-portfolio_PF.parquet')
            profile=json.loads((out/'stage-profile.json').read_text());assert profile['smf_reading_path']=='daily_snapshot'
        oldlate=run(source,'sparse-late',start='2023-01-03')
        newlate=run(target,'snapshot-late',start='2023-01-03')
        compare_files(oldlate/'country-portfolio_PF.parquet',newlate/'country-portfolio_PF.parquet')
        snap=target/'security_master.parquet';original=snap.read_bytes();schema=pq.read_schema(snap)
        def write(changes):
            with pq.ParquetWriter(snap,schema) as w:
                for d in days:
                    values=[r for r in changes if r['SnapshotDate']==d]
                    if values:w.write_table(pa.Table.from_pylist(values,schema=schema),row_group_size=2)
        try:
            write([r for r in snapshots if r['SnapshotDate']!=days[1]])
            run(target,'missing-date',failure='missing date')
            values=[dict(r) for r in snapshots];values[0]['EffectiveDate']=dt.date(2030,1,1);write(values)
            run(target,'future-date',failure='future security effective date')
            write(snapshots+[dict(snapshots[0])])
            run(target,'duplicate',failure='duplicate')
        finally:snap.write_bytes(original)
    print('Passed: full snapshots, real effective dates retained, new securities, future exclusion, serial/parallel/window parity, no-history-replay traces, missing-date/future/duplicate rejection.')

if __name__=='__main__':main()
