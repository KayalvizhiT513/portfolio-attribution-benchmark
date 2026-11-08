"""Verify staggered calendar-month updates and matching history/snapshot attribution."""
import collections,datetime as dt,json,subprocess,tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from prepare_monthly_smf import prepare
from prepare_daily_smf import prepare as snapshots
from test_six_inputs import fixture,rewrite_dates
from inspect_smf_trace import validate_trace
from test_attribution import compare_files


def main():
    with tempfile.TemporaryDirectory(prefix='monthly-smf-') as temp:
        root=Path(temp);source=root/'source';source.mkdir();fixture(source)
        path=source/'security_master.parquet';schema=pq.read_schema(path)
        base=[dict(InstrumentId=f'S{i:06}',EffectiveDate=dt.date(2023,1,1),Currency='USD',Country='DE' if i==2 else 'US',AnalyticsSecId=(i-1)%3+1) for i in range(1,57)]
        rewrite_dates(path,pa.Table.from_pylist(base,schema=schema));before=path.read_bytes()
        schedule=root/'calendar';m=prepare(source,schedule,'2024-01-01','2024-03-31')
        history=pq.read_table(schedule/'security_master.parquet').to_pylist();counts=collections.Counter();persecurity=collections.defaultdict(list);previous={}
        for row in history:
            sid=row['InstrumentId'];day=row['EffectiveDate'];counts[(sid,day.year,day.month)]+=1
            assert day.day==1+(int(sid[1:])-1)%28
            if sid in previous:assert (row['Country'],row['Currency'])!=(previous[sid]['Country'],previous[sid]['Currency'])
            previous[sid]=row;persecurity[sid].append(day)
        assert len(history)==224 and len(persecurity)==56 and all(n==1 for n in counts.values())
        assert all(len(v)==4 for v in persecurity.values())
        assert len({d for dates in persecurity.values() for d in dates})==112
        assert persecurity['S000028']==[dt.date(2023,12,28),dt.date(2024,1,28),dt.date(2024,2,28),dt.date(2024,3,28)]
        assert (persecurity['S000028'][3]-persecurity['S000028'][2]).days==29
        assert path.read_bytes()==before
        monthly=root/'monthly';prepare(source,monthly,'2023-01-01','2023-01-04')
        daily=root/'daily';snapshots(monthly,daily)
        binary=str(Path('rust/target/release/portfolio-attribution').resolve());reports=[]
        for name,data in [('history',monthly),('snapshots',daily)]:
            for threads in (1,3):
                out=root/f'{name}-{threads}'
                r=subprocess.run([binary,'--scenario','six-input','--data',str(data),'--output-dir',str(out),'--metrics',str(out/'metrics.csv'),'--threads',str(threads),'--start-date','2023-01-01','--end-date','2023-01-04','--smf-trace','records','--stage-profile','true'],capture_output=True,text=True)
                assert r.returncode==0,r.stderr
                for trace in out.glob('smf-trace-worker-*.jsonl'):validate_trace(trace,data)
                report=out/'country-portfolio_PF.parquet'
                if reports:compare_files(reports[0],report)
                reports.append(report)
    print('Passed: all-security monthly cadence, 28 staggered dates, leap-year calendar intervals, true attribute changes, source preservation, snapshot/history parity at 1 and 3 workers, independent trace replay.')

if __name__=='__main__':main()
