"""Independent sector-level Brinson oracle, security/sector rows and date counts."""
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


def oracle(data):
    master=pq.read_table(data/'security_master.parquet').to_pylist()
    holdings=[pq.read_table(data/f'{name}.parquet').to_pylist() for name in ('portfolio_PF','index')]
    result={}
    for day in sorted({r['SnapshotDate'] for r in master}):
        smf={r['InstrumentId']:r for r in master if r['SnapshotDate']==day}
        sides=[]
        for records in holdings:
            rows=[r for r in records if r['EffectiveDate']==day and smf[r['InstrumentId']]['Country']=='US']
            total=sum(r['Weight'] for r in rows)
            sides.append({r['InstrumentId']:(r['Weight']/total,
                [r['Weight']/total*r[col] for col in ('BaseReturn','LocalReturn')]) for r in rows})
        totals=[[sum(v[1][t] for v in side.values()) for t in range(2)] for side in sides]
        grouped={}
        def add(kind,sector,sid,p,b):
            for t,label in enumerate(('base','local')):
                wp,pc=p[0],p[1][t];wb,bc=b[0],b[1][t]
                rp=pc/wp if wp else 0.;rb=bc/wb if wb else 0.
                # BF empty-side convention: match the other side's constituent return.
                ep=rp if wp else rb;eb=rb if wb else ep
                a=(wp-wb)*(eb-totals[1][t]);s=wb*(ep-eb);i=(wp-wb)*(ep-eb)
                values=[wp,wb,a,s,i,a+s+i,rp,rb,rp-rb,pc,bc,pc-bc]
                dest=result.setdefault((kind,sector,sid,label),dict(Count=0,**{k:0. for k in NUMBERS}))
                dest['Count']+=1
                for k,v in zip(NUMBERS,values):dest[k]+=v
        for sid in sorted(set(sides[0])|set(sides[1])):
            sector=smf[sid]['Sector'];p=sides[0].get(sid,(0.,[0.,0.]));b=sides[1].get(sid,(0.,[0.,0.]))
            add('security',sector,sid,p,b)
            group=grouped.setdefault(sector,[[0.,[0.,0.]],[0.,[0.,0.]]])
            for dest,src in zip(group,(p,b)):
                dest[0]+=src[0]
                for t in range(2):dest[1][t]+=src[1][t]
        for sector,(p,b) in grouped.items():add('sector',sector,'',p,b)
    return result


def main():
    binary=str(Path('rust/target/release/portfolio-attribution').resolve())
    with tempfile.TemporaryDirectory(prefix='sector-smf-') as temp:
        root=Path(temp);source=root/'source';source.mkdir();fixture(source)
        history=root/'sector-history';prepare(source,history)
        path=history/'security_master.parquet';table=pq.read_table(path);values=table.to_pylist()
        for value in values:
            if value['InstrumentId']=='S000002':value['Sector']=SECTORS[0]
            if value['InstrumentId']=='S000003':value['Country']='US';value['Sector']='Utilities'
            if value['InstrumentId']=='S000001' and value['EffectiveDate']>=dt.date(2023,1,3):value['Sector']='Energy'
        # Return to the first sector on day four: the same key must resume counting.
        value=next(dict(r) for r in values if r['InstrumentId']=='S000001')
        value.update(EffectiveDate=dt.date(2023,1,4),Sector=SECTORS[0]);values.append(value)
        rewrite_dates(path,pa.Table.from_pylist(values,schema=table.schema))
        # Utilities is a benchmark-only sector; retain normalized source weights.
        pf=source/'portfolio_PF.parquet';pt=pq.read_table(pf);pr=[r for r in pt.to_pylist() if r['InstrumentId']!='S000003']
        for row in pr:row['Weight']/=.8
        rewrite_dates(pf,pa.Table.from_pylist(pr,schema=pt.schema))
        data=root/'snapshots';snapshots(history,data);expected=oracle(data)
        baseline=None
        for workers,return_type in [(1,'both'),(3,'both'),(3,'base'),(3,'local')]:
            out=root/f'out-{workers}-{return_type}'
            command=[binary,'--scenario','six-input','--data',str(data),'--output-dir',str(out),
                '--metrics',str(out/'metrics.csv'),'--aggregate','sector','--threads',str(workers),
                '--return-type',return_type,'--smf-trace','records']
            result=subprocess.run(command,capture_output=True,text=True);assert result.returncode==0,result.stderr
            for trace in out.glob('smf-trace-worker-*.jsonl'):validate_trace(trace,data)
            table=pq.read_table(out/'country-portfolio_PF.parquet');assert table.schema.field('Count').type==pa.uint32()
            rows=table.to_pylist();actual={}
            for row in rows:
                key=(row['RowType'],row['Sector'],row['Security'],row['ReturnType'])
                assert key not in actual and row['Days']==4 and row['Scenario']=='sector_attribution'
                assert row['Segment']==(row['Sector'] if row['RowType']=='sector' else row['Security'])
                assert row['Count']==expected[key]['Count']
                for col in NUMBERS:close(row[col],expected[key][col])
                actual[key]=row
            selected={k for k in expected if return_type=='both' or k[3]==return_type};assert set(actual)==selected
            tech=SECTORS[0]
            if return_type in ('base','both'):
                assert actual[('security',tech,'S000001','base')]['Count']==3
                assert actual[('security','Energy','S000001','base')]['Count']==1
                assert actual[('sector',tech,'','base')]['Count']==3
                assert actual[('security',tech,'S000002','base')]['Count']==2
                # Sector effects must be computed on grouped holdings, not summed security effects.
                parent=actual[('sector',tech,'','base')]
                children=[r for k,r in actual.items() if k[0]=='security' and k[1]==tech and k[3]=='base']
                assert any(not math.isclose(parent[col],sum(r[col] for r in children),abs_tol=1e-10)
                           for col in ('Allocation','Selection','Interaction'))
            for label in ('base','local'):
                if return_type!='both' and return_type!=label:continue
                parents=[r for k,r in actual.items() if k[0]=='sector' and k[3]==label]
                children=[r for k,r in actual.items() if k[0]=='security' and k[3]==label]
                for col in ('PortfolioWeight','BenchmarkWeight','Total','PortfolioContribution','BenchmarkContribution','ActiveContribution'):
                    close(sum(r[col] for r in parents),sum(r[col] for r in children))
        raw=root/'raw';snapshots(source,raw)
        result=subprocess.run([binary,'--scenario','six-input','--data',str(raw),'--output-dir',str(root/'missing'),
            '--metrics',str(root/'missing/metrics.csv'),'--aggregate','sector'],capture_output=True,text=True)
        assert result.returncode and 'requires Sector' in result.stderr,result.stderr
        # Snapshot order and universe can change. A held ID disappears, a new
        # ID takes its place for two dates, then the old ID returns to its pair.
        for stem in ('security_master','portfolio_PF','index'):
            path=data/f'{stem}.parquet';table=pq.read_table(path);values=table.to_pylist()
            date_col='SnapshotDate' if stem=='security_master' else 'EffectiveDate'
            for row in values:
                if row['InstrumentId']=='S000001' and row[date_col].day in (2,3):
                    row['InstrumentId']='NEW_SECURITY'
            if stem=='security_master':
                extra=next(dict(r) for r in values if r['SnapshotDate'].day==2)
                extra['InstrumentId']='UNHELD_SECURITY';values.append(extra)
            if path.is_symlink():path.unlink()
            with pq.ParquetWriter(path,table.schema) as writer:
                for day in sorted({r[date_col] for r in values}):
                    selected=[r for r in values if r[date_col]==day]
                    if day.day%2:selected.reverse()
                    writer.write_table(pa.Table.from_pylist(selected,schema=table.schema),row_group_size=2)
        reordered_expected=oracle(data)
        for workers in (1,3):
            out=root/f'reordered-{workers}'
            result=subprocess.run([binary,'--scenario','six-input','--data',str(data),
                '--output-dir',str(out),'--metrics',str(out/'metrics.csv'),'--aggregate','sector',
                '--threads',str(workers),'--return-type','both'],capture_output=True,text=True)
            assert result.returncode==0,result.stderr
            actual={}
            for row in pq.read_table(out/'country-portfolio_PF.parquet').to_pylist():
                key=(row['RowType'],row['Sector'],row['Security'],row['ReturnType'])
                assert key not in actual and row['Count']==reordered_expected[key]['Count']
                for col in NUMBERS:close(row[col],reordered_expected[key][col])
                actual[key]=row
            assert set(actual)==set(reordered_expected)
            assert actual[('security',SECTORS[0],'S000001','base')]['Count']==2
            assert actual[('security',SECTORS[0],'NEW_SECURITY','base')]['Count']==1
            assert actual[('security','Energy','NEW_SECURITY','base')]['Count']==1
    print('Passed: independent sector BF versus security effects; combined schema; distinct-date counts; sector changes and return; benchmark-only sector; base/local; 1/3-worker parity; reordered/resized snapshots, new/absent/returning securities; missing-sector rejection.')


if __name__=='__main__':main()
