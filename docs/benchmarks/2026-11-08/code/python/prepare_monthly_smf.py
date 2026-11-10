"""Generate monthly updates for all securities, staggered across calendar days 1-28."""
import argparse,calendar,datetime as dt,json,time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def prepare(source,output,start=None,end=None):
    began=time.perf_counter();source=source.resolve()
    old=json.loads((source/'manifest.json').read_text()) if (source/'manifest.json').exists() else {}
    first=dt.date.fromisoformat(start or old['start']);last=dt.date.fromisoformat(end or old['end'])
    if last<first:raise ValueError('end precedes start')
    file=pq.ParquetFile(source/'security_master.parquet');col=file.schema_arrow.get_field_index('EffectiveDate');current={}
    for g in range(file.num_row_groups):
        s=file.metadata.row_group(g).column(col).statistics
        if s is None or not s.has_min_max or s.min!=s.max:raise ValueError('source requires date-pure SMF row groups')
        if s.min>first:continue
        for b in file.iter_batches(row_groups=[g]):
            for row in b.to_pylist():
                previous=current.get(row['InstrumentId'])
                if previous is None or previous['EffectiveDate']<row['EffectiveDate']:current[row['InstrumentId']]=row
                elif previous['EffectiveDate']==row['EffectiveDate']:raise ValueError('duplicate security/effective date')
    if not current:raise ValueError('no SMF state effective at start')
    rows=sorted(current.values(),key=lambda r:r['InstrumentId']);base=pa.Table.from_pylist(rows,schema=file.schema_arrow)
    numeric=np.array([int(r['InstrumentId'].removeprefix('S')) for r in rows],dtype=np.int64)
    if len(set(numeric))!=len(numeric):raise ValueError('security numeric suffixes must be unique')
    update_day=(numeric-1)%28+1
    countries=np.array([r['Country'] for r in rows],dtype=object);currencies=np.array([r['Currency'] for r in rows],dtype=object)
    output.mkdir(parents=True,exist_ok=False)
    first_month=first.year*12+first.month-1;last_month=last.year*12+last.month-1
    total=updates=0;date_counts=[]
    with pq.ParquetWriter(output/'security_master.parquet.partial',base.schema,compression='zstd') as writer:
        # Seed on staggered dates in the preceding calendar month. No common
        # effective date is introduced just to initialize the calculation window.
        for month in range(first_month-1,last_month+1):
            year,month0=divmod(month,12);version=month-(first_month-1)
            for day in range(1,29):
                effective=dt.date(year,month0+1,day)
                if effective>last:continue
                selected=np.flatnonzero(update_day==day)
                if not len(selected):continue
                table=base.take(pa.array(selected))
                cs=countries[selected];fx=currencies[selected]
                if version%2:
                    cs=np.where(cs=='US','DE','US');fx=np.where(fx=='USD','EUR','USD')
                for name,values in [('EffectiveDate',pa.array([effective]*len(selected),pa.date32())),
                                    ('Country',pa.array(cs,pa.string())),('Currency',pa.array(fx,pa.string()))]:
                    table=table.set_column(table.schema.get_field_index(name),name,values)
                writer.write_table(table,row_group_size=65536)
                total+=len(selected)
                if version:updates+=len(selected)
                date_counts.append(dict(date=str(effective),rows=len(selected),kind='initial' if version==0 else 'monthly_update'))
    (output/'security_master.parquet.partial').rename(output/'security_master.parquet')
    for name in ('accounts','forex','analytics','portfolio_PF','index'):(output/f'{name}.parquet').symlink_to((source/f'{name}.parquet').resolve())
    result={k:v for k,v in old.items() if not k.startswith('smf_') and k not in ('historical_smf','snapshot_counts','preparation_seconds','snapshot_date_semantics')}
    result.update(start=str(first),end=str(last),dates=(last-first).days+1,
        smf_format='sparse per-security effective-date updates',smf_source=str(source),smf_schedule='Every security updates once per calendar month on 1 + ((numeric InstrumentId - 1) % 28).',
        smf_securities=len(rows),smf_rows=total,smf_update_records=updates,smf_effective_dates=len(date_counts),smf_rows_per_date=date_counts,
        smf_initialization='Seed records on each security assigned day in the preceding month. Seed attributes resolve source at calculation start; subsequent history is synthetic.',
        smf_update_rule='Every monthly record changes country and currency by alternating initial/alternative attributes; analytics aliases preserved.',
        smf_day_distribution={str(day):int((update_day==day).sum()) for day in range(1,29)},
        preparation_seconds=time.perf_counter()-began,smf_bytes=(output/'security_master.parquet').stat().st_size,
        baseline_comparison='Compare history and snapshots built from this monthly source; prior daily-heavy outputs are not the expected baseline.')
    (output/'manifest.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('smf_securities','smf_rows','smf_update_records','smf_effective_dates','preparation_seconds','smf_bytes')},indent=2))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,default=Path('results/six-input/inputs'))
    p.add_argument('--output',type=Path,default=Path('results/six-input/monthly-staggered-history'))
    p.add_argument('--start-date');p.add_argument('--end-date');a=p.parse_args()
    prepare(a.source,a.output,a.start_date,a.end_date)
