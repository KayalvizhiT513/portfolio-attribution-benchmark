"""Resolve sparse date-ordered SMF changes into complete snapshots per return date."""
import argparse,datetime as dt,json,time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def date_groups(file,column):
    index=file.schema_arrow.get_field_index(column);groups={};previous=None
    for i in range(file.num_row_groups):
        stats=file.metadata.row_group(i).column(index).statistics
        if stats is None or not stats.has_min_max or stats.min!=stats.max:raise ValueError('source requires single-date row groups')
        if previous is not None and stats.min<previous:raise ValueError('source date groups must be ordered')
        previous=stats.min;groups.setdefault(stats.min,[]).append(i)
    return groups


def prepare(source,output):
    start=time.perf_counter();source=source.resolve()
    old=json.loads((source/'manifest.json').read_text()) if (source/'manifest.json').exists() else {}
    pf=set(date_groups(pq.ParquetFile(source/'portfolio_PF.parquet'),'EffectiveDate'))
    ix=set(date_groups(pq.ParquetFile(source/'index.parquet'),'EffectiveDate'))
    days=sorted(pf & ix)
    if 'start' in old:days=[d for d in days if dt.date.fromisoformat(old['start'])<=d<=dt.date.fromisoformat(old['end'])]
    if not days:raise ValueError('no shared return dates')
    for name,col in [('accounts','EffectiveDate'),('forex','EffectiveDate'),('analytics','Effective Date')]:
        if not set(days)<=set(date_groups(pq.ParquetFile(source/f'{name}.parquet'),col)):raise ValueError(f'{name} lacks return dates')
    path=source/'security_master.parquet';stat=path.stat();file=pq.ParquetFile(path)
    has_sector='Sector' in file.schema_arrow.names
    sectors=np.empty(0,dtype=object)
    changes=date_groups(file,'EffectiveDate');effective=iter(sorted(changes));pending=next(effective,None)
    if pending is None or pending>days[0]:raise ValueError('no SMF history effective on first return date')
    schema=file.schema_arrow.append(pa.field('SnapshotDate',pa.date32()))
    output.mkdir(parents=True,exist_ok=False)
    ids=[];id_array=pa.array([],pa.string());countries=np.empty(0,dtype=object);currencies=np.empty(0,dtype=object)
    dates=np.empty(0,dtype=np.int32);aliases=np.empty(0,dtype=np.int64);order=np.empty(0,dtype=np.int64)
    totals=0;counts=[]
    with pq.ParquetWriter(output/'security_master.parquet.partial',schema,compression='zstd') as writer:
        for day in days:
            while pending is not None and pending<=day:
                t=file.read_row_groups(changes[pending]).combine_chunks()
                if any(c.null_count for c in t.columns):raise ValueError('null SMF attribute')
                if len(pc.unique(t['InstrumentId']))!=len(t):raise ValueError('duplicate security/effective date')
                if any(pc.any(pc.equal(t[name],'' )).as_py() for name in ('InstrumentId','Country','Currency')):raise ValueError('empty SMF attribute')
                if pc.any(pc.less_equal(t['AnalyticsSecId'],0)).as_py():raise ValueError('invalid analytics alias')
                locations=pc.index_in(t['InstrumentId'],value_set=id_array)
                if locations.null_count:
                    new=pc.filter(t['InstrumentId'],pc.is_null(locations)).to_pylist()
                    ids.extend(new);id_array=pa.array(ids,pa.string())
                    countries=np.resize(countries,len(ids));currencies=np.resize(currencies,len(ids))
                    if has_sector:sectors=np.resize(sectors,len(ids))
                    dates=np.resize(dates,len(ids));aliases=np.resize(aliases,len(ids))
                    order=np.array(sorted(range(len(ids)),key=lambda i:ids[i]),dtype=np.int64)
                    locations=pc.index_in(t['InstrumentId'],value_set=id_array)
                slots=locations.to_numpy()
                if has_sector:
                    if pc.any(pc.equal(t['Sector'],'')).as_py():raise ValueError('empty SMF sector')
                    sectors[slots]=t['Sector'].to_numpy()
                countries[slots]=t['Country'].to_numpy();currencies[slots]=t['Currency'].to_numpy()
                dates[slots]=pc.cast(t['EffectiveDate'],pa.int32()).to_numpy();aliases[slots]=t['AnalyticsSecId'].to_numpy()
                pending=next(effective,None)
            n=len(ids)
            values={'InstrumentId':pc.take(id_array,pa.array(order)),
                'EffectiveDate':pa.array(dates[order],pa.date32()),'Country':pa.array(countries[order],pa.string()),
                'Currency':pa.array(currencies[order],pa.string()),'AnalyticsSecId':pa.array(aliases[order],pa.int64()),
                'SnapshotDate':pa.array(np.full(n,(day-dt.date(1970,1,1)).days,dtype=np.int32),pa.date32())}
            if has_sector:values['Sector']=pa.array(sectors[order],pa.string())
            writer.write_table(pa.Table.from_arrays([values[f.name] for f in schema],schema=schema),row_group_size=65536)
            totals+=n;counts.append(dict(date=str(day),securities=n))
            if len(counts)%100==0:print(f'{len(counts)}/{len(days)} snapshots; {totals:,} rows',flush=True)
    assert (path.stat().st_size,path.stat().st_mtime_ns)==(stat.st_size,stat.st_mtime_ns)
    (output/'security_master.parquet.partial').rename(output/'security_master.parquet')
    for name in ('accounts','forex','analytics','portfolio_PF','index'):(output/f'{name}.parquet').symlink_to((source/f'{name}.parquet').resolve())
    result={**old,'smf_format':'daily_complete_snapshots','smf_snapshot_source':str(source),
        'start':str(days[0]),'end':str(days[-1]),'dates':len(days),'smf_rows':totals,
        'snapshot_counts':counts,'snapshot_date_semantics':'SnapshotDate is the return date; EffectiveDate preserves each security last-change date.',
        'preparation_seconds':time.perf_counter()-start,'smf_snapshot_bytes':(output/'security_master.parquet').stat().st_size}
    (output/'manifest.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('smf_format','dates','smf_rows','preparation_seconds','smf_snapshot_bytes')},indent=2))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,default=Path('results/six-input/security-history-daily-heavy-inputs'))
    p.add_argument('--output',type=Path,default=Path('results/six-input/daily-snapshot-inputs'))
    a=p.parse_args();prepare(a.source,a.output)
