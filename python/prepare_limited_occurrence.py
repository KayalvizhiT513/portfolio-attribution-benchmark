"""Prepare sparse holdings presence for exact distinct-date count checks.

Operate on effective-dated history before resolving snapshots. Selected securities
remain US on every SMF record, retain changing sectors, and occur on exactly N
portfolio/benchmark union dates. Unmodified inputs are never overwritten.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from prepare_daily_smf import date_groups

DEFAULT_IDS=['S000001','S000013','S100001','S100013']


def prepare(source,output,count=600,securities=None):
    started=time.perf_counter();source=source.resolve();securities=list(securities or DEFAULT_IDS)
    if len(set(securities))!=len(securities) or not securities:raise ValueError('unique nonempty security IDs required')
    master=pq.ParquetFile(source/'security_master.parquet')
    if 'SnapshotDate' in master.schema_arrow.names:raise ValueError('prepare from history before building snapshots')
    if 'Sector' not in master.schema_arrow.names:raise ValueError('requires sector history')
    manifest=json.loads((source/'manifest.json').read_text())
    files=[pq.ParquetFile(source/f'{name}.parquet') for name in ('portfolio_PF','index')]
    groups=[date_groups(f,'EffectiveDate') for f in files]
    days=sorted(set(groups[0])&set(groups[1]))
    if 'start' in manifest:days=[d for d in days if dt.date.fromisoformat(manifest['start'])<=d<=dt.date.fromisoformat(manifest['end'])]
    if not 0<count<len(days):raise ValueError('count must be positive and smaller than calculation window')
    # Different calendars test truncation, late entry and gaps across worker ranges.
    n=len(days);patterns=[list(range(count)),list(range(n-count,n)),
        np.linspace(0,n-1,count,dtype=int).tolist(),
        sorted(range(n),key=lambda i:((i*7919)%n,i))[:count]]
    schedules={sid:set(days[i] for i in patterns[k%len(patterns)]) for k,sid in enumerate(securities)}
    assert all(len(v)==count for v in schedules.values())
    output.mkdir(parents=True,exist_ok=False)
    lookup=pa.array(securities,pa.string());seen=set();history_rows=0;changes={sid:[] for sid in securities};previous={}
    with pq.ParquetWriter(output/'security_master.parquet',master.schema_arrow,compression='zstd') as writer:
        for g in range(master.num_row_groups):
            table=master.read_row_group(g);mask=pc.is_in(table['InstrumentId'],value_set=lookup)
            for row in table.filter(mask).to_pylist():
                sid=row['InstrumentId'];seen.add(sid)
                if previous.get(sid)!=row['Sector']:
                    changes[sid].append({'effective_date':str(row['EffectiveDate']),'sector':row['Sector']})
                    previous[sid]=row['Sector']
            table=table.set_column(table.schema.get_field_index('Country'),'Country',pc.if_else(mask,pa.scalar('US'),table['Country']))
            writer.write_table(table,row_group_size=len(table));history_rows+=len(table)
    if seen!=set(securities):raise ValueError(f'selected IDs missing from SMF: {set(securities)-seen}')
    presence={sid:set() for sid in securities};side_counts={};rows_written={}
    for stem,file,by_date in zip(('portfolio_PF','index'),files,groups):
        found={sid:0 for sid in securities};written=0
        with pq.ParquetWriter(output/f'{stem}.parquet',file.schema_arrow,compression='zstd') as writer:
            for index,day in enumerate(days):
                table=file.read_row_groups(by_date[day])
                excluded=pa.array([sid for sid in securities if day not in schedules[sid]],pa.string())
                if len(excluded):table=table.filter(pc.invert(pc.is_in(table['InstrumentId'],value_set=excluded)))
                total=pc.sum(table['Weight']).as_py()
                if not total or total<=0:raise ValueError('no remaining positive holdings weight')
                table=table.set_column(table.schema.get_field_index('Weight'),'Weight',pc.divide(table['Weight'],total))
                if not np.isclose(pc.sum(table['Weight']).as_py(),1,rtol=0,atol=1e-12):raise ValueError('weight normalization failed')
                selected=table.filter(pc.is_in(table['InstrumentId'],value_set=lookup))
                for sid in selected['InstrumentId'].to_pylist():found[sid]+=1;presence[sid].add(day)
                writer.write_table(table,row_group_size=65536);written+=len(table)
                if (index+1)%250==0:print(f'{stem}: {index+1}/{len(days)} dates',flush=True)
        side_counts[stem]=found;rows_written[stem]=written
    expected={}
    for sid in securities:
        if presence[sid]!=schedules[sid]:raise ValueError(f'{sid}: source holdings do not cover the requested {count} dates')
        by_sector={}
        for day in sorted(presence[sid]):
            applicable=[v for v in changes[sid] if v['effective_date']<=str(day)]
            if not applicable:raise ValueError('missing effective sector')
            sector=applicable[-1]['sector'];by_sector[sector]=by_sector.get(sector,0)+1
        expected[sid]={'Count':len(presence[sid]),'sector_counts':by_sector,
            'presence_dates':[str(d) for d in sorted(presence[sid])],
            'pattern':['first dates','last dates','evenly spaced dates','deterministic shuffled dates'][securities.index(sid)%4],
            'portfolio_occurrences':side_counts['portfolio_PF'][sid],'benchmark_occurrences':side_counts['index'][sid]}
    for stem in ('accounts','forex','analytics'):(output/f'{stem}.parquet').symlink_to((source/f'{stem}.parquet').resolve())
    spec={'source':str(source),'dates':len(days),'target_count':count,'securities':expected,
          'country_override':'Selected securities remain US on all SMF records; sectors and other attributes are preserved.',
          'absence_semantics':'Removed from BOTH holdings sides on excluded dates; snapshot presence alone does not count.',
          'weights':'Remaining source holdings re-normalized to one independently per side/date.',
          'rows_written':rows_written,'preparation_seconds':time.perf_counter()-started}
    (output/'expected-counts.json').write_text(json.dumps(spec,indent=2)+'\n')
    manifest.update(start=str(days[0]),end=str(days[-1]),dates=len(days),limited_occurrence=spec,smf_rows=history_rows)
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps({sid:{k:v for k,v in value.items() if k!='presence_dates'} for sid,value in expected.items()},indent=2))
    return spec


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--count',type=int,default=600)
    p.add_argument('--securities',nargs='+',default=DEFAULT_IDS);args=p.parse_args()
    prepare(args.source,args.output,args.count,args.securities)
