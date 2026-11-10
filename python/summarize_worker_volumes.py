"""Reconcile per-worker input row volumes against API metrics and batch totals."""
import argparse
import collections
import csv
import datetime as dt
import json
from pathlib import Path
import pyarrow.parquet as pq
from plot_worker_race import collect

NAMES=['accounts','forex','security_master','analytics','portfolio_PF','index']


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path,required=True);args=p.parse_args()
    experiment=json.loads((args.run/'summary.json').read_text());data=Path(experiment['data']);slots,_=collect(args.run)
    responses=[json.loads(l) for l in (args.run/'responses.jsonl').read_text().splitlines()]
    source={}
    for name in NAMES:
        file=pq.ParquetFile(data/f'{name}.parquet')
        date_name='SnapshotDate' if name=='security_master' and 'SnapshotDate' in file.schema_arrow.names else ('Effective Date' if name=='analytics' else 'EffectiveDate')
        col=file.schema_arrow.get_field_index(date_name);counts=collections.Counter()
        for g in range(file.num_row_groups):
            group=file.metadata.row_group(g);stat=group.column(col).statistics
            assert stat is not None and stat.min==stat.max,'requires date-pure row groups'
            counts[str(stat.min)]+=group.num_rows
        source[name]=counts
    rows=[];requests=[]
    for response in sorted(responses,key=lambda r:r['request_index']):
        api=response['request_index']+1;body=response['response']['body'];out=Path(body['output_dir']);metrics=body['metrics'];workers=[]
        for slot in sorted([s for s in slots if s['api']==api],key=lambda s:s['worker']):
            profile=out/f'stage-profile-worker-{slot["worker"]:02}.jsonl'
            events=[json.loads(l) for l in profile.read_text().splitlines()]
            dates=[e for e in events if e['event']=='date_timing']
            if dates and all('input_rows_decoded' in e for e in dates):
                counts=[sum(e['input_rows_decoded'][i] for e in dates) for i in range(6)];basis='recorded worker counters'
            else:
                # This snapshot reader decodes each complete date exactly once.
                assert json.loads((out/'stage-profile.json').read_text())['smf_reading_path']=='daily_snapshot'
                assert int(metrics['smf_cache_hits'])==0
                counts=[sum(n for day,n in source[name].items() if slot['first_date']<=day<=slot['last_date']) for name in NAMES];basis='exact Parquet row-group metadata for assigned dates; reconciled to request counters'
            days=(dt.date.fromisoformat(slot['last_date'])-dt.date.fromisoformat(slot['first_date'])).days+1
            assert len(dates)==days
            row={'api':api,'worker':slot['worker'],'slot':slot['slot'],'first_date':slot['first_date'],'last_date':slot['last_date'],
                 'dates':days,**{f'{name}_rows':counts[i] for i,name in enumerate(NAMES)},'total_input_rows':sum(counts),
                 'joined_us_security_dates':sum(e['joined_us_securities'] for e in dates) if all('joined_us_securities' in e for e in dates) else None,
                 'volume_basis':basis,'slot_held_seconds':slot['held_s']}
            workers.append(row);rows.append(row)
        for name in NAMES:assert sum(r[f'{name}_rows'] for r in workers)==int(metrics[f'{name}_rows_decoded']),(api,name)
        assert sum(r['dates'] for r in workers)==int(metrics['days'])
        requests.append({'api':api,'workers':len(workers),'dates':sum(r['dates'] for r in workers),
            **{f'{name}_rows':sum(r[f'{name}_rows'] for r in workers) for name in NAMES},
            'total_input_rows':sum(r['total_input_rows'] for r in workers),'joined_us_security_dates':int(metrics['joined_us_securities']),
            'final_output_rows':int(metrics['output_rows'])})
    total={key:sum(r[key] for r in requests) for key in ['dates',*[f'{name}_rows' for name in NAMES],'total_input_rows','joined_us_security_dates','final_output_rows']}
    for filename,records in [('worker-volumes.csv',rows),('request-volumes.csv',requests)]:
        with (args.run/filename).open('w',newline='') as stream:
            w=csv.DictWriter(stream,fieldnames=list(records[0]));w.writeheader();w.writerows(records)
    result={'workers':rows,'requests':requests,'batch_totals':total,'all_input_volumes_reconcile_to_recorded_request_metrics':True,
        'note':'Rows summed across six distinct input tables and repeated requests; not unique securities, disk I/O bytes or output rows. Joined security-dates is the daily retained union. Per-worker joined counts unavailable in older profiles; no estimate is substituted.'}
    (args.run/'worker-volumes.json').write_text(json.dumps(result,indent=2)+'\n')
    lines=['# Worker input volumes','',result['note'],'','Each request has the same date partitions and row volumes. The table below shows API 1; worker-volumes.csv contains all APIs and their slot assignments.','',
        '| Worker | Dates | SMF rows | Analytics rows | Portfolio rows | Benchmark rows | All six input rows |','|---|---:|---:|---:|---:|---:|---:|']
    for r in [r for r in rows if r['api']==1]:
        lines.append(f"| {r['worker']} | {r['dates']} | {r['security_master_rows']:,} | {r['analytics_rows']:,} | {r['portfolio_PF_rows']:,} | {r['index_rows']:,} | {r['total_input_rows']:,} |")
    lines+=['',f"Per API: {requests[0]['total_input_rows']:,} input rows. Full batch: {total['total_input_rows']:,} input rows; {total['joined_us_security_dates']:,} joined security-dates; {total['final_output_rows']:,} final output rows.",'', '[All 240 workers](worker-volumes.csv) · [Per-request volumes](request-volumes.csv) · [Interactive Gantt](worker-race.html)']
    (args.run/'VOLUMES.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps({'workers':len(rows),'per_request':requests[0],'batch':total,'reconciled':True},indent=2))


if __name__=='__main__':main()
