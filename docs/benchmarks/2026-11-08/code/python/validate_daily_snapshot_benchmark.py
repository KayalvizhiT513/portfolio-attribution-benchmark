"""Check snapshot API outputs, exact date reads, worker traces and cache replacement."""
import argparse,collections,csv,datetime as dt,json,re
from pathlib import Path
import pyarrow.parquet as pq
from plot_worker_race import collect
from test_attribution import compare_files


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path,required=True);p.add_argument('--baseline',type=Path,required=True);a=p.parse_args()
    exp=json.loads((a.run/'summary.json').read_text());data=Path(exp['data']);file=pq.ParquetFile(data/'security_master.parquet')
    snapshot=file.schema_arrow.get_field_index('SnapshotDate');counts=collections.Counter()
    for i in range(file.num_row_groups):
        group=file.metadata.row_group(i);stats=group.column(snapshot).statistics;assert stats.min==stats.max
        counts[str(stats.min)]+=group.num_rows
    slots,slot_stats=collect(a.run);worker_rows=[];decoded=events_count=0;times=[]
    for batch in exp['batches']:
        for response in batch['responses']:
            assert response['status']==200,response
            body=response['body'];compare_files(a.baseline,Path(body['report']));times.append(body['elapsed_seconds'])
            paths=sorted(Path(body['output_dir']).glob('smf-trace-worker-*.jsonl'));assert len(paths)==12
            request_rows=0;request_decoded=0
            for path in paths:
                events=[json.loads(line) for line in path.read_text().splitlines()];header=events[0];assert header['initialization_mode']=='daily_snapshot'
                previous=None;state=0;loaded=False;reset=False;total=0;complete=False;ready=[]
                first=dt.date.fromisoformat(header['first_date']);last=dt.date.fromisoformat(header['last_date'])
                for sequence,e in enumerate(events):
                    events_count+=1
                    assert e['sequence']==sequence and e['worker_id']==header['worker_id'] and e['request_id']==body['request_id'] and not complete
                    kind=e['event']
                    if kind=='worker_start':assert sequence==0
                    elif kind=='snapshot_reset':
                        assert not reset and not loaded and e['previous_state_securities']==state
                        reset=True;state=0
                    elif kind=='snapshot_loaded':
                        assert reset and not loaded
                        day=e['calculation_date'];assert e['snapshot_date']==day and e['effective_date'] is None
                        assert e['rows']==e['inserted']==e['state_securities']==counts[day]
                        assert e['updated']==e['unchanged']==0
                        state=e['rows'];loaded=True
                    elif kind=='date_ready':
                        day=e['calculation_date'];expected=str(first) if previous is None else str(dt.date.fromisoformat(previous)+dt.timedelta(days=1))
                        assert reset and loaded and day==expected and e['previous_date']==previous
                        assert e['rows_applied']==state==counts[day] and e['effective_dates_applied']==1 and e['state_securities']==state
                        assert e['mode']==('snapshot_initialize' if previous is None else 'snapshot_replace')
                        total+=state;request_decoded+=e.get('rows_decoded',state);ready.append(e);previous=day;loaded=reset=False
                    elif kind=='worker_complete':assert previous==str(last) and not loaded and not reset;complete=True
                    else:raise AssertionError(e)
                assert complete and len(ready)==(last-first).days+1
                slot=next(r for r in slots if r['worker']==header['worker_id'] and r['api']==body['request_id'])
                worker_rows.append(dict(worker=header['worker_id'],first_date=str(first),last_date=str(last),initial_smf_records=ready[0]['rows_applied'],initial_rows_decoded=ready[0].get('rows_decoded',ready[0]['rows_applied']),initialization_seconds=ready[0]['seconds']-header['seconds'],processing_afterward_seconds=events[-1]['seconds']-ready[0]['seconds'],finished_since_first_queue_seconds=slot['released_s'],total_smf_rows=total))
                request_rows+=total
            assert request_decoded==int(body['metrics']['security_master_rows_decoded'])
            assert request_rows==int(body['metrics'].get('smf_snapshot_rows_served',request_rows))
            assert int(body['metrics']['smf_cache_hits'])+int(body['metrics']['smf_snapshot_loads'])==int(body['metrics']['days'])
            decoded+=request_decoded
    swap=[]
    for sample in exp['samples']:
        m=re.search(r'used = ([\d.]+)([MG])',sample.get('host_swap',''))
        if m:swap.append(float(m[1])/(1024 if m[2]=='M' else 1))
    final=next(s['health']['body'] for s in reversed(exp['samples']) if s['health']['status']==200)
    assert final['active']==final['running_date_workers']==final['waiting_date_workers']==0
    assert all(v['current']==0 for v in final['read_activity'].values())
    with (a.run/'worker-timing-split.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(worker_rows[0]));w.writeheader();w.writerows(worker_rows)
    result=dict(requests=len(times),workers=len(worker_rows),api_seconds=times,peak_sampled_rss_gib=exp['peak_sampled_rss_bytes']/2**30,
        swap_start_gib=swap[0] if swap else None,swap_peak_gib=max(swap) if swap else None,
        snapshot_rows_served=sum(r['total_smf_rows'] for r in worker_rows),smf_rows_decoded=decoded,trace_events=events_count,outputs_match_baseline=True,worker_trace=slot_stats,
        snapshot_checks='Every worker receives a complete snapshot per assigned date, through cache or decode; physical decoded rows and logical served rows accounted separately; no history replay or index opening; resets/references and footer row counts verified.',
        cache_policy='User confirmed purge immediately before run. No preparation, source validation or warm-up reads intervened. Purge is user-reported, not independently verified.')
    (a.run/'validation.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))

if __name__=='__main__':main()
