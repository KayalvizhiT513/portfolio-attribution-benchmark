"""Export request latency, worker timing and memory summaries after a concurrency run."""
import argparse,csv,json,re,statistics
from pathlib import Path
from plot_worker_race import collect


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path,required=True);a=p.parse_args()
    experiment=json.loads((a.run/'summary.json').read_text())
    responses=[json.loads(l) for l in (a.run/'responses.jsonl').read_text().splitlines()]
    assert responses and all(r['response']['status']==200 for r in responses)
    slots,slotstats=collect(a.run);origin=min(json.loads(l)['seconds'] for l in (a.run/'worker-trace.jsonl').read_text().splitlines())
    request_rows=[];worker_rows=[]
    for record in sorted(responses,key=lambda r:r['request_index']):
        response=record['response'];body=response['body'];output=Path(body['output_dir'])
        stage=json.loads((output/'stage-profile.json').read_text())
        request_rows.append(dict(client_request=record['request_index']+1,server_request_id=body['request_id'],
            api_seconds=body['elapsed_seconds'],request_admission_wait_seconds=body.get('request_admission_wait_seconds',0),
            admitted_calculation_seconds=body.get('admitted_calculation_seconds',body['elapsed_seconds']),client_seconds=response['seconds'],completed_since_batch_start_seconds=response['end_offset_seconds'],
            planning_seconds=stage['planning_seconds'],worker_phase_including_queue_seconds=stage['worker_phase_seconds'],
            report_write_seconds=stage['report_write_seconds'],smf_records=int(body['metrics']['security_master_rows_decoded']),
            snapshot_rows_served=int(body['metrics'].get('smf_snapshot_rows_served',0)),output_dir=str(output)))
        for path in sorted(output.glob('smf-trace-worker-*.jsonl')):
            events=[json.loads(l) for l in path.read_text().splitlines()];header=events[0]
            ready=next(e for e in events if e['event']=='date_ready');assert events[-1]['event']=='worker_complete'
            slot=next(s for s in slots if s['api']==record['request_index']+1 and s['worker']==header['worker_id'])
            worker_rows.append(dict(client_request=record['request_index']+1,server_request_id=body['request_id'],worker=header['worker_id'],
                first_date=header['first_date'],last_date=header['last_date'],initial_smf_records=ready['rows_applied'],
                initial_rows_decoded=ready.get('rows_decoded',ready['rows_applied']),
                initialization_seconds=ready['seconds']-header['seconds'],processing_afterward_seconds=events[-1]['seconds']-ready['seconds'],
                queue_wait_seconds=slot['wait_s'],slot_held_seconds=slot['held_s'],finished_since_first_worker_queued_seconds=slot['released_s']))
    for name,rows in [('request-timings.csv',request_rows),('worker-timings.csv',worker_rows)]:
        with (a.run/name).open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    samples=experiment['samples'];swap=[];vm={key:[] for key in ('Swapins','Swapouts')}
    for s in samples:
        match=re.search(r'used = ([\d.]+)([MG])',s.get('host_swap',''))
        if match:swap.append(float(match[1])/(1024 if match[2]=='M' else 1))
        for key in vm:
            match=re.search(rf'^{key}:\s+(\d+)',s.get('host_vm_stat',''),re.MULTILINE)
            if match:vm[key].append(int(match[1]))
    health=[s['health']['body'] for s in samples if s['health']['status']==200];assert health[-1]['active']==0
    durations=sorted(r['client_seconds'] for r in request_rows)
    request_gate=None
    if 'request_limit' in health[-1]:
        request_gate=dict(limit=health[-1]['request_limit'],peak_admitted=max(h['peak_running_requests'] for h in health),peak_sampled_waiting=max(h['waiting_requests'] for h in health),final_admitted=health[-1]['running_requests'],final_waiting=health[-1]['waiting_requests'])
        assert request_gate['peak_admitted']<=request_gate['limit']
        assert request_gate['final_admitted']==request_gate['final_waiting']==0
    result=dict(requests=len(request_rows),successful=len(request_rows),workers=len(worker_rows),
        batch_seconds=[b['seconds'] for b in experiment['batches']],first_request_completed_seconds=min(r['completed_since_batch_start_seconds'] for r in request_rows),
        request_seconds_min=durations[0],request_seconds_median=statistics.median(durations),request_seconds_max=durations[-1],
        request_admission=request_gate,request_admission_wait_seconds_max=max(r['request_admission_wait_seconds'] for r in request_rows),
        planning_seconds_min=min(r['planning_seconds'] for r in request_rows),planning_seconds_median=statistics.median(r['planning_seconds'] for r in request_rows),planning_seconds_max=max(r['planning_seconds'] for r in request_rows),
        peak_sampled_rss_gib=experiment['peak_sampled_rss_bytes']/2**30,swap_start_gib=swap[0] if swap else None,swap_peak_gib=max(swap) if swap else None,
        host_vm_counter_deltas={k:v[-1]-v[0] if v else None for k,v in vm.items()},worker_trace=slotstats,
        total_smf_records=sum(r['smf_records'] for r in request_rows),total_snapshot_rows_served=sum(r['snapshot_rows_served'] for r in request_rows),
        note='Request admission, when enabled, precedes planning. Planning is per admitted request and precedes shared worker admission; worker phase includes date-worker queue waits. RSS includes mapped/shared pages. Swap is host-wide. Durations overlap and cannot be added across requests.')
    if (a.run/'startup.json').exists():
        startup=json.loads((a.run/'startup.json').read_text())
        result['server_ready_seconds']=startup['server_ready_seconds']
        result['shared_smf']=health[-1].get('shared_smf')
        result['startup_note']='Server ready/load time is outside API batch latency; report both to include shared SMF metadata initialization cost.'
    (a.run/'concurrency-summary.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))

if __name__=='__main__':main()
