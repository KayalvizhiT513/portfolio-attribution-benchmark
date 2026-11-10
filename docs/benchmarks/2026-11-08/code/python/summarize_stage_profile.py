"""Export elapsed and accumulated stage timings for one profiled Rust API run."""
import argparse
import csv
import json
import math
from pathlib import Path

from plot_worker_race import collect

STAGES = ['input_phase_seconds', 'accounts_fx_lookup_seconds', 'analytics_lookup_seconds',
          'returns_processing_seconds', 'join_validation_seconds', 'attribution_seconds',
          'calculate_cleanup_and_other_seconds', 'date_other_seconds']
SMF = ['smf_read_decode_seconds', 'smf_apply_and_trace_seconds', 'smf_release_and_other_seconds']


def summarize(root):
    experiment = json.loads((root/'summary.json').read_text())
    responses = [r for b in experiment['batches'] for r in b['responses']]
    assert len(responses) == 1 and responses[0]['status'] == 200
    response = responses[0]['body']
    output = Path(response['output_dir'])
    global_times = json.loads((output/'stage-profile.json').read_text())
    slots, _ = collect(root)
    last_worker = max(slots, key=lambda r:r['released_s'])['worker']
    workers = []
    for path in sorted(output.glob('stage-profile-worker-*.jsonl')):
        events = [json.loads(line) for line in path.read_text().splitlines()]
        assert events[0]['event']=='worker_start' and events[-1]['event']=='worker_complete'
        dates = [e for e in events if e['event']=='date_timing']
        assert dates and dates[0]['initialization'] and sum(d['initialization'] for d in dates)==1
        for date in dates:
            assert math.isclose(sum(date[k] for k in STAGES),date['date_seconds'],abs_tol=1e-8)
            assert math.isclose(sum(date[k] for k in SMF),date['input_read_decode_seconds'][2],abs_tol=1e-8)
            assert all(date[k]>=0 for k in STAGES+SMF)
        row = dict(worker_id=events[0]['worker_id'],first_date=dates[0]['date'],last_date=dates[-1]['date'],
            dates=len(dates),date_seconds=sum(d['date_seconds'] for d in dates),
            initialization_input_seconds=dates[0]['input_phase_seconds'],
            initialization_smf_read_seconds=dates[0][SMF[0]],
            initialization_smf_apply_and_trace_seconds=dates[0][SMF[1]],
            initialization_smf_other_seconds=dates[0][SMF[2]],
            admission_wait_seconds=events[0]['admission_wait_seconds'])
        row.update({k:sum(d[k] for d in dates) for k in STAGES+SMF})
        row.update({f'{name}_read_decode_seconds':sum(d['input_read_decode_seconds'][i] for d in dates)
                    for i,name in enumerate(['accounts','forex','smf_total','analytics','portfolio','index'])})
        slot = next(s for s in slots if s['worker']==row['worker_id'])
        row['slot_held_seconds'] = slot['held_s']
        row['worker_setup_teardown_profile_and_other_seconds'] = slot['held_s']-row['date_seconds']
        assert row['worker_setup_teardown_profile_and_other_seconds']>=0
        workers.append(row)
    assert sum(w['dates'] for w in workers)==int(response['metrics']['days'])
    with (root/'stage-worker-summary.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(workers[0]));writer.writeheader();writer.writerows(workers)
    critical = next(w for w in workers if w['worker_id']==last_worker)
    totals = {k:sum(w[k] for w in workers) for k in STAGES+SMF}
    summary=dict(api_elapsed_seconds=response['elapsed_seconds'],global_stages=global_times,
                 last_worker=critical,accumulated_worker_stages=totals,
                 caveat='Per-reader SMF timings overlap input phases; worker stages overlap across workers. These are elapsed spans including waits, not CPU times. SMF apply includes enabled summary tracing.')
    (root/'stage-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))
    return summary


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True)
    summarize(p.parse_args().run)
