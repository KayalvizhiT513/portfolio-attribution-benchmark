"""Validate daily-heavy benchmark outputs, worker admission and SMF summary traces."""
import argparse
import csv
from collections import defaultdict
import datetime as dt
import json
from pathlib import Path
import re

import pyarrow.parquet as pq
from plot_worker_race import collect
from test_attribution import compare_files


def validate(root, baseline=None):
    experiment = json.loads((root/'summary.json').read_text())
    data = Path(experiment['data'])
    manifest = json.loads((data/'manifest.json').read_text())
    assert manifest['smf_format'] == 'sparse per-security effective-date updates'
    file = pq.ParquetFile(data/'security_master.parquet')
    col = file.schema_arrow.get_field_index('EffectiveDate')
    source_rows = defaultdict(int)
    for i in range(file.num_row_groups):
        group = file.metadata.row_group(i)
        stats = group.column(col).statistics
        assert stats.min == stats.max
        source_rows[str(stats.min)] += group.num_rows
    effective_dates = sorted(source_rows)
    securities = manifest['smf_securities']
    start, end = map(dt.date.fromisoformat, [experiment['start_date'], experiment['end_date']])
    days = [str(start + dt.timedelta(days=n)) for n in range((end-start).days+1)]
    requests = [r for batch in experiment['batches'] for r in batch['responses']]
    assert requests and all(r['status'] == 200 for r in requests)
    slots, slot_stats = collect(root)
    worker_count = trace_events = smf_rows = 0
    reports = []
    worker_summaries = []
    for response in requests:
        body = response['body']
        out = Path(body['output_dir'])
        report = Path(body['report']); reports.append(report)
        if baseline:
            compare_files(baseline, report)
        elif report != reports[0]:
            compare_files(reports[0], report)
        paths = sorted(out.glob('smf-trace-worker-*.jsonl'))
        workers = int(body['metrics']['workers'])
        assert len(paths) == workers == 12
        request_rows = 0
        for worker, path in enumerate(paths):
            assigned = days[len(days)*worker//workers:len(days)*(worker+1)//workers]
            previous = None
            applied = []
            ready_count = rows = 0
            complete = False
            initialization_rows = incremental_rows = 0
            with path.open() as f:
                for sequence, line in enumerate(f):
                    event = json.loads(line); trace_events += 1
                    assert event['sequence'] == sequence and event['worker_id'] == worker+1
                    assert event['request_id'] == body['request_id'] and not complete
                    kind = event['event']
                    if kind == 'worker_start':
                        assert sequence == 0 and event['trace_level'] == 'summary'
                        assert (event['first_date'],event['last_date']) == (assigned[0], assigned[-1])
                    elif kind == 'effective_date_applied':
                        effective = event['effective_date']
                        assert event['calculation_date'] == assigned[ready_count]
                        assert event['rows'] == source_rows[effective]
                        # This generated workload initializes the universe once;
                        # every subsequent record changes existing attributes.
                        initial = effective == effective_dates[0]
                        assert event['inserted'] == (securities if initial else 0)
                        assert event['updated'] == (0 if initial else event['rows'])
                        assert event['unchanged'] == 0 and event['state_securities'] == securities
                        applied.append(effective); rows += event['rows']
                    elif kind == 'date_ready':
                        date = assigned[ready_count]
                        expected = [d for d in effective_dates if (previous is None or d > previous) and d <= date]
                        assert applied == expected
                        assert event['calculation_date'] == date and event['previous_date'] == previous
                        assert event['effective_dates_applied'] == len(expected) and event['rows_applied'] == rows
                        assert event['state_securities'] == securities
                        mode = 'initialize' if previous is None else ('update' if expected else 'reuse')
                        assert event['mode'] == mode
                        request_rows += rows
                        if previous is None: initialization_rows += rows
                        else: incremental_rows += rows
                        previous, applied, rows = date, [], 0
                        ready_count += 1
                    elif kind == 'worker_complete':
                        assert ready_count == len(assigned) and not applied
                        complete = True
                    else:
                        raise AssertionError(('unexpected event', event))
            assert complete
            worker_count += 1
            worker_summaries.append(dict(request_id=body['request_id'], worker_id=worker+1,
                first_date=assigned[0], last_date=assigned[-1], dates=len(assigned),
                initialization_rows=initialization_rows, incremental_rows=incremental_rows,
                total_decoded_rows=initialization_rows+incremental_rows, trace=str(path)))
        assert request_rows == int(body['metrics']['security_master_rows_decoded'])
        assert int(body['metrics']['days']) == len(days)
        smf_rows += request_rows
    health = [s['health']['body'] for s in experiment['samples'] if s['health']['status']==200]
    assert health[-1]['active'] == health[-1]['running_date_workers'] == health[-1]['waiting_date_workers'] == 0
    assert all(v['current']==0 for v in health[-1]['read_activity'].values())
    swap = []
    for sample in experiment['samples']:
        match = re.search(r'used = ([\d.]+)([MG])', sample.get('host_swap',''))
        if match: swap.append(float(match[1]) / (1024 if match[2]=='M' else 1))
    vm = [s['host_vm_stat'] for s in experiment['samples'] if 'host_vm_stat' in s]
    vm_deltas = {}
    for key in ('Swapins', 'Swapouts'):
        values = [int(re.search(rf'^{key}:\s+(\d+)', text, re.MULTILINE)[1]) for text in vm]
        vm_deltas[key] = values[-1]-values[0] if values else None
    result = dict(requests=len(requests), workers=worker_count, smf_trace_events=trace_events,
        smf_rows_decoded=smf_rows, batch_seconds=[b['seconds'] for b in experiment['batches']],
        first_request_completed_seconds=min(r['end_offset_seconds'] for r in requests),
        peak_sampled_rss_gib=experiment['peak_sampled_rss_bytes']/2**30,
        swap_start_gib=swap[0] if swap else None, swap_peak_gib=max(swap) if swap else None,
        host_vm_counter_deltas=vm_deltas,
        worker_trace=slot_stats, baseline=str(baseline or reports[0]), reports_matched=True,
        smf_summary_checks='Every worker/date checked against source row-group counts and generated schedule; aggregate insert/update counts verified. Per-security values were verified on small record-level fixtures, not logged in this full run.',
        cache_policy='OS-managed cache; no purge; separate server process per experiment.')
    with (root/'smf-worker-summary.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(worker_summaries[0]))
        writer.writeheader(); writer.writerows(worker_summaries)
    (root/'validation.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True)
    p.add_argument('--baseline',type=Path)
    args=p.parse_args()
    validate(args.run,args.baseline)


if __name__=='__main__':main()
