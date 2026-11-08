"""Exercise concurrent in-process requests against the independent six-input oracle."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import math
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

from load_attribution_api import fetch
from test_six_inputs import fixture
from test_six_inputs import rewrite_dates
from inspect_smf_trace import validate_trace
import pyarrow as pa
import pyarrow.parquet as pq
from test_attribution import FIELDS, key, records, reference


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, default=Path('results/api/benchmark-go'))
    p.add_argument('--worker-limit', type=int)
    p.add_argument('--request-limit', type=int)
    p.add_argument('--private-smf', action='store_true')
    p.add_argument('--snapshots', action='store_true')
    p.add_argument('--requests', type=int, default=4)
    a = p.parse_args()
    with tempfile.TemporaryDirectory(prefix='attribution-api-') as tmp:
        root = Path(tmp); inputs = root/'inputs'; inputs.mkdir()
        days, data = fixture(inputs)
        server_inputs = inputs
        if a.snapshots:
            from prepare_daily_smf import prepare
            server_inputs = root / 'snapshots'
            prepare(inputs, server_inputs)

        expected = {}
        for i, day in enumerate(days):
            countries = {'S000001': 'US', 'S000002': 'DE' if i >= 2 else 'US', 'S000003': 'DE'}
            expected.update(reference(countries, {e: [v[i]] for e,v in data.items()}, 'PF', 'country', 'day'))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0)); port = sock.getsockname()[1]
        url = f'http://127.0.0.1:{port}'
        with (root/'server.log').open('w') as log:
            command = [str(a.binary.resolve()), '--listen', f'127.0.0.1:{port}',
                '--data', str(server_inputs), '--output-dir', str(root/'outputs'), '--threads', '3', '--smf-trace', 'records']
            if a.worker_limit is not None: command += ['--worker-limit',str(a.worker_limit)]
            if a.request_limit is not None: command += ['--request-limit',str(a.request_limit)]
            if a.private_smf: command += ['--private-smf','true','--stage-profile','true']
            process = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                deadline = time.monotonic()+20
                while fetch(url+'/health', timeout=.2)['status'] != 200:
                    assert process.poll() is None and time.monotonic()<deadline
                    time.sleep(.05)
                startup_health = fetch(url+'/health')['body']
                if a.private_smf:
                    assert startup_health['shared_smf']==dict(enabled=False,state_scope='worker',source_scope='worker')
                if 'shared_smf' in startup_health and not a.private_smf:
                    shared = startup_health['shared_smf']
                    assert shared['metadata_loads']==1 and shared['request_uses']==0
                    assert shared['scope']=='server'
                    assert shared['state_scope']=='worker'
                    assert 'snapshot_cache' not in shared and 'parsed_snapshot_cache' not in shared
                payload = dict(start_date='2023-01-01',end_date='2023-01-04')
                assert fetch(url+'/attribution', dict(payload, threads=100))['status'] == 400
                assert fetch(url+'/attribution', dict(payload, end_date='2022-01-01'))['status'] == 400
                assert fetch(url+'/attribution', dict(payload, end_date='2023-01-05'))['status'] == 422
                # Fail inside a date calculation after admission, then restore
                # the source and prove later requests can still acquire slots.
                fx = inputs/'forex.parquet'
                original = fx.read_bytes()
                try:
                    table = pq.read_table(fx)
                    rates = table.schema.get_field_index('Rate')
                    rewrite_dates(fx,table.set_column(rates,'Rate',pa.array([0.]*len(table))))
                    assert fetch(url+'/attribution',payload)['status']==422
                finally:
                    fx.write_bytes(original)
                if a.worker_limit is not None:
                    released = fetch(url+'/health')['body']
                    assert released['running_date_workers']==released['waiting_date_workers']==0,released
                    if a.request_limit is not None:
                        assert released['running_requests']==released['waiting_requests']==0,released
                    if 'read_activity' in released:
                        assert all(v['current']==0 for v in released['read_activity'].values()),released
                # Two batches test output isolation and reuse of the same live process.
                paths = set()
                snapshots = []
                for _ in range(2):
                    with ThreadPoolExecutor(max_workers=a.requests) as pool:
                        futures = [pool.submit(fetch,url+'/attribution',payload) for _ in range(a.requests)]
                        while not all(f.done() for f in futures):
                            snapshots.append(fetch(url+'/health')['body'])
                            time.sleep(.002)
                        responses = [f.result() for f in futures]
                    for response in responses:
                        assert response['status']==200, response
                        body = response['body']; paths.add(body['report'])
                        actual = list(records(Path(body['report'])))
                        assert len(actual)==len(expected)
                        for row in actual:
                            for field, target in zip(FIELDS, expected[key(row)]):
                                assert math.isclose(float(row[field]),target,rel_tol=1e-8,abs_tol=1e-10),(field,row,target)
                        traces = list(Path(body['output_dir']).glob('smf-trace-worker-*.jsonl'))
                        assert len(traces) == 3
                        for trace in traces:
                            validate_trace(trace, inputs)
                        if a.request_limit is not None:
                            assert 0 <= body['request_admission_wait_seconds'] <= body['elapsed_seconds']
                            assert body['admitted_calculation_seconds'] <= body['elapsed_seconds']
                        assert body['metrics']['workers']=='3'
                        if a.snapshots:
                            assert body['metrics']['smf_snapshot_loads']=='4'
                            assert body['metrics']['smf_cache_hits']=='0'
                            assert body['metrics']['smf_snapshot_rows_served']=='12'
                            assert body['metrics']['security_master_rows_decoded']=='12'
                        else:
                            assert body['metrics']['smf_snapshot_loads']=='3'
                assert len(paths)==a.requests*2, 'requests overwrote one another'
                health = fetch(url+'/health')['body']
                assert (health['active'],health['completed'],health['failed'])==(0,a.requests*2,2),health
                if a.request_limit is not None:
                    assert health['request_limit']==a.request_limit
                    assert 0 < health['peak_running_requests'] <= a.request_limit
                    assert health['running_requests']==health['waiting_requests']==0
                    assert all(s['running_requests']<=a.request_limit for s in snapshots)
                    if a.requests > a.request_limit:
                        assert any(s['waiting_requests']>0 for s in snapshots), 'no request admission queue observed'
                    assert any(r['body']['request_admission_wait_seconds']>0 for r in responses)
                if a.worker_limit is not None:
                    assert 0 < health['peak_running_date_workers'] <= a.worker_limit,health
                    assert health['running_date_workers']==health['waiting_date_workers']==0,health
                    assert all(s['running_date_workers']<=a.worker_limit for s in snapshots)
                    assert any(s['waiting_date_workers']>0 for s in snapshots),'no worker queue observed'
                    if 'read_activity' in health:
                        activity=health['read_activity']
                        assert all(v['current']==0 for v in activity.values()),activity
                        assert 0<activity['read_decode_calls']['peak']<=6*a.worker_limit,activity
                        assert 0<activity['reading_workers']['peak']<=a.worker_limit,activity
                        assert 0<activity['retained_daily_inputs']['peak']<=5*a.worker_limit,activity
                        assert 0<activity['workers_with_smf_cache']['peak']<=a.worker_limit,activity
                if a.private_smf:
                    profiles=list((root/'outputs').glob('request-*/stage-profile.json'))
                    assert len(profiles)>=a.requests*2
                    for profile_path in profiles:
                        profile=json.loads(profile_path.read_text())
                        assert profile['smf_source_scope']=='worker' and not profile['smf_shared_at_server']
                extra_successes = 0
                if 'shared_smf' in health and not a.private_smf:
                    shared = health['shared_smf']
                    assert shared['metadata_loads']==1
                    assert shared['request_uses']==a.requests*2+2
                    # Shared histories must not mix current states from different windows.
                    windows = [dict(start_date='2023-01-01',end_date='2023-01-02'),
                               dict(start_date='2023-01-03',end_date='2023-01-04')]
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        mixed = list(pool.map(lambda window:fetch(url+'/attribution',window),windows))
                    for window,response in zip(windows,mixed):
                        assert response['status']==200,response
                        rows=list(records(Path(response['body']['report'])))
                        expected_window={k:v for k,v in expected.items() if window['start_date']<=k[1]<=window['end_date']}
                        assert len(rows)==len(expected_window)
                        for row in rows:
                            for field,value in zip(FIELDS,expected_window[key(row)]):
                                assert math.isclose(float(row[field]),value,rel_tol=1e-8,abs_tol=1e-10),(field,row,value)
                    extra_successes += 2
                    # Reject stale sources without silently reopening/rebuilding the shared metadata.
                    master = server_inputs/'security_master.parquet'
                    stamp=master.stat()
                    try:
                        os.utime(master,ns=(stamp.st_atime_ns,stamp.st_mtime_ns+1000000))
                        changed=fetch(url+'/attribution',payload)
                        assert changed['status']==422 and 'shared SMF source changed' in changed['body'],changed
                    finally:
                        os.utime(master,ns=(stamp.st_atime_ns,stamp.st_mtime_ns))
                    recovered=fetch(url+'/attribution',payload)
                    assert recovered['status']==200,recovered
                    extra_successes += 1
                    after=fetch(url+'/health')['body']
                    assert after['shared_smf']['metadata_loads']==1
                    assert after['shared_smf']['request_uses']==a.requests*2+5
                    assert after['completed']==a.requests*2+3 and after['failed']==3
                    if a.request_limit is not None:assert after['running_requests']==after['waiting_requests']==0
                print(f'Passed: {a.requests*2+extra_successes} successful requests including concurrent/repeated outputs match independent oracle; isolated outputs; validation failures; counters; shared SMF metadata reuse; request admission and shared worker limits checked when configured.')
            finally:
                process.terminate(); process.wait(timeout=10)
        assert 'WARNING: DATA RACE' not in (root/'server.log').read_text(), 'Go race detector reported a race'

if __name__=='__main__':
    main()
