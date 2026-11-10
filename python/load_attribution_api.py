"""Measure API concurrency, sampled process RSS and natural post-request heap behavior.
Starts one server per experiment, retaining it across batches without forced GC.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import socket
import sys
import subprocess
import threading
import time
import urllib.error
import urllib.request


def fetch(url, payload=None, timeout=600):
    req = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(),
                                 headers={'Content-Type': 'application/json'})
    began = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return dict(status=response.status, seconds=time.monotonic()-began, body=json.load(response))
    except urllib.error.HTTPError as error:
        return dict(status=error.code, seconds=time.monotonic()-began, body=error.read().decode())
    except Exception as error:
        return dict(status=0, seconds=time.monotonic()-began, error=str(error))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, default=Path('results/api/benchmark-go'))
    p.add_argument('--data', type=Path, default=Path('results/six-input/inputs'))
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--start-date', default='2023-01-01')
    p.add_argument('--end-date', default='2025-12-28')
    p.add_argument('--workers', type=int, default=12)
    p.add_argument('--worker-limit', type=int, help='Rust API shared date-worker slots (default in Rust: 12)')
    p.add_argument('--request-limit', type=int, help='Rust API request admission slots before metadata/index opening (default 12)')
    p.add_argument('--worker-trace', action='store_true', help='Record Rust worker queue/admission/release events')
    p.add_argument('--smf-trace', choices=['off', 'summary', 'records'], default='off', help='Per-worker SMF update tracing in request output directories')
    p.add_argument('--aggregate', choices=['daily', 'total', 'security', 'sector'], default='daily')
    p.add_argument('--return-type', choices=['base', 'local', 'both'], default='base')
    p.add_argument('--private-smf', action='store_true', help='Independent SMF source plan and state per worker and request')
    p.add_argument('--stage-profile', action='store_true', help='Rust detailed per-date stage timings')
    p.add_argument('--concurrency', type=int, nargs='+', default=[1, 2, 4])
    p.add_argument('--repeats', type=int, default=2)
    p.add_argument('--settle-seconds', type=float, default=3)
    p.add_argument('--timeout', type=float, default=600)
    p.add_argument('--interval', type=float, default=.1)
    a = p.parse_args()
    if min([a.workers, a.repeats, *a.concurrency]) < 1 or min(a.interval,a.timeout) <= 0 or a.settle_seconds < 0:
        p.error('positive workers/concurrency/repeats/interval/timeout required; settle must be nonnegative')
    a.output.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    url = f'http://127.0.0.1:{port}'
    command = [str(a.binary.resolve()), '--listen', f'127.0.0.1:{port}', '--data', str(a.data.resolve()),
               '--output-dir', str((a.output/'requests').resolve()), '--threads', str(a.workers)]
    if a.worker_limit is not None:
        if a.worker_limit < 1: p.error('worker-limit must be positive')
        command += ['--worker-limit', str(a.worker_limit)]
    if a.request_limit is not None:
        if a.request_limit < 1: p.error('request-limit must be positive')
        command += ['--request-limit', str(a.request_limit)]
    if a.worker_trace:
        command += ['--worker-trace',str((a.output/'worker-trace.jsonl').resolve())]
    command += ['--smf-trace', a.smf_trace, '--aggregate', a.aggregate, '--return-type', a.return_type]
    if a.private_smf:
        command += ['--private-smf', 'true']
    if a.stage_profile:
        command += ['--stage-profile', 'true']
    samples, batches = [], []
    response_lock = threading.Lock()
    stop = threading.Event()
    started = time.monotonic()
    phase = 'startup'
    log = (a.output/'server.log').open('w')
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
    def sample():
        # Preserve samples as they arrive, including if the host/driver fails.
        with (a.output/'samples.jsonl').open('w', buffering=1) as evidence:
            last_system = 0
            while not stop.is_set():
                try:
                    rss = subprocess.check_output(['ps', '-o', 'rss=', '-p', str(process.pid)], text=True).strip()
                    health = fetch(url+'/health', timeout=2)
                    item = dict(seconds=time.monotonic()-started, phase=phase,
                                rss_bytes=int(rss)*1024, health=health)
                    if sys.platform == 'darwin' and time.monotonic()-last_system >= 5:
                        item['host_swap'] = subprocess.check_output(['sysctl', '-n', 'vm.swapusage'], text=True).strip()
                        item['host_vm_stat'] = subprocess.check_output(['vm_stat'], text=True)
                        last_system = time.monotonic()
                    samples.append(item)
                    evidence.write(json.dumps(item)+'\n')
                except (subprocess.CalledProcessError, ValueError):
                    pass
                stop.wait(a.interval)
    sampler = threading.Thread(target=sample, daemon=True)
    try:
        deadline = time.monotonic()+30
        while fetch(url+'/health', timeout=.5)['status'] != 200:
            if process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('server failed to start; see server.log')
            time.sleep(.05)
        server_ready_seconds = time.monotonic()-started
        startup_health = fetch(url+'/health')['body']
        (a.output/'startup.json').write_text(json.dumps(dict(server_ready_seconds=server_ready_seconds,health=startup_health,
            note='Server startup precedes timed API batches. Shared SMF load/opening cost is included here and must be reported separately.'),indent=2)+'\n')
        sampler.start()
        phase = 'baseline'; time.sleep(a.settle_seconds)
        for concurrency in a.concurrency:
            for repeat in range(a.repeats):
                phase = f'load-c{concurrency}-r{repeat+1}'
                gate = threading.Barrier(concurrency)
                def request(_):
                    gate.wait()
                    request_started = time.monotonic()
                    result = fetch(url+'/attribution', {'start_date': a.start_date, 'end_date': a.end_date}, a.timeout)
                    result['start_offset_seconds'] = request_started-began
                    result['end_offset_seconds'] = time.monotonic()-began
                    with response_lock, (a.output/'responses.jsonl').open('a') as evidence:
                        evidence.write(json.dumps(dict(phase=phase, request_index=_, response=result))+'\n')
                    return result
                began = time.monotonic()
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    responses = list(pool.map(request, range(concurrency)))
                batch = dict(phase=phase, concurrency=concurrency, repeat=repeat+1,
                             seconds=time.monotonic()-began, responses=responses)
                batches.append(batch)
                print(f'{phase}: {[r["status"] for r in responses]} in {batch["seconds"]:.3f}s', flush=True)
                phase = f'idle-c{concurrency}-r{repeat+1}'
                time.sleep(a.settle_seconds)
                if process.poll() is not None or any(r['status'] != 200 for r in responses):
                    break
            else:
                continue
            break
    finally:
        stop.set()
        if sampler.ident is not None: sampler.join(timeout=4)
        exit_before_stop = process.poll()
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        log.close()
        summary = dict(command=command, pid=process.pid, workers_per_request=a.workers,
                       data=str(a.data.resolve()), start_date=a.start_date, end_date=a.end_date,
                       process_exit_before_driver_shutdown=exit_before_stop,
                       peak_sampled_rss_bytes=max((s['rss_bytes'] for s in samples), default=0),
                       batches=batches, samples=samples,
                       caveats=['Sampled RSS may miss short peaks; shared pages are included.',
                                'Heap counters, when provided by the engine, are server-wide.',
                                'RSS retention alone does not establish a leak.',
                                'Process death is not classified as OOM without OS evidence.'])
        (a.output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
        print(f'Evidence: {a.output}/summary.json', flush=True)
    if exit_before_stop is not None or any(r['status'] != 200 for b in batches for r in b['responses']):
        raise SystemExit(1)

if __name__ == '__main__':
    main()
