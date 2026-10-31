"""Process resource measurement and deterministic subset selection helpers."""
import csv
import os
import subprocess
import sys
import time

def measured(command, log):
    started = time.perf_counter()
    with log.open('w') as f:
        process = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
        _, status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(status)
    if process.returncode:
        raise RuntimeError(f'Command failed: {command}\n{log.read_text()}')
    elapsed = time.perf_counter() - started
    rss = usage.ru_maxrss / (1024 ** 2 if sys.platform == 'darwin' else 1024)
    cpu = usage.ru_utime + usage.ru_stime
    return {'process_wall_seconds': elapsed, 'cpu_seconds': cpu, 'peak_rss_mib': rss, 'average_cores': cpu / elapsed}


def subset_file(data, target):
    # Include a useful standalone subset from every portfolio, without assuming ID ranges.
    selected = set()
    for path in sorted(data.glob('portfolio_*.csv')):
        with path.open(newline='') as f:
            for i, row in enumerate(csv.DictReader(f)):
                selected.add(row['InstrumentId'])
                if i == 99:
                    break
    with target.open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['InstrumentId'])
        w.writerows((sid,) for sid in sorted(selected))


if __name__ == '__main__':
    raise SystemExit('Run python3 python/benchmark_compact.py --runs 2.')
