"""Benchmark the five native scenarios with stage, OS-resource and correctness metrics.

Build both executables first; see ../rust/README.md. Default: three dates,
three runs, all portfolios. Outputs are reused between repetitions.
"""
import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from test_attribution import compare_files

SCENARIOS = ['security', 'subset', 'country', 'weekly', 'monthly']
STAGES = ['read_parse', 'filter', 'aggregate', 'compute', 'write']


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


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--data', type=Path, default=Path('attribution_data'))
    ap.add_argument('--output-dir', type=Path, default=Path('results/native-benchmark'))
    ap.add_argument('--rust', type=Path, default=Path('rust/target/release/portfolio-attribution'))
    ap.add_argument('--go', type=Path, default=Path('benchmark-go-attribution'))
    ap.add_argument('--rust-threads', type=int, default=min(4, os.cpu_count() or 1))
    ap.add_argument('--go-threads', type=int, default=os.cpu_count() or 1)
    ap.add_argument('--days', type=int, default=3, help='0 means all dates; may require hundreds of GiB')
    ap.add_argument('--runs', type=int, default=3)
    ap.add_argument('--scenarios', nargs='+', choices=SCENARIOS, default=SCENARIOS)
    args = ap.parse_args()
    if args.runs < 2 or args.days < 0 or min(args.rust_threads, args.go_threads) < 1:
        ap.error('runs >= 2, days >= 0, threads >= 1 required')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(args.output_dir).free < 5 * 1024 ** 3:
        ap.error('at least 5 GiB free space required for benchmark outputs')
    subset = args.output_dir / 'subset.csv'
    subset_file(args.data, subset)
    identity = {language: {'path': str(getattr(args, language).resolve()), 'sha256': hashlib.sha256(getattr(args, language).read_bytes()).hexdigest()} for language in ['rust', 'go']}
    input_files = [{'name': p.name, 'bytes': p.stat().st_size, 'mtime_ns': p.stat().st_mtime_ns} for p in sorted(args.data.glob('*.csv'))]
    raw = []
    checks = []
    for scenario in args.scenarios:
        for repetition in range(args.runs):
            order = ['rust', 'go'] if repetition % 2 == 0 else ['go', 'rust']
            for language in order:
                print(f'{scenario}: run {repetition + 1}/{args.runs}, {language}', flush=True)
                output = args.output_dir / scenario / language
                output.mkdir(parents=True, exist_ok=True)
                metrics_path = output / 'metrics.csv'
                binary = getattr(args, language).resolve()
                threads = getattr(args, language + '_threads')
                command = [str(binary), '--data', str(args.data), '--output-dir', str(output), '--metrics', str(metrics_path), '--scenario', scenario, '--threads', str(threads), '--max-days', str(args.days), '--instruments-file', str(subset)]
                resource = measured(command, output / f'run-{repetition + 1}.log')
                with metrics_path.open(newline='') as f:
                    metrics = next(csv.DictReader(f))
                row = {'repetition': repetition + 1, **metrics, **resource}
                raw.append(row)
                print(f"  {resource['process_wall_seconds']:.3f}s, peak RSS {resource['peak_rss_mib']:.1f} MiB", flush=True)
        # Compare the final repetition, streaming rather than loading giant outputs.
        print(f'{scenario}: comparing CSV results', flush=True)
        rust_dir, go_dir = [args.output_dir / scenario / lang for lang in ['rust', 'go']]
        rust_files = sorted(rust_dir.glob(f'{scenario}-portfolio_*.csv'))
        assert [p.name for p in rust_files] == [p.name for p in sorted(go_dir.glob(f'{scenario}-portfolio_*.csv'))]
        count, difference = 0, 0.0
        for path in rust_files:
            n, delta = compare_files(path, go_dir / path.name)
            count += n
            difference = max(difference, delta)
        checks.append({'scenario': scenario, 'rows_compared': count, 'max_absolute_difference': difference, 'status': 'PASS'})
        print(f'  PASS: {count:,} rows; maximum absolute difference {difference:.3g}', flush=True)
    with (args.output_dir / 'runs.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(raw[0]))
        w.writeheader()
        w.writerows(raw)
    summary = []
    numeric = ['process_wall_seconds', 'cpu_seconds', 'peak_rss_mib', 'average_cores'] + [s + '_seconds' for s in STAGES]
    for scenario in args.scenarios:
        for language in ['rust', 'go']:
            samples = [r for r in raw if r['scenario'] == scenario and r['language'] == language]
            s = {'scenario': scenario, 'language': language, 'runs': args.runs, 'days': samples[0]['days'], 'threads': samples[0]['threads']}
            for field in numeric:
                values = [float(r[field]) for r in samples]
                s[field] = statistics.median(values)
                s[field + '_min'] = min(values)
                s[field + '_max'] = max(values)
            for field in ['rows_read', 'rows_retained', 'groups', 'effect_values', 'output_rows', 'output_bytes', 'parsed_input_bytes']:
                s[field] = int(samples[0][field])
            for stage, numerator in [('read_parse', 'rows_read'), ('filter', 'rows_read'), ('aggregate', 'rows_retained'), ('compute', 'effect_values'), ('write', 'output_rows')]:
                s[stage + '_million_units_per_worker_second'] = s[numerator] / max(s[stage + '_seconds'], 1e-12) / 1e6
            s['read_parse_mib_per_worker_second'] = s['parsed_input_bytes'] / max(s['read_parse_seconds'], 1e-12) / 1024 ** 2
            s['write_mib_per_worker_second'] = s['output_bytes'] / max(s['write_seconds'], 1e-12) / 1024 ** 2
            summary.append(s)
    with (args.output_dir / 'summary.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]))
        w.writeheader()
        w.writerows(summary)
    metadata = {'platform': platform.platform(), 'machine': platform.machine(), 'logical_cpus': os.cpu_count(), 'data': str(args.data.resolve()), 'max_days': args.days, 'runs': args.runs, 'cache_policy': 'OS-managed; no forced cold-cache clearing; language order alternates; first run retained', 'rust_threads': args.rust_threads, 'go_GOMAXPROCS': args.go_threads, 'correctness': checks, 'executables': identity, 'input_files': input_files}
    assert all(hashlib.sha256(getattr(args, lang).read_bytes()).hexdigest() == identity[lang]['sha256'] for lang in ['rust', 'go']), 'executable changed during benchmark'
    (args.output_dir / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    lines = ['# Native attribution benchmark', '', f"Scope: {'all' if args.days == 0 else 'first ' + str(args.days)} effective dates, all supplied portfolios, {args.runs} repetitions. OS-managed filesystem cache; language order alternates. First run is retained. Results describe these implementations and this workload, not universal language performance.", '', f"Machine: {metadata['platform']}; {metadata['logical_cpus']} logical CPUs. Rust: {args.rust_threads} portfolio-worker limit; Go: GOMAXPROCS={args.go_threads} with concurrent I/O goroutines. Separate settings use each runtime's concurrency model; no shared thread cap is imposed.", '', '## Overall results', '', '| Scenario | Rust seconds (min–max) | Go seconds (min–max) | Go/Rust time | Rust/Go peak MiB | Rust/Go CPU seconds | Rust/Go average cores |', '|---|---:|---:|---:|---:|---:|---:|']
    for scenario in args.scenarios:
        r, g = [next(s for s in summary if s['scenario'] == scenario and s['language'] == lang) for lang in ['rust', 'go']]
        duration = lambda s: f"{s['process_wall_seconds']:.3f} ({s['process_wall_seconds_min']:.3f}–{s['process_wall_seconds_max']:.3f})"
        lines.append(f"| {scenario} | {duration(r)} | {duration(g)} | {g['process_wall_seconds'] / r['process_wall_seconds']:.2f}× | {r['peak_rss_mib']:.1f} / {g['peak_rss_mib']:.1f} | {r['cpu_seconds']:.2f} / {g['cpu_seconds']:.2f} | {r['average_cores']:.2f} / {g['average_cores']:.2f} |")
    lines += ['', 'Go/Rust time > 1 means Rust was faster. Peak RSS is the median per-process maximum resident set, not total OS cache usage. CPU seconds include all native process threads.', '', '## Stage efficiency', '', 'Stage times are summed elapsed time inside each stage across reader/worker threads. They include scheduling pauses and can overlap; they are not CPU time and do not add up to overall wall time. Rates below divide work by this accumulated stage time. Read includes CSV decoding, numeric parsing, key lookup and input validation. Aggregate includes normalization, dense-group allocation, and weighted contributions. Write includes formatting, buffering, flushing and fsync. Master loading, channel waits and setup are only in overall time. Filter is N/A except for the standalone-subset scenario.', '', '| Scenario | Language | Read s; M rows/s; MiB/s | Filter s; M rows/s | Aggregate s; M retained rows/s | Compute s; M effect values/s | Write s; M rows/s; MiB/s |', '|---|---|---:|---:|---:|---:|---:|']
    for s in summary:
        value = lambda stage: f"{s[stage + '_seconds']:.4f}; {s[stage + '_million_units_per_worker_second']:.3f}"
        filt = value('filter') if s['scenario'] == 'subset' else 'N/A'
        lines.append(f"| {s['scenario']} | {s['language']} | {value('read_parse')}; {s['read_parse_mib_per_worker_second']:.1f} | {filt} | {value('aggregate')} | {value('compute')} | {value('write')}; {s['write_mib_per_worker_second']:.1f} |")
    lines += ['', '## Correctness', '', '| Scenario | Rows compared | Maximum absolute numeric difference | Status |', '|---|---:|---:|---|']
    for check in checks:
        lines.append(f"| {check['scenario']} | {check['rows_compared']:,} | {check['max_absolute_difference']:.3g} | {check['status']} |")
    lines += ['', 'Every date and period also reconciles within each implementation. The independent synthetic oracle in `python/test_attribution.py` covers nine scenario/period combinations and invalid inputs. Numeric comparisons allow absolute tolerance 1e-10 or relative tolerance 1e-8. Output is decimal round-trip precision; Rust and Go may use different exponent notation.', '', 'Input byte throughput uses parsed return-file bytes, including one-record lookahead at the date limit; it excludes security-master/header bytes. Full input file sizes are recorded separately and are not used as the byte-throughput numerator for truncated runs. Filtering is computational filtering after CSV parsing, so a small subset does not avoid reading source rows.', '', 'The subset is the union of the first 100 instruments found in each portfolio. Each side is independently normalized every date. Period labels use observed first/last effective dates, including partial weeks/months. These runs do not establish full-history output-write performance. Use `--days 0` with sufficient disk space to measure it.', '', 'Raw repetitions: `runs.csv`; medians, ranges and throughput: `summary.csv`; configuration and correctness: `environment.json`.']
    (args.output_dir / 'REPORT.md').write_text('\n'.join(lines) + '\n')
    print(f"Report: {args.output_dir / 'REPORT.md'}", flush=True)


if __name__ == '__main__':
    main()
