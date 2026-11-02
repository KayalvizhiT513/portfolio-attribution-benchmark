"""Optional pre-run OS input-cache preparation shared by Go/Rust benchmarks."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

MODES = ('normal', 'evict', 'macos-purge')


def input_paths(command):
    def option(name, default=None):
        for i, arg in enumerate(command):
            if arg == name:
                return command[i + 1]
            if arg.startswith(name + '='):
                return arg.split('=', 1)[1]
        return default

    data = Path(option('--data', 'attribution_data'))
    paths = []
    for stem in ('security_master', 'index'):
        matches = [data / (stem + ext) for ext in ('.csv', '.parquet')
                   if (data / (stem + ext)).is_file()]
        if len(matches) != 1:
            raise ValueError(f'Expected exactly one CSV/Parquet input for {data / stem}')
        paths.extend(matches)
    portfolios = sorted(p for p in data.glob('portfolio_*')
                        if p.suffix in ('.csv', '.parquet') and p.is_file())
    if not portfolios:
        raise ValueError(f'No portfolio inputs in {data}')
    paths.extend(portfolios)
    subset = option('--instruments-file')
    if subset:
        paths.append(Path(subset))
    # Resolve symlinks, including the runtime runner's Parquet holdings links.
    return sorted({p.resolve(strict=True) for p in paths})


def prepare_input_cache(command, mode=None):
    mode = mode or os.environ.get('BENCHMARK_INPUT_CACHE', 'normal')
    if mode not in MODES:
        raise ValueError(f'Invalid BENCHMARK_INPUT_CACHE {mode!r}; choose {MODES}')
    started = time.perf_counter()
    paths = []
    if mode != 'normal':
        paths = input_paths(command)
        if mode == 'evict':
            if not sys.platform.startswith('linux') or not hasattr(os, 'posix_fadvise'):
                raise RuntimeError('evict requires Linux posix_fadvise; on macOS explicitly choose macos-purge')
            for path in paths:
                with path.open('rb') as f:
                    os.fsync(f.fileno())
                    os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        else:
            if sys.platform != 'darwin':
                raise RuntimeError('macos-purge requires macOS')
            # Noninteractive sudo: authenticate separately; never run the benchmark as root.
            label = Path(command[0]).name
            print(f'[cache] {label}: clearing system-wide disk cache (macos-purge)...',
                  file=sys.stderr, flush=True)
            purge_started = time.perf_counter()
            try:
                subprocess.run(['/usr/bin/sudo', '-n', '/usr/sbin/purge'], check=True)
            except (OSError, subprocess.CalledProcessError):
                print(f'[cache] {label}: purge FAILED; benchmark will not start.',
                      file=sys.stderr, flush=True)
                raise
            print(f'[cache] {label}: purge succeeded in '
                  f'{time.perf_counter() - purge_started:.3f}s; starting benchmark '
                  '(purge excluded from benchmark timing).', file=sys.stderr, flush=True)
    return {'input_cache_mode': mode, 'cache_prepare_seconds': time.perf_counter() - started,
            'cache_input_files': [str(p) for p in paths],
            'cache_eviction_verified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache', choices=MODES, default='normal')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command[:1] == ['--']:
        command = command[1:]
    if not command:
        parser.error('supply a Go or Rust attribution command after --')
    print(json.dumps(prepare_input_cache(command, args.cache)), flush=True)
    return subprocess.call(command)


if __name__ == '__main__':
    sys.exit(main())
