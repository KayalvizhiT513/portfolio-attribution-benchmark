#!/usr/bin/env python3
"""Generate synthetic 3-year portfolio/index returns and a security master.

Install: pip install numpy
Run: python python/generate_attribution_csvs.py --output-dir ./attribution_data

The dataset includes 1,095 calendar dates (including weekends), 1 portfolio of
60,000 instruments, 1 index of 100,000 instruments, and 160,000 unique
security-master identifiers. All returns and weights are synthetic.
The security master has one effective-dated record per security per month
from January 2023 through December 2025 (36 records per security).
"""
import argparse
import csv
from datetime import date, timedelta
from pathlib import Path

import numpy as np

DAYS = 1095
N_SECURITIES = 160_000
US_SECURITIES = 159_000
PORT_HOLDINGS = 60_000
INDEX_HOLDINGS = 100_000
MASTER_DATES = tuple(date(year, month, 1).isoformat()
                     for year in range(2023, 2026) for month in range(1, 13))
HEADER = ['Entity', 'EffectiveDate', 'InstrumentId', 'Weight', 'BaseReturn', 'LocalReturn']


def write_master(out: Path):
    with (out / 'security_master.csv').open('w', newline='', buffering=1024 * 1024) as f:
        w = csv.writer(f)
        w.writerow(['InstrumentId', 'EffectiveDate', 'Currency', 'Country'])
        w.writerows((f'S{i:06d}', effective_date,
                     'USD' if i <= US_SECURITIES else 'EUR',
                     'US' if i <= US_SECURITIES else 'DE')
                    for i in range(1, N_SECURITIES + 1)
                    for effective_date in MASTER_DATES)


def daily_rows(rng, security_ids, entity, day, weight_mode, batch_size):
    """Generate one entity-date in small chunks, without holding year-scale data."""
    n = len(security_ids)
    # Dirichlet-like normalized exponential weights for each date/entity.
    raw = rng.exponential(scale=1.0, size=n)
    weights = raw / raw.sum() if weight_mode == 'variable' else np.full(n, 1.0 / n)
    # Consistent positive correlation between local and base currency returns,
    # with a small synthetic currency component.
    fx = rng.uniform(-0.002, 0.002)
    for start in range(0, n, batch_size):
        end = min(n, start + batch_size)
        local = rng.uniform(-0.03, 0.03, size=end - start)
        base = np.clip(local + fx, -0.03, 0.03)
        yield ((entity, day, f'S{sid:06d}', f'{weights[j]:.12f}',
                f'{base[j-start]:.8f}', f'{local[j-start]:.8f}')
               for j, sid in enumerate(security_ids[start:end], start))


def write_returns(out, name, entity, securities, rng, start_date, batch_size, weight_mode):
    path = out / name
    with path.open('w', newline='', buffering=8 * 1024 * 1024) as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        for d in range(DAYS):
            day = (start_date + timedelta(days=d)).isoformat()
            for rows in daily_rows(rng, securities, entity, day, weight_mode, batch_size):
                writer.writerows(rows)
            if (d + 1) % 100 == 0 or d + 1 == DAYS:
                print(f'  {name}: {d+1}/{DAYS} dates', flush=True)
    return path


def verify(path, expected):
    # Streaming byte-level line count: constant memory, faster than CSV parsing.
    with path.open('rb') as f:
        total_lines = sum(chunk.count(b'\n') for chunk in iter(lambda: f.read(8 * 1024 * 1024), b''))
    actual = total_lines - 1
    if actual != expected:
        raise RuntimeError(f'{path.name}: expected {expected:,}, found {actual:,}')
    print(f'VERIFIED {path.name}: {actual:,} data rows, {path.stat().st_size / (1024**3):.3f} GiB')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=Path('attribution_data'))
    parser.add_argument('--seed', type=int, default=20261030)
    parser.add_argument('--batch-size', type=int, default=10_000)
    parser.add_argument('--weights', choices=('variable', 'equal'), default='variable')
    parser.add_argument('--verify', action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error('--batch-size must be positive')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    first_day = date(2023, 1, 1)
    print('Generating security master...', flush=True)
    master = args.output_dir / 'security_master.csv'
    write_master(args.output_dir)
    if args.verify:
        verify(master, N_SECURITIES * len(MASTER_DATES))

    # One 60k-instrument portfolio: S000001-S060000.
    # Index overlaps the portfolio, allowing valid attribution joins.
    securities = np.arange(1, PORT_HOLDINGS + 1)
    path = write_returns(args.output_dir, 'portfolio_PF.csv', 'PF',
                         securities, rng, first_day, args.batch_size, args.weights)
    if args.verify:
        verify(path, DAYS * PORT_HOLDINGS)

    path = write_returns(args.output_dir, 'index.csv', 'INDEX',
                         np.arange(1, INDEX_HOLDINGS + 1), rng, first_day,
                         args.batch_size, args.weights)
    if args.verify:
        verify(path, DAYS * INDEX_HOLDINGS)
    print(f'Done. CSV files saved to {args.output_dir.resolve()}', flush=True)


if __name__ == '__main__':
    main()
