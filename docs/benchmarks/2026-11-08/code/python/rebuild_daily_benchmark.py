"""Rebuild the daily benchmark from retained canonical inputs into a fresh directory."""
import argparse
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True,help='Fresh destination; may require over 50 GiB')
    parser.add_argument('--variant',choices=['daily','fixed-sector','dated-sector','all'],default='dated-sector')
    args=parser.parse_args();out=args.output.resolve()
    if out.exists():parser.error('output must be a fresh directory')
    for path in [ROOT/'attribution_data_parquet/portfolio_PF.parquet',ROOT/'attribution_data_parquet/index.parquet',ROOT/'docs/benchmarks/2026-11-08/source/security_master_history.parquet']:
        if not path.is_file():parser.error(f'missing retained source: {path}')
    def run(script,*values):
        subprocess.run([sys.executable,str(ROOT/'python'/script),*map(str,values)],check=True,cwd=ROOT)
    run('benchmark_large_parquet.py','generate','--path',out/'analytics/securities.parquet')
    run('prepare_six_inputs.py','--source',ROOT/'attribution_data_parquet',
        '--history',ROOT/'docs/benchmarks/2026-11-08/source/security_master_history.parquet',
        '--analytics',out/'analytics/securities.parquet','--output',out/'six-input')
    run('prepare_monthly_smf.py','--source',out/'six-input','--output',out/'monthly-history')
    if args.variant in ('daily','fixed-sector','all'):
        run('prepare_daily_smf.py','--source',out/'monthly-history','--output',out/'daily-snapshots')
    if args.variant in ('fixed-sector','all'):
        run('prepare_sector_smf.py','--source',out/'daily-snapshots','--output',out/'fixed-sector-snapshots')
    if args.variant in ('dated-sector','all'):
        run('prepare_sector_smf.py','--source',out/'monthly-history','--output',out/'dated-sector-history','--effective-dated')
        run('prepare_daily_smf.py','--source',out/'dated-sector-history','--output',out/'dated-sector-snapshots')
        run('validate_dated_sector_smf.py','--history',out/'dated-sector-history','--snapshots',out/'dated-sector-snapshots')
    print(f'Rebuilt {args.variant} in {out}. Purge after preparation/validation before cold-cache timing.')


if __name__=='__main__':main()
