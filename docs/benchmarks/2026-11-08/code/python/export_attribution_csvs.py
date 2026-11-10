#!/usr/bin/env python3
"""Synchronize attribution CSV inputs from Parquet, verifying every value first."""
import argparse
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq


def verify(source, target):
    parquet = pq.ParquetFile(source)
    expected_batches = iter(parquet.iter_batches(batch_size=65536))
    expected = None
    position = 0
    count = 0
    with csv.open_csv(target, read_options=csv.ReadOptions(block_size=8 * 1024 * 1024),
                      convert_options=csv.ConvertOptions(column_types={f.name: f.type for f in parquet.schema_arrow})) as reader:
        for actual in reader:
            consumed = 0
            while consumed < len(actual):
                if expected is None or position == len(expected):
                    expected = next(expected_batches, None)
                    position = 0
                if expected is None:
                    raise ValueError(f'Extra CSV records in {target}')
                size = min(len(actual) - consumed, len(expected) - position)
                if not actual.slice(consumed, size).equals(expected.slice(position, size)):
                    raise ValueError(f'CSV round-trip mismatch at row {count + consumed} in {target}')
                consumed += size
                position += size
            count += len(actual)
    if ((expected is not None and position != len(expected))
            or next(expected_batches, None) is not None or count != parquet.metadata.num_rows):
        raise ValueError(f'Incomplete CSV output: {target}')
    return count


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--input-dir', type=Path, default=Path('attribution_data_parquet'))
    ap.add_argument('--output-dir', type=Path, default=Path('attribution_data'))
    args = ap.parse_args()
    portfolios = sorted(args.input_dir.glob('portfolio_*.parquet'))
    if not portfolios:
        raise ValueError('No source portfolios')
    sources = [args.input_dir / 'security_master.parquet', *portfolios, args.input_dir / 'index.parquet']
    args.output_dir.mkdir(parents=True, exist_ok=True)
    converted = []
    for source in sources:
        target = args.output_dir / f'{source.stem}.csv'
        partial = target.with_suffix('.csv.partial')
        parquet = pq.ParquetFile(source)
        with csv.CSVWriter(partial, parquet.schema_arrow) as writer:
            for batch in parquet.iter_batches(batch_size=65536):
                writer.write_batch(batch)
        rows = verify(source, partial)
        converted.append((partial, target, rows))
        print(f'{target.name}: {rows:,} rows; every value verified', flush=True)
    # Publish only after every file passes; remove obsolete portfolio inputs so
    # file discovery cannot accidentally include the four previous portfolios.
    for partial, target, _ in converted:
        os.replace(partial, target)
    names = {target.name for _, target, _ in converted}
    for old in args.output_dir.glob('portfolio_*.csv'):
        if old.name not in names:
            old.unlink()
    manifest = {
        'source': str(args.input_dir),
        'verification': 'Every parsed CSV value equals its typed Parquet source value',
        'files': {target.name: {'rows': rows, 'bytes': target.stat().st_size} for _, target, rows in converted},
    }
    source_manifest = args.input_dir / 'manifest.json'
    if source_manifest.exists():
        details = json.loads(source_manifest.read_text())
        for key in ['dates', 'portfolio', 'index', 'shared_ids_per_date', 'shared_country', 'shared_currency']:
            if key in details:
                manifest[key] = details[key]
    (args.output_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(f'Updated {args.output_dir} in place; obsolete portfolio CSVs removed', flush=True)


if __name__ == '__main__':
    main()
