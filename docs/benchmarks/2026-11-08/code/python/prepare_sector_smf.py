"""Add deterministic synthetic sectors to an SMF, preserving source row groups."""
import argparse
import json
from pathlib import Path
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SECTORS = ['Communication Services', 'Consumer Discretionary', 'Consumer Staples',
           'Energy', 'Financials', 'Health Care', 'Industrials',
           'Information Technology', 'Materials', 'Real Estate', 'Utilities']


def prepare(source, output, effective_dated=False):
    source = source.resolve()
    output.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    file = pq.ParquetFile(source/'security_master.parquet')
    if 'Sector' in file.schema_arrow.names:
        raise ValueError('source already has Sector')
    if effective_dated and 'SnapshotDate' in file.schema_arrow.names:
        raise ValueError('effective-dated sectors must be assigned to source history before snapshot preparation')
    schema = file.schema_arrow.append(pa.field('Sector', pa.string(), nullable=False))
    rows = 0
    temporary = output/'security_master.parquet.partial'
    with pq.ParquetWriter(temporary, schema, compression='zstd') as writer:
        for group in range(file.num_row_groups):
            table = file.read_row_group(group)
            ids = table['InstrumentId']
            if not pc.all(pc.starts_with(ids, 'S')).as_py():
                raise ValueError('synthetic sector assignment requires S-prefixed numeric IDs')
            numbers = pc.cast(pc.utf8_slice_codeunits(ids, 1), pa.int64()).to_numpy()
            if np.any(numbers <= 0): raise ValueError('security IDs must be positive')
            revisions = 0
            if effective_dated:
                # Each ID changes once per year in its assigned calendar month,
                # on the existing history record's effective day.
                months = pc.year(table['EffectiveDate']).to_numpy()*12 + pc.month(table['EffectiveDate']).to_numpy()-1
                revisions = (months - 2023*12 - (numbers-1)%12)//12 + 1
            sectors = pc.take(pa.array(SECTORS), pa.array((numbers-1+revisions) % len(SECTORS)))
            table = table.append_column(schema.field('Sector'), sectors)
            writer.write_table(table, row_group_size=len(table))
            rows += len(table)
            if (group+1) % 300 == 0:
                print(f'{group+1}/{file.num_row_groups} groups; {rows:,} rows', flush=True)
    temporary.rename(output/'security_master.parquet')
    for name in ('accounts', 'forex', 'analytics', 'portfolio_PF', 'index'):
        (output/f'{name}.parquet').symlink_to((source/f'{name}.parquet').resolve())
    manifest = json.loads((source/'manifest.json').read_text()) if (source/'manifest.json').exists() else {}
    manifest.update(sector_source=str(source), sectors=SECTORS,
                    sector_assignment=('Synthetic: annual reclassification on each security assigned month and existing effective day; SECTORS[(id-1+revision)%11], revision=floor((year*12+month-1-2023*12-(id-1)%12)/12)+1.' if effective_dated else 'Synthetic: SECTORS[(numeric InstrumentId suffix - 1) % 11]; stable across dates.'),
                    sector_effective_dated=effective_dated,
                    sector_preparation_seconds=time.perf_counter()-started,
                    sector_smf_bytes=(output/'security_master.parquet').stat().st_size)
    manifest.setdefault('schemas', {})['security_master.parquet'] = str(schema)
    (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    print(json.dumps({'rows': rows, 'seconds': manifest['sector_preparation_seconds'],
                      'bytes': manifest['sector_smf_bytes'], 'sectors': len(SECTORS)}, indent=2))
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--effective-dated', action='store_true', help='Assign annual sector changes to effective-dated history before resolving snapshots')
    args = parser.parse_args()
    prepare(args.source, args.output, args.effective_dated)
