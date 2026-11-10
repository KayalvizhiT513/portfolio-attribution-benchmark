#!/usr/bin/env python3
"""Expand the current portfolio from 56k to 59k US IDs, preserving daily returns.

Update CSV and Parquet in place after verifying staged files. Each of 3,000
existing US rows is split equally between its old ID and an unused US ID.
"""
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq

from export_attribution_csvs import verify


def require(ok, message):
    if not ok:
        raise ValueError(message)


def returns(table):
    return [pc.sum(pc.multiply(table['Weight'], table[name])).as_py()
            for name in ['BaseReturn', 'LocalReturn']]


def main():
    root = Path('attribution_data_parquet')
    source = root / 'portfolio_PF.parquet'
    target = source.with_suffix('.parquet.partial')
    csv_target = Path('attribution_data/portfolio_PF.csv')
    csv_partial = csv_target.with_suffix('.csv.partial')
    master = pq.read_table(root / 'security_master.parquet').to_pydict()
    countries = dict(zip(master['InstrumentId'], master['Country']))
    us = {sid for sid, country in countries.items() if country == 'US'}
    de = {sid for sid, country in countries.items() if country == 'DE'}
    index = next(pq.ParquetFile(root / 'index.parquet').iter_batches(batch_size=100000))
    index_ids = set(index['InstrumentId'].to_pylist())
    reader = pq.ParquetFile(source)
    require(reader.metadata.num_rows == 57000 * 1095 and reader.num_row_groups == 1095,
            'Expected the existing 57,000-holding, 1,095-date portfolio')
    checks = []
    with pq.ParquetWriter(target, reader.schema_arrow, compression='zstd') as writer:
        for i in range(reader.num_row_groups):
            table = reader.read_row_group(i)
            ids = table['InstrumentId'].to_pylist()
            if i == 0:
                old_ids = set(ids)
                require(len(old_ids & us) == 56000 and old_ids & de == de, 'Unexpected source holdings')
                donor_ids = pa.array(ids[:3000])
                require(set(donor_ids.to_pylist()) <= us, 'Donor rows must all be US')
                added_ids = pa.array(sorted(us - old_ids - index_ids)[-3000:])
                require(len(added_ids) == 3000, 'Not enough unused US IDs')
                wanted_ids = old_ids | set(added_ids.to_pylist())
                require(wanted_ids & index_ids == de and len(wanted_ids & us) == 59000,
                        'Expanded holdings must share exactly DE IDs with index')
            require(len(table) == 57000 and set(ids) == old_ids
                    and ids[:3000] == donor_ids.to_pylist(), 'Source holdings/order changed')
            require(len(pc.unique(table['EffectiveDate'])) == 1, 'Expected one date per row group')
            weights = pc.if_else(pc.is_in(table['InstrumentId'], value_set=donor_ids),
                                 pc.multiply(table['Weight'], 0.5), table['Weight'])
            adjusted = table.set_column(table.schema.get_field_index('Weight'), 'Weight', weights)
            added = adjusted.slice(0, 3000).set_column(table.schema.get_field_index('InstrumentId'),
                                                     'InstrumentId', added_ids)
            expanded = pa.concat_tables([adjusted, added])
            expected_returns = returns(table)
            require(abs(pc.sum(expanded['Weight']).as_py() - 1) < 1e-12, 'Invalid expanded weights')
            require(all(abs(a - b) < 1e-12 for a, b in zip(returns(expanded), expected_returns)),
                    'Expanded daily returns changed')
            checks.append((table['EffectiveDate'][0].as_py(), expected_returns))
            writer.write_table(expanded)
    persisted = pq.ParquetFile(target)
    require(persisted.num_row_groups == 1095 and persisted.metadata.num_rows == 60000 * 1095,
            'Expanded row count mismatch')
    for i, (day, expected_returns) in enumerate(checks):
        table = persisted.read_row_group(i)
        ids = set(table['InstrumentId'].to_pylist())
        require(len(table) == 60000 and ids == wanted_ids and ids & index_ids == de,
                'Persisted holdings mismatch')
        require(pc.unique(table['EffectiveDate']).to_pylist() == [day], 'Persisted date mismatch')
        require(abs(pc.sum(table['Weight']).as_py() - 1) < 1e-12
                and all(abs(a - b) < 1e-12 for a, b in zip(returns(table), expected_returns)),
                'Persisted weights/returns mismatch')
    print('Parquet verified: all 1,095 dates have 59,000 US + 1,000 DE holdings; daily returns preserved', flush=True)
    with csv.CSVWriter(csv_partial, persisted.schema_arrow) as writer:
        for batch in persisted.iter_batches(batch_size=65536):
            writer.write_batch(batch)
    count = verify(target, csv_partial)
    print(f'CSV verified: all {count:,} records match Parquet exactly', flush=True)
    # Record the split provenance; index and master are unchanged.
    splits = pa.table({'SourceInstrumentId': donor_ids, 'AddedInstrumentId': added_ids,
                       'OriginalWeightFraction': [0.5] * 3000, 'AddedWeightFraction': [0.5] * 3000})
    split_path = root / 'expansion_provenance.parquet'
    pq.write_table(splits, split_path, compression='zstd')
    os.replace(target, source)
    os.replace(csv_partial, csv_target)
    for directory, filename in [(root, source.name), (csv_target.parent, csv_target.name)]:
        manifest_path = directory / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['portfolio'].update({'holdings_per_date': 60000, 'USD': 59000, 'EUR': 1000,
                                      'rows': count})
        manifest['files'][filename] = {'bytes': (directory / filename).stat().st_size, 'rows': count}
        manifest['portfolio_expansion'] = {
            'additional_US_ids': 3000,
            'rule': 'Split 3,000 existing US weights equally between original and new US IDs; copy base/local returns',
            'provenance': 'attribution_data_parquet/expansion_provenance.parquet',
            'verification': 'All dates checked for exact holdings, DE-only overlap, normalized weights and preserved daily returns; every CSV value equals Parquet',
        }
        if directory == root:
            manifest['files'][split_path.name] = {'bytes': split_path.stat().st_size, 'rows': 3000}
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    print('Updated CSV and Parquet in place: 60,000 portfolio holdings; 159,000 union IDs; 1,000 shared DE IDs', flush=True)


if __name__ == '__main__':
    main()
