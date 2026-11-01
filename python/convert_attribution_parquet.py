#!/usr/bin/env python3
"""Stream attribution CSVs to Parquet, replacing 1,000 IDs in each portfolio.

Weights and returns are preserved; this changes security identity/country exposure,
not pricing or FX simulation. The index and security master are copied unchanged.
"""
import argparse
from collections import Counter
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq


def batches(path):
    types = ({'InstrumentId': pa.string(), 'Currency': pa.string(), 'Country': pa.string()}
             if path.stem == 'security_master' else {
                 'Entity': pa.string(), 'EffectiveDate': pa.date32(),
                 'InstrumentId': pa.string(), 'Weight': pa.float64(),
                 'BaseReturn': pa.float64(), 'LocalReturn': pa.float64()})
    return csv.open_csv(path, read_options=csv.ReadOptions(block_size=32 * 1024 * 1024),
                        convert_options=csv.ConvertOptions(column_types=types))


def transform(batch, old_ids, new_ids):
    if old_ids is None:
        return batch
    column = batch.schema.get_field_index('InstrumentId')
    ids = batch.column(column)
    positions = pc.index_in(ids, value_set=old_ids)
    replacement = pc.take(new_ids, positions)
    return batch.set_column(column, 'InstrumentId', pc.coalesce(replacement, ids))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, default=Path('attribution_data'))
    parser.add_argument('--output-dir', type=Path, default=Path('attribution_data_parquet'))
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    names = ['security_master', 'portfolio_PF1', 'portfolio_PF2', 'portfolio_PF3', 'portfolio_PF4', 'index']
    for name in names:
        if (args.output_dir / f'{name}.parquet').exists():
            raise FileExistsError(f'Refusing to overwrite {name}.parquet')
    master = pa.Table.from_batches(list(batches(args.input_dir / 'security_master.csv')))
    eur = master.filter(pc.equal(master['Currency'], 'EUR'))
    eur_ids = eur['InstrumentId'].combine_chunks()
    if len(eur_ids) != 1000 or len(set(eur_ids.to_pylist())) != 1000:
        raise ValueError('Expected exactly 1,000 unique EUR instruments in the master')
    master_ids = set(master['InstrumentId'].to_pylist())
    manifest = {'source': str(args.input_dir), 'compression': 'zstd',
                'semantics': 'Replace IDs only; preserve weights and returns. Index unchanged.',
                'files': {}}
    for name in names:
        source = args.input_dir / f'{name}.csv'
        target = args.output_dir / f'{name}.parquet'
        old_ids = None
        mapping = {}
        if name.startswith('portfolio_PF'):
            number = int(name[-1])
            old = [f'S{i:06d}' for i in range(number * 15000 - 999, number * 15000 + 1)]
            if not set(old) <= master_ids:
                raise ValueError('Replacement source IDs missing from master')
            old_ids = pa.array(old)
            mapping = dict(zip(old, eur_ids.to_pylist()))
        rows = 0
        dates, eur_dates, replaced = Counter(), Counter(), Counter()
        with batches(source) as reader:
            with pq.ParquetWriter(target, reader.schema, compression='zstd') as writer:
                for batch in reader:
                    if old_ids is not None:
                        for entry in pc.value_counts(batch['InstrumentId']).to_pylist():
                            if entry['values'] in mapping:
                                replaced[entry['values']] += entry['counts']
                    converted = transform(batch, old_ids, eur_ids)
                    writer.write_batch(converted)
                    rows += len(converted)
                    if name.startswith('portfolio') or name == 'index':
                        for entry in pc.value_counts(converted['EffectiveDate']).to_pylist():
                            dates[str(entry['values'])] += entry['counts']
                        if old_ids is not None:
                            eur_mask = pc.is_in(converted['InstrumentId'], value_set=eur_ids)
                            for entry in pc.value_counts(pc.filter(converted['EffectiveDate'], eur_mask)).to_pylist():
                                eur_dates[str(entry['values'])] += entry['counts']
        # Each CSV batch becomes one Parquet row group. Compare every value after
        # reloading, including the preserved numeric columns and changed IDs.
        parquet = pq.ParquetFile(target)
        with batches(source) as reader:
            groups = 0
            for groups, batch in enumerate(reader, 1):
                expected = pa.Table.from_batches([transform(batch, old_ids, eur_ids)])
                if not parquet.read_row_group(groups - 1).equals(expected):
                    raise AssertionError(f'Round-trip mismatch: {name}, group {groups}')
            assert groups == parquet.num_row_groups
        assert parquet.metadata.num_rows == rows
        if dates:
            expected_daily = 100000 if name == 'index' else 15000
            assert len(dates) == 1095 and set(dates.values()) == {expected_daily}
        if old_ids is not None:
            assert set(replaced) == set(mapping) and set(replaced.values()) == {1095}
            assert set(eur_dates) == set(dates) and set(eur_dates.values()) == {1000}
        manifest['files'][target.name] = {
            'rows': rows, 'bytes': target.stat().st_size,
            'verified': 'Every value compared against transformed source CSV',
            'dates': len(dates), 'eur_holdings_per_date': 1000 if mapping else 0,
            'replacement_mapping': mapping,
        }
        print(f'{target}: {rows:,} rows, {target.stat().st_size / 1024**2:.1f} MiB; verified', flush=True)
    (args.output_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
