#!/usr/bin/env python3
"""Merge four fixed-holding portfolios; share only DE securities with the index.

Streams one date at a time. Sources are unchanged. Each source portfolio gets
25% of the combined portfolio; duplicate EUR IDs use weighted average returns.
"""
import argparse
from contextlib import ExitStack
from itertools import zip_longest
import json
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def require(condition, message):
    if not condition:
        raise ValueError(message)


def day_batches(path, rows):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=rows):
        table = pa.Table.from_batches([batch])
        require(len(table) == rows and len(pc.unique(table['EffectiveDate'])) == 1,
                f'Expected {rows} rows per date in {path}')
        yield table


def remap(table, old, new):
    ids = table['InstrumentId']
    replacement = pc.take(new, pc.index_in(ids, value_set=old))
    return table.set_column(table.schema.get_field_index('InstrumentId'),
                            'InstrumentId', pc.coalesce(replacement, ids))


def total(table, name):
    return pc.sum(pc.multiply(table['Weight'], table[name])).as_py()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--input-dir', type=Path, default=Path('attribution_data_parquet'))
    ap.add_argument('--output-dir', type=Path, default=Path('attribution_data_parquet_merged'))
    args = ap.parse_args()
    require(not args.output_dir.exists(), 'Choose a new output directory')
    started = perf_counter()
    master = pq.read_table(args.input_dir / 'security_master.parquet')
    ids = master['InstrumentId'].to_pylist()
    currencies = dict(zip(ids, master['Currency'].to_pylist()))
    countries = dict(zip(ids, master['Country'].to_pylist()))
    de = {sid for sid, country in countries.items() if country == 'DE'}
    require(len(de) == 1000, 'Expected 1,000 DE securities')
    require(all(currencies[sid] == 'EUR' for sid in de), 'Expected EUR currency for DE securities')
    streams = [day_batches(args.input_dir / f'portfolio_PF{pf}.parquet', 15000) for pf in range(1, 5)]
    streams.append(day_batches(args.input_dir / 'index.parquet', 100000))
    args.output_dir.mkdir()
    pq.write_table(master, args.output_dir / 'security_master.parquet', compression='zstd')
    expected = []
    previous = None
    with ExitStack() as stack:
        for number, tables in enumerate(zip_longest(*streams), 1):
            require(all(t is not None for t in tables), 'Input date counts differ')
            dates = [t['EffectiveDate'][0].as_py() for t in tables]
            require(len(set(dates)) == 1 and (previous is None or dates[0] > previous),
                    'Input dates differ or do not strictly increase')
            previous = dates[0]
            holdings = [set(t['InstrumentId'].to_pylist()) for t in tables]
            require(all(len(s) == len(t) for s, t in zip(holdings, tables)), 'Duplicate input holding')
            if number == 1:
                source_holdings = holdings
                portfolio_us = sorted(set.union(*holdings[:4]) - de)
                require(len(portfolio_us) == 56000, 'Expected 56,000 distinct portfolio US holdings')
                require(all((s & de) == de for s in holdings[:4]), 'Each portfolio must hold all EUR IDs')
                available = sorted(sid for sid in ids if currencies[sid] == 'USD' and sid not in holdings[4])
                require(len(available) >= len(portfolio_us), 'Not enough non-index US IDs in master')
                old, new = pa.array(portfolio_us), pa.array(available[:len(portfolio_us)])
                require(all(currencies[sid] == 'USD' for sid in portfolio_us), 'Unexpected portfolio currency')
                # Keep the 100,000-security index size. Its last 1,000 US IDs
                # become the EUR IDs that must overlap the merged portfolio.
                require(not (holdings[4] & de), 'Expected source index without EUR holdings')
                index_old = pa.array(sorted(holdings[4])[-1000:])
                require(all(currencies[sid] == 'USD' for sid in index_old.to_pylist()), 'Expected US index replacements')
                index_new = pa.array(sorted(de))
                merged_ids = set(new.to_pylist()) | de
                index_ids = (holdings[4] - set(index_old.to_pylist())) | de
                require(merged_ids & index_ids == de, 'Unexpected portfolio/index overlap')
                mappings = pa.table({
                    'Scope': ['portfolio'] * len(old) + ['index'] * len(index_old),
                    'SourceInstrumentId': old.to_pylist() + index_old.to_pylist(),
                    'InstrumentId': new.to_pylist() + index_new.to_pylist(),
                })
                pq.write_table(mappings, args.output_dir / 'instrument_remapping.parquet', compression='zstd')
            require(holdings == source_holdings, 'Source holdings change across dates')
            inputs = []
            wanted = [0., 0.]
            for table in tables[:4]:
                weight = table['Weight']
                denominator = pc.sum(weight).as_py()
                require(abs(denominator - 1) < 1e-6, 'Source portfolio weights do not sum to one')
                weight = pc.divide(weight, 4 * denominator)
                mapped = remap(table, old, new)
                contributions = [pc.multiply(weight, table[col]) for col in ['BaseReturn', 'LocalReturn']]
                for i, values in enumerate(contributions):
                    wanted[i] += pc.sum(values).as_py()
                inputs.append(pa.table({'InstrumentId': mapped['InstrumentId'], 'Weight': weight,
                                        'BaseContribution': contributions[0], 'LocalContribution': contributions[1]}))
            grouped = pa.concat_tables(inputs).group_by('InstrumentId', use_threads=False).aggregate([
                ('Weight', 'sum'), ('BaseContribution', 'sum'), ('LocalContribution', 'sum')])
            weights = grouped['Weight_sum']
            require(pc.min(weights).as_py() > 0, 'Cannot average returns for a zero-weight group')
            merged = pa.table({
                'Entity': pa.repeat('PF_MERGED', len(grouped)),
                'EffectiveDate': pa.repeat(pa.scalar(dates[0], type=pa.date32()), len(grouped)),
                'InstrumentId': grouped['InstrumentId'], 'Weight': weights,
                'BaseReturn': pc.divide(grouped['BaseContribution_sum'], weights),
                'LocalReturn': pc.divide(grouped['LocalContribution_sum'], weights),
            })
            index = remap(tables[4], index_old, index_new)
            require(abs(pc.sum(merged['Weight']).as_py() - 1) < 1e-12, 'Merged weights do not sum to one')
            require(all(abs(total(merged, col) - wanted[i]) < 1e-12
                        for i, col in enumerate(['BaseReturn', 'LocalReturn'])), 'Merged return changed')
            if number == 1:
                pw = stack.enter_context(pq.ParquetWriter(args.output_dir / 'portfolio_PF.parquet', merged.schema, compression='zstd'))
                iw = stack.enter_context(pq.ParquetWriter(args.output_dir / 'index.parquet', index.schema, compression='zstd'))
            pw.write_table(merged)
            iw.write_table(index)
            expected.append((dates[0], wanted, [total(index, col) for col in ['BaseReturn', 'LocalReturn']]))
            if number % 100 == 0:
                print(f'Merged {number} dates', flush=True)
    require(len(expected) == 1095, 'Expected 1,095 dates')
    # Reload every output date to verify persisted holdings, overlap, weights and
    # weighted returns, using the source-derived values collected above.
    out_streams = [day_batches(args.output_dir / 'portfolio_PF.parquet', 57000),
                   day_batches(args.output_dir / 'index.parquet', 100000)]
    verified = 0
    for p, b, checks in zip_longest(*out_streams, expected):
        require(p is not None and b is not None and checks is not None, 'Output date counts differ')
        day, portfolio_returns, benchmark_returns = checks
        require(p['EffectiveDate'][0].as_py() == b['EffectiveDate'][0].as_py() == day, 'Persisted date mismatch')
        pids, bids = set(p['InstrumentId'].to_pylist()), set(b['InstrumentId'].to_pylist())
        require(pids == merged_ids and bids == index_ids and pids & bids == de, 'Persisted holdings/overlap mismatch')
        require(abs(pc.sum(p['Weight']).as_py() - 1) < 1e-12, 'Persisted portfolio weights invalid')
        require(abs(pc.sum(b['Weight']).as_py() - 1) < 1e-6, 'Persisted index weights invalid')
        for table, returns in [(p, portfolio_returns), (b, benchmark_returns)]:
            for i, col in enumerate(['BaseReturn', 'LocalReturn']):
                require(abs(total(table, col) - returns[i]) < 1e-12, 'Persisted weighted return mismatch')
        verified += 1
    manifest = {
        'source': str(args.input_dir), 'dates': verified,
        'portfolio': {'entity': 'PF_MERGED', 'holdings_per_date': 57000, 'USD': 56000, 'EUR': 1000,
                      'rows': 57000 * verified, 'source_allocations': {f'PF{i}': 0.25 for i in range(1, 5)}},
        'index': {'holdings_per_date': 100000, 'USD': 99000, 'EUR': 1000, 'rows': 100000 * verified},
        'shared_ids_per_date': 1000, 'shared_currency': 'EUR', 'shared_country': 'DE',
        'merge_rule': 'Normalize each source daily, allocate 25% each, sum duplicate weights and weight-average base/local returns',
        'index_rule': 'Replace last 1,000 source index US IDs with EUR IDs; preserve weights and returns',
        'remapping_file': 'instrument_remapping.parquet',
        'verification': 'Reloaded all dates; exact ID sets, DE-only intersection, weights and source-derived weighted returns checked',
        'elapsed_seconds': perf_counter() - started,
        'files': {p.name: {'bytes': p.stat().st_size, 'rows': pq.ParquetFile(p).metadata.num_rows}
                  for p in args.output_dir.glob('*.parquet')},
    }
    (args.output_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(f'Verified {verified} dates: 57,000 portfolio holdings, 100,000 index holdings, exactly 1,000 shared DE IDs', flush=True)


if __name__ == '__main__':
    main()
