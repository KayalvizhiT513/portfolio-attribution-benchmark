"""Prepare six date-aligned Parquet inputs without copying the 42 GiB analytics.

AnalyticsSecId is an explicit synthetic alias, not an economic security identity.
Historical SMF versions are complete monthly snapshots (validated below).
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def prepare(source, history, analytics, output):
    output.mkdir(parents=True, exist_ok=True)
    targets = [output / f'{s}.parquet' for s in
               ('accounts', 'forex', 'security_master', 'analytics', 'portfolio_PF', 'index')]
    if any(p.exists() or p.is_symlink() for p in targets):
        raise ValueError('use a fresh output directory; existing inputs will not be overwritten')
    pf = pq.ParquetFile(source / 'portfolio_PF.parquet')
    ix = pq.ParquetFile(source / 'index.parquet')

    def dates(file, column):
        i = file.schema_arrow.get_field_index(column)
        result = []
        for g in range(file.num_row_groups):
            s = file.metadata.row_group(g).column(i).statistics
            if s is None or not s.has_min_max or s.min != s.max:
                raise ValueError('inputs require single-date row groups')
            if not result or result[-1] != s.min:
                result.append(s.min)
        if result != sorted(set(result)):
            raise ValueError('dates must increase')
        return result

    days = dates(pf, 'EffectiveDate')
    if days != dates(ix, 'EffectiveDate'):
        raise ValueError('portfolio/index date mismatch')
    af = pq.ParquetFile(analytics)
    analytics_days = dates(af, 'Effective Date')
    return_days = days
    days = [d for d in days if analytics_days[0] <= d <= analytics_days[-1]]
    if not days or not set(days) <= set(analytics_days):
        raise ValueError('analytics has no overlap or has missing dates inside overlap')
    # The reusable fixture has exactly IDs 1..100000 on every date.
    first_analytics = af.read_row_group(0, columns=['SecId'])['SecId'].to_numpy()
    if not np.array_equal(first_analytics, np.arange(1, 100001)):
        raise ValueError('unexpected analytics ID universe')
    entity_values = pc.unique(pf.read_row_group(0, columns=['Entity'])['Entity']).to_pylist()
    if len(entity_values) != 1:
        raise ValueError('expected one portfolio entity')
    entity = entity_values[0]

    accounts_schema = pa.schema([
        ('AccountId', pa.string()), ('EffectiveDate', pa.date32()),
        ('PortfolioEntity', pa.string()), ('BaseCurrency', pa.string()), ('IsActive', pa.bool_())])
    forex_schema = pa.schema([
        ('EffectiveDate', pa.date32()), ('FromCurrency', pa.string()),
        ('ToCurrency', pa.string()), ('Rate', pa.float64())])
    with pq.ParquetWriter(targets[0], accounts_schema, compression='zstd') as aw, \
            pq.ParquetWriter(targets[1], forex_schema, compression='zstd') as fw:
        for n, day in enumerate(days):
            aw.write_table(pa.Table.from_pylist([dict(
                AccountId='ACCOUNT_PF', EffectiveDate=day, PortfolioEntity=entity,
                BaseCurrency='USD', IsActive=True)], schema=accounts_schema))
            # Reproducible synthetic spot quotes, not used to rewrite existing returns.
            eur_usd = 1.08 + 0.04 * np.sin(n / 37.0)
            quotes = [('USD', 'USD', 1.0), ('EUR', 'USD', eur_usd),
                      ('USD', 'EUR', 1 / eur_usd), ('EUR', 'EUR', 1.0)]
            fw.write_table(pa.Table.from_pylist([dict(
                EffectiveDate=day, FromCurrency=a, ToCurrency=b, Rate=r)
                for a, b, r in quotes], schema=forex_schema))

    master = pq.read_table(history)
    required = ['InstrumentId', 'EffectiveDate', 'Currency', 'Country']
    if any(c not in master.column_names for c in required):
        raise ValueError('historical master must have effective dates')
    master = master.select(required)
    if master['EffectiveDate'].type != pa.date32():
        master = master.set_column(1, 'EffectiveDate', pc.cast(master['EffectiveDate'], pa.date32()))
    master = master.sort_by([('EffectiveDate', 'ascending'), ('InstrumentId', 'ascending')])
    versions = pc.unique(master['EffectiveDate']).to_pylist()
    if not versions or versions[0] > days[0]:
        raise ValueError('historical master starts after returns')
    schema = master.schema.append(pa.field('AnalyticsSecId', pa.int64()))
    universe = set(pq.read_table(source / 'security_master.parquet', columns=['InstrumentId'])
                   ['InstrumentId'].to_pylist())
    with pq.ParquetWriter(targets[2], schema, compression='zstd') as writer:
        for day in versions:
            snapshot = master.filter(pc.equal(master['EffectiveDate'], pa.scalar(day, pa.date32())))
            ids = snapshot['InstrumentId'].to_pylist()
            if len(ids) != len(set(ids)) or set(ids) != universe:
                raise ValueError('each SMF version must be a complete unique snapshot')
            aliases = [((int(s.removeprefix('S')) - 1) % 100000) + 1 for s in ids]
            writer.write_table(snapshot.append_column('AnalyticsSecId', pa.array(aliases, pa.int64())))

    for src, dst in [(analytics, targets[3]), (source / 'portfolio_PF.parquet', targets[4]),
                     (source / 'index.parquet', targets[5])]:
        dst.symlink_to(src.resolve())
    manifest = dict(
        dates=len(days), start=str(days[0]), end=str(days[-1]),
        excluded_return_dates=[str(d) for d in return_days if d not in set(days)],
        historical_smf=str(history.resolve()), smf_snapshots=len(versions),
        analytics=str(analytics.resolve()), analytics_dates=len(dates(af, 'Effective Date')),
        analytics_mapping='AnalyticsSecId = ((numeric suffix of InstrumentId - 1) % 100000) + 1',
        mapping_semantics='Synthetic many-to-one aliases; distinct holdings can share analytics values.',
        accounts='One active USD account per date for the existing portfolio entity.',
        forex='Daily synthetic spot quotes; Rate = ToCurrency units per FromCurrency unit.',
        calculation='Existing BaseReturn/LocalReturn retained; analytics and FX joined and validated, not used to rewrite returns.',
        schemas={p.name: str(pq.ParquetFile(p).schema_arrow) for p in targets})
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({k: v for k, v in manifest.items() if k != 'schemas'}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=Path('attribution_data_parquet'))
    parser.add_argument('--history', type=Path, default=Path(
        'results/country-security-first-v2-2026-11-02/inputs/history/parquet/security_master.parquet'))
    parser.add_argument('--analytics', type=Path, default=Path('results/large_parquet/securities.parquet'))
    parser.add_argument('--output', type=Path, default=Path('results/six-input/inputs'))
    args = parser.parse_args()
    prepare(args.source, args.history, args.analytics, args.output)
