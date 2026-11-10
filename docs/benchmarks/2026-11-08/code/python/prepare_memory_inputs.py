"""Generate valid six-input fixtures with independently scalable daily dimensions."""
import argparse
import datetime as dt
import json
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def generate(output, securities=1000, accounts=1, days=12, account_history_days=0,
             analytics_securities=None, master_securities=None):
    analytics_securities = securities if analytics_securities is None else analytics_securities
    master_securities = securities if master_securities is None else master_securities
    if min(securities, accounts, days, analytics_securities, master_securities) < 1 or account_history_days < 0:
        raise ValueError('counts must be positive; history must be nonnegative')
    if analytics_securities < master_securities or master_securities < securities:
        raise ValueError('require analytics >= master >= portfolio securities')
    output.mkdir(parents=True, exist_ok=False)
    start = dt.date(2023, 1, 1)
    dates = [start + dt.timedelta(days=i) for i in range(days)]
    schemas = {
        'accounts': pa.schema([('AccountId', pa.string()), ('EffectiveDate', pa.date32()),
                              ('PortfolioEntity', pa.string()), ('BaseCurrency', pa.string()), ('IsActive', pa.bool_())]),
        'forex': pa.schema([('EffectiveDate', pa.date32()), ('FromCurrency', pa.string()),
                           ('ToCurrency', pa.string()), ('Rate', pa.float64())]),
        'security_master': pa.schema([('InstrumentId', pa.string()), ('EffectiveDate', pa.date32()),
                                     ('Currency', pa.string()), ('Country', pa.string()), ('AnalyticsSecId', pa.int64())]),
        'analytics': pa.schema([('SecId', pa.int64()), ('Effective Date', pa.date32())]
                               + [(f'Float{i:02}', pa.float64()) for i in range(1, 16)]),
        'returns': pa.schema([('Entity', pa.string()), ('EffectiveDate', pa.date32()),
                             ('InstrumentId', pa.string()), ('Weight', pa.float64()),
                             ('BaseReturn', pa.float64()), ('LocalReturn', pa.float64())]),
    }
    def write(writer, schema, columns):
        writer.write_table(pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(columns, schema)], schema=schema), row_group_size=32768)
    with pq.ParquetWriter(output / 'accounts.parquet', schemas['accounts']) as w:
        for offset in range(-account_history_days, days):
            date = start + dt.timedelta(days=offset)
            for lo in range(0, accounts, 32768):
                hi = min(lo + 32768, accounts); n = hi-lo
                write(w, schemas['accounts'], [[f'A{i}' for i in range(lo, hi)], [date]*n,
                      ['PF' if i == 0 else f'OTHER{i}' for i in range(lo, hi)], ['USD']*n, [True]*n])
    with pq.ParquetWriter(output / 'forex.parquet', schemas['forex']) as w:
        for date in dates:
            write(w, schemas['forex'], [[date], ['USD'], ['USD'], [1.]])
    with pq.ParquetWriter(output / 'security_master.parquet', schemas['security_master']) as w:
        for lo in range(0, master_securities, 32768):
            hi = min(lo+32768, master_securities); n = hi-lo
            write(w, schemas['security_master'], [[f'S{i:09}' for i in range(lo, hi)], [start]*n,
                  ['USD']*n, ['US']*n, np.arange(lo+1, hi+1, dtype=np.int64)])
    with pq.ParquetWriter(output / 'analytics.parquet', schemas['analytics']) as w:
        for date in dates:
            for lo in range(0, analytics_securities, 32768):
                hi = min(lo+32768, analytics_securities); n = hi-lo
                ids = np.arange(lo+1, hi+1, dtype=np.int64)
                write(w, schemas['analytics'], [ids, [date]*n] + [ids*1e-6+i*.001 for i in range(1,16)])
    for stem, entity, shift in [('portfolio_PF', 'PF', .001), ('index', 'INDEX', 0.)]:
        with pq.ParquetWriter(output / f'{stem}.parquet', schemas['returns']) as w:
            for date in dates:
                for lo in range(0, securities, 32768):
                    hi = min(lo+32768, securities); n = hi-lo
                    values = (np.arange(lo,hi)%11-5)*.0001+shift
                    write(w, schemas['returns'], [[entity]*n, [date]*n, [f'S{i:09}' for i in range(lo,hi)],
                          np.full(n,1./securities), values, values])
    manifest = dict(securities=securities, accounts_per_date=accounts, days=days,
                    account_history_days=account_history_days, analytics_securities=analytics_securities,
                    master_securities=master_securities, start_date=str(start), end_date=str(dates[-1]))
    (output/'memory-manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--securities', type=int, default=1000)
    p.add_argument('--accounts', type=int, default=1)
    p.add_argument('--days', type=int, default=12)
    p.add_argument('--account-history-days', type=int, default=0)
    p.add_argument('--analytics-securities', type=int)
    p.add_argument('--master-securities', type=int)
    a = p.parse_args()
    print(json.dumps(generate(a.output, a.securities, a.accounts, a.days, a.account_history_days,
                              a.analytics_securities, a.master_securities), indent=2))

if __name__ == '__main__':
    main()
