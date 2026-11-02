"""Hand-calculated regression: security effects must precede country reduction."""
import csv
import math
from pathlib import Path
import tempfile

import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from test_country_parquet_parallel import execute, BINARIES
from test_attribution import records


def main():
    with tempfile.TemporaryDirectory(prefix='security-country-') as tmp:
        root = Path(tmp)
        data = root / 'csv'
        data.mkdir()
        (data / 'security_master.csv').write_text(
            'InstrumentId,EffectiveDate,Currency,Country\n'
            'A,2023-01-01,USD,US\nA,2023-02-01,EUR,DE\n'
            'B,2023-01-01,USD,US\nC,2023-01-01,USD,US\n')
        for name, entity, holdings in [
            ('index', 'INDEX', [('A', .5, .02, .03), ('C', .5, .04, .01)]),
            ('portfolio_PF', 'PF', [('B', .4, .08, -.02), ('A', .6, .10, .05)])]:
            with (data / f'{name}.csv').open('w', newline='') as f:
                w = csv.writer(f)
                w.writerow(['Entity', 'EffectiveDate', 'InstrumentId', 'Weight', 'BaseReturn', 'LocalReturn'])
                for date in ['2023-01-31', '2023-02-01']:
                    w.writerows((entity, date, *row) for row in holdings)
        parquet = root / 'parquet'
        parquet.mkdir()
        for path in data.glob('*.csv'):
            pq.write_table(pacsv.read_csv(path), parquet / path.with_suffix('.parquet').name,
                           row_group_size=2, compression='zstd')
        # Jan: shared A, portfolio-only B, benchmark-only C. Feb: A moves to DE.
        expected = {('2023-01-31', 'base'): (.014, .040, .008, .092, .030),
                    ('2023-01-31', 'local'): (-.010, .010, .002, .022, .020),
                    ('2023-02-01', 'base'): (.040, 0., 0., .080, .040),
                    ('2023-02-01', 'local'): (-.030, 0., 0., -.020, .010)}
        for lang, binary in BINARIES.items():
            for folder in [data, parquet]:
                for threads in [1, 4]:
                    out = root / f'{lang}-{folder.name}-{threads}'
                    m = execute(binary, folder, out, threads, limit=2)
                    assert int(m['rows_read']) == 8
                    assert int(m['rows_retained']) == 6
                    assert int(m['effect_values']) == 30  # (3+2) securities × 2 returns × 3 effects.
                    path = next(out.glob(f'country-portfolio_PF.{folder.name}'))
                    rows = list(records(path))
                    assert len(rows) == 4
                    for row in rows:
                        want = expected[(str(row['PeriodStart']), row['ReturnType'])]
                        for field, value in zip(['Allocation', 'Selection', 'Interaction', 'PortfolioReturn', 'BenchmarkReturn'], want):
                            assert math.isclose(float(row[field]), value, abs_tol=1e-12), (lang, field, row, want)
                        assert math.isclose(float(row['Total']), want[3]-want[4], abs_tol=1e-12)
        print('PASS: hand-calculated security-first country effects, unmatched IDs, reordered holdings and country changes; both engines, CSV/Parquet, streaming/parallel')


if __name__ == '__main__':
    main()
