"""Dates without US holdings must not end the stream or produce attribution."""
import csv
from pathlib import Path
import tempfile

import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from test_attribution import records, compare_files
from test_country_parquet_parallel import execute, BINARIES


def main():
    with tempfile.TemporaryDirectory(prefix='country-empty-dates-') as tmp:
        root = Path(tmp)
        data = root / 'csv'
        data.mkdir()
        (data / 'security_master.csv').write_text(
            'InstrumentId,EffectiveDate,Currency,Country\n'
            'A,2025-12-29,USD,IND\nA,2025-12-30,USD,US\n'
            'A,2025-12-31,USD,IND\nB,2025-12-29,USD,IND\n')
        for name, entity, ret in [('index', 'INDEX', .02), ('portfolio_PF', 'PF', .10)]:
            with (data / f'{name}.csv').open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Entity', 'EffectiveDate', 'InstrumentId', 'Weight', 'BaseReturn', 'LocalReturn'])
                for date in ['2025-12-29', '2025-12-30', '2025-12-31']:
                    for sid in ['A', 'B']:
                        writer.writerow([entity, date, sid, .5, ret, ret])
        parquet = root / 'parquet'
        parquet.mkdir()
        for path in data.glob('*.csv'):
            pq.write_table(pacsv.read_csv(path), parquet / path.with_suffix('.parquet').name, row_group_size=2)
        reference = None
        for lang, binary in BINARIES.items():
            for folder in [data, parquet]:
                for threads in [1, 4]:
                    out = root / f'{lang}-{folder.name}-{threads}'
                    m = execute(binary, folder, out, threads, limit=3)
                    assert int(m['rows_read']) == 12, m
                    assert int(m['rows_retained']) == 2, m
                    assert int(m['effect_values']) == 6, m
                    path = out / f'country-portfolio_PF.{folder.name}'
                    rows = list(records(path))
                    assert len(rows) == 2, rows
                    for row in rows:
                        assert str(row['PeriodStart']) == '2025-12-30', row
                        assert abs(float(row['Total']) - .08) < 1e-12, row
                    if reference is None:
                        reference = path
                    assert compare_files(reference, path) == (2, 0), path
        # An empty portfolio paired with a nonempty index remains an error.
        path = data / 'portfolio_PF.csv'
        path.write_text(path.read_text().replace(',A,', ',B,'))
        # Use distinct IDs with zero US exposure, avoiding duplicate holdings.
        with path.open(newline='') as f:
            rows = list(csv.DictReader(f))
        with path.open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows[::2]:
                writer.writerow({**row, 'Weight': '1'})
        for binary in BINARIES.values():
            for threads in [1, 4]:
                execute(binary, data, root / f'one-sided-{binary.name}-{threads}', threads, limit=3, valid=False)
    print('PASS: empty dates before/after US date; both engines, CSV/Parquet, serial/parallel; one-sided absence fails')


if __name__ == '__main__':
    main()
