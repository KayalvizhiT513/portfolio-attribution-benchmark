"""US-filtered cumulative oracle: effective-dated classifications, normalization and parallel ends."""
import calendar
import csv
import datetime as dt
import math
from pathlib import Path
import statistics
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]

def main():
    import pyarrow as pa
    import pyarrow.parquet as pq
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        dates = [dt.date(2023, 1, 1) + dt.timedelta(days=i) for i in range(1096)]
        dates = [d for d in dates if d <= dt.date(2025,12,30)]
        daily = []
        csv_dir, parquet_dir = root / 'csv', root / 'parquet'
        csv_dir.mkdir(); parquet_dir.mkdir()
        header = ['Entity', 'EffectiveDate', 'InstrumentId', 'Weight', 'BaseReturn', 'LocalReturn']
        for name, entity in [('index', 'BM'), ('portfolio_PF', 'PF')]:
            rows = []
            totals = []
            for i, date in enumerate(dates):
                b = ((i % 19) - 9) / 1000
                p = 1.7 * b + ((i % 7) - 2) / 2000
                r = b if name == 'index' else p
                # Varying excluded returns expose accidental all-holdings aggregation.
                returns = [r * 1.3 + (i % 11 - 5) * .0007, -r * .8 + (i % 13 - 6) * .002]
                for security, weight, value in [('US1', .75, returns[0]), ('DE1', .25, returns[1])]:
                    rows.append([entity, date.isoformat(), security, weight, value, value * .6 + .0002])
                selected = returns[1] if dt.date(2024,7,1) <= date < dt.date(2025,1,1) else returns[0]
                totals.append(None if date in [dates[0], dates[-1]] else (selected, selected * .6 + .0002))
            daily.append(totals)
            with (csv_dir / f'{name}.csv').open('w') as f:
                w = csv.writer(f); w.writerow(header); w.writerows(rows)
            table = pa.table({h: [r[j] for r in rows] for j, h in enumerate(header)})
            pq.write_table(table, parquet_dir / f'{name}.parquet', row_group_size=2)
        master_header = ['InstrumentId', 'EffectiveDate', 'Currency', 'Country']
        master = [
            ['US1','2023-01-01','USD','DE'], ['US1','2023-01-02','USD','US'],
            ['US1','2024-07-01','USD','DE'], ['US1','2025-01-01','USD','US'],
            ['US1','2025-12-30','USD','DE'],
            ['DE1','2023-01-01','EUR','DE'], ['DE1','2024-07-01','EUR','US'],
            ['DE1','2025-01-01','EUR','DE'],
        ]
        with (csv_dir / 'security_master.csv').open('w') as f:
            w=csv.writer(f); w.writerow(master_header); w.writerows(master)
        pq.write_table(pa.table({h: [r[j] for r in master] for j,h in enumerate(master_header)}), parquet_dir / 'security_master.parquet')
        checks = 0
        for language, binary in [('go', ROOT / 'benchmark-go-attribution'), ('rust', ROOT / 'rust/target/release/portfolio-attribution')]:
            for fmt, data in [('csv', csv_dir), ('parquet', parquet_dir)]:
                for readers, workers in [(1,1),(4,4)]:
                    out = root / f'{language}-{fmt}-{readers}'
                    subprocess.run([str(binary), '--scenario', 'cumulative', '--data', str(data), '--output-dir', str(out), '--metrics', str(out/'metrics.csv'), '--threads', str(readers), '--effective-date-workers', str(workers)], check=True, capture_output=True)
                    rows = list(csv.DictReader((out / 'cumulative-portfolio_PF.csv').open()))
                    assert len(rows) == 36
                    metrics = next(csv.DictReader((out/'metrics.csv').open()))
                    assert int(metrics['effective_date_tasks']) == 36
                    assert int(metrics['date_range_workers']) == (0 if readers == 1 else 4)
                    for i, row in enumerate(rows):
                        year, month = 2023+i//12, i%12+1
                        end = dt.date(year, month, calendar.monthrange(year, month)[1])
                        selected_dates = [j for j, date in enumerate(dates) if date <= end and daily[0][j] is not None]
                        n = len(selected_dates)
                        assert row['EffectiveDate'] == end.isoformat() and int(row['Observations']) == n
                        assert row['Level'] == 'Total'
                        assert row['LastObservationDate'] == dates[selected_dates[-1]].isoformat()
                        for t, prefix in enumerate(['Base', 'Local']):
                            b = [daily[0][j][t] for j in selected_dates]
                            p = [daily[1][j][t] for j in selected_dates]
                            a = [x-y for x,y in zip(p,b)]
                            te = statistics.stdev(a)
                            want = [statistics.stdev(p), statistics.stdev(b), te, statistics.mean(a)/te, statistics.covariance(p,b)/statistics.variance(b)]
                            for field, expected in zip(['PortfolioStdDev','BenchmarkStdDev','TrackingError','InformationRatio','Beta'], want):
                                assert math.isclose(float(row[prefix+field]), expected, rel_tol=1e-10, abs_tol=1e-12), (language, fmt, row, field, expected)
                                checks += 1
        # Preserve validation despite bypassing security-level attribution buffers.
        original = list(csv.reader((csv_dir / 'portfolio_PF.csv').open()))
        rejected = 0
        for name in ['duplicate', 'weight', 'number', 'mixed_entity', 'dates', 'unmapped', 'one_sided_us']:
            rows = [row.copy() for row in original]
            if name == 'duplicate': rows[2][2] = rows[1][2]
            elif name == 'weight': rows[1][3] = '0'
            elif name == 'number': rows[1][4] = 'nan'
            elif name == 'mixed_entity': rows[2][0] = 'OTHER'
            elif name == 'unmapped': rows[1][2] = 'UNKNOWN'
            elif name == 'one_sided_us': rows[3][3], rows[4][3] = '0', '1'
            else: rows[1:3], rows[3:5] = rows[3:5], rows[1:3]
            with (csv_dir / 'portfolio_PF.csv').open('w') as f:
                csv.writer(f).writerows(rows)
            for language, binary in [('go', ROOT / 'benchmark-go-attribution'), ('rust', ROOT / 'rust/target/release/portfolio-attribution')]:
                for threads in [1,4]:
                    out = root / f'invalid-{name}-{language}-{threads}'
                    result = subprocess.run([str(binary), '--scenario', 'cumulative', '--data', str(csv_dir), '--output-dir', str(out), '--metrics', str(out / 'metrics.csv'), '--threads', str(threads)], capture_output=True)
                    assert result.returncode != 0, (name, language, threads)
                    assert not list(out.glob('cumulative-*.csv'))
                    rejected += 1
        print(f'Passed {checks} independent statistic checks across 8 executions; {rejected} invalid inputs rejected.')

if __name__ == '__main__':
    main()
