"""Independent end-to-end oracle for both native implementations.

python3 python/test_attribution.py --rust rust/target/release/portfolio-attribution --go ./benchmark-go-attribution
"""
import argparse
import csv
import math
import subprocess
import tempfile
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

HEADER = ['Entity', 'EffectiveDate', 'InstrumentId', 'Weight', 'BaseReturn', 'LocalReturn']
FIELDS = ['PortfolioWeight', 'BenchmarkWeight', 'Allocation', 'Selection', 'Interaction', 'Total', 'PortfolioReturn', 'BenchmarkReturn', 'ActiveReturn', 'PortfolioContribution', 'BenchmarkContribution', 'ActiveContribution']


def key(row):
    return tuple(str(row[k]) for k in ['Entity', 'PeriodStart', 'PeriodEnd', 'Days', 'RowType', 'Segment', 'ReturnType'])


def records(path):
    """Read CSV or typed Parquet reports with bounded memory."""
    if path.suffix == '.parquet':
        import pyarrow.parquet as pq
        for batch in pq.ParquetFile(path).iter_batches(batch_size=65536):
            for row in batch.to_pylist():
                for name in ['Scenario', 'Entity', 'PeriodStart', 'PeriodEnd', 'Days', 'RowType', 'Segment', 'ReturnType']:
                    row[name] = str(row[name])
                yield row
    else:
        with path.open(newline='') as f:
            yield from csv.DictReader(f)


def compare_files(left, right):
    """Bounded-memory, cross-format comparison of every output field."""
    from itertools import zip_longest
    maximum = 0.0
    count = 0
    for x, y in zip_longest(records(left), records(right)):
        assert x is not None and y is not None, 'output row count differs'
        assert x['Scenario'] == y['Scenario'] and key(x) == key(y), (key(x), key(y))
        for f in FIELDS:
            xv, yv = float(x[f]), float(y[f])
            assert math.isfinite(xv) and math.isfinite(yv)
            maximum = max(maximum, abs(xv - yv))
            assert math.isclose(xv, yv, rel_tol=1e-8, abs_tol=1e-10), (key(x), f, xv, yv)
        count += 1
    return count, maximum


def fixture(folder):
    countries = {'A': 'US', 'B': 'DE', 'C': 'US', 'D': 'JP', 'E': 'DE', 'F': 'US'}
    with (folder / 'security_master.csv').open('w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['InstrumentId', 'Currency', 'Country'])
        w.writerows((sid, 'USD', country) for sid, country in countries.items())
    with (folder / 'subset.csv').open('w', newline='') as f:
        csv.writer(f).writerows([['InstrumentId'], ['A'], ['B']])
    data = {}
    for entity in ['INDEX', 'PF1', 'PF2']:
        days = []
        for i in range(105):
            d = (date(2023, 12, 25) + timedelta(days=i)).isoformat()
            ids = [sid for sid in countries if sid != 'F'] if entity == 'INDEX' else (['A', 'C', 'F'] + (['D'] if i % 3 else []) if entity == 'PF1' else ['B', 'D', 'C'])
            raw = [1 + (i + j) % 5 for j in range(len(ids))]
            weights = [v / sum(raw) for v in raw]
            holdings = {}
            for j, (sid, weight) in enumerate(zip(ids, weights)):
                base = 0.0 if i % 7 == 0 else (j - 2) * 0.007 + (i % 5) * 0.002
                local = base + (0.001 if entity != 'INDEX' else -0.001)
                holdings[sid] = (weight, base, local)
            days.append((d, holdings))
        data[entity] = days
        name = 'index.csv' if entity == 'INDEX' else f'portfolio_{entity}.csv'
        with (folder / name).open('w', newline='') as f:
            w = csv.writer(f)
            w.writerow(HEADER)
            for d, holdings in days:
                w.writerows((entity, d, sid, *values) for sid, values in holdings.items())
    return countries, data


def period_key(d, period):
    if period == 'all':
        return 'ALL'
    dt = date.fromisoformat(d)
    if period == 'week':
        return str(dt - timedelta(days=dt.weekday()))
    if period == 'month':
        return dt.year, dt.month
    if period == 'quarter':
        return dt.year, (dt.month - 1) // 3
    if period == 'year':
        return dt.year
    return d


def reference(countries, data, entity, scenario, period):
    buckets = defaultdict(list)
    for (d, pr), (_, br) in zip(data[entity], data['INDEX']):
        sides = []
        for holdings in [pr, br]:
            filtered = {s: v for s, v in holdings.items() if (scenario != 'subset' or s in {'A', 'B'}) and (scenario != 'country' or countries[s] == 'US')}
            denominator = sum(v[0] for v in filtered.values())
            groups = defaultdict(lambda: [0., 0., 0.])
            for sid, (w, base, local) in filtered.items():
                g = countries[sid] if scenario == 'country' else sid
                groups[g][0] += w / denominator
                groups[g][1] += w * base / denominator
                groups[g][2] += w * local / denominator
            sides.append(groups)
        p, b = sides
        daily = {}
        returns = [[sum(v[t + 1] for v in side.values()) for t in range(2)] for side in sides]
        for g in p.keys() | b.keys():
            ps, bs = p.get(g, [0.] * 3), b.get(g, [0.] * 3)
            effects = []
            for t in range(2):
                rp = ps[t + 1] / ps[0] if ps[0] else None
                rb = bs[t + 1] / bs[0] if bs[0] else None
                if rp is None:
                    rp = rb or 0.
                if rb is None:
                    rb = rp
                effects.append([(ps[0] - bs[0]) * (rb - returns[1][t]), bs[0] * (rp - rb), (ps[0] - bs[0]) * (rp - rb)])
            daily[g] = (ps[0], bs[0], effects, [ps[1:], bs[1:]])
        buckets[period_key(d, period)].append((d, daily, returns))
    expected = {}
    for days in buckets.values():
        groups = set().union(*(day[1].keys() for day in days))
        growth = [[math.prod(1 + day[2][side][t] for day in days) - 1 for t in range(2)] for side in range(2)]
        totals = [[0.] * 3 for _ in range(2)]
        for g in groups:
            weights = [sum(day[1].get(g, (0., 0., None))[side] for day in days) / len(days) for side in range(2)]
            for t in range(2):
                # Closed-form forward linking, independent of native recurrence.
                effects = []
                for component in range(3):
                    effect = sum(day[1][g][2][t][component] * math.prod(1 + x[2][0][t] for x in days[:i]) * math.prod(1 + x[2][1][t] for x in days[i + 1:]) for i, day in enumerate(days) if g in day[1])
                    effects.append(effect)
                    totals[t][component] += effect
                k = (entity, days[0][0], days[-1][0], str(len(days)), 'detail', g, ['base', 'local'][t])
                contribution = [sum(day[1][g][3][side][t] * math.prod(1 + x[2][side][t] for x in days[:i])
                                    for i, day in enumerate(days) if g in day[1]) for side in range(2)]
                expected[k] = weights + effects + [sum(effects), growth[0][t], growth[1][t], growth[0][t] - growth[1][t]] + contribution + [contribution[0] - contribution[1]]
        for t in range(2):
            k = (entity, days[0][0], days[-1][0], str(len(days)), 'total', 'ALL', ['base', 'local'][t])
            expected[k] = [1., 1.] + totals[t] + [sum(totals[t]), growth[0][t], growth[1][t], growth[0][t] - growth[1][t], growth[0][t], growth[1][t], growth[0][t] - growth[1][t]]
    # Map the detailed closed-form oracle to the requested final report contract.
    compact = {}
    residuals = {}
    for k, values in expected.items():
        if scenario in ['weekly', 'monthly']:
            if k[4] == 'total':
                compact[k] = values
            continue
        if k[4] != 'detail':
            continue
        if scenario in ['security', 'country']:
            compact[k] = values
            continue
        held = {sid for day, holdings in data[entity] if k[1] <= day <= k[2] for sid in holdings if scenario != 'subset' or sid in {'A', 'B'}}
        if k[5] in held:
            compact[k] = values
        else:
            rk = (*k[:4], 'benchmark_only', 'BENCHMARK_ONLY', k[6])
            if rk not in residuals:
                residuals[rk] = [0.] * 6 + values[6:9] + [0.] * 3
            for i in [*range(6), 9, 10, 11]:
                residuals[rk][i] += values[i]
    compact.update(residuals)
    return compact


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--rust', type=Path, default=Path('rust/target/release/portfolio-attribution'))
    ap.add_argument('--go', type=Path, default=Path('benchmark-go-attribution'))
    args = ap.parse_args()
    binaries = [args.rust.resolve(), args.go.resolve()]
    with tempfile.TemporaryDirectory(prefix='attribution-oracle-') as tmp:
        root = Path(tmp)
        data_dir = root / 'input'
        data_dir.mkdir()
        countries, data = fixture(data_dir)
        cases = [('security', 'all'), ('subset', 'all'), ('country', 'day'), ('weekly', 'week'), ('monthly', 'month'), ('monthly', 'quarter'), ('monthly', 'year'), ('subset', 'month'), ('security', 'month')]
        for scenario, period in cases:
            for language, binary in zip(['rust', 'go'], binaries):
                out = root / language / f'{scenario}-{period}'
                command = [str(binary), '--data', str(data_dir), '--output-dir', str(out), '--metrics', str(out / 'metrics.csv'), '--scenario', scenario, '--period', period, '--threads', '1', '--instruments-file', str(data_dir / 'subset.csv')]
                subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
                for entity in ['PF1', 'PF2']:
                    expected = reference(countries, data, entity, scenario, period)
                    with (out / f'{scenario}-portfolio_{entity}.csv').open(newline='') as f:
                        rows = list(csv.DictReader(f))
                    if scenario == 'security':
                        assert all(row['RowType'] == 'detail' for row in rows)
                        assert any(row['Segment'] == 'E' and float(row['PortfolioWeight']) == 0
                                   and float(row['BenchmarkWeight']) > 0 for row in rows)
                        if entity == 'PF1':
                            assert any(row['Segment'] == 'F' and float(row['BenchmarkWeight']) == 0
                                       and float(row['PortfolioWeight']) > 0 for row in rows)
                    assert len(rows) == len(expected), (language, scenario, len(rows), len(expected))
                    contribution_groups = defaultdict(list)
                    for row in rows:
                        contribution_groups[(row['PeriodStart'], row['PeriodEnd'], row['ReturnType'])].append(row)
                        assert math.isclose(float(row['ActiveContribution']),
                                            float(row['PortfolioContribution']) - float(row['BenchmarkContribution']),
                                            abs_tol=1e-12)
                        if scenario == 'security':
                            if float(row['PortfolioWeight']) == 0:
                                assert float(row['PortfolioContribution']) == 0
                            if float(row['BenchmarkWeight']) == 0:
                                assert float(row['BenchmarkContribution']) == 0
                    for group in contribution_groups.values():
                        for contribution, total_return in [('PortfolioContribution', 'PortfolioReturn'),
                                                           ('BenchmarkContribution', 'BenchmarkReturn'),
                                                           ('ActiveContribution', 'ActiveReturn')]:
                            assert math.isclose(sum(float(r[contribution]) for r in group),
                                                float(group[0][total_return]), abs_tol=1e-10)
                    seen = set()
                    for row in rows:
                        k = key(row)
                        assert k not in seen
                        seen.add(k)
                        for field, value in zip(FIELDS, expected[k]):
                            assert math.isclose(float(row[field]), value, abs_tol=1e-10, rel_tol=1e-8), (language, scenario, k, field, row[field], value)
            for entity in ['PF1', 'PF2']:
                compare_files(root / 'rust' / f'{scenario}-{period}' / f'{scenario}-portfolio_{entity}.csv', root / 'go' / f'{scenario}-{period}' / f'{scenario}-portfolio_{entity}.csv')
        # Invalid inputs must fail without publishing a new final output.
        original = (data_dir / 'portfolio_PF1.csv').read_text()
        header, *records = original.splitlines()
        bad_inputs = [header + '\n' + '\n'.join([records[0], records[0]] + records[1:]) + '\n', original.replace(',A,', ',UNKNOWN,', 1), original.replace(records[0].split(',')[4], 'NaN', 1), header + '\n' + records[0] + '\n']
        for index, bad in enumerate(bad_inputs):
            (data_dir / 'portfolio_PF1.csv').write_text(bad)
            for language, binary in zip(['rust', 'go'], binaries):
                out = root / f'bad-{language}-{index}'
                result = subprocess.run([str(binary), '--data', str(data_dir), '--output-dir', str(out), '--metrics', str(out / 'metrics.csv'), '--scenario', 'security'], capture_output=True, text=True)
                assert result.returncode != 0, (language, index)
                assert not list(out.glob('*.csv'))
        (data_dir / 'portfolio_PF1.csv').write_text(original)
        # A nonempty but unheld subset must fail on both sides.
        (data_dir / 'subset.csv').write_text('InstrumentId\nE\n')
        for binary in binaries:
            result = subprocess.run([str(binary), '--data', str(data_dir), '--output-dir', str(root / 'zero'), '--scenario', 'subset', '--instruments-file', str(data_dir / 'subset.csv')], capture_output=True, text=True)
            assert result.returncode != 0 and 'zero selected weight' in result.stderr
    print('PASS: both languages match independent reference across 9 compact scenario/period combinations; security outer-join detail and subset residuals reconcile; invalid inputs rejected')


if __name__ == '__main__':
    main()
