"""Create a six-input dataset with deterministic, uneven per-security SMF changes."""
import argparse
import datetime as dt
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Percentages are assigned by numeric instrument ID, independently of row order.
COHORTS = [('daily', 50, 1), ('weekly', 59, 7), ('30-day', 79, 30),
           ('90-day', 99, 90), ('unchanged', 100, 0)]


def prepare(source, output, start=None, end=None):
    source = source.resolve()
    manifest_path = source / 'manifest.json'
    original = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    start = dt.date.fromisoformat(start or original['start'])
    end = dt.date.fromisoformat(end or original['end'])
    if end < start:
        raise ValueError('end must not precede start')
    # Resolve each security independently, accepting snapshot or sparse input.
    file = pq.ParquetFile(source / 'security_master.parquet')
    current = {}
    effective_col = file.schema_arrow.get_field_index('EffectiveDate')
    for group in range(file.num_row_groups):
        stats = file.metadata.row_group(group).column(effective_col).statistics
        if stats is None or not stats.has_min_max or stats.min != stats.max:
            raise ValueError('source SMF requires single-date row groups')
        if stats.min > start:
            continue
        for batch in file.iter_batches(row_groups=[group]):
            for row in batch.to_pylist():
                old = current.get(row['InstrumentId'])
                if old is None or row['EffectiveDate'] > old['EffectiveDate']:
                    current[row['InstrumentId']] = row
                elif row['EffectiveDate'] == old['EffectiveDate']:
                    raise ValueError('duplicate instrument/effective date')
    if not current:
        raise ValueError('no SMF effective at start')
    rows = sorted(current.values(), key=lambda r: r['InstrumentId'])
    ids = np.array([int(r['InstrumentId'].removeprefix('S')) for r in rows], dtype=np.int64)
    if len(set(ids)) != len(ids):
        raise ValueError('instrument IDs must have unique S-prefixed numeric suffixes')
    base = pa.Table.from_pylist(rows, schema=file.schema_arrow)
    bucket = (ids - 1) % 100
    interval = np.zeros(len(rows), dtype=np.int64)
    cohorts = {}
    previous = 0
    for name, upper, frequency in COHORTS:
        selected = (bucket >= previous) & (bucket < upper)
        interval[selected] = frequency
        cohorts[name] = dict(securities=int(selected.sum()), interval_days=frequency or None,
                             update_records=0, examples=[rows[i]['InstrumentId'] for i in np.flatnonzero(selected)[:3]])
        previous = upper
    # Security-specific phases stagger changes across dates within each cohort.
    phase = np.zeros_like(ids)
    active = interval > 0
    phase[active] = (ids[active] // 100) % interval[active]
    output.mkdir(parents=True, exist_ok=False)
    original_countries = np.array([r['Country'] for r in rows], dtype=object)
    original_currencies = np.array([r['Currency'] for r in rows], dtype=object)
    total_rows, effective_dates = 0, 0
    per_date = []
    with pq.ParquetWriter(output / 'security_master.parquet', base.schema, compression='zstd') as writer:
        for offset in range((end - start).days + 1):
            day = start + dt.timedelta(days=offset)
            selected = np.ones(len(rows), dtype=bool) if offset == 0 else active & ((offset + phase) % np.maximum(interval, 1) == 0)
            indices = np.flatnonzero(selected)
            if not len(indices):
                continue
            versions = (offset + phase[indices]) // np.maximum(interval[indices], 1)
            odd = versions % 2 == 1
            countries = original_countries[indices].copy()
            currencies = original_currencies[indices].copy()
            countries[odd] = np.where(countries[odd] == 'US', 'DE', 'US')
            currencies[odd] = np.where(currencies[odd] == 'USD', 'EUR', 'USD')
            changes = base.take(pa.array(indices))
            for column, values in [('EffectiveDate', pa.array([day] * len(indices), pa.date32())),
                                   ('Country', pa.array(countries, pa.string())),
                                   ('Currency', pa.array(currencies, pa.string()))]:
                changes = changes.set_column(changes.schema.get_field_index(column), column, values)
            writer.write_table(changes, row_group_size=65536)
            total_rows += len(indices)
            effective_dates += 1
            per_date.append(dict(date=str(day), rows=len(indices)))
            if offset:
                previous = 0
                for name, upper, _ in COHORTS:
                    cohorts[name]['update_records'] += int((selected & (bucket >= previous) & (bucket < upper)).sum())
                    previous = upper
    for name in ('accounts', 'forex', 'analytics', 'portfolio_PF', 'index'):
        (output / f'{name}.parquet').symlink_to((source / f'{name}.parquet').resolve())
    manifest = {**original, 'start':str(start), 'end':str(end), 'dates':(end-start).days+1,
                'smf_format':'sparse per-security effective-date updates', 'smf_source':str(source),
                'smf_securities':len(rows), 'smf_rows':total_rows, 'smf_effective_dates':effective_dates,
                'smf_cohorts':cohorts, 'smf_rows_per_date':per_date,
                'smf_initialization':'Latest source record per security at start, rebased to start date.',
                'smf_update_rule':'Alternately change country (US versus DE/initial) and currency (USD versus EUR/initial); preserve analytics alias.',
                'smf_schedule':'Numeric ID bucket selects cohort; ID-derived phase staggers updates. 30/90 days are fixed intervals, not calendar months/quarters.',
                'baseline_comparison':'Synthetic attributes differ from original SMF; original attribution baseline is not expected to match.'}
    manifest.pop('smf_snapshots', None)
    manifest.pop('historical_smf', None)
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({k:v for k,v in manifest.items() if k.startswith('smf_') and k != 'smf_rows_per_date'}, indent=2))
    return manifest


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, default=Path('results/six-input/inputs'))
    p.add_argument('--output', type=Path, default=Path('results/six-input/security-history-daily-heavy-inputs'))
    p.add_argument('--start-date')
    p.add_argument('--end-date')
    args = p.parse_args()
    prepare(args.source, args.output, args.start_date, args.end_date)
