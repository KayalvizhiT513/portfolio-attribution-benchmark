"""Replay record-level worker traces against SMF input and export a compact CSV."""
import argparse
import csv
import datetime as dt
import json
from pathlib import Path

import pyarrow.parquet as pq


def attributes(row):
    result = dict(country=row['Country'], currency=row['Currency'], analytics_id=row['AnalyticsSecId'])
    if 'Sector' in row: result['sector'] = row['Sector']
    return result


def validate_trace(path, data):
    """Use an independent streaming latest-per-security oracle for each worker."""
    synthetic_sectors = None
    manifest_path = data / 'manifest.json'
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest.get('sector_assignment', '').startswith('Synthetic:') and not manifest.get('sector_effective_dated'):
            synthetic_sectors = manifest['sectors']
        if manifest.get('smf_format') == 'daily_complete_snapshots':
            data = Path(manifest['smf_snapshot_source'])
    source = (row for batch in pq.ParquetFile(data / 'security_master.parquet').iter_batches()
              for row in batch.to_pylist())
    if synthetic_sectors:
        source = (dict(row, Sector=row.get('Sector', synthetic_sectors[(int(row['InstrumentId'][1:])-1) % len(synthetic_sectors)])) for row in source)
    pending = next(source, None)
    actual, expected = {}, {}
    summaries = []
    counts = dict(inserted=0, updated=0, unchanged=0)
    date_rows = date_groups = 0
    previous_date = None
    previous_effective = None
    seen = set()
    worker = None
    complete = False
    with path.open() as f:
        for sequence, line in enumerate(f):
            event = json.loads(line)
            assert event['sequence'] == sequence, (path, 'sequence', event)
            if sequence == 0:
                assert event['event'] == 'worker_start' and event['trace_level'] == 'records', 'Validation requires --smf-trace records'
                snapshot = event.get('initialization_mode') == 'daily_snapshot'
                worker = event['worker_id']
                first, last = event['first_date'], event['last_date']
            assert event['worker_id'] == worker and not complete, (path, event)
            kind = event['event']
            if kind == 'security_applied':
                security, effective = event['instrument_id'], event['effective_date']
                assert effective <= event['calculation_date'], event
                assert snapshot or previous_date is None or effective > previous_date, event
                if not snapshot and previous_effective != effective:
                    assert previous_effective is None or effective > previous_effective, event
                    previous_effective, seen = effective, set()
                assert security not in seen, ('duplicate effective-date security', event)
                seen.add(security)
                before = actual.get(security)
                assert event['before'] == (before[1] if before else None), ('before mismatch', event)
                action = 'inserted' if before is None else ('unchanged' if before[1] == event['after'] else 'updated')
                assert event['action'] == action, event
                counts[action] += 1
                actual[security] = (effective, event['after'])
            elif kind == 'snapshot_reset':
                assert snapshot and event['previous_state_securities'] == len(actual), event
                actual.clear(); seen = set()
                assert not any(counts.values()) and date_rows == 0, event
            elif kind in ('effective_date_applied', 'snapshot_loaded'):
                assert (kind == 'snapshot_loaded') == snapshot, event
                assert all(event[k] == v for k, v in counts.items()), event
                assert event['rows'] == sum(counts.values()) and event['state_securities'] == len(actual), event
                date_rows += event['rows']
                date_groups += 1
                counts = dict(inserted=0, updated=0, unchanged=0)
                summaries.append(event)
            elif kind == 'date_ready':
                date = event['calculation_date']
                next_date = first if previous_date is None else str(dt.date.fromisoformat(previous_date) + dt.timedelta(days=1))
                assert date == next_date and date <= last, event
                assert event['previous_date'] == previous_date, event
                expected_rows, expected_dates = 0, set()
                while pending is not None and str(pending['EffectiveDate']) <= date:
                    expected_rows += 1
                    expected_dates.add(str(pending['EffectiveDate']))
                    expected[pending['InstrumentId']] = (str(pending['EffectiveDate']), attributes(pending))
                    pending = next(source, None)
                if snapshot:
                    expected_rows, expected_dates = len(expected), {date}
                assert (date_rows, date_groups) == (expected_rows, len(expected_dates)), (path, date, 'applied history coverage mismatch')
                assert actual == expected, (path, date, 'cache differs from independent latest-effective-date oracle')
                assert event['state_securities'] == len(expected), event
                assert event['rows_applied'] == date_rows and event['effective_dates_applied'] == date_groups, event
                mode = ('snapshot_initialize' if previous_date is None else 'snapshot_replace') if snapshot else ('initialize' if previous_date is None else ('reuse' if date_rows == 0 else 'update'))
                assert event['mode'] == mode, event
                previous_date, date_rows, date_groups = date, 0, 0
                summaries.append(event)
            elif kind == 'worker_complete':
                assert previous_date == last and date_rows == 0 and not any(counts.values()), event
                complete = True
            else:
                assert kind == 'worker_start' and sequence == 0, event
    assert complete, (path, 'missing worker_complete: worker failed or trace is incomplete')
    return summaries


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data', type=Path, required=True)
    p.add_argument('--trace-dir', type=Path, required=True)
    p.add_argument('--output', type=Path, help='compact CSV path; defaults to trace-dir/smf-trace-summary.csv')
    args = p.parse_args()
    paths = sorted(args.trace_dir.glob('smf-trace-worker-*.jsonl'))
    assert paths, 'No worker traces found'
    rows = [row for path in paths for row in validate_trace(path, args.data)]
    target = args.output or args.trace_dir / 'smf-trace-summary.csv'
    fields = ['worker_id', 'sequence', 'event', 'calculation_date', 'effective_date', 'mode',
              'rows', 'inserted', 'updated', 'unchanged', 'rows_applied', 'effective_dates_applied', 'state_securities']
    with target.open('w', newline='') as f:
        writer = csv.DictWriter(f, fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print(f'Validated {len(paths)} worker traces against source SMF; summary: {target}')


if __name__ == '__main__':
    main()
