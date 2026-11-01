"""Add measured detailed stage tables and five consolidated scenario flow SVGs to the report."""
import json
import statistics
from pathlib import Path

import benchmark_csv_parquet as benchmark
import render_attribution_flows as flow

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / 'docs/flows/csv-parquet'
STAGES = [('read_parse_seconds', 'Read/decode'), ('filter_seconds', 'Filter'),
          ('aggregate_seconds', 'Aggregate'), ('compute_seconds', 'Compute/link'),
          ('write_seconds', 'Write')]


def seconds(value):
    value = float(value)
    return f'{value:.6f}' if abs(value) < 0.001 else f'{value:.3f}'


def latest_datasets():
    ready = {}
    for language in ['rust', 'go']:
        path = ROOT / f'results/{language}-date-cache/runs.json'
        if path.exists():
            data = json.loads(path.read_text())
            if len(data) == 14: ready[language] = data
    return ready


GO_UPDATED = 'go' in latest_datasets()
LATEST_NOTE = ('Both engines use their date-cache follow-up measurements: Country is the median of three CSV/Parquet pairs per engine; each other scenario uses one pair. Rust and Go were measured in separate sessions.' if GO_UPDATED else
              'Rust uses the latest date-cache measurements: the median of three runs for Country and one run for each other scenario. Go uses its earlier single-run baseline; Go was not rerun for this change.')


def latest_rust_rows(runs):
    # Keep the established helper name for the country renderer, but merge the
    # newest complete experiment for either engine without changing raw files.
    datasets = latest_datasets()
    if not datasets: return runs, False
    merged = []
    for original in runs:
        row = dict(original)
        latest = datasets.get(row['language'], [])
        samples = [r for r in latest if r['scenario'] == row['scenario'] and r['format'] == row['format']]
        if samples:
            for key in row:
                try: row[key] = statistics.median(float(r[key]) for r in samples)
                except (ValueError, TypeError, KeyError): pass
            row['measurement_runs'] = len(samples)
            row['process_wall_seconds_min'] = min(float(r['process_wall_seconds']) for r in samples)
            row['process_wall_seconds_max'] = max(float(r['process_wall_seconds']) for r in samples)
            row['peak_rss_mib'] = max(float(r['peak_rss_mib']) for r in samples)
        merged.append(row)
    return merged, True


def read_breakdown(scenario=None, language='rust'):
    path = ROOT / f'results/{language}-date-cache/profile/runs.json'
    if not path.exists(): return []
    profiles = [r for r in json.loads(path.read_text()) if r.get('mode', 'parquet') == 'parquet']
    if not profiles: return []
    lines = [f'## {language.capitalize()} Parquet: where read time goes', '',
             'These separate profiling runs split the read step. Times combine both readers, so they can exceed total runtime. Country uses the run with the middle read time out of three; other scenarios use one.', '',
             '| Scenario | Read + decompress/decode seconds | Prepare daily buffers seconds | Total read seconds |',
             '|---|---:|---:|---:|']
    for name, _ in benchmark.SCENARIOS:
        if scenario and name != scenario: continue
        samples = sorted([r for r in profiles if r['scenario'] == name], key=lambda r: float(r['read_parse_seconds']))
        if not samples: continue
        r = samples[len(samples)//2]
        lines.append(f"| {name.capitalize()} | {float(r['parquet_batch_read_decode_seconds']):.3f} | {float(r['parquet_to_daily_buffer_seconds']):.3f} | {float(r['read_parse_seconds']):.3f} |")
    lines += ['', 'The first part includes file I/O, decompression, decoding and Arrow batch creation. The second includes row handling, date caching, ID lookup, validation and building daily buffers. It is the remaining read time after subtracting batch time.', '',
              f'[Full date-cache comparison and profiling evidence]({language.upper()}_DATE_READ_REPORT.md)', '']
    return lines


def go_diagnostics_section():
    path = ROOT / 'results/go-date-cache/profile/runs.json'
    if not path.exists(): return []
    profiles = json.loads(path.read_text())
    groups = {mode: [p for p in profiles if p['mode'] == mode]
              for mode in ('csv', 'parquet')}
    if any(len(samples) != 3 for samples in groups.values()): return []
    lines = ['<details>', '<summary>Go: decoding, row handling, allocations and garbage collection</summary>', '',
             'Country diagnostic runs only: three runs per format, separate from the normal timing runs above. These measure the whole process, including setup and output.', '',
             '| Memory / garbage collection | CSV | Parquet with cached dates |',
             '|---|---:|---:|']
    metrics = [('Total allocated GiB', 'allocated_bytes', 1024**3),
               ('Allocated objects (millions)', 'allocated_objects', 1e6),
               ('Garbage collections', 'gc_cycles', 1),
               ('Estimated garbage-collection CPU seconds', 'gc_cpu_seconds', 1),
               ('Pause seconds', 'gc_pause_wall_seconds', 1)]
    for label, key, scale in metrics:
        values = [statistics.median(p['runtime'][key] for p in samples) / scale
                  for samples in groups.values()]
        lines.append(f'| {label} | {values[0]:.3f} | {values[1]:.3f} |')
    lines += ['', 'Values above are medians. Allocated GiB is the total allocated over the run, **not peak memory**. Garbage-collection CPU is a Go-runtime estimate that may overestimate CPU use and is not directly comparable with OS CPU time. It overlaps the stage measurements; do not add it to runtime.', '',
              '| Work sampled on CPU | CSV CPU seconds | Parquet CPU seconds |',
              '|---|---:|---:|']
    labels = [('file_read', 'File reads'), ('decode', 'Text parsing / Parquet decoding'),
              ('row_handling', 'Row handling and ID lookup'), ('allocation', 'Allocate / resize memory'),
              ('gc', 'Garbage collection'), ('heap_maintenance', 'Release unused heap memory'),
              ('runtime_support', 'Go runtime / profiling support')]
    for key, label in labels:
        values = [statistics.mean(p['cpu']['exclusive_cpu_seconds'].get(key, 0)
                                  for p in samples) for samples in groups.values()]
        lines.append(f'| {label} | {values[0]:.3f} | {values[1]:.3f} |')
    lines += ['', 'CPU rows are means from 10 ms samples, not elapsed stage times. Each sample is counted once; parsing/decoding and row handling exclude samples assigned to file reads, allocation or garbage collection. Runtime support includes scheduling, waits and profiling support. Remaining calculation, output and setup samples are in the full Go report.', '',
              'CSV spends most sampled CPU on file reads and text parsing. Parquet allocates large Arrow and dictionary-decoding buffers. A separate single-run control with the date cache disabled creates about 175.2 million more objects than cached Parquet; the cache removes repeated date-string allocations.', '',
              '[Full Go analysis](GO_DATE_READ_REPORT.md) · [Diagnostic measurements](results/go-date-cache/profile/runs.json) · [Diagnostic checks](results/go-date-cache/profile/checks.json)', '',
              '</details>', '']
    return lines


CONTENT = {
    'security': {
        'filter': ['No instrument predicate; validate every record and scan weight totals.',
                   'Retain all 60,000 portfolio and 100,000 index holdings per date.'],
        'aggregate': ['Normalize each side; build aligned instrument slots using the master lookup.',
                      'Full outer-join semantics: 59,000 portfolio-only + 1,000 shared + 99,000 index-only IDs.',
                      'Missing-side weight and contribution are zero; 159,000 IDs participate per date.'],
        'compute': ['Daily Brinson-Fachler allocation, selection and interaction for each union ID.',
                    'Also accumulate portfolio, benchmark and active contributions; reconcile totals.',
                    'Link each ID across all 1,095 dates; maintain full-history per-ID accumulators.'],
        'write': ['318,000 individual detail rows: 159,000 union IDs × base/local.',
                  '20 columns including BF effects and contributions; no index-only summary bucket.'],
        'boundary': 'EOF after all dates',
    },
    'subset': {
        'filter': ['Membership mask: retain 400 selected IDs; excluded rows were still read and validated.',
                   'Selection = 200 portfolio-only US + 100 index-only US + 100 shared DE IDs.'],
        'aggregate': ['Normalize each side within the selected universe; reject zero selected weight.',
                      '300 portfolio holdings and 200 index holdings form a 400-ID selected union.',
                      'Use compact selected-ID slots; missing-side weights and contributions are zero.'],
        'compute': ['Daily BF effects and contributions use the independently normalized subset.',
                    'Link across all dates; keep selected per-ID state and reconcile active return.',
                    'At output, reduce the 100 selected index-only IDs into one residual bucket.'],
        'write': ['602 rows: 300 held IDs × base/local + 2 index-only residual rows.',
                  'Residual preserves BF effects and contributions; output remains compact.'],
        'boundary': 'EOF after all dates',
    },
    'country': {
        'filter': ['Country lookup: retain US on both sides; exclude DE after full input validation.',
                   '59,000 US portfolio and 99,000 US index holdings per date.'],
        'aggregate': ['Normalize US portfolio weights and US index weights separately to one.',
                      'Sum normalized weights and weighted returns into one US group on each side.',
                      'Align the US country groups; BF is calculated after country aggregation.'],
        'compute': ['Calculate daily US-group BF effects and portfolio/index/active contributions.',
                    'One US group: allocation/interaction are zero up to rounding; selection is active return.',
                    'Reset period state each date; require daily output and reconcile each side.'],
        'write': ['2,190 rows: 1 US group × 1,095 dates × base/local.',
                  'PeriodStart = PeriodEnd; Days = 1; Segment = US.'],
        'boundary': 'Each date boundary + EOF',
    },
    'weekly': {
        'filter': ['No instrument predicate; validate all daily records and scan weight totals.',
                   'Retain all portfolio and index holdings.'],
        'aggregate': ['Normalize both sides; align the 159,000-ID full security union each date.',
                      '59,000 portfolio-only US + 1,000 shared DE + 99,000 index-only US IDs.',
                      'Missing-side weights and contributions are zero.'],
        'compute': ['Calculate daily per-security BF effects and weighted-return contributions.',
                    'Sum security effects/contributions to portfolio level before period linking.',
                    'One portfolio accumulator; close Monday–Sunday buckets and reset period growth.'],
        'write': ['316 rows: 158 observed weeks × base/local; partial boundary weeks included.',
                  'One portfolio total per week/return type; no per-security output history.'],
        'boundary': 'Week boundary + EOF',
    },
    'monthly': {
        'filter': ['No instrument predicate; validate all daily records and scan weight totals.',
                   'Retain all portfolio and index holdings.'],
        'aggregate': ['Normalize both sides; align the 159,000-ID full security union each date.',
                      '59,000 portfolio-only US + 1,000 shared DE + 99,000 index-only US IDs.',
                      'Missing-side weights and contributions are zero.'],
        'compute': ['Calculate daily per-security BF effects and weighted-return contributions.',
                    'Sum security effects/contributions to portfolio level before period linking.',
                    'One portfolio accumulator; close calendar-month buckets and reset period growth.'],
        'write': ['72 rows: 36 observed calendar months × base/local.',
                  'One portfolio total per month/return type; no per-security output history.'],
        'boundary': 'Month boundary + EOF',
    },
}

OPERATIONS = {
    'security': {
        'Filter': 'Keep all holdings; total the weights on each side.',
        'Aggregate': 'Scale each side’s weights to 100%; line up matching security IDs.',
        'Compute/link': 'Calculate BF effects and contributions for every security over the full period.',
    },
    'subset': {
        'Filter': 'Keep the 400 selected IDs on both sides.',
        'Aggregate': 'Scale selected weights to 100% on each side; line up selected IDs.',
        'Compute/link': 'Calculate selected-security results; combine index-only results into one summary.',
    },
    'country': {
        'Filter': 'Keep US holdings in the portfolio and index.',
        'Aggregate': 'Scale US weights to 100% on each side; add weighted returns into one US group.',
        'Compute/link': 'Calculate the US group’s BF effects and contributions for each date.',
    },
    'weekly': {
        'Filter': 'Keep all holdings; total the weights on each side.',
        'Aggregate': 'Prepare weights and matching security IDs for each day.',
        'Compute/link': 'Add security results to portfolio totals, then combine them by week.',
    },
    'monthly': {
        'Filter': 'Keep all holdings; total the weights on each side.',
        'Aggregate': 'Prepare weights and matching security IDs for each day.',
        'Compute/link': 'Add security results to portfolio totals, then combine them by month.',
    },
}


def timing_table(rows, fmt, scenario):
    definitions = [
        ('Read', 'Read and check CSV records.' if fmt == 'csv' else 'Read and decode Parquet batches.', 'read_parse_seconds'),
        ('Filter', OPERATIONS[scenario]['Filter'], 'filter_seconds'),
        ('Prepare / group', OPERATIONS[scenario]['Aggregate'], 'aggregate_seconds'),
        ('Calculate', OPERATIONS[scenario]['Compute/link'], 'compute_seconds'),
        ('Write', f'Save the {fmt.upper()} report to disk.', 'write_seconds'),
        ('**Whole run**', 'Elapsed time from start to finish.', 'process_wall_seconds'),
    ]
    lines = ['| Step | Work done | Rust seconds | Go seconds |', '|---|---|---:|---:|']
    for stage, work, field in definitions:
        values = [seconds(rows[fmt, scenario, lang][field]) for lang in ['rust', 'go']]
        lines.append(f'| {stage} | {work} | ' + ' | '.join(values) + ' |')
    return lines


def metrics_table(rows, fmt, scenario):
    def value(r, k): return float(r[k])
    def number(r, k): return f"{value(r, k):.6f}"
    def count(r, k): return f"{int(r[k]):,}"
    def rate(r, numerator, denominator, scale=1):
        return f"{value(r, numerator) / value(r, denominator) / scale:,.3f}" if value(r, denominator) else 'N/A'
    metrics = [
        ('Read/parse' if fmt == 'csv' else 'Read/decode', 'Accumulated reader-seconds', lambda r: number(r, 'read_parse_seconds')),
        ('', 'Million rows/sec', lambda r: rate(r, 'rows_read', 'read_parse_seconds', 1e6)),
        ('', 'Parsed input MiB/sec', lambda r: rate(r, 'parsed_input_bytes', 'read_parse_seconds', 1024**2) if fmt == 'csv' else 'N/A (no parsed-byte counter)'),
        ('', 'Stored input MiB/sec (size-based)', lambda r: rate(r, 'input_file_bytes', 'read_parse_seconds', 1024**2)),
        ('Filter', 'Accumulated worker-seconds', lambda r: number(r, 'filter_seconds')),
        ('', 'Rows examined', lambda r: count(r, 'rows_read')),
        ('', 'Rows retained', lambda r: count(r, 'rows_retained')),
        ('Aggregate', 'Accumulated worker-seconds', lambda r: number(r, 'aggregate_seconds')),
        ('', 'Records processed/sec', lambda r: rate(r, 'rows_retained', 'aggregate_seconds')),
        ('', 'Input entity/group/date groups produced', lambda r: count(r, 'groups')),
        ('Compute/link', 'Accumulated worker-seconds', lambda r: number(r, 'compute_seconds')),
        ('', 'Daily BF effect components computed', lambda r: count(r, 'effect_values')),
        ('', 'Daily BF effect components/sec', lambda r: rate(r, 'effect_values', 'compute_seconds')),
        ('', 'Logical output BF effect components/sec', lambda r: f"{3 * value(r, 'report_rows') / value(r, 'compute_seconds'):,.3f}"),
        ('', 'Logical report rows', lambda r: count(r, 'report_rows')),
        ('Write CSV' if fmt == 'csv' else 'Write Parquet', 'Accumulated worker-seconds', lambda r: number(r, 'write_seconds')),
        ('', 'Rows/sec', lambda r: rate(r, 'output_rows', 'write_seconds')),
        ('', 'Output MiB/sec', lambda r: rate(r, 'output_bytes', 'write_seconds', 1024**2)),
        ('', 'Attribution rows written', lambda r: count(r, 'output_rows')),
        ('', 'Attribution output MB', lambda r: f"{value(r, 'output_bytes') / 1e6:.6f}"),
        ('Overall', 'Wall seconds (single run)', lambda r: number(r, 'process_wall_seconds')),
        ('', 'Wall min–max seconds', lambda r: f"{float(r['process_wall_seconds_min']):.3f}–{float(r['process_wall_seconds_max']):.3f}" if r.get('measurement_runs', 1) > 1 else 'N/A (one run)'),
        ('', 'Peak RSS MiB', lambda r: f"{value(r, 'peak_rss_mib'):.3f}"),
        ('', 'CPU seconds', lambda r: number(r, 'cpu_seconds')),
        ('', 'Average cores', lambda r: f"{value(r, 'average_cores'):.3f}"),
    ]
    lines = ['| Step | Supporting metric | Rust | Go |', '|---|---|---:|---:|']
    current = ''
    names = {'Read/parse': 'Read', 'Read/decode': 'Read', 'Filter': 'Filter', 'Aggregate': 'Prepare / group',
             'Compute/link': 'Calculate', 'Write CSV': 'Write', 'Write Parquet': 'Write', 'Overall': 'Whole run'}
    labels = {
        'Million rows/sec': 'Million rows read per second',
        'Parsed input MiB/sec': 'CSV bytes parsed per second (MiB)',
        'Stored input MiB/sec (size-based)': 'Stored file size / read time (MiB/sec)',
        'Rows examined': 'Rows checked', 'Rows retained': 'Rows kept',
        'Records processed/sec': 'Rows prepared per second',
        'Input entity/group/date groups produced': 'Groups prepared across dates, counting portfolio and index separately',
        'Daily BF effect components computed': 'Daily BF effect values calculated',
        'Daily BF effect components/sec': 'Daily BF effect values per second',
        'Logical output BF effect components/sec': 'Final BF effect values / calculation time',
        'Logical report rows': 'Report rows',
        'Rows/sec': 'Report rows written per second',
        'Output MiB/sec': 'Output written per second (MiB)',
        'Attribution rows written': 'Report rows written', 'Attribution output MB': 'Output file size (MB)',
        'Wall min–max seconds': 'Runtime range across repeat runs',
        'Peak RSS MiB': 'Highest memory use (MiB)', 'CPU seconds': 'Total CPU time (seconds)',
        'Average cores': 'Average CPU cores used',
    }
    for stage, label, render in metrics:
        if stage: current = names[stage]
        if label in ['Accumulated reader-seconds', 'Accumulated worker-seconds', 'Wall seconds (single run)']:
            continue
        cells = [render(rows[fmt, scenario, lang]) for lang in ['rust', 'go']]
        lines.append(f'| {current} | {labels.get(label, label)} | ' + ' | '.join(cells) + ' |')

    return lines


def draw(scenario, rows):
    c = flow.Canvas(f'{scenario.capitalize()} · CSV and Parquet · Rust and Go',
                    'Alternative input paths → shared BF/contribution engine → native CSV or Parquet output · 1,095 dates', 2180)
    c.box(75, 130, 1290, 110, 'note', 'MASTER LOOKUP AND WORKERS', [
        'Rust: integer master slots; index Arc<Day> → capacity-1 channel → one portfolio worker; --threads 4.',
        'Go: concurrent index producer + portfolio goroutine; shared immutable day; channel capacity 1; GOMAXPROCS 4.'])
    def pair(fmt, field):
        return f"{fmt.upper()}: Rust {seconds(rows[fmt, scenario, 'rust'][field])}s · Go {seconds(rows[fmt, scenario, 'go'][field])}s"
    c.box(75, 280, 600, 135, 'note', 'CSV INPUT FILES', [
        'Buffered text: portfolio, index and security master.',
        'Reader/header construction occurs during setup.',
        'CSV path does not use Arrow.'])
    c.box(765, 280, 600, 135, 'note', 'PARQUET FILES + COLUMN PROJECTION', [
        'Columnar portfolio/index files; master loaded once.',
        'Rust: ProjectionMask; Go: required leaf columns.',
        'Schema/type validation and projection are setup.'])
    c.edge('M375 240 V280'); c.edge('M1065 240 V280')
    c.edge('M375 415 V455'); c.edge('M1065 415 V455')
    c.box(75, 455, 600, 160, 'read', '1a · CSV RECORDS → PARSED VALUES', [
        'Rust: csv Reader + reusable StringRecord.',
        'Go: encoding/csv + ReuseRecord.',
        'Numeric text → f64 / float64 using parsers.',
        'Read records; validate values and look up IDs.'])
    c.box(765, 455, 600, 135, 'read', '1a · COLUMN CHUNKS → DECODE', [
        'Read projected Parquet pages/column chunks.',
        'Decompress Zstandard; decode encodings.',
        'Numeric fields retain their typed representation.'])
    c.edge('M375 615 V655'); c.edge('M1065 590 V630')
    c.box(75, 655, 600, 160, 'read', '1b · CSV DAILY ADAPTER', [
        'Rust Stream::next / Go stream.next group dates.',
        'Lookahead preserves the next date boundary.',
        'Reusable raw records with integer IDs.', pair('csv', 'read_parse_seconds')])
    c.box(765, 630, 600, 135, 'read', '1b · APACHE ARROW RECORD BATCHES', [
        'Rust: ParquetRecordBatchReaderBuilder.',
        'Go: column decoding is serial within each reader.',
        'Typed string/date32/float64 arrays; ≤65,536 rows.'])
    c.edge('M1065 765 V805')
    c.box(765, 805, 600, 160, 'read', '1c · ARROW → DAILY ADAPTER', [
        'Both cache date text; numeric values stay typed.' if GO_UPDATED else 'Rust caches date text; Go formats date32.',
        'ID lookup/validation; assemble complete dates',
        'across batches into reusable raw buffers.', pair('parquet', 'read_parse_seconds')])
    c.edge('M375 815 V1010 H720 V1050'); c.edge('M1065 965 V1010 H720')
    content = CONTENT[scenario]
    def stamps(field): return [pair(fmt, field) for fmt in ['csv', 'parquet']]
    c.box(160, 1050, 1120, 160, 'filter', '2 · SHARED FILTER + DENOMINATOR SCAN', content['filter'] + stamps('filter_seconds'))
    c.edge('M720 1210 V1250')
    c.box(160, 1250, 1120, 185, 'aggregate', '3 · SHARED NORMALIZATION + AGGREGATION + ALIGNMENT', content['aggregate'] + stamps('aggregate_seconds'))
    c.edge('M720 1435 V1475')
    c.box(160, 1475, 1120, 185, 'compute', '4 · SHARED BF + CONTRIBUTIONS + LINK + RECONCILE', content['compute'] + stamps('compute_seconds'))
    c.edge('M720 1660 V1705 H375 V1740', content['boundary'], (970, 1690))
    c.edge('M720 1705 H1065 V1740')
    c.box(75, 1740, 600, 185, 'write', '5a · CSV OUTPUT', [
        f"{int(rows['csv', scenario, 'rust']['output_rows']):,} rows · 20 columns · numeric text formatting.",
        'Rust: csv Writer + Ryū; Go: escaped byte buffer.',
        'Buffer → flush → fsync → publish final .csv.',
        pair('csv', 'write_seconds')])
    c.box(765, 1740, 600, 185, 'write', '5b · TYPED PARQUET OUTPUT', [
        f"{int(rows['parquet', scenario, 'rust']['output_rows']):,} rows · 20 typed columns · Arrow builders.",
        'Rust: ArrowWriter; Go: pqarrow.FileWriter.',
        '≤65,536 rows → Zstd → footer → fsync → .parquet.',
        pair('parquet', 'write_seconds')])
    c.edge('M160 1540 H35 V435 H375 V455')
    c.edge('M1280 1540 H1400 V435 H1065 V455')
    c.text(85, 1310, ['Next', 'date'], 'label')
    c.text(1350, 1310, ['Next', 'date'], 'label')
    c.box(75, 1950, 1290, 170, 'note', 'WHAT IS SHARED AND WHAT THE TIMES MEAN', [
        'Within a portfolio worker, read → filter → aggregate → compute → write execute sequentially.',
        'Parquet uses Arrow decoding followed by an adapter; the BF calculation is currently in dense buffers.',
        'All displayed stage times accumulate across workers, overlap, and do not sum to wall time.',
        'Read times cover decoding/parsing, typed access, lookup and validation; setup is outside stage clocks.'])
    c.finish(f'{scenario}.svg')


def main():
    runs = json.loads((benchmark.OUT / 'runs.json').read_text())
    checks = json.loads((benchmark.OUT / 'checks.json').read_text())
    env = json.loads((benchmark.OUT / 'environment.json').read_text())
    assert len(runs) == 20 and len(checks) == 15
    display_runs, updated = latest_rust_rows(runs)
    rows = {(r['format'], r['scenario'], r['language']): r for r in display_runs}
    DEST.mkdir(parents=True, exist_ok=True)
    flow.DEST = DEST
    benchmark.save(display_runs, checks, env, persist=False)
    if updated:
        text = benchmark.REPORT.read_text()
        text = text.replace('**Baseline measurements.** See the [Rust date-format fix and fresh comparison](RUST_DATE_READ_REPORT.md) for the follow-up.', LATEST_NOTE)
        text = text.replace('Complete: 10 CSV runs, followed by 10 Parquet runs.', 'Updated with the Rust date-format fix and follow-up measurements.')
        text = text.replace('One full-history run per cell;', 'Rust Country cells are three-run medians; other cells are single runs;')
        text = text.replace('in this single pass', 'for the displayed results')
        text = text.replace('¹ Total CPU time across the five scenarios. ² Highest memory use seen in any scenario.', '¹ Sum of the displayed scenario CPU values; Rust Country uses its three-run median. ² Highest memory use observed, including repeats.')
        text = text.replace('CSV ran first, then Parquet. The starting language alternates by scenario. We used the normal filesystem cache and measured each configuration once.', 'Latest Rust Country pairs alternate CSV/Parquet order over three pairs; other Rust scenarios have one fresh pair. Go retains the earlier CSV-first measurements. All used normal filesystem caching.')
        text = text.replace('Validation: 15/15 file comparisons passed (5 CSV Go/Rust, 5 Parquet Go/Rust, 5 CSV/Parquet). Every output field is compared; engines also enforce BF and contribution reconciliation.', 'Checks: the updated Rust outputs passed 35 comparisons against the earlier Rust/Go results and fresh CSV/Parquet outputs. All 14 profiling comparisons passed. The original Go baseline also retains its 15 passed comparisons. Each engine checks BF and contribution totals.')
        if GO_UPDATED:
            text = text.replace('Updated with the Rust date-format fix and follow-up measurements.', 'Updated with date caches in both Rust and Go.')
            text = text.replace('Rust Country cells are three-run medians; other cells are single runs;', 'Country cells are three-run medians for both engines; other cells are single runs;')
            text = text.replace('Go retains the earlier CSV-first measurements.', 'Go also uses three alternating country pairs and one fresh pair per other scenario.')
            text = text.replace('the updated Rust outputs passed 35 comparisons against the earlier Rust/Go results and fresh CSV/Parquet outputs. All 14 profiling comparisons passed. The original Go baseline also retains its 15 passed comparisons.', 'the updated Rust and Go outputs each passed 35 comparisons against the original results and fresh CSV/Parquet outputs. Rust has 14 passed profiling checks; Go has 14 passed diagnostic/control checks.')
            text = text.replace('Rust Country uses its three-run median.', 'Country uses three-run medians for both engines.')
            text += '\nLatest measurements: [Rust](results/rust-date-cache/runs.csv) · [Go](results/go-date-cache/runs.csv). Detailed diagnostics: [Go read report](GO_DATE_READ_REPORT.md).\n'
        benchmark.REPORT.write_text(text + '\n'.join(read_breakdown() + read_breakdown(language='go') + go_diagnostics_section()))
    sections = ['', '<details>', '<summary>How the CSV and Parquet runs work</summary>', '',
                '| Layer | CSV | Parquet / Arrow |', '|---|---|---|',
                '| Storage / decode | Buffered text records; parse numeric strings | Project Parquet columns; decompress/decode into typed Arrow RecordBatches |',
                '| Daily adapter | Parsed values → reusable raw buffers | Typed Arrow access → reusable raw buffers; date fragments assembled across batches |',
                '| Calculation | Dense ID/country slots; existing BF/contribution engine | Same dense engine after Arrow decoding |',
                '| Filtering | After full record validation | After full record validation; no instrument predicate pushdown |',
                '| Output | Text CSV formatting and buffering | Typed Arrow output batches → Zstandard Parquet row groups and footer |', '',
                '**These runs measure Phase 2: native Parquet-to-Arrow readers feeding the existing calculation buffers. They do not measure an Arrow-native/vectorized BF engine.** Each scenario diagram shows the CSV and Arrow/Parquet paths converging at the shared calculation buffers. Output paths split again: CSV formatting for CSV inputs; typed Arrow batches, Parquet encoding/compression and footer finalization for Parquet inputs.', '',
                '### Concurrency scope', '',
                'The index producer and portfolio worker execute concurrently. With this merged single-portfolio fixture, there is one portfolio worker; a capacity-1 channel provides bounded index prefetch. Within that worker, portfolio reading, filtering, aggregation, BF/contribution calculation and output writing execute sequentially. There are no separate application-level stage workers or writer queue.', '',
                'Go `pqarrow` uses `Parallel=false` only for decoding columns within each individual reader. It does not disable the concurrent index producer and portfolio goroutine. `GOMAXPROCS=4` permits four logical CPUs but does not create four portfolio workers or parallelize the security loop. Enabling per-column decoding would be a separate configuration experiment requiring new measurements.', '',
                '</details>', '',
                '## Time by stage', '',
                'Each scenario has the same layout: a timing table, its supporting numbers, then the next input format. Read down to follow the steps; compare Rust and Go across the row.', '',
                '**Why stage times can exceed the whole run:** the index and portfolio workers can run at the same time. Stage figures add their time together; “Whole run” is how long you actually wait. Do not add the stage rows to estimate runtime.', '',
                LATEST_NOTE if updated else 'One measurement per configuration. Small timing differences need repeat runs to confirm.', '']
    for scenario, _ in benchmark.SCENARIOS:
        draw(scenario, rows)
        path = f'docs/flows/csv-parquet/{scenario}.svg'
        sections += [f'### {scenario.capitalize()}', '']
        for fmt in ['csv', 'parquet']:
            sections += [f'#### {fmt.upper()} input → {fmt.upper()} output', '']
            sections += timing_table(rows, fmt, scenario) + ['', '<details>',
                         f'<summary>Counts, throughput, and memory — {scenario.capitalize()}, {fmt.upper()}</summary>', '',
                         'Steps appear in the same order as the timing table. Rates use the time spent in that step.', '']
            sections += metrics_table(rows, fmt, scenario)
            sections += ['', 'MiB = 1,048,576 bytes; MB = 1,000,000 bytes. The stored-file rate uses file size, not measured disk traffic. Parquet has no parsed-text byte count. BF value counts exclude contribution columns. Rust Country reports medians and an observed runtime range from three runs; other cells use one run. Memory shows the highest observed peak.', '', '</details>', '']
        sections += ['<details>', f'<summary>View the {scenario} flow diagram</summary>', '',
                     f'[![{scenario.capitalize()} — CSV/Parquet and Rust/Go flow]({path})]({path})', '', '</details>', '']
    benchmark.REPORT.write_text(benchmark.REPORT.read_text() + '\n'.join(sections))
    from render_country_report import main as render_country
    render_country()
    print(f'Updated {benchmark.REPORT}; generated 5 scenario flow SVGs with separate input paths and Rust/Go timings in {DEST}')


if __name__ == '__main__':
    main()
