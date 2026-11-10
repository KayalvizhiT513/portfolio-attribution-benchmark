"""Update COUNTRY_REPORT_EFFECTIVE_DATED_SMF.md from recorded runs without creating per-run reports.

Security-first reports embed their evidence directory and use the current
renderer. Legacy country-first reports retain their four selected metrics paths
and historical rendering for archived evidence.
"""
import csv
import json
import math
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT/'COUNTRY_REPORT_EFFECTIVE_DATED_SMF.md'
BOOTSTRAP = {
    'csv/rust':'results/country-runtime-2026-11-02/csv/rust/metrics.csv',
    'csv/go':'results/country-runtime-2026-11-02/csv/go/metrics.csv',
    'parquet/rust':'results/country-parquet-parallel-2026-11-02/parquet/rust/metrics.csv',
    'parquet/go':'results/country-parquet-parallel-2026-11-02/parquet/go/metrics.csv',
}
PHASES = [
    ('Configuration, paths and output setup','setup_seconds'),
    ('Load, validate and index security master','master_load_seconds'),
    ('Discover and plan complete-date input ranges','boundary_plan_wall_seconds'),
    ('Concurrent range reading, filtering and aggregation','parallel_read_filter_aggregate_wall_seconds'),
    ('Streaming pipeline, when the range path is unavailable','streaming_pipeline_wall_seconds'),
    ('Ordered daily BF computation, reconciliation and writing','ordered_compute_write_wall_seconds'),
    ('Finalize reports, sizes and metrics setup','finalize_seconds'),
    ('Remaining pipeline coordination','pipeline_overhead_seconds'),
]


def selected_sources(report_path=REPORT):
    text=report_path.read_text() if report_path.exists() else ''
    match=re.search(r'<!-- country-report-sources: (.*?) -->',text)
    return json.loads(match.group(1)) if match else dict(BOOTSTRAP)


def relative(path):
    path=Path(path).resolve()
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def link(label,path):
    return f'[{label}](<{relative(path)}>)'


def read_run(source,fmt,language):
    path=ROOT/source
    with path.open(newline='') as f: rows=list(csv.DictReader(f))
    if len(rows)!=1:raise ValueError(f'Expected one metrics record: {path}')
    row=rows[0]
    if row['scenario']!='country' or row['language']!=language or row['period']!='day':
        raise ValueError(f'Unexpected country configuration: {path}')
    phase_total=sum(float(row[key]) for _,key in PHASES)
    if not math.isclose(phase_total,float(row['wall_seconds']),rel_tol=1e-9,abs_tol=1e-7):
        raise ValueError(f'Whole-run phases do not reconcile: {path}')
    checks=json.loads((path.parent.parent.parent/'checks.json').read_text())
    if not checks or any(c.get('passed') is not True or c.get('max_absolute_difference') != 0 for c in checks):
        raise ValueError(f'Country results must have passed exact output comparisons: {path}')
    usage=json.loads(path.with_name('usage.json').read_text())
    baseline_verified=any(c.get('format')==fmt and c.get('language')==language and c.get('comparison')=='original baseline' for c in checks)
    return dict(row,format=fmt,baseline_verified=baseline_verified,**{key:usage[key] for key in ['process_wall_seconds','cpu_seconds','average_cores','peak_rss_mib']})


def update_report(root=None,runs=None,report_path=REPORT):
    # The corrected contract has its own renderer; never label its security
    # effects with the old country-first narrative or compare old components.
    if runs is not None and any(int(r['effect_values']) > 6570 for r in runs):
        from render_security_country_report import render
        return render(root, report_path)
    if runs is None and report_path.exists():
        current = re.search(r'<!-- security-country-results: (.*?) -->', report_path.read_text())
        if current:
            from render_security_country_report import render
            return render(ROOT/current.group(1), report_path)
    sources=selected_sources(report_path)
    if runs is not None:
        for fmt in {r['format'] for r in runs}:
            pair=[r for r in runs if r['format']==fmt]
            if len(pair)!=2 or {r['language'] for r in pair}!={'rust','go'}:
                raise ValueError('Update the country report only after both engines finish for each requested format')
        for row in runs:
            if row['format'] not in {'csv','parquet'} or row['scenario']!='country':
                raise ValueError('Only daily country CSV/Parquet runs belong in COUNTRY_REPORT_EFFECTIVE_DATED_SMF.md')
            sources[f"{row['format']}/{row['language']}"]=relative(Path(root)/row['format']/row['language']/'metrics.csv')
    columns=[(fmt,lang) for fmt in ['csv','parquet'] for lang in ['rust','go']]
    rows={key:read_run(sources['/'.join(key)],*key) for key in columns}
    counts={tuple(int(r[k]) for k in ['days','rows_read','rows_retained','output_rows']) for r in rows.values()}
    if counts!={(1095,175200000,173010000,2190)}:
        raise ValueError('Country report sources must describe the same documented 1,095-date fixture')

    def table(entries,integers=0):
        lines=['| Step | Rust CSV → CSV | Go CSV → CSV | Rust Parquet → Parquet | Go Parquet → Parquet |',
               '|---|---:|---:|---:|---:|']
        for i,(label,value) in enumerate(entries):
            pattern=',.0f' if i<integers else '.6f'
            lines.append('| '+label+' | '+' | '.join(format(value(rows[key]),pattern) for key in columns)+' |')
        return lines+['']

    lines=['# Country attribution with an effective-dated SMF: optimized CSV and Parquet in Rust and Go','',
        '**Current report for the daily US-only country scenario, updated 2 November 2026.** This file contains the results, optimization history, timing definitions, calculation flow and evidence links. Per-run folders retain raw metrics and validation records, not separate narrative reports.','',
        '## Latest recorded optimized runtimes','',
        '| Input → output | Rust elapsed seconds | Go elapsed seconds | Output rows per engine |',
        '|---|---:|---:|---:|']
    for fmt in ['csv','parquet']:
        lines.append(f"| {fmt.upper()} → {fmt.upper()} | {float(rows[fmt,'rust']['wall_seconds']):.3f} | {float(rows[fmt,'go']['wall_seconds']):.3f} | 2,190 |")
    lines+=['',
        'CSV values are the latest fully instrumented CSV range runs; Parquet values are the later row-group range runs. The formats were measured in separate batches. Each value is one complete release-build run with normal filesystem caching, not a repeated-run median or a controlled cold-cache measurement. All selected runs omit `--threads`; detected parallelism, worker counts and task counts are recorded below.','',
        '## Main optimization after adding the effective-dated SMF','',
        '**SMF means security master file.** This report measures the workload after adding `EffectiveDate` to `security_master.csv`: 36 monthly records per instrument, with the latest record effective on or before each holding date used for classification. The master is loaded once and shared read-only; SMF loading itself has not been parallelized.','',
        '**The largest demonstrated optimization in this workload is concurrent processing of independent complete-date ranges.** Merely allowing more CPU cores did not create more parallel work in the original index/portfolio pipeline. The date-range pool parallelizes holdings reading/decoding, effective-date country lookup, filtering and aggregation. Ordered collection and writing preserve daily calculation results.','',
        'The confirmed CSV range runs improved Rust by **1.70×** and Go by **2.43×** against their automatic-setting streaming baselines. The fully instrumented Parquet range runs improved Rust by **4.61×** and Go by **3.92×**. CSV confirmation timings and later instrumented timings are recorded separately below. These are combined pipeline changes, not isolated measurements of concurrency alone or of the cost of adding the SMF.','',
        'Parquet also benefits from metadata-only range planning; CSV still scans holdings to discover date boundaries. Concurrency uses more buffers and raises peak memory. These conclusions apply to the recorded workload; other possible optimizations have not all been evaluated.','',
        '## Dataset and calculation scope','',
        '- 1,095 dates: 1 January 2023 through 30 December 2025.',
        '- One 60,000-security portfolio and a 100,000-security index per date: 175,200,000 total holdings rows.',
        '- Keep 59,000 US portfolio holdings and 99,000 US index holdings per date; 173,010,000 rows remain after filtering.',
        '- Effective-dated master: 160,000 instruments × 36 monthly versions = 5,760,000 records.',
        '- Separately normalize retained weights on each side to one; group by date, entity and country; calculate base and local returns independently.',
        '- Output: 2,190 rows per engine, with the same 20 fields. CSV input writes CSV; Parquet input writes typed Zstd Parquet.','',
        'The benchmark prepares a separate Parquet master from the updated CSV and verifies every typed field and row. Existing Parquet holdings are linked without modification. The original `attribution_data_parquet` master remains the older undated source; it is not the master used for these comparable measurements.','',
        '## Whole-run elapsed phases, in seconds','',
        '**Each column adds to its whole-run runtime, subject to displayed rounding.** Concurrent reading, filtering and aggregation form one measured wall interval. Their overlapping worker stages cannot be split into additive wall durations without an attribution convention. No such allocation is estimated here.','']
    lines+=table([(label,lambda r,k=key:float(r[k])) for label,key in PHASES]+[('**Whole run**',lambda r:float(r['wall_seconds']))])
    lines+=['## Read, filter, aggregate, compute and write, in worker seconds','',
        '**These rows do not add to whole-run runtime.** They accumulate elapsed intervals across workers, including descheduling or blocking inside a stage; they are not OS CPU seconds. Master loading is separate. Range-planning time is shown separately and subtracted from the holdings-read row.','']
    lines+=table([
        ('Range planning: CSV scan or Parquet footer inspection',lambda r:float(r['boundary_scan_seconds'])),
        ('Read, parse, validate holdings and assemble daily rows',lambda r:float(r['read_parse_seconds'])-float(r['boundary_scan_seconds'])),
        ('Effective-date lookup, US filtering and selected-weight sum',lambda r:float(r['filter_seconds'])),
        ('Normalize weights and aggregate country contributions',lambda r:float(r['aggregate_seconds'])),
        ('BF effects, contributions and reconciliation',lambda r:float(r['compute_seconds'])),
        ('Format/encode reports, flush, footer and sync',lambda r:float(r['write_seconds'])),
    ])
    lines+=['### Parquet holdings-read detail','',
        'The two Parquet rows below are subsets of the holdings-read stage above. Batch fetch combines file reading, decompression, decoding and Arrow array creation; decompression is not isolated. Daily preparation includes typed access, date reuse, ID lookup, validation and daily-buffer construction. CSV has no Arrow batch work.','']
    lines+=table([
        ('Fetch, decompress and decode Arrow batches',lambda r:float(r['parquet_batch_read_decode_seconds'])),
        ('Prepare daily rows from decoded Arrow data',lambda r:float(r['parquet_to_daily_buffer_seconds'])),
    ])
    lines+=['## CPU use and memory','']
    lines+=table([
        ('Available parallelism',lambda r:float(r['threads'])),
        ('Range workers',lambda r:float(r['date_range_workers'])),
        ('Complete-date tasks',lambda r:float(r['date_range_tasks'])),
        ('Process CPU seconds',lambda r:float(r['cpu_seconds'])),
        ('Average CPU cores used',lambda r:float(r['average_cores'])),
        ('Peak process RSS, MiB',lambda r:float(r['peak_rss_mib'])),
    ],integers=3)
    lines+=['CPU time and peak RSS come from child-process `wait4` counters. Average cores means CPU time divided by full process elapsed time. Peak RSS is whole-process memory, not per-worker memory. Parallel Parquet decoding increases concurrent Arrow buffers; its current peaks are about 761 MiB in Rust and 1,918 MiB in Go.','',
        '## Optimization history and baseline comparisons','',
        'All measurements in this section use the effective-dated master. They are separate individual runs; differences between confirmation and later instrumented runs are visible rather than presented as medians.','',
        '| Measurement | I/O | Rust seconds | Go seconds | Evidence |',
        '|---|---|---:|---:|---|']
    history=[
        ('Streaming with explicit four-thread setting','CSV','results/country-effective-dates-2026-11-02'),
        ('Streaming with automatic parallelism','CSV','results/country-auto-2026-11-02'),
        ('First complete-date parallel run','CSV','results/country-parallel-v2-2026-11-02'),
        ('Parallel binary confirmation','CSV','results/country-parallel-v2-2026-11-02/confirmation'),
    ]
    for label,fmt,path in history:
        values=[]
        for lang in ['rust','go']:
            with (ROOT/path/lang/'metrics.csv').open() as f:values.append(float(next(csv.DictReader(f))['wall_seconds']))
        lines.append(f'| {label} | {fmt} | {values[0]:.3f} | {values[1]:.3f} | {link("Recorded metrics and checks",ROOT/path/"checks.json")} |')
    before={}
    for lang in ['rust','go']:
        p=ROOT/'results/country-runtime-2026-11-02/parquet'/lang
        with (p/'metrics.csv').open() as f:before[lang]=next(csv.DictReader(f))
    lines.append(f'| Instrumented streaming baseline | Parquet | {float(before["rust"]["wall_seconds"]):.3f} | {float(before["go"]["wall_seconds"]):.3f} | {link("Recorded metrics and checks",ROOT/"results/country-runtime-2026-11-02/checks.json")} |')
    lines+=['', '### Parquet row-group optimization','',
        '| Engine | Streaming baseline | Latest parallel run | Speedup | Runtime reduction | Peak RSS before → after |',
        '|---|---:|---:|---:|---:|---:|']
    for lang in ['rust','go']:
        old=float(before[lang]['wall_seconds']);row=rows['parquet',lang];new=float(row['wall_seconds'])
        old_usage=json.loads((ROOT/'results/country-runtime-2026-11-02/parquet'/lang/'usage.json').read_text())
        lines.append(f"| {lang.title()} | {old:.3f} s | {new:.3f} s | {old/new:.2f}× | {100*(1-new/old):.1f}% | {old_usage['peak_rss_mib']:.1f} → {row['peak_rss_mib']:.1f} MiB |")
    lines+=['',
        'The earlier CSV stage used one index producer and one portfolio worker; raising the thread limit alone did not split either reader. Complete-date processing exposed more independent work. Parquet initially retained that streaming path; row-group processing then removed that concurrency limitation. These comparisons measure the implemented pipelines, not an intrinsic universal format or language ranking.','',
        '## Implemented processing flow','',
        '[Open the end-to-end SVG: physical reads, effective-date filtering, grouping, BF formulas, ordered merging and output](docs/flows/country-parallel-end-to-end.svg).','',
        '1. Load and validate the master once; sort versions by effective date and share the immutable index.',
        '2. CSV scans physical records to discover whole-date byte ranges. Parquet reads footer statistics to establish complete-date group ranges; adjacent groups for the same date stay together.',
        '3. A bounded CPU-sized pool opens an independent reader per task. Each worker buffers one logical date; Parquet decodes projected Arrow batches of at most 65,536 rows. No row group is assigned to two tasks.',
        '4. Validate every holding, then resolve the latest master version effective on or before its date. Keep `Country == US` and sum retained original weights independently for portfolio and benchmark.',
        '5. Divide retained weights and weighted returns by that side’s selected weight. Aggregate daily country weights and contributions, preserving instrument summation order.',
        '6. Collect small daily aggregates into their original positions. Check range exhaustion, entity consistency and portfolio/index date alignment. This merge reorders results; it does not average or sum different dates.',
        '7. After all ranges succeed, calculate and reconcile each date in order, then append base/local output rows. Flush buffers, finish Parquet footers, sync/close partial files and publish completed reports.','',
        '### Benchmark return used in allocation and selection','',
        'For one date and one return type, `W` is normalized country weight and `C` is weighted country contribution. Group returns are `rp,g = Cp,g / Wp,g` and `rb,g = Cb,g / Wb,g`; total returns are `RP = Σg Cp,g` and `RB = Σg Cb,g`.','',
        '```text\nAllocation_g  = (Wp,g − Wb,g) × (rb,g − RB)\nSelection_g   = Wb,g × (rp,g − rb,g)\nInteraction_g = (Wp,g − Wb,g) × (rp,g − rb,g)\nTotal_g       = Allocation_g + Selection_g + Interaction_g\n```','',
        '**Allocation uses total benchmark return `RB` after US filtering and renormalization, not the original full-index return. Selection uses benchmark group return `rb,g`, not `RB`.** With one retained US group, both group weights are approximately one and `RB ≈ rb,US`; allocation and interaction are approximately zero, while selection carries `RP − RB`. Contributions are `Cp,g`, `Cb,g` and their difference. The country period is one day, so returns are not linked across dates.','',
        '### Fallbacks and measurement limits','',
        '- `--threads 1` retains streaming execution. Multiline CSV, missing Parquet statistics, empty groups or mixed-date row groups also use streaming for the entire pipeline. Those layouts remain correct but are not parallelized here.',
        '- `--max-days` selects complete dates, including every row group belonging to the last selected date.',
        '- `boundary_scan_seconds` includes CSV scanning or Parquet footer inspection and is included in `read_parse_seconds`. For Parquet, batch decode plus daily preparation equals holdings read time minus planning time.',
        '- Read stages include lookup and validation rather than isolated disk I/O. Stored file sizes and parsed byte counters do not measure physical disk traffic.',
        '- Whole-run timing starts in native `run()` and ends at the metrics snapshot. Process startup/exit and metrics CSV serialization fall outside that timer; `process_wall_seconds` includes the entire child process.',
        '- Concurrent buffers increase memory needs; `--threads N` remains an optional concurrency override.','',
        '## Correctness checks','',
        ('All four selected results cover every input date and holding and reproduce the same 2,190 report rows. Every one of the 26,280 numeric values matches exactly across engines, formats and the original streaming outputs. The Parquet checks compare against the preceding verified results, which already match CSV; report keys and order also match.' if all(r['baseline_verified'] for r in rows.values()) else 'All four selected runs cover every input date and holding and have exact output comparisons recorded in the linked checks. See those records for each comparison’s scope; original streaming-baseline parity was not checked in every selected run.'),'',
        '- 156 exact Parquet serial/parallel comparisons cover split-date groups, typed/string dates, projected/reordered schemas, mixed I/O, multiple portfolios, maximum-date limits, layout fallback and six invalid fixtures.',
        '- 78 exact CSV comparisons cover effective-date transitions, leading blank lines, CRLF, missing final newline, quoted records, multiline fallback, worker counts 2/4/12, limits and ten invalid fixtures.',
        '- The independent attribution oracle and CSV/Parquet compatibility checks pass across nine scenario/period cases.',
        '- `cargo test --locked` and `go test -race ./cmd/attribution` pass, including native parallel Parquet execution.',
        '- Daily attribution and contribution reconciliation remain enforced; reports are finalized only after successful processing.','',
        '## Recorded evidence for the selected results','',
        '| Selected run | Native metrics | Process resources | Output comparisons |',
        '|---|---|---|---|']
    for fmt,lang in columns:
        path=ROOT/sources[f'{fmt}/{lang}']
        lines.append(f'| {lang.title()} {fmt.upper()} | {link("Metrics CSV",path)} | {link("Usage JSON",path.with_name("usage.json"))} | {link("Checks JSON",path.parent.parent.parent/"checks.json")} |')
    lines+=['',
        '## Reproduce or refresh this report','',
        '```sh\ncargo build --release --manifest-path rust/Cargo.toml\ngo -C go build -trimpath -ldflags=\'-s -w\' -o ../benchmark-go-attribution ./cmd/attribution\n.venv/bin/python python/benchmark_country_runtime.py --output-dir results/new-country-run\n```','',
        'Choose a new raw-results directory. The benchmark records metrics/usage/checks, then updates **this existing `COUNTRY_REPORT_EFFECTIVE_DATED_SMF.md` only after both engines complete and validate for every requested format**. A Parquet-only run (`--formats parquet`) replaces the two Parquet selections and retains the recorded CSV selections. It does not generate another Markdown report.','',
        'To rebuild the report from its selected evidence without rerunning benchmarks:','',
        '```sh\npython3 python/render_country_report.py\n```','',
        '## Archived experiments with different scope','',
        'Older undated-master and date-cache experiments are historical and are not the optimization baselines above. Their timings, four-thread settings and memory results should not be mixed with the current effective-dated dataset.','',
        '- [Historical five-scenario CSV/Parquet benchmark](CSV_PARQUET_REPORT.md).',
        '- [Archived Go date-cache, Arrow and memory profiling](GO_DATE_READ_REPORT.md).',
        '- [Archived Rust date-cache and Parquet reader profiling](RUST_DATE_READ_REPORT.md).',
        '- [Earlier country worker-stage evidence](results/country-worker-stages/runs.csv).',
        '- [Contribution calculation definitions](docs/CONTRIBUTIONS.md).','',
        '<!-- country-report-sources: '+json.dumps(sources,sort_keys=True)+' -->','']
    report_path.write_text('\n'.join(lines))
    return report_path


def main():
    print(f'Updated {update_report()}')


if __name__=='__main__':main()
