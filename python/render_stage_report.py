"""Render the five-scenario end-to-end attribution benchmark."""
import json
from pathlib import Path
from attribution_flow_content import OVERVIEW, SCENARIO_FLOWS, SHARED_LOGIC

ROOT=Path(__file__).resolve().parents[1]
FOLDER=ROOT/'results/compact-full-history'
SCENARIOS=[('security','Security'),('subset','Standalone subset'),('country','Country'),('weekly','Weekly'),('monthly','Monthly')]


def cells(rows,scenario,render):
    return ' | '.join(render(rows[scenario,language]) for language in ['rust','go'])


def metrics_table(rows,scenario):
    value=lambda r,k:float(r[k])
    num=lambda r,k:f'{value(r,k):.6f}'
    count=lambda r,k:f'{int(r[k]):,}'
    rate=lambda r,n,t:f'{value(r,n)/value(r,t):,.2f}' if value(r,t)>0 else 'N/A'
    metrics=[
        ('Read/parse','Accumulated reader-seconds',lambda r:num(r,'read_parse_seconds')),
        ('','Million rows/sec',lambda r:f"{value(r,'rows_read')/value(r,'read_parse_seconds')/1e6:.3f}"),
        ('','Input MiB/sec',lambda r:f"{value(r,'parsed_input_bytes')/value(r,'read_parse_seconds')/1024**2:.3f}"),
        ('Filter','Accumulated worker-seconds',lambda r:num(r,'filter_seconds')),
        ('','Rows examined',lambda r:count(r,'rows_read')),
        ('','Rows retained',lambda r:count(r,'rows_retained')),
        ('Aggregate','Accumulated worker-seconds',lambda r:num(r,'aggregate_seconds')),
        ('','Records processed/sec',lambda r:rate(r,'rows_retained','aggregate_seconds')),
        ('','Input entity/group/date groups produced',lambda r:count(r,'groups')),
        ('Compute/link','Accumulated worker-seconds',lambda r:num(r,'compute_seconds')),
        ('','Daily effect components computed',lambda r:count(r,'effect_values')),
        ('','Daily effect components/sec',lambda r:rate(r,'effect_values','compute_seconds')),
        ('','Logical output effect components/sec',lambda r:f"{3*value(r,'report_rows')/value(r,'compute_seconds'):,.2f}"),
        ('','Logical report rows',lambda r:count(r,'report_rows')),
        ('Write CSV','Accumulated worker-seconds',lambda r:num(r,'write_seconds')),
        ('','Rows/sec',lambda r:rate(r,'output_rows','write_seconds')),
        ('','Output MiB/sec',lambda r:f"{value(r,'output_bytes')/value(r,'write_seconds')/1024**2:.3f}" if value(r,'write_seconds') else 'N/A'),
        ('','Attribution rows written',lambda r:count(r,'output_rows')),
        ('','Attribution output MB',lambda r:f"{value(r,'output_bytes')/1e6:.6f}"),
        ('Overall','Median wall seconds',lambda r:num(r,'process_wall_seconds')),
        ('','Wall min–max seconds',lambda r:f"{value(r,'process_wall_seconds_min'):.3f}–{value(r,'process_wall_seconds_max'):.3f}"),
        ('','Median peak RSS MiB',lambda r:f"{value(r,'peak_rss_mib'):.3f}"),
        ('','Median CPU seconds',lambda r:num(r,'cpu_seconds')),
        ('','Median average cores',lambda r:f"{value(r,'average_cores'):.3f}"),
    ]
    lines=['**Timing units:** stage values accumulate across concurrent readers/workers; overall wall time measures end-to-end elapsed time. Stage throughput is per accumulated stage-second.', '', '| Stage | Metric | Rust | Go |','|---|---|---:|---:|']
    lines += [f'| {stage} | {label} | {cells(rows,scenario,fn)} |' for stage,label,fn in metrics]
    return lines


def main():
    summary=json.loads((FOLDER/'summary.json').read_text())
    runs=json.loads((FOLDER/'runs.json').read_text())
    checks=json.loads((FOLDER/'checks.json').read_text())
    env=json.loads((FOLDER/'environment.json').read_text())
    thread_settings={lang: ', '.join(str(n) for n in sorted({int(r['threads']) for r in runs if r['language']==lang})) for lang in ['rust','go']}
    rows={(r['scenario'],r['language']):r for r in summary}
    assert len(rows)==10 and len(runs)==10*env['runs'] and len(checks)==5
    assert all(int(r['days'])==1095 and int(r['runs'])==env['runs'] for r in summary)
    lines=['# Historical attribution stage benchmarks: five scenarios and four portfolios','','Archived experiment; see [COUNTRY_REPORT.md](COUNTRY_REPORT.md) for the current optimized country measurements.','',
           'All five scenarios process all **1,095 effective dates**, all four portfolios, and separate base/local returns. Calculation granularity is independent of report granularity. The report contains one section per scenario, comparing Rust and Go.','',
           f"Each language/scenario has **{env['runs']} fresh complete repetitions**. Stage times, OS CPU, peak RSS and process wall times are medians; overall ranges are also shown. All displayed measurements use the reporting contracts defined below.",'',
           '## Output contract and benchmark-only reconciliation','',OVERVIEW,'',
           'Portfolio-held is presence-based: the ID appears in a portfolio holding record on at least one date in the reporting range. A benchmark-only bucket covers instruments never held by that portfolio in that range. It is part of the mathematical report, not an optional discarded residual. It preserves benchmark weights and linked effects that would otherwise be lost by suppressing benchmark-only detail. Security/subset detail plus this bucket reconcile to complete active return.','',
           '## Overall results and output sizes','',
           '| Scenario | Rust seconds | Go seconds | Report rows/language | Rust output MB | Go output MB | Combined MB |',
           '|---|---:|---:|---:|---:|---:|---:|']
    for scenario,_ in SCENARIOS:
        a=rows[scenario,'rust'];b=rows[scenario,'go']
        times = cells(rows,scenario,lambda r:format(float(r['process_wall_seconds']),'.3f'))
        lines.append(f"| {scenario} | {times} | {int(a['report_rows']):,} | {int(a['output_bytes'])/1e6:.6f} | {int(b['output_bytes'])/1e6:.6f} | {(int(a['output_bytes'])+int(b['output_bytes']))/1e6:.6f} |")
    lines += ['', 'MB uses 1,000,000 bytes. CSV sizes include report headers and all four portfolio CSVs, including security/subset reconciliation rows, but exclude metrics/logs. Output files are small and are retained as the final reports; no large daily/detail CSV is produced.','',
              '## Measurement method','',
           'The fixture has four disjoint portfolios of 15,000 fixed instruments each, a shared 100,000-instrument index, and 1,095 calendar dates. All portfolio holdings are in the index. Each execution reads 175,200,000 return records; weights vary daily and are normalized per entity/date. All held instruments map to US/USD.', '',
              f"Machine: {env['platform']}, {env['logical_cpus']} logical CPUs. Rust's recorded portfolio worker limit is {thread_settings['rust']}; Go uses GOMAXPROCS={thread_settings['go']} with concurrent portfolio goroutines. Each uses its own optimized parser/buffers/formatter. Language order alternates between repetitions; filesystem cache is OS-managed and first runs are retained. These are workload-specific observations, not controlled cold-cache or universal language claims.",'',
              '**Stage seconds are accumulated worker elapsed time, include scheduling pauses and overlap across threads. They are not stage CPU time and cannot be added to obtain wall time.** Read includes decoding, numeric parsing, lookup and validation. Filter includes selection and denominator scan; only subset applies an instrument predicate. Aggregate includes normalization, dense slot construction and weighted contributions. Compute includes daily effects, linking, reconciliation, report planning, residual reduction and checksums. Write includes only attribution file creation/header, numeric/CSV formatting, buffering, flush and fsync. Metrics-file writing, setup, channel waits and publication contribute to external overall time, not the attribution write stage.','',
              'Per-stage rates divide work by the corresponding median stage duration. Groups are input entity/group/date combinations, not output row counts. Logical output effect components count three components per report row. Daily effect components count the daily engine’s values before linking/reduction. Average cores is process CPU time divided by process wall time; it is descriptive, not a metric where larger is automatically better.','',
              '### Why a stage can show 30 seconds while the program finishes in 15 seconds', '',
              '**Overall wall time is the elapsed time you wait, from process start to completion. Stage timings sum elapsed durations inside that stage across concurrent readers/workers.** For example, one index reader taking 12 seconds and four portfolio readers taking 3 seconds each contribute 24 accumulated reader-seconds. Because they run concurrently, the complete program can still finish in about 15 wall seconds. This is an illustration, not a measured timing breakdown.', '',
              '| Timing label | Interpretation |', '|---|---|',
              '| Accumulated reader-seconds | Sum of read/parse elapsed durations across the index and portfolio readers |',
              '| Accumulated worker-seconds | Sum of elapsed durations inside a stage across its executing workers |',
              '| Median wall seconds | Median end-to-end process duration across the two repetitions |',
              '| CPU seconds | OS-measured CPU time across process threads; excludes time waiting for I/O |', '',
              '**Do not add stage times or compare one accumulated stage duration directly with wall time.** Scheduling pauses can also contribute to accumulated stage durations. Stage throughput uses accumulated stage time as its denominator: rows/sec and MiB/sec are per accumulated reader/worker-second, not end-to-end program throughput.', '',
              '## Five scenarios','']
    for i,(scenario,title) in enumerate(SCENARIOS,1):
        lines += [f'### {i}. {title}','',SCENARIO_FLOWS[scenario],'','**Scope:** all 1,095 dates, all four portfolios, both return types; complete read, calculation and CSV-writing executions.','']
        lines += metrics_table(rows,scenario)+['']
    lines += [SHARED_LOGIC,'','## Correctness evidence','',
              '| Scenario | Compact rows compared | Maximum absolute numeric difference | Repetition checksum agreement | Result |',
              '|---|---:|---:|---|---|']
    for c in checks:
        lines.append(f"| {c['scenario']} | {c['rows_compared']:,} | {c['max_absolute_difference']:.3g} | {c['checksum_repetitions']} | {c['status']} |")
    lines += ['', 'Every numeric field of the final Rust and Go CSVs is compared (absolute tolerance 1e-10 or relative tolerance 1e-8), not a sample. Every execution validates daily and linked reconciliation. Logical row counts and all three checksum components must agree across languages and repetitions. An independent Python closed-form linking oracle covers nine small scenario/period combinations, changing holdings, full-history held membership, benchmark-only reconciliation and invalid inputs. Checksum equality and reconciliation are consistency evidence, not independent proof of an economically realistic model.','',
              '## Who wins, and where?','',
              'Lower median accumulated stage time, process wall time, memory and CPU determine each winner. Filter is N/A without an instrument predicate. Small stages and two-repeat medians are noisy and do not establish statistical significance.','',
              '| Scenario | Read/parse | Filter | Aggregate | Compute/link | Write | Overall | Peak memory | CPU time | Overall speed advantage |',
              '|---|---|---|---|---|---|---|---|---|---|']
    for scenario,_ in SCENARIOS:
        a,b=[rows[scenario,l] for l in ['rust','go']]
        winners=[]
        for field in ['read_parse_seconds','filter_seconds','aggregate_seconds','compute_seconds','write_seconds','process_wall_seconds','peak_rss_mib','cpu_seconds']:
            if (field=='filter_seconds' and scenario!='subset'):winners.append('N/A')
            else:winners.append('Rust' if float(a[field])<float(b[field]) else 'Go' if float(b[field])<float(a[field]) else 'Tie')
        x,y=float(a['process_wall_seconds']),float(b['process_wall_seconds'])
        advantage=f"{'Rust' if x<y else 'Go'} {max(x,y)/min(x,y):.2f}×" if x!=y else 'Tie'
        lines.append('| '+' | '.join([scenario,*winners,advantage])+' |')
    lines += ['', '## Recorded sources and reproduction','',
              '- [All raw executions](results/compact-full-history/runs.json), [CSV version](results/compact-full-history/runs.csv), and [median/range summaries](results/compact-full-history/summary.json).',
              '- [Correctness checks](results/compact-full-history/checks.json), [environment, input metadata and executable hashes](results/compact-full-history/environment.json).',
              '- [Contracts, formulas, CLI and limitations](rust/README.md).','',
              '```sh','python3 python/test_attribution.py','python3 python/benchmark_compact.py --runs 2','python3 python/render_stage_report.py','```','',
              'The benchmark requires a new output directory if one already exists; use `--output-dir` for a fresh execution. The report, metrics and diagrams use the output contracts defined above.','']
    (ROOT/'ATTRIBUTION_STAGE_COMPARISON.md').write_text('\n'.join(lines))
    print(ROOT/'ATTRIBUTION_STAGE_COMPARISON.md')

if __name__=='__main__':main()
