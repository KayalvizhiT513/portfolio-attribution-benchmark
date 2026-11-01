"""Go date-cache comparison plus separate CPU/allocation/GC diagnostics."""
import csv
import hashlib
import json
import math
import statistics
import subprocess
from pathlib import Path

from analyze_go_profile import analyze
from benchmark_native import measured
from test_attribution import compare_files

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'results/go-date-cache'
BASE=ROOT/'results/csv-parquet-native-output'
REPORT=ROOT/'GO_DATE_READ_REPORT.md'
PPROF='/private/tmp/attribution-pprof'
SCENARIOS=[('country',2190,3),('security',318000,1),('weekly',316,1),('monthly',72,1),('subset',602,1)]


def dump(path,value): path.write_text(json.dumps(value,indent=2)+'\n')
def median(rows,key): return statistics.median(float(r[key]) for r in rows)


def save(runs,checks,environment,profiles,profile_checks):
    dump(OUT/'runs.json',runs);dump(OUT/'checks.json',checks);dump(OUT/'environment.json',environment)
    if runs:
        with (OUT/'runs.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(runs[0]));w.writeheader();w.writerows(runs)
    dump(OUT/'profile/runs.json',profiles);dump(OUT/'profile/checks.json',profile_checks)
    status='Complete' if len(runs)==14 and len(profiles)==7 else 'Progress'
    lines=['# Archived Go profiling: date caching, Arrow decoding and memory','','Archived experiment; see [COUNTRY_REPORT.md](COUNTRY_REPORT.md) for the current optimized country measurements.','',f'{status}: {len(runs)}/14 normal runs; {len(profiles)}/7 diagnostic runs. All use all 1,095 dates and four configured threads.','',
           'Date32 text is now cached until the date changes, including across batches. CSV parsing, calculations, native output formats and batch sizes are unchanged. Normal timing runs do not enable the diagnostic clocks, CPU sampling or memory snapshots.','',
           '## Normal runtime after the fix','',
           'Country uses the median of three alternating CSV/Parquet pairs; other scenarios use one pair. Read time combines the two input workers and can exceed whole-run time.','',
           '| Scenario | CSV read s | Parquet read s | CSV whole run s | Parquet whole run s |','|---|---:|---:|---:|---:|']
    for scenario in ['security','country','weekly','monthly','subset']:
        cells=[]
        for field in ['read_parse_seconds','process_wall_seconds']:
            for fmt in ['csv','parquet']:
                samples=[r for r in runs if r['scenario']==scenario and r['format']==fmt]
                cells.append(f'{median(samples,field):.3f}' if samples else '—')
        lines.append(f'| {scenario.capitalize()} | '+' | '.join(cells)+' |')
    country_csv=[r for r in runs if r['scenario']=='country' and r['format']=='csv']
    country_pq=[r for r in runs if r['scenario']=='country' and r['format']=='parquet']
    if len(country_csv)==len(country_pq)==3:
        saving=(1-median(country_pq,'process_wall_seconds')/median(country_csv,'process_wall_seconds'))*100
        lines+=['',f'Country: cached Parquet takes **{saving:.1f}% less time** than CSV in the three-pair comparison.','']
    if profiles:
        groups={name:[p for p in profiles if p['mode']==name] for name in ['csv','parquet','parquet-cache-off']}
        lines+=['','## Country diagnostics: read time','',
                'These are separate profiled runs. Parquet has one timer around each batch fetch; the remaining read time covers typed access, lookup, validation and daily-buffer construction. CSV decode/row CPU costs are estimated from sampled stacks, avoiding an expensive timer on every CSV row.','',
                '| Mode | Batch read/decode s | Row handling + buffers s | Whole run s |','|---|---:|---:|---:|']
        for mode,samples in groups.items():
            if not samples:continue
            row=sorted(samples,key=lambda r:float(r['read_parse_seconds']))[len(samples)//2]
            decode=f"{float(row['parquet_batch_read_decode_seconds']):.3f}" if mode!='csv' else 'CPU sampling below'
            adapter=f"{float(row['parquet_to_daily_buffer_seconds']):.3f}" if mode!='csv' else 'CPU sampling below'
            lines.append(f'| {mode} | {decode} | {adapter} | {median(samples,"process_wall_seconds"):.3f} |')
        lines+=['','Batch time includes file I/O, decompression, decoding and Arrow batch creation. It is not pure decompression time. Country has three diagnostic runs per format; cache-off is a single same-binary control with the old per-row formatting restored.','',
                '## Allocations and garbage collection','',
                'These cover the whole process, including setup, reading and output. Allocated bytes are cumulative, not peak memory. Allocation counts/bytes and pause totals come from runtime counters. GC CPU figures are Go-runtime estimates that may overestimate CPU use; they are not directly comparable with OS CPU time. GC work overlaps stage/runtime measurements; do not add it to runtime.','',
                '| Metric | CSV | Parquet, cached dates | Parquet, cache off |','|---|---:|---:|---:|']
        metrics=[('Allocated GiB','allocated_bytes',1024**3),('Allocated objects (millions)','allocated_objects',1e6),
                 ('Garbage collections','gc_cycles',1),('Estimated GC CPU seconds','gc_cpu_seconds',1),
                 ('Estimated GC assistance CPU seconds','gc_assist_cpu_seconds',1),('Stop-the-world pause seconds','gc_pause_wall_seconds',1)]
        for label,key,scale in metrics:
            cells=[f'{median([p["runtime"] for p in groups[m]],key)/scale:.3f}' if groups[m] else '—' for m in groups]
            lines.append(f'| {label} | '+' | '.join(cells)+' |')
        if groups['parquet'] and groups['parquet-cache-off']:
            saved_objects=median([p['runtime'] for p in groups['parquet-cache-off']],'allocated_objects')-median([p['runtime'] for p in groups['parquet']],'allocated_objects')
            lines+=['',f'The same-binary cache-off control creates about **{saved_objects/1e6:.1f} million more objects** than cached Parquet. The control is one run; cached values are medians of three.','']
        lines+=['','## Where CPU samples land','',
                'Mean estimated CPU seconds from 10 ms samples (three runs for CSV/cached Parquet, one cache-off control). Each sample is counted once: GC, allocation, file reads, heap/runtime support, then the remaining decoding/row handling/calculation/output. Decoding therefore excludes samples charged to file I/O, allocation or GC. Runtime support includes scheduler, semaphore and signal/profiler support. These estimates differ from the runtime GC counters; zero means no samples, not zero work.','',
                '| Work | CSV CPU s | Parquet CPU s | Cache-off CPU s |','|---|---:|---:|---:|']
        for category in ['file_read','decode','row_handling','allocation','gc','heap_maintenance','runtime_support','calculate','output','setup','other']:
            cells=[]
            for mode in groups:
                samples=groups[mode]
                cells.append(f'{statistics.mean(p["cpu"]["exclusive_cpu_seconds"].get(category,0) for p in samples):.3f}' if samples else '—')
            label={'file_read':'File reads','decode':'Text parsing / Parquet decoding','row_handling':'Row handling and ID lookup',
                   'allocation':'Allocate / resize memory','gc':'Garbage collection','heap_maintenance':'Release unused heap memory',
                   'runtime_support':'Go runtime / profiling support','calculate':'Calculate BF and contributions',
                   'output':'Write reports','setup':'Set up inputs','other':'Other'}[category]
            lines.append(f'| {label} | '+' | '.join(cells)+' |')
        for label,field in [('Total sampled CPU','sampled_cpu_seconds'),('OS-measured process CPU','cpu_seconds')]:
            values=[]
            for mode,samples in groups.items():
                values.append(f'{statistics.mean(p["cpu"][field] if field=="sampled_cpu_seconds" else p[field] for p in samples):.3f}' if samples else '—')
            lines.append(f'| {label} | '+' | '.join(values)+' |')
        lines+=['','The profiles point to file reads and text parsing as the largest CSV costs. Parquet allocation profiles point to Arrow buffers and dictionary-decoding buffers. These findings separate memory/GC activity from row handling; they do not claim that GC pause time explains the entire runtime.','']
        for mode,samples in groups.items():
            if not samples:continue
            row=samples[len(samples)//2]
            lines+=['','<details>',f'<summary>Allocation sites — {mode}</summary>','','Allocation profiles are sampled by bytes; the counters above are exact runtime totals. No forced garbage collection is introduced.','','```text',row['allocation_top'].strip(),'```','','</details>']
    lines+=['',f'Checks passed: {len(checks)} normal-output comparisons and {len(profile_checks)} diagnostic comparisons. Every field is compared; BF/contribution reconciliation remains enabled.','',
            '[Normal measurements](results/go-date-cache/runs.csv) · [Normal checks](results/go-date-cache/checks.json) · [Diagnostic measurements](results/go-date-cache/profile/runs.json) · [Diagnostic checks](results/go-date-cache/profile/checks.json) · [Settings](results/go-date-cache/environment.json)','']
    REPORT.write_text('\n'.join(lines))


def main():
    if OUT.exists():raise FileExistsError(OUT)
    (OUT/'profile').mkdir(parents=True)
    binary=ROOT/'benchmark-go-attribution'
    environment={'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'thread_policy':'runtime default; resolved threads recorded in each run',
                 'normal_repetitions':{'country':3,'other_scenarios':1},
                 'normal_order':'CSV/Parquet pairs alternate; OS-managed caches',
                 'diagnostics':'three country pairs plus one cache-off control; CPU samples and whole-process runtime counters',
                 'allocation_profile_sample_rate_bytes':512*1024,'pprof_tool':PPROF}
    runs,checks,profiles,profile_checks=[],[],[],[]
    save(runs,checks,environment,profiles,profile_checks)
    for scenario,expected,repetitions in SCENARIOS:
        for repetition in range(1,repetitions+1):
            paths={}
            for fmt in (['csv','parquet'] if repetition%2 else ['parquet','csv']):
                out=OUT/scenario/f'run-{repetition}'/fmt;out.mkdir(parents=True)
                data=ROOT/('attribution_data' if fmt=='csv' else 'attribution_data_parquet')
                metrics=out/'metrics.csv'
                command=[str(binary),'--data',str(data),'--scenario',scenario,'--max-days','0',
                         '--output-dir',str(out),'--metrics',str(metrics),'--instruments-file',str(BASE/'subset.csv')]
                print(f'NORMAL {scenario} {repetition} {fmt}',flush=True)
                usage=measured(command,out/'run.log')
                with metrics.open() as f:r=next(csv.DictReader(f))
                assert int(r['rows_read'])==175200000 and int(r['days'])==1095 and int(r['output_rows'])==expected
                assert 'parquet_batch_read_decode_seconds' not in r
                r.update({'format':fmt,'repetition':repetition,**usage});runs.append(r)
                path=out/f'{scenario}-portfolio_PF.{fmt}';paths[fmt]=path
                for lang in ['rust','go']:
                    count,delta=compare_files(BASE/fmt/scenario/lang/path.name,path);assert count==expected
                    checks.append({'scenario':scenario,'format':fmt,'repetition':repetition,'comparison':f'original {lang}','rows':count,'max_difference':delta,'status':'PASS'})
                save(runs,checks,environment,profiles,profile_checks)
                print(f"  wall {usage['process_wall_seconds']:.3f}s; read {float(r['read_parse_seconds']):.3f}s",flush=True)
            count,delta=compare_files(paths['csv'],paths['parquet']);assert count==expected
            checks.append({'scenario':scenario,'repetition':repetition,'comparison':'fresh CSV/Parquet','rows':count,'max_difference':delta,'status':'PASS'})
            save(runs,checks,environment,profiles,profile_checks)
    cases=[(mode,i) for i in range(1,4) for mode in (['csv','parquet'] if i%2 else ['parquet','csv'])]+[('parquet-cache-off',1)]
    for mode,repetition in cases:
        fmt='csv' if mode=='csv' else 'parquet'
        out=OUT/'profile'/f'{mode}-{repetition}';out.mkdir()
        prefix=OUT/'profile'/f'{mode}-{repetition}'
        cpu=prefix.with_suffix('.cpu.pprof');alloc=prefix.with_suffix('.allocs.pprof');stats=prefix.with_suffix('.runtime.json')
        metrics=out/'metrics.csv'
        env=['env','ATTRIBUTION_PROFILE_READ=1',f'ATTRIBUTION_CPU_PROFILE={cpu}',f'ATTRIBUTION_ALLOC_PROFILE={alloc}',f'ATTRIBUTION_RUNTIME_STATS={stats}']
        if mode=='parquet-cache-off':env+=['ATTRIBUTION_DISABLE_DATE_CACHE=1']
        command=env+[str(binary),'--data',str(ROOT/('attribution_data' if fmt=='csv' else 'attribution_data_parquet')),
                     '--scenario','country','--max-days','0','--output-dir',str(out),'--metrics',str(metrics)]
        print(f'DIAGNOSTIC {mode} {repetition}',flush=True)
        usage=measured(command,out/'run.log')
        with metrics.open() as f:r=next(csv.DictReader(f))
        assert int(r['rows_read'])==175200000 and int(r['output_rows'])==2190
        if fmt=='parquet':assert math.isclose(float(r['parquet_batch_read_decode_seconds'])+float(r['parquet_to_daily_buffer_seconds']),float(r['read_parse_seconds'])-float(r.get('boundary_scan_seconds',0)),rel_tol=1e-9,abs_tol=1e-6)
        runtime=json.loads(stats.read_text());cpu_summary=analyze(PPROF,cpu)
        top=subprocess.check_output([PPROF,'-sample_index=alloc_space','-top','-nodecount=12',str(alloc)],text=True)
        r.update({'mode':mode,'repetition':repetition,'runtime':runtime,'cpu':cpu_summary,'allocation_top':top,**usage});profiles.append(r)
        dump(prefix.with_suffix('.cpu.json'),cpu_summary)
        for original_fmt in ['csv','parquet']:
            count,delta=compare_files(OUT/'country'/'run-1'/original_fmt/f'country-portfolio_PF.{original_fmt}',out/f'country-portfolio_PF.{fmt}');assert count==2190
            profile_checks.append({'mode':mode,'repetition':repetition,'comparison':f'fresh {original_fmt}','rows':count,'max_difference':delta,'status':'PASS'})
        save(runs,checks,environment,profiles,profile_checks)
        print(f"  allocated {runtime['allocated_bytes']/1024**3:.2f} GiB; GC CPU {runtime['gc_cpu_seconds']:.3f}s",flush=True)
    assert len(runs)==14 and len(checks)==35 and len(profiles)==7 and len(profile_checks)==14
    assert hashlib.sha256(binary.read_bytes()).hexdigest()==environment['binary_sha256']
    print(f'COMPLETE {REPORT}',flush=True)


if __name__=='__main__':main()
