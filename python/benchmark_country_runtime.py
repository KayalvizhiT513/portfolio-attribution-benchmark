"""Measure country wall phases and worker stages for CSV/CSV and Parquet/Parquet.

Run with the repository's PyArrow environment after building both native binaries.
All runs use native automatic parallelism. The original input files are read only.
"""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import platform

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from benchmark_native import measured
from test_attribution import compare_files

ROOT = Path(__file__).resolve().parents[1]
PHASES = [
    ('Configuration, paths and output setup', 'setup_seconds'),
    ('Load, validate and index security master', 'master_load_seconds'),
    ('Discover and plan whole-date input ranges', 'boundary_plan_wall_seconds'),
    ('Concurrent range reading / filtering / aggregation', 'parallel_read_filter_aggregate_wall_seconds'),
    ('Streaming pipeline (overlapping index and portfolio stages)', 'streaming_pipeline_wall_seconds'),
    ('Ordered daily BF computation, reconciliation and writing', 'ordered_compute_write_wall_seconds'),
    ('Finalize reports, file sizes and metrics setup', 'finalize_seconds'),
    ('Remaining pipeline coordination', 'pipeline_overhead_seconds'),
]
MASTER_TYPES = {'InstrumentId': pa.string(), 'EffectiveDate': pa.date32(),
                'Currency': pa.string(), 'Country': pa.string()}


def master_batches(path):
    return pacsv.open_csv(path, read_options=pacsv.ReadOptions(block_size=4*1024*1024),
                          convert_options=pacsv.ConvertOptions(column_types=MASTER_TYPES))


def verify_batches(left, right):
    """Compare complete typed streams without requiring matching batch sizes."""
    left, right = iter(left), iter(right)
    a, b = next(left, None), next(right, None)
    ai = bi = count = 0
    while a is not None and b is not None:
        n = min(a.num_rows-ai, b.num_rows-bi)
        assert a.slice(ai,n).equals(b.slice(bi,n)), 'effective-dated master mismatch'
        ai += n; bi += n; count += n
        if ai == a.num_rows: a, ai = next(left,None), 0
        if bi == b.num_rows: b, bi = next(right,None), 0
    assert a is None and b is None, 'effective-dated master row counts differ'
    return count


def prepare_parquet(csv_data, source, target):
    target.mkdir(parents=True)
    master = csv_data/'security_master.csv'
    partial = target/'security_master.parquet.partial'
    with master_batches(master) as batches:
        assert batches.schema.names == list(MASTER_TYPES), batches.schema
        with pq.ParquetWriter(partial,batches.schema,compression='zstd') as writer:
            for batch in batches: writer.write_batch(batch,row_group_size=262144)
    partial.rename(target/'security_master.parquet')
    with master_batches(master) as batches:
        rows = verify_batches(batches,pq.ParquetFile(target/'security_master.parquet').iter_batches(batch_size=65536))
    holdings = [source/'index.parquet', *sorted(source.glob('portfolio_*.parquet'))]
    assert len(holdings)>1 and all(path.is_file() for path in holdings)
    for path in holdings: (target/path.name).symlink_to(path.resolve())
    metadata = {'master_source':str(master.resolve()), 'master_source_sha256':hashlib.sha256(master.read_bytes()).hexdigest(),
                'master_rows':rows,'master_verification':'Every typed field and row matches updated CSV, in order',
                'holdings_symlinks':[str(p.resolve()) for p in holdings]}
    (target/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n')
    return metadata


def validate_metrics(m):
    wall = float(m['wall_seconds'])
    assert math.isclose(sum(float(m[key]) for _,key in PHASES),wall,rel_tol=1e-9,abs_tol=1e-7), m
    for stage,total in [('read','read_parse_seconds'),('filter','filter_seconds'),('aggregate','aggregate_seconds'),
                        ('compute','compute_seconds'),('write','write_seconds')]:
        assert math.isclose(float(m[f'index_{stage}_seconds'])+float(m[f'portfolio_{stage}_seconds']),
                            float(m[total]),rel_tol=1e-9,abs_tol=1e-6)
    if int(m['parquet_batches']):
        parquet_read = float(m['parquet_batch_read_decode_seconds'])+float(m['parquet_to_daily_buffer_seconds'])
        all_read = float(m['read_parse_seconds'])-float(m['boundary_scan_seconds'])
        assert parquet_read <= all_read+1e-6
        if int(m['parsed_input_bytes']) == 0:
            assert math.isclose(parquet_read,all_read,rel_tol=1e-9,abs_tol=1e-6)


def report(root,runs):
    columns = [(fmt,lang) for fmt in ['csv','parquet'] for lang in ['rust','go']]
    rows = {(r['format'],r['language']):r for r in runs}
    columns = [key for key in columns if key in rows]
    def table(title, entries, integral_rows=0):
        text = ([title,''] if title else [])+['| Step | '+' | '.join(f'{lang.title()} {fmt.upper()}' for fmt,lang in columns)+' |',
                '|---|'+'---:|'*len(columns)]
        for i,(label,value) in enumerate(entries):
            pattern=',.0f' if i<integral_rows else '.6f'
            text.append('| '+label+' | '+' | '.join(format(value(rows[key]),pattern) for key in columns)+' |')
        return text+['']
    lines=['# Country runtime breakdown','',
           'One complete profiled release run per engine/format, executed sequentially with automatic CPU detection and normal filesystem caching. These are individual observations, not repeated-run medians. CSV input writes CSV; Parquet input writes Parquet. Both formats use the same effective-dated master; the prepared Parquet master is verified field-for-field against CSV. Holdings use the existing Parquet files through read-only symlinks.','',
           '## Whole-run elapsed phases (seconds)','',
           '**Each column below adds to its whole-run elapsed time.** A concurrent pipeline is one measured wall interval: its overlapping read/filter/aggregate stages cannot be separated into additive wall durations without an attribution convention. These measurements use no artificial allocation of shared wall time.','']
    lines+=table('',[(label,lambda r,k=key:float(r[k])) for label,key in PHASES]+[('**Whole run**',lambda r:float(r['wall_seconds']))])
    lines+=['## Read, filter, aggregate, compute and write (accumulated worker seconds)','',
            '**These rows do not add to whole-run elapsed time.** They sum timed intervals across concurrent workers and include time spent blocked or descheduled within each stage; they are not OS CPU seconds. Range planning is shown separately and subtracted from the read row here. Master loading is measured separately in the whole-run table.','']
    lines+=table('',[
        ('Range planning: CSV scanning / Parquet footer inspection',lambda r:float(r['boundary_scan_seconds'])),
        ('Read / parse / validate holdings and assemble daily rows',lambda r:float(r['read_parse_seconds'])-float(r['boundary_scan_seconds'])),
        ('Effective-date lookup, US filtering and selected-weight sum',lambda r:float(r['filter_seconds'])),
        ('Normalize weights and aggregate country returns',lambda r:float(r['aggregate_seconds'])),
        ('BF effects, contributions and reconciliation',lambda r:float(r['compute_seconds'])),
        ('Format / encode report, flush, footer and sync',lambda r:float(r['write_seconds'])),
    ])
    lines+=['## Parquet read detail (accumulated worker seconds)','',
            'These two rows are subsets of the holdings read stage above. Batch fetch combines file reads, decompression, decoding and Arrow construction; decompression is not isolated. Row preparation is the remaining read time, including typed access, ID lookup, validation and daily-buffer construction. CSV columns have zero Arrow batch work.','']
    lines+=table('',[
        ('Fetch / decompress / decode Arrow batches',lambda r:float(r['parquet_batch_read_decode_seconds'])),
        ('Prepare daily data from decoded Arrow rows',lambda r:float(r['parquet_to_daily_buffer_seconds'])),
    ])
    lines+=['## Scope and resource checks','']
    lines+=table('',[
        ('Input dates',lambda r:float(r['days'])),('Holdings rows read',lambda r:float(r['rows_read'])),
        ('US rows retained',lambda r:float(r['rows_retained'])),('Output rows',lambda r:float(r['output_rows'])),
        ('Detected parallelism',lambda r:float(r['threads'])),('Date-range workers (zero means streaming)',lambda r:float(r['date_range_workers'])),
        ('Date-range tasks',lambda r:float(r['date_range_tasks'])),('OS CPU seconds',lambda r:float(r['cpu_seconds'])),
        ('Average CPU cores',lambda r:float(r['average_cores'])),('Peak RSS (MiB)',lambda r:float(r['peak_rss_mib'])),
    ], integral_rows=7)
    lines+=['Read timing includes numeric validation, instrument mapping and assembly; it does not isolate physical disk I/O. Filter timing includes effective-dated country selection. Write includes buffered report serialization and final flush/sync; final publication/rename appears under finalization. Input file sizes describe stored bytes, not measured disk traffic.','',
            'The internal whole-run timer starts in `run()` and ends at the metrics snapshot; process startup/exit and serialization of the metrics CSV itself are outside it. `process_wall_seconds` records the full child-process duration. Detailed read profiling is enabled for every run.','',
            'The date-range worker count identifies parallel versus streaming execution. CSV discovers byte boundaries; Parquet uses footer statistics to group complete dates. Dates spanning adjacent single-date row groups stay together. Missing statistics or mixed-date groups retain streaming execution. Both formats write daily results in input date order.','',
            'Artifacts: `runs.csv`, `runs.json`, `checks.json`, `environment.json`, and per-run `metrics.csv`, `usage.json`, and `run.log`. Each run is checked against the first run and the requested original baseline.','']
    (root/'REPORT.md').write_text('\n'.join(lines))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--csv-data',type=Path,default=ROOT/'attribution_data')
    parser.add_argument('--parquet-source',type=Path,default=ROOT/'attribution_data_parquet')
    parser.add_argument('--reference-dir',type=Path)
    parser.add_argument('--formats',nargs='+',choices=['csv','parquet'],default=['csv','parquet'])
    args=parser.parse_args()
    root=args.output_dir.resolve();root.mkdir(parents=True,exist_ok=False)
    csv_data=args.csv_data.resolve();parquet_data=root/'inputs/parquet'
    print('Preparing and verifying effective-dated Parquet master...',flush=True)
    prepared=prepare_parquet(csv_data,args.parquet_source.resolve(),parquet_data)
    binaries={'rust':ROOT/'rust/target/release/portfolio-attribution','go':ROOT/'benchmark-go-attribution'}
    order=[(fmt,lang) for fmt,lang in [('csv','rust'),('csv','go'),('parquet','go'),('parquet','rust')] if fmt in args.formats]
    environment={'platform':platform.platform(),'logical_cpus':os.cpu_count(),
                 'thread_policy':'Native runtime automatic; no --threads flag',
                 'read_profile':'ATTRIBUTION_PROFILE_READ=1', 'prepared_parquet':prepared,
                 'order':[f'{lang} {fmt.upper()}' for fmt,lang in order],
                 'binary_sha256':{lang:hashlib.sha256(p.read_bytes()).hexdigest() for lang,p in binaries.items()}}
    (root/'environment.json').write_text(json.dumps(environment,indent=2)+'\n')
    runs=[];checks=[];reference=None;reference_counts=None
    saved=dict(os.environ)
    try:
        for key in list(os.environ):
            if key.startswith('ATTRIBUTION_'): del os.environ[key]
        os.environ['ATTRIBUTION_PROFILE_READ']='1'
        for fmt,lang in order:
            folder=root/fmt/lang;folder.mkdir(parents=True)
            data=csv_data if fmt=='csv' else parquet_data
            command=[str(binaries[lang]),'--data',str(data),'--scenario','country','--output-dir',str(folder),'--metrics',str(folder/'metrics.csv')]
            print(f'RUN {lang} {fmt}',flush=True)
            usage=measured(command,folder/'run.log')
            (folder/'usage.json').write_text(json.dumps({'command':command,**usage},indent=2)+'\n')
            with (folder/'metrics.csv').open() as f: metrics=next(csv.DictReader(f))
            validate_metrics(metrics)
            counts=[metrics[k] for k in ['days','rows_read','rows_retained','output_rows']]
            if reference_counts is None: reference_counts=counts
            assert counts==reference_counts,(fmt,lang,counts,reference_counts)
            if int(metrics['threads'])>1:
                assert int(metrics['date_range_workers'])>0
            else:
                assert int(metrics['date_range_workers'])==0
            run=dict(metrics,format=fmt,**usage);runs.append(run)
            outputs=sorted(folder.glob(f'country-portfolio_*.{fmt}'))
            if reference is None: reference={p.stem:p for p in outputs}
            assert {p.stem for p in outputs}==set(reference)
            for path in outputs:
                original = None
                if args.reference_dir:
                    original=next((args.reference_dir/lang/(path.stem+ext) for ext in ['.csv','.parquet'] if (args.reference_dir/lang/(path.stem+ext)).is_file()),None)
                    assert original is not None, (args.reference_dir,lang,path.stem)
                for tag,left in [('cross-format/engine',reference[path.stem])]+(
                    [('original baseline',original)] if args.reference_dir else []):
                    n,delta=compare_files(left,path)
                    checks.append({'format':fmt,'language':lang,'comparison':tag,'rows':n,'max_absolute_difference':delta,'passed':True})
            (root/'runs.json').write_text(json.dumps(runs,indent=2)+'\n')
            (root/'checks.json').write_text(json.dumps(checks,indent=2)+'\n')
            with (root/'runs.csv').open('w',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=list(runs[0]));writer.writeheader();writer.writerows(runs)
            report(root,runs)
            print(f"PASS {lang} {fmt}: {float(metrics['wall_seconds']):.3f}s; {metrics['output_rows']} output rows",flush=True)
    finally:
        os.environ.clear();os.environ.update(saved)
    print(f'Complete: {root}/REPORT.md',flush=True)


if __name__=='__main__': main()
