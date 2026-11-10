"""Archive benchmark contracts/evidence and journal removal of generated bulk files.

Run without --apply to prepare the archive and inventory only. --apply deletes
only untracked CSV/Parquet/binary files >=20 MiB under results, plus redundant
attribution_data CSVs. Canonical Parquet holdings and legacy data/ sources stay.
"""
import argparse
import datetime as dt
import hashlib
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
ARCHIVE=ROOT/'docs/benchmarks/2026-11-08'
IST=dt.timezone(dt.timedelta(hours=5,minutes=30))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true');args=parser.parse_args()
    ARCHIVE.mkdir(parents=True,exist_ok=True)
    source=ARCHIVE/'source';source.mkdir(exist_ok=True)
    seed=ROOT/'results/country-security-first-v2-2026-11-02/inputs/history/parquet/security_master.parquet'
    retained=source/'security_master_history.parquet'
    if not retained.exists():shutil.copy2(seed,retained)
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    env={'python':sys.version,'numpy':np.__version__,'pyarrow':pa.__version__,
         'git_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
         'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),
         'cargo':subprocess.check_output(['cargo','--version'],text=True).strip(),
         'timezone':'Asia/Kolkata','archive_date':'2026-11-08',
         'note':'Current working-tree changes and script sources are preserved locally; older runs used earlier implementations.'}
    (ARCHIVE/'environment.json').write_text(json.dumps(env,indent=2)+'\n')
    (ARCHIVE/'python-packages.txt').write_text(subprocess.check_output([sys.executable,'-m','pip','freeze'],text=True))
    for directory,patterns in [('python',['*.py','requirements.txt']),('rust',['src/**/*.rs','Cargo.toml','Cargo.lock']),('go',['**/*.go','go.mod','go.sum'])]:
        for pattern in patterns:
            for path in (ROOT/directory).glob(pattern):
                if not path.is_file():continue
                dest=ARCHIVE/'code'/path.relative_to(ROOT)
                dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest)

    patch=subprocess.check_output(['git','diff','--','rust','go','python','README.md','docs/DAILY_SMF_SNAPSHOTS.md'],cwd=ROOT)
    (ARCHIVE/'working-tree.patch').write_bytes(patch)
    runs=[]
    for path in sorted((ROOT/'results/api').glob('*/summary.json')):
        summary=json.loads(path.read_text());folder=path.parent;dest=ARCHIVE/'runs'/folder.name;dest.mkdir(parents=True,exist_ok=True)
        for evidence in ['summary.json','startup.json','validation.json','cache-policy.json','three-mode-comparison.json']:
            if (folder/evidence).is_file():shutil.copy2(folder/evidence,dest/evidence)
        command=summary.get('command',[])
        def option(name,default=None):
            return command[command.index(name)+1] if name in command else default
        record={'run':folder.name,'command':command,'input_directory':summary.get('data'),
                'date_window':[summary.get('start_date'),summary.get('end_date')],
                'aggregate':option('--aggregate','daily (legacy default)'),
                'return_type':option('--return-type','both for daily'),
                'workers':summary.get('workers_per_request'),
                'peak_sampled_rss_bytes':summary.get('peak_sampled_rss_bytes'),
                'cache_policy':json.loads((folder/'cache-policy.json').read_text()) if (folder/'cache-policy.json').exists() else 'not recorded; do not infer cold-cache',
                'batches':[]}
        timestamp=None
        for bi,batch in enumerate(summary.get('batches',[])):
            br={'concurrency':batch.get('concurrency'),'repeat':batch.get('repeat'),
                'batch_seconds':batch.get('seconds'),'responses':[]}
            for ri,response in enumerate(batch.get('responses',[])):
                body=response.get('body');entry={k:response[k] for k in ['status','seconds'] if k in response}
                if isinstance(body,dict):
                    out=Path(body.get('output_dir','__missing__'))
                    entry['metrics']=body.get('metrics');entry['report']=body.get('report')
                    found=re.search(r'request-\d+-(\d{18,20})-\d+',str(out))
                    if found and timestamp is None:timestamp=dt.datetime.fromtimestamp(int(found.group(1))/1e9,IST)
                    if out.is_dir():
                        request_dest=dest/f'batch-{bi+1}-request-{ri+1}';request_dest.mkdir(exist_ok=True)
                        for evidence in ['metrics.csv','stage-profile.json']:
                            if (out/evidence).is_file():shutil.copy2(out/evidence,request_dest/evidence)
                        if (out/'stage-profile.json').exists():entry['profile']=json.loads((out/'stage-profile.json').read_text())
                        report=Path(body.get('report','__missing__'))
                        if report.exists() and report.suffix=='.parquet':
                            f=pq.ParquetFile(report);entry['output_rows']=f.metadata.num_rows
                            entry['output_schema']=str(f.schema_arrow)
                            if f.metadata.num_rows<=3000:
                                with gzip.open(request_dest/'output-rows.json.gz','wt') as stream:
                                    json.dump(pq.read_table(report).to_pylist(),stream,default=str)
                        profiles=list(out.glob('stage-profile-worker-*.jsonl'))
                        if profiles:
                            totals={}
                            for profile in profiles:
                                with profile.open('rb') as src, gzip.open(request_dest/(profile.name+'.gz'),'wb') as dst:
                                    shutil.copyfileobj(src,dst)
                                for line in profile.read_text().splitlines():
                                    e=json.loads(line)
                                    if e.get('event')=='date_timing':
                                        for k,v in e.items():
                                            if k.endswith('_seconds') and isinstance(v,(int,float)):totals[k]=totals.get(k,0)+v
                            entry['summed_worker_elapsed_seconds']=totals
                br['responses'].append(entry)
            record['batches'].append(br)
        record['run_date_ist']=timestamp.isoformat() if timestamp else dt.datetime.fromtimestamp(path.stat().st_mtime,IST).isoformat()
        record['run_date_basis']='server launch timestamp encoded in request directory' if timestamp else 'summary file mtime; approximate'
        runs.append(record)
    (ARCHIVE/'results.json').write_text(json.dumps(runs,indent=2,default=str)+'\n')
    # Preserve every input manifest and generation/scan specification independently
    # of large Parquet and binary inputs scheduled for deletion.
    for path in (ROOT/'results').rglob('*.json'):
        if path.name in ['manifest.json','memory-manifest.json','generate.json','scan.json','dated-sector-validation.json','sector-validation.json']:
            dest=ARCHIVE/'input-manifests'/path.relative_to(ROOT/'results');dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest)
    canonical=[]
    for path in [*sorted((ROOT/'attribution_data_parquet').glob('*')),retained]:
        if not path.is_file():continue
        item={'path':str(path.relative_to(ROOT)),'bytes':path.stat().st_size}
        if path.suffix=='.parquet':
            f=pq.ParquetFile(path);item.update(rows=f.metadata.num_rows,row_groups=f.num_row_groups,schema=str(f.schema_arrow))
        with path.open('rb') as stream:item['sha256']=hashlib.file_digest(stream,'sha256').hexdigest()
        canonical.append(item)
    (ARCHIVE/'canonical-inputs.json').write_text(json.dumps(canonical,indent=2)+'\n')
    tracked=set(subprocess.check_output(['git','ls-files'],cwd=ROOT,text=True).splitlines())
    candidates=[]
    for parent,dirs,names in os.walk(ROOT/'results',followlinks=False):
        for name in names:
            path=Path(parent)/name
            if not path.is_symlink() and path.is_file() and path.suffix in ['.parquet','.csv','.bin'] and path.stat().st_size>=20*1024**2:
                candidates.append(path)
    candidates += [p for p in (ROOT/'attribution_data').glob('*.csv') if p.is_file() and not p.is_symlink()]
    records=[]
    for path in sorted(set(candidates)):
        relative=str(path.relative_to(ROOT))
        if relative in tracked:continue
        stat=path.stat();entry={'path':relative,'bytes':stat.st_size,'allocated_bytes':stat.st_blocks*512,'mtime_ns':stat.st_mtime_ns,'status':'planned'}
        if path.suffix=='.parquet':
            f=pq.ParquetFile(path);entry.update(rows=f.metadata.num_rows,row_groups=f.num_row_groups,schema=str(f.schema_arrow))
        if path.suffix=='.csv':
            with path.open() as stream:entry['header']=stream.readline().strip()
        records.append(entry)
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    journal=ARCHIVE/f'cleanup-{stamp}.json'
    payload={'apply':args.apply,'records':records,'logical_bytes':sum(e['bytes'] for e in records),'allocated_bytes':sum(e['allocated_bytes'] for e in records),
             'rebuild':'python/rebuild_daily_benchmark.py; see README.md for canonical dependencies and legacy limitations',
             'retained':'canonical attribution_data_parquet, historical SMF seed, legacy data/ source fixtures, supplied inputs, all small evidence and outputs, source code, .git, dependencies'}
    def save():journal.write_text(json.dumps(payload,indent=2)+'\n')
    save()
    if args.apply:
        for entry in records:
            path=ROOT/entry['path']
            if path.is_symlink() or path.stat().st_size!=entry['bytes'] or path.stat().st_mtime_ns!=entry['mtime_ns']:raise RuntimeError(f'File changed after inventory: {path}')
            path.unlink();entry['status']='deleted';save()
    lines=['# Benchmark result catalog — archived 2026-11-08','', 'Times below are observed runs, not guarantees. Dates use Asia/Kolkata. Complete commands, inputs, cache policy, schemas, per-response metrics, and stage timings are in [results.json](results.json). Original evidence is under `runs/`.', '', '| Run date (IST) | Run | Aggregate | API requests / successful | Batch seconds | Peak sampled RSS GiB |','|---|---|---|---:|---:|---:|']
    for run in runs:
        responses=[r for b in run['batches'] for r in b['responses']];seconds=', '.join(f"{b['batch_seconds']:.3f}" for b in run['batches'] if isinstance(b.get('batch_seconds'),(int,float)))
        rss=run.get('peak_sampled_rss_bytes') or 0
        lines.append(f"| {run['run_date_ist'][:19]} | {run['run']} | {run['aggregate']} | {len(responses)} / {sum(r.get('status')==200 for r in responses)} | {seconds} | {rss/1024**3:.3f} |")
    (ARCHIVE/'RESULTS.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps({'api_runs_archived':len(runs),'journal':str(journal.relative_to(ROOT)),'apply':args.apply,'selected_files':len(records),'allocated_gib':payload['allocated_bytes']/1024**3},indent=2))


if __name__=='__main__':main()
