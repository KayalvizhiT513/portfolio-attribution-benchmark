"""Full-history read, attribution and CSV benchmark for five scenarios."""
import argparse
import csv
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
from pathlib import Path
from benchmark_native import measured, subset_file
from test_attribution import compare_files

ROOT=Path(__file__).resolve().parents[1]
SCENARIOS=['security','subset','country','weekly','monthly']
COUNTS={'security':120008,'subset':808,'country':8,'weekly':1264,'monthly':288}
CHECKSUMS=['checksum_allocation','checksum_selection','checksum_interaction']
NUMERIC=['process_wall_seconds','cpu_seconds','peak_rss_mib','average_cores']+[s+'_seconds' for s in ['read_parse','filter','aggregate','compute','write']]


def save(root,runs,checks,metadata):
    (root/'runs.json').write_text(json.dumps(runs,indent=2)+'\n')
    (root/'checks.json').write_text(json.dumps(checks,indent=2)+'\n')
    (root/'environment.json').write_text(json.dumps(metadata,indent=2)+'\n')
    if runs:
        summary=[]
        for scenario in SCENARIOS:
            for language in ['rust','go']:
                samples=[r for r in runs if r['scenario']==scenario and r['language']==language]
                if not samples:continue
                r=dict(samples[0]);r['runs']=len(samples)
                for field in NUMERIC:
                    values=[float(x[field]) for x in samples]
                    r[field]=statistics.median(values);r[field+'_min']=min(values);r[field+'_max']=max(values)
                summary.append(r)
        (root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
        with (root/'runs.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(runs[0]),lineterminator='\n');w.writeheader();w.writerows(runs)


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--output-dir',type=Path,default=ROOT/'results/compact-full-history')
    ap.add_argument('--runs',type=int,default=2)
    args=ap.parse_args()
    if args.runs<2:ap.error('at least two runs required')
    root=args.output_dir.resolve()
    if root.exists():ap.error('choose a new output directory')
    root.mkdir(parents=True)
    data=ROOT/'attribution_data';subset=root/'subset.csv';subset_file(data,subset)
    binaries={'rust':ROOT/'rust/target/release/portfolio-attribution','go':ROOT/'benchmark-go-attribution'}
    metadata={'platform':platform.platform(),'logical_cpus':os.cpu_count(),'days':1095,'runs':args.runs,'thread_policy':'runtime default; resolved threads recorded in each run',
              'cache_policy':'OS-managed; language order alternates; first run included',
              'executables':{l:{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for l,p in binaries.items()},
              'inputs':[{'name':p.name,'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns} for p in sorted(data.glob('*.csv'))]}
    runs=[];checks=[]
    for scenario in SCENARIOS:
        for rep in range(1,args.runs+1):
            languages=['rust','go'] if rep%2 else ['go','rust']
            for language in languages:
                folder=root/scenario/'compact'/language;folder.mkdir(parents=True,exist_ok=True)
                metrics=folder/f'metrics-run-{rep}.csv'
                command=[str(binaries[language]),'--data',str(data),'--scenario',scenario,'--output-dir',str(folder),'--metrics',str(metrics),'--max-days','0','--instruments-file',str(subset)]
                print(f'{scenario} | {language} | full history {rep}/{args.runs}',flush=True)
                usage=measured(command,folder/f'run-{rep}.log')
                with metrics.open(newline='') as f:r=next(csv.DictReader(f))
                assert int(r['days'])==1095 and int(r['report_rows'])==COUNTS[scenario],r
                assert int(r['output_rows'])==COUNTS[scenario]
                r.update({'repetition':rep,**usage});runs.append(r);save(root,runs,checks,metadata)
                print(f"  {usage['process_wall_seconds']:.2f}s; logical rows {r['report_rows']}; written {r['output_rows']}; {int(r['output_bytes'])/1e6:.3f} MB",flush=True)
        base=next(r for r in runs if r['scenario']==scenario)
        for r in [x for x in runs if x['scenario']==scenario]:
            for field in CHECKSUMS:
                assert math.isclose(float(base[field]),float(r[field]),rel_tol=1e-8,abs_tol=1e-10),(scenario,field,base[field],r[field])
        count=0;maximum=0.0
        left=root/scenario/'compact'/'rust';right=root/scenario/'compact'/'go'
        for path in sorted(left.glob(f'{scenario}-portfolio_*.csv')):
            n,delta=compare_files(path,right/path.name);count+=n;maximum=max(maximum,delta)
        assert count==COUNTS[scenario]
        checks.append({'scenario':scenario,'status':'PASS','rows_compared':count,'max_absolute_difference':maximum,'checksum_repetitions':'PASS'})
        save(root,runs,checks,metadata)
        print(f'{scenario}: PASS; every CSV row compared; all repetition checksums agree',flush=True)
    assert len(runs)==len(SCENARIOS)*2*args.runs
    assert all(hashlib.sha256(p.read_bytes()).hexdigest()==metadata['executables'][l]['sha256'] for l,p in binaries.items())
    subprocess.run([__import__('sys').executable,str(ROOT/'python/render_stage_report.py')],check=True,cwd=ROOT)
    print('Completed all five full-history scenarios',flush=True)

if __name__=='__main__':main()
