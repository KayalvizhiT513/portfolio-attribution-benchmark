"""Plot API durations using measured request start offsets when available."""
import argparse
import csv
import json
import os
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/tmp/portfolio-attribution-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run', type=Path, default=Path('results/api/rust-full-20'))
    p.add_argument('--phase', default='load-c20-r1')
    args = p.parse_args()
    rows = sorted([json.loads(line) for line in (args.run/'responses.jsonl').read_text().splitlines()
                   if json.loads(line)['phase'] == args.phase], key=lambda row: row['request_index'])
    if not rows:
        p.error('no responses for selected phase')
    summary = json.loads((args.run/'summary.json').read_text())
    durations = [r['response']['seconds'] for r in rows]
    statuses = [r['response']['status'] for r in rows]
    measured = all('start_offset_seconds' in r['response'] for r in rows)
    starts = [r['response'].get('start_offset_seconds',0) for r in rows]
    ends = [start+duration for start,duration in zip(starts,durations)]
    labels = [f'API {r["request_index"]+1:02}' for r in rows]
    fig, ax = plt.subplots(figsize=(12, 9), facecolor='#f8fafc')
    ax.set_facecolor('#f8fafc')
    colors = ['#2563eb' if status == 200 else '#dc2626' for status in statuses]
    ax.barh(range(len(rows)), durations, left=starts, height=.65, color=colors, zorder=3)
    ax.set_yticks(range(len(rows)), labels, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlim(0, max(ends)*1.14)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=8, integer=True))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{int(value)//60}:{int(value)%60:02}'))
    ax.set_xlabel('Elapsed time from shared launch barrier (minutes:seconds)', fontsize=10, labelpad=12)
    ax.grid(axis='x', color='#dce3ee', zorder=0)
    ax.tick_params(axis='both', length=0, pad=8, colors='#334155')
    for spine in ax.spines.values():
        spine.set_visible(False)
    for i, duration in enumerate(durations):
        ax.text(ends[i]+max(ends)*.01, i, f'{duration:.3f} s', va='center', fontsize=9, color='#334155')
    command = summary.get('command', [])
    mode = command[command.index('--aggregate')+1] if '--aggregate' in command else 'attribution'
    title = 'sector + security attribution' if mode == 'sector' else mode
    fig.text(.09,.955,f'Rust {title} · {len(rows)} simultaneous API calls',fontsize=18,weight='bold',color='#0f172a')
    fig.text(.09,.918,f'{summary["workers_per_request"]} workers/request  •  {summary["start_date"]} → {summary["end_date"]}  •  {statuses.count(200)}/{len(rows)} HTTP 200',fontsize=11,color='#475569')
    note = 'Bars use measured client start offsets and durations relative to batch launch.' if measured else 'Start offsets unavailable; bars align to the common launch barrier.'
    fig.text(.09,.055,note,fontsize=9,color='#64748b')
    fig.text(.09,.036,'Durations include request admission and worker queue waits; they are not pure computation time.',fontsize=9,color='#64748b')
    fig.subplots_adjust(left=.10,right=.97,top=.88,bottom=.13)
    for extension in ['png','svg','pdf']:
        fig.savefig(args.run/f'gantt.{extension}',dpi=180,facecolor=fig.get_facecolor())
    plt.close(fig)
    with (args.run/'gantt.csv').open('w',newline='') as f:
        w=csv.writer(f)
        w.writerow(['request','start_offset_seconds','measured_duration_seconds','http_status','start_offset_basis'])
        for label,start,duration,status in zip(labels,starts,durations,statuses):
            w.writerow([label,start,duration,status,'measured' if measured else 'assumed zero'])
    print(f'Created {args.run}/gantt.png, gantt.svg, gantt.pdf and gantt.csv')

if __name__=='__main__':
    main()
