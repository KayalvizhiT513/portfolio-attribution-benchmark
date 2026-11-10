"""Plot recorded API durations, aligned to their shared launch barrier.

The existing run did not record individual absolute start times. Zero therefore
represents the common launch barrier, not measured identical request starts.
"""
import argparse
import csv
import json
import os
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/tmp/portfolio-attribution-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


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
    labels = [f'API {r["request_index"]+1:02}' for r in rows]
    fig, ax = plt.subplots(figsize=(12, 9), facecolor='#f8fafc')
    ax.set_facecolor('#f8fafc')
    colors = ['#2563eb' if status == 200 else '#dc2626' for status in statuses]
    ax.barh(range(len(rows)), durations, height=.65, color=colors, zorder=3)
    ax.set_yticks(range(len(rows)), labels, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlim(0, max(durations)*1.14)
    ax.set_xticks(range(0, 901, 120))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{int(value)//60}:{int(value)%60:02}'))
    ax.set_xlabel('Elapsed time from shared launch barrier (minutes:seconds)', fontsize=10, labelpad=12)
    ax.grid(axis='x', color='#dce3ee', zorder=0)
    ax.tick_params(axis='both', length=0, pad=8, colors='#334155')
    for spine in ax.spines.values():
        spine.set_visible(False)
    for i, duration in enumerate(durations):
        ax.text(duration+8, i, f'{duration:.3f} s', va='center', fontsize=9, color='#334155')
    fig.text(.09,.955,f'Rust attribution · {len(rows)} simultaneous API requests',fontsize=19,weight='bold',color='#0f172a')
    fig.text(.09,.918,f'{summary["workers_per_request"]} workers/request  •  {summary["start_date"]} → {summary["end_date"]}  •  {statuses.count(200)}/{len(rows)} HTTP 200',fontsize=11,color='#475569')
    fig.text(.09,.055,'Bars show measured client request durations. Starts are aligned to the shared launch barrier;',fontsize=9,color='#64748b')
    fig.text(.09,.036,'individual start offsets were not recorded. These are request timings, not internal stage timings.',fontsize=9,color='#64748b')
    fig.subplots_adjust(left=.10,right=.97,top=.88,bottom=.13)
    for extension in ['png','svg','pdf']:
        fig.savefig(args.run/f'gantt.{extension}',dpi=180,facecolor=fig.get_facecolor())
    plt.close(fig)
    with (args.run/'gantt.csv').open('w',newline='') as f:
        w=csv.writer(f)
        w.writerow(['request','assumed_start_offset_seconds','measured_duration_seconds','http_status'])
        for label,duration,status in zip(labels,durations,statuses):
            w.writerow([label,0,duration,status])
    print(f'Created {args.run}/gantt.png, gantt.svg, gantt.pdf and gantt.csv')

if __name__=='__main__':
    main()
