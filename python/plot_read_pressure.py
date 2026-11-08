"""Plot Rust RSS alongside measured read/decode and input-retention counts."""
import argparse
import json
import os
import re
from pathlib import Path
os.environ.setdefault('MPLCONFIGDIR','/tmp/portfolio-attribution-matplotlib')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,required=True)
    args=parser.parse_args()
    experiment=json.loads((args.run/'summary.json').read_text())
    samples=[s for s in experiment['samples'] if s['health']['status']==200 and 'read_activity' in s['health']['body']]
    if not samples: parser.error('this run has no read-activity measurements')
    t=[s['seconds']/60 for s in samples]
    health=[s['health']['body'] for s in samples]
    kinds=['reading_workers','read_decode_calls','retained_daily_inputs','workers_with_smf_cache']
    stats=dict(worker_limit=health[0]['worker_limit'],
               peak_admitted_workers=max(h['peak_running_date_workers'] for h in health),
               peak_rss_gib=experiment['peak_sampled_rss_bytes']/2**30,
               activity_peaks={k:max(h['read_activity'][k]['peak'] for h in health) for k in kinds},
               final_activity=health[-1]['read_activity'],
               caveat='Read/decode spans include blocking and OS preemption. Retained input counts cover completed non-SMF inputs until their date calculation exits; buffers can be consumed/released earlier internally. Counts are not allocated bytes or CPU memory-read instructions.')
    fig,axes=plt.subplots(4,1,figsize=(13,12),sharex=True,facecolor='#f8fafc')
    for ax in axes:
        ax.set_facecolor('#f8fafc');ax.grid(alpha=.2)
    axes[0].plot(t,[s['rss_bytes']/2**30 for s in samples],label='Rust process RSS',color='#2563eb')
    swapt=[];swap=[]
    for s in samples:
        if 'host_swap' in s:
            m=re.search(r'used = ([\d.]+)([MG])',s['host_swap'])
            if m: swapt.append(s['seconds']/60);swap.append(float(m[1])/(1024 if m[2]=='M' else 1))
    if swap: axes[0].plot(swapt,swap,'--',label='Host-wide swap used',color='#ea580c')
    axes[0].set_ylabel('GiB');axes[0].legend(loc='upper left')
    axes[1].plot(t,[h['running_date_workers'] for h in health],label='Date workers holding slots',color='#475569')
    axes[1].plot(t,[h['read_activity']['reading_workers']['current'] for h in health],label='Workers in read phase',color='#0d9488')
    axes[1].plot(t,[h['read_activity']['workers_with_smf_cache']['current'] for h in health],label='Workers with SMF cache',color='#9333ea',alpha=.65)
    axes[1].set_ylabel('Workers');axes[1].legend(loc='upper left')
    axes[2].plot(t,[h['read_activity']['read_decode_calls']['current'] for h in health],color='#d97706')
    axes[2].set_ylabel('Active read/decode calls')
    axes[2].text(.99,.92,f'Persistent peak: {stats["activity_peaks"]["read_decode_calls"]}',
                 transform=axes[2].transAxes,ha='right',va='top')
    axes[3].plot(t,[h['read_activity']['retained_daily_inputs']['current'] for h in health],color='#db2777')
    axes[3].set_ylabel('Completed daily inputs\nin current date scopes');axes[3].set_xlabel('Minutes since driver launch')
    axes[3].text(.99,.92,f'Persistent peak: {stats["activity_peaks"]["retained_daily_inputs"]}',
                 transform=axes[3].transAxes,ha='right',va='top')
    fig.suptitle(f'Rust memory and input concurrency · shared worker limit {stats["worker_limit"]}',fontsize=17,weight='bold')
    fig.text(.08,.018,'Application phase counts, not CPU utilization. RSS is sampled; activity peaks are persistent counters. Swap is host-wide.',fontsize=9,color='#475569')
    fig.tight_layout(rect=(0,.04,1,.96))
    for ext in ['png','svg','pdf']:fig.savefig(args.run/f'read-pressure.{ext}',dpi=160)
    (args.run/'read-pressure-stats.json').write_text(json.dumps(stats,indent=2)+'\n')
    print(json.dumps(stats,indent=2))

if __name__=='__main__':main()
