"""Validate Rust worker events and export slot handoffs and worker queue timelines."""
import argparse
import csv
import json
import os
from collections import defaultdict
from pathlib import Path

os.environ.setdefault('MPLCONFIGDIR', '/tmp/portfolio-attribution-matplotlib')


def collect(root):
    events = [json.loads(line) for line in (root/'worker-trace.jsonl').read_text().splitlines()]
    experiment = json.loads((root/'summary.json').read_text())
    responses = [json.loads(line) for line in (root/'responses.jsonl').read_text().splitlines()]
    api_ids = {r['response']['body']['request_id']:r['request_index']+1 for r in responses if r['response']['status']==200}
    limit = next(s['health']['body']['worker_limit'] for s in experiment['samples'] if s['health']['status']==200)
    groups = defaultdict(dict)
    active = peak = 0
    admission = []
    for e in events:
        assert e['event'] not in groups[e['ticket']], 'duplicate event'
        groups[e['ticket']][e['event']] = e
        if e['event']=='admitted': active+=1; admission.append(e['ticket'])
        if e['event']=='released': active-=1
        peak=max(peak,active)
        assert 0<=active<=limit, 'slot cap violated'
    assert active==0 and admission==sorted(groups), 'incomplete/non-FIFO trace'
    origin=min(e['seconds'] for e in events)
    rows=[]; previous={}
    for rank,ticket in enumerate(admission,1):
        g=groups[ticket]; assert set(g)=={'queued','admitted','released'}
        q,a,r=[g[k] for k in ['queued','admitted','released']]
        assert q['seconds']<=a['seconds']<=r['seconds']
        for e in [a,r]:
            assert all(e[k]==q[k] for k in ['request_id','worker_id','rust_thread_id','first_date','last_date'])
        assert r['slot_id']==a['slot_id']
        slot=a['slot_id']; assert 1<=slot<=limit
        api=api_ids[q['request_id']]
        old=previous.get(slot)
        if old: assert old['released_s']<=a['seconds']-origin, 'overlapping slot holders'
        row=dict(rank=rank,api=api,worker=q['worker_id'],ticket=ticket,slot=slot,
                 queued_s=q['seconds']-origin,admitted_s=a['seconds']-origin,released_s=r['seconds']-origin,
                 wait_s=a['seconds']-q['seconds'],held_s=r['seconds']-a['seconds'],
                 previous_api=old['api'] if old else None,previous_worker=old['worker'] if old else None,
                 thread=q['rust_thread_id'],first_date=q['first_date'],last_date=q['last_date'])
        previous[slot]=row; rows.append(row)
    for api in api_ids.values():
        assert sorted(r['worker'] for r in rows if r['api']==api)==list(range(1,13)), 'expected 12 workers/API'
    volumes=root/'worker-volumes.json'
    if volumes.exists():
        volume_data=json.loads(volumes.read_text())
        by_worker={(r['api'],r['worker']):r for r in volume_data['workers']}
        for row in rows:
            volume=by_worker[(row['api'],row['worker'])]
            row.update({k:v for k,v in volume.items() if k not in row})
    stats=dict(apis=len(api_ids),workers=len(rows),events=len(events),slots=limit,peak_holders=peak,
               all_workers_released=True,fifo_verified=True,slot_overlap=False,
               seconds=max(r['released_s'] for r in rows),
               note='Application slot occupancy includes reading, waiting and computation; this is not a CPU scheduler trace.')
    return rows,stats


def static_plot(root,rows,stats):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    cmap=plt.get_cmap('tab20')
    fig,ax=plt.subplots(figsize=(19,8),facecolor='#f8fafc')
    ax.set_facecolor('#f8fafc')
    for r in rows:
        color=cmap((r['api']-1)%20)
        ax.barh(r['slot'],r['held_s'],left=r['admitted_s'],height=.72,color=color,edgecolor='white',linewidth=.6,zorder=3)
        if r['held_s']>stats['seconds']*.022:
            ax.text(r['admitted_s']+r['held_s']/2,r['slot'],f'{r["api"]:02}.{r["worker"]:02}' + (f'\n{r["total_input_rows"]/1e6:.2f}M rows' if 'total_input_rows' in r else ''),ha='center',va='center',fontsize=5.8,color='#111827')
    ax.set_yticks(range(1,stats['slots']+1),[f'Slot {n:02}' for n in range(1,stats['slots']+1)])
    ax.invert_yaxis(); ax.set_xlim(0,stats['seconds']*1.005)
    ax.set_xlabel('Seconds since the first worker queued',labelpad=10)
    ax.grid(axis='x',color='#dbe3ef',zorder=0)
    for spine in ax.spines.values(): spine.set_visible(False)
    ax.tick_params(length=0,pad=8)
    fig.suptitle(f'How {stats["workers"]} Rust date workers competed for {stats["slots"]} shared slots',fontsize=20,weight='bold',x=.07,ha='left',y=.97)
    fig.text(.07,.905,'Each block is one worker’s full slot occupancy. Label 03.07 means API 03 / worker 07.',fontsize=11,color='#475569')
    fig.legend(handles=[Patch(facecolor=cmap((i-1)%20),label=f'API {i:02}') for i in range(1,stats['apis']+1)],loc='lower center',ncol=10,frameon=False,bbox_to_anchor=(.5,.04),fontsize=9)
    fig.text(.07,.018,'Slots are application permits, not CPU cores. Bars include I/O waits and OS preemption. All event boundaries are measured.',fontsize=10,color='#64748b')
    fig.subplots_adjust(left=.07,right=.98,top=.86,bottom=.19)
    for extension in ['png','svg','pdf']:
        fig.savefig(root/f'worker-slots.{extension}',dpi=170,facecolor=fig.get_facecolor())
    plt.close(fig)


HTML = r'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Rust worker competition</title>
<style>body{font:15px system-ui,sans-serif;background:#f8fafc;color:#172033;margin:30px auto;max-width:1450px;padding:0 20px}h1{font-size:29px;margin-bottom:8px}h2{font-size:20px}p{color:#475569;line-height:1.5}.card{background:white;border:1px solid #dce3ef;border-radius:12px;padding:18px;margin:20px 0;overflow:auto}select{padding:8px;font:inherit}svg{width:100%;min-width:1000px;display:block}text{font:11px system-ui;fill:#334155}.scroll{max-height:560px;overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px;white-space:nowrap}th,td{padding:9px;text-align:left;border-bottom:1px solid #e2e8f0}th{position:sticky;top:0;background:#eef2f8}.legend{display:flex;gap:12px;flex-wrap:wrap;margin:12px 0}.legend span{font-size:12px}.chip{display:inline-block;width:12px;height:12px;margin-right:4px;border-radius:2px}.note{background:#eef2ff;padding:12px;border-radius:8px}button{border:0;background:#e2e8f0;padding:7px;border-radius:5px;cursor:pointer}</style>
<h1>How each API’s 12 workers competed</h1><p id="summary"></p><p class="note">These are measured application-slot events, <b>not CPU execution events</b>. A colored bar means the worker held its slot, including input waits and times macOS paused its threads. Gray means queued for a slot.</p>
<div class="card"><h2>Who occupied each of the 12 slots?</h2><p>Time flows left to right. Hover for worker identity, wait time, date range and the previous holder of that slot. Click a block to inspect its API.</p><div id="legend" class="legend"></div><div id="slots"></div></div>
<div class="card"><label for="api">Inspect API: </label><select id="api"><option value="all">All 20 APIs / 240 workers</option></select> <button id="reset">Show all</button><p id="selection"></p><div class="scroll" id="workers"></div></div>
<div class="card"><h2>Exact admission and slot handoff order</h2><p>“Previous holder” means the worker that last held the same application slot, not the thread that previously ran on a CPU.</p><div class="scroll"><table><thead><tr><th>Admit #</th><th>API / worker</th><th>Slot</th><th>Queued s</th><th>Admitted s</th><th>Released s</th><th>Wait s</th><th>Held s</th><th>Previous holder</th><th>Rust thread ID</th><th>Date range</th><th>Dates</th><th>Input rows</th><th>SMF rows</th><th>Analytics rows</th><th>Portfolio rows</th><th>Benchmark rows</th></tr></thead><tbody id="order"></tbody></table></div></div>
<script>
const data=__DATA__,stats=__STATS__;
const colors=['#1f77b4','#aec7e8','#ff7f0e','#ffbb78','#2ca02c','#98df8a','#d62728','#ff9896','#9467bd','#c5b0d5','#8c564b','#c49c94','#e377c2','#f7b6d2','#7f7f7f','#c7c7c7','#bcbd22','#dbdb8d','#17becf','#9edae5'];
const fmt=x=>x.toFixed(3), label=r=>`API ${String(r.api).padStart(2,'0')} / W${String(r.worker).padStart(2,'0')}`;
const volume=r=>r.total_input_rows===undefined?'Volume not recorded':`${r.dates} dates · ${r.total_input_rows.toLocaleString()} total input rows\nSMF ${r.security_master_rows.toLocaleString()} · analytics ${r.analytics_rows.toLocaleString()}\nPortfolio ${r.portfolio_PF_rows.toLocaleString()} · benchmark ${r.index_rows.toLocaleString()}\nAccounts ${r.accounts_rows.toLocaleString()} · FX ${r.forex_rows.toLocaleString()}\nBasis: ${r.volume_basis}`;
const previous=r=>r.previous_api===null?'Initially free':`API ${String(r.previous_api).padStart(2,'0')} / W${String(r.previous_worker).padStart(2,'0')}`;
const width=1380,left=145,right=15,plot=width-left-right,x=t=>left+t/stats.seconds*plot;
const ns='http://www.w3.org/2000/svg';
function node(tag,attrs,text){let e=document.createElementNS(ns,tag);for(let [k,v] of Object.entries(attrs||{}))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e;}
function base(height){let s=node('svg',{viewBox:`0 0 ${width} ${height}`});for(let t=0;t<=stats.seconds;t+=20){s.append(node('line',{x1:x(t),x2:x(t),y1:25,y2:height-20,stroke:'#e2e8f0'}));s.append(node('text',{x:x(t),y:16,'text-anchor':'middle'},`${t}s`));}return s;}
function bar(s,r,y,wait=false){let a=wait?r.queued_s:r.admitted_s,b=wait?r.admitted_s:r.released_s;let e=node('rect',{x:x(a),y,width:Math.max(.4,x(b)-x(a)),height:19,fill:wait?'#d5dce8':colors[r.api-1],rx:2});e.append(node('title',{},`${label(r)} · slot ${r.slot}\nQueued ${fmt(r.queued_s)}s · admitted ${fmt(r.admitted_s)}s · released ${fmt(r.released_s)}s\nQueue wait ${fmt(r.wait_s)}s · slot held ${fmt(r.held_s)}s\nPrevious holder: ${previous(r)}\n${volume(r)}\n${r.first_date} → ${r.last_date}\n${r.thread}`));if(!wait)e.addEventListener('click',()=>{document.querySelector('#api').value=r.api;render();});s.append(e);return e;}
function render(){let chosen=document.querySelector('#api').value,rows=chosen==='all'?data:data.filter(r=>r.api===Number(chosen));
let overview=base(410);for(let n=1;n<=stats.slots;n++)overview.append(node('text',{x:left-12,y:35+(n-1)*29+14,'text-anchor':'end'},`Slot ${n}`));for(let r of data){let b=bar(overview,r,35+(r.slot-1)*29);if(chosen!=='all'&&r.api!==Number(chosen))b.setAttribute('opacity','.18');}document.querySelector('#slots').replaceChildren(overview);
let workers=base(rows.length*25+55);rows=rows.slice().sort((a,b)=>a.api-b.api||a.worker-b.worker);rows.forEach((r,i)=>{let y=30+i*25;workers.append(node('text',{x:left-12,y:y+14,'text-anchor':'end'},label(r)));bar(workers,r,y,true);bar(workers,r,y);});document.querySelector('#workers').replaceChildren(workers);
document.querySelector('#selection').textContent=`${rows.length} workers shown. Gray: waiting for admission. Color: holding a slot until the full assigned date range finishes.`;
document.querySelector('#order').replaceChildren(...data.filter(r=>chosen==='all'||r.api===Number(chosen)).map(r=>{let tr=document.createElement('tr');for(let value of [r.rank,label(r),r.slot,fmt(r.queued_s),fmt(r.admitted_s),fmt(r.released_s),fmt(r.wait_s),fmt(r.held_s),previous(r),r.thread,`${r.first_date} → ${r.last_date}`,r.dates??'—',r.total_input_rows?.toLocaleString()??'—',r.security_master_rows?.toLocaleString()??'—',r.analytics_rows?.toLocaleString()??'—',r.portfolio_PF_rows?.toLocaleString()??'—',r.index_rows?.toLocaleString()??'—']){let td=document.createElement('td');td.textContent=value;tr.append(td);}return tr;}));}
for(let i=1;i<=stats.apis;i++){let o=document.createElement('option');o.value=i;o.textContent=`API ${String(i).padStart(2,'0')} / 12 workers`;document.querySelector('#api').append(o);let sp=document.createElement('span');sp.innerHTML=`<i class="chip" style="background:${colors[i-1]}"></i>API ${String(i).padStart(2,'0')}`;document.querySelector('#legend').append(sp);}
document.querySelector('#summary').textContent=`${stats.apis} APIs · ${stats.workers} date workers · ${stats.slots} shared slots · ${stats.seconds.toFixed(2)} seconds from first queue event to last slot release. FIFO and non-overlapping slot occupancy verified.`;
document.querySelector('#api').addEventListener('change',render);document.querySelector('#reset').onclick=()=>{document.querySelector('#api').value='all';render();};render();
</script></html>'''


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,default=Path('results/api/rust-worker-race-20'))
    args=p.parse_args(); rows,stats=collect(args.run)
    with (args.run/'worker-order.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    (args.run/'worker-validation.json').write_text(json.dumps(stats,indent=2)+'\n')
    (args.run/'worker-race.html').write_text(HTML.replace('__DATA__',json.dumps(rows)).replace('__STATS__',json.dumps(stats)))
    static_plot(args.run,rows,stats)
    print(json.dumps(stats,indent=2));print(f'Interactive timeline: {args.run}/worker-race.html')

if __name__=='__main__': main()
