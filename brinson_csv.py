"""Brinson-Fachler 3-effect CSV benchmark with explicit dependency scheduling.

All securities must occur in both files. Each day must be contiguous in both
files; security order within a day is arbitrary. Computation is segment-level.
"""
import argparse,csv,json,os,time,math
from itertools import zip_longest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed

def groups(path):
    with open(path,newline="") as f:
        r=csv.DictReader(f)
        if r.fieldnames!=["date","security_id","segment_id","weight","return"]:raise ValueError("incorrect header: "+str(path))
        prev=None;rows={};seen=set()
        for row in r:
            d=row["date"];sid=row["security_id"];seg=row["segment_id"]
            if not d or not sid or not seg:raise ValueError("blank key")
            if prev is not None and d!=prev:
                seen.add(prev)
                if d in seen:raise ValueError("days must be grouped")
                yield prev,rows;rows={}
            prev=d
            if sid in rows:raise ValueError("duplicate security/day")
            w=float(row["weight"]);ret=float(row["return"])
            if not math.isfinite(w) or not math.isfinite(ret) or w<0:raise ValueError("invalid numeric value")
            rows[sid]=(seg,w,ret)
        if prev is not None:yield prev,rows

def timed_groups(path, counter):
    iterator = groups(path)
    while True:
        started = time.perf_counter()
        try:
            item = next(iterator)
        except StopIteration:
            counter[0] += time.perf_counter() - started
            break
        counter[0] += time.perf_counter() - started
        yield item

def benchmark_rows(path, counter):
    with open(path, newline="") as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=["date","security_id","segment_id","weight","return"]:
            raise ValueError("incorrect benchmark header")
        while True:
            start=time.perf_counter()
            try: row=next(reader)
            except StopIteration:
                counter[0]+=time.perf_counter()-start
                return
            counter[0]+=time.perf_counter()-start
            yield row

def effect(name,wp,wb,rp,rb,benchmark):
    if name=="selection":return name,wb*(rp-rb)
    if name=="interaction":return name,(wp-wb)*(rp-rb)
    return name,(wp-wb)*(rb-benchmark)

def main():
    p=argparse.ArgumentParser();p.add_argument("--data",default="data/brinson-15k");p.add_argument("--output",default="results/brinson-python.csv");a=p.parse_args()
    started=time.perf_counter()
    cpu_started=time.process_time()
    cpu=os.process_cpu_count() if hasattr(os,"process_cpu_count") else os.cpu_count()
    workers=max(1,cpu or 1)
    totals={"allocation":0.0,"selection":0.0,"interaction":0.0}
    days=securities=segments_processed=0
    read_time=[0.0]
    write_time=0.0
    output=Path(a.output);output.parent.mkdir(parents=True,exist_ok=True)
    with output.open("w",newline="") as outfile, ThreadPoolExecutor(max_workers=workers) as pool:
        writer=csv.writer(outfile)
        write_start=time.perf_counter()
        writer.writerow(["date","segment_id","allocation","selection","interaction","total"])
        write_time += time.perf_counter()-write_start
        benchmark=iter(benchmark_rows(Path(a.data)/"benchmark.csv",read_time))
        pending=next(benchmark,None)
        seen_dates=set()
        early_segment_days=0
        for date,pr in timed_groups(Path(a.data)/"portfolio.csv",read_time):
            if date in seen_dates:raise ValueError("repeated portfolio date")
            seen_dates.add(date)
            if pending is None or pending["date"]!=date:raise ValueError("mismatched day order")
            agg={}
            remaining={}
            total_wp=portfolio_return=0.0
            for sid,(seg,wp,rp) in pr.items():
                b=agg.setdefault(seg,[0.0,0.0,0.0,0.0])
                b[0]+=wp;b[2]+=wp*rp
                remaining[seg]=remaining.get(seg,0)+1
                total_wp+=wp;portfolio_return+=wp*rp
            if abs(total_wp-1)>1e-9:raise ValueError("portfolio weights not normalized")
            future_to_segment={}
            seen_ids=set()
            total_wb=benchmark_return=0.0
            def submit(segment,name,wp,wb,rp,rb,benchmark_total=None):
                fut=pool.submit(effect,name,wp,wb,rp,rb,benchmark_total)
                future_to_segment[fut]=segment
            # Consume benchmark rows one at a time. As soon as a segment's
            # last matched row arrives, schedule selection and interaction.
            while pending is not None and pending["date"]==date:
                row=pending
                sid=row["security_id"]
                if sid in seen_ids or sid not in pr:raise ValueError("mismatched/duplicate benchmark security")
                seen_ids.add(sid)
                seg,_,_=pr[sid]
                if seg!=row["segment_id"]:raise ValueError("segment mismatch "+sid)
                wb=float(row["weight"]);rb=float(row["return"])
                if not math.isfinite(wb) or not math.isfinite(rb) or wb<0:raise ValueError("invalid benchmark data")
                b=agg[seg];b[1]+=wb;b[3]+=wb*rb
                total_wb+=wb;benchmark_return+=wb*rb
                remaining[seg]-=1
                if remaining[seg]==0:
                    wp,swb,pc,bc=b
                    if wp<=0 or swb<=0:raise ValueError("zero-weight segment")
                    rp=pc/wp;rbseg=bc/swb
                    submit(seg,"selection",wp,swb,rp,rbseg)
                    submit(seg,"interaction",wp,swb,rp,rbseg)
                    early_segment_days+=1
                pending=next(benchmark,None)
            if seen_ids!=pr.keys() or any(remaining.values()):
                raise ValueError("missing benchmark securities")
            if abs(total_wb-1)>1e-9:raise ValueError("benchmark weights not normalized")
            # Global benchmark return is finalized only now: release allocations.
            for seg,(wp,wb,pc,bc) in agg.items():
                submit(seg,"allocation",wp,wb,pc/wp,bc/wb,benchmark_return)
            by_segment={seg:{} for seg in agg}
            daytotal=0.0
            for fut in as_completed(future_to_segment):
                name,val=fut.result()
                by_segment[future_to_segment[fut]][name]=val
                totals[name]+=val;daytotal+=val
            if not math.isclose(daytotal,portfolio_return-benchmark_return,rel_tol=1e-8,abs_tol=1e-10):
                raise ValueError("Brinson reconciliation failed: "+date)
            for seg in sorted(by_segment):
                effects=by_segment[seg]
                write_start=time.perf_counter()
                writer.writerow([date,seg,*[format(effects[name],".17g") for name in ("allocation","selection","interaction")],format(sum(effects.values()),".17g")])
                write_time+=time.perf_counter()-write_start
            days+=1;securities+=len(pr);segments_processed+=len(agg)
        if pending is not None:raise ValueError("extra benchmark dates")
        write_start=time.perf_counter()
        outfile.flush()
        os.fsync(outfile.fileno())
        write_time += time.perf_counter()-write_start
    elapsed=time.perf_counter()-started
    cpu_seconds=time.process_time()-cpu_started
    print(json.dumps({"language":"python","mode":"brinson-fachler","workers":workers,
      "days":days,"security_day_records":securities,"segment_days":segments_processed,
      "effect_tasks":3*segments_processed,"totals":totals,
      "pipeline":{"early_selection_interaction_segment_days":early_segment_days,"allocation_released_after_benchmark_total":True},"output_csv":str(output),
      "io_timing":{"input_read_parse_seconds":read_time[0],"output_write_sync_seconds":write_time},
      "cpu_usage":{"process_cpu_seconds":cpu_seconds,"average_effective_cores":cpu_seconds/elapsed if elapsed else 0.0,"measurement":"process CPU time / wall time; not exact peak concurrent cores"},
      "elapsed_seconds":elapsed,"timing_scope":"CSV input reading+join+aggregation+effect tasks+validation+CSV output write+flush+fsync"},indent=2))
if __name__=="__main__":main()
