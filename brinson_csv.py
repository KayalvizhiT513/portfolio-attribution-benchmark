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

def effect(name,wp,wb,rp,rb,benchmark):
    if name=="selection":return name,wb*(rp-rb)
    if name=="interaction":return name,(wp-wb)*(rp-rb)
    return name,(wp-wb)*(rb-benchmark)

def main():
    p=argparse.ArgumentParser();p.add_argument("--data",default="data/brinson-15k");a=p.parse_args()
    started=time.perf_counter()
    cpu=os.process_cpu_count() if hasattr(os,"process_cpu_count") else os.cpu_count()
    workers=max(1,cpu or 1)
    totals={"allocation":0.0,"selection":0.0,"interaction":0.0}
    days=securities=segments_processed=0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for pday,bday in zip_longest(groups(Path(a.data)/"portfolio.csv"),groups(Path(a.data)/"benchmark.csv")):
            if pday is None or bday is None:raise ValueError("different day sets")
            date,pr=pday;date2,br=bday
            if date!=date2 or pr.keys()!=br.keys():raise ValueError("mismatched date/security IDs")
            agg={}
            total_wp=total_wb=portfolio_return=benchmark_return=0.0
            for sid,(seg,wp,rp) in pr.items():
                bseg,wb,rb=br[sid]
                if seg!=bseg:raise ValueError("segment mismatch "+sid)
                total_wp+=wp;total_wb+=wb
                portfolio_return+=wp*rp;benchmark_return+=wb*rb
                bucket=agg.setdefault(seg,[0.0,0.0,0.0,0.0])
                bucket[0]+=wp;bucket[1]+=wb;bucket[2]+=wp*rp;bucket[3]+=wb*rb
            if abs(total_wp-1)>1e-9 or abs(total_wb-1)>1e-9:raise ValueError("weights must sum to 1 each day")
            # R_b is now ready. Selection and interaction do not use it.
            # Effects for all segments can be submitted independently.
            jobs=[]
            for wp,wb,pcontrib,bcontrib in agg.values():
                rp=pcontrib/wp if wp else 0.0
                rb=bcontrib/wb if wb else 0.0
                # Dataset generator ensures both universes have positive segment weights.
                if wp<=0 or wb<=0:raise ValueError("zero-weight segment not supported")
                jobs.append(pool.submit(effect,"selection",wp,wb,rp,rb,None))
                jobs.append(pool.submit(effect,"interaction",wp,wb,rp,rb,None))
                jobs.append(pool.submit(effect,"allocation",wp,wb,rp,rb,benchmark_return))
            daytotal=0.0
            for future in as_completed(jobs):
                name,val=future.result()
                totals[name]+=val;daytotal+=val
            if not math.isclose(daytotal,portfolio_return-benchmark_return,rel_tol=1e-8,abs_tol=1e-10):
                raise ValueError(f"Brinson reconciliation failed: {date}")
            days+=1;securities+=len(pr);segments_processed+=len(agg)
    elapsed=time.perf_counter()-started
    print(json.dumps({"language":"python","mode":"brinson-fachler","workers":workers,
      "days":days,"security_day_records":securities,"segment_days":segments_processed,
      "effect_tasks":3*segments_processed,"totals":totals,
      "elapsed_seconds":elapsed,"timing_scope":"CSV read+join+segment aggregation+effect tasks+validation"},indent=2))
if __name__=="__main__":main()
