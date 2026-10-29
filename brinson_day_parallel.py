"""Brinson–Fachler day-parallel baseline: one independent day per process task."""
import argparse,csv,json,math,os,time,resource
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor,wait,FIRST_COMPLETED
from pathlib import Path

FIELDS=["date","security_id","segment_id","weight","return"]
OUT=["date","segment_id","allocation","selection","interaction","total"]

def groups(path,read_time):
    with open(path,newline="") as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=FIELDS:raise ValueError(f"bad header: {path}")
        seen_dates=set();pending=None
        while True:
            t=time.perf_counter()
            row=pending if pending is not None else next(reader,None)
            read_time[0]+=time.perf_counter()-t
            pending=None
            if row is None:return
            date=row["date"]
            if not date or date in seen_dates:raise ValueError(f"invalid date order: {date}")
            seen_dates.add(date)
            rows={}
            while row is not None and row["date"]==date:
                sid=row["security_id"]
                if not sid or not row["segment_id"] or sid in rows:raise ValueError("missing or duplicate key")
                weight=float(row["weight"]);ret=float(row["return"])
                if not math.isfinite(weight) or weight<0 or not math.isfinite(ret):raise ValueError("invalid numeric input")
                rows[sid]=(row["segment_id"],weight,ret)
                t=time.perf_counter();row=next(reader,None);read_time[0]+=time.perf_counter()-t
            pending=row
            yield date,rows

def calculate(job):
    index,date,portfolio,benchmark=job
    if portfolio.keys()!=benchmark.keys():raise ValueError(f"security IDs differ on {date}")
    aggregates=defaultdict(lambda:[0.0,0.0,0.0,0.0])
    wp_total=wb_total=rp_total=rb_total=0.0
    for sid,(segment,wp,rp) in portfolio.items():
        bseg,wb,rb=benchmark[sid]
        if bseg!=segment:raise ValueError(f"segment mismatch on {date}: {sid}")
        x=aggregates[segment]
        x[0]+=wp;x[1]+=wb;x[2]+=wp*rp;x[3]+=wb*rb
        wp_total+=wp;wb_total+=wb;rp_total+=wp*rp;rb_total+=wb*rb
    if abs(wp_total-1)>1e-9 or abs(wb_total-1)>1e-9:raise ValueError(f"unnormalized day: {date}")
    rows=[];totals=[0.0,0.0,0.0]
    for segment in sorted(aggregates):
        wp,wb,pwr,bwr=aggregates[segment]
        if wp<=0 or wb<=0:raise ValueError("zero-weight segment")
        rp=pwr/wp;rb=bwr/wb
        values=((wp-wb)*(rb-rb_total),wb*(rp-rb),(wp-wb)*(rp-rb))
        rows.append((date,segment,*values,sum(values)))
        for j,value in enumerate(values):totals[j]+=value
    if not math.isclose(sum(totals),rp_total-rb_total,rel_tol=1e-8,abs_tol=1e-10):
        raise ValueError(f"reconciliation failed: {date}")
    return index,rows,totals,len(portfolio)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--data",default="data/brinson-15k")
    ap.add_argument("--output",default="results/brinson-python-day.csv")
    ap.add_argument("--architecture",choices=["day_parallel"],default="day_parallel")
    a=ap.parse_args()
    start=time.perf_counter();cpu_start=time.process_time()
    before=resource.getrusage(resource.RUSAGE_CHILDREN)
    child_cpu_start=before.ru_utime+before.ru_stime
    workers=max(1,(os.process_cpu_count() if hasattr(os,"process_cpu_count") else os.cpu_count()) or 1)
    out=Path(a.output);out.parent.mkdir(parents=True,exist_ok=True)
    read_time=[0.0];write_time=0.0
    pending=set();completed={}
    with out.open("w",newline="") as f, ProcessPoolExecutor(max_workers=workers) as pool:
        writer=csv.writer(f)
        t=time.perf_counter();writer.writerow(OUT);write_time+=time.perf_counter()-t
        pg=groups(Path(a.data)/"portfolio.csv",read_time)
        bg=groups(Path(a.data)/"benchmark.csv",read_time)
        index=0
        # Keep submitted days bounded so 252 days do not become 252 large pickled tasks.
        for pday,bday in __import__("itertools").zip_longest(pg,bg):
            if pday is None or bday is None:raise ValueError("day count mismatch")
            date,portfolio=pday;other,benchmark=bday
            if date!=other:raise ValueError("date mismatch")
            pending.add(pool.submit(calculate,(index,date,portfolio,benchmark)))
            index+=1
            if len(pending)>=workers*2:
                done,pending=wait(pending,return_when=FIRST_COMPLETED)
                for future in done:
                    item=future.result();completed[item[0]]=item
        for future in pending:
            item=future.result();completed[item[0]]=item
        totals=[0.0,0.0,0.0];securities=segment_days=0
        for i in range(index):
            _,rows,partial,count=completed[i]
            t=time.perf_counter()
            for row in rows:
                writer.writerow([row[0],row[1],*[format(x,".17g") for x in row[2:]]])
            write_time+=time.perf_counter()-t
            for j in range(3):totals[j]+=partial[j]
            securities+=count;segment_days+=len(rows)
        t=time.perf_counter();f.flush();os.fsync(f.fileno());write_time+=time.perf_counter()-t
    elapsed=time.perf_counter()-start
    cpu_seconds=time.process_time()-cpu_start
    after=resource.getrusage(resource.RUSAGE_CHILDREN)
    child_cpu=(after.ru_utime+after.ru_stime)-child_cpu_start
    combined_cpu=cpu_seconds+child_cpu
    print(json.dumps({"language":"python","mode":"brinson-fachler","architecture":"day_parallel",
        "workers":workers,"days":index,"security_day_records":securities,"segment_days":segment_days,
        "effect_tasks":index,"task_granularity":"one complete trading day",
        "totals":dict(zip(("allocation","selection","interaction"),totals)),
        "pipeline":{"segments_released_before_full_benchmark":0,"allocation_released_after_benchmark_total":True},
        "output_csv":str(out),"elapsed_seconds":elapsed,
        "io_timing":{"input_read_parse_seconds":read_time[0],"output_write_sync_seconds":write_time},
        "cpu_usage":{"process_cpu_seconds":combined_cpu,"average_effective_cores":combined_cpu/elapsed,
        "measurement":"parent and completed worker-process CPU / wall time; not peak simultaneous cores"},
        "timing_scope":"CSV input read+parse+day process tasks+validation+output write+sync"},indent=2))
if __name__=="__main__":main()
