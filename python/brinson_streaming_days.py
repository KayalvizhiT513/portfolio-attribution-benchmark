"""Brinson–Fachler day-parallel baseline: one independent day per process task."""
import argparse,csv,json,math,os,time,resource
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor,wait,FIRST_COMPLETED,as_completed
from pathlib import Path

FIELDS=["date","security_id","weight","return"]
OUT=["date","segment_id","allocation","selection","interaction","total"]

def groups(path,read_time,segments):
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
                if not sid or sid in rows:raise ValueError("missing or duplicate key")
                if sid not in segments:raise ValueError(f"unmapped security: {sid}")
                weight=float(row["weight"]);ret=float(row["return"])
                if not math.isfinite(weight) or weight<0 or not math.isfinite(ret):raise ValueError("invalid numeric input")
                rows[sid]=(segments[sid],weight,ret)
                t=time.perf_counter();row=next(reader,None);read_time[0]+=time.perf_counter()-t
            pending=row
            yield date,rows

def effect(kind,segment,wp,wb,pr,br,benchmark):
    if kind=="selection":return segment,kind,wb*(pr-br)
    if kind=="interaction":return segment,kind,(wp-wb)*(pr-br)
    return segment,kind,(wp-wb)*(br-benchmark)

def calculate(job):
    index,date,portfolio,benchmark=job
    if portfolio.keys()!=benchmark.keys():raise ValueError(f"security IDs differ on {date}")
    aggregates=defaultdict(lambda:[0.0,0.0,0.0,0.0])
    remaining=defaultdict(int)
    wp_total=rp_total=0.0
    for sid,(seg,wp,rp) in portfolio.items():
        x=aggregates[seg];x[0]+=wp;x[2]+=wp*rp
        wp_total+=wp;rp_total+=wp*rp;remaining[seg]+=1
    if abs(wp_total-1)>1e-9:raise ValueError(f"unnormalized portfolio: {date}")
    wb_total=rb_total=0.0
    futures=[]
    # Date-level workers are separate processes. Inside each day, effect
    # calculations are separate thread tasks released on true dependencies.
    # The thread pool is intentionally local to this day to expose overhead.
    with ThreadPoolExecutor(max_workers=min(len(aggregates)*3,os.cpu_count() or 1)) as executor:
        for sid,(seg,wb,rb) in benchmark.items():
            pseg,_,_=portfolio[sid]
            if pseg!=seg:raise ValueError(f"segment mismatch: {sid}")
            x=aggregates[seg];x[1]+=wb;x[3]+=wb*rb
            wb_total+=wb;rb_total+=wb*rb;remaining[seg]-=1
            if remaining[seg]==0:
                wp,bw,pc,bc=x
                if wp<=0 or bw<=0:raise ValueError("zero-weight segment")
                pr=pc/wp;br=bc/bw
                futures.append(executor.submit(effect,"selection",seg,wp,bw,pr,br,None))
                futures.append(executor.submit(effect,"interaction",seg,wp,bw,pr,br,None))
        if abs(wb_total-1)>1e-9:raise ValueError(f"unnormalized benchmark: {date}")
        for seg,(wp,wb,pc,bc) in aggregates.items():
            futures.append(executor.submit(effect,"allocation",seg,wp,wb,pc/wp,bc/wb,rb_total))
        values={seg:{} for seg in aggregates}
        for future in as_completed(futures):
            seg,kind,value=future.result()
            values[seg][kind]=value
    rows=[];totals=[0.0,0.0,0.0]
    for seg in sorted(values):
        x=values[seg]
        v=(x["allocation"],x["selection"],x["interaction"])
        rows.append((date,seg,*v,sum(v)))
        for i,value in enumerate(v):totals[i]+=value
    if not math.isclose(sum(totals),rp_total-rb_total,rel_tol=1e-8,abs_tol=1e-10):
        raise ValueError(f"reconciliation failed: {date}")
    return index,rows,totals,len(portfolio)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--data",default="data/brinson-15k")
    ap.add_argument("--output",default="results/brinson-python-day.csv")
    ap.add_argument("--architecture",choices=["streaming_days"],default="streaming_days")
    a=ap.parse_args()
    start=time.perf_counter();cpu_start=time.process_time()
    before=resource.getrusage(resource.RUSAGE_CHILDREN)
    child_cpu_start=before.ru_utime+before.ru_stime
    available=(os.process_cpu_count() if hasattr(os,"process_cpu_count") else os.cpu_count()) or 1
    workers=max(1,min(available,int(os.environ.get("BRINSON_CPU_BUDGET",available))))
    out=Path(a.output);out.parent.mkdir(parents=True,exist_ok=True)
    read_time=[0.0];write_time=0.0
    pending=set();completed={}
    with out.open("w",newline="") as f, ProcessPoolExecutor(max_workers=workers) as pool:
        writer=csv.writer(f)
        t=time.perf_counter();writer.writerow(OUT);write_time+=time.perf_counter()-t
        with open(Path(a.data)/"security_segments.csv",newline="") as mapping_file:
            mapping_reader=csv.DictReader(mapping_file)
            if mapping_reader.fieldnames!=["security_id","segment_id"]:raise ValueError("invalid mapping header")
            segments={}
            for mapping in mapping_reader:
                sid=mapping["security_id"];seg=mapping["segment_id"]
                if not sid or not seg or sid in segments:raise ValueError("invalid or duplicate security mapping")
                segments[sid]=seg
        pg=groups(Path(a.data)/"portfolio.csv",read_time,segments)
        bg=groups(Path(a.data)/"benchmark.csv",read_time,segments)
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
    print(json.dumps({"language":"python","mode":"brinson-fachler","architecture":"streaming_days",
        "workers":workers,"days":index,"security_day_records":securities,"segment_days":segment_days,
        "effect_tasks":3*segment_days,"task_granularity":"independent segment-effect tasks within concurrent days",
        "totals":dict(zip(("allocation","selection","interaction"),totals)),
        "pipeline":{"segments_released_before_full_benchmark":0,"allocation_released_after_benchmark_total":True},
        "output_csv":str(out),"elapsed_seconds":elapsed,
        "io_timing":{"input_read_parse_seconds":read_time[0],"output_write_sync_seconds":write_time},
        "cpu_usage":{"process_cpu_seconds":combined_cpu,"average_effective_cores":combined_cpu/elapsed,
        "measurement":"parent and completed worker-process CPU / wall time; not peak simultaneous cores"},
        "timing_scope":"CSV input read+parse+day process tasks+validation+output write+sync"},indent=2))
if __name__=="__main__":main()
