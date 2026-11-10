"""Four dependent stages per job: rolling risk -> signals -> daily portfolio -> drawdown."""
import argparse, json, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import numpy as np
from numba import njit

@njit(nogil=True)
def stage1_rolling(rp, rb, intermediate, window):
    days, secs = rp.shape
    for d in range(days):
        start = max(0, d-window+1)
        n = d-start+1
        for i in range(secs):
            mean = 0.0
            for k in range(start, d+1):
                mean += rp[k,i]-rb[k,i]
            mean /= n
            variance = 0.0
            for k in range(start, d+1):
                z = (rp[k,i]-rb[k,i])-mean
                variance += z*z
            intermediate[d,i] = mean / (1.0 + (variance/n)**0.5*100.0)

@njit(nogil=True)
def stage2_signal(wp, wb, intermediate, job):
    days, secs = wp.shape
    threshold = (job+1)*0.000001
    for d in range(days):
        for i in range(secs):
            score = intermediate[d,i]
            # Stage 2 consumes the full stage 1 result.
            intermediate[d,i] = (wp[d,i]-wb[d,i])*score if abs(score)>threshold else 0.0

@njit(nogil=True)
def stage3_aggregate(intermediate, daily):
    for d in range(intermediate.shape[0]):
        value = 0.0
        for i in range(intermediate.shape[1]):
            value += intermediate[d,i]
        daily[d] = value

@njit(nogil=True)
def stage4_drawdown(daily):
    wealth, peak, sum_dd = 1.0, 1.0, 0.0
    for d in range(daily.shape[0]):
        wealth *= 1.0 + daily[d]
        if wealth > peak: peak = wealth
        sum_dd += 1.0 - wealth/peak
    return sum_dd + wealth

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--data",default="data/100k")
    p.add_argument("--jobs",type=int,default=10)
    p.add_argument("--concurrency",type=int,default=4)
    p.add_argument("--window",type=int,default=20)
    p.add_argument("--runs",type=int,default=3)
    args=p.parse_args()
    if min(args.jobs,args.concurrency,args.window,args.runs)<1: p.error("positive arguments required")
    c=min(args.jobs,args.concurrency)
    folder=Path(args.data)
    meta=json.loads((folder/"metadata.json").read_text())
    shape=(meta["days"],meta["securities"])
    wp,wb,rp,rb=(np.memmap(folder/(k+".bin"),dtype="<f8",mode="r",shape=shape)
                  for k in ("wp","wb","rp","rb"))
    scratch=[np.empty(shape,dtype=np.float64) for _ in range(c)]
    daily=[np.empty(shape[0],dtype=np.float64) for _ in range(c)]
    stage1_rolling(rp,rb,scratch[0],args.window)
    stage2_signal(wp,wb,scratch[0],0)
    stage3_aggregate(scratch[0],daily[0])
    stage4_drawdown(daily[0])
    def worker(slot):
        total=0.0
        for job in range(slot,args.jobs,c):
            stage1_rolling(rp,rb,scratch[slot],args.window)
            stage2_signal(wp,wb,scratch[slot],job)
            stage3_aggregate(scratch[slot],daily[slot])
            total+=stage4_drawdown(daily[slot])
        return total
    elapsed=[];baseline=None
    with ThreadPoolExecutor(max_workers=c) as pool:
        for _ in range(args.runs):
            started=time.perf_counter()
            values=list(pool.map(worker,range(c)))
            elapsed.append(time.perf_counter()-started)
            checksum=float(sum(values))
            if not np.isfinite(checksum): raise RuntimeError("invalid checksum")
            if baseline is not None and not np.isclose(baseline,checksum,rtol=1e-10,atol=1e-10): raise RuntimeError("unstable output")
            baseline=checksum
    med=float(np.median(elapsed))
    print(json.dumps({"language":"python-numba","mode":"dependent-multistage","stages":4,"jobs":args.jobs,
      "concurrency":c,"window":args.window,"days":shape[0],"securities":shape[1],
      "runs_seconds":elapsed,"median_seconds":med,"jobs_per_second":args.jobs/med,
      "checksum":baseline,"timing_scope":"all 4 dependent stages x jobs, excluding input loading/JIT/output allocations"},indent=2))
if __name__=="__main__":main()
