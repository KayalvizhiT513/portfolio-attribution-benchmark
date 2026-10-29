"""Generate two shuffled Brinson-Fachler input CSVs with segment identifiers."""
import argparse, csv
from pathlib import Path
import numpy as np

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--securities",type=int,default=15000)
    p.add_argument("--days",type=int,default=252)
    p.add_argument("--segments",type=int,default=20)
    p.add_argument("--seed",type=int,default=42)
    p.add_argument("--output",default="data/brinson-15k")
    a=p.parse_args()
    if min(a.securities,a.days,a.segments)<1 or a.segments>a.securities:p.error("invalid counts")
    folder=Path(a.output);folder.mkdir(parents=True,exist_ok=True)
    rng=np.random.default_rng(a.seed)
    # The security-to-segment mapping is stored once, separately from daily holdings.
    with (folder/"security_segments.csv").open("w",newline="") as mf:
        mw=csv.writer(mf);mw.writerow(["security_id","segment_id"])
        for i in range(a.securities):mw.writerow([f"SEC{i+1:06d}",f"SEG{i%a.segments+1:03d}"])
    # Independent row shuffle in each holdings file.
    with (folder/"portfolio.csv").open("w",newline="") as pf, (folder/"benchmark.csv").open("w",newline="") as bm:
        pw,bw=csv.writer(pf),csv.writer(bm)
        pw.writerow(["date","security_id","weight","return"])
        bw.writerow(["date","security_id","weight","return"])
        for d in range(a.days):
            wp=rng.random(a.securities);wp/=wp.sum()
            wb=rng.random(a.securities);wb/=wb.sum()
            rb=rng.uniform(-0.03,0.03,a.securities)
            rp=np.clip(rb+rng.uniform(-0.0005,0.0005,a.securities),-0.03,0.03)
            for i in rng.permutation(a.securities):
                pw.writerow([f"D{d+1:04d}",f"SEC{i+1:06d}",format(wp[i],".17g"),format(rp[i],".17g")])
            for i in rng.permutation(a.securities):
                bw.writerow([f"D{d+1:04d}",f"SEC{i+1:06d}",format(wb[i],".17g"),format(rb[i],".17g")])
    print(f"Generated {a.days*a.securities:,} rows per file; independent security ordering within each date.")

if __name__=="__main__":main()
