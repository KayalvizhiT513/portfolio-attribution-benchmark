"""Create distinct portfolio fixtures sharing one benchmark, without duplicating the benchmark CSV."""
import argparse,csv,math,os,shutil
from pathlib import Path

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source",default="data/brinson-15k")
    ap.add_argument("--output",default="data/brinson-multi")
    ap.add_argument("--portfolios",type=int,default=4)
    a=ap.parse_args()
    if a.portfolios<2:ap.error("require >=2 portfolios")
    source=Path(a.source).resolve();dest=Path(a.output).resolve()
    if source==dest:ap.error("source and output must differ")
    original=source/"portfolio.csv";benchmark=source/"benchmark.csv";mapping=source/"security_segments.csv"
    if not original.is_file() or not benchmark.is_file() or not mapping.is_file():ap.error("source CSVs missing")
    dest.mkdir(parents=True,exist_ok=True)
    for index in range(a.portfolios):
        folder=dest/f"portfolio-{index+1:03d}";folder.mkdir(exist_ok=True)
        btarget=folder/"benchmark.csv"
        if btarget.is_symlink() or btarget.exists():btarget.unlink()
        btarget.symlink_to(os.path.relpath(benchmark,folder))
        mtarget=folder/"security_segments.csv"
        if mtarget.is_symlink() or mtarget.exists():mtarget.unlink()
        mtarget.symlink_to(os.path.relpath(mapping,folder))
        target=folder/"portfolio.csv"
        if index==0:
            shutil.copyfile(original,target)
            continue
        with original.open(newline="") as inp,target.open("w",newline="") as out:
            reader=csv.DictReader(inp);writer=csv.DictWriter(out,fieldnames=reader.fieldnames)
            writer.writeheader();day=None;block=[]
            def flush(rows):
                if not rows:return
                adjusted=[]
                for r in rows:
                    sid=int(r["security_id"].removeprefix("SEC"))
                    d=int(r["date"].removeprefix("D"))
                    factor=0.75+0.50*((sid*37+d*11+index*101)%997)/996
                    w=float(r["weight"])*factor
                    rp=float(r["return"])+0.0001*math.sin(sid*0.01+index+d*0.03)
                    adjusted.append((r,w,rp))
                total=sum(w for _,w,_ in adjusted)
                for row,w,rp in adjusted:
                    row["weight"]=format(w/total,".17g")
                    row["return"]=format(rp,".17g")
                    writer.writerow(row)
            for row in reader:
                if day is not None and row["date"]!=day:
                    flush(block);block=[]
                day=row["date"];block.append(row)
            flush(block)
        print(f"Created {folder}",flush=True)
    print(f"Prepared {a.portfolios} distinct portfolio fixtures sharing {benchmark}")

if __name__=="__main__":main()
