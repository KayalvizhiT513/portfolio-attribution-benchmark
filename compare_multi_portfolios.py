"""Compare Go baseline vs streaming across independent portfolios, repeated rounds."""
import argparse,csv,json,math,os,statistics,subprocess,time,sys
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path

def check_equal(a,b,tag):
    with open(a,newline="") as f1,open(b,newline="") as f2:
        x=list(csv.DictReader(f1));y=list(csv.DictReader(f2))
    if len(x)!=len(y):raise ValueError(f"{tag}: row count mismatch")
    for r,s in zip(x,y):
        key=(r["date"],r["segment_id"])
        if key!=(s["date"],s["segment_id"]):raise ValueError(f"{tag}: key mismatch {key}")
        for col in ("allocation","selection","interaction","total"):
            if not math.isclose(float(r[col]),float(s[col]),rel_tol=1e-8,abs_tol=1e-11):
                raise ValueError(f"{tag}: {key} {col} mismatch")

def one_job(binary,folder,output,cpu_budget):
    env=os.environ.copy();env["GOMAXPROCS"]=str(cpu_budget)
    run=subprocess.run([str(binary),"--data",str(folder),"--output",str(output),"--architecture",one_job.architecture],
                       capture_output=True,text=True,env=env)
    if run.returncode:raise RuntimeError(f"{folder}: {run.stderr}\n{run.stdout}")
    return json.loads(run.stdout)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--data",default="data/brinson-multi")
    ap.add_argument("--go",default="./benchmark-brinson")
    ap.add_argument("--go-day",default="./benchmark-brinson-day")
    ap.add_argument("--go-stream-days",default="./benchmark-brinson-stream-days")
    ap.add_argument("--language",choices=("go","python"),default="go")
    ap.add_argument("--runs",type=int,default=10)
    ap.add_argument("--parallel",type=int,default=None)
    ap.add_argument("--output-dir",default="results/multi-portfolio")
    args=ap.parse_args()
    folders=sorted(x for x in Path(args.data).glob("portfolio-*") if x.is_dir())
    if len(folders)<2:ap.error("generate at least two portfolio fixtures first")
    if args.runs<2:ap.error("runs must be >=2")
    cores=os.cpu_count() or 1
    parallel=args.parallel or min(len(folders),cores)
    if parallel<1 or parallel>cores:ap.error("parallel must be between 1 and detected logical CPUs")
    per_job=max(1,cores//parallel)
    binary=Path(args.go).resolve()
    day_binary=Path(args.go_day).resolve()
    stream_binary=Path(args.go_stream_days).resolve()
    if args.language=="go" and (not day_binary.exists() or not stream_binary.exists()):
        ap.error("Build both Go executables: ./cmd/brinsonday and ./cmd/brinsonstreamdays")
    output=Path(args.output_dir);output.mkdir(parents=True,exist_ok=True)
    measurements=[]
    for round_number in range(1,args.runs+1):
        order=("baseline","streaming") if round_number%2 else ("streaming","baseline")
        completed={}
        for architecture in order:
            started=time.perf_counter()
            def execute(folder):
                path=output/f"{folder.name}-{architecture}-round{round_number:02d}.csv"
                env=os.environ.copy();env["GOMAXPROCS"]=str(per_job)
                env["BRINSON_CPU_BUDGET"]=str(per_job)
                selected="day_parallel" if architecture=="baseline" else "streaming_days"
                if args.language=="go":
                    command=[str(day_binary if architecture=="baseline" else stream_binary)]
                else:
                    command=[sys.executable,"brinson_day_parallel.py" if architecture=="baseline" else "brinson_streaming_days.py"]
                proc=subprocess.run(command+["--data",str(folder),"--output",str(path),
                                     "--architecture",selected],capture_output=True,text=True,env=env)
                if proc.returncode:raise RuntimeError(f"{architecture}/{folder.name}: {proc.stderr}\n{proc.stdout}")
                return folder.name,path,json.loads(proc.stdout)
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                results=list(executor.map(execute,folders))
            wall=time.perf_counter()-started
            completed[architecture]={name:path for name,path,_ in results}
            record={"round":round_number,"architecture":architecture,"batch_elapsed_seconds":wall,
                    "portfolios":len(folders),"parallel_processes":parallel,"gomaxprocs_per_process":per_job,
                    "sum_job_elapsed_seconds":sum(r["elapsed_seconds"] for _,_,r in results),
                    "sum_read_seconds":sum(r["io_timing"]["input_read_parse_seconds"] for _,_,r in results),
                    "sum_write_seconds":sum(r["io_timing"]["output_write_sync_seconds"] for _,_,r in results),
                    "sum_process_cpu_seconds":sum(r["cpu_usage"]["process_cpu_seconds"] for _,_,r in results),
                    "total_effect_tasks":sum(r["effect_tasks"] for _,_,r in results)}
            record["batch_effective_cores"]=record["sum_process_cpu_seconds"]/wall
            measurements.append(record)
            print(f"Round {round_number} {architecture}: {wall:.3f}s (validated after pair)",flush=True)
        for folder in folders:
            check_equal(completed["baseline"][folder.name],completed["streaming"][folder.name],folder.name)
        print(f"Round {round_number}: all {len(folders)} output pairs numerically match",flush=True)
    with (output/"runs.csv").open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(measurements[0]));w.writeheader();w.writerows(measurements)
    summary=[]
    for arch in ("baseline","streaming"):
        sub=[r for r in measurements if r["architecture"]==arch]
        values=[r["batch_elapsed_seconds"] for r in sub]
        summary.append({"architecture":arch,"runs":len(values),"portfolios":len(folders),
                        "median_batch_seconds":statistics.median(values),
                        "mean_batch_seconds":statistics.mean(values),
                        "min_batch_seconds":min(values),"max_batch_seconds":max(values),
                        "stdev_batch_seconds":statistics.stdev(values),
                        "median_batch_effective_cores":statistics.median(r["batch_effective_cores"] for r in sub),
                        "median_sum_read_seconds":statistics.median(r["sum_read_seconds"] for r in sub),
                        "median_sum_write_seconds":statistics.median(r["sum_write_seconds"] for r in sub)})
    with (output/"summary.csv").open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(summary[0]));w.writeheader();w.writerows(summary)
    (output/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps({"language":args.language,"baseline_architecture":"day_parallel","streaming_architecture":"streaming_days",
                      "logical_cpus":cores,"parallel_processes":parallel,"gomaxprocs_per_process":per_job,
                      "summary":summary},indent=2))

if __name__=="__main__":main()
