"""Repeated paired benchmark with numerical CSV validation and summary statistics."""
import argparse,csv,json,math,statistics,subprocess,sys
from pathlib import Path

def read_rows(path):
    with open(path,newline="") as f:
        reader=csv.DictReader(f)
        expected=["date","segment_id","allocation","selection","interaction","total"]
        if reader.fieldnames!=expected:raise ValueError(f"unexpected output CSV header: {path}")
        rows={}
        for r in reader:
            key=(r["date"],r["segment_id"])
            if key in rows:raise ValueError(f"duplicate output row: {key}")
            rows[key]=tuple(float(r[k]) for k in expected[2:])
        return rows

def compare(reference,candidate,tag):
    if reference.keys()!=candidate.keys():raise ValueError(f"output keys differ: {tag}")
    for key,expected in reference.items():
        for x,y in zip(expected,candidate[key]):
            if not math.isclose(x,y,rel_tol=1e-8,abs_tol=1e-11):
                raise ValueError(f"output mismatch: {tag}, {key}: {x} vs {y}")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--data",default="data/brinson-15k")
    p.add_argument("--go",default="./benchmark-brinson")
    p.add_argument("--runs",type=int,default=10)
    p.add_argument("--output-dir",default="results/architecture-comparison")
    a=p.parse_args()
    if a.runs<2:p.error("use at least 2 measured runs")
    folder=Path(a.output_dir);folder.mkdir(parents=True,exist_ok=True)
    variants=[("python","baseline"),("python","streaming"),("go","baseline"),("go","streaming")]
    records=[];reference=None
    # Deterministic rotating order: four variants alternate start positions.
    for index in range(a.runs):
        order=variants[index%4:]+variants[:index%4]
        for language,arch in order:
            output=folder/f"{language}-{arch}-run{index+1:02d}.csv"
            cmd=([sys.executable,"brinson_csv.py"] if language=="python" else [a.go])
            cmd+=["--architecture",arch,"--data",a.data,"--output",str(output)]
            process=subprocess.run(cmd,capture_output=True,text=True)
            if process.returncode:
                raise RuntimeError(f"{language}/{arch} run {index+1} failed:\n{process.stderr}\n{process.stdout}")
            result=json.loads(process.stdout)
            rows=read_rows(output)
            if reference is None:reference=rows
            else:compare(reference,rows,f"{language}/{arch} run {index+1}")
            records.append({"run":index+1,"language":language,"architecture":arch,
                "elapsed_seconds":result["elapsed_seconds"],
                "read_seconds":result["io_timing"]["input_read_parse_seconds"],
                "write_seconds":result["io_timing"]["output_write_sync_seconds"],
                "average_effective_cores":result["cpu_usage"]["average_effective_cores"],
                "workers":result["workers"],
                "early_segments":result["pipeline"]["segments_released_before_full_benchmark"]})
            print(f"Validated {language}/{arch} #{index+1}: {result['elapsed_seconds']:.4f}s",flush=True)
    columns=list(records[0])
    with (folder/"runs.csv").open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=columns);w.writeheader();w.writerows(records)
    summary=[]
    for language,arch in variants:
        subset=[r for r in records if r["language"]==language and r["architecture"]==arch]
        times=[r["elapsed_seconds"] for r in subset]
        summary.append({"language":language,"architecture":arch,"runs":len(times),
            "median_seconds":statistics.median(times),"mean_seconds":statistics.mean(times),
            "min_seconds":min(times),"max_seconds":max(times),
            "stddev_seconds":statistics.stdev(times),
            "median_read_seconds":statistics.median(r["read_seconds"] for r in subset),
            "median_write_seconds":statistics.median(r["write_seconds"] for r in subset),
            "median_effective_cores":statistics.median(r["average_effective_cores"] for r in subset)})
    with (folder/"summary.csv").open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(summary[0]));w.writeheader();w.writerows(summary)
    (folder/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps({"validated_output_rows":len(reference),"summary":summary,"output_dir":str(folder)},indent=2))

if __name__=="__main__":main()
