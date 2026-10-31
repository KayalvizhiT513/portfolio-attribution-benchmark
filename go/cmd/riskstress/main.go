package main

import (
 "encoding/binary"
 "encoding/json"
 "flag"
 "fmt"
 "math"
 "os"
 "path/filepath"
 "runtime"
 "sort"
 "sync"
 "time"
)
type Meta struct {Days int `json:"days"`; Securities int `json:"securities"`}
func load(path string, n int) []float64 {
 f,err:=os.Open(path); if err!=nil {panic(err)}; defer f.Close()
 a:=make([]float64,n)
 if err:=binary.Read(f,binary.LittleEndian,a);err!=nil{panic(err)}
 return a
}
func simulate(wp,wb,rp,rb,out []float64,days,secs,scenarios,steps,job int) {
 for d:=0;d<days;d++ {
  start:=d*secs
  for scenario:=0;scenario<scenarios;scenario++ {
   shock:=(float64(scenario)-float64(scenarios-1)*0.5)*0.001+float64(job)*0.0001
   total:=0.0
   for i:=0;i<secs;i++ {
    idx:=start+i
    p,b:=1.0,1.0
    baseP:=rp[idx]+shock
    baseB:=rb[idx]+shock*0.6
    for k:=0;k<steps;k++ {
     drift:=float64(k%5-2)*0.00012
     p*=1.0+baseP+drift
     b*=1.0+baseB-drift
    }
    total+=wp[idx]*(p-1.0)-wb[idx]*(b-1.0)
   }
   out[d*scenarios+scenario]=total
  }
 }
}
func sum(v []float64) float64 {s:=0.0;for _,x:=range v{s+=x};return s}
func main() {
 folder:=flag.String("data","data/100k","fixture directory")
 jobs:=flag.Int("jobs",10,"independent jobs")
 concurrency:=flag.Int("concurrency",4,"concurrent jobs")
 scenarios:=flag.Int("scenarios",8,"scenarios per job")
 steps:=flag.Int("steps",12,"compounding steps per scenario")
 runs:=flag.Int("runs",3,"repetitions")
 flag.Parse()
 if *jobs<1||*concurrency<1||*scenarios<1||*steps<1||*runs<1 {panic("all arguments must be positive")}
 c:=*concurrency;if c>*jobs{c=*jobs}
 runtime.GOMAXPROCS(c)
 raw,err:=os.ReadFile(filepath.Join(*folder,"metadata.json"));if err!=nil{panic(err)}
 var m Meta;if err:=json.Unmarshal(raw,&m);err!=nil{panic(err)}
 n:=m.Days*m.Securities
 wp:=load(filepath.Join(*folder,"wp.bin"),n)
 wb:=load(filepath.Join(*folder,"wb.bin"),n)
 rp:=load(filepath.Join(*folder,"rp.bin"),n)
 rb:=load(filepath.Join(*folder,"rb.bin"),n)
 outs:=make([][]float64,c);for i:=range outs{outs[i]=make([]float64,m.Days**scenarios)}
 simulate(wp,wb,rp,rb,outs[0],m.Days,m.Securities,*scenarios,*steps,0)
 durations:=make([]float64,*runs)
 var checksum float64
 for rep:=range durations {
  checks:=make([]float64,c)
  start:=time.Now()
  var wg sync.WaitGroup
  for slot:=0;slot<c;slot++ {
   wg.Add(1)
   go func(slot int) {
    defer wg.Done()
    local:=0.0
    for job:=slot;job<*jobs;job+=c {
     simulate(wp,wb,rp,rb,outs[slot],m.Days,m.Securities,*scenarios,*steps,job)
     local+=sum(outs[slot])
    }
    checks[slot]=local
   }(slot)
  }
  wg.Wait()
  durations[rep]=time.Since(start).Seconds()
  current:=0.0;for _,v:=range checks{current+=v}
  if math.IsNaN(current)||math.IsInf(current,0){panic("nonfinite checksum")}
  if rep>0 && math.Abs(current-checksum)>1e-10{panic("inconsistent checksum")}
  checksum=current
 }
 sorted:=append([]float64(nil),durations...);sort.Float64s(sorted)
 med:=sorted[len(sorted)/2];if len(sorted)%2==0{med=(med+sorted[len(sorted)/2-1])/2}
 report:=map[string]any{
  "language":"go","mode":"risk-scenarios","jobs":*jobs,"concurrency":c,
  "days":m.Days,"securities":m.Securities,"scenarios":*scenarios,"steps":*steps,
  "path_updates":int64(*jobs)*int64(m.Days)*int64(m.Securities)*int64(*scenarios)*int64(*steps)*2,
  "runs_seconds":durations,"median_seconds":med,"jobs_per_second":float64(*jobs)/med,
  "checksum":checksum,"timing_scope":"all stress computations; excludes input loading/output allocation",
 }
 out,_:=json.MarshalIndent(report,"","  ");fmt.Println(string(out))
}
