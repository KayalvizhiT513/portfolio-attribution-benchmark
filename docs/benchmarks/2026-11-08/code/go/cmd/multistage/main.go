package main
import("encoding/binary";"encoding/json";"flag";"fmt";"math";"os";"path/filepath";"runtime";"sort";"sync";"time")
type Meta struct{Days int `json:"days"`;Securities int `json:"securities"`}
func read(path string,n int)[]float64{f,e:=os.Open(path);if e!=nil{panic(e)};defer f.Close();v:=make([]float64,n);if e=binary.Read(f,binary.LittleEndian,v);e!=nil{panic(e)};return v}
func rolling(rp,rb,tmp []float64,days,secs,window int){
 for d:=0;d<days;d++{start:=d-window+1;if start<0{start=0};count:=float64(d-start+1)
  for i:=0;i<secs;i++{
   mean:=0.0
   for k:=start;k<=d;k++{idx:=k*secs+i;mean+=rp[idx]-rb[idx]}
   mean/=count;variance:=0.0
   for k:=start;k<=d;k++{idx:=k*secs+i;z:=(rp[idx]-rb[idx])-mean;variance+=z*z}
   tmp[d*secs+i]=mean/(1.0+math.Sqrt(variance/count)*100.0)
  }
 }
}
func signal(wp,wb,tmp []float64,job int){threshold:=float64(job+1)*0.000001
 for i,v:=range tmp{if math.Abs(v)>threshold{tmp[i]=(wp[i]-wb[i])*v}else{tmp[i]=0}}
}
func aggregate(tmp,daily []float64,days,secs int){for d:=0;d<days;d++{v:=0.0;for i:=d*secs;i<(d+1)*secs;i++{v+=tmp[i]};daily[d]=v}}
func drawdown(daily []float64)float64{wealth,peak,total:=1.0,1.0,0.0
 for _,v:=range daily{wealth*=1.0+v;if wealth>peak{peak=wealth};total+=1.0-wealth/peak};return total+wealth}
func main(){
 folder:=flag.String("data","data/100k","fixture");jobs:=flag.Int("jobs",10,"jobs");concurrency:=flag.Int("concurrency",4,"active jobs")
 window:=flag.Int("window",20,"rolling window");runs:=flag.Int("runs",3,"runs");flag.Parse()
 if *jobs<1||*concurrency<1||*window<1||*runs<1{panic("positive args required")}
 c:=*concurrency;if c>*jobs{c=*jobs};runtime.GOMAXPROCS(c)
 b,e:=os.ReadFile(filepath.Join(*folder,"metadata.json"));if e!=nil{panic(e)};var m Meta;if e=json.Unmarshal(b,&m);e!=nil{panic(e)}
 n:=m.Days*m.Securities
 wp:=read(filepath.Join(*folder,"wp.bin"),n);wb:=read(filepath.Join(*folder,"wb.bin"),n)
 rp:=read(filepath.Join(*folder,"rp.bin"),n);rb:=read(filepath.Join(*folder,"rb.bin"),n)
 scratch:=make([][]float64,c);daily:=make([][]float64,c)
 for i:=range scratch{scratch[i]=make([]float64,n);daily[i]=make([]float64,m.Days)}
 roll:=func(slot,job int)float64{
  rolling(rp,rb,scratch[slot],m.Days,m.Securities,*window)
  signal(wp,wb,scratch[slot],job)
  aggregate(scratch[slot],daily[slot],m.Days,m.Securities)
  return drawdown(daily[slot])
 }
 roll(0,0)
 durations:=make([]float64,*runs);checksum:=0.0
 for rep:=range durations{
  partial:=make([]float64,c);var wg sync.WaitGroup
  start:=time.Now()
  for slot:=0;slot<c;slot++{wg.Add(1);go func(s int){defer wg.Done();v:=0.0
   for j:=s;j<*jobs;j+=c{v+=roll(s,j)};partial[s]=v}(slot)}
  wg.Wait();durations[rep]=time.Since(start).Seconds()
  current:=0.0;for _,v:=range partial{current+=v}
  if math.IsNaN(current)||math.IsInf(current,0){panic("invalid checksum")}
  if rep>0&&math.Abs(current-checksum)>1e-10{panic("unstable output")}
  checksum=current
 }
 ordered:=append([]float64(nil),durations...);sort.Float64s(ordered)
 med:=ordered[len(ordered)/2];if len(ordered)%2==0{med=(med+ordered[len(ordered)/2-1])/2}
 result:=map[string]any{"language":"go","mode":"dependent-multistage","stages":4,"jobs":*jobs,"concurrency":c,
 "window":*window,"days":m.Days,"securities":m.Securities,"runs_seconds":durations,"median_seconds":med,
 "jobs_per_second":float64(*jobs)/med,"checksum":checksum,
 "timing_scope":"all 4 dependent stages x jobs, excluding input loading/output allocations"}
 jsonbytes,_:=json.MarshalIndent(result,"","  ");fmt.Println(string(jsonbytes))
}
