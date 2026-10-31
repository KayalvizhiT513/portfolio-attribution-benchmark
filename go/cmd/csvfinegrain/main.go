package main

import (
 "encoding/csv"
 "encoding/json"
 "flag"
 "fmt"
 "io"
 "os"
 "path/filepath"
 "runtime"
 "strconv"
 "sync"
 "time"
)
type Key struct{date,id string}
type Record struct{weight,ret float64}
type Task struct{kind uint8; wp,wb,rp,rb float64}
type Result struct{kind uint8; value float64}
func load(path string) map[Key]Record {
 f,e:=os.Open(path);if e!=nil{panic(e)};defer f.Close()
 reader:=csv.NewReader(f);h,e:=reader.Read();if e!=nil{panic(e)}
 expected:=[]string{"date","security_id","weight","return"}
 if len(h)!=len(expected){panic("bad CSV header")}
 for i,v:=range expected{if h[i]!=v{panic("bad CSV header")}}
 out:=make(map[Key]Record)
 for{
  row,e:=reader.Read();if e==io.EOF{break};if e!=nil{panic(e)}
  key:=Key{row[0],row[1]}
  if key.date==""||key.id==""{panic("empty key")}
  if _,found:=out[key];found{panic("duplicate key")}
  w,e:=strconv.ParseFloat(row[2],64);if e!=nil{panic(e)}
  r,e:=strconv.ParseFloat(row[3],64);if e!=nil{panic(e)}
  out[key]=Record{w,r}
 }
 return out
}
func main(){
 folder:=flag.String("data","data/csv-15k","folder containing portfolio.csv and benchmark.csv")
 flag.Parse()
 start:=time.Now()
 pf:=load(filepath.Join(*folder,"portfolio.csv"))
 bm:=load(filepath.Join(*folder,"benchmark.csv"))
 if len(pf)!=len(bm){panic("mismatched key count")}
 workers:=runtime.GOMAXPROCS(0)
 if workers<1{workers=runtime.NumCPU()}
 tasks:=make(chan Task,workers*4)
 results:=make(chan Result,workers*4)
 var wg sync.WaitGroup
 for i:=0;i<workers;i++{
  wg.Add(1)
  go func(){
   defer wg.Done()
   for task:=range tasks{
    if task.kind==0{
     results<-Result{0,(task.wp-task.wb)*task.rb}
    }else{
     results<-Result{1,task.wp*(task.rp-task.rb)}
    }
   }
  }()
 }
 go func(){wg.Wait();close(results)}()
 var collect sync.WaitGroup
 allocation,selection:=0.0,0.0
 collect.Add(1)
 go func(){defer collect.Done();for result:=range results{
  if result.kind==0{allocation+=result.value}else{selection+=result.value}
 }}()
 for key,p:=range pf{
  b,ok:=bm[key];if !ok{panic(fmt.Sprintf("missing benchmark key: %+v",key))}
  // Two independent runnable tasks for EVERY security-day pair.
  tasks<-Task{0,p.weight,b.weight,p.ret,b.ret}
  tasks<-Task{1,p.weight,b.weight,p.ret,b.ret}
 }
 close(tasks)
 collect.Wait()
 out:=map[string]any{
  "language":"go","mode":"csv-security-effect-tasks",
  "workers":workers,"cpu_detected":runtime.NumCPU(),
  "security_day_records":len(pf),"independent_effect_tasks":len(pf)*2,
  "total_allocation":allocation,"total_selection":selection,
  "elapsed_seconds":time.Since(start).Seconds(),
  "timing_scope":"CSV parsing, keyed join, task dispatch and independent effects",
 }
 enc,_:=json.MarshalIndent(out,"","  ");fmt.Println(string(enc))
}
