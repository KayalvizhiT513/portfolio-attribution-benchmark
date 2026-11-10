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
type entry struct{w,r float64}
type group struct{date string; rows map[string]entry}
type reader struct{f *os.File; csv *csv.Reader; buffered []string; prev map[string]bool; path string}
func newReader(path string)*reader{
 f,err:=os.Open(path);if err!=nil{panic(err)}
 r:=csv.NewReader(f);h,err:=r.Read();if err!=nil{panic(err)}
 expected:=[]string{"date","security_id","weight","return"}
 if len(h)!=4{panic("unexpected CSV header")}
 for i:=range expected{if h[i]!=expected[i]{panic("unexpected CSV header")}}
 return &reader{f:f,csv:r,prev:make(map[string]bool),path:path}
}
func (r *reader)close(){r.f.Close()}
func (r *reader)next()(group,bool){
 first:=r.buffered;r.buffered=nil
 if first==nil{v,err:=r.csv.Read();if err==io.EOF{return group{},false};if err!=nil{panic(err)};first=v}
 date:=first[0];if date==""{panic("empty date")}
 if r.prev[date]{panic("date not grouped: "+date)}
 rows:=make(map[string]entry)
 add:=func(row []string){
  if row[1]==""{panic("empty security ID")}
  if _,ok:=rows[row[1]];ok{panic("duplicate security: "+row[1])}
  w,e:=strconv.ParseFloat(row[2],64);if e!=nil{panic(e)}
  ret,e:=strconv.ParseFloat(row[3],64);if e!=nil{panic(e)}
  rows[row[1]]=entry{w,ret}
 }
 add(first)
 for{
  row,err:=r.csv.Read()
  if err==io.EOF{break};if err!=nil{panic(err)}
  if row[0]!=date{r.buffered=row;break}
  add(row)
 }
 r.prev[date]=true
 return group{date,rows},true
}
type task struct{kind uint8; pf,bm map[string]entry}
type result struct{kind uint8; value float64}
func worker(tasks <-chan task, results chan<- result, wg *sync.WaitGroup){
 defer wg.Done()
 for t:=range tasks{
  total:=0.0
  for key,p:=range t.pf{
   b,ok:=t.bm[key];if !ok{panic("missing security")}
   if t.kind==0{total+=(p.w-b.w)*b.r}else{total+=p.w*(p.r-b.r)}
  }
  results<-result{t.kind,total}
 }
}
func main(){
 folder:=flag.String("data","data/csv-15k","directory holding portfolio.csv and benchmark.csv");flag.Parse()
 start:=time.Now()
 cpu:=runtime.GOMAXPROCS(0)
 if cpu<1{cpu=runtime.NumCPU()}
 pf:=newReader(filepath.Join(*folder,"portfolio.csv"));defer pf.close()
 bm:=newReader(filepath.Join(*folder,"benchmark.csv"));defer bm.close()
 tasks:=make(chan task,cpu)
 results:=make(chan result,cpu)
 var wg sync.WaitGroup
 for i:=0;i<cpu;i++{wg.Add(1);go worker(tasks,results,&wg)}
 go func(){wg.Wait();close(results)}()
 // Collect concurrently so result sends cannot block the producer.
 var resultsWG sync.WaitGroup
 allocation,selection:=0.0,0.0
 resultsWG.Add(1)
 go func(){defer resultsWG.Done();for v:=range results{
  if v.kind==0{allocation+=v.value}else{selection+=v.value}
 }}()
 days,records:=0,0
 for{
  pg,pok:=pf.next();bg,bok:=bm.next()
  if pok!=bok{panic("portfolio and benchmark date counts differ")}
  if !pok{break}
  if pg.date!=bg.date{panic(fmt.Sprintf("date mismatch: %s vs %s",pg.date,bg.date))}
  if len(pg.rows)!=len(bg.rows){panic("security count mismatch on "+pg.date)}
  for key:=range pg.rows{if _,ok:=bg.rows[key];!ok{panic("missing benchmark security: "+key)}}
  // Separate effects are independently schedulable, but share input maps.
  tasks<-task{0,pg.rows,bg.rows}
  tasks<-task{1,pg.rows,bg.rows}
  days++;records+=len(pg.rows)
 }
 close(tasks);resultsWG.Wait()
 elapsed:=time.Since(start).Seconds()
 output:=map[string]any{"language":"go","mode":"csv-keyed-effect-tasks",
 "workers":cpu,"cpu_detected":runtime.NumCPU(),"days":days,
 "security_day_records":records,"total_allocation":allocation,"total_selection":selection,
 "elapsed_seconds":elapsed,"timing_scope":"CSV read + parse + keyed match + compute + dispatch"}
 enc,_:=json.MarshalIndent(output,"","  ");fmt.Println(string(enc))
}
