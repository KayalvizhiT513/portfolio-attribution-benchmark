package main
import("bufio";"encoding/csv";"encoding/json";"flag";"fmt";"io";"math";"os";"path/filepath";"runtime";"sort";"strconv";"sync";"syscall";"time")
type rec struct{segment string;weight,ret float64}
type reader struct{file *os.File;csv *csv.Reader;next []string;seen map[string]bool}
func open(path string)*reader{f,e:=os.Open(path);if e!=nil{panic(e)};r:=csv.NewReader(f);h,e:=r.Read();if e!=nil{panic(e)}
 exp:=[]string{"date","security_id","segment_id","weight","return"};if len(h)!=len(exp){panic("bad header")}
 for i:=range exp{if h[i]!=exp[i]{panic("bad header")}};return &reader{file:f,csv:r,seen:make(map[string]bool)}}
func (r *reader)group()(string,map[string]rec,bool){first:=r.next;r.next=nil
 if first==nil{var e error;first,e=r.csv.Read();if e==io.EOF{return "",nil,false};if e!=nil{panic(e)}}
 day:=first[0];if day==""||r.seen[day]{panic("missing/non-grouped date")}
 rows:=make(map[string]rec)
 add:=func(v []string){if v[1]==""||v[2]==""{panic("empty id")};if _,ok:=rows[v[1]];ok{panic("duplicate security")}
 w,e:=strconv.ParseFloat(v[3],64);if e!=nil||math.IsNaN(w)||math.IsInf(w,0)||w<0{panic("bad weight")}
 ret,e:=strconv.ParseFloat(v[4],64);if e!=nil||math.IsNaN(ret)||math.IsInf(ret,0){panic("bad return")}
 rows[v[1]]=rec{v[2],w,ret}}
 add(first)
 for{v,e:=r.csv.Read();if e==io.EOF{break};if e!=nil{panic(e)}
 if v[0]!=day{r.next=v;break};add(v)}
 r.seen[day]=true;return day,rows,true}
func (r *reader)row()([]string,bool){
 if r.next!=nil{v:=r.next;r.next=nil;return v,true}
 v,e:=r.csv.Read();if e==io.EOF{return nil,false};if e!=nil{panic(e)};return v,true
}
type agg struct{wp,wb,pr,br float64}
type task struct{kind int;segment string;wp,wb,rp,rb,benchmark float64}
type result struct{kind int;segment string;value float64}
func cpuSeconds()float64{var usage syscall.Rusage;if e:=syscall.Getrusage(syscall.RUSAGE_SELF,&usage);e!=nil{panic(e)};return float64(usage.Utime.Sec)+float64(usage.Utime.Usec)/1e6+float64(usage.Stime.Sec)+float64(usage.Stime.Usec)/1e6}
func main(){folder:=flag.String("data","data/brinson-15k","fixture folder");output:=flag.String("output","results/brinson-go.csv","output CSV path");flag.Parse();start:=time.Now();cpuStart:=cpuSeconds()
 if e:=os.MkdirAll(filepath.Dir(*output),0755);e!=nil{panic(e)}
 outFile,e:=os.Create(*output);if e!=nil{panic(e)};defer outFile.Close()
 buffered:=bufio.NewWriter(outFile);writer:=csv.NewWriter(buffered)
 writeTime:=time.Duration(0)
 writeStart:=time.Now()
 if e:=writer.Write([]string{"date","segment_id","allocation","selection","interaction","total"});e!=nil{panic(e)}
 writeTime+=time.Since(writeStart)
 p:=open(filepath.Join(*folder,"portfolio.csv"));defer p.file.Close()
 b:=open(filepath.Join(*folder,"benchmark.csv"));defer b.file.Close()
 workers:=runtime.GOMAXPROCS(0);if workers<1{workers=runtime.NumCPU()}
 tasks:=make(chan task,workers*4);results:=make(chan result,workers*4)
 var wg sync.WaitGroup
 for i:=0;i<workers;i++{wg.Add(1);go func(){defer wg.Done();for t:=range tasks{
 var v float64
 switch t.kind{case 0:v=(t.wp-t.wb)*(t.rb-t.benchmark)
 case 1:v=t.wb*(t.rp-t.rb)
 case 2:v=(t.wp-t.wb)*(t.rp-t.rb)}
 results<-result{t.kind,t.segment,v}}}()}
 go func(){wg.Wait();close(results)}()
 totals:=[]float64{0,0,0};days,securities,segments:=0,0,0
 readTime:=time.Duration(0);earlySegments:=0
 // Read portfolio one day at a time, then stream benchmark rows for that day.
 // Segment completion releases selection and interaction without waiting for total Rb.
 for{
 readStart:=time.Now();day,pr,pok:=p.group();readTime+=time.Since(readStart)
 if !pok{break}
 a:=make(map[string]*agg);remaining:=make(map[string]int)
 var totalWp,totalWb,retP,retB float64
 for _,pp:=range pr{
 totalWp+=pp.weight;retP+=pp.weight*pp.ret
 x:=a[pp.segment];if x==nil{x=&agg{};a[pp.segment]=x}
 x.wp+=pp.weight;x.pr+=pp.weight*pp.ret;remaining[pp.segment]++
 }
 if math.Abs(totalWp-1)>1e-9{panic("portfolio weights not normalized")}
 // The existing reader returns a complete benchmark day, which must be
 // replaced with row streaming to overlap read/compute at segment boundaries.
 // For now, drain and schedule the completed segments as soon as matched.
 var dispatch sync.WaitGroup
 dispatch.Add(1)
 go func(){
 defer dispatch.Done()
 seen:=make(map[string]bool,len(pr))
 current:=true
 for current{
 readStart:=time.Now();row,ok:=b.row();readTime+=time.Since(readStart)
 if !ok{break}
 if row[0]!=day{b.next=row;break}
 id:=row[1]
 if seen[id]{panic("duplicate benchmark security")};seen[id]=true
 pp,found:=pr[id];if !found||pp.segment!=row[2]{panic("key/segment mismatch")}
 w,err:=strconv.ParseFloat(row[3],64);if err!=nil||math.IsNaN(w)||math.IsInf(w,0)||w<0{panic("bad benchmark weight")}
 r,err:=strconv.ParseFloat(row[4],64);if err!=nil||math.IsNaN(r)||math.IsInf(r,0){panic("bad benchmark return")}
 x:=a[pp.segment];x.wb+=w;x.br+=w*r
 totalWb+=w;retB+=w*r;remaining[pp.segment]--
 if remaining[pp.segment]==0{
 if x.wp<=0||x.wb<=0{panic("zero-weight segment")}
 rp:=x.pr/x.wp;rb:=x.br/x.wb
 tasks<-task{1,pp.segment,x.wp,x.wb,rp,rb,0}
 tasks<-task{2,pp.segment,x.wp,x.wb,rp,rb,0}
 earlySegments++
 }
 }
 if len(seen)!=len(pr){panic("missing benchmark records")}
 if math.Abs(totalWb-1)>1e-9{panic("benchmark weights not normalized")}
 for segment,x:=range a{
 tasks<-task{0,segment,x.wp,x.wb,x.pr/x.wp,x.br/x.wb,retB}
 }
 }()
 expected:=3*len(a);bySegment:=make(map[string]*[3]float64,len(a));dayTotal:=0.0
 for i:=0;i<expected;i++{
 v:=<-results;totals[v.kind]+=v.value;dayTotal+=v.value
 row:=bySegment[v.segment];if row==nil{row=&[3]float64{};bySegment[v.segment]=row};row[v.kind]=v.value
 }
 dispatch.Wait()
 if math.Abs(dayTotal-(retP-retB))>1e-10{panic(fmt.Sprintf("reconciliation failed: %s",day))}
 segmentIDs:=make([]string,0,len(bySegment));for id:=range bySegment{segmentIDs=append(segmentIDs,id)};sort.Strings(segmentIDs)
 for _,segment:=range segmentIDs{
 row:=bySegment[segment];total:=row[0]+row[1]+row[2]
 record:=[]string{day,segment,strconv.FormatFloat(row[0],'g',17,64),strconv.FormatFloat(row[1],'g',17,64),strconv.FormatFloat(row[2],'g',17,64),strconv.FormatFloat(total,'g',17,64)}
 writeStart=time.Now();if e:=writer.Write(record);e!=nil{panic(e)};writeTime+=time.Since(writeStart)
 }
 days++;securities+=len(pr);segments+=len(a)
 }
 readStart:=time.Now();_,remainingBenchmark:=b.row();readTime+=time.Since(readStart)
 if remainingBenchmark{panic("extra benchmark dates")}
 close(tasks);for range results{}
 writeStart=time.Now()
 writer.Flush();if e:=writer.Error();e!=nil{panic(e)}
 if e:=buffered.Flush();e!=nil{panic(e)}
 if e:=outFile.Sync();e!=nil{panic(e)}
 writeTime+=time.Since(writeStart)
 elapsed:=time.Since(start).Seconds();cpuElapsed:=cpuSeconds()-cpuStart
 report:=map[string]any{"language":"go","mode":"brinson-fachler","workers":workers,"days":days,
 "security_day_records":securities,"segment_days":segments,"effect_tasks":3*segments,
 "pipeline":map[string]any{"early_selection_interaction_segment_days":earlySegments,"allocation_released_after_benchmark_total":true},
 "totals":map[string]float64{"allocation":totals[0],"selection":totals[1],"interaction":totals[2]},
 "elapsed_seconds":elapsed,"output_csv":*output,
 "cpu_usage":map[string]any{"process_cpu_seconds":cpuElapsed,"average_effective_cores":cpuElapsed/elapsed,"measurement":"process CPU time / wall time; not exact peak concurrent cores"},
 "io_timing":map[string]float64{"input_read_parse_seconds":readTime.Seconds(),"output_write_sync_seconds":writeTime.Seconds()},"timing_scope":"CSV input reading+join+aggregation+effect tasks+validation+CSV output write+flush+fsync"}
 data,_:=json.MarshalIndent(report,"","  ");fmt.Println(string(data))
}
