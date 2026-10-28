package main
import("bufio";"encoding/csv";"encoding/json";"flag";"fmt";"io";"math";"os";"path/filepath";"runtime";"sort";"strconv";"sync";"time")
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
type agg struct{wp,wb,pr,br float64}
type task struct{kind int;segment string;wp,wb,rp,rb,benchmark float64}
type result struct{kind int;segment string;value float64}
func main(){folder:=flag.String("data","data/brinson-15k","fixture folder");output:=flag.String("output","results/brinson-go.csv","output CSV path");flag.Parse();start:=time.Now()
 if e:=os.MkdirAll(filepath.Dir(*output),0755);e!=nil{panic(e)}
 outFile,e:=os.Create(*output);if e!=nil{panic(e)};defer outFile.Close()
 buffered:=bufio.NewWriter(outFile);writer:=csv.NewWriter(buffered)
 if e:=writer.Write([]string{"date","segment_id","allocation","selection","interaction","total"});e!=nil{panic(e)}
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
 for{day,pr,pok:=p.group();day2,br,bok:=b.group()
 if pok!=bok{panic("different day sets")};if !pok{break}
 if day!=day2||len(pr)!=len(br){panic("date or security count mismatch")}
 a:=make(map[string]*agg);var totalWp,totalWb,retP,retB float64
 for id,pp:=range pr{bb,ok:=br[id];if !ok||pp.segment!=bb.segment{panic("key/segment mismatch")}
 totalWp+=pp.weight;totalWb+=bb.weight
 retP+=pp.weight*pp.ret;retB+=bb.weight*bb.ret
 x:=a[pp.segment];if x==nil{x=&agg{};a[pp.segment]=x}
 x.wp+=pp.weight;x.wb+=bb.weight;x.pr+=pp.weight*pp.ret;x.br+=bb.weight*bb.ret
 }
 if math.Abs(totalWp-1)>1e-9||math.Abs(totalWb-1)>1e-9{panic("weights not normalized")}
 // Dispatch in a separate goroutine while collecting effects, to keep both
 // queues bounded without deadlocking on backpressure.
 var dispatch sync.WaitGroup
 dispatch.Add(1)
 go func(){defer dispatch.Done()
 for segment,x:=range a{if x.wp<=0||x.wb<=0{panic("zero weight segment")}
 rp:=x.pr/x.wp;rb:=x.br/x.wb
 tasks<-task{1,segment,x.wp,x.wb,rp,rb,0}
 tasks<-task{2,segment,x.wp,x.wb,rp,rb,0}
 tasks<-task{0,segment,x.wp,x.wb,rp,rb,retB}
 }
 }()
 expected:=3*len(a)
 dayTotal:=0.0;bySegment:=make(map[string]*[3]float64,len(a))
 for i:=0;i<expected;i++{v:=<-results;totals[v.kind]+=v.value;dayTotal+=v.value
 row:=bySegment[v.segment];if row==nil{row=&[3]float64{};bySegment[v.segment]=row};row[v.kind]=v.value}
 segmentIDs:=make([]string,0,len(bySegment));for id:=range bySegment{segmentIDs=append(segmentIDs,id)};sort.Strings(segmentIDs)
 for _,segment:=range segmentIDs{row:=bySegment[segment]
 total:=row[0]+row[1]+row[2]
 record:=[]string{day,segment,strconv.FormatFloat(row[0],'g',17,64),strconv.FormatFloat(row[1],'g',17,64),strconv.FormatFloat(row[2],'g',17,64),strconv.FormatFloat(total,'g',17,64)}
 if e:=writer.Write(record);e!=nil{panic(e)}
 }
 dispatch.Wait()
 if math.Abs(dayTotal-(retP-retB))>1e-10{panic(fmt.Sprintf("reconciliation failed: %s, %.12g",day,dayTotal-(retP-retB)))}
 days++;securities+=len(pr);segments+=len(a)
 }
 close(tasks);for range results{}
 writer.Flush();if e:=writer.Error();e!=nil{panic(e)}
 if e:=buffered.Flush();e!=nil{panic(e)}
 if e:=outFile.Sync();e!=nil{panic(e)}
 report:=map[string]any{"language":"go","mode":"brinson-fachler","workers":workers,"days":days,
 "security_day_records":securities,"segment_days":segments,"effect_tasks":3*segments,
 "totals":map[string]float64{"allocation":totals[0],"selection":totals[1],"interaction":totals[2]},
 "elapsed_seconds":time.Since(start).Seconds(),"output_csv":*output,"timing_scope":"CSV input reading+join+aggregation+effect tasks+validation+CSV output write+flush+fsync"}
 data,_:=json.MarshalIndent(report,"","  ");fmt.Println(string(data))
}
