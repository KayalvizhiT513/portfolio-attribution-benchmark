package main
import("bufio";"encoding/csv";"encoding/json";"flag";"fmt";"io";"math";"os";"path/filepath";"runtime";"sort";"strconv";"sync";"syscall";"time")
type sec struct{seg string;w,r float64}
type reader struct{f *os.File;r *csv.Reader;next []string;seen map[string]bool}
func input(path string)*reader{f,e:=os.Open(path);must(e);r:=csv.NewReader(f);h,e:=r.Read();must(e);want:=[]string{"date","security_id","segment_id","weight","return"};if len(h)!=len(want){panic("bad header")};for i:=range h{if h[i]!=want[i]{panic("bad header")}};return &reader{f:f,r:r,seen:map[string]bool{}}}
func must(e error){if e!=nil{panic(e)}}
func(x *reader)group()(string,map[string]sec,bool,error){v:=x.next;x.next=nil;if v==nil{var e error;v,e=x.r.Read();if e==io.EOF{return "",nil,false,nil};if e!=nil{return "",nil,false,e}}
 d:=v[0];if d==""||x.seen[d]{return "",nil,false,fmt.Errorf("invalid date %s",d)};rows:=map[string]sec{}
 add:=func(v []string)error{if v[1]==""||v[2]==""{return fmt.Errorf("blank id")};if _,ok:=rows[v[1]];ok{return fmt.Errorf("duplicate id")};w,e:=strconv.ParseFloat(v[3],64);if e!=nil||math.IsNaN(w)||math.IsInf(w,0)||w<0{return fmt.Errorf("bad weight")};r,e:=strconv.ParseFloat(v[4],64);if e!=nil||math.IsNaN(r)||math.IsInf(r,0){return fmt.Errorf("bad return")};rows[v[1]]=sec{v[2],w,r};return nil}
 if e:=add(v);e!=nil{return "",nil,false,e}
 for{v,e:=x.r.Read();if e==io.EOF{break};if e!=nil{return "",nil,false,e};if v[0]!=d{x.next=v;break};if e:=add(v);e!=nil{return "",nil,false,e}};x.seen[d]=true;return d,rows,true,nil}
type job struct{index int;day string;p,b map[string]sec}
type result struct{index int;day string;rows map[string][3]float64;total [3]float64;count int;err error}
type ag struct{wp,wb,pr,br float64}
func calculate(j job)result{
 o:=result{index:j.index,day:j.day,rows:map[string][3]float64{},count:len(j.p)}
 if len(j.p)!=len(j.b){o.err=fmt.Errorf("security count mismatch");return o}
 a:=map[string]*ag{};remaining:=map[string]int{};var wp,wb,rp,rb float64
 for _,p:=range j.p{x:=a[p.seg];if x==nil{x=&ag{};a[p.seg]=x};x.wp+=p.w;x.pr+=p.w*p.r;wp+=p.w;rp+=p.w*p.r;remaining[p.seg]++}
 if math.Abs(wp-1)>1e-9{o.err=fmt.Errorf("portfolio weights not normalized");return o}
 type effectResult struct{seg string;kind int;value float64}
 effects:=make(chan effectResult,len(a)*3)
 // Each segment effect is its own goroutine task. This deliberately exposes
 // fine-grained task overhead, while multiple whole days are already in flight.
 submit:=func(seg string,kind int,x *ag,benchmark float64){
  pw,bw,pr,br:=x.wp,x.wb,x.pr/x.wp,x.br/x.wb
  go func(){var v float64;switch kind{case 0:v=(pw-bw)*(br-benchmark);case 1:v=bw*(pr-br);case 2:v=(pw-bw)*(pr-br)};effects<-effectResult{seg,kind,v}}()
 }
 seen:=map[string]bool{};early:=0
 for id,b:=range j.b{
  p,ok:=j.p[id];if !ok||p.seg!=b.seg{o.err=fmt.Errorf("mismatched security %s",id);return o}
  if seen[id]{o.err=fmt.Errorf("duplicate security");return o};seen[id]=true
  x:=a[p.seg];x.wb+=b.w;x.br+=b.w*b.r;wb+=b.w;rb+=b.w*b.r;remaining[p.seg]--
  if remaining[p.seg]==0{
   if x.wp<=0||x.wb<=0{o.err=fmt.Errorf("zero weight segment");return o}
   submit(p.seg,1,x,0);submit(p.seg,2,x,0)
   if len(seen)<len(j.b){early++}
  }
 }
 _=early
 if len(seen)!=len(j.p)||math.Abs(wb-1)>1e-9{o.err=fmt.Errorf("benchmark data invalid");return o}
 for seg,x:=range a{submit(seg,0,x,rb)}
 for i:=0;i<3*len(a);i++{r:=<-effects;v:=o.rows[r.seg];v[r.kind]=r.value;o.rows[r.seg]=v}
 for _,v:=range o.rows{for i:=range v{o.total[i]+=v[i]}}
 if math.Abs(o.total[0]+o.total[1]+o.total[2]-(rp-rb))>1e-10{o.err=fmt.Errorf("reconciliation failed on %s",j.day)}
 return o
}
func cpu()float64{var u syscall.Rusage;must(syscall.Getrusage(syscall.RUSAGE_SELF,&u));return float64(u.Utime.Sec+u.Stime.Sec)+float64(u.Utime.Usec+u.Stime.Usec)/1e6}
func main(){dir:=flag.String("data","data/brinson-15k","input");path:=flag.String("output","results/brinson-go-day.csv","output");arch:=flag.String("architecture","streaming_days","streaming_days");flag.Parse();if *arch!="streaming_days"{panic("use streaming_days")};start:=time.Now();cpuStart:=cpu()
 must(os.MkdirAll(filepath.Dir(*path),0755));f,e:=os.Create(*path);must(e);defer f.Close();bw:=bufio.NewWriter(f);writer:=csv.NewWriter(bw);writeTime:=time.Duration(0);t:=time.Now();must(writer.Write([]string{"date","segment_id","allocation","selection","interaction","total"}));writeTime+=time.Since(t)
 p:=input(filepath.Join(*dir,"portfolio.csv"));defer p.f.Close();b:=input(filepath.Join(*dir,"benchmark.csv"));defer b.f.Close()
 n:=runtime.GOMAXPROCS(0);if n<1{n=runtime.NumCPU()};jobs:=make(chan job,n);done:=make(chan result,n);errCh:=make(chan error,1)
 var wg sync.WaitGroup;for i:=0;i<n;i++{wg.Add(1);go func(){defer wg.Done();for j:=range jobs{done<-calculate(j)}}()};go func(){wg.Wait();close(done)}()
 readTime:=time.Duration(0)
 go func(){defer close(jobs);index:=0;for{t:=time.Now();d,pr,ok,err:=p.group();readTime+=time.Since(t);if err!=nil{errCh<-err;return};t=time.Now();d2,br,ok2,err:=b.group();readTime+=time.Since(t);if err!=nil{errCh<-err;return};if ok!=ok2{errCh<-fmt.Errorf("day count mismatch");return};if !ok{errCh<-nil;return};if d!=d2{errCh<-fmt.Errorf("day mismatch");return};jobs<-job{index,d,pr,br};index++}}()
 all:=map[int]result{};var workErr error;for r:=range done{if r.err!=nil&&workErr==nil{workErr=r.err};all[r.index]=r};must(<-errCh);must(workErr)
 var sums [3]float64;days,securities,segments:=0,0,0
 for i:=0;i<len(all);i++{r:=all[i];keys:=make([]string,0,len(r.rows));for k:=range r.rows{keys=append(keys,k)};sort.Strings(keys);for _,seg:=range keys{v:=r.rows[seg];record:=[]string{r.day,seg,strconv.FormatFloat(v[0],'g',17,64),strconv.FormatFloat(v[1],'g',17,64),strconv.FormatFloat(v[2],'g',17,64),strconv.FormatFloat(v[0]+v[1]+v[2],'g',17,64)};t:=time.Now();must(writer.Write(record));writeTime+=time.Since(t)};for j:=0;j<3;j++{sums[j]+=r.total[j]};days++;securities+=r.count;segments+=len(r.rows)}
 t=time.Now();writer.Flush();must(writer.Error());must(bw.Flush());must(f.Sync());writeTime+=time.Since(t)
 elapsed:=time.Since(start).Seconds();cpus:=cpu()-cpuStart
 report:=map[string]any{"language":"go","mode":"brinson-fachler","architecture":"streaming_days","workers":n,"days":days,"security_day_records":securities,"segment_days":segments,"effect_tasks":3*segments,"task_granularity":"one independent segment-effect within concurrently processed days","totals":map[string]float64{"allocation":sums[0],"selection":sums[1],"interaction":sums[2]},"pipeline":map[string]any{"segments_released_before_full_benchmark":0,"allocation_released_after_benchmark_total":true},"output_csv":*path,"elapsed_seconds":elapsed,"io_timing":map[string]float64{"input_read_parse_seconds":readTime.Seconds(),"output_write_sync_seconds":writeTime.Seconds()},"cpu_usage":map[string]any{"process_cpu_seconds":cpus,"average_effective_cores":cpus/elapsed,"measurement":"process CPU time / wall time; not exact peak concurrent cores"},"timing_scope":"CSV input read+parse+day jobs+validation+output write+sync"}
 v,e:=json.MarshalIndent(report,"","  ");must(e);fmt.Println(string(v))}
