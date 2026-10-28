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

type Meta struct {
    Days int `json:"days"`
    Securities int `json:"securities"`
}

func readFloats(path string, n int) ([]float64, error) {
    f, err := os.Open(path)
    if err != nil { return nil, err }
    defer f.Close()
    data := make([]float64, n)
    if err := binary.Read(f, binary.LittleEndian, data); err != nil { return nil, err }
    var extra [1]byte
    if k, _ := f.Read(extra[:]); k != 0 { return nil, fmt.Errorf("extra bytes in %s", path) }
    return data, nil
}

func compute(wp, wb, rp, rb, a, s []float64, days, secs, workers int) {
    jobs := make(chan int, days)
    var wg sync.WaitGroup
    for w := 0; w < workers; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for d := range jobs {
                for j := d * secs; j < (d+1)*secs; j++ {
                    a[j] = (wp[j] - wb[j]) * rb[j]
                    s[j] = wp[j] * (rp[j] - rb[j])
                }
            }
        }()
    }
    for d := 0; d < days; d++ { jobs <- d }
    close(jobs)
    wg.Wait()
}

func computeSteps(wp, wb, rp, rb, a, s []float64, days, secs, workers int) {
    // Two independent tasks per day: allocation and selection.
    jobs := make(chan int, days*2)
    var wg sync.WaitGroup
    for w := 0; w < workers; w++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for task := range jobs {
                d, effect := task/2, task%2
                start, end := d*secs, (d+1)*secs
                if effect == 0 {
                    for j := start; j < end; j++ {
                        a[j] = (wp[j] - wb[j]) * rb[j]
                    }
                } else {
                    for j := start; j < end; j++ {
                        s[j] = wp[j] * (rp[j] - rb[j])
                    }
                }
            }
        }()
    }
    for task := 0; task < days*2; task++ { jobs <- task }
    close(jobs)
    wg.Wait()
}

func main() {
    folder := flag.String("data", "data", "fixture folder")
    workers := flag.Int("workers", runtime.NumCPU(), "parallel workers")
    runs := flag.Int("runs", 5, "timed repetitions")
    mode := flag.String("mode", "fused", "fused or steps")
    flag.Parse()
    if *workers < 1 || *runs < 1 { panic("workers/runs must be positive") }
    if *mode != "fused" && *mode != "steps" { panic("mode must be fused or steps") }
    blob, err := os.ReadFile(filepath.Join(*folder, "metadata.json"))
    if err != nil { panic(err) }
    var m Meta
    if err := json.Unmarshal(blob, &m); err != nil { panic(err) }
    if m.Days < 1 || m.Securities < 1 { panic("bad dimensions") }
    n := m.Days * m.Securities
    keys := []string{"wp", "wb", "rp", "rb"}
    vals := make([][]float64, 4)
    for i, k := range keys {
        vals[i], err = readFloats(filepath.Join(*folder, k+".bin"), n)
        if err != nil { panic(err) }
    }
    wp, wb, rp, rb := vals[0], vals[1], vals[2], vals[3]
    a, s := make([]float64, n), make([]float64, n)
    runtime.GOMAXPROCS(*workers)
    runKernel := compute
    if *mode == "steps" { runKernel = computeSteps }
    runKernel(wp, wb, rp, rb, a, s, m.Days, m.Securities, *workers)
    durations := make([]float64, *runs)
    for i := range durations {
        start := time.Now()
        runKernel(wp, wb, rp, rb, a, s, m.Days, m.Securities, *workers)
        durations[i] = time.Since(start).Seconds()
    }
    maxErr, totalA, totalS := 0.0, 0.0, 0.0
    for d := 0; d < m.Days; d++ {
        actual, expected := 0.0, 0.0
        for j := d*m.Securities; j < (d+1)*m.Securities; j++ {
            actual += a[j]+s[j]
            expected += wp[j]*rp[j]-wb[j]*rb[j]
            totalA += a[j]
            totalS += s[j]
        }
        maxErr = math.Max(maxErr, math.Abs(actual-expected))
    }
    if maxErr > 1e-10 { panic(fmt.Sprintf("reconciliation failed: %g", maxErr)) }
    ordered := append([]float64(nil), durations...)
    sort.Float64s(ordered)
    mid := len(ordered)/2
    median := ordered[mid]
    if len(ordered)%2 == 0 { median = (ordered[mid-1]+ordered[mid])/2 }
    report := map[string]any{
        "language": "go", "mode": *mode, "workers": *workers,
        "days": m.Days, "securities": m.Securities, "runs_seconds": durations,
        "median_seconds": median, "max_reconciliation_error": maxErr,
        "total_allocation": totalA, "total_selection": totalS,
    }
    b, _ := json.MarshalIndent(report, "", "  ")
    fmt.Println(string(b))
}
