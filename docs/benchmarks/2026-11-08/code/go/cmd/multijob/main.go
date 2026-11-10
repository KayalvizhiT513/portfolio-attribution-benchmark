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

type Meta struct { Days int `json:"days"`; Securities int `json:"securities"` }

func readFloats(path string, n int) ([]float64, error) {
    f, err := os.Open(path)
    if err != nil { return nil, err }
    defer f.Close()
    a := make([]float64, n)
    if err := binary.Read(f, binary.LittleEndian, a); err != nil { return nil, err }
    return a, nil
}
func calc(wp, wb, rp, rb, a, s []float64) {
    for i := range a {
        a[i] = (wp[i] - wb[i]) * rb[i]
        s[i] = wp[i] * (rp[i] - rb[i])
    }
}
func sum(a []float64) float64 {
    total := 0.0
    for _, x := range a { total += x }
    return total
}
type Pair struct { a, s []float64 }
type Totals struct { a, s float64 }

func main() {
    folder := flag.String("data", "data/100k", "fixture directory")
    jobs := flag.Int("jobs", 10, "number of independent calculation jobs")
    concurrency := flag.Int("concurrency", 4, "maximum simultaneously running jobs")
    runs := flag.Int("runs", 5, "repetitions")
    flag.Parse()
    if *jobs < 1 || *concurrency < 1 || *runs < 1 { panic("positive jobs, concurrency and runs required") }
    c := *concurrency
    if c > *jobs { c = *jobs }
    runtime.GOMAXPROCS(c)
    metadata, err := os.ReadFile(filepath.Join(*folder, "metadata.json"))
    if err != nil { panic(err) }
    var meta Meta
    if err = json.Unmarshal(metadata, &meta); err != nil { panic(err) }
    n := meta.Days * meta.Securities
    paths := []string{"wp", "wb", "rp", "rb"}
    var input [4][]float64
    for i, name := range paths {
        input[i], err = readFloats(filepath.Join(*folder, name+".bin"), n)
        if err != nil { panic(err) }
    }
    slots := make([]Pair, c)
    for i := range slots {
        slots[i] = Pair{make([]float64, n), make([]float64, n)}
    }
    wp, wb, rp, rb := input[0], input[1], input[2], input[3]
    calc(wp, wb, rp, rb, slots[0].a, slots[0].s)
    expectedA, expectedS := sum(slots[0].a), sum(slots[0].s)

    counts := make([]int, c)
    for i := 0; i < c; i++ {
        counts[i] = *jobs / c
        if i < *jobs % c { counts[i]++ }
    }
    durations := make([]float64, *runs)
    for rep := range durations {
        results := make(chan Totals, c)
        var wg sync.WaitGroup
        start := time.Now()
        for i := 0; i < c; i++ {
            wg.Add(1)
            go func(slot, count int) {
                defer wg.Done()
                a, s := slots[slot].a, slots[slot].s
                for j := 0; j < count; j++ {
                    calc(wp, wb, rp, rb, a, s)
                }
                results <- Totals{sum(a), sum(s)}
            }(i, counts[i])
        }
        wg.Wait()
        durations[rep] = time.Since(start).Seconds()
        close(results)
        for actual := range results {
            if math.Abs(actual.a-expectedA) > 1e-11 || math.Abs(actual.s-expectedS) > 1e-11 {
                panic(fmt.Sprintf("concurrent job results disagree: %+v", actual))
            }
        }
    }
    sorted := append([]float64(nil), durations...)
    sort.Float64s(sorted)
    median := sorted[len(sorted)/2]
    if len(sorted)%2 == 0 { median = (sorted[len(sorted)/2-1]+median)/2 }
    result := map[string]any{
        "language":"go", "mode":"concurrent-jobs",
        "jobs":*jobs, "concurrency":c, "days":meta.Days,
        "securities":meta.Securities, "runs_seconds":durations,
        "median_seconds":median, "throughput_jobs_per_second":float64(*jobs)/median,
        "output_buffers":c, "input_sharing":"read-only shared slices",
        "timing_scope":"all jobs plus dispatch and completion, no input loading/output allocation",
    }
    encoded, _ := json.MarshalIndent(result, "", "  ")
    fmt.Println(string(encoded))
}
