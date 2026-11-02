package main

// Cumulative Total statistics share the engine's format adapters and date-range planners.
import (
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func cumulativePath(c config, path string) string {
	return filepath.Join(c.output, "cumulative-"+strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))+".csv")
}

type totalObservation struct {
	date string
	p, b [2]float64
}

func cumulativeValues(obs []totalObservation, t int) []string {
	n := 0.0
	mp, mb, ma, vp, vb, va, cov := 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
	for _, x := range obs {
		n++
		p, b := x.p[t], x.b[t]
		a := p - b
		dp, db, da := p-mp, b-mb, a-ma
		mp += dp / n
		mb += db / n
		ma += da / n
		vp += dp * (p - mp)
		vb += db * (b - mb)
		va += da * (a - ma)
		cov += dp * (b - mb)
	}
	out := make([]string, 5)
	if n < 2 {
		return out
	}
	sp, sb, te := math.Sqrt(math.Max(0, vp)/(n-1)), math.Sqrt(math.Max(0, vb)/(n-1)), math.Sqrt(math.Max(0, va)/(n-1))
	out[0], out[1], out[2] = number(sp), number(sb), number(te)
	if te > 0 {
		out[3] = number(ma / te)
	}
	if vb > 0 {
		out[4] = number(cov / vb)
	}
	return out
}

// cumulativeRows is a pure calculation API over aligned daily return observations.
func cumulativeRows(obs []totalObservation, entity string, workers int) ([][]string, error) {
	if workers < 1 {
		return nil, errors.New("cumulative requires positive workers")
	}
	rows := make([][]string, 36)
	tasks := make(chan int, 36)
	var wg sync.WaitGroup
	for i := 0; i < 36; i++ {
		tasks <- i
	}
	close(tasks)
	for k := 0; k < workers && k < 36; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range tasks {
				end := time.Date(2023, time.Month(i+2), 0, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
				n := sort.Search(len(obs), func(j int) bool { return obs[j].date > end })
				last := ""
				if n > 0 {
					last = obs[n-1].date
				}
				row := []string{entity, "Total", "2023-01-01", end, strconv.Itoa(n), last}
				for t := 0; t < 2; t++ {
					row = append(row, cumulativeValues(obs[:n], t)...)
				}
				rows[i] = row
			}
		}()
	}
	wg.Wait()
	return rows, nil
}
func writeCumulative(path string, c config, obs []totalObservation, entity string, mt *metrics) error {
	start := time.Now()
	rows, e := cumulativeRows(obs, entity, c.effectiveWorkers)
	if e != nil {
		return e
	}
	mt.compute += seconds(start)
	start = time.Now()
	f, e := os.Create(cumulativePath(c, path) + ".partial")
	if e != nil {
		return e
	}
	defer f.Close()
	w := csv.NewWriter(f)
	header := []string{"Entity", "Level", "PeriodStart", "EffectiveDate", "Observations", "LastObservationDate"}
	for _, t := range []string{"Base", "Local"} {
		for _, v := range []string{"PortfolioStdDev", "BenchmarkStdDev", "TrackingError", "InformationRatio", "Beta"} {
			header = append(header, t+v)
		}
	}
	if e = w.Write(header); e != nil {
		return e
	}
	for _, row := range rows {
		if e = w.Write(row); e != nil {
			return e
		}
	}
	w.Flush()
	if e = w.Error(); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	mt.written = 36
	mt.reportRows = 36
	mt.write += seconds(start)
	return nil
}

// Daily US returns produced by the shared attribution day reader.
type dailyTotal struct {
	date, entity string
	present      bool
	returns      [2]float64
}

// runCumulative is the calculation entry point selected by the main dispatcher.
func runCumulative(c config) error {
	wall := time.Now()
	if e := os.MkdirAll(c.output, 0755); e != nil {
		return e
	}
	out, e := filepath.EvalSymlinks(c.output)
	if e != nil {
		return e
	}
	data, e := filepath.EvalSymlinks(c.data)
	if e != nil {
		return e
	}
	oa, _ := filepath.Abs(out)
	da, _ := filepath.Abs(data)
	if oa == da {
		return errors.New("output directory must differ from input")
	}
	masterStart := time.Now()
	m, e := loadMaster(c)
	if e != nil {
		return e
	}
	masterSeconds := seconds(masterStart)
	masterPath, e := resolveInput(c.data, "security_master")
	if e != nil {
		return e
	}
	masterInfo, e := os.Stat(masterPath)
	if e != nil {
		return e
	}
	portfolios, e := portfolioInputs(c.data)
	if e != nil {
		return e
	}
	index, e := resolveInput(c.data, "index")
	if e != nil {
		return e
	}
	paths := append([]string{index}, portfolios...)
	type job struct {
		paired [][]dateRange
	}
	type result struct {
		file    int
		totals  []dailyTotal
		rows    uint64
		elapsed float64
		err     error
	}
	jobs := []job{}
	planned := make([][]dateRange, len(paths))
	allRanges := true
	inputBytes := uint64(masterInfo.Size())
	boundaryStart := time.Now()
	rangeTasks := 0
	for i, path := range paths {
		info, e := os.Stat(path)
		if e != nil {
			return e
		}
		inputBytes += uint64(info.Size())
		var ranges []dateRange
		ok := false
		if c.threads > 1 {
			if filepath.Ext(path) == ".parquet" {
				ranges, ok, e = parquetCountryRanges(path, c.maxDays)
			} else {
				ranges, ok, e = countryRanges(path, c.maxDays)
			}
			if e != nil {
				return e
			}
		}
		planned[i] = ranges
		if !ok || len(ranges) == 0 {
			allRanges = false
		}

	}
	{
		if allRanges {
			for _, r := range planned[1:] {
				if len(r) != len(planned[0]) {
					return errors.New("portfolio/index date mismatch")
				}
				for j := range r {
					if r[j].date != planned[0][j].date {
						return errors.New("portfolio/index date mismatch")
					}
				}
			}
			size := (len(planned[0]) + c.threads*2 - 1) / (c.threads * 2)
			for first := 0; first < len(planned[0]); first += size {
				last := min(first+size, len(planned[0]))
				paired := make([][]dateRange, len(paths))
				for i := range paths {
					paired[i] = planned[i][first:last]
				}
				jobs = append(jobs, job{paired: paired})
				rangeTasks++
			}
		} else {
			jobs = append(jobs, job{paired: make([][]dateRange, len(paths))})
		}
	}
	poolStart := time.Now()
	boundarySeconds := seconds(boundaryStart)
	workers := min(c.threads, len(jobs))
	queue := make(chan job, len(jobs))
	results := make(chan result, len(jobs)*len(paths))
	for _, j := range jobs {
		queue <- j
	}
	close(queue)
	for k := 0; k < workers; k++ {
		go func() {
			for j := range queue {
				start := time.Now()
				data, taskMetrics, e := readAttributionDaily(paths, j.paired, c, &m)
				elapsed := seconds(start)
				for i := range paths {
					var d []dailyTotal
					if e == nil {
						d = data[i]
					}
					rows := uint64(0)
					duration := 0.0
					if i == 0 {
						rows = taskMetrics.rows
						duration = elapsed
					}
					results <- result{file: i, totals: d, rows: rows, elapsed: duration, err: e}
				}
			}
		}()
	}
	totals := make([][]dailyTotal, len(paths))
	mt := metrics{}
	var firstError error
	expected := len(jobs) * len(paths)
	for k := 0; k < expected; k++ {
		x := <-results
		if x.err != nil && firstError == nil {
			firstError = x.err
		}
		mt.rows += x.rows
		mt.read += x.elapsed
		totals[x.file] = append(totals[x.file], x.totals...)
	}
	poolSeconds := seconds(poolStart)
	if firstError != nil {
		return firstError
	}
	for i := range totals {
		sort.Slice(totals[i], func(a, b int) bool { return totals[i][a].date < totals[i][b].date })
		if len(totals[i]) == 0 {
			return errors.New("no input dates")
		}
		for j := 1; j < len(totals[i]); j++ {
			if totals[i][j].date <= totals[i][j-1].date || totals[i][j].entity != totals[i][0].entity {
				return errors.New("duplicate dates or mixed entities across ranges")
			}
		}
	}
	for i, path := range portfolios {
		p, b := totals[i+1], totals[0]
		if len(p) != len(b) {
			return errors.New("portfolio/index date mismatch")
		}
		if p[0].date != "2023-01-01" || p[len(p)-1].date < "2025-12-01" {
			return errors.New("36 month ends require history starting 2023-01-01 and extending into December 2025")
		}
		obs := make([]totalObservation, 0, len(p))
		for j := range p {
			if p[j].date != b[j].date {
				return errors.New("portfolio/index date mismatch")
			}
			if p[j].present != b[j].present {
				return fmt.Errorf("US holdings missing on only one side: %s", p[j].date)
			}
			if p[j].present {
				obs = append(obs, totalObservation{date: p[j].date, p: p[j].returns, b: b[j].returns})
			}
		}
		side := metrics{}
		if e = writeCumulative(path, c, obs, p[0].entity, &side); e != nil {
			return e
		}
		mt.add(side)
	}
	for _, path := range portfolios {
		target := cumulativePath(c, path)
		if e = os.Rename(target+".partial", target); e != nil {
			return e
		}
	}
	if e = os.MkdirAll(filepath.Dir(c.metrics), 0755); e != nil {
		return e
	}
	f, e := os.Create(c.metrics)
	if e != nil {
		return e
	}
	defer f.Close()
	w := csv.NewWriter(f)
	rangeWorkers := 0
	if rangeTasks > 0 {
		rangeWorkers = workers
	}
	w.Write([]string{"language", "scenario", "threads", "days", "input_file_bytes", "rows_read", "output_rows", "read_tasks", "read_workers", "date_range_tasks", "date_range_workers", "effective_date_workers", "effective_date_tasks", "cumulative_reader", "read_pipeline_wall_seconds", "master_load_seconds", "boundary_scan_seconds", "read_filter_aggregate_seconds", "compute_seconds", "write_seconds", "wall_seconds"})
	w.Write([]string{"go", "cumulative", strconv.Itoa(c.threads), strconv.Itoa(len(totals[0])), strconv.FormatUint(inputBytes, 10), strconv.FormatUint(mt.rows, 10), strconv.FormatUint(mt.written, 10), strconv.Itoa(len(jobs)), strconv.Itoa(workers), strconv.Itoa(rangeTasks), strconv.Itoa(rangeWorkers), strconv.Itoa(min(c.effectiveWorkers, 36)), strconv.Itoa(36 * len(portfolios)), "attribution", number(poolSeconds), number(masterSeconds), number(boundarySeconds), number(mt.read), number(mt.compute), number(mt.write), number(seconds(wall))})
	w.Flush()
	if e = w.Error(); e != nil {
		return e
	}
	fmt.Printf("%d rows written; metrics %s\n", mt.written, c.metrics)
	return nil
}

// Reuse attribution's day reader, US filter and normalized security buffers.
// Each task alternates benchmark/portfolio reads over matching complete dates.
func readAttributionDaily(paths []string, ranges [][]dateRange, c config, m *master) (totals [][]dailyTotal, mt metrics, err error) {
	c.scenario = "country"
	c.period = "day"
	streams := make([]*stream, len(paths))
	totals = make([][]dailyTotal, len(paths))
	defer func() {
		for _, s := range streams {
			if s != nil {
				s.r.Close()
			}
		}
	}()
	for i, path := range paths {
		var r inputReader
		var e error
		if len(ranges[i]) > 0 {
			r, e = openCountryRange(path, ranges[i][0], ranges[i][len(ranges[i])-1])
		} else {
			r, e = openInput(path, inputHeader)
		}
		if e != nil {
			return nil, mt, e
		}
		streams[i] = &stream{r: r}
	}
	for count := 0; c.maxDays == 0 || count < c.maxDays; count++ {
		date := ""
		ended := false
		for i, s := range streams {
			d, e := s.next(m, c, &mt)
			if e != nil {
				return nil, mt, e
			}
			if i == 0 && d == nil {
				ended = true
			}
			if ended {
				if d != nil {
					return nil, mt, errors.New("extra portfolio dates")
				}
				continue
			}
			if d == nil {
				return nil, mt, errors.New("portfolio ended before index")
			}
			if i == 0 {
				date = d.date
			} else if d.date != date {
				return nil, mt, errors.New("portfolio/index date mismatch")
			}
			if len(ranges[i]) > 0 && (count >= len(ranges[i]) || ranges[i][count].date != d.date) {
				return nil, mt, errors.New("date range mismatch")
			}
			total := dailyTotal{date: d.date, entity: d.entity}
			for _, v := range d.stats {
				total.present = total.present || v.w > 0
				for t := 0; t < 2; t++ {
					total.returns[t] += v.r[t]
				}
			}
			totals[i] = append(totals[i], total)
		}
		if ended {
			if len(ranges[0]) > 0 && count != len(ranges[0]) {
				return nil, mt, errors.New("missing dates in range")
			}
			break
		}
	}
	return totals, mt, nil
}
