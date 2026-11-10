package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Ranges contain whole dates: CSV byte offsets or Parquet row-group indices.
type dateRange struct {
	date       string
	start, end int64
}
type countryFile struct {
	path    string
	ranges  []dateRange
	metrics metrics
}

func countryRanges(path string, limit int) ([]dateRange, bool, error) {
	f, header, err := openCSV(path, inputHeader)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	offset := header.InputOffset()
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return nil, false, err
	}
	reader := bufio.NewReaderSize(f, 1024*1024)
	var ranges []dateRange
	entity := ""
	for {
		line, err := reader.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			return nil, false, nil
		}
		if err != nil && err != io.EOF {
			return nil, false, err
		}
		if len(line) == 0 {
			break
		}
		content := bytes.TrimSuffix(line, []byte{'\n'})
		content = bytes.TrimSuffix(content, []byte{'\r'})
		if bytes.IndexByte(content, '\r') >= 0 {
			return nil, false, nil
		}
		// Balanced quotes keep this physical line a complete CSV record. Escaped
		// quotes occur in pairs. Multiline records retain the general parser.
		if bytes.Count(line, []byte{'"'})%2 != 0 {
			return nil, false, nil
		}
		first, second := -1, -1
		quoted := false
		for j, b := range line {
			if b == '"' {
				quoted = !quoted
			}
			if b == ',' && !quoted {
				if first < 0 {
					first = j
				} else {
					second = j
					break
				}
			}
		}
		if first < 1 || second < 0 {
			return nil, false, nil
		}
		rest := line[first+1 : second]
		if len(rest) >= 2 && rest[0] == '"' && rest[len(rest)-1] == '"' {
			rest = rest[1 : len(rest)-1]
		}
		dateBytes := rest
		if len(ranges) == 0 || ranges[len(ranges)-1].date != string(dateBytes) {
			date := string(dateBytes)
			if len(ranges) > 0 {
				ranges[len(ranges)-1].end = offset
				if limit > 0 && len(ranges) == limit {
					return ranges, true, nil
				}
				if date <= ranges[len(ranges)-1].date {
					return nil, false, errors.New("dates must strictly increase")
				}
			}
			if _, err := parseDate(date); err != nil {
				return nil, false, err
			}
			ranges = append(ranges, dateRange{date: date, start: offset})
		}
		if entity == "" {
			entity = string(line[:first])
		}
		if !bytes.Equal(line[:first], []byte(entity)) {
			// Different spellings/quoting may still denote the same entity.
			// Let the full parser distinguish that from an actual entity change.
			return nil, false, nil
		}
		offset += int64(len(line))
		if err == io.EOF {
			break
		}
	}
	if len(ranges) > 0 {
		ranges[len(ranges)-1].end = offset
	}
	return ranges, true, nil
}

func countryParallel(c config, m *master, paths []string, indexPath string) (mt, indexMt, portfolioMt metrics, days int, used bool, err error) {
	if c.scenario != "country" || c.threads <= 1 {
		return
	}
	planStart := time.Now()
	all := append([]string{indexPath}, paths...)
	// Both adapters must provide complete-date ranges.
	for _, path := range all {
		if ext := strings.ToLower(filepath.Ext(path)); ext != ".csv" && ext != ".parquet" {
			return
		}
	}
	files := make([]countryFile, len(all))
	for i, path := range all {
		start := time.Now()
		var ranges []dateRange
		var ok bool
		var e error
		if filepath.Ext(path) == ".parquet" {
			ranges, ok, e = parquetCountryRanges(path, c.maxDays)
		} else {
			ranges, ok, e = countryRanges(path, c.maxDays)
		}
		if e != nil {
			err = e
			return
		}
		if !ok {
			return
		}
		files[i] = countryFile{path: path, ranges: ranges}
		files[i].metrics.read = seconds(start)
		files[i].metrics.boundary = files[i].metrics.read
	}
	used = true
	days = len(files[0].ranges)
	if days == 0 {
		err = errors.New("no input dates")
		return
	}
	for i := 1; i < len(files); i++ {
		if len(files[i].ranges) != days {
			err = errors.New("portfolio/index date mismatch")
			return
		}
		for j, r := range files[i].ranges {
			if r.date != files[0].ranges[j].date {
				err = errors.New("portfolio/index date mismatch")
				return
			}
		}
	}
	planSeconds := seconds(planStart)
	poolStart := time.Now()
	type task struct{ first, last int }
	type result struct {
		task     task
		reports  [][]*period
		metrics  []metrics
		entities []string
		err      error
	}
	size := (days + c.threads*2 - 1) / (c.threads * 2)
	taskCount := (days + size - 1) / size
	workers := min(c.threads, taskCount)
	jobs := make(chan task)
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				r := result{task: job, reports: make([][]*period, len(paths)), metrics: make([]metrics, len(files)), entities: make([]string, len(files))}
				r.err = func() (runErr error) {
					streams := make([]*stream, len(files))
					defer func() {
						for _, s := range streams {
							if s != nil {
								if closeErr := s.r.Close(); runErr == nil {
									runErr = closeErr
								}
							}
						}
					}()
					for i, file := range files {
						input, e := openCountryRange(file.path, file.ranges[job.first], file.ranges[job.last-1])
						if e != nil {
							return e
						}
						streams[i] = &stream{r: input}
					}
					for j := job.first; j < job.last; j++ {
						benchmark, e := streams[0].next(m, c, &r.metrics[0])
						if e != nil {
							return e
						}
						if benchmark == nil || benchmark.date != files[0].ranges[j].date {
							return errors.New("date range mismatch")
						}
						for i := 1; i < len(files); i++ {
							portfolio, e := streams[i].next(m, c, &r.metrics[i])
							if e != nil {
								return e
							}
							if portfolio == nil || portfolio.date != benchmark.date {
								return errors.New("date range mismatch")
							}
							p := newPeriod(portfolio, portfolio.date, c)
							if e = p.add(portfolio, benchmark, &r.metrics[i]); e != nil {
								return e
							}
							r.reports[i-1] = append(r.reports[i-1], p)
						}
					}
					for i, s := range streams {
						extra, e := s.next(m, c, &r.metrics[i])
						if e != nil {
							return e
						}
						if extra != nil {
							return errors.New("extra dates in range")
						}
						r.entities[i] = s.entity
					}
					return nil
				}()
				results <- r
			}
		}()
	}
	go func() {
		for first := 0; first < days; first += size {
			jobs <- task{first, min(first+size, days)}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	reports := make([][]*period, len(paths))
	for i := range reports {
		reports[i] = make([]*period, days)
	}
	entities := make([]string, len(files))
	// Workers read file descriptors while results arrive. Keep collected metrics
	// separate until all workers finish, avoiding concurrent struct reads/writes.
	collected := make([]metrics, len(files))
	for r := range results {
		if r.err != nil {
			if err == nil {
				err = fmt.Errorf("dates %d-%d: %w", r.task.first, r.task.last, r.err)
			}
			continue
		}
		for i, name := range r.entities {
			if entities[i] == "" {
				entities[i] = name
			} else if entities[i] != name {
				err = errors.New("mixed entities across ranges")
			}
			collected[i].add(r.metrics[i])
		}
		for i, periods := range r.reports {
			copy(reports[i][r.task.first:r.task.last], periods)
		}
	}
	if err != nil {
		return
	}
	for i := range files {
		files[i].metrics.add(collected[i])
	}
	poolSeconds := seconds(poolStart)
	orderedStart := time.Now()
	indexMt = files[0].metrics
	mt = indexMt
	for i := 1; i < len(files); i++ {
		file := &files[i]
		start := time.Now()
		var w *reportOutput
		w, err = newReportOutput(outputPath(c, file.path)+".partial", filepath.Ext(file.path) == ".parquet")
		if err != nil {
			return
		}
		file.metrics.write += seconds(start)
		for _, p := range reports[i-1] {
			if p == nil {
				err = errors.New("missing date report")
				break
			}
			if err = p.write(c, entities[i], m.countryNames, w, &file.metrics); err != nil {
				break
			}
		}
		if err == nil {
			start = time.Now()
			err = w.finish()
			file.metrics.write += seconds(start)
		}
		w.abort()
		if err != nil {
			return
		}
		portfolioMt.add(file.metrics)
	}
	mt.add(portfolioMt)
	mt.phaseBoundary = planSeconds
	mt.phaseParallel = poolSeconds
	mt.phaseOrdered = seconds(orderedStart)
	mt.rangeWorkers = uint64(workers)
	mt.rangeTasks = uint64(taskCount)
	return
}

func openCountryRange(path string, first, last dateRange) (inputReader, error) {
	if filepath.Ext(path) == ".parquet" {
		groups := make([]int, last.end-first.start)
		for i := range groups {
			groups[i] = int(first.start) + i
		}
		return openInputGroups(path, inputHeader, groups)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(bufio.NewReaderSize(io.NewSectionReader(f, first.start, last.end-first.start), 1024*1024))
	reader.ReuseRecord = true
	reader.FieldsPerRecord = len(inputHeader)
	return &csvInput{f: f, r: reader}, nil
}
