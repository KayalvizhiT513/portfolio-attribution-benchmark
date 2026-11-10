// Streaming Brinson-Fachler attribution with bounded benchmark broadcasts.
package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

var inputHeader = []string{"Entity", "EffectiveDate", "InstrumentId", "Weight", "BaseReturn", "LocalReturn"}
var outputHeader = []string{"Scenario", "Entity", "PeriodStart", "PeriodEnd", "Days", "RowType", "Segment", "ReturnType", "PortfolioWeight", "BenchmarkWeight", "Allocation", "Selection", "Interaction", "Total", "PortfolioReturn", "BenchmarkReturn", "ActiveReturn", "PortfolioContribution", "BenchmarkContribution", "ActiveContribution"}

type config struct {
	data, output, metrics, scenario, period, subset string
	startDate, endDate                              string
	smfTrace                                        string
	threads, maxDays                                int
	effectiveWorkers                                int
}
type stat struct {
	w       float64
	r       [2]float64
	present bool
}
type masterVersion struct {
	date    string
	country int
}
type master struct {
	ids                 map[string]int
	names, countryNames []string
	history             [][]masterVersion
	selected            []bool
	subsetSlots         []int
	subsetNames         []string
}
type metrics struct {
	phaseSetup, phaseMaster, phaseBoundary, phaseParallel, phaseOrdered float64
	phaseStreaming, phaseFinalize, phaseOverhead                        float64
	rangeWorkers, rangeTasks                                            uint64
	boundary                                                            float64
	read, filter, aggregate, compute, write                             float64
	rows, retained, groups, effects, written, bytes, parsed             uint64
	reportRows                                                          uint64
	checksum                                                            [3]float64
	batchDecode, toDailyBuffer                                          float64
	parquetBatches                                                      uint64
}

func (m *metrics) add(b metrics) {
	m.boundary += b.boundary
	m.read += b.read
	m.batchDecode += b.batchDecode
	m.toDailyBuffer += b.toDailyBuffer
	m.parquetBatches += b.parquetBatches
	m.filter += b.filter
	m.aggregate += b.aggregate
	m.compute += b.compute
	m.write += b.write
	m.rows += b.rows
	m.retained += b.retained
	m.groups += b.groups
	m.effects += b.effects
	m.written += b.written
	m.bytes += b.bytes
	m.parsed += b.parsed
	m.reportRows += b.reportRows
	for j := 0; j < 3; j++ {
		m.checksum[j] += b.checksum[j]
	}
}
func seconds(t time.Time) float64 { return time.Since(t).Seconds() }
func openCSV(path string, header []string) (*os.File, *csv.Reader, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, nil, e
	}
	r := csv.NewReader(bufio.NewReaderSize(f, 1024*1024))
	r.ReuseRecord = true
	h, e := r.Read()
	if e != nil || strings.Join(h, "\x00") != strings.Join(header, "\x00") {
		f.Close()
		return nil, nil, fmt.Errorf("invalid header: %s", path)
	}
	return f, r, nil
}
func loadMaster(c config) (master, error) {
	m := master{ids: map[string]int{}}
	path, e := resolveInput(c.data, "security_master")
	if e != nil {
		return m, e
	}
	dated := true
	r, e := openInput(path, []string{"InstrumentId", "EffectiveDate", "Currency", "Country"})
	if e != nil {
		dated = false
		r, e = openInput(path, []string{"InstrumentId", "Currency", "Country"})
	}
	if e != nil {
		return m, e
	}
	defer r.Close()
	countries := map[string]int{}
	for {
		ok, e := r.next()
		if e != nil {
			return m, e
		}
		if !ok {
			break
		}
		effective := ""
		currencyCol, countryCol := 1, 2
		if dated {
			effective = strings.Clone(r.text(1))
			if _, err := parseDate(effective); err != nil {
				return m, err
			}
			currencyCol, countryCol = 2, 3
		}
		// Master strings outlive the current Arrow batch.
		row := []string{strings.Clone(r.text(0)), r.text(currencyCol), strings.Clone(r.text(countryCol))}
		for _, v := range row {
			if v == "" {
				return m, errors.New("blank master field")
			}
		}
		id, exists := m.ids[row[0]]
		if exists && !dated {
			return m, errors.New("duplicate master ID")
		}
		if !exists {
			id = len(m.names)
			m.ids[row[0]] = id
			m.names = append(m.names, row[0])
			m.history = append(m.history, nil)
		}
		k, ok := countries[row[2]]
		if !ok {
			k = len(countries)
			countries[row[2]] = k
			m.countryNames = append(m.countryNames, row[2])
		}
		m.history[id] = append(m.history[id], masterVersion{effective, k})
	}
	for id := range m.history {
		versions := m.history[id]
		sort.Slice(versions, func(i, j int) bool { return versions[i].date < versions[j].date })
		for i := 1; i < len(versions); i++ {
			if versions[i-1].date == versions[i].date {
				return m, fmt.Errorf("duplicate master ID/date: %s/%s", m.names[id], versions[i].date)
			}
		}
	}
	if len(m.names) == 0 {
		return m, errors.New("empty master")
	}
	m.selected = make([]bool, len(m.names))
	for i := range m.selected {
		m.selected[i] = c.scenario != "subset"
	}
	if c.subset != "" {
		sr, e := openInput(c.subset, []string{"InstrumentId"})
		if e != nil {
			return m, e
		}
		defer sr.Close()
		for {
			ok, e := sr.next()
			if e != nil {
				return m, e
			}
			if !ok {
				break
			}
			row := []string{sr.text(0)}
			i, ok := m.ids[row[0]]
			if !ok {
				return m, errors.New("unknown subset instrument")
			}
			m.selected[i] = true
		}
	}
	if c.scenario == "subset" {
		m.subsetSlots = make([]int, len(m.names))
		for id, selected := range m.selected {
			if selected {
				m.subsetSlots[id] = len(m.subsetNames)
				m.subsetNames = append(m.subsetNames, m.names[id])
			}
		}
	}
	return m, nil
}

// Select the latest master version effective on or before the holdings date.
func (m *master) countryOn(id int, date string) (int, error) {
	versions := m.history[id]
	i := sort.Search(len(versions), func(i int) bool { return versions[i].date > date })
	if i == 0 {
		return 0, fmt.Errorf("no effective master record: %s on %s", m.names[id], date)
	}
	return versions[i-1].country, nil
}

type day struct {
	date, entity string
	stats        []stat
	countries    []int
	countryCount int
}
type stream struct {
	r                inputReader
	pending          bool
	previous, entity string
	rowsBuf          []raw
	seen             []bool
}

func newStream(path string) (*stream, error) {
	r, e := openInput(path, inputHeader)
	if e != nil {
		return nil, e
	}
	return &stream{r: r}, nil
}

type raw struct {
	id int
	s  stat
}

func (s *stream) next(m *master, c config, mt *metrics) (*day, error) {
	start := time.Now()
	offset := s.r.parsedBytes()
	var profileBefore inputReadProfile
	profiler, profileAvailable := s.r.(profiledInput)
	if profileAvailable {
		profileBefore = profiler.readProfile()
	}
	rows := s.rowsBuf[:0]
	if len(s.seen) != len(m.names) {
		s.seen = make([]bool, len(m.names))
	} else {
		clear(s.seen)
	}
	seen := s.seen
	date := ""
	total := 0.0
	for {
		if s.pending {
			s.pending = false
		} else {
			ok, e := s.r.next()
			if e != nil {
				return nil, e
			}
			if !ok {
				break
			}
		}
		row := [3]string{s.r.text(0), s.r.text(1), s.r.text(2)}
		if date == "" {
			date = strings.Clone(row[1])
			if s.previous != "" && date <= s.previous {
				return nil, errors.New("dates must strictly increase")
			}
			if _, e := parseDate(date); e != nil {
				return nil, e
			}
		}
		if row[1] != date {
			s.pending = true
			break
		}
		for _, v := range row {
			if v == "" {
				return nil, errors.New("blank return field")
			}
		}
		if s.entity == "" {
			s.entity = strings.Clone(row[0])
		}
		if s.entity != row[0] {
			return nil, errors.New("mixed entities")
		}
		id, ok := m.ids[row[2]]
		if !ok {
			return nil, errors.New("unmapped instrument")
		}
		if seen[id] {
			return nil, errors.New("duplicate instrument/day")
		}
		seen[id] = true
		var values [3]float64
		for i := 0; i < 3; i++ {
			v, e := s.r.number(i + 3)
			if e != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				return nil, errors.New("invalid number")
			}
			values[i] = v
		}
		w, b, l := values[0], values[1], values[2]
		if w < 0 || b <= -1 || l <= -1 {
			return nil, errors.New("invalid weight/return (returns must exceed -1)")
		}
		rows = append(rows, raw{id, stat{w: w, r: [2]float64{w * b, w * l}, present: true}})
		total += w
	}
	s.rowsBuf = rows[:0]
	readElapsed := seconds(start)
	mt.read += readElapsed
	if profileBefore.enabled {
		after := profiler.readProfile()
		decoding := after.seconds - profileBefore.seconds
		mt.batchDecode += decoding
		mt.toDailyBuffer += math.Max(0, readElapsed-decoding)
		mt.parquetBatches += after.batches - profileBefore.batches
	}
	mt.parsed += uint64(s.r.parsedBytes() - offset)
	mt.rows += uint64(len(rows))
	if date == "" {
		return nil, nil
	}
	if math.Abs(total-1) > 1e-6 {
		return nil, fmt.Errorf("weights sum to %g on %s", total, date)
	}
	s.previous = date
	start = time.Now()
	datedCountries := make([]int, len(m.names))
	for _, r := range rows {
		country, err := m.countryOn(r.id, date)
		if err != nil {
			return nil, err
		}
		datedCountries[r.id] = country
	}
	retained := rows[:0]
	selected := 0.0
	for _, r := range rows {
		keep := c.scenario != "subset" || m.selected[r.id]
		if c.scenario == "country" {
			keep = m.countryNames[datedCountries[r.id]] == "US"
		}
		if keep {
			retained = append(retained, r)
			selected += r.s.w
		}
	}
	if selected <= 0 && !(c.scenario == "country" && len(retained) == 0) {
		return nil, fmt.Errorf("zero selected weight: %s/%s", s.entity, date)
	}
	mt.retained += uint64(len(retained))
	mt.filter += seconds(start)
	start = time.Now()
	n := len(m.names)
	if c.scenario == "subset" {
		n = len(m.subsetNames)
	}
	stats := make([]stat, n)
	for _, r := range retained {
		k := r.id
		if c.scenario == "subset" {
			k = m.subsetSlots[k]
		}
		a := &stats[k]
		a.present = true
		a.w += r.s.w / selected
		for t := 0; t < 2; t++ {
			a.r[t] += r.s.r[t] / selected
		}
	}
	for _, s := range stats {
		if s.present {
			mt.groups++
		}
	}
	mt.aggregate += seconds(start)
	d := &day{date: date, entity: s.entity, stats: stats}
	if c.scenario == "country" {
		d.countries, d.countryCount = datedCountries, len(m.countryNames)
	}
	return d, nil
}
func parseDate(s string) (time.Time, error) {
	t, e := time.Parse("2006-01-02", s)
	if e != nil || t.Format("2006-01-02") != s || t.Year() < 1 {
		return t, errors.New("invalid YYYY-MM-DD date")
	}
	return t, nil
}
func bucket(date, period string) (string, error) {
	if period == "all" {
		return "ALL", nil
	}
	d, e := parseDate(date)
	if e != nil {
		return "", e
	}
	switch period {
	case "week":
		weekday := (int(d.Weekday()) + 6) % 7
		return d.AddDate(0, 0, -weekday).Format("2006-01-02"), nil
	case "month":
		return d.Format("2006-01"), nil
	case "quarter":
		return fmt.Sprintf("%d-Q%d", d.Year(), (int(d.Month())-1)/3+1), nil
	case "year":
		return strconv.Itoa(d.Year()), nil
	default:
		return date, nil
	}
}
func effect(p, b stat, t int, benchmark float64) [3]float64 {
	rp, rb := 0.0, 0.0
	if p.w > 0 {
		rp = p.r[t] / p.w
	}
	if b.w > 0 {
		rb = b.r[t] / b.w
	} else {
		rb = rp
	}
	if p.w == 0 {
		rp = rb
	}
	return [3]float64{(p.w - b.w) * (rb - benchmark), b.w * (rp - rb), (p.w - b.w) * (rp - rb)}
}

type acc struct {
	held         bool
	w            [2]float64
	e            [2][3]float64
	contribution [2][2]float64
	present      bool
}

// rowWriter reuses a byte buffer and formats numbers directly into it.
// Text fields retain RFC 4180 escaping, including embedded newlines/quotes.
type rowWriter struct {
	w      *bufio.Writer
	buffer []byte
}

func (w *rowWriter) record(fields []string, numbers []float64) error {
	w.buffer = w.buffer[:0]
	for i, field := range fields {
		if i > 0 {
			w.buffer = append(w.buffer, ',')
		}
		if strings.ContainsAny(field, ",\"\r\n") {
			w.buffer = append(w.buffer, '"')
			for j := 0; j < len(field); j++ {
				if field[j] == '"' {
					w.buffer = append(w.buffer, '"')
				}
				w.buffer = append(w.buffer, field[j])
			}
			w.buffer = append(w.buffer, '"')
		} else {
			w.buffer = append(w.buffer, field...)
		}
	}
	for _, n := range numbers {
		w.buffer = append(w.buffer, ',')
		w.buffer = strconv.AppendFloat(w.buffer, n, 'g', -1, 64)
	}
	w.buffer = append(w.buffer, '\n')
	_, err := w.w.Write(w.buffer)
	return err
}
func (w *rowWriter) Write(fields []string) error { return w.record(fields, nil) }

type period struct {
	key, start, end string
	dayText         string
	days            int
	growth          [2][2]float64
	acc             []acc
	portfolioOnly   bool
}

func newPeriod(d *day, key string, c config) *period {
	portfolioOnly := c.scenario == "weekly" || c.scenario == "monthly"
	n := len(d.stats)
	if d.countryCount > 0 {
		n = d.countryCount
	}
	if portfolioOnly {
		n = 1
	}
	return &period{key: key, start: d.date, end: d.date, growth: [2][2]float64{{1, 1}, {1, 1}}, acc: make([]acc, n), portfolioOnly: portfolioOnly}
}
func (p *period) add(pd, bd *day, mt *metrics) error {
	start := time.Now()
	if pd.countryCount > 0 {
		portfolioEmpty, benchmarkEmpty := true, true
		for _, s := range pd.stats {
			portfolioEmpty = portfolioEmpty && !s.present
		}
		for _, s := range bd.stats {
			benchmarkEmpty = benchmarkEmpty && !s.present
		}
		if portfolioEmpty || benchmarkEmpty {
			mt.compute += seconds(start)
			if portfolioEmpty && benchmarkEmpty {
				// Preserve input dates while emitting no attribution for empty dates.
				return nil
			}
			return fmt.Errorf("US holdings missing on only one side: %s", pd.date)
		}
	}
	var totals [2][2]float64
	for _, s := range pd.stats {
		for t := 0; t < 2; t++ {
			totals[0][t] += s.r[t]
		}
	}
	for _, s := range bd.stats {
		for t := 0; t < 2; t++ {
			totals[1][t] += s.r[t]
		}
	}
	var daily [2][3]float64
	for k := range pd.stats {
		ps, bs := pd.stats[k], bd.stats[k]
		slot := k
		if pd.countryCount > 0 {
			if ps.present {
				slot = pd.countries[k]
			} else {
				slot = bd.countries[k]
			}
		}
		if ps.present || bs.present {
			mt.effects += 6
		}
		for t := 0; t < 2; t++ {
			e := effect(ps, bs, t, totals[1][t])
			for j := 0; j < 3; j++ {
				daily[t][j] += e[j]
				if !p.portfolioOnly {
					if pd.countryCount > 0 {
						p.acc[slot].e[t][j] += e[j]
					} else {
						p.acc[slot].e[t][j] = p.acc[slot].e[t][j]*(1+totals[1][t]) + e[j]*p.growth[0][t]
					}
				}
			}
		}
		if !p.portfolioOnly {
			a := &p.acc[slot]
			a.present = a.present || ps.present || bs.present
			a.held = a.held || ps.present
			a.w[0] += ps.w
			a.w[1] += bs.w
			for t := 0; t < 2; t++ {
				a.contribution[0][t] += ps.r[t] * p.growth[0][t]
				a.contribution[1][t] += bs.r[t] * p.growth[1][t]
			}
		}
	}
	for t := 0; t < 2; t++ {
		if math.Abs(daily[t][0]+daily[t][1]+daily[t][2]-(totals[0][t]-totals[1][t])) > 1e-9 {
			return errors.New("daily reconciliation failed")
		}
		if p.portfolioOnly {
			for j := 0; j < 3; j++ {
				p.acc[0].e[t][j] = p.acc[0].e[t][j]*(1+totals[1][t]) + daily[t][j]*p.growth[0][t]
			}
		}
		for side := 0; side < 2; side++ {
			if p.portfolioOnly {
				p.acc[0].contribution[side][t] += totals[side][t] * p.growth[side][t]
			}
			p.growth[side][t] *= 1 + totals[side][t]
			if math.IsInf(p.growth[side][t], 0) || math.IsNaN(p.growth[side][t]) {
				return errors.New("nonfinite compounded return")
			}
		}
	}
	if p.portfolioOnly {
		a := &p.acc[0]
		a.present = true
		a.held = true
		a.w[0]++
		a.w[1]++
	}
	p.days++
	p.end = pd.date
	mt.compute += seconds(start)
	return nil
}
func number(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
func (p *period) row(c config, entity, kind, name string, t int, weights [2]float64, e [3]float64, contribution [2][2]float64, w *reportOutput, mt *metrics) error {
	mt.reportRows++
	for j := 0; j < 3; j++ {
		mt.checksum[j] += e[j]
	}
	start := time.Now()
	rp, rb := p.growth[0][t]-1, p.growth[1][t]-1
	rt := "base"
	if t == 1 {
		rt = "local"
	}
	err := w.record([]string{c.scenario, entity, p.start, p.end, p.dayText, kind, name, rt}, []float64{weights[0] / float64(p.days), weights[1] / float64(p.days), e[0], e[1], e[2], e[0] + e[1] + e[2], rp, rb, rp - rb, contribution[0][t], contribution[1][t], contribution[0][t] - contribution[1][t]})
	mt.written++
	mt.write += seconds(start)
	return err
}
func (p *period) write(c config, entity string, names []string, w *reportOutput, mt *metrics) error {
	start := time.Now()
	writeBefore := mt.write
	p.dayText = strconv.Itoa(p.days)
	var totals [2][3]float64
	var residual acc
	var contributions [2][2]float64
	compactSubset := c.scenario == "subset"
	for _, a := range p.acc {
		for side := 0; side < 2; side++ {
			for t := 0; t < 2; t++ {
				contributions[side][t] += a.contribution[side][t]
			}
		}
		for t := 0; t < 2; t++ {
			for j := 0; j < 3; j++ {
				totals[t][j] += a.e[t][j]
			}
		}
		if compactSubset && a.present && !a.held {
			residual.present = true
			for side := 0; side < 2; side++ {
				residual.w[side] += a.w[side]
				for t := 0; t < 2; t++ {
					residual.contribution[side][t] += a.contribution[side][t]
				}
			}
			for t := 0; t < 2; t++ {
				for j := 0; j < 3; j++ {
					residual.e[t][j] += a.e[t][j]
				}
			}
		}
	}
	for t := 0; t < 2; t++ {
		if math.Abs(totals[t][0]+totals[t][1]+totals[t][2]-(p.growth[0][t]-p.growth[1][t])) > 1e-8 {
			return errors.New("linked reconciliation failed")
		}
	}
	for side := 0; side < 2; side++ {
		for t := 0; t < 2; t++ {
			v := contributions[side][t]
			if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v-(p.growth[side][t]-1)) > 1e-8 {
				return errors.New("contribution reconciliation failed")
			}
		}
	}
	if p.portfolioOnly {
		for t := 0; t < 2; t++ {
			if e := p.row(c, entity, "total", "ALL", t, p.acc[0].w, totals[t], p.acc[0].contribution, w, mt); e != nil {
				return e
			}
		}
	} else {
		for k, a := range p.acc {
			if a.present && (!compactSubset || a.held) {
				for t := 0; t < 2; t++ {
					if e := p.row(c, entity, "detail", names[k], t, a.w, a.e[t], a.contribution, w, mt); e != nil {
						return e
					}
				}
			}
		}
		if residual.present {
			for t := 0; t < 2; t++ {
				if e := p.row(c, entity, "benchmark_only", "BENCHMARK_ONLY", t, residual.w, residual.e[t], residual.contribution, w, mt); e != nil {
					return e
				}
			}
		}
	}
	mt.compute += math.Max(0, seconds(start)-(mt.write-writeBefore))
	return nil
}

func outputPath(c config, path string) string {
	return filepath.Join(c.output, c.scenario+"-"+strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))+filepath.Ext(path))
}
func worker(path string, c config, m *master, input <-chan *day) (mt metrics, err error) {
	s, e := newStream(path)
	if e != nil {
		return mt, e
	}
	defer s.r.Close()
	start := time.Now()
	w, e := newReportOutput(outputPath(c, path)+".partial", filepath.Ext(path) == ".parquet")
	if e != nil {
		return mt, e
	}
	defer w.abort()
	mt.write += seconds(start)

	var p *period
	entity := ""
	names := m.names
	if c.scenario == "country" {
		names = m.countryNames
	} else if c.scenario == "subset" {
		names = m.subsetNames
	}
	count := 0
	for b := range input {
		d, e := s.next(m, c, &mt)
		if e != nil {
			return mt, e
		}
		if d == nil {
			return mt, errors.New("portfolio ended before index")
		}
		if d.date != b.date {
			return mt, errors.New("portfolio/index date mismatch")
		}
		entity = d.entity
		key, e := bucket(d.date, c.period)
		if e != nil {
			return mt, e
		}
		if p != nil && p.key != key {
			if e = p.write(c, entity, names, w, &mt); e != nil {
				return mt, e
			}
			p = nil
		}
		if p == nil {
			p = newPeriod(d, key, c)
		}
		if e = p.add(d, b, &mt); e != nil {
			return mt, e
		}
		count++
	}
	if count == 0 {
		return mt, errors.New("no input dates")
	}
	if c.maxDays == 0 || count < c.maxDays {
		d, e := s.next(m, c, &mt)
		if e != nil {
			return mt, e
		}
		if d != nil {
			return mt, errors.New("extra portfolio dates")
		}
	}
	if e = p.write(c, entity, names, w, &mt); e != nil {
		return mt, e
	}

	start = time.Now()
	if e = w.finish(); e != nil {
		return mt, e
	}
	mt.write += seconds(start)

	return mt, nil
}
func run() error {
	wall := time.Now()
	var c config
	var listen string
	flag.StringVar(&listen, "listen", "", "serve six-input HTTP API, e.g. 127.0.0.1:8080")
	flag.StringVar(&c.data, "data", "attribution_data", "CSV and/or Parquet input directory")
	flag.StringVar(&c.output, "output-dir", "results/go", "CSV output directory")
	flag.StringVar(&c.metrics, "metrics", "results/go-metrics.csv", "metrics CSV")
	flag.StringVar(&c.scenario, "scenario", "country", "security|subset|country|weekly|monthly|cumulative|six-input")
	flag.StringVar(&c.period, "period", "", "all|day|week|month|quarter|year")
	flag.StringVar(&c.subset, "instruments-file", "", "one-column InstrumentId CSV or Parquet")
	flag.IntVar(&c.threads, "threads", runtime.GOMAXPROCS(0), "CPU parallelism (default: Go runtime limit; respects GOMAXPROCS)")
	flag.IntVar(&c.maxDays, "max-days", 0, "0 reads all dates")
	flag.IntVar(&c.effectiveWorkers, "effective-date-workers", runtime.GOMAXPROCS(0), "parallel cumulative month-end calculations")
	flag.StringVar(&c.startDate, "start-date", "", "six-input inclusive calculation start YYYY-MM-DD")
	flag.StringVar(&c.endDate, "end-date", "", "six-input inclusive calculation end YYYY-MM-DD")
	flag.StringVar(&c.smfTrace, "smf-trace", "off", "off|summary|records: per-worker SMF JSONL in output directory")
	flag.Parse()
	if c.smfTrace != "off" && c.smfTrace != "summary" && c.smfTrace != "records" {
		return errors.New("smf-trace must be off, summary, or records")
	}
	if c.threads < 1 || c.maxDays < 0 {
		return errors.New("invalid threads/max-days")
	}
	if listen != "" {
		runtime.GOMAXPROCS(c.threads)
		return serveAttribution(listen, c)
	}
	switch c.scenario {
	case "security", "subset", "country", "weekly", "monthly", "cumulative", "six-input":
	default:
		return errors.New("invalid scenario")
	}
	if c.period == "" {
		c.period = "all"
		if c.scenario == "country" || c.scenario == "six-input" {
			c.period = "day"
		}
		if c.scenario == "weekly" {
			c.period = "week"
		}
		if c.scenario == "monthly" {
			c.period = "month"
		}
	}
	switch c.period {
	case "all", "day", "week", "month", "quarter", "year":
	default:
		return errors.New("invalid period")
	}
	if c.scenario == "country" && c.period != "day" {
		return errors.New("country requires --period day (US-only daily attribution)")
	}
	if c.scenario == "subset" && c.subset == "" {
		return errors.New("subset requires --instruments-file")
	}
	if (c.startDate != "" || c.endDate != "") && c.scenario != "six-input" {
		return errors.New("start-date/end-date currently apply to six-input only")
	}
	if c.scenario == "six-input" {
		runtime.GOMAXPROCS(c.threads)
		return runSixInput(c)
	}
	if c.scenario == "cumulative" {
		if c.effectiveWorkers < 1 || c.period != "all" || c.subset != "" {
			return errors.New("cumulative requires positive effective-date-workers, period all, and no instrument filter")
		}
		runtime.GOMAXPROCS(c.threads)
		return runCumulative(c)
	}
	runtime.GOMAXPROCS(c.threads)
	masterStart := time.Now()
	m, e := loadMaster(c)
	masterSeconds := seconds(masterStart)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(c.output, 0755); e != nil {
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
	paths, e := portfolioInputs(c.data)
	if e != nil {
		return e
	}
	if len(paths) == 0 {
		return errors.New("no portfolios")
	}
	indexPath, e := resolveInput(c.data, "index")
	if e != nil {
		return e
	}
	masterPath, e := resolveInput(c.data, "security_master")
	if e != nil {
		return e
	}
	pipelineStart := time.Now()
	mt, indexMetrics, portfolioMetrics, days, parallel, e := countryParallel(c, &m, paths, indexPath)
	if e != nil {
		return e
	}
	if !parallel {
		type result struct {
			m metrics
			e error
		}
		results := make(chan result, len(paths))
		inputs := make([]chan *day, len(paths))
		done := make([]chan struct{}, len(paths))
		for i, path := range paths {
			inputs[i] = make(chan *day, 1)
			done[i] = make(chan struct{})
			go func(i int, path string) {
				mt, e := worker(path, c, &m, inputs[i])
				close(done[i])
				results <- result{mt, e}
			}(i, path)
		}
		index, e := newStream(indexPath)
		days = 0
		production := e
		if e == nil {
			for c.maxDays == 0 || days < c.maxDays {
				d, e := index.next(&m, c, &mt)
				if e != nil {
					production = e
					break
				}
				if d == nil {
					break
				}
				for i, ch := range inputs {
					select {
					case ch <- d:
					case <-done[i]:
					}
				}
				days++
			}
			index.r.Close()
		}
		for _, ch := range inputs {
			close(ch)
		}
		indexMetrics = mt
		var workerError error
		for range paths {
			r := <-results
			mt.add(r.m)
			portfolioMetrics.add(r.m)
			if r.e != nil {
				workerError = r.e
			}
		}
		if production != nil {
			return production
		}
		if workerError != nil {
			return workerError
		}
	}
	pipelineEnd := time.Now()
	mt.phaseMaster = masterSeconds
	mt.phaseSetup = pipelineStart.Sub(wall).Seconds() - masterSeconds
	if parallel {
		mt.phaseOverhead = math.Max(0, pipelineEnd.Sub(pipelineStart).Seconds()-mt.phaseBoundary-mt.phaseParallel-mt.phaseOrdered)
	} else {
		mt.phaseStreaming = pipelineEnd.Sub(pipelineStart).Seconds()
	}
	var inputBytes uint64
	for _, p := range append(append([]string{}, paths...), indexPath, masterPath) {
		s, e := os.Stat(p)
		if e != nil {
			return e
		}
		inputBytes += uint64(s.Size())
	}

	for _, p := range paths {
		target := outputPath(c, p)
		if e = os.Rename(target+".partial", target); e != nil {
			return e
		}
		st, e := os.Stat(target)
		if e != nil {
			return e
		}
		mt.bytes += uint64(st.Size())
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
	wallSeconds := seconds(wall)
	mt.phaseFinalize = wallSeconds - pipelineEnd.Sub(wall).Seconds()
	header := []string{"language", "scenario", "period", "threads", "days", "input_file_bytes", "rows_read", "rows_retained", "groups", "effect_values", "output_rows", "output_bytes", "parsed_input_bytes", "read_parse_seconds", "filter_seconds", "aggregate_seconds", "compute_seconds", "write_seconds", "wall_seconds", "report_rows", "checksum_allocation", "checksum_selection", "checksum_interaction"}
	u := func(v uint64) string { return strconv.FormatUint(v, 10) }
	values := []string{"go", c.scenario, c.period, strconv.Itoa(c.threads), strconv.Itoa(days), u(inputBytes), u(mt.rows), u(mt.retained), u(mt.groups), u(mt.effects), u(mt.written), u(mt.bytes), u(mt.parsed), number(mt.read), number(mt.filter), number(mt.aggregate), number(mt.compute), number(mt.write), number(wallSeconds), u(mt.reportRows), number(mt.checksum[0]), number(mt.checksum[1]), number(mt.checksum[2])}
	if os.Getenv("ATTRIBUTION_PROFILE_READ") == "1" {
		header = append(header, "parquet_batch_read_decode_seconds", "parquet_to_daily_buffer_seconds", "parquet_batches")
		values = append(values, number(mt.batchDecode), number(mt.toDailyBuffer), u(mt.parquetBatches))
	}
	for _, side := range []struct {
		name string
		m    metrics
	}{{"index", indexMetrics}, {"portfolio", portfolioMetrics}} {
		header = append(header, side.name+"_read_seconds", side.name+"_filter_seconds", side.name+"_aggregate_seconds", side.name+"_compute_seconds", side.name+"_write_seconds")
		values = append(values, number(side.m.read), number(side.m.filter), number(side.m.aggregate), number(side.m.compute), number(side.m.write))
	}
	header = append(header, "date_range_workers", "date_range_tasks", "boundary_scan_seconds")
	values = append(values, u(mt.rangeWorkers), u(mt.rangeTasks), number(mt.boundary))
	header = append(header, "setup_seconds", "master_load_seconds", "boundary_plan_wall_seconds", "parallel_read_filter_aggregate_wall_seconds", "ordered_compute_write_wall_seconds", "streaming_pipeline_wall_seconds", "finalize_seconds", "pipeline_overhead_seconds")
	values = append(values, number(mt.phaseSetup), number(mt.phaseMaster), number(mt.phaseBoundary), number(mt.phaseParallel), number(mt.phaseOrdered), number(mt.phaseStreaming), number(mt.phaseFinalize), number(mt.phaseOverhead))
	w.Write(header)
	w.Write(values)
	w.Flush()
	fmt.Printf("%d rows written; metrics %s\n", mt.written, c.metrics)
	return w.Error()
}
func main() {
	finish, err := startDiagnostics()
	if err == nil {
		err = run()
		if stopErr := finish(); err == nil {
			err = stopErr
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
