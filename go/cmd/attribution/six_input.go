package main

// Six persistent readers per date worker. Decoding runs concurrently across all
// six files; the requested batch size is the selected complete date's row count.
import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

var sixNames = []string{"accounts", "forex", "security_master", "analytics", "portfolio_PF", "index"}

type sixRange struct {
	date   string
	groups []int
	rows   int64
}
type sixPlan struct {
	path     string
	metadata *metadata.FileMetaData
	schema   *arrow.Schema
	ranges   []sixRange
	byDate   map[string]int
}
type sixLoaded struct {
	batches []arrow.RecordBatch
	rows    uint64
	seconds float64
	err     error
}
type sixReport struct {
	date, entity        string
	names               []string
	period              *period
	mt                  metrics
	rows                [6]uint64
	reads               [6]float64
	joined              uint64
	analyticsSum, fxSum float64
}

func sixSchema(i int) []arrow.Field {
	s, d, f := arrow.BinaryTypes.String, arrow.FixedWidthTypes.Date32, arrow.PrimitiveTypes.Float64
	field := func(n string, t arrow.DataType) arrow.Field { return arrow.Field{Name: n, Type: t} }
	switch i {
	case 0:
		return []arrow.Field{field("AccountId", s), field("EffectiveDate", d), field("PortfolioEntity", s), field("BaseCurrency", s), field("IsActive", arrow.FixedWidthTypes.Boolean)}
	case 1:
		return []arrow.Field{field("EffectiveDate", d), field("FromCurrency", s), field("ToCurrency", s), field("Rate", f)}
	case 2:
		return []arrow.Field{field("InstrumentId", s), field("EffectiveDate", d), field("Currency", s), field("Country", s), field("AnalyticsSecId", arrow.PrimitiveTypes.Int64)}
	case 3:
		out := []arrow.Field{field("SecId", arrow.PrimitiveTypes.Int64), field("Effective Date", d)}
		for n := 1; n <= 15; n++ {
			out = append(out, field(fmt.Sprintf("Float%02d", n), f))
		}
		return out
	default:
		return []arrow.Field{field("Entity", s), field("EffectiveDate", d), field("InstrumentId", s), field("Weight", f), field("BaseReturn", f), field("LocalReturn", f)}
	}
}
func planSix(dir string, i int) (p sixPlan, err error) {
	p.path = filepath.Join(dir, sixNames[i]+".parquet")
	f, e := file.OpenParquetFile(p.path, false)
	if e != nil {
		return p, e
	}
	defer f.Close()
	ar, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if e != nil {
		return p, e
	}
	p.schema, e = ar.Schema()
	if e != nil {
		return p, e
	}
	expected := sixSchema(i)
	if p.schema.NumFields() != len(expected) {
		return p, fmt.Errorf("%s: unexpected columns", sixNames[i])
	}
	for _, field := range expected {
		indices := p.schema.FieldIndices(field.Name)
		if len(indices) != 1 || !arrow.TypeEqual(p.schema.Field(indices[0]).Type, field.Type) {
			return p, fmt.Errorf("%s: invalid column %s", sixNames[i], field.Name)
		}
	}
	dc := "EffectiveDate"
	if i == 3 {
		dc = "Effective Date"
	}
	col := ar.Manifest.Fields[p.schema.FieldIndices(dc)[0]].ColIndex
	p.byDate = map[string]int{}
	for g := 0; g < f.NumRowGroups(); g++ {
		rg := f.MetaData().RowGroup(g)
		chunk, e := rg.ColumnChunk(col)
		if e != nil {
			return p, e
		}
		stats, e := chunk.Statistics()
		if e != nil {
			return p, e
		}
		if stats == nil || !stats.HasMinMax() {
			return p, fmt.Errorf("missing date statistics")
		}
		lo, hi := stats.EncodeMin(), stats.EncodeMax()
		if len(lo) != 4 || len(hi) != 4 || binary.LittleEndian.Uint32(lo) != binary.LittleEndian.Uint32(hi) || rg.NumRows() <= 0 {
			return p, fmt.Errorf("six-input requires nonempty single-date row groups")
		}
		date := arrow.Date32(int32(binary.LittleEndian.Uint32(lo))).FormattedString()
		if _, e = parseDate(date); e != nil {
			return p, e
		}
		n := len(p.ranges)
		if n > 0 && date < p.ranges[n-1].date {
			return p, fmt.Errorf("date groups must be ordered")
		}
		if n == 0 || date != p.ranges[n-1].date {
			p.byDate[date] = n
			p.ranges = append(p.ranges, sixRange{date: date})
			n++
		}
		r := &p.ranges[n-1]
		r.groups = append(r.groups, g)
		r.rows += rg.NumRows()
	}
	if len(p.ranges) == 0 {
		return p, fmt.Errorf("empty input")
	}
	p.metadata = f.MetaData()
	return p, nil
}
func (p *sixPlan) selected(date string, snapshot bool) (*sixRange, error) {
	if !snapshot {
		n, ok := p.byDate[date]
		if !ok {
			return nil, fmt.Errorf("%s: missing date %s", p.path, date)
		}
		return &p.ranges[n], nil
	}
	n := sort.Search(len(p.ranges), func(n int) bool { return p.ranges[n].date > date }) - 1
	if n < 0 {
		return nil, fmt.Errorf("no SMF snapshot effective on %s", date)
	}
	return &p.ranges[n], nil
}
func releaseSix(batches []arrow.RecordBatch) {
	for _, b := range batches {
		b.Release()
	}
}
func readSix(f *file.Reader, p *sixPlan, date string, snapshot bool) (out sixLoaded) {
	start := time.Now()
	defer func() {
		out.seconds = seconds(start)
		if out.err != nil {
			releaseSix(out.batches)
			out.batches = nil
		}
	}()
	r, e := p.selected(date, snapshot)
	if e != nil {
		out.err = e
		return
	}
	ar, e := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{BatchSize: r.rows, Parallel: false}, memory.DefaultAllocator)
	if e != nil {
		out.err = e
		return
	}
	rr, e := ar.GetRecordReader(context.Background(), nil, r.groups)
	if e != nil {
		out.err = e
		return
	}
	defer rr.Release()
	dc := "EffectiveDate"
	if filepath.Base(p.path) == "analytics.parquet" {
		dc = "Effective Date"
	}
	di := p.schema.FieldIndices(dc)[0]
	for rr.Next() {
		b := rr.RecordBatch()
		for _, a := range b.Columns() {
			if a.NullN() != 0 {
				out.err = fmt.Errorf("null required input value")
				return
			}
		}
		ds := b.Column(di).(*array.Date32)
		if ds.Len() > 0 {
			expected := ds.Value(0)
			if expected.FormattedString() != r.date {
				out.err = fmt.Errorf("decoded date differs from planned date")
				return
			}
			for n := 0; n < ds.Len(); n++ {
				if ds.Value(n) != expected {
					out.err = fmt.Errorf("mixed decoded dates")
					return
				}
			}
		}
		b.Retain()
		out.batches = append(out.batches, b)
		out.rows += uint64(b.NumRows())
	}
	out.err = rr.Err()
	if out.err == nil && out.rows != uint64(r.rows) {
		out.err = fmt.Errorf("decoded row count differs from metadata")
	}
	return
}
func sixCol(b arrow.RecordBatch, n string) arrow.Array {
	return b.Column(b.Schema().FieldIndices(n)[0])
}
func sixString(b arrow.RecordBatch, n string, r int) string {
	return sixCol(b, n).(*array.String).Value(r)
}
func sixFloat(b arrow.RecordBatch, n string, r int) float64 {
	return sixCol(b, n).(*array.Float64).Value(r)
}
func sixDaily(batches []arrow.RecordBatch, m *master, c config, mt *metrics) (*day, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("empty return date")
	}
	rr, e := array.NewRecordReader(batches[0].Schema(), batches)
	if e != nil {
		return nil, e
	}
	cols := make([]int, len(inputHeader))
	for i, n := range inputHeader {
		cols[i] = rr.Schema().FieldIndices(n)[0]
	}
	input := &parquetInput{r: rr, columns: cols, dateCache: &date32Cache{column: rr.Schema().FieldIndices("EffectiveDate")[0]}}
	defer input.Close()
	s := stream{r: input}
	d, e := s.next(m, c, mt)
	if e != nil {
		return nil, e
	}
	extra, e := s.next(m, c, mt)
	if e != nil {
		return nil, e
	}
	if extra != nil {
		return nil, fmt.Errorf("extra return date")
	}
	return d, nil
}

type sixSMF struct {
	master     master
	currencies []string
	aliases    []int64
}

// A worker owns its trace; no global trace lock is needed.
type sixSMFTrace struct {
	file     *os.File
	writer   *bufio.Writer
	worker   int
	records  bool
	sequence uint64
	started  time.Time
}

func (t *sixSMFTrace) emit(event map[string]any) error {
	if t == nil {
		return nil
	}
	event["worker_id"] = t.worker
	event["sequence"] = t.sequence
	event["seconds"] = seconds(t.started)
	t.sequence++
	return json.NewEncoder(t.writer).Encode(event)
}
func sixSMFAttributes(country, currency string, alias int64) map[string]any {
	return map[string]any{"country": country, "currency": currency, "analytics_id": alias}
}

// Apply one effective date; securities absent from this delta retain their state.
func applySixSMF(cache **sixSMF, batches []arrow.RecordBatch, trace *sixSMFTrace, calculationDate, effectiveDate string) error {
	if *cache == nil {
		*cache = &sixSMF{master: master{ids: map[string]int{}}}
	}
	state := *cache
	m := &state.master
	seen := map[string]bool{}
	inserted, updated, unchanged := 0, 0, 0
	for _, b := range batches {
		ids := sixCol(b, "InstrumentId").(*array.String)
		cs := sixCol(b, "Currency").(*array.String)
		countries := sixCol(b, "Country").(*array.String)
		aliasIDs := sixCol(b, "AnalyticsSecId").(*array.Int64)
		for r := 0; r < int(b.NumRows()); r++ {
			id, currency, country, alias := ids.Value(r), cs.Value(r), countries.Value(r), aliasIDs.Value(r)
			if id == "" || currency == "" || country == "" || alias <= 0 || seen[id] {
				return fmt.Errorf("invalid or duplicate effective-date instrument")
			}
			seen[id] = true
			slot, exists := m.ids[id]
			var before any
			action := "inserted"
			if exists {
				oldCountry := m.countryNames[m.history[slot][0].country]
				if oldCountry != country || state.currencies[slot] != currency || state.aliases[slot] != alias {
					action = "updated"
					updated++
				} else {
					action = "unchanged"
					unchanged++
				}
				if trace != nil && trace.records {
					before = sixSMFAttributes(oldCountry, state.currencies[slot], state.aliases[slot])
				}
			} else {
				inserted++
			}
			countrySlot := 0
			for countrySlot < len(m.countryNames) && m.countryNames[countrySlot] != country {
				countrySlot++
			}
			if countrySlot == len(m.countryNames) {
				m.countryNames = append(m.countryNames, country)
			}
			if slot, exists := m.ids[id]; exists {
				m.history[slot] = []masterVersion{{"", countrySlot}}
				state.currencies[slot] = currency
				state.aliases[slot] = alias
			} else {
				m.ids[id] = len(m.names)
				m.names = append(m.names, id)
				m.history = append(m.history, []masterVersion{{"", countrySlot}})
				m.selected = append(m.selected, true)
				state.currencies = append(state.currencies, currency)
				state.aliases = append(state.aliases, alias)
			}
			if trace != nil && trace.records {
				if err := trace.emit(map[string]any{"event": "security_applied", "calculation_date": calculationDate,
					"effective_date": effectiveDate, "instrument_id": id, "action": action,
					"before": before, "after": sixSMFAttributes(country, currency, alias)}); err != nil {
					return err
				}
			}
		}
	}
	return trace.emit(map[string]any{"event": "effective_date_applied", "calculation_date": calculationDate,
		"effective_date": effectiveDate, "rows": inserted + updated + unchanged,
		"inserted": inserted, "updated": updated, "unchanged": unchanged, "state_securities": len(m.names)})
}

// Bootstrap history once, then read only new effective dates. Release each
// decoded change date immediately rather than retaining the historical batches.
func advanceSixSMF(f *file.Reader, p *sixPlan, date, previous string, cache **sixSMF, trace *sixSMFTrace) (out sixLoaded) {
	start := time.Now()
	defer func() { out.seconds = seconds(start) }()
	effectiveDates := 0
	first := sort.Search(len(p.ranges), func(i int) bool { return p.ranges[i].date > previous })
	for n := first; n < len(p.ranges) && p.ranges[n].date <= date; n++ {
		loaded := readSix(f, p, p.ranges[n].date, false)
		if loaded.err != nil {
			out.err = loaded.err
			return
		}
		out.err = applySixSMF(cache, loaded.batches, trace, date, p.ranges[n].date)
		effectiveDates++
		out.rows += loaded.rows
		releaseSix(loaded.batches)
		if out.err != nil {
			return
		}
	}
	if *cache == nil {
		out.err = fmt.Errorf("no SMF history effective on date")
		return
	}
	if trace != nil {
		mode := "update"
		var previousValue any = previous
		if previous == "" {
			mode = "initialize"
			previousValue = nil
		} else if out.rows == 0 {
			mode = "reuse"
		}
		out.err = trace.emit(map[string]any{"event": "date_ready", "calculation_date": date,
			"previous_date": previousValue, "mode": mode, "effective_dates_applied": effectiveDates,
			"rows_applied": out.rows, "state_securities": len((*cache).master.names)})
		if out.err == nil {
			out.err = trace.writer.Flush()
		}
	}
	return
}

func calculateSix(date string, loaded [6]sixLoaded, c config, smfCache **sixSMF) (out sixReport, err error) {
	defer func() {
		for _, v := range loaded {
			releaseSix(v.batches)
		}
	}()
	out.date = date
	for i, v := range loaded {
		out.rows[i] = v.rows
		out.reads[i] = v.seconds
	}
	type account struct {
		base   string
		active bool
	}
	accounts := map[string]account{}
	accountIDs := map[string]bool{}
	for _, b := range loaded[0].batches {
		for r := 0; r < int(b.NumRows()); r++ {
			id, entity, base := sixString(b, "AccountId", r), sixString(b, "PortfolioEntity", r), sixString(b, "BaseCurrency", r)
			_, exists := accounts[entity]
			if id == "" || entity == "" || base == "" || accountIDs[id] || exists {
				return out, fmt.Errorf("invalid or duplicate account/entity")
			}
			accountIDs[id] = true
			accounts[entity] = account{base, sixCol(b, "IsActive").(*array.Boolean).Value(r)}
		}
	}
	fx := map[[2]string]float64{}
	for _, b := range loaded[1].batches {
		for r := 0; r < int(b.NumRows()); r++ {
			a, z, rate := sixString(b, "FromCurrency", r), sixString(b, "ToCurrency", r), sixFloat(b, "Rate", r)
			key := [2]string{a, z}
			_, exists := fx[key]
			if a == "" || z == "" || math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 || (a == z && rate != 1) || exists {
				return out, fmt.Errorf("invalid or duplicate FX quote")
			}
			fx[key] = rate
		}
	}
	state := *smfCache
	m, currencies, aliases := &state.master, state.currencies, state.aliases
	analytics := map[int64]float64{}
	for _, b := range loaded[3].batches {
		vals := make([]*array.Float64, 15)
		for n := range vals {
			vals[n] = sixCol(b, fmt.Sprintf("Float%02d", n+1)).(*array.Float64)
		}
		ids := sixCol(b, "SecId").(*array.Int64)
		for r := 0; r < int(b.NumRows()); r++ {
			id := ids.Value(r)
			sum := 0.0
			for _, a := range vals {
				v := a.Value(r)
				if math.IsNaN(v) || math.IsInf(v, 0) {
					return out, fmt.Errorf("nonfinite analytics value")
				}
				sum += v
			}
			_, exists := analytics[id]
			if id <= 0 || exists {
				return out, fmt.Errorf("invalid or duplicate analytics security")
			}
			analytics[id] = sum
		}
	}
	pd, e := sixDaily(loaded[4].batches, m, c, &out.mt)
	if e != nil {
		return out, e
	}
	bd, e := sixDaily(loaded[5].batches, m, c, &out.mt)
	if e != nil {
		return out, e
	}
	if pd == nil || bd == nil || pd.date != date || bd.date != date {
		return out, fmt.Errorf("return date mismatch")
	}
	ac, ok := accounts[pd.entity]
	if !ok {
		return out, fmt.Errorf("missing portfolio account")
	}
	if !ac.active {
		return out, fmt.Errorf("portfolio account is inactive")
	}
	for k := range m.names {
		if pd.stats[k].present || bd.stats[k].present {
			v, ok := analytics[aliases[k]]
			if !ok {
				return out, fmt.Errorf("missing analytics for %s", m.names[k])
			}
			out.analyticsSum += v
			rate, ok := fx[[2]string{currencies[k], ac.base}]
			if !ok {
				return out, fmt.Errorf("missing FX currency/base quote")
			}
			out.fxSum += rate
			out.joined++
		}
	}
	out.period = newPeriod(pd, date, c)
	if e = out.period.add(pd, bd, &out.mt); e != nil {
		return out, e
	}
	out.entity = pd.entity
	out.names = m.countryNames
	return out, nil
}
func runSixInput(c config) error {
	start := time.Now()
	if c.period != "day" || c.subset != "" {
		return fmt.Errorf("six-input requires daily country calculation without subset")
	}
	c.scenario = "country"
	if e := os.MkdirAll(c.output, 0755); e != nil {
		return e
	}
	data, e := filepath.EvalSymlinks(c.data)
	if e != nil {
		return e
	}
	out, e := filepath.EvalSymlinks(c.output)
	if e != nil {
		return e
	}
	data, _ = filepath.Abs(data)
	out, _ = filepath.Abs(out)
	if data == out {
		return fmt.Errorf("output must differ from inputs")
	}
	plans := make([]sixPlan, 6)
	for i := range plans {
		plans[i], e = planSix(c.data, i)
		if e != nil {
			return e
		}
	}
	first, last := c.startDate, c.endDate
	if (first == "") != (last == "") {
		return fmt.Errorf("provide both start-date and end-date")
	}
	if first == "" {
		for i, p := range plans {
			if i == 2 {
				continue
			}
			a, z := p.ranges[0].date, p.ranges[len(p.ranges)-1].date
			if first == "" || a > first {
				first = a
			}
			if last == "" || z < last {
				last = z
			}
		}
	}
	firstDay, e := parseDate(first)
	if e != nil {
		return e
	}
	lastDay, e := parseDate(last)
	if e != nil {
		return e
	}
	if firstDay.After(lastDay) {
		return fmt.Errorf("start-date must not exceed end-date; inputs must overlap")
	}
	var days []string
	for d := firstDay; !d.After(lastDay); d = d.AddDate(0, 0, 1) {
		days = append(days, d.Format("2006-01-02"))
	}
	if c.maxDays > 0 && c.maxDays < len(days) {
		days = days[:c.maxDays]
	}
	for _, d := range days {
		for i := range plans {
			if _, e = plans[i].selected(d, i == 2); e != nil {
				return e
			}
		}
	}
	workers := min(c.threads, len(days))
	type result struct {
		reports []sixReport
		err     error
	}
	results := make(chan result, workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			res := result{}
			defer func() { results <- res }()
			files := make([]*file.Reader, 6)
			defer func() {
				for _, f := range files {
					if f != nil {
						f.Close()
					}
				}
			}()
			for i := range files {
				files[i], res.err = file.OpenParquetFile(plans[i].path, false, file.WithMetadata(plans[i].metadata))
				if res.err != nil {
					return
				}
			}
			var cachedMaster *sixSMF
			previousDate := ""
			first, last := len(days)*worker/workers, len(days)*(worker+1)/workers
			var trace *sixSMFTrace
			if c.smfTrace != "" && c.smfTrace != "off" {
				f, err := os.OpenFile(filepath.Join(c.output, fmt.Sprintf("smf-trace-worker-%02d.jsonl", worker+1)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
				if err != nil {
					res.err = err
					return
				}
				trace = &sixSMFTrace{file: f, writer: bufio.NewWriter(f), worker: worker + 1, records: c.smfTrace == "records", started: time.Now()}
				defer f.Close()
				defer trace.writer.Flush()
				res.err = trace.emit(map[string]any{"event": "worker_start", "engine": "go", "trace_level": c.smfTrace,
					"first_date": days[first], "last_date": days[last-1]})
				if res.err == nil {
					res.err = trace.writer.Flush()
				}
				if res.err != nil {
					return
				}
			}
			for n := first; n < last; n++ {
				date := days[n]
				var loaded [6]sixLoaded
				var wg sync.WaitGroup
				for i := range files {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						if i == 2 {
							loaded[i] = advanceSixSMF(files[i], &plans[i], date, previousDate, &cachedMaster, trace)
						} else {
							loaded[i] = readSix(files[i], &plans[i], date, false)
						}
					}(i)
				}
				wg.Wait()
				for _, v := range loaded {
					if v.err != nil {
						res.err = fmt.Errorf("date %s: %w", date, v.err)
					}
				}
				if res.err != nil {
					for _, v := range loaded {
						releaseSix(v.batches)
					}
					return
				}
				var report sixReport
				report, res.err = calculateSix(date, loaded, c, &cachedMaster)
				if res.err != nil {
					res.err = fmt.Errorf("date %s: %w", date, res.err)
					return
				}
				res.reports = append(res.reports, report)
				previousDate = date
			}
			if trace != nil {
				res.err = trace.emit(map[string]any{"event": "worker_complete"})
				if res.err == nil {
					res.err = trace.writer.Flush()
				}
			}
		}(worker)
	}
	var reports []sixReport
	var runErr error
	for k := 0; k < workers; k++ {
		r := <-results
		if r.err != nil {
			runErr = r.err
		}
		reports = append(reports, r.reports...)
	}
	if runErr != nil {
		return runErr
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].date < reports[j].date })
	target := outputPath(c, plans[4].path)
	partial := target + ".partial"
	w, e := newReportOutput(partial, true)
	if e != nil {
		return e
	}
	defer w.abort()
	var mt metrics
	var rows [6]uint64
	var reads [6]float64
	var joined uint64
	analyticsSum, fxSum := 0.0, 0.0
	entity := ""
	var smfLoads, smfHits uint64
	for _, r := range reports {
		if entity != "" && entity != r.entity {
			return fmt.Errorf("portfolio entity changes across dates")
		}
		entity = r.entity
		mt.add(r.mt)
		if e = r.period.write(c, r.entity, r.names, w, &mt); e != nil {
			return e
		}
		for i := range rows {
			rows[i] += r.rows[i]
			reads[i] += r.reads[i]
		}
		if r.rows[2] > 0 {
			smfLoads++
		} else {
			smfHits++
		}
		joined += r.joined
		analyticsSum += r.analyticsSum
		fxSum += r.fxSum
	}
	if e = w.finish(); e != nil {
		return e
	}
	if e = os.Rename(partial, target); e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(c.metrics), 0755); e != nil {
		return e
	}
	mf, e := os.Create(c.metrics)
	if e != nil {
		return e
	}
	defer mf.Close()
	cw := csv.NewWriter(mf)
	header := []string{"language", "workers", "readers_per_worker", "days", "output_rows", "joined_us_securities", "analytics_checksum", "fx_checksum", "wall_seconds"}
	values := []string{"go", strconv.Itoa(workers), "6", strconv.Itoa(len(days)), strconv.FormatUint(mt.written, 10), strconv.FormatUint(joined, 10), number(analyticsSum), number(fxSum), number(seconds(start))}
	for i, name := range sixNames {
		header = append(header, name+"_rows_decoded", name+"_read_decode_seconds")
		values = append(values, strconv.FormatUint(rows[i], 10), number(reads[i]))
	}
	header = append(header, "smf_snapshot_loads", "smf_cache_hits")
	values = append(values, strconv.FormatUint(smfLoads, 10), strconv.FormatUint(smfHits, 10))
	if e = cw.Write(header); e != nil {
		return e
	}
	if e = cw.Write(values); e != nil {
		return e
	}
	cw.Flush()
	return cw.Error()
}
