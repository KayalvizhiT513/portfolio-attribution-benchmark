package main

// Native format adapters. Parquet stays typed through Arrow decoding; numeric
// values are never formatted as text and reparsed for the attribution engine.
import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type inputReader interface {
	next() (bool, error)
	text(int) string
	number(int) (float64, error)
	parsedBytes() int64
	Close() error
}

func resolveInput(dir, stem string) (string, error) {
	var found string
	for _, ext := range []string{".csv", ".parquet"} {
		path := filepath.Join(dir, stem+ext)
		st, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !st.Mode().IsRegular() {
			return "", fmt.Errorf("not a regular input file: %s", path)
		}
		if found != "" {
			return "", fmt.Errorf("ambiguous input: both CSV and Parquet exist for %s", stem)
		}
		found = path
	}
	if found == "" {
		return "", fmt.Errorf("missing %s.csv or %s.parquet", stem, stem)
	}
	return found, nil
}

func portfolioInputs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	stems := map[string]bool{}
	var paths []string
	for _, entry := range entries {
		name := entry.Name()
		ext := filepath.Ext(name)
		if entry.IsDir() || !strings.HasPrefix(name, "portfolio_") || (ext != ".csv" && ext != ".parquet") {
			continue
		}
		stem := strings.TrimSuffix(name, ext)
		if stems[stem] {
			return nil, fmt.Errorf("ambiguous portfolio input: %s", stem)
		}
		stems[stem] = true
		paths = append(paths, filepath.Join(dir, name))
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no portfolios")
	}
	return paths, nil
}

type csvInput struct {
	f   *os.File
	r   *csv.Reader
	row []string
}

func (r *csvInput) next() (bool, error) {
	row, err := r.r.Read()
	if err == io.EOF {
		return false, nil
	}
	r.row = row
	return err == nil, err
}
func (r *csvInput) text(i int) string             { return r.row[i] }
func (r *csvInput) number(i int) (float64, error) { return strconv.ParseFloat(r.row[i], 64) }
func (r *csvInput) parsedBytes() int64            { return r.r.InputOffset() }
func (r *csvInput) Close() error                  { return r.f.Close() }

type parquetInput struct {
	f            *file.Reader
	r            array.RecordReader
	batch        arrow.RecordBatch
	row          int
	columns      []int
	dateCache    *date32Cache
	profileRead  bool
	batchSeconds float64
	batches      uint64
}

type date32Cache struct {
	column  int
	day     arrow.Date32
	valid   bool
	text    string
	formats uint64
}

func (c *date32Cache) refresh(batch arrow.RecordBatch, row int) {
	day := batch.Column(c.column).(*array.Date32).Value(row)
	if !c.valid || day != c.day {
		c.text = day.FormattedString()
		c.day = day
		c.valid = true
		c.formats++
	}
}

type inputReadProfile struct {
	seconds float64
	batches uint64
	enabled bool
}
type profiledInput interface{ readProfile() inputReadProfile }

func (r *parquetInput) readProfile() inputReadProfile {
	return inputReadProfile{r.batchSeconds, r.batches, r.profileRead}
}

func (r *parquetInput) next() (bool, error) {
	if r.batch != nil {
		r.row++
		if int64(r.row) < r.batch.NumRows() {
			if r.dateCache != nil {
				r.dateCache.refresh(r.batch, r.row)
			}
			return true, nil
		}
	}
	for {
		var started time.Time
		if r.profileRead {
			started = time.Now()
		}
		next := r.r.Next()
		if r.profileRead {
			r.batchSeconds += time.Since(started).Seconds()
			if next {
				r.batches++
			}
		}
		if !next {
			break
		}
		r.batch = r.r.RecordBatch()
		r.row = 0
		if r.batch.NumRows() == 0 {
			continue
		}
		for _, col := range r.batch.Columns() {
			if col.NullN() != 0 {
				return false, fmt.Errorf("null Parquet input field")
			}
		}
		if r.dateCache != nil {
			r.dateCache.refresh(r.batch, 0)
		}
		return true, nil
	}
	r.batch = nil
	return false, r.r.Err()
}
func (r *parquetInput) text(i int) string {
	if r.dateCache != nil && r.columns[i] == r.dateCache.column {
		return r.dateCache.text
	}
	switch a := r.batch.Column(r.columns[i]).(type) {
	case *array.String:
		return a.Value(r.row)
	case *array.LargeString:
		return a.Value(r.row)
	case *array.StringView:
		return a.Value(r.row)
	case *array.Date32:
		return a.Value(r.row).FormattedString()
	default:
		panic("text accessed on a non-text column after schema validation")
	}
}
func (r *parquetInput) number(i int) (float64, error) {
	return r.batch.Column(r.columns[i]).(*array.Float64).Value(r.row), nil
}

// There is no CSV parser offset for compressed Parquet; physical sizes remain
// available in input_file_bytes. Do not confuse decoded bytes with disk I/O.
func (r *parquetInput) parsedBytes() int64 { return 0 }
func (r *parquetInput) Close() error {
	r.r.Release()
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}

func openInput(path string, header []string) (inputReader, error) {
	return openInputGroups(path, header, nil)
}

// A non-nil rowGroups list bounds Parquet decoding to this task's groups.
func openInputGroups(path string, header []string, rowGroups []int) (inputReader, error) {
	if filepath.Ext(path) == ".csv" {
		f, r, err := openCSV(path, header)
		if err != nil {
			return nil, err
		}
		return &csvInput{f: f, r: r}, nil
	}
	if filepath.Ext(path) != ".parquet" {
		return nil, fmt.Errorf("unsupported input format: %s", path)
	}
	f, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (inputReader, error) { f.Close(); return nil, err }
	ar, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{BatchSize: 65536, Parallel: false}, memory.DefaultAllocator)
	if err != nil {
		return fail(err)
	}
	schema, err := ar.Schema()
	if err != nil {
		return fail(err)
	}
	var projection []int
	for _, name := range header {
		matches := schema.FieldIndices(name)
		if len(matches) != 1 {
			return fail(fmt.Errorf("missing or duplicate Parquet column %s", name))
		}
		index := matches[0]
		t := schema.Field(index).Type.ID()
		valid := t == arrow.STRING || t == arrow.LARGE_STRING || t == arrow.STRING_VIEW
		switch name {
		case "Weight", "BaseReturn", "LocalReturn":
			valid = t == arrow.FLOAT64
		case "EffectiveDate":
			valid = valid || t == arrow.DATE32
		}
		if !valid {
			return fail(fmt.Errorf("unsupported Parquet type for %s: %s", name, schema.Field(index).Type))
		}
		projection = append(projection, index)
	}
	// Required fields are flat scalar columns, so field and leaf indices match
	// only when the file has no nested extras. Resolve physical leaf indices.
	var leaves []int
	for _, index := range projection {
		field := ar.Manifest.Fields[index]
		if len(field.Children) != 0 {
			return fail(fmt.Errorf("nested input field"))
		}
		leaves = append(leaves, field.ColIndex)
	}
	rr, err := ar.GetRecordReader(context.Background(), leaves, rowGroups)
	if err != nil {
		return fail(err)
	}
	columns := make([]int, len(header))
	for i, name := range header {
		columns[i] = rr.Schema().FieldIndices(name)[0]
	}
	var cache *date32Cache
	if os.Getenv("ATTRIBUTION_DISABLE_DATE_CACHE") != "1" {
		indices := rr.Schema().FieldIndices("EffectiveDate")
		if len(indices) == 1 && rr.Schema().Field(indices[0]).Type.ID() == arrow.DATE32 {
			cache = &date32Cache{column: indices[0]}
		}
	}
	return &parquetInput{f: f, r: rr, columns: columns, dateCache: cache, profileRead: os.Getenv("ATTRIBUTION_PROFILE_READ") == "1"}, nil
}
