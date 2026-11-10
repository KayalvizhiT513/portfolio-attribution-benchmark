package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func writeCountryGroups(t *testing.T, path, entity string, mixed bool) {
	t.Helper()
	fields := []arrow.Field{
		{Name: "Entity", Type: arrow.BinaryTypes.String},
		{Name: "EffectiveDate", Type: arrow.FixedWidthTypes.Date32},
		{Name: "InstrumentId", Type: arrow.BinaryTypes.String},
		{Name: "Weight", Type: arrow.PrimitiveTypes.Float64},
		{Name: "BaseReturn", Type: arrow.PrimitiveTypes.Float64},
		{Name: "LocalReturn", Type: arrow.PrimitiveTypes.Float64},
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for date := 0; date < 5; date++ {
		name := entity
		if mixed && date == 4 {
			name = "OTHER"
		}
		for _, id := range []string{"A", "B"} {
			builder.Field(0).(*array.StringBuilder).Append(name)
			builder.Field(1).(*array.Date32Builder).Append(19358 + arrow.Date32(date))
			builder.Field(2).(*array.StringBuilder).Append(id)
			builder.Field(3).(*array.Float64Builder).Append(0.5)
			builder.Field(4).(*array.Float64Builder).Append(0.01)
			builder.Field(5).(*array.Float64Builder).Append(0.02)
		}
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(1)), pqarrow.NewArrowWriterProperties())
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = writer.Write(batch); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParquetCompleteDateRanges(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "security_master.csv"), []byte("InstrumentId,Currency,Country\nA,USD,US\nB,USD,US\n"), 0600); err != nil {
		t.Fatal(err)
	}
	index, portfolio := filepath.Join(dir, "index.parquet"), filepath.Join(dir, "portfolio_PF.parquet")
	writeCountryGroups(t, index, "INDEX", false)
	writeCountryGroups(t, portfolio, "PF", false)
	ranges, ok, err := parquetCountryRanges(index, 0)
	if err != nil || !ok || len(ranges) != 5 {
		t.Fatalf("ranges=%v ok=%v err=%v", ranges, ok, err)
	}
	for i, r := range ranges {
		if r.start != int64(2*i) || r.end != int64(2*i+2) {
			t.Fatal("split a date across workers", ranges)
		}
	}
	c := config{data: dir, output: dir, scenario: "country", period: "day", threads: 12}
	m, err := loadMaster(c)
	if err != nil {
		t.Fatal(err)
	}
	mt, _, _, days, used, err := countryParallel(c, &m, []string{portfolio}, index)
	if err != nil || !used || days != 5 || mt.rows != 20 || mt.written != 10 {
		t.Fatalf("days=%d rows=%d written=%d used=%v err=%v", days, mt.rows, mt.written, used, err)
	}
	c.maxDays = 2
	mt, _, _, days, used, err = countryParallel(c, &m, []string{portfolio}, index)
	if err != nil || !used || days != 2 || mt.rows != 8 {
		t.Fatalf("max-days: days=%d rows=%d used=%v err=%v", days, mt.rows, used, err)
	}
	writeCountryGroups(t, portfolio, "PF", true)
	c.maxDays = 0
	if _, _, _, _, _, err = countryParallel(c, &m, []string{portfolio}, index); err == nil {
		t.Fatal("accepted mixed entities across range boundaries")
	}
}
