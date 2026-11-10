package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// reportOutput retains typed values through Arrow/Parquet output. Only the CSV
// branch formats numeric text. Both writers buffer at most 65,536 report rows.
type reportOutput struct {
	f       *os.File
	bw      *bufio.Writer
	csv     *rowWriter
	pq      *pqarrow.FileWriter
	builder *array.RecordBuilder
	closed  bool
}

func newReportOutput(path string, isParquet bool) (*reportOutput, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &reportOutput{f: f}
	if !isParquet {
		w.bw = bufio.NewWriterSize(f, 1024*1024)
		w.csv = &rowWriter{w: w.bw, buffer: make([]byte, 0, 512)}
		if err = w.csv.Write(outputHeader); err != nil {
			w.abort()
			return nil, err
		}
		return w, nil
	}
	fields := make([]arrow.Field, len(outputHeader))
	for i, name := range outputHeader {
		var dtype arrow.DataType = arrow.BinaryTypes.String
		switch {
		case i == 2 || i == 3:
			dtype = arrow.FixedWidthTypes.Date32
		case i == 4:
			dtype = arrow.PrimitiveTypes.Uint32
		case i >= 8:
			dtype = arrow.PrimitiveTypes.Float64
		}
		fields[i] = arrow.Field{Name: name, Type: dtype, Nullable: false}
	}
	schema := arrow.NewSchema(fields, nil)
	w.builder = array.NewRecordBuilder(memory.DefaultAllocator, schema)
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd), parquet.WithMaxRowGroupLength(65536))
	// Hide os.File.Close from the Parquet sink so fsync can follow the footer.
	w.pq, err = pqarrow.NewFileWriter(schema, struct{ io.Writer }{f}, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		w.abort()
		return nil, err
	}
	return w, nil
}
func (w *reportOutput) record(fields []string, numbers []float64) error {
	if w.csv != nil {
		return w.csv.record(fields, numbers)
	}
	if len(fields) != 8 || len(numbers) != 12 {
		return fmt.Errorf("invalid report row")
	}
	for i, value := range fields {
		switch i {
		case 2, 3:
			dt, err := parseDate(value)
			if err != nil {
				return err
			}
			w.builder.Field(i).(*array.Date32Builder).Append(arrow.Date32FromTime(dt))
		case 4:
			n, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return err
			}
			w.builder.Field(i).(*array.Uint32Builder).Append(uint32(n))
		default:
			w.builder.Field(i).(*array.StringBuilder).Append(value)
		}
	}
	for i, n := range numbers {
		w.builder.Field(i + 8).(*array.Float64Builder).Append(n)
	}
	if w.builder.Field(0).Len() == 65536 {
		return w.flushBatch()
	}
	return nil
}
func (w *reportOutput) flushBatch() error {
	if w.builder == nil || w.builder.Field(0).Len() == 0 {
		return nil
	}
	record := w.builder.NewRecordBatch()
	defer record.Release()
	return w.pq.Write(record)
}
func (w *reportOutput) finish() error {
	if w.csv != nil {
		if err := w.bw.Flush(); err != nil {
			return err
		}
	} else {
		if err := w.flushBatch(); err != nil {
			return err
		}
		if err := w.pq.Close(); err != nil {
			return err
		}
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	if w.builder != nil {
		w.builder.Release()
		w.builder = nil
	}
	w.closed = true
	return w.f.Close()
}
func (w *reportOutput) abort() {
	if w.closed {
		return
	}
	if w.pq != nil {
		_ = w.pq.Close()
	}
	if w.builder != nil {
		w.builder.Release()
		w.builder = nil
	}
	_ = w.f.Close()
	w.closed = true
}
