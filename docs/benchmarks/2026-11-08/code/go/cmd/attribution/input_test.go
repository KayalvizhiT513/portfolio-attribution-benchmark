package main

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"testing"
)

func dateBatch(days []arrow.Date32) arrow.RecordBatch {
	b := array.NewDate32Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(days, nil)
	a := b.NewArray()
	defer a.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "EffectiveDate", Type: arrow.FixedWidthTypes.Date32}}, nil), []arrow.Array{a}, int64(len(days)))
}
func TestDateCacheAcrossBatches(t *testing.T) {
	a := dateBatch([]arrow.Date32{19358, 19358})
	defer a.Release()
	b := dateBatch([]arrow.Date32{19358, 19359, 19359})
	defer b.Release()
	c := date32Cache{column: 0}
	c.refresh(a, 0)
	c.refresh(a, 1)
	c.refresh(b, 0)
	if c.text != "2023-01-01" || c.formats != 1 {
		t.Fatal(c)
	}
	c.refresh(b, 1)
	c.refresh(b, 2)
	if c.text != "2023-01-02" || c.formats != 2 {
		t.Fatal(c)
	}
}
