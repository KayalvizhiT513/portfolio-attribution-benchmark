package main

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Footer statistics must establish one complete date per contiguous group range.
// A date can span several groups. Mixed-date groups or missing statistics use
// streaming, so no records are split, guessed, skipped, or decoded twice.
func parquetCountryRanges(path string, limit int) ([]dateRange, bool, error) {
	f, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	ar, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, false, err
	}
	schema, err := ar.Schema()
	if err != nil {
		return nil, false, err
	}
	indices := schema.FieldIndices("EffectiveDate")
	if len(indices) != 1 {
		return nil, false, nil
	}
	index := indices[0]
	field := ar.Manifest.Fields[index]
	if len(field.Children) != 0 {
		return nil, false, nil
	}
	kind := schema.Field(index).Type.ID()
	if kind != arrow.DATE32 && kind != arrow.STRING && kind != arrow.LARGE_STRING && kind != arrow.STRING_VIEW {
		return nil, false, nil
	}
	var result []dateRange
	for group := 0; group < f.NumRowGroups(); group++ {
		rg := f.MetaData().RowGroup(group)
		if rg.NumRows() == 0 {
			return nil, false, nil
		}
		column, err := rg.ColumnChunk(field.ColIndex)
		if err != nil {
			return nil, false, err
		}
		stats, err := column.Statistics()
		if err != nil {
			return nil, false, err
		}
		if stats == nil || !stats.HasMinMax() {
			return nil, false, nil
		}
		lo, hi := stats.EncodeMin(), stats.EncodeMax()
		if !bytes.Equal(lo, hi) {
			return nil, false, nil
		}
		var date string
		if kind == arrow.DATE32 {
			if len(lo) != 4 {
				return nil, false, nil
			}
			date = arrow.Date32(int32(binary.LittleEndian.Uint32(lo))).FormattedString()
		} else {
			date = string(lo)
		}
		if _, err := parseDate(date); err != nil {
			return nil, false, nil
		}
		if len(result) > 0 && result[len(result)-1].date == date {
			result[len(result)-1].end = int64(group + 1)
			continue
		}
		if limit > 0 && len(result) == limit {
			break
		}
		if len(result) > 0 && date < result[len(result)-1].date {
			return nil, false, fmt.Errorf("dates must strictly increase: %s", path)
		}
		result = append(result, dateRange{date: date, start: int64(group), end: int64(group + 1)})
	}
	return result, true, nil
}
