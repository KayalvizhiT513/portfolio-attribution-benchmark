package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func run() error {
	path := "results/large_parquet/securities.parquet"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	workers := 12
	if len(os.Args) > 2 {
		var err error
		workers, err = strconv.Atoi(os.Args[2])
		if err != nil {
			return err
		}
	}
	if workers < 1 || len(os.Args) > 3 {
		return fmt.Errorf("usage: scan-go [path] [positive worker count]")
	}
	start := time.Now()
	f, err := file.OpenParquetFile(path, false)
	if err != nil {
		return err
	}
	defer f.Close()
	groups := f.NumRowGroups()
	if groups == 0 {
		return fmt.Errorf("no row groups")
	}
	workers = min(workers, groups)
	type result struct {
		rows, batches int64
		err           error
	}
	results := make(chan result, workers)
	for worker := 0; worker < workers; worker++ {
		first, last := groups*worker/workers, groups*(worker+1)/workers
		go func() {
			r := result{}
			defer func() { results <- r }()
			pf, e := file.OpenParquetFile(path, false, file.WithMetadata(f.MetaData()))
			if e != nil {
				r.err = e
				return
			}
			defer pf.Close()
			var selected []int
			for i := first; i < last; i++ {
				selected = append(selected, i)
			}
			r.rows, r.batches, r.err = scan(pf, selected)
		}()
	}
	var rows, batches int64
	var firstError error
	for i := 0; i < workers; i++ {
		r := <-results
		rows += r.rows
		batches += r.batches
		if r.err != nil && firstError == nil {
			firstError = r.err
		}
	}
	if firstError != nil {
		return firstError
	}
	if rows != 365000000 {
		return fmt.Errorf("unexpected row count: %d", rows)
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"language": "go", "parallelism": "row_groups", "parallel_columns": false, "workers": workers, "rows": rows, "columns": 17, "batch_size": 65536, "batches": batches, "parquet_bytes": info.Size(), "seconds": time.Since(start).Seconds(), "gomaxprocs": runtime.GOMAXPROCS(0)})
}

func scan(f *file.Reader, groups []int) (int64, int64, error) {
	ar, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{BatchSize: 65536, Parallel: false}, memory.DefaultAllocator)
	if err != nil {
		return 0, 0, err
	}
	rr, err := ar.GetRecordReader(context.Background(), nil, groups)
	if err != nil {
		return 0, 0, err
	}
	defer rr.Release()
	var rows, batches int64
	for rr.Next() {
		batch := rr.RecordBatch()
		if batch.NumCols() != 17 {
			return 0, 0, fmt.Errorf("expected 17 columns")
		}
		rows += batch.NumRows()
		batches++
		runtime.KeepAlive(batch)
	}
	return rows, batches, rr.Err()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
