package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
)

// The HTTP adapter invokes the existing engine in-process. It neither pools
// calculation buffers nor changes their lifetime, workers, or cache policy.
func attributionHandler(base config) http.Handler {
	var active, completed, failed atomic.Int64
	mux := http.NewServeMux()
	send := func(w http.ResponseWriter, status int, value any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(value)
	}
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		send(w, 200, map[string]any{"pid": os.Getpid(), "active": active.Load(), "completed": completed.Load(), "failed": failed.Load(), "heap_alloc_bytes": m.HeapAlloc, "heap_inuse_bytes": m.HeapInuse, "heap_sys_bytes": m.HeapSys, "gc_cycles": m.NumGC, "workers_per_request": base.threads})
	})
	mux.HandleFunc("POST /attribution", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			StartDate string `json:"start_date"`
			EndDate   string `json:"end_date"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		err := decoder.Decode(&req)
		if err == nil {
			var extra any
			if decoder.Decode(&extra) != io.EOF {
				err = errors.New("expected one JSON object")
			}
		}
		start, e1 := time.Parse(time.DateOnly, req.StartDate)
		end, e2 := time.Parse(time.DateOnly, req.EndDate)
		if err != nil || e1 != nil || e2 != nil || end.Before(start) {
			send(w, 400, map[string]string{"error": "provide start_date and end_date as YYYY-MM-DD in ascending order"})
			return
		}
		out, err := os.MkdirTemp(base.output, "request-")
		if err != nil {
			send(w, 500, map[string]string{"error": err.Error()})
			return
		}
		c := base
		c.scenario, c.period, c.subset, c.maxDays = "six-input", "day", "", 0
		c.startDate, c.endDate = req.StartDate, req.EndDate
		c.output, c.metrics = out, filepath.Join(out, "metrics.csv")
		active.Add(1)
		defer active.Add(-1)
		began := time.Now()
		if err := runSixInput(c); err != nil {
			failed.Add(1)
			send(w, 422, map[string]string{"error": err.Error(), "output_dir": out})
			return
		}
		f, err := os.Open(c.metrics)
		if err != nil {
			failed.Add(1)
			send(w, 500, map[string]string{"error": err.Error()})
			return
		}
		rows, err := csv.NewReader(f).ReadAll()
		f.Close()
		if err != nil || len(rows) != 2 {
			failed.Add(1)
			send(w, 500, map[string]string{"error": "cannot read calculation metrics"})
			return
		}
		metrics := map[string]string{}
		for i, key := range rows[0] {
			metrics[key] = rows[1][i]
		}
		completed.Add(1)
		send(w, 200, map[string]any{"elapsed_seconds": time.Since(began).Seconds(), "output_dir": out, "report": filepath.Join(out, "country-portfolio_PF.parquet"), "metrics": metrics})
	})
	return mux
}

func serveAttribution(address string, c config) error {
	if err := os.MkdirAll(c.output, 0755); err != nil {
		return err
	}
	fmt.Printf("six-input API listening on %s; %d workers/request; inputs %s\n", address, c.threads, c.data)
	// Calculations have their existing finite date window, without an HTTP write
	// timeout or forced cancellation that would change measured engine behavior.
	server := &http.Server{Addr: address, Handler: attributionHandler(c), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	return server.ListenAndServe()
}
