package main

import (
	"encoding/json"
	"os"
	"runtime"
	runtimemetrics "runtime/metrics"
	"runtime/pprof"
)

type runtimeSnapshot struct {
	memory             runtime.MemStats
	gcCPU, gcAssistCPU float64
}

func snapshotRuntime() runtimeSnapshot {
	var s runtimeSnapshot
	runtime.ReadMemStats(&s.memory)
	samples := []runtimemetrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/cpu/classes/gc/mark/assist:cpu-seconds"}}
	runtimemetrics.Read(samples)
	s.gcCPU = samples[0].Value.Float64()
	s.gcAssistCPU = samples[1].Value.Float64()
	return s
}

// Optional process-wide diagnostics. No memory snapshots or profiling calls are
// performed on the per-row path. Allocation/GC counters cover the whole process,
// including setup/output; CPU and allocation profiles locate their callers.
func startDiagnostics() (func() error, error) {
	cpuPath, statsPath, allocPath := os.Getenv("ATTRIBUTION_CPU_PROFILE"), os.Getenv("ATTRIBUTION_RUNTIME_STATS"), os.Getenv("ATTRIBUTION_ALLOC_PROFILE")
	if cpuPath == "" && statsPath == "" && allocPath == "" {
		return func() error { return nil }, nil
	}
	before := snapshotRuntime()
	var cpu *os.File
	if cpuPath != "" {
		var err error
		cpu, err = os.Create(cpuPath)
		if err != nil {
			return nil, err
		}
		if err = pprof.StartCPUProfile(cpu); err != nil {
			cpu.Close()
			return nil, err
		}
	}
	return func() error {
		if cpu != nil {
			pprof.StopCPUProfile()
			if err := cpu.Close(); err != nil {
				return err
			}
		}
		after := snapshotRuntime()
		if statsPath != "" {
			result := map[string]any{
				"allocated_bytes":        after.memory.TotalAlloc - before.memory.TotalAlloc,
				"allocated_objects":      after.memory.Mallocs - before.memory.Mallocs,
				"freed_objects":          after.memory.Frees - before.memory.Frees,
				"gc_cycles":              after.memory.NumGC - before.memory.NumGC,
				"gc_pause_wall_seconds":  float64(after.memory.PauseTotalNs-before.memory.PauseTotalNs) / 1e9,
				"gc_cpu_seconds":         after.gcCPU - before.gcCPU,
				"gc_assist_cpu_seconds":  after.gcAssistCPU - before.gcAssistCPU,
				"heap_live_bytes_at_end": after.memory.HeapAlloc,
				"scope":                  "whole process, including setup/output and diagnostic overhead; GC counters overlap phase elapsed times",
			}
			data, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return err
			}
			if err = os.WriteFile(statsPath, append(data, '\n'), 0644); err != nil {
				return err
			}
		}
		if allocPath != "" {
			f, err := os.Create(allocPath)
			if err != nil {
				return err
			}
			// Allocation profiles are sampled by the Go runtime. Do not force a
			// GC, which would change the workload and its measured GC costs.
			err = pprof.Lookup("allocs").WriteTo(f, 0)
			closeErr := f.Close()
			if err != nil {
				return err
			}
			return closeErr
		}
		return nil
	}, nil
}
