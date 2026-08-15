package metronome

import "time"

// Result is the outcome of one unit of work.
type Result struct {
	Start   time.Time
	Latency time.Duration
	Err     error
	Code    string // status/gRPC code, caller-supplied
	Bytes   int64
	Labels  map[string]string // e.g. {"endpoint": "get_user"} for breakdown
}

// Success reports whether the unit of work completed without error.
func (r Result) Success() bool { return r.Err == nil }

// Snapshot is an aggregated view of many Results over a window.
type Snapshot struct {
	Count         int64
	Errors        int64
	RPS           float64
	ErrorRate     float64
	P50, P95, P99 time.Duration
	Max           time.Duration
}
