package metronome

import (
	"fmt"
	"time"
)

// Result is the outcome of one unit of work.
type Result struct {
	Start   time.Time
	Latency time.Duration
	Err     error
	Code    string // status/gRPC code, caller-supplied
	Bytes   int64

	// Labels is caller-side metadata carried alongside the Result, e.g.
	// {"endpoint": "get_user"}. Stats does not aggregate it: a per-label
	// breakdown helper is roadmapped, and until it exists a caller who needs
	// one keys their own map off these Results. Leave it nil if unused — it is
	// an allocation per request.
	Labels map[string]string
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

	// Clamped counts Results whose Latency fell outside the histogram's range
	// and was clamped to a bound. Non-zero means the percentiles above
	// understate reality at one end — widen the range with NewStatsRange. Max
	// always reports the true maximum regardless.
	Clamped int64

	// Bytes is the sum of Result.Bytes.
	Bytes int64

	// Throughput is Bytes/sec over the same span RPS is measured across.
	Throughput float64

	// Codes counts Results by Result.Code. Results with an empty Code are
	// counted in Count but not here. The map is a copy owned by the caller.
	Codes map[string]int64
}

// PanicError reports a panic raised inside a Runner. The Driver recovers it so
// that one bad unit of work cannot abort a whole load run; the panic is
// delivered as a failed Result instead, and Stack holds the stack captured at
// the point of recovery.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("metronome: runner panicked: %v", e.Value)
}
