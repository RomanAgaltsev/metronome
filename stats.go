package metronome

import (
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	bytesPerCount      = 8 // one int64 per histogram bucket count
	histogramsPerStats = 2 // raw and corrected
)

// statsID orders lock acquisition in Merge. Construction order is a total
// order over every Stats in the process, which is all Merge needs and is
// cheaper to reason about than comparing pointer addresses through unsafe.
var statsID atomic.Uint64

// Stats aggregates Results into percentile Snapshots. Safe for concurrent Record.
type Stats struct {
	mu               sync.Mutex
	lat              *latencyHist
	count            int64
	errors           int64
	saturated        int64
	clamped          int64
	first            time.Time
	last             time.Time
	maxLat           time.Duration // largest raw Latency
	maxLag           time.Duration // largest Start-Scheduled, floored at zero
	bytes            int64
	codes            map[string]int64
	correctedCount   int64
	correctedClamped int64

	id uint64 // construction order; see statsID
}

// NewStats returns Stats recording latencies from 1µs to 60s with 3 significant
// digits — a sensible default for HTTP and gRPC. Use NewStatsRange when your
// work is slower or faster than that.
func NewStats() *Stats { return NewStatsRange(time.Microsecond, time.Minute, 3) }

// NewStatsRange returns Stats recording latencies in [lo, hi] with sigfigs
// significant digits. Latencies outside the range are clamped to the nearest
// bound and counted in Snapshot.Clamped rather than dropped; Snapshot.Max always
// reports the true maximum.
//
// It panics if lo <= 0, hi <= lo, or sigfigs is outside [1, 5] — programmer
// errors, and there is no error path on a constructor two consumers call at
// startup.
func NewStatsRange(lo, hi time.Duration, sigfigs int) *Stats {
	if lo <= 0 {
		panic("metronome: NewStatsRange lo must be > 0")
	}
	if hi <= lo {
		panic("metronome: NewStatsRange hi must be > lo")
	}
	if sigfigs < 1 || sigfigs > 5 {
		panic("metronome: NewStatsRange sigfigs must be in [1, 5]")
	}
	return &Stats{
		lat:   newLatencyHist(lo, hi, sigfigs, false),
		codes: make(map[string]int64),
		id:    statsID.Add(1),
	}
}

// clampMicros converts d to whole microseconds inside the histogram's range,
// reporting whether it had to clamp. Caller holds s.mu.
func (s *Stats) clampMicros(d time.Duration) (int64, bool) { return s.lat.clamp(d) }

// Record adds one Result to the aggregate. It is safe to call concurrently.
func (s *Stats) Record(r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	if !r.Success() {
		s.errors++
		if errors.Is(r.Err, ErrSaturated) {
			s.saturated++
		}
	}
	s.bytes += r.Bytes
	if r.Code != "" {
		s.codes[r.Code]++
	}

	// Failed Results' latencies are recorded too (the k6/vegeta convention):
	// timeouts and slow errors are part of the latency story.
	v, clamped := s.clampMicros(r.Latency)
	if clamped {
		s.clamped++
	}
	s.lat.recordRaw(v)

	// Coordinated-omission correction: a unit that started late was, from the
	// schedule's point of view, already in flight while it waited. Results
	// without a Scheduled stamp (not produced by a Driver) are simply not
	// represented in the corrected percentiles.
	if !r.Scheduled.IsZero() {
		queued := max(r.Start.Sub(r.Scheduled),
			// ran early: never correct downward
			0)
		if queued > s.maxLag {
			s.maxLag = queued
		}
		// Counted separately from s.clamped: this says the *corrected*
		// percentiles hit a bound, which a raw latency well inside the range can
		// still cause once the queueing delay is added.
		cv, clamped := s.clampMicros(r.Latency + queued)
		if clamped {
			s.correctedClamped++
		}
		s.lat.recordCorrected(cv)
		s.correctedCount++
	}

	if r.Latency > s.maxLat {
		s.maxLat = r.Latency
	}
	if s.first.IsZero() || r.Start.Before(s.first) {
		s.first = r.Start
	}
	if r.Start.After(s.last) {
		s.last = r.Start
	}
}

// Snapshot returns the cumulative aggregate so far. Safe to call concurrently
// with Record.
func (s *Stats) Snapshot() Snapshot { return s.snapshot(0) }

// snapshot builds a Snapshot over a known or an inferred duration.
//
// A positive window means the caller knows how much wall time these Results
// cover — the rolling-window case — so RPS is count/window. That estimator can
// report zero, which is what makes a stall visible.
//
// A zero window means the duration is unknown and must be inferred from the
// Result timestamps themselves. N Result.Start timestamps bound N-1 intervals,
// so the unbiased estimate is (N-1)/span; using N/span reports r*N/(N-1) — 10%
// high at N=11 and 100% high at N=2. A single Result bounds no interval and
// reports 0. This estimator cannot represent a stall: no new Results means no
// new span, and the ratio holds at the last healthy rate.
//
// Throughput is RPS x the mean bytes per Result either way, so it is always the
// same kind of estimate as RPS. Bytes/span would spend all N samples' bytes over
// N-1 intervals and report the N/(N-1) bias RPS exists to avoid — 2x at N=2.
func (s *Stats) snapshot(window time.Duration) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	rps := 0.0
	if window > 0 {
		rps = float64(s.count) / window.Seconds()
	} else if span := s.last.Sub(s.first).Seconds(); s.count > 1 && span > 0 {
		rps = float64(s.count-1) / span
	}

	throughput := 0.0
	if s.count > 0 {
		throughput = rps * float64(s.bytes) / float64(s.count)
	}

	errRate := 0.0
	if s.count > 0 {
		errRate = float64(s.errors) / float64(s.count)
	}

	// A percentile is the one question a sparse store cannot answer as HDR
	// would, so reading one promotes it. Nothing on the RollingStats path gets
	// here: Window merges the buckets into a dense scratch and snapshots that.
	rawHist, corrHist := s.lat.hists()

	us := func(p float64) time.Duration {
		return time.Duration(rawHist.ValueAtQuantile(p)) * time.Microsecond
	}
	corrected := func(p float64) time.Duration {
		if s.correctedCount == 0 {
			return 0
		}
		return time.Duration(corrHist.ValueAtQuantile(p)) * time.Microsecond
	}

	return Snapshot{
		Window:           window,
		Count:            s.count,
		Errors:           s.errors,
		RPS:              rps,
		ErrorRate:        errRate,
		P50:              us(50),
		P95:              us(95),
		P99:              us(99),
		Max:              s.maxLat,
		MaxScheduleLag:   s.maxLag,
		Clamped:          s.clamped,
		CorrectedClamped: s.correctedClamped,
		Saturated:        s.saturated,
		Bytes:            s.bytes,
		Throughput:       throughput,
		Codes:            maps.Clone(s.codes),
		CorrectedP50:     corrected(50),
		CorrectedP95:     corrected(95),
		CorrectedP99:     corrected(99),
		CorrectedCount:   s.correctedCount,
	}
}

// reset returns s to its just-constructed state so a rolling bucket can be
// recycled. The histograms keep their allocated counts arrays — only the
// recorded values go — which is the whole reason a ring is affordable.
func (s *Stats) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lat.reset()
	s.count, s.errors, s.saturated, s.clamped = 0, 0, 0, 0
	s.correctedCount, s.correctedClamped = 0, 0
	s.bytes = 0
	s.maxLat, s.maxLag = 0, 0
	s.first, s.last = time.Time{}, time.Time{}
	clear(s.codes)
}

// merge folds src into s.
//
// The caller must hold exclusive use of s; src is locked for the read. That
// asymmetry is deliberate and sufficient for RollingStats, which only ever
// merges buckets into its own private scratch Stats. Merge is the exported
// form and takes both locks.
func (s *Stats) merge(src *Stats) {
	src.mu.Lock()
	defer src.mu.Unlock()
	s.mergeLocked(src)
}

// mergeLocked folds src into s. The caller holds exclusive use of s and holds
// src.mu.
func (s *Stats) mergeLocked(src *Stats) {
	// Both callers guarantee identical ranges — RollingStats by construction,
	// Merge by an explicit check — so nothing is ever dropped for falling
	// outside the destination's. A sparse src folds in as value-count pairs; a
	// dense one as a histogram merge. The destination is always dense.
	src.lat.mergeInto(s.lat)

	s.count += src.count
	s.errors += src.errors
	s.saturated += src.saturated
	s.clamped += src.clamped
	s.correctedCount += src.correctedCount
	s.correctedClamped += src.correctedClamped
	s.bytes += src.bytes

	s.maxLat = max(s.maxLat, src.maxLat)
	s.maxLag = max(s.maxLag, src.maxLag)

	for code, n := range src.codes {
		s.codes[code] += n
	}
	if !src.first.IsZero() && (s.first.IsZero() || src.first.Before(s.first)) {
		s.first = src.first
	}
	if src.last.After(s.last) {
		s.last = src.last
	}
}

// Bytes reports the memory this Stats holds for its latency distributions: two
// count arrays of int64. The surrounding struct, the codes map and the recorded
// Results are not included — the count arrays dominate and are the term that
// scales with the range and significant figures.
//
// Every Stats a caller can construct is dense, so this is fixed at construction
// and needs no lock. The sparse form exists only inside a RollingStats ring,
// where RollingStats.Bytes takes the lock and totals the buckets.
func (s *Stats) Bytes() int64 { return s.lat.bytes() }

// sameRange reports whether s and src record over the same range with the same
// significant figures. It reads only values fixed at construction, so it needs
// no lock — and it reads them off the store's own fields rather than off a
// histogram, which a sparse store does not have until it promotes.
func (s *Stats) sameRange(src *Stats) bool {
	return s.lat.loMicros == src.lat.loMicros &&
		s.lat.hiMicros == src.lat.hiMicros &&
		s.lat.sigfigs == src.lat.sigfigs
}

// Merge folds src into s, so that s reports what it would have reported had it
// recorded every Result src did. Percentiles cannot be combined after the fact,
// so this merges the underlying histograms rather than the Snapshots.
//
// Use it to roll a LabeledStats breakdown up into an ad-hoc subtotal, to
// combine per-worker aggregates recorded in parallel, or to combine separate
// runs.
//
// Both Stats must have been built with the same range and significant figures;
// Merge panics otherwise, and panics if src is s. Range equality is a property
// of construction, so a mismatch fires on the first call rather than later and
// only for some values — it is a programmer error, and it is reported as one.
//
// It is safe to call concurrently with Record on either Stats, and safe to call
// in both directions from different goroutines.
func (s *Stats) Merge(src *Stats) {
	if s == src {
		panic("metronome: Stats.Merge cannot merge a Stats into itself")
	}
	if !s.sameRange(src) {
		panic("metronome: Stats.Merge requires both Stats to have the same range and sigfigs")
	}

	// Lock in construction order so that a.Merge(b) racing b.Merge(a) cannot
	// deadlock on taking the two mutexes in opposite orders.
	first, second := s, src
	if src.id < s.id {
		first, second = src, s
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()

	s.mergeLocked(src)
}

// sparseBytesPerEntry is the amortised cost of one map[int64]int64 entry: an
// 8-byte key, an 8-byte value, and the runtime's per-bucket overhead and load
// factor. It sets where sparse stops being cheaper than the dense array, so it
// is deliberately conservative — over-estimating promotes slightly early, which
// costs memory we were going to spend anyway.
const sparseBytesPerEntry = 50

// latencyHist holds a Stats' two latency distributions in one of two modes.
//
// Dense is the HDR pair and is what every directly-constructed Stats uses.
// Sparse is a value-to-count map per distribution, and exists because a
// RollingStats ring bucket is sized by its configuration rather than by what it
// holds: at 1,000 buckets a bucket may carry ten Results in a 136 KiB array.
//
// A sparse store supports what a ring bucket needs — record, merge into a dense
// target, reset and bytes — and promotes itself if anything asks it a question
// it cannot answer sparsely. Nothing on the ring path does: Window merges the
// live buckets into scratch and reads the snapshot off scratch.
//
// Every value it holds is whole microseconds, already clamped into
// [loMicros, hiMicros]. That is the unit the HDR pair is built in, so the two
// modes index the same distribution and a promotion cannot shift a percentile.
type latencyHist struct {
	loMicros, hiMicros int64
	sigfigs            int
	countsLen          int64

	// dense mode; nil while sparse
	hist      *hdr.Histogram
	corrected *hdr.Histogram

	// sparse mode; nil once promoted
	sparseRaw  map[int64]int64
	sparseCorr map[int64]int64

	// promoteAt is the distinct-value count per map at which sparse stops being
	// cheaper. Computed from the configuration so an unusual range or Sigfigs
	// gets its own break-even rather than one tuned for the default.
	promoteAt int
}

// newLatencyHist builds a store over [lo, hi] at sigfigs significant digits.
//
// The microsecond bounds are derived exactly as NewStatsRange derives them for
// hdr.New, including New's own flooring of a sub-microsecond lo at 1, so that
// LowestTrackableValue and HighestTrackableValue agree with loMicros and
// hiMicros in either mode. Clamping parity depends on that and nothing else.
func newLatencyHist(lo, hi time.Duration, sigfigs int, sparse bool) *latencyHist {
	counts := histogramCounts(lo, hi, sigfigs)
	lh := &latencyHist{
		loMicros:  max(int64(lo/time.Microsecond), 1),
		hiMicros:  int64(hi / time.Microsecond),
		sigfigs:   sigfigs,
		countsLen: counts,
		promoteAt: int(counts * bytesPerCount / sparseBytesPerEntry),
	}
	if sparse {
		lh.sparseRaw = make(map[int64]int64)
		lh.sparseCorr = make(map[int64]int64)
		return lh
	}
	lh.allocDense()
	return lh
}

func (lh *latencyHist) allocDense() {
	lh.hist = hdr.New(lh.loMicros, lh.hiMicros, lh.sigfigs)
	lh.corrected = hdr.New(lh.loMicros, lh.hiMicros, lh.sigfigs)
}

func (lh *latencyHist) isSparse() bool { return lh.sparseRaw != nil }

// promote allocates the dense pair, replays the sparse pairs into it, and drops
// the maps. It runs at most once per latencyHist: a store dense enough to lose
// the advantage becomes dense and stays that way.
func (lh *latencyHist) promote() {
	raw, corr := lh.sparseRaw, lh.sparseCorr
	lh.sparseRaw, lh.sparseCorr = nil, nil
	lh.allocDense()
	for v, n := range raw {
		//nolint:gosec // a sparse map only ever holds values clamp already bounded
		_ = lh.hist.RecordValues(v, n)
	}
	for v, n := range corr {
		//nolint:gosec // a sparse map only ever holds values clamp already bounded
		_ = lh.corrected.RecordValues(v, n)
	}
}

// clamp pins d into the histogram's range in whole microseconds, reporting
// whether it had to. It is the sparse path's clamping as much as the dense
// one's: a consumer reads Snapshot.Clamped to know its percentiles understate
// reality, so the two modes must agree on it exactly.
func (lh *latencyHist) clamp(d time.Duration) (int64, bool) {
	v := int64(d / time.Microsecond)
	switch {
	case v < lh.loMicros:
		return lh.loMicros, true
	case v > lh.hiMicros:
		return lh.hiMicros, true
	default:
		return v, false
	}
}

// recordRaw adds one already-clamped microsecond value to the raw
// distribution. Results with no Scheduled stamp reach only this one: they
// cannot be placed against a schedule, so they are absent from the corrected
// distribution rather than counted in it at their raw latency.
func (lh *latencyHist) recordRaw(v int64) {
	if lh.isSparse() {
		lh.sparseRaw[v]++
		lh.maybePromote()
		return
	}
	//nolint:gosec // the caller clamped v into the histogram bounds
	_ = lh.hist.RecordValue(v)
}

// recordCorrected adds one already-clamped microsecond value to the corrected
// distribution.
func (lh *latencyHist) recordCorrected(v int64) {
	if lh.isSparse() {
		lh.sparseCorr[v]++
		lh.maybePromote()
		return
	}
	//nolint:gosec // the caller clamped v into the histogram bounds
	_ = lh.corrected.RecordValue(v)
}

// record adds one observation to each distribution, for a Result carrying both.
// Both values are already clamped microseconds.
func (lh *latencyHist) record(raw, corrected int64) {
	lh.recordRaw(raw)
	lh.recordCorrected(corrected)
}

// maybePromote goes dense once either map holds more distinct values than the
// dense array would cost. Checked per record rather than per merge, because
// recording is the only thing that grows a map.
func (lh *latencyHist) maybePromote() {
	if len(lh.sparseRaw) > lh.promoteAt || len(lh.sparseCorr) > lh.promoteAt {
		lh.promote()
	}
}

// hists returns the dense pair, promoting first if the store is still sparse.
//
// A sparse store cannot answer a percentile with the values HDR would give —
// HDR quantises to its bucket midpoints, and reproducing that outside it would
// be a second implementation of the thing that must not drift. So a question
// only the dense form can answer is the one event besides a full map that
// promotes a store. Nothing on the RollingStats path asks: buckets are merged
// into a dense scratch and the snapshot is read off scratch. A caller who does
// ask — Snapshot on a bucket Stats — pays for the histogram once and gets an
// answer identical to a dense store's.
func (lh *latencyHist) hists() (*hdr.Histogram, *hdr.Histogram) {
	if lh.isSparse() {
		lh.promote()
	}
	return lh.hist, lh.corrected
}

// mergeInto folds this store into dst, which must be dense. scratch is built by
// NewStatsRange and is the only merge target, so there is no sparse-into-sparse
// case and none is written.
func (lh *latencyHist) mergeInto(dst *latencyHist) {
	if dst.isSparse() {
		panic("metronome: latencyHist merge target must be dense")
	}
	if lh.isSparse() {
		for v, n := range lh.sparseRaw {
			//nolint:gosec // src and dst share a range, so a clamped key is in bounds
			_ = dst.hist.RecordValues(v, n)
		}
		for v, n := range lh.sparseCorr {
			//nolint:gosec // src and dst share a range, so a clamped key is in bounds
			_ = dst.corrected.RecordValues(v, n)
		}
		return
	}
	// Merge reports values it had to drop for falling outside the destination's
	// range. Both stores are built over the same range, so this is always zero.
	_ = dst.hist.Merge(lh.hist)
	_ = dst.corrected.Merge(lh.corrected)
}

// reset empties the store and keeps its mode. A promoted store stays dense,
// matching what Stats.reset already did by calling hist.Reset rather than
// reallocating — and meaning a burst's cost converges on the old behaviour
// rather than thrashing at the break-even boundary.
func (lh *latencyHist) reset() {
	if lh.isSparse() {
		clear(lh.sparseRaw)
		clear(lh.sparseCorr)
		return
	}
	lh.hist.Reset()
	lh.corrected.Reset()
}

// bytes reports what this store currently holds.
func (lh *latencyHist) bytes() int64 {
	if lh.isSparse() {
		return int64(len(lh.sparseRaw)+len(lh.sparseCorr)) * sparseBytesPerEntry
	}
	return lh.countsLen * bytesPerCount * histogramsPerStats
}

// newBucketStats returns a Stats whose latency store begins sparse. Only the
// RollingStats ring uses it: a ring bucket is recorded into, reset, and merged
// into a dense scratch, and nothing reads its percentiles.
func newBucketStats(lo, hi time.Duration, sigfigs int) *Stats {
	s := NewStatsRange(lo, hi, sigfigs)
	s.lat = newLatencyHist(lo, hi, sigfigs, true)
	return s
}
