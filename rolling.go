package metronome

import (
	"math"
	"sync"
	"time"
)

// DefaultWindow is the trailing window NewRollingStats uses when Rolling.Window
// is zero.
const DefaultWindow = 10 * time.Second

// DefaultBuckets is the ring size NewRollingStats uses when Rolling.Buckets is
// zero.
const DefaultBuckets = 10

// Rolling configures a RollingStats. The zero value is valid and gives a 10s
// window in 10 buckets on the wall clock over the NewStats histogram range.
type Rolling struct {
	// Window is the trailing duration Window() reports over. Zero selects
	// DefaultWindow.
	//
	// It is truncated to a whole multiple of Buckets: 10s over 3 buckets gives
	// 3.333s buckets spanning 9.999s. Snapshot.Window always reports what the
	// ring actually covers, so the difference is visible rather than assumed.
	Window time.Duration

	// Buckets is the ring size — the resolution at which the window retires old
	// data. Zero selects DefaultBuckets. More buckets means a smoother window.
	//
	// It is also a memory multiplier, which is the part that surprises people.
	// Each bucket holds a raw and a corrected HDR histogram, and a RollingStats
	// allocates Buckets+2 of them: the ring, the lifetime aggregate and a scratch
	// merge target. At the default range that is ~272 KiB per bucket, so the
	// default Rolling is ~3.2 MiB and Buckets: 1000 is ~270 MiB. Widening Lo/Hi
	// or raising Sigfigs multiplies again — 5 significant figures over a
	// microsecond-to-hour range is ~16 MiB per histogram.
	//
	// Call Bytes to price a configuration before building it. Narrow the
	// histogram range when you want a large ring: 1,000 buckets over
	// 1ms-1s at 1 significant figure is ~2 MiB.
	Buckets int

	// Clock supplies the time that rotates the ring. Nil selects the wall clock;
	// inject a ManualClock to make window tests exact. Wire it to the same clock
	// as Driver.Clock when both are in play.
	Clock Clock

	// Lo, Hi and Sigfigs are the histogram range every bucket, the lifetime
	// aggregate and the scratch target are built with, exactly as in
	// NewStatsRange. Zero values select that function's defaults: 1µs, 60s and
	// 3 significant digits.
	Lo, Hi  time.Duration
	Sigfigs int
}

// rollingConfig is the resolved, validated form of a Rolling.
type rollingConfig struct {
	window  time.Duration
	buckets int
	lo, hi  time.Duration
	sigfigs int
	clock   Clock
}

// windowAndBuckets resolves the ring geometry, panicking on input no defaulting
// can rescue.
func (cfg Rolling) windowAndBuckets() (time.Duration, int) {
	window, buckets := cfg.Window, cfg.Buckets
	if window < 0 {
		panic("metronome: Rolling.Window must not be negative")
	}
	if window == 0 {
		window = DefaultWindow
	}
	if buckets < 0 {
		panic("metronome: Rolling.Buckets must not be negative")
	}
	if buckets == 0 {
		buckets = DefaultBuckets
	}
	// Below this, Window/Buckets floors to a zero interval, which makes rotation
	// a division by zero and an unbounded loop.
	if window < time.Duration(buckets) {
		panic("metronome: Rolling.Window must be at least Rolling.Buckets nanoseconds, so each bucket spans at least 1ns")
	}
	return window, buckets
}

// histRange resolves the histogram parameters, panicking on input no defaulting
// can rescue.
//
// It repeats the three rules NewStatsRange enforces rather than leaving them to
// it. NewRollingStats reaches NewStatsRange and would panic either way, but
// Bytes never constructs anything, so without these it would price a
// configuration that cannot be built — Rolling{Sigfigs: 6} would report 1.3 GiB
// for a config whose constructor panics. The panics name the Rolling field the
// caller actually typed.
func (cfg Rolling) histRange() (lo, hi time.Duration, sigfigs int) {
	lo, hi, sigfigs = cfg.Lo, cfg.Hi, cfg.Sigfigs
	if lo == 0 {
		lo = time.Microsecond
	}
	if hi == 0 {
		hi = time.Minute
	}
	if sigfigs == 0 {
		sigfigs = 3
	}

	if lo <= 0 {
		panic("metronome: Rolling.Lo must be > 0")
	}
	if hi <= lo {
		panic("metronome: Rolling.Hi must be > Rolling.Lo")
	}
	if sigfigs < 1 || sigfigs > 5 {
		panic("metronome: Rolling.Sigfigs must be in [1, 5]")
	}
	return lo, hi, sigfigs
}

// config validates the Rolling and resolves its defaults, mirroring
// Driver.config. It panics on programmer error: there is no error path on a
// constructor called once at startup.
func (cfg Rolling) config() rollingConfig {
	window, buckets := cfg.windowAndBuckets()
	lo, hi, sigfigs := cfg.histRange()

	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}

	return rollingConfig{
		window:  window,
		buckets: buckets,
		lo:      lo,
		hi:      hi,
		sigfigs: sigfigs,
		clock:   clock,
	}
}

// RollingStats aggregates Results into two views at once: the cumulative
// lifetime aggregate Stats gives, and a trailing window over the recent past.
//
// Use Snapshot for an end-of-run report and Window for anything watching a run
// while it happens. Every number Stats produces is cumulative, so one early
// stall pins Snapshot.MaxScheduleLag at its worst value for the rest of the run
// and a target that stops answering entirely never changes Snapshot.RPS — there
// are no new Results to move it. Window has neither property.
//
// Safe for concurrent Record.
type RollingStats struct {
	mu       sync.Mutex
	clock    Clock
	interval time.Duration // one bucket's span: window / buckets
	life     *Stats        // the cumulative aggregate
	ring     []*Stats      // the trailing buckets
	scratch  *Stats        // reusable merge target, so Window allocates no histograms
	idx      int           // ring index of the bucket now being filled
	live     int           // how many ring buckets hold data, for coverage
	origin   time.Time     // clock.Now() at construction: the bucket grid's anchor
	curStart time.Time     // when the current bucket opened
}

// NewRollingStats returns a RollingStats configured by cfg. The zero Rolling is
// valid.
//
// It panics on configuration no defaulting can rescue — a negative field, a
// histogram range NewStatsRange rejects, or a Window narrower than one
// nanosecond per bucket. Those are programmer errors, and there is no error path
// on a constructor two consumers call at startup.
func NewRollingStats(cfg Rolling) *RollingStats {
	c := cfg.config()
	now := c.clock.Now()

	rs := &RollingStats{
		clock:    c.clock,
		interval: c.window / time.Duration(c.buckets),
		life:     NewStatsRange(c.lo, c.hi, c.sigfigs),
		ring:     make([]*Stats, c.buckets),
		scratch:  NewStatsRange(c.lo, c.hi, c.sigfigs),
		live:     1,
		origin:   now,
		curStart: now,
	}
	for i := range rs.ring {
		rs.ring[i] = NewStatsRange(c.lo, c.hi, c.sigfigs)
	}
	return rs
}

// rotate advances the ring until curStart is the start of the bucket containing
// now. The caller holds rs.mu.
func (rs *RollingStats) rotate(now time.Time) {
	gap := now.Sub(rs.curStart)
	if gap < rs.interval {
		// Still inside the current bucket. A clock that went backwards lands here
		// too, which is the right answer: never rotate on negative time.
		return
	}
	n := int(gap / rs.interval)

	if n >= len(rs.ring) {
		// The gap is at least a whole window, so every bucket is stale whatever n
		// is. Clearing the ring in one pass keeps this O(Buckets): an hour-long
		// stall against a one-second bucket would otherwise iterate 3,600 times
		// on the next call.
		for _, b := range rs.ring {
			b.reset()
		}
		rs.idx, rs.live = 0, 1
	} else {
		for range n {
			rs.idx = (rs.idx + 1) % len(rs.ring)
			rs.ring[rs.idx].reset()
		}
		rs.live = min(rs.live+n, len(rs.ring))
	}

	// Advance by whole intervals rather than jumping to now, so bucket boundaries
	// stay anchored to origin instead of drifting by the remainder on every
	// rotation. The pacer's nominal schedule holds its grid the same way, for the
	// same reason.
	rs.curStart = rs.curStart.Add(time.Duration(n) * rs.interval)
}

// Record adds one Result to both the lifetime aggregate and the current bucket.
// It is safe to call concurrently.
//
// Which bucket a Result lands in is decided by the clock at the moment Record is
// called, not by Result.Start. A caller that buffers Results and drains them in
// batches therefore attributes a batch to the window it drained in rather than
// the window the work happened in. Record as you receive, which is what ranging
// over the Driver's channel already does.
func (rs *RollingStats) Record(r Result) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.rotate(rs.clock.Now())
	rs.life.Record(r)
	rs.ring[rs.idx].Record(r)
}

// Snapshot returns the cumulative aggregate over the whole run, with exactly the
// meaning Stats.Snapshot has: RPS inferred from the Result timestamps and
// Snapshot.Window zero. Use Window for the trailing view.
func (rs *RollingStats) Snapshot() Snapshot {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	return rs.life.snapshot(0)
}

// covered is how much wall time the live ring actually spans: the completed
// buckets behind the current one, plus however far into the current one the
// clock has moved. The caller holds rs.mu.
//
// It never exceeds the age of the run, so a caller reading during the first
// window gets the truth rather than a window padded with time that never
// happened. That holds by construction rather than by clamping: curStart only
// ever moves from origin by whole intervals, and the same rotation that moves it
// raises live by the same count, so (live-1)*interval is at most curStart-origin.
//
// It is floored at one interval, because RPS divides by it and a RollingStats
// read in the same instant it was built has covered nothing.
func (rs *RollingStats) covered(now time.Time) time.Duration {
	d := now.Sub(rs.curStart) + time.Duration(rs.live-1)*rs.interval
	if d <= 0 {
		d = rs.interval
	}
	return d
}

// Window returns the aggregate over the trailing Rolling.Window — or over
// however much of it the run has covered so far, which Snapshot.Window reports.
//
// It rotates the ring before reading, so a run that has stopped producing
// Results drains to zero instead of freezing at its last healthy numbers. That
// is the difference from Snapshot, and the reason to call this one from a
// control loop: Snapshot.MaxScheduleLag is a lifetime maximum that cannot tell
// "the generator is behind now" from "the generator was behind once".
//
// Safe to call concurrently with Record. The returned Snapshot is a value and
// its Codes map is a copy, so it is the caller's to keep.
func (rs *RollingStats) Window() Snapshot {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	now := rs.clock.Now()
	rs.rotate(now)

	rs.scratch.reset()
	for i := range rs.live {
		// Walk back from the current bucket over the live ones only. Buckets
		// beyond live have never been reached, so the cost tracks how much of
		// the window the run has covered rather than Buckets; merge skips the
		// live-but-empty ones, so a stall does not pay for them either.
		idx := rs.idx - i
		if idx < 0 {
			idx += len(rs.ring)
		}
		rs.scratch.merge(rs.ring[idx])
	}

	return rs.scratch.snapshot(rs.covered(now))
}

// histogramCounts is the length of the counts array hdr.New allocates for a
// [lo, hi] range at sigfigs significant digits — the same derivation
// NewStatsRange triggers, reproduced so a configuration can be priced without
// allocating one.
//
// Reproducing it is a coupling with the hdrhistogram dependency: if a future
// version changes its sizing, this drifts silently. TestRollingBytes asserts
// exact byte counts so the drift breaks a test instead of a memory budget.
func histogramCounts(lo, hi time.Duration, sigfigs int) int64 {
	lowest := int64(lo / time.Microsecond)
	if lowest < 1 {
		lowest = 1
	}
	highest := int64(hi / time.Microsecond)

	// Unit resolution must hold to 2x10^sigfigs, rounded up to a power of two.
	magnitude := int(math.Ceil(math.Log2(2 * math.Pow10(sigfigs))))
	if magnitude < 1 {
		magnitude = 1
	}
	subBucketCount := int64(1) << uint(magnitude)

	unitMagnitude := int(math.Floor(math.Log2(float64(lowest))))
	if unitMagnitude < 0 {
		unitMagnitude = 0
	}

	// How many power-of-two buckets it takes to reach highest.
	smallestUntrackable := subBucketCount << uint(unitMagnitude)
	buckets := int64(1)
	for smallestUntrackable < highest {
		smallestUntrackable <<= 1
		buckets++
	}

	return (buckets + 1) * (subBucketCount / 2)
}

// Bytes reports the memory NewRollingStats(cfg) would allocate for its
// histograms, so a configuration can be priced before it is built:
//
//	if cfg.Bytes() > budget {
//	    cfg.Buckets /= 2
//	}
//
// Buckets is a memory multiplier as much as a resolution knob — Buckets+2
// Stats, each holding a raw and a corrected histogram — and this is the number
// that says so. It counts the histogram count arrays, which dominate; the
// surrounding structs, the codes maps and the recorded Results are not
// included.
//
// It panics on exactly the configurations NewRollingStats panics on, so pricing
// an unbuildable config reports the problem rather than a number for it.
func (cfg Rolling) Bytes() int64 {
	c := cfg.config()

	const (
		bytesPerCount      = 8 // int64
		histogramsPerStats = 2 // raw and corrected
		extraStats         = 2 // the lifetime aggregate and the scratch merge target
	)
	counts := histogramCounts(c.lo, c.hi, c.sigfigs)

	return int64(c.buckets+extraStats) * histogramsPerStats * counts * bytesPerCount
}
