package metronome

import (
	"maps"
	"sync"
)

// DefaultMaxSeries is the number of distinct label values a LabeledStats gives
// their own child recorder before further values land in the overflow series.
const DefaultMaxSeries = 100

// DefaultOverflowLabel names the series that absorbs label values past
// MaxSeries.
const DefaultOverflowLabel = "__overflow__"

// DefaultUnlabeledLabel names the series for Results carrying no usable value
// for the configured key.
const DefaultUnlabeledLabel = "__none__"

// Labeled configures a LabeledStats. Key and New are required.
type Labeled[R Recorder] struct {
	// Key is the Result.Labels key to split on. Required.
	//
	// One key, not a set of them: a series here is a whole aggregate — at
	// least 272 KiB as a *Stats and about 3.2 MiB as a *RollingStats — so
	// keying on combinations would multiply memory by the cardinality of every
	// label a Result happens to carry.
	Key string

	// New constructs one child recorder. Required: there is no expression for
	// the zero value of an arbitrary R that would produce a working recorder,
	// so this cannot be defaulted. NewStats has exactly this signature:
	//
	//	Labeled[*Stats]{Key: "endpoint", New: NewStats}
	//
	// It is called lazily — once per distinct label value, plus once for the
	// total and once for overflow — so a run with four endpoints allocates six
	// children, not MaxSeries of them.
	New func() R

	// MaxSeries caps the number of distinct label values that get their own
	// child. 0 means DefaultMaxSeries.
	//
	// This is a memory bound, not a tuning knob. A label value that turns out
	// to carry a request ID rather than a route name would otherwise allocate a
	// child per request; past the cap those Results land in the Overflow series
	// instead, so a mislabelled run loses reporting detail rather than memory.
	//
	// The Unlabeled series counts against the cap, because it is created by the
	// same lazy path a real value takes. A run at the default that carries any
	// unlabeled traffic therefore breaks down 99 label values, not 100. The
	// Overflow series does not count against it — it is where the cap sends
	// things, so charging the cap for it would be circular.
	MaxSeries int

	// Overflow names the series absorbing label values past MaxSeries.
	// "" means DefaultOverflowLabel. A real label value equal to it shares that
	// series.
	Overflow string

	// Unlabeled names the series for Results whose Labels is nil, lacks Key, or
	// holds an empty string for it. "" means DefaultUnlabeledLabel. Such
	// Results get a named series rather than being dropped, because "which
	// requests were not labelled?" is the question being asked, and dropping
	// them would stop the series reconciling with the total.
	Unlabeled string
}

// labeledConfig is Labeled resolved and validated once, at construction.
type labeledConfig[R Recorder] struct {
	key       string
	newChild  func() R
	maxSeries int
	overflow  string
	unlabeled string
}

// config validates cfg and applies defaults. It panics on programmer error,
// following NewStatsRange and NewRollingStats: there is no error path worth
// threading through a constructor called once at startup.
func (cfg Labeled[R]) config() labeledConfig[R] {
	if cfg.Key == "" {
		panic("metronome: Labeled.Key must not be empty")
	}
	if cfg.New == nil {
		panic("metronome: Labeled.New must not be nil")
	}
	if cfg.MaxSeries < 0 {
		panic("metronome: Labeled.MaxSeries must be >= 0")
	}

	c := labeledConfig[R]{
		key:       cfg.Key,
		newChild:  cfg.New,
		maxSeries: cfg.MaxSeries,
		overflow:  cfg.Overflow,
		unlabeled: cfg.Unlabeled,
	}
	if c.maxSeries == 0 {
		c.maxSeries = DefaultMaxSeries
	}
	if c.overflow == "" {
		c.overflow = DefaultOverflowLabel
	}
	if c.unlabeled == "" {
		c.unlabeled = DefaultUnlabeledLabel
	}
	// Two distinct meanings collapsed into one series is a silent wrong answer,
	// and it is reachable from a two-field struct literal.
	if c.overflow == c.unlabeled {
		panic("metronome: Labeled.Overflow and Labeled.Unlabeled must differ")
	}
	return c
}

// LabeledStats splits a Result stream on one Result.Labels key, accumulating a
// child Recorder per distinct value alongside a total fed by every Result.
//
// Snapshot reports the total, and is exactly what a plain Stats fed the same
// stream would have produced — so existing code holding a *Stats can hold one
// of these instead and see its numbers unchanged, gaining the breakdown rather
// than trading for it.
//
// It is generic over the child so that a breakdown of rolling windows keeps its
// type: LabeledStats[*RollingStats].Series()["get_user"].Window() needs no type
// assertion.
//
// Nesting typechecks — *LabeledStats[R] is itself a Recorder — and is almost
// always a mistake: LabeledStats[*LabeledStats[*Stats]] multiplies the child
// count by both caps, which at the defaults is 100 x 100 x 272 KiB. A second
// dimension usually wants to be a second LabeledStats fed the same stream
// through Multi, not a nested one.
//
// Safe for concurrent Record.
type LabeledStats[R Recorder] struct {
	cfg labeledConfig[R]

	mu     sync.RWMutex
	series map[string]R
	named  int // series created for real label values, excluding overflow

	total R
}

// NewLabeledStats returns a LabeledStats configured by cfg. It panics on the
// programmer errors listed on Labeled.
func NewLabeledStats[R Recorder](cfg Labeled[R]) *LabeledStats[R] {
	c := cfg.config()
	return &LabeledStats[R]{
		cfg:    c,
		series: make(map[string]R),
		total:  c.newChild(),
	}
}

// Len reports how many series exist so far, including the overflow series once
// it has been created. Series are lazy, so this grows as label values are seen.
func (ls *LabeledStats[R]) Len() int {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return len(ls.series)
}

// Bytes reports the histogram memory currently held: the total plus every live
// series. It returns -1 if the child type does not report its own size, which a
// custom Recorder is not required to do — -1 rather than 0, because 0 is a
// plausible-looking number that would read as "free".
//
// Series are lazy, so this is what is held now. The worst case is
// (MaxSeries + 2) x one child: the cap, plus overflow, plus the total.
func (ls *LabeledStats[R]) Bytes() int64 {
	total, ok := any(ls.total).(byteSizer)
	if !ok {
		return -1
	}

	ls.mu.RLock()
	defer ls.mu.RUnlock()

	n := total.Bytes()
	for _, c := range ls.series {
		if b, ok := any(c).(byteSizer); ok {
			n += b.Bytes()
		}
	}
	return n
}

// label picks the series name for r: the configured key's value, or the
// unlabeled sentinel when there is nothing usable there.
func (ls *LabeledStats[R]) label(r Result) string {
	v := r.Labels[ls.cfg.key] // a nil map reads as the zero value
	if v == "" {
		return ls.cfg.unlabeled
	}
	return v
}

// child returns the recorder for series v, creating it on first sight, or the
// overflow series once MaxSeries distinct values already have one.
func (ls *LabeledStats[R]) child(v string) R {
	ls.mu.RLock()
	c, ok := ls.series[v]
	ls.mu.RUnlock()
	if ok {
		return c
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Re-check: another goroutine may have created it between the two locks,
	// and the loser must not discard the winner's child along with whatever has
	// already been recorded into it.
	if c, ok = ls.series[v]; ok {
		return c
	}

	if ls.named >= ls.cfg.maxSeries {
		v = ls.cfg.overflow
		if c, ok = ls.series[v]; ok {
			return c
		}
	} else {
		ls.named++
	}

	c = ls.cfg.newChild()
	ls.series[v] = c
	return c
}

// Record adds r to the total and to exactly one series. It is safe to call
// concurrently.
//
// Every Result lands in the total and in exactly one series, so once recording
// has stopped the sum of the series' Counts equals the total's. That reconciles
// at rest, not at every instant: the two child calls below are made in sequence
// and outside this LabeledStats' lock, so a reader polling a live run can catch
// a Result counted in the total and not yet in its series, or a series that
// exists with a Count of zero. Serialising them would make the invariant hold
// continuously at the cost of funnelling every series through one mutex, which
// is the contention this type is shaped to avoid.
func (ls *LabeledStats[R]) Record(r Result) {
	c := ls.child(ls.label(r))

	// Neither call happens under ls.mu: holding it would serialise every series
	// through one mutex at whatever rate the run is generating, which is the
	// contention Stats' own per-instance mutex exists to avoid. The child
	// pointer is stable once created, so releasing before using it is safe.
	ls.total.Record(r)
	c.Record(r)
}

// Snapshot returns the total: the aggregate over every Result recorded,
// regardless of label. It is exactly what a plain Stats fed the same stream
// would have produced.
func (ls *LabeledStats[R]) Snapshot() Snapshot { return ls.total.Snapshot() }

// Total returns the recorder behind Snapshot, typed. Use it to reach a child
// method the Recorder seam does not carry — most usefully
// LabeledStats[*RollingStats].Total().Window().
func (ls *LabeledStats[R]) Total() R { return ls.total }

// Series returns the per-label recorders, including the overflow and unlabeled
// series once they exist.
//
// The map is a copy the caller owns; the recorders in it are live and shared
// with this LabeledStats. Read them — Snapshot, and Window when the child type
// has one — but do not Record into them, which would count a Result in a series
// without counting it in the total.
func (ls *LabeledStats[R]) Series() map[string]R {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return maps.Clone(ls.series)
}
