package metronome

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// minRPS is the floor the Driver applies to any controller-reported rate. It is
// not zero: a limiter with limit 0 hands out no tokens at all and cannot be
// resumed by a later SetLimit, so "paused" is expressed as one token roughly
// every 10,000 seconds instead.
const minRPS = 0.0001

// sanitizeRate maps a RateController's reported rate onto a limit the limiter
// can honour.
//
// NaN is the dangerous case and the reason this function exists: Go's max
// propagates NaN, so a naive max(rps, minRPS) floor does not catch it and
// rate.Limit(NaN) makes every Wait and Reserve return immediately - the Driver
// stops pacing and floods the target. A control loop that divides by an empty
// PromQL vector produces exactly that. NaN is therefore treated as "paused",
// the safe direction to fail.
//
// +Inf is honoured as rate.Inf: "as fast as possible" is a legitimate request
// (it is what the overhead benchmark asks for) and, unlike NaN, it is explicit.
func sanitizeRate(rps float64) rate.Limit {
	switch {
	case math.IsNaN(rps):
		return rate.Limit(minRPS)
	case math.IsInf(rps, 1):
		return rate.Inf
	case rps < minRPS: // covers zero, negatives and -Inf
		return rate.Limit(minRPS)
	default:
		return rate.Limit(rps)
	}
}

// pacer converts a rate limit into scheduled send times. It exists so that the
// *ideal* send time is knowable — lim.Wait only tells you when it returned, not
// when it should have — and so that the waiting happens on the injected Clock,
// which is what makes pacing tests deterministic.
//
// mu serialises the clock read with the limiter call it feeds. x/time/rate's
// reserveN sets lim.last = t unconditionally, so a stale t — one goroutine read
// the clock, then lost the race for the limiter's own mutex — moves the
// limiter's anchor backwards and the next caller mints tokens for an interval
// that has already elapsed. The bias is one-directional: it drives faster than
// asked, which is the worst direction for a tool whose product is accuracy.
type pacer struct {
	mu    sync.Mutex
	lim   *rate.Limiter
	clock Clock

	// nominal is the send time of the NEXT unit: the run's origin plus one
	// interval for every unit dispatched so far. It advances independently of
	// when next is actually called, which is what makes it an anchor — see
	// next's comment. Zero before the first unit.
	nominal time.Time
}

// maxReservationWait caps how long the pacer will commit to a single
// reservation before re-reading the rate.
//
// x/time/rate grants a reservation at a fixed time and SetLimitAt cannot shorten
// one that already exists. Without this cap, a rate at the minRPS floor — which
// is where a zero, negative or NaN rate lands — hands out a reservation roughly
// 2h46m away, and every worker parks on one. Restoring the rate a moment later
// changes nothing, so "paused" becomes "dead" with no way for the caller to tell
// the difference. Beyond this cap the pacer gives the token back and re-reserves
// instead, which costs one wasted reservation per tick and makes a floored rate
// a pause you can come back from.
//
// It is the Driver's rate-update cadence because that is how often the limit can
// actually change.
const maxReservationWait = rateUpdateInterval

// errRateTooLow signals that the next unit is further away than
// maxReservationWait, so next should wait a tick and re-reserve rather than
// commit. It never escapes next.
var errRateTooLow = errors.New("metronome: next unit is beyond the reservation cap")

// intervalOf is the nominal gap between consecutive units at limit. A zero
// interval means "no schedule": rate.Inf demands everything immediately, so
// nothing can be late against it.
//
// Truncating to whole nanoseconds drifts by under 1ns per unit even for an
// irrational interval (3 rps), i.e. 0.3ms over a million units against a
// schedule measured in milliseconds. Accepted.
func intervalOf(limit rate.Limit) time.Duration {
	if limit <= 0 || math.IsInf(float64(limit), 1) {
		return 0
	}
	return time.Duration(float64(time.Second) / float64(limit))
}

func newPacer(clock Clock, rps float64, burst int) *pacer {
	if burst <= 0 {
		burst = 1
	}
	return &pacer{lim: rate.NewLimiter(sanitizeRate(rps), burst), clock: clock}
}

// next blocks until the next unit of work is due and returns the time it was
// due — which is not the same as the time next returned, and that difference is
// the point. It returns ctx.Err() if ctx is done first, cancelling the
// reservation so the token is not lost.
func (p *pacer) next(ctx context.Context) (time.Time, error) {
	for {
		s, err := p.reserve()
		switch {
		case errors.Is(err, errRateTooLow):
			// The rate is so low that committing would outlive any plausible
			// change to it. Wait one update cadence and ask again; the schedule
			// has not moved, because reserve did not advance it.
			if err := p.clock.Sleep(ctx, maxReservationWait); err != nil {
				return time.Time{}, err
			}
			continue
		case err != nil:
			return time.Time{}, err
		}

		if err := p.clock.Sleep(ctx, s.delay); err != nil {
			s.rsv.CancelAt(p.clock.Now())
			p.abandon(s)
			return time.Time{}, err
		}
		return s.scheduled, nil
	}
}

// slot is one reserved place in the schedule.
type slot struct {
	scheduled time.Time         // when the unit was due
	delay     time.Duration     // how long until it may go out
	rsv       *rate.Reservation // the limiter token, to hand back if abandoned
	advanced  time.Time         // what reserve set p.nominal to
}

// abandon undoes the schedule advance of a slot whose unit never went out, so a
// cancelled wait does not silently skip a place in the schedule — the mirror of
// the reservation being cancelled alongside it.
//
// It rolls back only if nothing has advanced the schedule since. Another worker
// reserving in the meantime means the schedule has legitimately moved on, and
// dragging it backwards under a concurrent pacer would be worse than leaking one
// slot.
func (p *pacer) abandon(s slot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nominal.Equal(s.advanced) {
		p.nominal = s.scheduled
	}
}

// reserve takes the run's next slot: it reads the clock, claims a token, and
// advances the nominal schedule, all under one lock. It returns the time the
// unit was due and how long the caller must wait for it.
//
// The lock is held across the clock read and the reservation because
// x/time/rate anchors the limiter to whatever timestamp it is handed, so a
// stale one rewinds that anchor and over-grants (see the pacer doc comment). It
// is deliberately NOT held across the sleep in next, which would serialise every
// worker onto one schedule slot.
func (p *pacer) reserve() (slot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.clock.Now()
	rsv := p.lim.ReserveN(now, 1)
	if !rsv.OK() {
		// Only possible with burst < 1, which newPacer prevents.
		return slot{}, errors.New("metronome: rate limiter burst is too small for one event")
	}

	// Hand the token straight back if the unit is further out than we are willing
	// to commit to, and leave the schedule untouched — an abandoned slot must not
	// advance it. See maxReservationWait.
	delay := rsv.DelayFrom(now)
	if delay > maxReservationWait {
		rsv.CancelAt(now)
		return slot{}, errRateTooLow
	}

	// The schedule is anchored to the run's origin and advances one interval per
	// unit, independent of when this call actually happened. Deriving it from
	// arrival instead (now + delay) lets a late pacer redefine the schedule: the
	// limiter already holds a token, the delay is zero, and the queueing delay
	// reads as zero in exactly the case that produces it. That is what made
	// v0.2's corrected percentiles equal to the raw ones.
	scheduled := p.nominal
	interval := intervalOf(p.lim.Limit())
	if scheduled.IsZero() || interval == 0 {
		scheduled = now // the first unit anchors the run; rate.Inf has no schedule
	}

	// ...but the schedule may never claim a unit was due LATER than it actually
	// goes out. A burst releases several units at once while the schedule spaces
	// them one interval apart, which would otherwise leave it permanently
	// (burst-1) intervals ahead of reality — and every Start-Scheduled after that
	// is negative, which Stats floors to zero, hiding that much lag for the whole
	// run. Running early is not something to correct for; running late is.
	if actual := now.Add(delay); scheduled.After(actual) {
		scheduled = actual
	}
	p.nominal = scheduled.Add(interval)

	return slot{scheduled: scheduled, delay: delay, rsv: rsv, advanced: p.nominal}, nil
}

// setRate applies a new limit. It reads the injected Clock itself, under the
// same lock next uses: SetLimitAt also anchors the limiter to the timestamp it
// is given, so passing in a separately-read "now" would reintroduce exactly the
// backwards-anchor bug the lock exists to prevent.
func (p *pacer) setRate(rps float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lim.SetLimitAt(p.clock.Now(), sanitizeRate(rps))
}
