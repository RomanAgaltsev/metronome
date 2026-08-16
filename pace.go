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
	// Held only across the two calls that must agree on "now" — never across the
	// sleep below, which would serialise every worker onto one schedule slot.
	p.mu.Lock()
	now := p.clock.Now()
	rsv := p.lim.ReserveN(now, 1)
	p.mu.Unlock()

	if !rsv.OK() {
		// Only possible with burst < 1, which newPacer prevents.
		return time.Time{}, errors.New("metronome: rate limiter burst is too small for one event")
	}

	delay := rsv.DelayFrom(now)
	scheduled := now.Add(delay)
	if err := p.clock.Sleep(ctx, delay); err != nil {
		rsv.CancelAt(p.clock.Now())
		return time.Time{}, err
	}
	return scheduled, nil
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
