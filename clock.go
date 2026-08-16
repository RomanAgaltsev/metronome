package metronome

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Clock supplies time to the Driver. It is injected so that pacing is testable:
// with a ManualClock the whole pacing path — reservations, sleeps and the rate
// update cadence — advances only when the test says so.
type Clock interface {
	// Now reports the current time.
	Now() time.Time

	// Sleep blocks for d, returning nil, or ctx.Err() if ctx is done first. A
	// non-positive d returns immediately with ctx.Err() (nil for a live ctx).
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

// Now reports the clock's current time.
func (realClock) Now() time.Time { return time.Now() }

// Sleep blocks for d, returning nil, or ctx.Err() if ctx is done first.
func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SystemClock returns a Clock backed by the wall clock. It is what the Driver
// uses when Driver.Clock is nil.
func SystemClock() Clock { return realClock{} }

// sleeper is one goroutine blocked in ManualClock.Sleep.
type sleeper struct {
	deadline time.Time
	wake     chan struct{}
}

// ManualClock is a Clock whose time only advances when Advance is called.
// Goroutines sleeping on it wake when Advance moves the clock to or past their
// deadline, which makes pacing tests exact instead of tolerance-based.
type ManualClock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	cur     time.Time
	waiters []*sleeper
}

// NewManualClock returns a ManualClock reading t.
func NewManualClock(t time.Time) *ManualClock {
	m := &ManualClock{cur: t}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// Now reports the clock's current time.
func (m *ManualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur
}

// Sleep blocks until Advance moves the clock to or past now+d, or ctx is done.
func (m *ManualClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}

	m.mu.Lock()
	s := &sleeper{deadline: m.cur.Add(d), wake: make(chan struct{})}
	m.waiters = append(m.waiters, s)
	m.cond.Broadcast() // release any BlockUntilSleepers waiting for this one
	m.mu.Unlock()

	select {
	case <-s.wake:
		return nil
	case <-ctx.Done():
		m.mu.Lock()
		m.waiters = slices.DeleteFunc(m.waiters, func(w *sleeper) bool { return w == s })
		m.mu.Unlock()
		return ctx.Err()
	}
}

// Advance moves the clock forward by d, waking every sleeper whose deadline the
// clock has now reached, in deadline order.
func (m *ManualClock) Advance(d time.Duration) {
	m.mu.Lock()
	m.cur = m.cur.Add(d)

	var due []*sleeper
	kept := m.waiters[:0]
	for _, w := range m.waiters {
		if w.deadline.After(m.cur) {
			kept = append(kept, w)
		} else {
			due = append(due, w)
		}
	}
	m.waiters = kept
	m.mu.Unlock()

	// Deadline order, so a test observing several sleepers sees a deterministic
	// sequence rather than map/scheduler order.
	slices.SortStableFunc(due, func(a, b *sleeper) int { return a.deadline.Compare(b.deadline) })
	for _, w := range due {
		close(w.wake)
	}
}

// BlockUntilSleepers blocks until at least n goroutines are asleep on this
// clock. It exists to remove the race between a goroutine reaching Sleep and a
// test calling Advance: without it, an Advance that lands first is simply lost
// and the test hangs.
//
// It has no timeout on purpose — if the count is never reached, go test's own
// deadline reports it with every goroutine's stack, which is more useful than a
// bespoke error.
func (m *ManualClock) BlockUntilSleepers(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.waiters) < n {
		m.cond.Wait()
	}
}
