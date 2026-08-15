package metronome

import (
	"sync"
	"time"
)

// Clock supplies the current time. Injected so pacing logic is testable.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// SystemClock returns a Clock backed by the wall clock.
func SystemClock() Clock { return realClock{} }

// ManualClock is a Clock whose time only advances when Advance is called.
type ManualClock struct {
	mu  sync.Mutex
	cur time.Time
}

func NewManualClock(t time.Time) *ManualClock {
	return &ManualClock{cur: t}
}

func (m *ManualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cur
}

func (m *ManualClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cur = m.cur.Add(d)
}
