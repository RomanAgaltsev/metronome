package metronome

import (
	"context"
	"sync"
	"time"
)

// minDeliveryBacklog is the smallest number of completed-but-undelivered units
// the open loop will hold before treating its own pipeline as saturated.
//
// The backlog normally tracks ResultBuffer, but it never drops below this:
// choosing an unbuffered result channel is legitimate, and it must not turn a
// consumer's momentary pause into a report of target saturation. It is finite so
// that a consumer which stops reading altogether cannot grow the generator
// without bound.
const minDeliveryBacklog = 64

// semaphore is a counting semaphore with a non-blocking acquire. The open loop
// needs "is there capacity right now?" as a decision, never as a wait: waiting
// is precisely what the mode exists not to do.
type semaphore chan struct{}

func newSemaphore(n int) semaphore {
	s := make(semaphore, n)
	for range n {
		s <- struct{}{}
	}
	return s
}

func (s semaphore) tryAcquire() bool {
	select {
	case <-s:
		return true
	default:
		return false
	}
}

func (s semaphore) release() { s <- struct{}{} }

// runOpenLoop paces from a single goroutine that never blocks on the target.
//
// There are no long-lived workers: each scheduled unit gets its own goroutine,
// and two counting semaphores bound what may exist at once. A unit that cannot
// claim capacity is emitted immediately as an ErrSaturated Result rather than
// delayed, which is the whole point of the mode — the schedule is kept, and an
// inability to keep up is recorded rather than absorbed as sag.
//
// The two bounds are deliberately separate:
//
//   - running caps units inside Runner.Do at Workers. Exhausting it means the
//     TARGET cannot keep up, which is the saturation this mode exists to report.
//   - live caps goroutines that exist at all, including one still blocked
//     delivering its Result. Its slot in running is released before that send,
//     so a consumer that pauses briefly is not reported as target saturation;
//     without live, that release would leave nothing bounding goroutine growth
//     and a consumer that stops reading would grow the generator at the offered
//     rate. Exhausting live means our own delivery pipeline is a whole backlog
//     behind — also saturation, ours rather than the target's.
//
// Semaphores are used rather than a non-blocking send to a work channel because
// capacity is exact state: a channel send would also fail whenever a receiver
// happened not to be parked at that instant, reporting saturation that did not
// happen.
func (d *Driver) runOpenLoop(ctx, stopCtx context.Context, stop context.CancelFunc, cfg runConfig, out chan<- Result) {
	defer stop()

	running := newSemaphore(cfg.workers)
	live := newSemaphore(cfg.workers + max(cfg.buffer, minDeliveryBacklog))

	var inflight sync.WaitGroup
	defer inflight.Wait() // out is closed by Run only after this returns

	emit := func(res Result) bool {
		select {
		case out <- res:
			return true
		case <-ctx.Done():
			return false
		}
	}
	saturated := func(scheduled time.Time) Result {
		return Result{Scheduled: scheduled, Start: cfg.clock.Now(), Err: ErrSaturated}
	}

	for {
		scheduled, err := cfg.pacer.next(stopCtx)
		if err != nil {
			return
		}

		// MaxRequests counts dispatched units including saturated ones: a
		// saturated attempt was an attempt, and counting only completions would
		// make a saturated run take unboundedly long. One dispatcher increments
		// cfg.claimed, so n reaches MaxRequests exactly once and the loop returns
		// on it below — there is no racing overshoot to guard against here, which
		// is the difference from the closed-loop worker.
		n := cfg.claimed.Add(1)

		switch {
		case !running.tryAcquire():
			if !emit(saturated(scheduled)) {
				return
			}
		case !live.tryAcquire():
			running.release()
			if !emit(saturated(scheduled)) {
				return
			}
		default:
			inflight.Go(func() {
				defer live.release()
				res := safeDo(ctx, d.Runner, cfg.clock)
				res.Scheduled = scheduled
				running.release() // before emitting; see the doc comment
				emit(res)
			})
		}

		if d.MaxRequests > 0 && n == int64(d.MaxRequests) {
			return
		}
	}
}
