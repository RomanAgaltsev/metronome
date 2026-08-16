package metronome

import (
	"context"
	"sync"
)

// runOpenLoop paces from a single goroutine that never blocks on the target.
//
// Each scheduled unit either claims one of Workers slots and runs, or - if
// every slot is busy - is emitted immediately as an ErrSaturated Result. That
// is the whole point of the mode: the schedule is kept and the target's
// inability to keep up is recorded rather than absorbed as a delay that quietly
// deflates the achieved rate.
//
// A semaphore is used rather than a non-blocking send to a work channel because
// slot availability is exact state: a channel send would also fail whenever a
// worker happened not to be parked in its receive at that instant, reporting
// saturation that did not happen.
func (d *Driver) runOpenLoop(ctx, stopCtx context.Context, stop context.CancelFunc, cfg runConfig, out chan<- Result) {
	defer stop()

	slots := make(chan struct{}, cfg.workers)
	for range cfg.workers {
		slots <- struct{}{}
	}

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

	for {
		scheduled, err := cfg.pacer.next(stopCtx)
		if err != nil {
			return
		}

		// MaxRequests counts dispatched units including saturated ones: a
		// saturated attempt was an attempt, and counting only completions would
		// make a saturated run take unboundedly long.
		n := cfg.claimed.Add(1)
		if d.MaxRequests > 0 && n > int64(d.MaxRequests) {
			return
		}

		select {
		case <-slots:
			inflight.Go(func() {
				defer func() { slots <- struct{}{} }()
				res := safeDo(ctx, d.Runner, cfg.clock)
				res.Scheduled = scheduled
				emit(res)
			})
		default:
			if !emit(Result{Scheduled: scheduled, Start: cfg.clock.Now(), Err: ErrSaturated}) {
				return
			}
		}

		if d.MaxRequests > 0 && n == int64(d.MaxRequests) {
			return
		}
	}
}
