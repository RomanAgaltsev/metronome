package metronome

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const defWorkers int = 10

// Driver runs a Runner under a RateController across Workers goroutines,
// emitting Results on the returned channel until ctx is cancelled or
// MaxRequest results have been produced (MaxRequests == 0 means unlimited).
//
// Contract: when MaxRequest is set, exactly MaxRequest Results are
// delivered unless ctx is cancelled first (cancellation aborts promptly and
// may drop in-flight results). The channel is closed once all internal
// goroutines exit. The caller must drain the channel or cancel ctx.
type Driver struct {
	Runner      Runner
	Rate        RateController
	Workers     int
	MaxRequests int
	Clock       Clock
}

func (d *Driver) Run(ctx context.Context) <-chan Result {
	if d.Runner == nil {
		panic("metronome: Driver.Runner is nil")
	}
	if d.Rate == nil {
		panic("metronome: Driver.Rate is nil")
	}

	workers := d.Workers
	if workers <= 0 {
		workers = defWorkers
	}

	clock := d.Clock
	if clock == nil {
		clock = realClock{}
	}
	start := clock.Now()

	lim := rate.NewLimiter(rate.Limit(max(d.Rate.Rate(0), 0.0001)), 1)
	out := make(chan Result)

	// stopCtx releases workers blocked in lim.Wait once MaxRequests is
	// exhausted. The send path below deliberately selects on the parent ctx,
	// NOT stopCtx - exhaustion must never drop an already-produced Result
	// (selecting on the same ctx that exhaustion cancels wuold randomly lose
	// the final results when both select cases are ready).
	stopCtx, stop := context.WithCancel(ctx)

	var claimed atomic.Int64
	var wg sync.WaitGroup

	// Rate updater.
	wg.Go(func() {

		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()

		for {
			select {
			case <-stopCtx.Done():
				return
			case <-t.C:
				lim.SetLimit(rate.Limit(max(d.Rate.Rate(clock.Now().Local().Sub(start)), 0.0001)))

			}
		}
	})

	// Workers
	for range workers {
		wg.Go(func() {
			for {
				if err := lim.Wait(stopCtx); err != nil {
					return
				}
				n := claimed.Add(1)
				if d.MaxRequests > 0 && n > int64(d.MaxRequests) {
					stop()
					return
				}
				res := d.Runner.Do(ctx) // parent ctx: exhaustion must not cancel in-flight work
				select {
				case out <- res:
					if d.MaxRequests > 0 && n == int64(d.MaxRequests) {
						stop() // last result delivered: release workers still in lim.Wait
					}
				case <-ctx.Done():
					return
				}
			}
		})
	}

	go func() {
		wg.Wait()
		stop()
		close(out)
	}()

	return out
}
