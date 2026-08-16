package metronome

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const defWorkers int = 10

// DefaultResultBuffer is the capacity of Run's result channel when
// Driver.ResultBuffer is zero.
const DefaultResultBuffer = 1024

// Driver runs a Runner under a RateController across Workers goroutines,
// emitting Results on the returned channel until ctx is cancelled or
// MaxRequests results have been produced (MaxRequests == 0 means unlimited).
//
// Contract: when MaxRequests is set, exactly MaxRequest Results are
// delivered unless ctx is cancelled first (cancellation aborts promptly and
// may drop in-flight results). The channel is closed once all internal
// goroutines exit. The caller must drain the channel or cancel ctx.
type Driver struct {
	Runner      Runner
	Rate        RateController
	Workers     int
	MaxRequests int
	Clock       Clock

	// ResultBuffer is the capacity of the channel Run returns. Zero selects
	// DefaultResultBuffer; a negative value selects an unbuffered channel.
	//
	// Buffering matters for measurement, not just throughput: on an unbuffered
	// channel a consumer that pauses blocks a worker that is holding a
	// rate-limiter token, producing rate sag the target did not cause.
	ResultBuffer int
}

// Run starts the workers and returns the channel Results are delivered on.
//
// Run has a pointer receiver, so it cannot be called on a composite literal:
// bind the Driver to a variable first (d := Driver{...}; d.Run(ctx)).
//
// The caller MUST drain the returned channel until it closes, or cancel ctx.
// Abandoning a live channel leaves the workers blocked on the send and leaks
// them for the lifetime of the process.
//
// Run panics if Runner or Rate is nil — programmer errors, not runtime
// conditions. Workers <= 0 defaults to DefaultWorkers.
//
//nolint:gocognit // -
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

	lim := rate.NewLimiter(sanitizeRate(d.Rate.Rate(0)), 1)

	buffer := d.ResultBuffer
	switch {
	case buffer == 0:
		buffer = DefaultResultBuffer
	case buffer < 0:
		buffer = 0
	}
	out := make(chan Result, buffer)

	// stopCtx releases workers blocked in lim.Wait once MaxRequests is
	// exhausted. The send path below deliberately selects on the parent ctx,
	// NOT stopCtx - exhaustion must never drop an already-produced Result
	// (selecting on the same ctx that exhaustion cancels would randomly lose
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
				lim.SetLimit(sanitizeRate(d.Rate.Rate(clock.Now().Sub(start))))
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
				res := safeDo(ctx, d.Runner, clock) // parent ctx: exhaustion must not cancel in-flight work
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

// safeDo runs r.Do, converting a panic into a failed Result rather than letting
// it kill the process. It also stamps Start when the Runner left it zero, since
// Stats derives the achieved rate from Result.Start.
func safeDo(ctx context.Context, r Runner, clock Clock) (res Result) {
	start := clock.Now()
	defer func() {
		if v := recover(); v != nil {
			res = Result{
				Start:   start,
				Latency: clock.Now().Sub(start),
				Err:     &PanicError{Value: v, Stack: debug.Stack()},
			}
		}
	}()
	res = r.Do(ctx)
	if res.Start.IsZero() {
		res.Start = start
	}
	return res
}
