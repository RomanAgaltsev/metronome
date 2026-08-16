package metronome

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultResultBuffer is the capacity of Run's result channel when
// Driver.ResultBuffer is zero.
const DefaultResultBuffer = 1024

// DefaultWorkers is the worker count Run uses when Driver.Workers is not positive.
const DefaultWorkers = 10

// rateUpdateInterval is how often the limiter is re-read from the RateController.
const rateUpdateInterval = 100 * time.Millisecond

// runConfig is the resolved, validated form of a Driver for one Run.
type runConfig struct {
	workers int
	buffer  int
	clock   Clock
	start   time.Time
	pacer   *pacer
	claimed *atomic.Int64
}

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

	// Clock supplies time to the whole pacing path: the rate schedule, the
	// sleeps between units of work, and the rate-update cadence. Leave nil for
	// the wall clock; inject a ManualClock to drive pacing exactly in tests.
	Clock Clock

	// ResultBuffer is the capacity of the channel Run returns. Zero selects
	// DefaultResultBuffer; a negative value selects an unbuffered channel.
	//
	// Buffering matters for measurement, not just throughput: on an unbuffered
	// channel a consumer that pauses blocks a worker that is holding a
	// rate-limiter token, producing rate sag the target did not cause.
	ResultBuffer int
}

// config validates the Driver and resolves its defaults. It panics on nil
// Runner or Rate: those are programmer errors, and Run has no error return.
func (d *Driver) config() runConfig {
	if d.Runner == nil {
		panic("metronome: Driver.Runner is nil")
	}
	if d.Rate == nil {
		panic("metronome: Driver.Rate is nil")
	}

	workers := d.Workers
	if workers <= 0 {
		workers = DefaultWorkers
	}

	buffer := d.ResultBuffer
	switch {
	case buffer == 0:
		buffer = DefaultResultBuffer
	case buffer < 0:
		buffer = 0
	}

	clock := d.Clock
	if clock == nil {
		clock = realClock{}
	}

	return runConfig{
		workers: workers,
		buffer:  buffer,
		clock:   clock,
		start:   clock.Now(),
		pacer:   newPacer(clock, d.Rate.Rate(0), 1),
		claimed: new(atomic.Int64),
	}
}

// runUpdater re-reads the RateController every rateUpdateInterval and applies
// the result to the limiter, so a live SetRate takes effect mid-run.
func (d *Driver) runUpdater(ctx context.Context, cfg runConfig) {
	for {
		if err := cfg.clock.Sleep(ctx, rateUpdateInterval); err != nil {
			return
		}
		now := cfg.clock.Now()
		cfg.pacer.setRate(now, d.Rate.Rate(now.Sub(cfg.start)))
	}
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
func (d *Driver) Run(ctx context.Context) <-chan Result {
	cfg := d.config()
	out := make(chan Result, cfg.buffer)
	stopCtx, stop := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Go(func() { d.runUpdater(stopCtx, cfg) })
	for range cfg.workers {
		wg.Go(func() { d.runClosedLoopWorker(ctx, stopCtx, stop, cfg, out) })
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
