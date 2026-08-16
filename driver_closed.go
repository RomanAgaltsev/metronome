package metronome

import "context"

// runClosedLoopWorker is one closed-loop worker: claim a token, run the work,
// deliver the Result, repeat. "Closed loop" means the worker does not ask for
// the next token until this unit of work has completed and been delivered - so
// a slow target reduces the achieved rate (see the README's pacing model).
//
// stopCtx is cancelled when MaxRequests is exhausted, releasing workers blocked
// on the limiter. The send below deliberately selects on the parent ctx and NOT
// on stopCtx: exhaustion must never drop an already-produced Result and
// selecting on the context that exhaustion cancels would randomly lose the
// final results whenever both select cases are ready.
func (d *Driver) runClosedLoopWorker(ctx, stopCtx context.Context, stop context.CancelFunc, cfg runConfig, out chan<- Result) {
	for {
		if err := cfg.lim.Wait(stopCtx); err != nil {
			return
		}
		n := cfg.claimed.Add(1)
		if d.MaxRequests > 0 && n > int64(d.MaxRequests) {
			stop()
			return
		}
		res := safeDo(ctx, d.Runner, cfg.clock)
		select {
		case out <- res:
			if d.MaxRequests > 0 && n == int64(d.MaxRequests) {
				stop() // last result delivered: release workers still waiting
			}
		case <-ctx.Done():
			return
		}
	}
}
