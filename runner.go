package metronome

import (
	"context"
	"math/rand/v2"
	"slices"
)

// Runner is one unit of work. The integration seam for request executors.
type Runner interface {
	Do(ctx context.Context) Result
}

// RunnerFunc adapts a function to a Runner.
type RunnerFunc func(ctx context.Context) Result

// Do calls f.
func (f RunnerFunc) Do(ctx context.Context) Result { return f(ctx) }

// Weighted pairs a Runner with a selection weight.
type Weighted struct {
	Runner Runner
	Weight int
}

// Mix returns a Runner that picks one sub-Runner per Do, weighted by Weight.
// It panics if no runners are given, a weight is negative, or all weights are
// zero - programmer errors and Runner has no error path to report them.
func Mix(ws ...Weighted) Runner {
	// The caller may retain and mutate the slice they passed (a variadic call
	// site like Mix(cfg...) hands us their slice, not a copy), which would make
	// the precomputed total stale and silently bias every draw toward the last
	// runner.
	ws = slices.Clone(ws)

	total := 0
	for _, w := range ws {
		if w.Weight < 0 {
			panic("metronome: Mix weight must be >= 0")
		}
		total += w.Weight
	}
	if len(ws) == 0 || total <= 0 {
		panic("metronome: Mix requires at least one Weighted with a positive Weight")
	}
	return RunnerFunc(func(ctx context.Context) Result {
		n := rand.IntN(total) //nolint:gosec // -
		for _, w := range ws {
			n -= w.Weight
			if n < 0 {
				return w.Runner.Do(ctx)
			}
		}
		return ws[len(ws)-1].Runner.Do(ctx)
	})
}
