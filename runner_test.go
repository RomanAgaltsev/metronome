package metronome

import (
	"context"
	"testing"
)

func TestRunnerFunc(t *testing.T) {
	var r Runner = RunnerFunc(func(context.Context) Result { return Result{Code: "ok"} })
	if r.Do(context.Background()).Code != "ok" {
		t.Fatal("RunnerFunc should invoke the wrapped func")
	}
}

func TestMixPicksAll(t *testing.T) {
	a := RunnerFunc(func(context.Context) Result { return Result{Code: "a"} })
	b := RunnerFunc(func(context.Context) Result { return Result{Code: "b"} })
	m := Mix(Weighted{a, 1}, Weighted{b, 1})
	seen := map[string]bool{}
	for i := 0; i < 500; i++ {
		seen[m.Do(context.Background()).Code] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("Mix should eventually pick both, saw %v", seen)
	}
}

func TestMixRespectsWeights(t *testing.T) {
	a := RunnerFunc(func(context.Context) Result { return Result{Code: "a"} })
	b := RunnerFunc(func(context.Context) Result { return Result{Code: "b"} })
	m := Mix(Weighted{a, 9}, Weighted{b, 1})
	counts := map[string]int{}
	const draws = 2000
	for i := 0; i < draws; i++ {
		counts[m.Do(context.Background()).Code]++
	}
	// Binomial(2000, 0.9): mean 1800, sd ~13.4 — [1700, 1900] is ±7σ, so this
	// effectively never flakes but catches any real weighting bug.
	if counts["a"] < 1700 || counts["a"] > 1900 {
		t.Fatalf("weight 9:1 gave a=%d of %d draws (want ~1800)", counts["a"], draws)
	}
}

func TestMixRejectsBadInput(t *testing.T) {
	nop := RunnerFunc(func(context.Context) Result { return Result{} })
	cases := []struct {
		name string
		fn   func()
	}{
		{"no runners", func() { Mix() }},
		{"all-zero weights", func() { Mix(Weighted{nop, 0}) }},
		{"negative weight", func() { Mix(Weighted{nop, -1}, Weighted{nop, 2}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.fn()
		})
	}
}
