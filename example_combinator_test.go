package metronome_test

import (
	"context"
	"fmt"
	"time"

	"github.com/RomanAgaltsev/metronome"
)

func ExampleAfter() {
	// The first 100 units are cold; the rest are fast.
	n := 0
	runner := metronome.RunnerFunc(func(context.Context) metronome.Result {
		n++
		lat := 5 * time.Millisecond
		if n <= 100 {
			lat = 200 * time.Millisecond
		}
		return metronome.Result{Latency: lat, Labels: map[string]string{"endpoint": "list"}}
	})

	d := metronome.Driver{
		Runner:      runner,
		Rate:        metronome.Constant(10000),
		Workers:     1, // one worker, so the cold units are the first ones
		MaxRequests: 500,
	}

	report := metronome.NewStats()
	byRoute := metronome.NewLabeledStats(metronome.Labeled[*metronome.Stats]{
		Key: "endpoint",
		New: metronome.NewStats,
	})
	measured := metronome.AfterN(100, metronome.Multi(report, byRoute))

	metronome.Drain(d.Run(context.Background()), measured)

	fmt.Println("measured:", report.Snapshot().Count)
	fmt.Println("excluded:", measured.Skipped())
	fmt.Println("warm p99 under 10ms:", report.Snapshot().P99 < 10*time.Millisecond)
	// Output:
	// measured: 400
	// excluded: 100
	// warm p99 under 10ms: true
}
