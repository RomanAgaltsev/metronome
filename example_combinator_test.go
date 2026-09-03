package metronome_test

import (
	"context"
	"fmt"
	"time"

	"github.com/RomanAgaltsev/metronome"
)

func ExampleAfter() {
	// A hand-fed stream, so the schedule is exact and the example is
	// deterministic: ten units due one second apart, the first three of them
	// cold. After anchors on the first Result's Scheduled stamp, so the warmup
	// boundary comes from the run rather than from when this was constructed.
	origin := time.Unix(0, 0)
	stream := make([]metronome.Result, 0, 10)
	for i := range 10 {
		lat := 5 * time.Millisecond
		if i < 3 {
			lat = 200 * time.Millisecond // cold: pools, handshakes, an empty cache
		}
		at := origin.Add(time.Duration(i) * time.Second)
		stream = append(stream, metronome.Result{Scheduled: at, Start: at, Latency: lat})
	}

	warm := metronome.NewStats()
	measured := metronome.After(3*time.Second, warm)

	everything := metronome.NewStats()
	for _, r := range stream {
		everything.Record(r)
		measured.Record(r)
	}

	// Rounded, because the exact figures are HDR bucket boundaries and this
	// example is about the exclusion, not about histogram resolution.
	fmt.Println("whole run p99: ", everything.Snapshot().P99.Round(time.Millisecond))
	fmt.Println("warmed p99:    ", measured.Snapshot().P99.Round(time.Millisecond))
	fmt.Printf("measured %d of %d, excluded %d as warmup\n",
		measured.Snapshot().Count, everything.Snapshot().Count, measured.Skipped())
	// Output:
	// whole run p99:  200ms
	// warmed p99:     5ms
	// measured 7 of 10, excluded 3 as warmup
}

func ExampleAfterN() {
	// AfterN counts units instead of time, which is the right shape when the
	// warmup is "the first N requests" rather than "the first N seconds".
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
	// Warmup exclusion wraps the fan-out, so both recorders see the same
	// measured population. Multi(AfterN(100, report), byRoute) would be a
	// different, also-valid program — and the difference is in the expression.
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
