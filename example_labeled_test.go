package metronome_test

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/RomanAgaltsev/metronome"
)

func ExampleLabeledStats() {
	endpoint := func(name string, lat time.Duration) metronome.Runner {
		return metronome.RunnerFunc(func(context.Context) metronome.Result {
			return metronome.Result{
				Latency: lat,
				Labels:  map[string]string{"endpoint": name},
			}
		})
	}

	d := metronome.Driver{
		Runner: metronome.Mix(
			metronome.Weighted{Runner: endpoint("list", 5*time.Millisecond), Weight: 3},
			metronome.Weighted{Runner: endpoint("search", 80*time.Millisecond), Weight: 1},
		),
		Rate:        metronome.Constant(5000),
		Workers:     8,
		MaxRequests: 400,
	}

	stats := metronome.NewLabeledStats(metronome.Labeled[*metronome.Stats]{
		Key: "endpoint",
		New: metronome.NewStats,
	})
	for r := range d.Run(context.Background()) {
		stats.Record(r)
	}

	names := make([]string, 0, stats.Len())
	for name := range stats.Series() {
		names = append(names, name)
	}
	sort.Strings(names)

	fmt.Println("total:", stats.Snapshot().Count)
	for _, name := range names {
		fmt.Printf("%s p50 >= 5ms: %v\n", name, stats.Series()[name].Snapshot().P50 >= 5*time.Millisecond)
	}
	// Output:
	// total: 400
	// list p50 >= 5ms: true
	// search p50 >= 5ms: true
}
