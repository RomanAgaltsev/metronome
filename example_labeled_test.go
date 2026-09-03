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

	series := stats.Series()
	list, search := series["list"].Snapshot(), series["search"].Snapshot()
	total := stats.Snapshot()

	fmt.Println("series:", names)
	fmt.Println("total count:", total.Count)

	// The motivating defect, shown rather than described. list is three
	// quarters of the traffic and sixteen times faster, but the aggregate P99
	// is search's alone — so the one number a flat Stats reports describes a
	// quarter of the requests and hides the rest.
	fmt.Println("list   p99:", list.P99.Round(time.Millisecond))
	fmt.Println("search p99:", search.P99.Round(time.Millisecond))
	fmt.Println("total  p99:", total.P99.Round(time.Millisecond))

	// And the series always reconcile with the total.
	fmt.Println("series sum == total:", list.Count+search.Count == total.Count)
	// Output:
	// series: [list search]
	// total count: 400
	// list   p99: 5ms
	// search p99: 80ms
	// total  p99: 80ms
	// series sum == total: true
}
