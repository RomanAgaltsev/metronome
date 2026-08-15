package metronome_test

import (
	"context"
	"fmt"
	"time"

	"github.com/RomanAgaltsev/metronome"
)

func ExampleDriver() {
	runner := metronome.RunnerFunc(func(context.Context) metronome.Result {
		return metronome.Result{Start: time.Now(), Latency: time.Millisecond}
	})
	d := metronome.Driver{Runner: runner, Rate: metronome.Constant(500), Workers: 4, MaxRequests: 100}
	stats := metronome.NewStats()
	for r := range d.Run(context.Background()) {
		stats.Record(r)
	}
	fmt.Println(stats.Snapshot().Count)
	// Output: 100
}
