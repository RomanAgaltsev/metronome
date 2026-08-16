# metronome

[![CI](https://github.com/RomanAgaltsev/metronome/actions/workflows/test.yml/badge.svg)](https://github.com/RomanAgaltsev/metronome/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/RomanAgaltsev/metronome.svg)](https://pkg.go.dev/github.com/RomanAgaltsev/metronome)

A protocol-agnostic Go **load kernel**: drive a unit of work at a controlled,
live-adjustable rate across N workers, and measure latency and errors.

metronome knows nothing about HTTP, gRPC, Prometheus, configuration files or UI.
You supply a `Runner` — one unit of work — and a `RateController`, and it gives
you a stream of `Result`s and an aggregator that turns them into percentiles.

```bash
go get github.com/RomanAgaltsev/metronome
```

## Example

```go
runner := metronome.RunnerFunc(func(ctx context.Context) metronome.Result {
	start := time.Now()
	resp, err := http.Get("http://localhost:8080/api/users/42")
	if err != nil {
		return metronome.Result{Start: start, Latency: time.Since(start), Err: err}
	}
	defer resp.Body.Close()
	n, _ := io.Copy(io.Discard, resp.Body)
	return metronome.Result{
		Start:   start,
		Latency: time.Since(start),
		Code:    strconv.Itoa(resp.StatusCode),
		Bytes:   n,
	}
})

d := metronome.Driver{
	Runner:      runner,
	Rate:        metronome.Ramp{Start: 10, End: 200, Over: 30 * time.Second},
	Workers:     16,
	MaxRequests: 5000,
}

stats := metronome.NewStats()
for r := range d.Run(context.Background()) {
	stats.Record(r)
}

snap := stats.Snapshot()
fmt.Printf("%d requests, %.1f rps, %.2f%% errors, p95 %v\n",
	snap.Count, snap.RPS, snap.ErrorRate*100, snap.P95)
```

You **must** drain the channel until it closes, or cancel the context —
abandoning a live channel leaks the workers.

## What's in it

| Piece | What it does |
|---|---|
| `Runner` / `RunnerFunc` | one unit of work; the integration seam |
| `Mix(...Weighted)` | weighted random pick among sub-Runners |
| `Constant`, `Ramp`, `Phased` | static rate profiles |
| `Adaptive` + `SetRate` | rate driven live from outside (a control loop) |
| `Driver` | the paced worker pool; `Run(ctx) <-chan Result` |
| `Stats` / `Snapshot` | HDR-histogram percentiles, error rate, achieved rps |
| `Clock` / `ManualClock` | injected time, for deterministic tests |

## Pacing model — read this before trusting the numbers

metronome offers two pacing modes and reports latency two ways. The defaults are
conservative; the honest defaults for a latency-measuring tool are the other ones.

### Closed loop (default)

Each worker waits for a rate-limiter token, calls your `Runner`, and only then asks
for the next token. Simple, self-throttling, and the right choice when *you* are the
backpressure mechanism (a control loop that deliberately probes for the point where
the target sags). Two consequences:

- **Rate sag.** When the target slows, workers sit inside `Do`, tokens go unclaimed,
  and the achieved rate falls below the offered rate — silently.
- **Coordinated omission.** Latency is measured from when work actually started, so
  every stall suppresses exactly the samples that would have revealed it.

### Open loop (`Pacing: metronome.OpenLoop`)

One dispatcher keeps the schedule and never blocks on the target. A unit that finds
no free worker is delivered immediately as a `Result` whose `Err` matches
`errors.Is(err, metronome.ErrSaturated)`. Saturation therefore shows up as
`Snapshot.ErrorRate`, not as invisible sag. `Workers` becomes the maximum in-flight
cap.

### Raw vs corrected percentiles

Every `Result` from a `Driver` carries `Scheduled`, the time it *should* have been
sent. `Stats` reports both:

- `P50` / `P95` / `P99` — measured from when work started. Optimistic under stall.
- `CorrectedP50` / `CorrectedP95` / `CorrectedP99` — measured from when the work was
  *due*, i.e. including the queueing delay a schedule-faithful client would have
  suffered.

Read them as a pair. A large gap means the generator queued: the raw numbers
understate what a real client would experience by roughly that amount.

```go
snap := stats.Snapshot()
fmt.Printf("p95 %v (corrected %v), achieved %.1f rps, %.1f%% saturated\n",
	snap.P95, snap.CorrectedP95, snap.RPS, snap.ErrorRate*100)
```

### Measured accuracy

<!-- Numbers from Task 15's real run; replace the machine line with the real one. -->

Measured on <CPU model>, <OS>, Go <version>, machine otherwise idle
(`go test -bench . ./...`):

| Offered rate | Closed-loop adherence | Open-loop adherence |
|---|---|---|
| 10 rps | <measured> | <measured> |
| 100 rps | <measured> | <measured> |
| 1,000 rps | <measured> | <measured> |
| 5,000 rps | <measured> | <measured> |

Per-request kernel overhead (no-op Runner, unlimited rate): **<measured> ns/op**
closed-loop, **<measured> ns/op** open-loop — an upper bound of roughly
**<measured> rps** on this machine. Reproduce with
`go test -run '^$' -bench . ./...`.
`

## Status

v0.1 — API is stable in shape and pinned by two consumers, but **pre-v1: minor
versions may carry small breaking changes**, always with a migration note in the
CHANGELOG. Pin an exact version.

## Used by

- **crescendo** — PromQL-driven adaptive load: a `Mix` of endpoint Runners under an
  `Adaptive` controller steered by a Prometheus feedback loop.
- **quiver** — `qv load`: one Runner over a saved API request, `Constant`/`Ramp`,
  `MaxRequests`, `Stats` summary.

## License

MIT
`
