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

metronome v0.1 is a **closed-loop** generator: each worker waits for a rate-limiter
token, calls your `Runner`, and only then asks for the next token. That has two
consequences you must know about, both shared with most simple load tools:

- **Rate sag.** When the target slows down, all workers sit inside `Do`, tokens go
  unclaimed, and the *achieved* rate silently falls below the *offered* rate. A run
  that reports "p95 300ms at 100 rps" may have been delivering 40 rps.
- **Coordinated omission.** Latency is measured from when the request actually
  started, not from when it *should* have started per the schedule. Every stall in
  the target suppresses exactly the samples that would have revealed the stall, so
  percentiles are systematically optimistic.

Compare `Snapshot.RPS` against the rate you asked for on every run. If it is lower,
your latency numbers are optimistic by roughly the amount of the gap.

An open-loop mode (the schedule never blocks on the target; saturation is recorded
rather than hidden) and coordinated-omission-corrected percentiles are the subject
of v0.2.

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
