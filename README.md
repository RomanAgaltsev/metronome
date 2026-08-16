# metronome

[![CI](https://github.com/RomanAgaltsev/metronome/actions/workflows/test.yml/badge.svg)](https://github.com/RomanAgaltsev/metronome/actions/workflows/test.yml)
[![Coverage](https://img.shields.io/badge/coverage-98%25-brightgreen)](https://github.com/RomanAgaltsev/metronome/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/RomanAgaltsev/metronome.svg)](https://pkg.go.dev/github.com/RomanAgaltsev/metronome)

<!--
The coverage badge is static: CI computes coverage on three OSes but publishes it
nowhere, and test.yml is keel-owned, so wiring a live badge would mean either a
permanent `keel update` conflict or a third-party service. Refresh it by hand when
it moves, using the same command CI runs:

    go test -covermode=atomic -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

It is quoted to the nearest whole percent on purpose: the timing-sensitive
saturation branches make the exact figure vary by a few tenths between runs.
-->


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

fmt.Println(stats.Snapshot())
// 5000 requests, 198.7 rps, 0.14% err (0 saturated), p50/p95/p99 12ms/41ms/88ms,
// corrected p95/p99 41ms/89ms, behind schedule 1.2ms
```

`Snapshot` is a plain struct — reach for the fields when you want them
(`snap.P95`, `snap.Codes`, `snap.Bytes`). `String()` is there so every consumer
does not reinvent the same summary line, and so the numbers that matter appear in
the order they should be read.

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
| `Pacing` + `ClosedLoop` / `OpenLoop` | how the Driver reacts when the target cannot keep up |
| `Burst` | rate-limiter burst size; 0 means 1 (smoothest schedule) |
| `ErrSaturated` | open-loop marker: no worker was free at the scheduled time |
| `Stats` / `Snapshot` | HDR-histogram percentiles, error rate, achieved rps, bytes/codes |
| `Snapshot.Saturated` | how much of the error rate was the target refusing work |
| `Snapshot.MaxScheduleLag` | how far the generator itself fell behind its own schedule |
| `Snapshot.String()` | the numbers a run is judged on, in the order to read them |
| `Snapshot.Corrected*` | coordinated-omission-corrected percentiles (see the caveat below) |
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
`Snapshot.ErrorRate`, not as invisible sag — and `Snapshot.Saturated` counts it
separately, so "my generator ran out of workers" stays distinguishable from "the
target failed". `Workers` becomes the maximum in-flight cap.

Open loop paces from **one** dispatcher goroutine, which sleeps once per unit of
work. Below roughly a 250µs interval that sleep costs more than the interval, and the
dispatcher — not the target — becomes the bottleneck: on the machine measured below,
open loop holds 1,000 rps exactly and delivers only **74% of 5,000 rps**, with zero
`ErrSaturated` results, because the target was never asked. `Snapshot.MaxScheduleLag`
is what makes that visible; see the table below. Closed loop does not have this ceiling
(its `Workers` goroutines sleep concurrently, so each individual sleep is `Workers`
times longer). Above ~1,000 rps, check `MaxScheduleLag`, and prefer closed loop if
throughput matters more to you than schedule fidelity.

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
fmt.Printf("p95 %v (corrected %v), achieved %.1f rps, %v behind schedule\n",
	snap.P95, snap.CorrectedP95, snap.RPS, snap.MaxScheduleLag)
```

The schedule is **anchored**: `Scheduled` is the run's origin plus one interval per
unit dispatched, advanced whether or not the generator got there on time. A late
generator is therefore *measured against* the schedule rather than redefining it. One
worker offered 100 rps against a 50ms target achieves 19.9 rps and reports:

```
raw       P50=50.0ms  P99=50.0ms
corrected P50=412.9ms P99=816.1ms   MaxScheduleLag=765.7ms
```

The 816ms is the honest number: by the twentieth request, a client that kept to the
10ms schedule would have been waiting that long. (Before v0.3 both lines read 50ms.)

### Who fell behind — you or the target?

`Snapshot.Saturated` and `Snapshot.MaxScheduleLag` answer that between them:

| `Saturated` | `MaxScheduleLag` | Meaning |
|---|---|---|
| 0 | ~0 | the schedule was kept and the target kept up |
| > 0 | ~0 | the **target** could not keep up — open loop working as designed |
| 0 | large | the **generator** could not keep up — lower the rate, or use closed loop |

The third row is why `MaxScheduleLag` exists: `ErrSaturated` can only report a target
that was too slow, never a dispatcher that was. At 5,000 rps open loop reports
`Saturated=0, MaxScheduleLag=330ms` — nothing was wrong with the target.

### Measured accuracy

Measured on an AMD Ryzen 5 3600 (6 cores / 12 threads), Windows 11, Go 1.26.6,
machine otherwise idle. Adherence is achieved rps ÷ offered rps; 1.00 is perfect.

| Offered rate | Closed-loop adherence | Open-loop adherence |
|---|---|---|
| 10 rps | 1.00 | 1.00 |
| 100 rps | 1.00 | 1.00 |
| 1,000 rps | 1.00 | 1.00 |
| 5,000 rps | 1.00 | **0.74** |

The 0.74 is real and reproducible, and it is a limit of the generator, not of the
target — see the open-loop note above. That run reports `Saturated=0` and
`MaxScheduleLag=330ms`, which is how you would tell without reading this table.
Anchoring the schedule in v0.3 made the shortfall *visible*, not smaller: the
adherence numbers are unchanged from v0.2. Reproduce with:

```bash
go test -run '^$' -bench BenchmarkDriverPaced -benchtime 3x ./...
```

Per-request kernel overhead (no-op `Runner`, unlimited rate, `Workers = GOMAXPROCS`):
**600 ns/op** closed-loop and **895 ns/op** open-loop, i.e. a plumbing ceiling near
**1.7M rps** and **1.1M rps** respectively. Note the gap between that ceiling and the
5,000 rps adherence figure above: metronome's limit at realistic rates is sleep
granularity, not CPU. `Stats.Record` costs **211 ns/op** under full contention.
Reproduce with:

```bash
go test -run '^$' -bench 'BenchmarkDriverOverhead|BenchmarkStatsRecord' ./...
```

## Status

v0.3 — API is stable in shape and pinned by two consumers, but **pre-v1: minor
versions may carry small breaking changes**, always with a migration note in the
CHANGELOG. Pin an exact version.

## Used by

- **crescendo** — PromQL-driven adaptive load: a `Mix` of endpoint Runners under an
  `Adaptive` controller steered by a Prometheus feedback loop.
- **quiver** — `qv load`: one Runner over a saved API request, `Constant`/`Ramp`,
  `MaxRequests`, `Stats` summary.

## License

MIT
