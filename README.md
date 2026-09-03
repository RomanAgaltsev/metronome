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
| `RollingStats` / `Rolling` | the same aggregate over a trailing window, for watching a live run |
| `Snapshot.Window` | the duration a `Snapshot` covers; zero means cumulative |
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

### Rolling window

Every number a `Snapshot` carries is cumulative over the whole run. That is right
for the report you print at the end and wrong for anything watching a run while it
happens: `MaxScheduleLag` is a running maximum, so one stall in the first second
pins it for the rest of the run, and a target that stops answering **never moves
any cumulative number at all** — there are no new `Result`s to move them, so `RPS`
goes on reporting the rate the run used to achieve.

`RollingStats` records into both views at once. Use `Snapshot()` for the end-of-run
report — it means exactly what `Stats.Snapshot()` means — and `Window()` for a
control loop, a progress line, or anything else reading a run live.

```go
rs := metronome.NewRollingStats(metronome.Rolling{Window: 10 * time.Second, Buckets: 10})
for r := range d.Run(ctx) {
	rs.Record(r)
}

// live, from a control loop or a progress ticker:
fmt.Println(rs.Window())   // last 9.7s: 4821 req, 497.0 rps, 0.00% err (0 saturated), ...
// at the end:
fmt.Println(rs.Snapshot()) // 50210 req, 499.1 rps, 0.02% err (0 saturated), ...
```

The zero `Rolling{}` is valid: a 10-second window in 10 buckets on the wall clock
over the `NewStats` histogram range. Pass a `Clock` to make window tests exact,
the same `ManualClock` the `Driver` takes.

`Window()` divides by the time the ring **actually** covers, not by the nominal
window, and reports that duration in `Snapshot.Window` — non-zero on a windowed
`Snapshot`, zero on a lifetime one, and `String()` prefixes `last 9.7s: ` so the
two are never confused in a log. So three seconds into a ten-second window a
healthy 100 rps reads as `100.0` over `Window: 3s` rather than `30.0` over a window
two-thirds empty, and at steady state the figure sawtooths across
`[window − interval, window]` because the newest bucket is always partial. The
numbers are correct for the period they cover; that field is what the period is.

Rotation happens on read as well as write, which is the whole point: with no
`Record` traffic to drive it, only a read can notice that time has passed, so a
full stall drains the window to zero instead of freezing it at the last healthy
figures.

Measured on the machine described under [Measured accuracy](#measured-accuracy):

| Operation | Cost |
|---|---|
| `Stats.Record` | 147 ns/op |
| `RollingStats.Record` | 223 ns/op — about **+75 ns**, or 1.5×, for the second view |
| `Window()`, ring full of traffic | 730 µs/op, 2 allocs |
| `Window()`, ring live but empty (a stall) | 2.7 µs/op, 2 allocs |

`Record` is the hot path and pays a flat 75 ns. `Window()` is not: it merges the
live buckets' histograms, so it costs roughly `live × countsLen`, independent of
how many `Result`s are in them — 0.7 ms at the default range and a full ring. Poll
it at 1–10 Hz from a control loop, not per request. Empty buckets are skipped, so
the stall case a control loop polls hardest in is the cheap one. Reproduce with:

```bash
go test -run '^$' -bench 'BenchmarkStatsRecord|BenchmarkRollingStats' -benchtime=2s -benchmem ./...
```

### Warmup exclusion and fan-out

The first seconds of a run measure cold connection pools, TLS handshakes that will be reused and
a target that has not warmed up — none of which is the system under test. `After` keeps them out
of the measurement without changing the load:

```go
report  := metronome.NewStats()
byRoute := metronome.NewLabeledStats(metronome.Labeled[*metronome.Stats]{
    Key: "endpoint", New: metronome.NewStats,
})

measured := metronome.After(10*time.Second, metronome.Multi(report, byRoute))
metronome.Drain(driver.Run(ctx), measured)

fmt.Println(report.Snapshot())
fmt.Printf("measured %d, excluded %d as warmup\n",
    report.Snapshot().Count, measured.Skipped())

#### Memory

A `RollingStats` allocates `Buckets+2` histogram pairs — the ring, the lifetime
aggregate and a scratch merge target — so `Buckets` multiplies memory as well as
resolution. Price any configuration before building it with `Rolling.Bytes()`.

| `Rolling` | Footprint |
|---|---|
| `Rolling{}` (10 buckets, 1µs–60s, 3 sig figs) | 3.2 MiB |
| `Rolling{Buckets: 100}` | 27 MiB |
| `Rolling{Buckets: 1000}` | 266 MiB |
| `Rolling{Buckets: 1000, Lo: time.Millisecond, Hi: time.Second, Sigfigs: 1}` | 2.0 MiB |

The last two rows are the rule: **narrow the histogram range when you want a large
ring.** `Bytes()` panics on exactly the configurations `NewRollingStats` panics on,
so pricing an unbuildable config reports the problem rather than a number for it.

Underneath the arithmetic is a representation mismatch, not a tuning problem. A
fine-grained ring — 1,000 buckets over 10s at 1,000 rps — puts about ten `Result`s
in each 136 KiB dense array, roughly **13 KiB per recorded sample**, and HDR is
array-backed. Sparse bucket storage is the fix and it is a known, costed trade
rather than an oversight: it is demand-gated on the roadmap, so open an issue if
you want a fine-grained window and it becomes a decision rather than a
rediscovery.

### Per-endpoint breakdown

A `Mix` of ten endpoints reports one P99 that no endpoint exhibits. `LabeledStats` splits the
stream on one `Result.Labels` key:

```go
stats := metronome.NewLabeledStats(metronome.Labeled[*metronome.Stats]{
    Key: "endpoint",
    New: metronome.NewStats,
})
for r := range driver.Run(ctx) {
    stats.Record(r)
}

stats.Snapshot()                      // the total — same as a plain Stats
stats.Series()["search"].Snapshot()   // just that endpoint


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
granularity, not CPU. `Stats.Record` costs **147 ns/op** under full contention
(`RollingStats.Record` 223 — see [Rolling window](#rolling-window)).
Reproduce with:

```bash
go test -run '^$' -bench 'BenchmarkDriverOverhead|BenchmarkStatsRecord' ./...
```

## Status

v0.5 — API is stable in shape and pinned by two consumers, but **pre-v1: minor
versions may carry small breaking changes**, always with a migration note in the
CHANGELOG. Pin an exact version.

## Used by

- **crescendo** — PromQL-driven adaptive load: a `Mix` of endpoint Runners under an
  `Adaptive` controller steered by a Prometheus feedback loop.
- **quiver** — `qv load`: one Runner over a saved API request, `Constant`/`Ramp`,
  `MaxRequests`, `Stats` summary.

## License

MIT
