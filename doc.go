// Package metronome drives a unit of work at a controlled, live-adjustable rate
// across N workers and measures latency and errors. It is protocol-agnostic:
// it knows nothing about HTTP, gRPC, Prometheus, configuration, or UI.
//
// The two seams are [Runner] (what to send) and [RateController] (how fast).
// [Driver] paces the work. [Stats] aggregates the resulting [Result] stream into
// percentile [Snapshot]s.
//
// # Pacing model
//
// [Driver.Pacing] selects between two modes.
//
// [ClosedLoop] (the default, and v0.1's only behaviour) gives each worker a
// rate-limiter token, runs the work, and only then asks for the next token. It is
// self-throttling, but when the target slows the achieved rate sags below the
// offered rate — silently.
//
// [OpenLoop] paces from a single dispatcher that never blocks on the target. A unit
// that finds no free worker is delivered immediately as a [Result] whose Err matches
// [ErrSaturated], so saturation is counted rather than absorbed. Workers becomes the
// maximum in-flight cap. The dispatcher sleeps once per unit, so above roughly
// 4,000 rps it becomes the bottleneck itself; see the README's measured accuracy
// table.
//
// Compare [Snapshot.RPS] against the rate you asked for on every run, in either mode.
//
// # Raw and corrected percentiles
//
// Every Result produced by a Driver carries [Result.Scheduled], the time the unit was
// due. The schedule is anchored: Scheduled is the run's origin plus one interval per
// unit dispatched, advanced whether or not the generator got there on time, so a late
// generator is measured against the schedule rather than redefining it.
//
// [Stats] reports raw percentiles (P50/P95/P99, from when work started) alongside
// coordinated-omission-corrected ones (CorrectedP50/P95/P99, from when the work was
// due). Read them as a pair: a large gap means the generator queued, and the raw
// numbers understate what a real client would have suffered by roughly that much.
//
// For a run being watched while it happens, use RollingStats instead of Stats.
// It records into both a lifetime aggregate and a ring of trailing buckets, so
// Snapshot keeps its cumulative meaning while Window reports only the recent
// past. The distinction matters most for Snapshot.MaxScheduleLag, which is a
// lifetime maximum: one early stall pins it for the rest of the run, and a
// target that stops answering never moves any cumulative number at all, because
// there are no new Results to move it.
//
// // Recorder names what Stats and RollingStats both do. LabeledStats splits a
// Result stream on one Result.Labels key into a child Recorder per value plus a
// total, so a Mix of endpoints reports per-endpoint percentiles instead of one
// aggregate that describes no endpoint. It is generic over the child, so
// LabeledStats[*RollingStats] gives a trailing window per endpoint.
//
// A series is a whole aggregate, so cardinality is capped: past MaxSeries
// distinct values, Results land in a single overflow series. Price a
// configuration with Bytes before paying for it.
//
// # Diagnosing a run that fell short
//
// [Snapshot.Saturated] counts units the target had no free worker for;
// [Snapshot.MaxScheduleLag] is how far the generator itself fell behind. Saturation
// with no lag means the target could not keep up. Lag with no saturation means
// metronome could not — lower the rate, or use ClosedLoop.
package metronome
