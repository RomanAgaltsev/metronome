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
// due. [Stats] reports raw percentiles (P50/P95/P99, measured from when work started)
// alongside coordinated-omission-corrected ones (CorrectedP50/P95/P99, measured from
// when the work was due).
//
// In this version the correction is inert under rate sag: Scheduled is derived from
// the pacer's arrival time plus the limiter's outstanding delay, so a late pacer finds
// that delay zero and stamps Scheduled onto Start. The corrected percentiles then
// equal the raw ones in exactly the case that should separate them. Do not rely on
// Corrected* in v0.2 — use the gap between [Snapshot.RPS] and your offered rate as the
// sag signal instead. Anchoring the schedule is the subject of v0.3; see the README.
package metronome
