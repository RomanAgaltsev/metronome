// Package metronome drives a unit of work at a controlled, live-adjustable rate
// across N workers and measures latency and errors. It is protocol-agnostic:
// it knows nothing about HTTP, gRPC, Prometheus, configuration, or UI.
//
// The two seams are [Runner] (what to send) and [RateController] (how fast).
// [Driver] paces the work. [Stats] aggregates the resulting [Result] stream into
// percentile [Snapshot]s.
//
// Pacing model: this version is a closed-loop generator — a worker waits for a
// rate-limiter token, runs the work, and only then asks for the next token. When
// the target slows, the achieved rate sags below the offered rate and latency
// percentiles are subject to coordinated omission (they are optimistic). Compare
// [Snapshot.RPS] against the rate you asked for on every run. See the README.
package metronome
