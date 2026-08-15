// Package metronome drives a unit of work at a controlled, live-adjustable rate
// across N workers and measures latency and errors. It is protocol-agnostic:
// it knows nothing about HTTP, Prometheus, configuration, or UI.
package metronome
