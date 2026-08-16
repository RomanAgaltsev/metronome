package metronome

import (
	"math"

	"golang.org/x/time/rate"
)

// minRPS is the floor the Driver applies to any controller-reported rate. It is
// not zero: a limiter with limit 0 hands out no tokens at all and cannot be
// resumed by a later SetLimit, so "paused" is expressed as one token roughly
// every 10,000 seconds instead.
const minRPS = 0.0001

// sanitizeRate maps a RateController's reported rate onto a limit the limiter
// can honour.
//
// NaN as the dangerous case and the reason this function exists: Go's max
// propogates NaN, so a naive max(rps, minRPS) floor does not catch it and
// rate.Limit(NaN) makes every Wait and Reserve return immediately - the Driver
// stops pacing and floods the target. A control loop that divides by an empty
// PromQL vector produces exactly that. NaN is therefore treated as "paused",
// the safe direction to fail.
//
// +Inf is honoured as rate.Inf: "as fast as possible" is a legitimate request
// (it is what the overhead benchmark asks for) and, unlike NaN, it is explicit.
func sanitizeRate(rps float64) rate.Limit {
	switch {
	case math.IsNaN(rps):
		return rate.Limit(minRPS)
	case math.IsInf(rps, 1):
		return rate.Inf
	case rps < minRPS: // covers zero, negatives and -Inf
		return rate.Limit(minRPS)
	default:
		return rate.Limit(rps)
	}
}
