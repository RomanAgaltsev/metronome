# Changelog

## [0.6.0](https://github.com/RomanAgaltsev/metronome/compare/v0.5.0...v0.6.0) (2026-09-03)


### Features

* labeled stats and the Recorder seam ([#23](https://github.com/RomanAgaltsev/metronome/issues/23)) ([c26cbd6](https://github.com/RomanAgaltsev/metronome/commit/c26cbd61cb3de9e90665042ebe0af0a3cd6ddcd1))

## [0.5.0](https://github.com/RomanAgaltsev/metronome/compare/v0.4.0...v0.5.0) (2026-09-02)


### Features

* rolling-window Stats ([#20](https://github.com/RomanAgaltsev/metronome/issues/20)) ([8cc0e0b](https://github.com/RomanAgaltsev/metronome/commit/8cc0e0bce32ae89626219556e369b64df68b6b52))

#### `RollingStats` — the same aggregate over a trailing window

Every `Snapshot` number is cumulative over the whole run. That is right for the report
you print at the end and wrong for anything watching a run while it happens: one stall
in the first second pins `MaxScheduleLag` for the rest of the run, and a target that
stops answering never moves any cumulative number at all, because there are no new
`Result`s to move them.

`RollingStats` records into both views at once — a lifetime `*Stats` and a ring of
bucket `*Stats`. `Snapshot()` keeps exactly the meaning `Stats.Snapshot()` has;
`Window()` returns the same aggregate over the trailing window, dividing by the
duration actually covered rather than the nominal one. Rotation runs on **read as well
as write**, so a stalled run drains the window to zero instead of freezing it at its
last healthy numbers.

Configured by `Rolling` (`Window`, `Buckets`, `Clock`, `Lo`, `Hi`, `Sigfigs`); the zero
value is valid and gives a 10s window in 10 buckets on the wall clock. `Stats` gains no
public surface and behaves identically.

`Rolling.Bytes()` prices a configuration before it is built, and panics on exactly the
configurations `NewRollingStats` panics on. Use it: `Buckets` reads as a resolution knob
and is a memory multiplier — a `RollingStats` allocates `Buckets+2` histogram pairs, so
`Rolling{Buckets: 1000}` is 266 MiB at the default range and 2.0 MiB with the histogram
range narrowed. The README carries the table.

#### `Snapshot` gains a `Window` field

**Additive, and called out rather than left to be discovered**, per the pre-v1 policy on
struct changes. `Snapshot.Window` is the trailing duration the numbers cover: zero on
the cumulative lifetime view — which is what `Stats.Snapshot()` always returns, so it
also distinguishes the two kinds — and positive on one from `RollingStats.Window()`,
where it is additionally the `RPS` denominator. `Snapshot.String()` prefixes
`last 9.7s: ` when it is set, so the two views are never confused in a log.

It is the **first** field of the struct. `Snapshot` is a return value rather than an
input, so no realistic consumer breaks — but an unkeyed composite literal of it would
now fail to compile.

#### What the second view costs

Measured on an AMD Ryzen 5 3600, `-benchtime=2s -count=5`, `b.RunParallel` at
GOMAXPROCS=12 — the cost of choosing `RollingStats` over `Stats`, on the artifact rather
than in a design note:

| | ns/op | allocs |
|---|---|---|
| `Stats.Record` | 147 | 0 |
| `RollingStats.Record` | 223 | 0 |
| **delta** | **+76 ns, 1.5×** | 0 |

`Window()` is a different shape and worth knowing before you wire it to a ticker: it
merges the live buckets' histograms, so it costs roughly `live × countsLen` and is
**independent of how many `Result`s are in them** — 730 µs/op over a full default ring,
2.7 µs/op over a stalled one, 256 B and 2 allocations either way. Poll it at 1–10 Hz
from a control loop, not per request.

## [0.4.0](https://github.com/RomanAgaltsev/metronome/compare/v0.3.2...v0.4.0) (2026-08-16)


### Features

* summarise a run with Snapshot.String() ([#18](https://github.com/RomanAgaltsev/metronome/issues/18)) ([9d148fb](https://github.com/RomanAgaltsev/metronome/commit/9d148fbb2703e2388015e7813ad3d4677070d300))

## [0.3.2](https://github.com/RomanAgaltsev/metronome/compare/v0.3.1...v0.3.2) (2026-08-16)


### Bug Fixes

* bound open-loop delivery and stop the schedule leading dispatch ([#15](https://github.com/RomanAgaltsev/metronome/issues/15)) ([4b1a399](https://github.com/RomanAgaltsev/metronome/commit/4b1a3995eba12ee2c5d3fe6118eb9cedcb9dbde5))

## [0.3.1](https://github.com/RomanAgaltsev/metronome/compare/v0.3.0...v0.3.1) (2026-08-16)


### Bug Fixes

* a zero, negative or NaN rate is a pause you can come back from ([#13](https://github.com/RomanAgaltsev/metronome/issues/13)) ([1b130c5](https://github.com/RomanAgaltsev/metronome/commit/1b130c55be60b68a74e4da98efce733548dc5436))

## [0.3.0](https://github.com/RomanAgaltsev/metronome/compare/v0.2.2...v0.3.0) (2026-08-16)


### ⚠ BREAKING CHANGES

* Result.Scheduled changes meaning. It is now the anchored nominal send time rather than arrival-time-plus-remaining-delay, so Snapshot.Corrected* starts returning values that differ from the raw percentiles. A consumer whose dashboards showed the two as identical will see them diverge -- that is the fix, not a regression. No signatures changed; Snapshot.MaxScheduleLag is additive.

### Features

* anchor the pacing schedule so corrected percentiles mean something ([#11](https://github.com/RomanAgaltsev/metronome/issues/11)) ([6de3081](https://github.com/RomanAgaltsev/metronome/commit/6de3081699c272d55545acb73700ea3aa697ee4b))

## [0.2.2](https://github.com/RomanAgaltsev/metronome/compare/v0.2.1...v0.2.2) (2026-08-16)


### Bug Fixes

* correct Snapshot's counters and open-loop's slot accounting ([#9](https://github.com/RomanAgaltsev/metronome/issues/9)) ([f8ac9e5](https://github.com/RomanAgaltsev/metronome/commit/f8ac9e54e13158febfe3526ee2aba5e958d6a93c))

## [0.2.1](https://github.com/RomanAgaltsev/metronome/compare/v0.2.0...v0.2.1) (2026-08-16)


### Bug Fixes

* correct the released README and package documentation ([#7](https://github.com/RomanAgaltsev/metronome/issues/7)) ([700606a](https://github.com/RomanAgaltsev/metronome/commit/700606a6a8ae9d9d2d45d9e697f0155186092297))

## [0.2.0](https://github.com/RomanAgaltsev/metronome/compare/v0.1.0...v0.2.0) (2026-08-16)


### ⚠ BREAKING CHANGES

* the Clock interface gains Sleep(ctx, d) error. Callers using SystemClock() or NewManualClock() are unaffected; a custom Clock implementation must add the method.

### Features

* pacing correctness, open-loop mode, and v0.1 review remediation ([#5](https://github.com/RomanAgaltsev/metronome/issues/5)) ([f1ee0ed](https://github.com/RomanAgaltsev/metronome/commit/f1ee0ed62e4b797d26523df8b3fd530f47042b29))

## 0.1.0 (2026-08-15)


### Features

* metronome v0.1 load kernel ([#2](https://github.com/RomanAgaltsev/metronome/issues/2)) ([2c14a45](https://github.com/RomanAgaltsev/metronome/commit/2c14a45d371d4ad673f7efd85b301137db9e13fa))


### Miscellaneous Chores

* let Actions open the release PR, and cut 0.1.0 not 1.0.0 ([60f9ee0](https://github.com/RomanAgaltsev/metronome/commit/60f9ee0d64f30e0e3dd992ae9199a6389edfee2d))
