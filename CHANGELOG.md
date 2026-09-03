# Changelog

## [0.7.0](https://github.com/RomanAgaltsev/metronome/compare/v0.6.1...v0.7.0) (2026-09-03)


### Features

* recorder combinators — Multi, Filter, After, AfterTime, AfterN, Drain ([#29](https://github.com/RomanAgaltsev/metronome/issues/29)) ([92f2ea0](https://github.com/RomanAgaltsev/metronome/commit/92f2ea00897b8f05cbe66b2e492c611eb9abca81))

> **On the version number.** This surface was first published in **v0.6.1**, which
> release-please cut as a *patch*. The commit that added it ([#25]) carried a body line
> its parser could not read, so the commit was discarded whole and the bump was computed
> from the remaining `fix:` alone. v0.6.1 and v0.7.0 therefore contain the same public
> API; v0.7.0 is the release that names and documents it, and is the one to depend on.
> A CI check now rejects the body shape that caused it.

[#25]: https://github.com/RomanAgaltsev/metronome/pull/25

#### Recorder combinators — fan out, filter, and exclude a warmup

v0.6 named the thing that receives a `Result`. This is the small algebra over that
name: five pure `Recorder → Recorder` transforms plus the canonical drain loop, none
of which owns a goroutine, a channel or a clock.

```go
// Multi fans each Result to every recorder, in argument order.
func Multi(recs ...Recorder) Recorder

// Filter records only the Results for which keep returns true.
func Filter(keep func(Result) bool, rec Recorder) Recorder

// After skips Results scheduled within d of the run's start. Warmup exclusion.
func After(d time.Duration, rec Recorder) *Skipper

// AfterTime skips Results scheduled before t.
func AfterTime(t time.Time, rec Recorder) *Skipper

// AfterN skips the first n Results it sees.
func AfterN(n int64, rec Recorder) *Skipper

// Drain records every Result from ch until ch is closed.
func Drain(ch <-chan Result, rec Recorder)
```

The problem they solve is one problem wearing two hats. **Warmup pollutes every
threshold** — cold connection pools, TLS handshakes that will be reused, an empty
page cache and an unwarmed target all land in the histogram a `p99 < 200ms`
assertion is evaluated against, and `Phased` does not help because it changes the
*load*, not the *measurement*. **And a second reader of the result stream cost
hand-rolled fan-out in every caller.** Both are "the caller wants a `Result` to
reach more than one place, or fewer than all places, and has no vocabulary for
saying so."

```go
report  := metronome.NewStats()
byRoute := metronome.NewLabeledStats(metronome.Labeled[*metronome.Stats]{
	Key: "endpoint", New: metronome.NewStats,
})

measured := metronome.After(10*time.Second, metronome.Multi(report, byRoute))
metronome.Drain(d.Run(ctx), measured)

fmt.Printf("measured %d, excluded %d as warmup\n",
	report.Snapshot().Count, measured.Skipped())
```

Order is visible in the expression and both readings are valid:
`After(w, Multi(a, b))` is two views of one warmed population;
`Multi(After(w, a), b)` is a warmed report beside an unwarmed breakdown.

#### `Skipper.Skipped()` makes an exclusion auditable

`Snapshot().Count + Skipped()` is the whole population the `Skipper` was offered.
"Measured 4,500 of 5,000 — 500 excluded as warmup" is the honest line; "4,500
requests" alone invites the reader to wonder where the rest went, and a threshold
evaluated over a silently reduced population is exactly what this exists to prevent.

It is **not** a `Snapshot` field: `Stats`, `RollingStats` and `LabeledStats` never
skip anything, so it would be zero in every existing use and non-zero only when
something upstream — which a `Snapshot` cannot see — happened to be a `Skipper`.
`Snapshot` describes what was recorded; `Skipper` describes what was not.
`Skipper.Snapshot()` delegates, so a `*Skipper` substitutes for the recorder it
wraps everywhere except where the count is wanted.

#### Warmup anchors on the schedule, not on a clock

`After(d)` takes its t=0 from the **first `Result`'s `Scheduled` stamp**, falling
back to `Start`. Three sources were available and only one is right: a `Clock`
captured at construction silently shortens the warmup by however long program setup
took, and `Result.Start` alone is when the generator got around to the unit, which
under sag is not when it was due. This is the v0.3 principle a third time — *measure
against the schedule, not against arrival* — and it removes the `Clock` dependency
entirely, so every test of the feature is deterministic without a `ManualClock`.

Two consequences that are properties rather than bugs, both documented:

- **Admission is per-`Result`, not a switch flipped once.** `Result`s arrive off a
  channel fed by N workers, so `Scheduled` is not monotonic: a unit due at 9.9s can
  arrive after one due at 10.1s. Filtering on each `Result`'s own stamp excludes it
  correctly, where a one-way switch would admit it.
- **The anchor can be up to one reordering window late**, since the first `Result`
  seen need not be the earliest scheduled. Negligible against a warmup measured in
  seconds; `AfterTime` is the exact form for a caller who knows the origin.

A `Result` carrying neither stamp cannot be placed on the timeline, so it is skipped
and counted and does not set the anchor. Under a `Driver` that never happens — the
`Driver` stamps `Start` even when a `Runner` forgets.

#### ⚠️ `Multi` is synchronous, and a slow recorder can look like a target failure

`Multi` calls each recorder in argument order on the calling goroutine. That is the
right default and it has a sharp edge worth stating plainly:

**Time spent in `Record` is time not spent receiving from the result channel.** A slow
recorder fills the channel, then the `Driver`'s delivery backlog, and in open loop the
symptom is `ErrSaturated` — which reads as "the generator ran out of workers" and is
counted inside `Errors` and `ErrorRate`. So **a slow JSON or file writer inside a
`Multi` can present itself as a *target* problem.**

Everything in a `Multi` should be an in-memory aggregate. Anything doing I/O belongs
behind your own buffered goroutine, where you own the policy for what happens when it
falls behind. (This is why an `Async` recorder is rejected rather than provided: its
full-buffer policy is a fork with no good answer — blocking recreates the hazard it
exists to remove, dropping silently corrupts every statistic downstream of it.)

Two more `Multi` notes: every recorder receives the same `Result`, and `Result` holds
`Labels` by reference, so treat `Result`s as read-only — a recorder that mutates the
map corrupts what the others see, including the series `LabeledStats` picks.
And `Multi(...).Snapshot()` delegates to the **first** recorder, because there is no
non-arbitrary aggregate across recorders measuring different populations; it exists to
satisfy `Recorder` so a `Multi` can nest inside an `After`, not to be read.

#### `Drain` deliberately takes no context

metronome's contract on the result channel is blunt — abandoning a live one leaks the
units still in flight for the lifetime of the process — so a `Drain` that returned
early on cancellation would be a leak generator wearing the shape of good practice.
Cancel the `Driver`'s context instead: it stops, closes the channel, and `Drain`
returns because the range ended.

#### `Phased.PhaseEnd` and `Phased.Duration`

```go
func (p Phased) PhaseEnd(i int) time.Duration // sum of phases 0..i; panics out of range
func (p Phased) Duration() time.Duration      // when the last phase ends; 0 if none
```

A convenience, not a new capability — `Phased.Phases` is already exported. What they
buy is one source of truth for a warmup boundary: `After(rate.PhaseEnd(0), stats)`
cannot drift from the phase table, where `After(30*time.Second, ...)` sitting next to
`Phase{Duration: 30 * time.Second}` is two places to change and only one of them fails
loudly. `Duration()`'s doc records that a `Phased` controller is a *measurement*
boundary, not a stop condition: `Rate` holds the last phase's rate past it, and a
`Driver` runs until its context ends or `MaxRequests` is reached.

## [0.6.1](https://github.com/RomanAgaltsev/metronome/compare/v0.6.0...v0.6.1) (2026-09-03)


### Bug Fixes

* remediate the v0.6 + v0.7 review findings ([#26](https://github.com/RomanAgaltsev/metronome/issues/26)) ([c419132](https://github.com/RomanAgaltsev/metronome/commit/c419132c8f0ede73a1b1811b0fd992921f32ba1e))

#### `After(0)` no longer skips Results

`After(0)` anchored on the first `Result` it saw and then admitted only stamps at or
after that anchor — so with out-of-order arrival, which is routine under N workers, a
`Result` scheduled *earlier* than the first one seen was excluded from a warmup of zero
length. A `--warmup` flag left unset therefore dropped a few `Result`s **and** reported
a non-zero "excluded as warmup" count for a warmup the caller never asked for.

A zero window has no boundary, so there is nothing to anchor: `After(0)` now admits
every placeable `Result` and never anchors. The placement rule is unchanged — a
`Result` carrying neither `Scheduled` nor `Start` is still skipped and counted, at
`d == 0` as everywhere else, because it cannot be put on the timeline at all.

Found by the 2026-09-03 comprehensive review. Nothing else changes; `After(d)` for
positive `d`, `AfterTime` and `AfterN` are unaffected.

### Documentation

- The **README's per-endpoint and rolling-window sections rendered inside a code
  block** on GitHub and pkg.go.dev: two ` ```go ` fences were never closed, and a
  closing fence may not carry an info string, so everything from the warmup example
  through the measured-accuracy adherence table was swallowed. Both are closed, and
  the sections are back in the order they document (rolling window → memory →
  per-endpoint → warmup → accuracy).
- The **v0.6.0 CHANGELOG entry has been backfilled** with the API list, the
  cardinality memory table and the measured benchmark figures its spec required and
  the squash merge dropped. See the note at the end of that section.
- **`Result.Labels`' doc comment no longer says the breakdown does not exist.** It
  told callers to key their own map off `Result`s — the exact sentence v0.6 was
  written to make false — and shipped unchanged in v0.6.0.
- The README carries the `Multi` backpressure hazard, the `LabeledStats` cardinality
  table (100 rolling series is ~320 MiB), and rows in "What's in it" for the whole
  v0.6 and v0.7 surface.
- `ExampleAfter` now demonstrates `After`; the old example, which used `AfterN`, is
  `ExampleAfterN`. `ExampleLabeledStats` now shows the motivating defect — the total's
  P99 is the slow endpoint's alone, while three quarters of the traffic is 16× faster.
- `Labeled.MaxSeries` documents that the `__none__` series counts against the cap and
  `__overflow__` does not, and `LabeledStats.Record` documents that the
  series-reconcile-with-total invariant holds at rest rather than at every instant.
- All benchmark figures in the README were re-measured in one sitting, so they are
  internally consistent. `Stats.Record` reads 201 ns/op where it previously read 147;
  nothing regressed — the two numbers came from different sessions on the same machine.


## [0.6.0](https://github.com/RomanAgaltsev/metronome/compare/v0.5.0...v0.6.0) (2026-09-03)


### Features

* labeled stats and the Recorder seam ([#23](https://github.com/RomanAgaltsev/metronome/issues/23)) ([c26cbd6](https://github.com/RomanAgaltsev/metronome/commit/c26cbd61cb3de9e90665042ebe0af0a3cd6ddcd1))

#### `LabeledStats` — per-endpoint percentiles instead of one number no endpoint exhibits

`Result.Labels` has existed since v0.1 and was aggregated by nothing, so a `Mix` of ten
endpoints reported a single P99 that describes no real client's experience. `LabeledStats`
splits the stream on one `Result.Labels` key into a child `Recorder` per value, alongside a
real total:

```go
stats := metronome.NewLabeledStats(metronome.Labeled[*metronome.Stats]{
	Key: "endpoint", New: metronome.NewStats,
})
stats.Snapshot()                    // the total
stats.Series()["search"].Snapshot() // just that endpoint
```

`Snapshot()` is **exactly** what a plain `Stats` fed the same stream would have produced, so
existing code swaps a `*Stats` for one of these and its numbers do not move. Every `Result`
lands in the total and in exactly one series, so `Σ Series().Count == Snapshot().Count`
always — the breakdown is a decomposition, not a sample.

It is **generic over the child**, which is what recovers typed access to v0.5's trailing
window: `Labeled[*RollingStats]` gives `Series()["search"].Window()` with no type assertion.
This is the package's first use of generics; the go directive is unchanged at 1.26.

**Cardinality is capped by construction.** A series is a whole aggregate — ≥272 KiB as a
`*Stats`, ~3.2 MiB as a `*RollingStats` — so a label value that turns out to carry a request
ID rather than a route name would be an OOM, not a reporting problem. Past `MaxSeries`
(default 100) further values share an `__overflow__` series; `Result`s with no usable value
for the key go to a named `__none__` series rather than being dropped. Series are lazy, so
four endpoints allocate six children, not 102.

| Child | Per series | 10 series | 100 series (the default cap) |
|---|---|---|---|
| `*Stats`, default range | 272 KiB | ~3.3 MiB | ~27 MiB |
| `*RollingStats`, `Rolling{}` | ~3.2 MiB | ~38 MiB | **~320 MiB** |

Nesting (`LabeledStats[*LabeledStats[*Stats]]`) typechecks and multiplies that by both caps.
The doc comment says so. A second dimension almost always wants a second `LabeledStats` fed
the same stream through `Multi` (v0.7).

#### Also new

- **`Recorder`** (`Record` + `Snapshot`) — a name for what `*Stats`, `*RollingStats` and
  `*LabeledStats` already do, so a caller can be handed "something that records". `Window` is
  deliberately not on it: it is meaningful for one implementation, and the generic design
  above is what recovers it without widening the interface. `Bytes` is not on it either — a
  `Recorder` that writes CSV has no meaningful size to report.
- **`Stats.Merge`** — folds one `Stats` into another, so a `LabeledStats` breakdown can be
  rolled up into an ad-hoc subtotal ("all the write endpoints"), per-worker aggregates can be
  combined, and separate runs can be added together. Percentiles cannot be combined after the
  fact, so it merges the underlying histograms. Both `Stats` must share a range and
  significant figures; it panics otherwise, and on a self-merge. Locks are taken in
  construction order, so `a.Merge(b)` racing `b.Merge(a)` terminates rather than deadlocking.
- **`Stats.Bytes`**, **`RollingStats.Bytes`**, **`LabeledStats.Bytes`** — the histogram memory
  actually held. `LabeledStats.Bytes` returns **-1**, not 0, when the child type does not
  report its own size: zero is a plausible-looking answer that would read as "free".
  `RollingStats.Bytes()` equals the `Rolling.Bytes()` v0.5 ships for the same config, and a
  test pins that rather than trusting two derivations of one formula to stay in step.

Nothing was removed or changed. `Stats`, `RollingStats`, `Driver`, `Result` and `Snapshot`
behave identically.

#### What the breakdown costs

Measured on an AMD Ryzen 5 3600, `-benchtime=2s -count=5`, `b.RunParallel` at GOMAXPROCS=12,
median of five. Every row records the **identical** `Result`, with the label maps built
outside the measured loop, so the delta is the breakdown and nothing else:

| | ns/op | allocs |
|---|---|---|
| a flat `Stats`, same Results | 107 | 0 |
| `LabeledStats`, 1 series | 389 | 0 |
| `LabeledStats`, 10 series | 342 | 0 |
| `LabeledStats`, 100 series | 352 | 0 |

**A breakdown costs roughly 3.3× a flat `Stats`** — about +250 ns per `Result`, with zero
allocations. Two aggregates rather than one accounts for half of it; the rest is the label
lookup, the `RWMutex` and the extra lock traffic. Stated as a number rather than as an
assurance that it is cheap: it is not free, and at 350 ns it is still far below any real
request.

The number that matters for the design is the **shape**, not the level: 100 series costs
**0.90×** what 1 series costs. The map does not bind as cardinality grows — with a single
series every goroutine contends on that one child's mutex as well as the total's, and
spreading the traffic across a hundred children removes the child contention. A breakdown
gets cheaper as it gets wider, so the documented fallback (sharding the map by label hash)
stays unbuilt.

Stamping the label is a separate cost and is not counted above, because it is paid by whoever
sets `Result.Labels` whether or not anything reads it. Recording a `Result` with a
one-entry `Labels` map into a plain `Stats` — which ignores it — costs **354 ns against 91 ns
with `Labels` nil: +263 ns and 2 allocations**, for the map alone. That is the measured reason
`Mix` still does not stamp a label of its own, and the reason `Result.Labels` tells you to
leave it nil when unused.

> **Note (added 2026-09-03).** This entry was written by hand after the release. `#23` was
> squash-merged, and release-please renders only the subject line of the resulting commit, so
> the seven commit bodies that carried the material above reached `git log` and nothing else —
> the same trap the `v0.5.0` notes describe. The GitHub release notes are generated once and
> are not regenerated, so they were edited by hand from this text.

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
