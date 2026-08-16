# Changelog

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
