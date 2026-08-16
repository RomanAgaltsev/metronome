# Changelog

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
