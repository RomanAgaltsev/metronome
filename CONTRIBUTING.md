# Contributing to metronome

Thanks for contributing! `metronome` is a Go module at `github.com/RomanAgaltsev/metronome`.

## Prerequisites

- Go 1.26+
- [Task](https://taskfile.dev): `go install github.com/go-task/task/v3/cmd/task@latest`

`task setup` installs the pinned dev tools (golangci-lint, gofumpt, gci) into `./bin`.

## Everyday commands

All commands run through `Taskfile.yml` so local == CI:

```bash
task            # list available tasks
task ci         # full local gate: tidy, vet, lint, race tests
task lint       # golangci-lint (strict ruleset)
task format     # gofumpt + gci
task test       # race + shuffled tests
```

## Commit & PR conventions

- We **squash-merge** PRs. The **PR title** becomes the commit on `main` and drives
  release-please, so it **must** be a
  [Conventional Commit](https://www.conventionalcommits.org/): `feat: ...`, `fix: ...`,
  `chore: ...`, `docs: ...`, `refactor: ...`, `test: ...`, `build: ...`, `ci: ...`,
  `perf: ...`. Scope optional: `feat(engine): ...`.
- Breaking changes: add `!` (`feat!: ...`) or a `BREAKING CHANGE:` footer.
- A `pr-title` check enforces the convention.

## Adding a dependency

`.golangci.yml` uses `depguard` with an allow-list seeded to the standard library and
`github.com/RomanAgaltsev/metronome`. Adding a third-party dependency means adding it to
`linters.settings.depguard.rules.Main.allow` in the same PR, which keeps the dependency
set a deliberate choice rather than an accident.

## Before opening a PR

Run `task ci` and make sure it is green.

## Maintainer setup (one-time)

One step needs repo-admin access and cannot be committed as code:

- [ ] **Codecov:** add the repo at codecov.io and set the `CODECOV_TOKEN` repo secret
  (`Settings → Secrets and variables → Actions`).

Everything else that used to be on this list — branch protection, private vulnerability
reporting, Dependabot alerts, the Actions policy and the merge policy — is declared in
[`.github/keel-settings.yml`](.github/keel-settings.yml) and applied by keel:

```bash
keel settings apply           # converge the remote to the file
keel settings apply --check   # report drift without changing anything (task keel:settings)
```

It needs a token with `administration:write` on this repository. keel converges only the
keys the file names and leaves everything else alone, so it is safe to run against a repo
you have also tuned by hand.
