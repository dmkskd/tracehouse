# Proposal: Vetted platform-specific npm lockfiles

**Status:** proposed, 2026-07-31. Intended as a follow-up to the v0.18.4
release incident; no implementation has been selected yet.

## Problem

The release workflow currently deletes `package-lock.json` before running
`npm install --legacy-peer-deps`. This was introduced because npm had recorded
native optional dependencies differently on macOS and Linux, and a lockfile
created on one platform did not reliably install on the other.

Deleting the lock solves that platform mismatch by resolving the entire
dependency graph again on every release. It also makes releases depend on the
state of the public registry at that exact moment.

The v0.18.4 release exposed the risk:

- The same dependency manifests built successfully roughly three hours earlier.
- `react-stately@3.49.0` was published at 15:25:09 UTC and referenced
  `@internationalized/date@^3.12.3`.
- `@internationalized/date@3.12.3` was published 59 seconds later, at 15:26:08.
- A release install resolving dependencies during that gap failed with
  `ETARGET`.
- A later rerun succeeded without any TraceHouse dependency change.

This was a benign publication-order race in Adobe's React Spectrum monorepo,
but the same mechanism could select an untested, broken, or compromised
transitive release.

## Goal

Make release dependency installation reproducible while retaining the native
optional dependencies required by each build runner.

Specifically:

- Never resolve new dependency versions during a release.
- Vet the Linux and macOS dependency trees before committing them.
- Fail clearly if a runner does not match a known lockfile.
- Keep dependency updates deliberate and reviewable.
- Preserve normal npm integrity and signature verification.

## Non-goals

- Removing `--legacy-peer-deps` as part of this work.
- Fixing existing npm audit findings bundled through Grafana dependencies.
- Maintaining a lockfile for each Rust compilation target. Node dependencies
  are installed for the host runner, not the Rust target.
- Automatically accepting dependency updates from the public registry.

## Proposed model

Maintain two distinct, vetted dependency graphs:

```text
locks/npm/package-lock.linux-x64.json
locks/npm/package-lock.darwin-<runner-arch>.json
```

The final Darwin architecture must be taken from the actual GitHub runner via
`process.arch`; it should not be inferred from the Rust target name.

Before installation, a small selector copies the matching file to the name npm
requires:

```text
locks/npm/package-lock.<platform>-<arch>.json
                         │
                         └── copy to package-lock.json
                                      │
                                      └── npm ci --legacy-peer-deps
```

The selector should:

1. Read `process.platform` and `process.arch` from the same Node runtime that
   will execute npm.
2. Map only explicitly supported pairs to a committed lockfile.
3. Refuse unknown pairs instead of falling back to `npm install`.
4. Print the chosen lockfile and its checksum in CI logs.
5. Never generate or modify a vetted lockfile.

The two Linux binary jobs currently run on an x64 Ubuntu host. The
`aarch64-unknown-linux-gnu` Rust build is cross-compiled, so both Linux jobs use
the Linux x64 npm lock. The macOS job uses the lock matching the macOS runner's
actual architecture.

## Root `package-lock.json`

npm only recognizes a lockfile named `package-lock.json` beside
`package.json`. Custom lockfile paths are not supported for `npm ci`, so a copy
step is unavoidable.

There are two viable layouts to decide between during implementation.

### A. Explicit platform locks are canonical

- Store both vetted files under `locks/npm/`.
- Treat root `package-lock.json` as a generated selection.
- Require the selector before all installs.

This is the clearest representation, but standard tools such as Dependabot and
editors expect a root lockfile. We would need to verify their behavior or keep
a documented generated copy.

### B. Root lock is one canonical platform

- Keep one platform's vetted graph as root `package-lock.json`.
- Store only the other platform under `locks/npm/`.
- Copy the alternate lock on the other runner.

This preserves standard npm tooling but makes the layout asymmetric and makes
it easier to update only one platform accidentally.

**Initial recommendation:** use layout A unless dependency tooling proves it
cannot consume the explicit locks. Clarity and symmetric validation are more
important than avoiding one deterministic copy step.

## Release workflow

Every release job that installs frontend dependencies should use the same
locked install sequence:

```bash
just npm-lock-select
npm ci --legacy-peer-deps
```

The existing `rm -f package-lock.json` and lockless `npm install` steps should
be removed only after both platform locks pass the validation matrix.

`just install` can remain convenient for local dependency development. Release
jobs should call a distinct locked recipe so a future edit to the local recipe
cannot silently reintroduce live dependency resolution.

## Creating and updating the locks

Lock updates must be a separate maintenance workflow, never part of a release.

1. Start from a clean checkout and the repository's pinned Node/npm versions.
2. Generate the Linux lock on the same Linux runner family used by releases.
3. Generate the Darwin lock on the same macOS runner family used by releases.
4. Install each result with `npm ci --legacy-peer-deps` on its native runner.
5. Run frontend tests, core tests, and representative production builds.
6. Run the security checks below.
7. Upload both locks as workflow artifacts for review.
8. Commit them only after reviewing the dependency diff between the old and
   new graphs.

A dependency-update pull request must update both locks. CI should fail if
`package.json` or any workspace manifest changes while either lock remains
incompatible with `npm ci`.

The workflow should record these generation inputs alongside each artifact:

- Node version
- npm version
- `process.platform`
- `process.arch`
- lockfile SHA-256
- generation timestamp

The metadata can live in CI output or a small checked-in manifest. It should
not be hand-edited into the npm lockfiles.

## Validation matrix

Add a lightweight pull-request job for each supported host:

| Runner | Selected lock | Required checks |
|---|---|---|
| Ubuntu x64 | Linux x64 | `npm ci`, tests, frontend build |
| macOS current architecture | Matching Darwin lock | `npm ci`, tests, frontend build |

The job must start without `node_modules` and must not access a lockless install
path. `npm ci` already verifies that each selected lock is compatible with all
workspace manifests.

The matrix should also assert that running the selector does not modify the
selected vetted file. A runner architecture change should fail with an
unsupported-platform error and require an intentional new lockfile.

## Security checks

For each newly generated graph, run:

```bash
npm audit --omit=dev
npm audit signatures
```

`npm audit signatures` should fail on invalid or missing registry signatures.

The production audit currently has known findings inherited mainly through
Grafana packages, so initially it should produce a reviewed report rather than
fail merely because the total is non-zero. The update workflow should compare
the old and new reports and block newly introduced critical findings. A later
policy can baseline specific accepted advisories and block any unreviewed
increase by advisory ID rather than by count.

Review should also call out:

- Newly introduced packages
- Changes to install scripts
- New package maintainers or repositories
- Packages published shortly before lock generation
- Integrity or signature failures

## Operational escape hatch

If a release is blocked and neither vetted lock can install, do not silently
fall back to deleting the lock.

An emergency override may be added to root `package.json` only when:

- the exact version is documented;
- a clean lockless reproduction demonstrates the failure;
- native builds and tests pass with the override;
- the override is marked for removal; and
- a follow-up lock refresh is opened.

This keeps an emergency action visible in source review rather than hidden in
the release environment.

## Rollout

1. Record the actual Node/npm platform and architecture used by every release
   job.
2. Generate candidate Linux and Darwin locks without changing release behavior.
3. Validate both candidates in the CI matrix and compare their dependency and
   audit reports.
4. Add the deterministic selector and locked install recipe.
5. Switch release jobs from lock deletion plus `npm install` to selection plus
   `npm ci`.
6. Document the two-platform update procedure for maintainers.
7. After two successful releases, remove any obsolete lock-generation
   workaround and audit the repository for remaining lockless CI installs.

## Success criteria

- Re-running the same release commit installs the same dependency versions.
- Linux and macOS native optional dependencies install successfully.
- A registry publication race cannot change or break a release build.
- Unknown runner platforms fail before dependency installation.
- Dependency pull requests visibly update and validate both platform graphs.
- No release job deletes a lockfile and resolves an unreviewed dependency tree.

## Open decisions

- Whether explicit platform locks or a root canonical lock should be the source
  of truth.
- The actual architecture of the current `macos-latest` runner and whether it
  is stable enough to name directly.
- Whether to pin the npm version as well as Node 22 for byte-stable lock
  generation.
- Whether dependency tooling can consume locks outside the repository root.
- Whether audit policy should block only new critical findings initially, or
  baseline all currently accepted advisory IDs.
