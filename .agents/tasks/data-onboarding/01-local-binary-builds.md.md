## Task B01 — Build and Install Approved Rust Binaries Through the Forge CLI

**Suggested task file:** `.agents/tasks/engine-tooling/01-local-binary-builds.md`

**Implementation report:** `.agents/reports/engine-tooling-01-local-binary-builds.md`

**Scope:** Add a Forge CLI command that compiles explicitly supported Rust executables and installs them into the repository-root `.bin/` directory.

**Initial supported executable:** `marketforge-normalize`.

This assignment is independent of external-data registration and normalization-orchestration integration.

## Objective

Provide this workflow:

```bash
mf engine build normalize
```

The command must build the approved normalization executable, verify the resulting artifact, install an executable copy at:

```text
<repository-root>/.bin/marketforge-normalize
```

and report the actual result.

Users should not need to remember Cargo feature combinations, locate build artifacts manually, copy binaries, or run `chmod` themselves.

The implementation must be extensible: enabling another approved executable later should require adding a target definition and its verification coverage, not rewriting the build and installation workflow.

**Build and install binaries only. Do not execute normalization, process datasets, start live services, or run benchmarks.**

## Assignment Boundaries and Concurrent Work

Other agents are working in this repository. Inspect applicable instructions, the current branch, working-tree changes, and staged changes before editing.

Implement only this assignment. Old instructions to continue historical task queues or execute unrelated tasks do not apply here.

Preserve unrelated modifications, including modifications within shared files. Do not reset, stash, revert, change branches, perform broad formatting, or overwrite other agents’ reports.

Do not stage, commit, or push unless separately authorized.

Use the assignment-specific report path above. Do not overwrite shared progress files, existing numbered reports, or `final-summary.md`.

You may inspect `engine/src/bin/`, Cargo manifests, feature declarations, build configuration, and relevant Rust entry points. Inspection does not authorize repairing every executable or reorganizing the engine.

## Inspect the Actual Build and CLI Infrastructure

Inspect the current repository before choosing implementation details:

```text
engine/Cargo.toml
engine/Cargo.lock
engine/src/bin/
Cargo.toml, if a root workspace manifest exists
rust-toolchain or rust-toolchain.toml, where present
.cargo/config.toml and applicable workspace Cargo configuration

forge/cli/
forge/shared/paths.py
forge/startup/
existing executable-resolution helpers
existing build, installation, and subprocess utilities
.bin/, if already present
```

Determine the actual package containing `marketforge-normalize`, its required features, the appropriate default-feature policy, and its safe help/version behavior.

A supplied implementation report records a successful release build using `historical,monitor-tui`. Use that as a starting point, not as a substitute for inspecting the current manifest and executable. :chatgpt-content-reference{index="0"}

Preserve normalization’s existing optional terminal-monitor capability in the installed build where that remains the verified production feature combination.

Do not enable test hooks, real-data test features, or every Cargo feature merely to make compilation succeed.

## User-Facing Commands

Implement the following interface within the existing `engine` command group:

```bash
# Build and install the explicitly configured default target set.
# Initially, this contains normalization only.
mf engine build

# Build and install normalization explicitly.
mf engine build normalize

# Display the planned command, features, and destination without building.
mf engine build normalize --dry-run

# Build using only dependencies already available to Cargo.
mf engine build normalize --offline

# Explicitly limit Cargo compilation concurrency.
mf engine build normalize --jobs 4

# Inspect supported targets and their installed artifacts.
mf engine status
```

Preserve the existing `marketforge` and `mf` entry points. Do not introduce another top-level application executable.

### Command semantics

`build` means **compile, validate, and install**. A successful compilation without successful installation must not be reported as a successful command.

Use release builds initially. Debug builds, arbitrary profiles, cross-compilation, and arbitrary Cargo argument forwarding are outside this task.

No target arguments means the explicit default target list, currently only `normalize`. It must not mean “every binary Cargo discovers.”

Unknown or disabled targets must fail before compilation and show the approved choices. Do not silently ignore invalid names.

`--jobs` must accept only positive integers and control Cargo compilation concurrency. It must not modify historical runtime worker counts or `general.toml`.

`--dry-run` must not compile, download dependencies, create installation directories, change installed binaries, or update manifests. It must remain useful when prerequisites are missing by reporting the unresolved prerequisite alongside the proposed plan.

Help, dry-run, build, and engine status must not require PostgreSQL, database credentials, acquisition configuration, or market-data files.

## Explicit, Extensible Target Catalogue

Create or reuse one small, centrally maintained catalogue of approved build targets.

A target definition should identify:

| Property | Purpose |
|---|---|
| Logical name | User-facing selector, initially `normalize` |
| Cargo manifest/package | Exact source package to build |
| Cargo binary target | Initially `marketforge-normalize` |
| Installed filename | Initially `marketforge-normalize` |
| Feature selection | Explicit approved features and default-feature policy |
| Default selection | Whether ordinary `mf engine build` includes it |
| Smoke-check arguments | Verified non-processing invocation |
| Availability | Whether the target is currently enabled |

Prefer a typed Python definition unless an appropriate project-controlled configuration mechanism already exists. Do not create a plugin framework or user-editable shell-command catalogue.

Initially enable only normalization. Leave merge, conversion, live, benchmark, experimental, and diagnostic executables outside the supported build set.

The presence of a source file or Cargo target is not approval to build or install it.

Inspect Cargo metadata or manifests to validate catalogue entries, but do not automatically turn every discovered binary into an enabled target.

The generic implementation must not contain separate build/copy branches for each executable. A future target should reuse the same planning, compilation, artifact selection, installation, and reporting code.

## Build Planning and Prerequisites

### Project paths

Resolve the repository through existing project-path utilities. Commands must work when invoked from a repository subdirectory, including `research/` or `forge/`.

Use an explicit working directory for subprocesses and absolute manifest/install paths. Do not rely on process-global `os.chdir()`.

The installation directory is:

```text
<repository-root>/.bin/
```

It is independent of the configured market-data storage root.

Keep Cargo caches and intermediate artifacts in Cargo’s build directories. Do not use `.bin/` as a Cargo target directory.

### Toolchain and dependency policy

Respect the repository’s Rust toolchain selection, applicable Cargo configuration, and committed lockfile.

Check for usable build prerequisites and fail with actionable diagnostics when they are unavailable.

Do not install or upgrade Rust, change the selected toolchain, modify system packages, invoke `sudo`, or bootstrap a new Python environment.

Build with locked dependency resolution. Do not silently rewrite `Cargo.lock` or run `cargo update`.

Ordinary builds may allow Cargo to fetch the dependencies required by the existing lockfile. Document this. `--offline` must prohibit that fetching and report missing cached dependencies clearly.

Do not download prebuilt engine releases in this task.

### Native platform only

Support native builds on the project’s Linux/macOS platforms.

Inspect configured target overrides rather than assuming that the compiler host is necessarily the selected build target. Reject an incompatible cross-compilation configuration clearly instead of installing a foreign-target executable as locally runnable.

Do not alter CPU optimization flags, linker settings, release profiles, or production resource budgets as an unsolicited optimization.

## Cargo Invocation and Artifact Selection

Build the selected binary explicitly.

Do not use broad build selections such as:

```text
--bins
--all-targets
--workspace
--all-features
```

Shared libraries and dependencies needed by the selected executable will still need to compile. If those fail, report the actual failure; do not claim that selecting one binary isolates it from all shared-code defects.

Use argument arrays, not shell-composed command strings. Keep the subprocess attached to the invoking operation and supervise its lifetime.

### Find the artifact from the current Cargo invocation

Consume Cargo’s structured build output to identify the executable emitted or confirmed by that invocation.

Match the intended package and binary target. Require an unambiguous executable artifact and successful Cargo completion before installation.

Do not guess the artifact path using:

```text
engine/target/release/marketforge-normalize
```

That path is only a common default. Configured target directories and workspace layouts may differ.

Do not search the filesystem for the newest executable or reuse a pre-existing target-directory file after a failed build.

An artifact reported as fresh by a successful Cargo invocation is acceptable. A stale executable found independently after Cargo fails is not.

Keep output processing bounded. Do not buffer unlimited compiler output in memory.

### Failure and cancellation

Compilation errors, missing artifacts, ambiguous artifacts, and interrupted builds must produce nonzero outcomes.

On interruption, terminate and reap the subprocesses owned by this operation using the established supervision facilities. Do not kill unrelated Cargo processes or leave this command’s build running unattended.

Never delete Cargo lockfiles or invoke `cargo clean` to resolve contention. Preserve incremental build caches.

## Safe Installation into `.bin/`

### Copy the executable, not the build directory

Install only the selected executable and its installation metadata.

Do not copy dependency trees, intermediate artifacts, test executables, or Cargo caches.

Use an independent file copy rather than a symlink or hard link into the Cargo target directory. A later Cargo clean or rebuild must not remove or mutate the installed executable.

Preserve unrelated `.bin/` entries, especially any Python launcher such as:

```text
.bin/marketforge
.bin/mf
```

Do not clear or recreate `.bin/` wholesale.

### Stage, verify, then publish

Copy the artifact to a uniquely owned temporary file on the destination filesystem.

Verify that it is a regular, nonempty file, establish the installed-byte checksum, and assign normal executable permissions, such as `0755` on supported Unix systems.

Do not use recursive permission changes, `chmod 777`, or change source-tree permissions.

Run the approved smoke check against the staged copy before replacing the active executable. Use a timeout and a deliberately selected working directory.

Use `--help` only after verifying that the entry point handles it without starting processing or requiring data services. Use `--version` only if it actually exists. Do not invent a flag or modify normalization behavior merely to satisfy an installer assumption.

A smoke check must not submit jobs, start a TUI, read the historical workload, or produce a dataset.

Publish through same-filesystem atomic replacement of the selected executable.

Build, staging-copy, permission, or smoke-check failure must leave the previously installed executable untouched.

Reject unsafe installation destinations, unexpected symlinks, and non-regular target entries rather than following them outside the managed installation location.

### Installation ownership and concurrency

Use an operating-system-backed lock for this checkout’s binary build/publication workflow, reusing appropriate existing infrastructure.

It must prevent competing invocations from replacing the same executable or losing each other’s manifest updates. It must not require PostgreSQL.

Do not create a second general orchestration scheduler. Do not introduce a daemon or detached build service.

### Metadata and interruption boundaries

Binary replacement and manifest replacement are separate filesystem operations. Do not describe them as one atomic transaction.

Prepare metadata before activation where practical, publish it safely, and make incomplete publication detectable.

If interruption or metadata failure leaves an executable inconsistent with its manifest, status must report that mismatch. Do not mark the installation verified merely because the executable exists.

Retain sufficient information for a subsequent explicit build to repair the managed installation. Do not automatically delete unrelated files or silently adopt an unexplained executable.

## Installation Metadata and Status

Reuse an existing compatible installation manifest, or introduce a small versioned manifest at:

```text
.bin/manifest.json
```

Do not create competing authoritative installation inventories.

For each managed executable, record the relevant build and installation facts:

```text
Logical target
Installed relative filename
Cargo package and binary target
Release profile
Selected features and default-feature policy
Host and effective build target
Cargo/rustc versions
Build and installation timestamps
Installed byte size and SHA-256
Source Git revision, when available
Dirty-working-tree indication
Actual smoke-check result
```

Record unavailable values as unavailable rather than inventing them. A Git commit plus a dirty flag is not an exact snapshot of modified source; do not claim reproducible-build provenance beyond the evidence retained.

Do not record secrets or dump the full process environment.

Preserve unrelated entries in an existing manifest. Rebuilding normalization must not relabel another installed executable as having been rebuilt.

### Status behavior

Extend the existing `engine status` implementation if present; do not create a parallel status system.

For approved targets, show the installed path, availability, executable permission, recorded build details, and integrity state.

Distinguish at least:

```text
Not installed
Installed and matches recorded bytes
Installed but not executable
Manifest missing or unreadable
Executable does not match recorded bytes
```

Status must be read-only. It must not rebuild, install dependencies, change permissions, or execute arbitrary pre-existing files.

Keep these claims separate:

```text
Compiled successfully
Installed successfully
Passed startup smoke check
Compatible with a particular normalization protocol
Successfully processed a dataset
```

This task establishes the first three where verified. It does not establish the last two.

Expose or reuse a small shared resolver that returns the managed executable’s absolute path for future orchestration integration. Do not rewrite normalization orchestration to consume it in this assignment, and do not add automatic build-on-launch behavior.

## Output and Error Reporting

Follow existing CLI presentation conventions.

Ordinary output should identify the selected target, build profile/features, current phase, and final installation path. Show useful compiler diagnostics rather than replacing failures with a generic message.

For global `--json` mode, stdout must contain only the defined result document. Send Cargo progress, diagnostics, and human-readable information to stderr.

Report compilation, validation, and installation outcomes separately. A failed installation after successful compilation must remain an overall failure.

Do not suppress failures in silent mode. Do not report a path as successfully installed unless publication actually completed.

## Tests and Verification

Use focused tests and isolated installation roots. Ordinary tests must not overwrite the user’s real `.bin/` installation or require PostgreSQL.

Cover the following behaviors:

| Area | Required verification |
|---|---|
| CLI | Default target selection, explicit normalization selection, unknown targets, positive job limits, help without service initialization |
| Dry-run | Correct plan with no compilation, network access, installation writes, or manifest changes |
| Build selection | Exact binary/package/features, locked resolution, no broad all-target/all-feature build |
| Artifact handling | Correct structured artifact selection, custom Cargo target directory, fresh artifacts, missing/ambiguous artifacts, failed Cargo with a stale executable present |
| Installation | Independent copy, executable permissions, checksum, successful replacement, unrelated files preserved |
| Failure safety | Copy/chmod/smoke-check failure preserves the old executable; interrupted metadata publication is detectable |
| Concurrency | Competing publishers cannot corrupt binaries or lose manifest entries |
| Process behavior | Cancellation cleanup, useful errors, clean JSON stdout |
| Status | Missing, valid, non-executable, malformed-manifest, and checksum-mismatch states |

Use controlled subprocess fixtures for ordinary failure cases. Where practical, include a tiny temporary Rust package to verify the real Cargo artifact-discovery and copy path without compiling the entire engine in every test.

Also perform one real release build of `marketforge-normalize` through the new CLI, install it through the implemented path, and run its approved non-processing smoke check.

This real build is authorized. A normalization workload is not.

If the current engine cannot compile because of a concurrent or pre-existing defect, preserve the installed binary and report the blocker. Do not repair unrelated binaries or weaken compiler checks to obtain a successful result.

Run applicable Python formatting, linting, type checking, and focused tests. Use scoped Rust checks only if Rust changes were necessary.

Do not run performance benchmarks, full historical tests, exchange requests, or dataset-processing jobs as verification for this task.

## Documentation

Update the existing Zensical site with the build workflow and command reference.

Relevant locations include:

```text
docs/content/getting-started/installation.md
docs/content/development/
docs/content/reference/cli/
docs/zensical.toml
```

Document the exact implemented commands, prerequisites, initial normalization-only support, release features, `.bin/` placement, executable permissions, manifest/status meanings, offline behavior, and rebuild procedure.

Explain how a maintainer enables another binary by extending the approved target catalogue and adding verification.

Clarify that the command neither modifies shell `PATH` nor installs system-wide binaries. Direct invocation remains available through:

```bash
./.bin/marketforge-normalize --help
```

Do not redesign the documentation site or describe this limited build command as a complete installer.

Validate documented commands against the actual CLI and run the configured documentation checks.

## Explicitly Out of Scope

Do not implement prebuilt-release downloading, GitHub release automation, a new `setup.sh`, Python environment installation, PostgreSQL provisioning, system package installation, cross-compilation, or automatic updates.

Do not enable other engine executables, repair their runtime behavior, change normalization input/output contracts, alter profiles, modify canonical schemas, or process market data.

Do not build a general package manager, release-history service, or automatic rollback framework.

Keep this a small, reliable local build-and-install capability.

## Completion Criteria and Report

The task is complete when:

```text
mf engine build normalize
    → builds the approved release target
    → identifies its actual Cargo artifact
    → stages and verifies an executable copy
    → installs it into repository-root .bin/
    → records accurate installation metadata
    → reports success without running normalization
```

Existing installed binaries must survive failures before activation. Unsupported binaries must remain excluded. Adding a future approved target must not require duplicating the workflow.

The report must identify changed files, actual CLI syntax, catalogue design, chosen feature/default-feature policy, installed paths, metadata format, failure behavior, tests executed, real-build/smoke-check results, documentation checks, and remaining limitations.

Distinguish actual verification from intended Linux/macOS support. Do not claim macOS execution was tested on a Linux host.

**Stop after this assignment. Do not continue into data registration, normalization orchestration, or another agent’s task queue.**