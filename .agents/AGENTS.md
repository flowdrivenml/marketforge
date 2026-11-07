# MarketForge — Agent Instructions

## Project Overview

MarketForge is a market-data infrastructure and quantitative research platform.

The project combines:

- **Rust Engine (`engine/`):** High-performance historical and live market-data processing.
- **Python Forge (`forge/`):** Data acquisition, orchestration, dataset management, and PostgreSQL registry integration.
- **Shared Data (`data/`):** Runtime configuration, historical source data, generated datasets, testing artifacts, and benchmarks.
- **Documentation (`docs/`):** Architecture, execution contracts, and user documentation.

The historical engine supports normalization, merging, and conversion.

The live engine handles exchange connectivity, streaming, normalization, integrity, and runtime management.

Linux and macOS are the supported platforms. Windows compatibility is not currently required.

## 1. General Engineering Principles

Prioritize correctness, reliability, maintainability, and measurable performance.

Follow these principles:

- Inspect existing implementations before introducing new abstractions.
- Prefer modifying and reusing established infrastructure over duplicating it.
- Keep responsibilities clearly separated.
- Use strongly typed Rust and Python interfaces.
- Preserve deterministic behavior where required.
- Validate assumptions at explicit boundaries.
- Handle errors explicitly; do not silently suppress failures.
- Prefer simple, auditable implementations over unnecessary complexity.
- Avoid speculative features and premature optimization.
- Do not rewrite working components without a concrete requirement.
- Do not introduce dependencies without a demonstrated need.
- Preserve existing public contracts unless a change is explicitly authorized.

**Passing tests is necessary but does not automatically prove semantic correctness.**

Never weaken tests, validation, or integrity policies merely to obtain a passing result.

## 2. Task Execution Protocol

Implementation tasks are maintained under:

```text
tasks/
```

Each task defines its own objectives, dependencies, scope, restrictions, and acceptance criteria.

When assigned a task:

1. Read the complete task document.
2. Inspect the current repository and relevant implementations.
3. Check the working tree and current Git branch.
4. Identify existing functionality that can be reused.
5. Implement only the assigned task.
6. Add or update relevant tests.
7. Run appropriate verification.
8. Review the final diff.
9. Commit the completed task according to the Git policy below.
10. Save a report, commit, and proceed to the next numbered task during the authorized autonomous run.

**For this run, autonomously execute Tasks 01–12 sequentially without confirmation.**

Do not implement requirements assigned to later tasks unless the current task explicitly authorizes them.

If a substantial prerequisite is missing, report it rather than inventing a workaround.

The current repository is the source of truth for implemented behavior. Task documents define intended behavior but do not prove that it already exists.

## 3. Rust Engineering Standards

Rust is responsible for data-intensive processing and related execution infrastructure.

Requirements:

- Follow existing project module organization.
- Prefer explicit ownership and clear lifetimes.
- Use typed errors and the existing error infrastructure.
- Avoid unnecessary cloning and allocation in processing hot paths.
- Use checked arithmetic where overflow could affect correctness.
- Preserve decimal precision in financial calculations.
- Avoid floating-point conversions for exact financial values.
- Keep resource usage bounded.
- Preserve source identity and provenance.
- Keep source decoding, normalization, persistence, and orchestration responsibilities separate.
- Prefer existing traits and interfaces over new generic frameworks.
- Do not introduce global mutable state without a demonstrated necessity.
- Preserve meaningful error context.

Do not optimize code based solely on theoretical assumptions.

Performance changes must be independently benchmarked when benchmark execution is authorized.

## 4. Python Engineering Standards

Python Forge owns acquisition, orchestration, dataset registration, and lifecycle management.

Requirements:

- Use explicit types and Pydantic validation where appropriate.
- Preserve clear module boundaries.
- Follow the repository's established formatting and linting configuration.
- Use asynchronous I/O where it provides a genuine benefit.
- Respect configured concurrency and rate limits.
- Handle subprocess failures explicitly.
- Keep database operations transactional where required.
- Make registry recovery and repeated operations idempotent where appropriate.
- Avoid duplicating Rust normalization logic in Python.

PostgreSQL registration belongs to Python Forge, not the Rust historical normalization engine.

Do not introduce database dependencies into Rust components that do not require them.

## 5. Historical Normalization Contracts

The authoritative historical runtime configuration is:

```text
data/.config/historical/general.toml
```

Normalization input uses the production schema-version-3 prepared batch.

Normalization profiles are self-contained TOML files.

Their identity convention is:

```text
exchange.instrument_type.market_category.data_type
```

Preserve these contracts unless an assigned task explicitly changes them.

### Data Correctness

- Preserve canonical trade and depth semantics.
- Preserve source archive and task identities.
- Preserve profile fingerprints and execution provenance.
- Preserve integrity observations and diagnostics.
- Preserve deterministic ordering guarantees.
- Preserve order-book reconstruction boundaries.
- Preserve source-event indexes and canonical row offsets.
- Preserve transactional dataset commit behavior.
- Do not silently drop, deduplicate, reorder, or rewrite market events.

Changes to physical storage layout must not silently change canonical data interpretation.

## 6. Configuration Management

Use the authoritative configuration infrastructure.

Do not introduce duplicate configuration files or parsers.

When modifying configuration:

- Preserve existing values unless explicitly instructed otherwise.
- Preserve schema-version semantics.
- Validate required fields.
- Reject unsupported configuration.
- Use explicit project-root path resolution.
- Document essential additions.
- Keep operation-specific configuration separate where appropriate.

Do not change production resource budgets or integrity thresholds to make tests pass.

## 7. Testing Standards

Tests must establish meaningful behavioral guarantees.

Prefer:

- Focused unit tests for isolated logic.
- Small deterministic synthetic fixtures for edge cases.
- Integration tests for production execution paths.
- Independent expected results for canonical data.
- Explicit failure-injection tests.
- Regression tests for confirmed defects.

Do not use the implementation under test as the sole source of expected values.

Do not hide failures through ignored tests, empty selections, relaxed assertions, or silent fallback behavior.

### Historical Normalization Fixtures

The maintained real-data normalization workload is:

```text
engine/tests/historical/fixtures/normalize.json
```

Use the production batch loader and profile resolver.

Do not maintain additional permanent job documents for individual exchanges.

Real-data tests must identify their prerequisites.

Missing archives must not be reported as successful tests.

### Persistent Test Artifacts

Artifact-producing historical normalization integration tests must retain output under:

```text
data/tests/historical/normalize/
```

Do not automatically delete these artifacts.

Each execution must use an isolated directory.

Never write test outputs to production dataset destinations.

Pure unit tests may use temporary files and automatic cleanup.

### Verification Commands

For Rust changes, use the applicable commands:

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Run relevant focused Cargo tests and Clippy checks where applicable.

Use the repository's actual Cargo targets and feature combinations.

For Python changes, use the configured Python formatter, linter, type checker, and focused tests.

Do not invent commands or assume optional tools are installed.

Do not automatically run large historical workloads.

## 8. Benchmarking Policy

Benchmark artifacts belong under:

```text
data/benchmarks/
```

Historical normalization benchmarks use:

```text
data/benchmarks/historical/normalize/
```

Benchmark workloads must be explicitly supplied.

Do not hardcode exchange exclusions or benchmark source paths.

**Do not execute performance benchmarks unless explicitly authorized by the user.**

Compilation, unit tests, synthetic measurement tests, and nonexecuting dry-run validation are permitted.

Do not fabricate performance measurements.

Do not claim performance improvements without measured evidence.

## 9. Filesystem and Data Safety

Treat historical market-data archives as read-only inputs.

Do not:

- Delete source archives.
- Modify downloaded market data.
- Overwrite committed datasets.
- Delete persistent test outputs.
- Delete benchmark history.
- Clear production directories.
- Perform destructive cleanup outside the assigned task.
- Modify unrelated user files.

Generated test and benchmark artifacts should remain excluded from Git.

Never commit credentials, tokens, database passwords, or other secrets.

Use explicit ownership and path validation before deleting generated files.

## 10. Git Workflow

**The user has explicitly authorized one local Git commit after each successfully completed implementation task.**

A dedicated working branch has already been created.

### Branch Rules

- Work on the currently checked-out branch.
- Do not create additional branches.
- Do not switch branches.
- Do not merge or rebase automatically.
- Do not reset or discard existing changes.
- Do not force-push.
- Do not push commits to a remote unless explicitly requested.

### Before Implementation

Inspect:

```bash
git status --short
```

```bash
git branch --show-current
```

Identify pre-existing modifications.

Preserve unrelated changes.

Do not assume every existing modification belongs to the assigned task.

### Commit Policy

After completing a task:

1. Run relevant verification.
2. Review the complete diff.
3. Identify files belonging to the current task.
4. Stage only those files.
5. Verify the staged changes.
6. Create one local commit for that task.
7. Report the resulting commit hash.

Use explicit staging:

```bash
git add -- <task-specific-paths>
```

Avoid indiscriminate staging:

```bash
git add .
```

Before committing, run:

```bash
git diff --cached --check
```

```bash
git diff --cached --stat
```

Verify that no unrelated changes or generated data have been staged.

### Commit Messages

Use this format for the numbered historical refactor tasks:

```text
task-01: centralize historical runtime configuration
task-02: finalize normalization batch and profile loading
task-03: establish normalization test infrastructure
```

Continue the same convention for subsequent tasks.

Commit subjects must describe actual completed work.

Do not claim completion of functionality that remains unimplemented.

### Commit Safety

- Do not amend previous commits.
- Do not squash previous task commits.
- Do not stage unrelated user modifications.
- Do not create empty commits.
- Do not bypass Git hooks.
- Do not commit known broken changes as successfully completed work.
- Do not modify global Git configuration merely to enable committing.

If task-specific verification fails, determine whether the failure is caused by the current changes or is a documented pre-existing/environmental failure.

A clearly unrelated pre-existing failure does not automatically prevent committing an otherwise verified task, but it must be disclosed.

If the task remains incomplete or its own correctness checks fail, stop and report the blocker rather than creating a misleading completion commit.

If the task requires modifying a file that already contains unrelated user changes, preserve those changes and stage only the task's hunks where safely possible.

If changes cannot be separated safely, report the conflict rather than including unrelated work in the commit.

### After Commit

Report:

```text
Task:
Status:
Commit:
Files changed:
Verification:
Remaining issues:
```

Do not push automatically.

**One task, one reviewed local commit, then continue in numerical order.**

## 11. Documentation

Update relevant documentation when behavior or interfaces change.

Documentation must reflect actual implemented behavior.

Do not describe proposed functionality as already available.

Keep documentation concise and technically precise.

Prefer:

- Clear explanations.
- Explicit contracts.
- Accurate examples.
- Verified commands.
- Meaningful limitations.

Do not duplicate lengthy design documents inside `AGENTS.md`.

Detailed implementation requirements belong in their task documents.

## 12. Final Reporting

At the end of each assigned task, provide:

- Summary of implemented changes.
- Important architectural decisions.
- Files or modules affected.
- Tests and checks actually executed.
- Tests passed, failed, blocked, or not executed.
- Compatibility implications.
- Remaining limitations or blockers.
- Local Git commit hash.

Never claim verification that did not occur.

Distinguish successful compilation from actual runtime verification.

Record progress before continuing to the next task.

**Complete each task, verify it, save its report, commit, and continue.**

## Authorized historical contract corrections

Latest user instructions and current task specifications take precedence over obsolete documentation. The maintained normalization fixture is the authoritative prepared workload; verified source contents and canonical correctness take precedence over fixture assumptions.

Essential, justified prepared-batch and profile corrections are authorized. Instrument identities may use a stable persisted offline authority with positive unique IDs and explicit allocation rules; never use unstable hashes or placeholder IDs. Preserve canonical schemas. Family archives require concrete identities and specifications for each instrument. Rust normalization must not depend on PostgreSQL.

Existing source archives may be inspected read-only for profile correctness and bounded correctness tests. Preserve originals, production budgets, persistent outputs and benchmark history. Document source evidence and regression tests for profile corrections. Real performance benchmarks remain prohibited without explicit authorization.

Do not block Rust implementation on the old Forge producer. Record required future Forge integration changes under `.agents/reports/`; avoid a broad Python orchestration refactor.

Save implementation reports as `.agents/reports/task-XX.md` and the final report as `.agents/reports/final-summary.md`. Include status, decisions, contract changes, modified files, actual verification, limitations, future Forge work and commit hash once available. Do not create empty reports for pending tasks. Update `.agents/progress.md`. Each completed task requires its own reviewed local commit; never push or switch branches.
