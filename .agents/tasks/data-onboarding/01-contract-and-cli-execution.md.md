## Task N01 — Align Forge Normalization Preparation with the Current Rust Contract and Implement Complete CLI Execution

**Suggested task file:** `.agents/tasks/normalization/01-contract-and-cli-execution.md`

**Implementation report:** `.agents/reports/normalization-01-contract-and-cli-execution.md`

**Scope:** Correct Forge’s normalization job builder against the current authoritative engine fixture and production Rust loader, then implement the complete preparation → execution → output verification → registration workflow through the Forge CLI.

**Related assignments:** Unified data registration (R01) and approved binary building/installation (B01). Reuse their implemented interfaces; do not reimplement those capabilities here.

## Objective

Make the ordinary Forge normalization workflow produce an executable batch that conforms exactly to the current Rust contract and then execute it through the installed normalization binary.

The intended workflow is:

```text
Normalization recipe
        │
        ▼
Registered raw-input selection
        │
        ▼
Instrument identity, specifications, source metadata, and profile bindings
        │
        ▼
Current-contract normalization batch
        │
        ▼
Native validation
        │
        ▼
.bin/marketforge-normalize
        │
        ▼
Committed datasets and authoritative execution artifacts
        │
        ▼
Verification and PostgreSQL registration
        │
        ▼
Final Forge outcome and recoverable run record
```

Generating a JSON file is not completion of normalization.

A successful command must establish that the requested processing completed, the expected outputs were committed, and their registry records and lineage were persisted.

The central requirement is:

> The production Forge builder must generate the same contract represented by the maintained engine fixture—not an older approximation, a second Python-only wrapper, or a document that merely has the right version number.

## Assignment Boundaries and Concurrent Work

Other agents are modifying this repository. Inspect applicable instructions, the current branch, working-tree changes, and staged changes before editing.

Implement only this assignment. Do not continue old task queues or execute unrelated historical tasks.

Preserve unrelated modifications, including modifications within shared files. Do not reset, stash, revert, change branches, perform broad formatting, or overwrite another agent’s reports.

Do not stage, commit, or push unless separately authorized.

Use the assignment-specific report above. Do not overwrite shared progress records, numbered reports belonging to earlier tasks, or `final-summary.md`.

R01 owns unified raw-data registration and instrument/profile onboarding. B01 owns binary compilation, installation, and executable resolution. Inspect what has actually landed before using either interface.

If a prerequisite is incomplete, continue independently implementable work and report the precise missing integration. Do not create a competing registry, local binary installer, or temporary success-producing stub.

All operational verification must use an explicitly verified testing database and isolated writable destinations. Do not migrate, reset, or modify a shared testing database underneath another active agent.

## Establish the Actual Source of Truth

### Locate and inspect the maintained fixture

Start with:

```text
engine/tests/historical/fixtures/normalize.json
```

The user referred to a similarly spelled filename. Verify the actual filename in the repository; do not create `normalaze.json`, rename the maintained fixture, or introduce another permanent workload document.

Read the complete fixture, including every distinct job/task shape and optional-field combination. Do not inspect only its first job or first task.

Then inspect the matching production interfaces:

```text
engine/src/historical/config/
engine/src/historical/job/
engine/src/bin/marketforge-normalize.rs
engine/src/historical/normalize/
engine/tests/historical/support/
engine/tests/historical/normalize/
data/.config/historical/general.toml
```

Use actual paths where the current organization differs.

Inspect the Rust input types, deserialization attributes, defaults, validation, profile resolution, CLI arguments, response types, manifest types, and existing contract tests.

The fixture is the authoritative maintained workload example. The production loader defines the complete accepted contract and its semantics. Both must agree.

If the fixture and loader disagree, report the exact discrepancy. Do not silently rewrite the fixture or weaken Rust validation to accommodate the old Python producer.

### Do not trust obsolete version descriptions

The supplied Python snapshot emits `schema_version = 3` and retains hard-coded diagnostics stating that Rust cannot execute its batch. Those are specific integration points to replace, not statements to preserve as current engine truth. :chatgpt-content-reference{index="0"}

A later documentation audit reports a version-3 Python producer versus a version-4 Rust prepared-batch loader. Recheck the current implementation rather than hard-coding either version from historical reports. :chatgpt-content-reference{index="1"} :chatgpt-content-reference{index="2"}

Do not assume that these independently versioned documents share the same version number:

```text
Prepared input batch
Resolved processing job
Runtime configuration
CLI execution response
Dataset manifest
Depth reconstruction metadata
Forge preparation/execution report
```

The attached reports are investigation aids, not replacements for the actual fixture and source definitions.

## Audit the Producer–Consumer Contract Before Editing

Inspect the existing Forge implementation, including:

```text
forge/historical/normalization/
forge/historical/engine/
forge/orchestration/
forge/cli/commands/normalization.py
forge/shared/identity.py
forge/shared/paths.py
forge/shared/ownership.py
forge/shared/database/
forge/storage/
forge/markets/
```

Prepare a concise field-level mapping in the implementation report.

For every relevant field, record:

| Item | Required information |
|---|---|
| JSON path | Exact location in the current engine document |
| Shape | Type, nesting, nullability, omission/default rules |
| Meaning | What the engine actually uses it for |
| Authority | Registry, specification version, profile, runtime configuration, or execution allocation |
| Existing producer | Correct, missing, obsolete, or incorrectly derived |
| Correction | Production change required |
| Verification | Contract and behavioral tests covering it |

Include fields required by the loader even when the maintained fixture does not exercise every allowed variant.

Pay particular attention to instrument namespaces and authorities, concrete IDs, family/member bindings, historical specifications, source bounds, partitions/priorities, feed options, ordering, profile expectations, and output identities.

These are inspection targets, not an instruction to invent those exact field names or force every field into the batch. Emit only fields supported by the current contract.

## Correct the Production Preparation Path

### One typed builder

Refactor the existing builder rather than creating another independent serializer.

Separate:

```text
User recipe
    → resolved registered inputs
    → typed preparation plan
    → current Rust wire document
```

The same builder must serve standalone preparation and execution. `normalize run` must not contain a second job-construction implementation.

Use strict boundary validation and exact types. Reject unknown fields and unsupported versions according to the actual contract. Do not let permissive dictionary handling or implicit coercion hide producer errors.

Keep Forge-only diagnostics and registry associations in the preparation report or run receipt unless the Rust input contract explicitly includes them.

Remove obsolete compatibility messages after replacing them with real validation. Do not simply change `engine_ready = False` to `True`.

### Stable identities

Resolve instrument identity through the shared catalogue and the current persisted engine namespace/authority mechanism.

Preserve the distinction between catalogue IDs, canonical engine instrument IDs, run IDs, job IDs, dataset IDs, task IDs, and stream identities.

Do not allocate canonical instrument IDs from task order, filenames, unstable hashes, or fresh enumeration on every run.

Do not assume PostgreSQL IDs and Rust IDs are interchangeable. Retain an explicit mapping where required, and preserve it across preparations and executions.

Reserve output identities through the existing registry/allocation infrastructure before execution. A check that an ID is currently unused is not a durable reservation.

Preserve integers exactly, including IDs above `2**53` and nanosecond timestamps. Validate representability at database boundaries; never truncate, wrap, or convert identifiers through floating point.

### Instrument specifications and family scopes

Resolve the applicable instrument specification version, not merely the latest catalogue row.

Preserve exact decimal representations and provenance. Missing required metadata must produce a diagnostic naming the input, instrument, and missing requirement.

For family or multi-instrument archives, supply the concrete members and member-specific specifications required by the current engine contract. Do not borrow one member’s tick size or contract multiplier for the whole family.

Keep archive/member selection and actual decoding responsibilities consistent with the current Rust resolver. Do not reintroduce Python archive expansion where Rust now owns it.

If authoritative family membership or historical specifications are unavailable, report a preparation blocker. Do not manufacture values, silently drop the affected selection, or mark the reduced batch as representing the original request.

### Source metadata, ordering, and provenance

Consume the common registered-input interface for both exchange-acquired and locally registered data.

Freeze the selected local-copy identities, paths, checksums, source/feed identity, specification references, and applicable profile bindings. Do not rediscover inputs midway through execution.

Keep these concepts separate:

```text
Requested selection interval
Declared source coverage
Observed source event bounds
Source partition/order evidence
Execution priority or stream rank
```

Implement the exact inclusive/exclusive endpoint and unit semantics defined by the loader. Do not mechanically subtract one nanosecond from an exclusive coverage endpoint and present the result as an observed maximum timestamp.

Task IDs, path sorting, download IDs, and stream ranks are not evidence of chronological source ordering.

Preserve explicit priority, partition, and feed distinctions where the contract uses them. Do not change source precedence, overlap treatment, duplicate semantics, or depth continuity to make a generated batch pass validation.

Pin any ranking snapshot actually used. Keep ranking calculations outside this refactor, and do not make canonical identity depend on current ranking position.

Planning and dry-run must not trigger downloads, live metadata collection, or ranking refreshes. Any separately supported refresh must remain explicit.

### Profiles and runtime configuration

Rust remains authoritative for raw-field mappings and normalization semantics.

Reuse registered profile bindings and the production profile-resolution mechanism. Do not manually compile equivalent rules in Python or add provider-specific record transformations to orchestration.

Runtime settings remain in the authoritative historical configuration:

```text
data/.config/historical/general.toml
```

Do not duplicate workers, memory budgets, Parquet targets, or integrity settings into batch tasks when the current contract assigns them to runtime configuration.

Retain the submitted configuration and identify the configuration/profile content actually used. Detect relevant changes between preparation and execution through supported snapshots, fingerprints, or ownership controls; a recorded checksum alone does not prevent a file from changing.

Preserve the engine’s persisted effective configuration as execution evidence.

### Failed preparation

A blocked plan must not be published as a valid executable batch.

Remove obsolete incomplete-input status handling where the current Rust contract no longer supports it. Do not confuse preparation blockers with mixed execution outcomes.

Diagnostics may retain the proposed work and unresolved selections, but only a fully resolved, contract-valid document may be marked executable.

## Prove Fixture Parity Through the Actual Builder

### Define “identical” correctly

Compare JSON structure and meaning, not whitespace or object-key ordering.

For equivalent controlled inputs, generated output must match the fixture’s required fields, types, nesting, values, omission/null behavior, and meaningful array ordering.

Run-specific identities, timestamps, output roots, and preparation artifact locations may differ only through explicit, narrowly documented transformations or deterministic injected allocators/clocks.

Do not ignore broad document sections, sort every array, remove unknown fields, or normalize away important differences until tests pass.

Do not mask instrument-namespace differences or source-priority changes as harmless run metadata.

### Round-trip tests are necessary but insufficient

Parsing the fixture and serializing it back only tests the wire model.

Also construct equivalent resolved inputs through the production preparation interfaces and call the real builder. Compare its output with the authoritative fixture or an explicitly selected fixture projection.

The production builder must not load the fixture as a template. It must work for inputs not present in the fixture.

Tests may derive controlled repository/test inputs from fixture facts, but the expected wire document must remain independently checked. Do not calculate expected output by calling the serializer under test.

### Required contract checks

Implement strict recursive comparisons with useful JSON-path diagnostics for missing fields, extra fields, type changes, value differences, and array-order differences.

Preserve distinctions such as:

```text
Integer versus string
Integer versus boolean
Exact decimal string versus JSON floating-point number
Omitted field versus explicit null
Empty collection versus missing collection
```

Add negative tests that remove or corrupt newly required fields, introduce conflicting instrument mappings, omit family members, misuse bounds/units, duplicate identities, or present unsupported versions.

Pass generated documents through the actual Rust production loader and resolver. Python model validation alone is not proof of compatibility.

Reuse a nonexecuting native validation interface where available. If none exists, a narrowly scoped validation mode or test harness delegating to the production loader is authorized. It must not create staging datasets or perform normalization.

Do not run the entire real workload merely to validate its document structure.

Record the fixture checksum used by contract tests and ensure tests do not modify it. A deliberate fixture-contract change must expose a meaningful test difference rather than silently updating expected output.

## Complete the Forge CLI Workflow

Extend the existing `normalize` command group rather than introducing another normalization command family.

The desired public behavior is:

```bash
# Existing recipe creation and structural validation remain available.
mf normalize new NAME --from-acquisition ACQUISITION_NAME
mf normalize validate NAME

# Resolve inputs and show readiness without publishing or executing.
mf normalize plan NAME --testing

# Persist a current-contract executable batch and preparation evidence.
mf normalize prepare NAME --testing

# Prepare and execute through the same services.
mf normalize run NAME --testing

# Execute a previously prepared batch without rebuilding its selection.
mf normalize run --batch /path/to/normalize-batch.json --testing

# Explicitly request the native normalization display.
mf normalize run NAME --testing --tui

# Reconcile an existing execution without repeating engine work.
mf runs reconcile RUN_ID --testing
```

Adapt placement of existing global options to the current parser. Recipe input and `--batch` must be mutually exclusive.

Preserve useful existing commands and provide a documented compatibility alias for `prepare --dry-run` if it already exists.

### Planning, preparation, and execution

`validate NAME` checks recipe structure without database initialization or engine execution.

`plan` may read the selected registry and perform nonexecuting engine validation. It must not allocate permanent identities, publish jobs, refresh markets, install binaries, or create output datasets. Report allocations still needed instead of presenting provisional IDs as reserved.

`prepare` may reserve the required identities and publish immutable preparation artifacts. Document this change from the older read-only preparation model. It must not invoke processing.

`run NAME` uses the same preparation service and then executes the resulting frozen batch.

`run --batch PATH` validates and uses the selected batch without silently regenerating IDs, reselecting archives, or rewriting its semantics.

A managed execution must establish its database environment, identity reservations, and registered raw-input lineage before spawning Rust. A manually authored batch lacking those associations must produce an actionable preflight error, not invented registry relationships.

Rust’s standalone CLI remains available for database-independent execution. Do not add an ambiguous unmanaged bypass to Forge merely to avoid registry validation.

Provide explicit runtime-configuration and isolated output-root overrides where consistent with the existing CLI. Record their effective values and apply them before execution; never rewrite the user’s original batch in place.

### Readiness is evidence-based

Distinguish configuration validity, input availability, native contract validation, executable availability, execution success, and registry completion.

A working `--help` invocation does not prove that the installed binary accepts the prepared batch.

A missing binary must produce guidance to use the B01 build command. Do not compile automatically during normalization, invoke `cargo run`, or silently fall back to an unrelated executable on `PATH`.

## Invoke and Supervise the Installed Binary

Resolve the normalization executable through the shared managed-binary resolver:

```text
<repository-root>/.bin/marketforge-normalize
```

Use absolute paths, explicit working-directory/root handling, and argument arrays rather than shell-generated command strings.

Inspect supported native flags before constructing the invocation. Do not guess flags from outdated documentation.

Submit the batch through the production batch interface. Do not split it into legacy per-job documents merely to accommodate the old Forge producer.

Run one supervised engine invocation at a time under the existing historical execution owner. Rust retains its internal concurrency. Do not multiply resource budgets by launching one engine process per source archive.

Keep database transactions short; do not hold a transaction open throughout normalization.

Retain the exact submitted batch, checksum, effective invocation, executable identity, output locations, timestamps, and raw engine response. Do not record secrets or forward database credentials to Rust.

Capture logs and responses with bounded buffering and durable artifacts. Avoid loading unlimited terminal output or detailed report data into memory.

### Terminal and JSON modes

Noninteractive execution must preserve a machine-readable engine result independently of human-readable progress.

Do not assume `--tui` produces the same stdout as headless execution. An earlier engine report explicitly described interactive stdout rendering for TUI mode versus a JSON document in normal machine mode; inspect current behavior. :chatgpt-content-reference{index="3"}

For `--tui`, hand terminal ownership to the native display and restore it on success, failure, and cancellation. Forge and Rust must not render competing interfaces simultaneously.

Obtain the authoritative result through an existing supported result artifact/interface. If necessary, a minimal native result-file option using the existing response schema is authorized. Do not parse ANSI screens or terminal summaries to determine success.

Reject incompatible combinations such as interactive TUI with machine-readable stdout unless the actual implementation provides a verified separation.

For Forge `--json`, emit one valid final result document on stdout and keep diagnostics on stderr. Silent mode changes presentation, not registration or failure handling.

### Cancellation and ownership

Use the existing process-supervision and workspace/data-root ownership mechanisms.

On cancellation, stop and reap owned child processes, retain available evidence, and record cancellation or an unresolved outcome accurately. Do not treat user interruption as successful completion.

Protect against a surviving engine child continuing to write after its parent exits. A replacement Forge process must not acquire conflicting write ownership while that child remains active.

Use a verified lock-lifetime/supervision mechanism; a persisted PID alone is insufficient. Do not introduce a daemon or distributed scheduler.

## Interpret Results and Verify Committed Outputs

Validate the native response against its actual independent schema version.

Check both process exit status and every requested job outcome. Match reported run/job/dataset identities against the frozen request, and detect missing, duplicate, or unexpected results.

A nonzero process exit can coexist with successfully committed independent jobs. Preserve and verify those outcomes without marking the overall request successful.

Likewise, exit code zero must not override missing results, malformed JSON, failed jobs, or absent committed manifests.

Use the engine’s existing interpretation of approved degraded integrity. Do not silently turn a permitted degraded dataset into either an unqualified clean result or an unrelated execution failure.

### Manifest verification

Register only outputs supported by valid committed manifests at the expected final destinations.

Verify identities, operation/content type, supported versions, artifact inventory, safe contained paths, file existence/sizes, declared checksums where present, and required provenance/auxiliary artifacts.

Use established readers or native verification helpers where possible. Do not make ordinary execution require the notebook/ML dependency stack.

Do not discover success by globbing staging directories or by treating every Parquet file beneath an output root as a committed dataset.

Respect the current stream-oriented layout. Multiple tasks may contribute to one physical file, and one source may span several parts. Count physical files once and preserve source spans rather than reconstructing the obsolete task-owned layout.

Retain depth event indexes, segment/boundary metadata, independent-source semantics, and source-local versus stream-global offsets. Keep source-event counts separate from canonical price-level row counts.

Do not reimplement order-book reconstruction merely to register outputs. Engine semantic checks and Forge publication/identity checks are distinct responsibilities.

Preserve exact-byte checksums for resolved processing documents. Do not deserialize and reserialize a document before checking a checksum defined over its original bytes.

## Dataset Registration and Recovery

Reuse the existing dataset, execution, storage, and lineage repositories. Add migrations only where the current schema genuinely lacks required lifecycle or provenance fields.

Keep engine publication and PostgreSQL completion separate.

The workflow must distinguish:

```text
Prepared
Executing
Engine committed output
Output verified
Registration pending
Complete and registered
Failed / cancelled / unresolved
```

Map these concepts to established statuses rather than introducing redundant vocabularies.

Register dataset identity, actual committed location, stream/file inventory where supported, integrity outcome, observed bounds, effective configuration references, and raw-input lineage.

Lineage must identify the registered source copies and checksums actually selected, not merely their filenames or mutable URLs.

Do not claim continuous coverage from minimum/maximum timestamps.

### Registration failure after engine success

Deliberately support this interruption:

```text
Rust commits a valid dataset
        │
        ▼
Forge registration fails or the parent exits
        │
        ▼
Dataset remains intact; registration is pending
        │
        ▼
Explicit reconciliation verifies existing artifacts
        │
        ▼
Registration completes without rerunning normalization
```

Persist sufficient intent and evidence before execution for reconciliation to locate the expected artifacts without scanning every historical run.

Recovery must preserve dataset identity, validate provenance, reject conflicting records, and remain idempotent.

Repeated reconciliation must not duplicate datasets, files, streams, or lineage.

Do not retry engine execution as a response to database registration failure. Do not overwrite committed output or reuse failed staging blindly.

An already completed prepared batch must be recognized before another execution attempt. Creating a genuinely new normalization result requires an explicit new run and appropriate identities/destinations.

## Verification Requirements

### Contract and preparation coverage

Verify complete field parity with the maintained fixture, not only a handful of headline keys.

Exercise every distinct fixture shape through production builder tests, including trade/depth distinctions and applicable family, source-ordering, feed, specification, and namespace cases.

Add independent synthetic cases for important optional rules not represented by the fixture.

Contract tests must remain separate from source-data correctness tests. If the maintained workload contains a legitimate processing blocker, preserve that outcome rather than weakening the producer or engine to obtain a green full-workload result.

### Real end-to-end CLI coverage

Mocks are useful for failure injection but do not satisfy the integration milestone.

Run small deterministic trade and depth cases through:

```text
Registered inputs
    → production Forge builder
    → generated batch
    → installed native binary
    → real committed artifacts
    → Forge verification
    → testing-database registration
```

Exercise both `run NAME` and execution of a saved prepared batch.

Include multiple sources contributing to a stream and multiple jobs in a batch where supported, so tests cannot pass while retaining one-task/one-file assumptions.

Use independently reviewed expected canonical values and meaningful depth metadata checks, not only nonempty files.

### Failure matrix

| Case | Required result |
|---|---|
| New required field omitted or invalid | Native validation fails before processing |
| Missing historical specs or family authority | Explicit preparation blocker |
| Binary absent, incompatible, or not executable | No engine execution; actionable error |
| Registered input changed or disappeared | Preflight failure; no silent replacement |
| Engine exits unsuccessfully with valid job results | Preserve individual results; overall nonzero |
| Engine exits zero with failed or missing results | Overall failure |
| Malformed/truncated response | Protocol error with retained evidence |
| Manifest identity/path mismatch | Reject registration |
| Missing depth auxiliary artifact | Reject registration |
| Existing staging/final destination | No overwrite |
| Registration fails after successful publication | Recover existing dataset without rerunning Rust |
| Cancellation or parent interruption | No competing writer or false completion |
| Repeated reconciliation | No duplicate registry records |
| Non-root invocation directory | Correct path resolution |
| JSON/silent/TUI modes | Same processing semantics and truthful final outcomes |

Where R01 is available, include a supported locally registered input through the same builder/executor. Do not create a special external execution path.

### Data and execution safety

Use unique retained directories beneath:

```text
data/tests/historical/normalize/forge-integration/<run-id>/
```

Redirect every writable location, including datasets, staging, scratch, logs, and reports, before execution. Preserve generated integration artifacts for inspection.

Keep raw inputs, the authoritative fixture, production profiles, and `general.toml` unchanged.

Pure unit/configuration tests may use disposable temporary files.

The optional local corpus is:

```text
research/databento_samples/
```

Inspect it only if relevant and available. Do not assume its instrument classes or formats are supported, invent specifications, download replacements, or consume vendor credits.

Do not execute the entire maintained historical workload or performance benchmarks during this assignment. Bounded real-engine integration is authorized; full-corpus processing is not required to prove the orchestration contract.

When an explicitly requested verification prerequisite is absent, report it as blocked, not passed.

## Documentation and Final Deliverables

Update the existing Zensical site with the implemented normalization workflow.

Suggested pages, adapted to current navigation:

```text
docs/content/workflows/normalization.md
docs/content/configuration/normalization.md
docs/content/reference/cli/normalization.md
docs/content/getting-started/quickstart.md
docs/zensical.toml
```

Update affected module documentation and stale integration notices. Preserve unrelated documentation and site design.

Document recipe selection, plan/prepare/run differences, preparation side effects, saved-batch execution, installed-binary prerequisites, runtime configuration, terminal modes, output locations, failure states, and reconciliation.

Explain current supported input versions and their authority without copying the entire engine schema into a competing specification.

Provide one actually verified bounded walkthrough with retained artifact references. Validate commands against the parser, examples against the implemented models/native loader, and run the configured strict documentation build.

The implementation report must include the field-by-field contract audit, fixture checksum, producer changes, parity-test method, actual native validation mechanism, CLI behavior, subprocess ownership, response/manifest handling, registry changes, recovery evidence, tests executed, and remaining blockers.

Run applicable formatting, linting, type checking, and focused tests. Report pre-existing failures separately; do not weaken checks or hide maintained modules.

## Completion Criteria

This assignment is complete when the production Forge builder emits the current engine contract, strict tests demonstrate parity with the authoritative fixture, and the Forge CLI executes real normalization through `.bin/marketforge-normalize`.

Successful outputs must be verified and registered with correct lineage. Failed, cancelled, mixed, and registration-pending outcomes must remain distinguishable and recoverable.

No hand-written job repair, legacy wrapper conversion, automatic compilation, alternate external-data pipeline, or manual SQL intervention should be required for the verified supported workflow.

**Stop after normalization preparation, execution, verification, registration, recovery, and documentation. Do not continue into merge/conversion orchestration, a pipeline framework, centralized dataset search, or unrelated engine redesign.**