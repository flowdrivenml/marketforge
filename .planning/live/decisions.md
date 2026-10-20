# MarketForge Live — Implementation Decisions

**Project:** MarketForge Live  
**Language:** Rust (edition 2024)  
**Crate:** `marketforge-engine`  
**Status:** Active implementation  
**Last updated:** 2026-10-20

---

## 1. Purpose

This document records implementation decisions that must remain consistent
across MarketForge Live development sessions.

It complements:

- Architecture documents `00–16`: authoritative system design.
- `17 — Implementation Roadmap`: implementation order and acceptance criteria.
- `.planning/live/status.md`: implementation progress.
- `.planning/live/evidence/`: implementation and verification reports.

### Authority

The architecture documents remain the primary source of requirements.

This register records decisions made while implementing those requirements.

When a conflict is discovered:

1. Identify the conflicting contracts.
2. Report the conflict explicitly.
3. Resolve it before implementing dependent modules.
4. Update the relevant architecture document when an approved design changes.
5. Update this register to reflect the resolution.

Do not silently change architectural contracts to accommodate an implementation.

---

# 2. Development Workflow

## DEC-001 — ChatGPT is the primary code author

**Status:** Accepted

ChatGPT is responsible for:

- Architecture and implementation design.
- Writing complete Rust implementations.
- Defining module interfaces and integration boundaries.
- Identifying dependencies before implementation.
- Handling compiler errors and integration corrections.
- Maintaining consistency with architectural contracts.

The user copies ChatGPT-generated Rust code into the repository and returns
compiler diagnostics or confirmation.

The preferred output is actual implementation code with exact file paths,
not an instruction asking another AI to design and implement the module.

## DEC-002 — Qwen/OpenCode is the Local Repository, Testing, and Diagnostics Assistant

**Status:** Accepted

### Purpose

Qwen/OpenCode serves as ChatGPT's local development assistant, providing
direct access to the MarketForge repository, executing explicitly authorized
validation procedures, collecting diagnostic evidence, and maintaining
implementation records.

**ChatGPT remains the primary architect, implementation author, and technical
decision-maker.**

Qwen is responsible for gathering reliable information and performing
well-defined operations, not independently designing MarketForge.

### 1. Repository Inspection and Source Extraction

Qwen is responsible for:

- Extracting exact existing Rust source definitions.
- Locating relevant functions, structs, enums, traits, and implementations.
- Identifying imports, exports, module boundaries, and dependencies.
- Finding call sites and references across the repository.
- Inspecting Cargo features and dependency relationships.
- Identifying existing utilities suitable for reuse.
- Reporting current module behavior based on actual source code.
- Identifying discrepancies between implementation and architectural contracts.

When ChatGPT needs repository information, Qwen should produce a targeted
extraction report containing:

- Exact file paths and line numbers.
- Relevant source code, not merely summaries.
- Required imports and related type definitions.
- Function signatures and return types.
- Relevant callers and consumers.
- Existing error handling.
- Any unresolved questions or ambiguities.

**Qwen must distinguish verified source observations from its own inferences.**

### 2. Mechanical Code Modifications

Qwen may perform explicitly requested mechanical operations, including:

- Adding specified imports or exports.
- Applying exact code replacements supplied by ChatGPT.
- Creating files containing ChatGPT-provided implementations.
- Performing repetitive, deterministic edits.
- Updating module declarations and feature gates as instructed.
- Applying narrowly specified corrections.

Qwen must not:

- Independently redesign module architecture.
- Invent new interfaces or behavioral requirements.
- Modify unrelated files.
- Change financial-data semantics.
- Weaken validation or error handling.
- Silently alter tests to make them pass.
- Introduce new dependencies without approval.

If an operation requires an architectural decision, Qwen must stop and report
the issue rather than choose a solution independently.

### 3. Test Execution and Validation

Qwen is responsible for executing validation commands explicitly authorized
by the user or ChatGPT.

This includes:

- Rust formatting verification.
- Narrowly scoped Cargo compilation checks.
- Approved unit tests.
- Approved integration tests.
- Approved regression tests.
- Approved runtime verification procedures.
- Approved benchmarks, only when explicitly authorized.

#### Execution restrictions

1. Never run Git commands.
2. Never execute unrestricted `cargo test` without explicit authorization.
3. Never launch broad historical test suites without explicit authorization.
4. Never run benchmarks without explicit authorization.
5. Never assume a filtered test is inexpensive.
6. Do not automatically expand the scope of an approved test command.
7. Do not repeatedly rerun failing tests without authorization.
8. Do not install dependencies or modify the environment without approval.
9. Do not start external services unless explicitly instructed.
10. Do not modify source code or test expectations in response to a failure
    unless ChatGPT provides an approved correction.

Prefer reproducible, narrowly scoped commands using `--locked` and `--offline`
where appropriate.

**A passing compilation check is not evidence that behavioral tests passed.**

### 4. Test Result Collection

For every authorized validation operation, Qwen should record:

- Exact command executed.
- Working directory.
- Relevant feature flags.
- Test target and test names.
- Exit status.
- Number of passed, failed, ignored, and skipped tests, when applicable.
- Execution duration, when available.
- Compiler warnings.
- Relevant environmental conditions.
- Whether the operation completed, timed out, or was interrupted.

Results must distinguish:

- Compilation failures.
- Behavioral assertion failures.
- Runtime panics.
- Infrastructure or dependency failures.
- Timeouts and hangs.
- Data-integrity failures.
- Suspected regressions.

Qwen must not classify a failure as a regression without evidence of
previously passing behavior.

### 5. Automatic Failure Diagnostics

When an authorized test or validation operation fails, Qwen must prepare
a diagnostic report suitable for investigation by ChatGPT.

The report should include:

1. Exact failing command.
2. Complete failure classification.
3. Relevant error messages.
4. Failing test names.
5. Assertion failures and expected/actual values.
6. Panic locations and stack traces, when available.
7. Exact source of the failing tests.
8. Relevant production implementation code.
9. Related data types, traits, interfaces, and invariants.
10. Relevant call sites and dependency relationships.
11. Architectural requirements associated with the failure.
12. Missing information needed for further investigation.

Source extracts must include file paths and line numbers.

Qwen should gather enough context for ChatGPT to investigate without
requiring several additional repository-inspection requests.

However, extraction must remain targeted. Do not dump the entire repository
or unrelated source files.

#### Diagnostic reasoning

Qwen may report potential causes, but must separate:

- **Verified facts:** Directly supported by source code or execution output.
- **Hypotheses:** Plausible explanations not yet verified.
- **Unknowns:** Information that remains unavailable.

Qwen must not present hypotheses as established facts.

#### Failure-handling boundaries

When a test fails:

- Preserve the original failure evidence.
- Do not silently retry.
- Do not modify implementation code.
- Do not relax assertions.
- Do not update expected results to match incorrect behavior.
- Do not conceal failing tests.
- Do not declare the work package accepted.

ChatGPT reviews the evidence, diagnoses the problem, and determines the fix.

### 6. Evidence Management

Qwen maintains implementation evidence under:

```text
.planning/live/evidence/
```

Evidence should be organized by work package when appropriate:

```text
.planning/live/evidence/
└── WP-XX-module-name/
    ├── source-extraction.md
    ├── validation-results.md
    ├── failures.md
    ├── relevant-source.md
    ├── raw-output.log
    └── handoff.md
```

Not every task requires every file.

For successful operations, a concise validation report may be sufficient.

For failures, preserve complete original command output and provide a
self-contained diagnostic summary.

#### Evidence requirements

- Preserve raw output without modifying its contents.
- Never silently truncate evidence.
- Include exact commands and exit codes.
- Include relevant source snippets.
- Include timestamps when available.
- Distinguish confirmed observations from interpretations.
- Reference the applicable work package.
- Record unresolved issues explicitly.
- Do not fabricate validation results or source excerpts.

If output is too large to include in Markdown, save the complete output
separately and reference its exact location.

### 7. Implementation Progress Tracking

Qwen maintains:

```text
.planning/live/status.md
```

This file tracks:

- Completed work packages.
- Work currently in progress.
- Blocked work.
- Verification status.
- Outstanding implementation tasks.
- Next approved work package.
- References to supporting evidence.

Qwen may record factual progress automatically.

However:

**Only the user or ChatGPT may authorize marking a work package as Accepted.**

Compilation success alone does not establish acceptance.

Qwen must preserve the official work-package numbering from
Document 17 and correctly associate implementation subtasks with their
parent roadmap packages.

### 8. Architectural Decision Register

Qwen maintains:

```text
.planning/live/decisions.md
```

Its responsibility is administrative.

Qwen may:

- Record decisions explicitly approved by the user or ChatGPT.
- Update the status of resolved decisions when instructed.
- Record references to supporting evidence.
- Identify possible contradictions between existing decisions.
- Report architectural questions requiring resolution.

Qwen must not:

- Independently create accepted architectural decisions.
- Change existing accepted decisions without approval.
- Resolve open architectural questions by guessing.
- Modify authoritative architecture documents without authorization.

When a contradiction is found, Qwen must report it before modifying
the affected decision.

### 9. Implementation Handoff Reports

After completing a substantial work package, Qwen may generate:

```text
.planning/live/evidence/WP-XX-handoff.md
```

The handoff should contain:

- Implemented functionality.
- Exact files created or modified.
- Important public interfaces.
- Relevant type definitions and function signatures.
- Established invariants.
- Dependencies on other modules.
- Compilation and test results.
- Remaining failures or limitations.
- Accepted decisions relevant to future work.
- Outstanding architectural questions.
- Recommended next work based on the approved roadmap.

A handoff report must distinguish implemented functionality from planned
functionality.

**The purpose is to make development transferable between ChatGPT sessions
without relying exclusively on conversational memory.**

### 10. Operating Procedure

The standard development cycle is:

1. **ChatGPT** selects the next work package using the roadmap and status.
2. **Qwen** extracts current repository context when needed.
3. **ChatGPT** designs and writes the implementation.
4. **User or Qwen** applies the exact implementation.
5. **Qwen** executes explicitly approved validation commands.
6. **Qwen** captures results and automatically gathers targeted diagnostics
   for failures.
7. **ChatGPT** reviews results and provides corrections when necessary.
8. **Qwen** applies approved mechanical corrections.
9. **ChatGPT** authorizes acceptance when the agreed criteria are met.
10. **Qwen** updates status, decisions, and evidence accordingly.

### 11. General Operating Rules

Qwen must always:

- Follow the authoritative architecture documents.
- Preserve historical-engine compatibility.
- Respect explicit file and command restrictions.
- Avoid unnecessary dependencies.
- Prefer exact source evidence over assumptions.
- Maintain reproducible validation records.
- Report unexpected changes or failures.
- Stop when a task requires an unapproved architectural decision.
- Preserve evidence of unsuccessful operations.
- Keep documentation synchronized with approved implementation progress.

**Qwen is responsible for local execution, repository intelligence,
diagnostics, and documentation. ChatGPT remains responsible for architecture,
implementation design, technical corrections, and final acceptance.**

## DEC-003 — Implement coherent modules completely

**Status:** Accepted

Prefer completing a coherent module with its known responsibilities and
interfaces rather than repeatedly adding minimal placeholders.

Before implementing a module, consider its approved requirements for:

- Configuration.
- Metrics and diagnostics.
- Error classification and recovery.
- Cancellation and shutdown.
- Concurrency and resource ownership.
- Testability.
- Runtime integration.
- Public interfaces consumed by other modules.

This does not authorize implementing unrelated subsystems prematurely.

Avoid speculative abstractions that are not required by the architecture.

## DEC-004 — No Git commands

**Status:** Accepted

Do not request or execute Git commands as part of implementation tasks.

This includes status, diff, add, commit, reset, checkout, and other Git
operations unless the user explicitly changes this rule.

## DEC-005 — No expensive tests without authorization

**Status:** Accepted

Do not execute or request broad test suites or benchmarks without explicit
authorization.

In particular, do not run unrestricted `cargo test`.

Even filtered Rust tests may compile expensive targets.

The normal validation commands are narrowly scoped:

```bash
cargo fmt --check

cargo check --locked --offline --lib --no-default-features --features live

cargo check --locked --offline --lib --no-default-features --features live,historical
```

Use only the checks appropriate to the changed files.

A compilation check does not establish behavioral correctness.

Tests should still be designed and implemented where appropriate, but
execution remains subject to this restriction.

---

# 3. Historical Compatibility and Shared Infrastructure

## DEC-006 — Historical behavior must remain compatible

**Status:** Accepted

MarketForge Live is added to the existing MarketForge historical engine.

Shared code should be reused where the semantics genuinely match.

Do not modify historical financial models, processing semantics, or public
interfaces merely to simplify a Live implementation.

Where implementation is safely extracted into shared infrastructure,
preserve historical import paths through compatibility re-exports when useful.

## DEC-007 — Shared normalization infrastructure

**Status:** Implemented

The following components have been made available to Live:

- Shared depth processing outcome types.
- Shared trade specification and transformation components.
- Shared trade identity-policy types where compatible.
- Canonical trade and L2 financial models.

Live-specific trade continuity and deduplication remain separate concerns.

The historical archive identity policy must not automatically be reused
as the Live exchange-trade deduplication policy.

## DEC-008 — Shared Parquet infrastructure

**Status:** Partially implemented

Shared Parquet infrastructure is located under:

```text
engine/src/storage/parquet/
```

The extracted components include:

- Arrow schema definitions.
- Decimal conversion utilities.
- `TradeBatchBuilder`.

Historical compatibility re-exports preserve existing import paths.

The complete Live retention and replay implementation is not yet finished.

## DEC-009 — Decimal representation

**Status:** Accepted and verified

The shared Arrow schema uses:

```rust
pub const DECIMAL_PRECISION: u8 = 76;
pub const DECIMAL_SCALE: i8 = 28;
```

Financial decimal data uses Arrow `Decimal256` with precision 76 and scale 28.

The shared `TradeBatchBuilder` uses these schema constants.

Do not substitute a different precision or scale without an approved
schema migration.

## DEC-010 — Cargo feature isolation

**Status:** Implemented

The principal feature groups are:

- `historical`
- `live`
- `live-retention`
- `monitor-tui`

The current `live` feature enables `live-retention`, making Arrow/Parquet
dependencies available to Live.

The shared `storage` module is available under historical or
live-retention feature gates.

The Live feature must not implicitly enable historical execution modules.

The Ratatui interface remains optional.

---

# 4. Market and Instrument Identity

## DEC-011 — Reuse the canonical Exchange enum

**Status:** Accepted and implemented

`ExchangeId` aliases the existing:

```rust
crate::canonical::Exchange
```

Do not introduce a second independent exchange enum with the same meaning.

Adding a new exchange should not require maintaining competing Live and
Historical exchange identities.

## DEC-012 — Use the existing instrument catalog identity

**Status:** Accepted and implemented

`SourceStreamKey` uses the existing:

```rust
crate::instrument::InstrumentIdentity
```

rather than an arbitrary instrument-symbol string.

The current source acquisition identity contains:

```rust
pub struct SourceStreamKey {
    pub exchange: ExchangeId,
    pub market: MarketKind,
    pub instrument: InstrumentIdentity,
    pub channel: ChannelSpec,
}
```

The key identifies a logical source acquisition independently of:

- WebSocket connection assignment.
- Subscriber identity.
- Recording state.
- Metrics state.
- Recovery state.

Source identities must support deterministic equality and hashing.

## DEC-013 — SourceStreamKey validation

**Status:** Accepted and implemented

Local validation checks:

- Exchange consistency.
- Market-family consistency where determinable.
- Unsupported instrument kinds.
- Structural channel parameter validity.

Validation distinguishes:

1. Definite failures.
2. Unresolved instrument classification.

A candidate constructor does not establish complete acquisition validity.

Catalog existence and exchange capability validation belong to the
configuration compiler.

The Stream Registry should receive validated acquisition requirements.

## DEC-014 — Perpetual market-family ambiguity

**Status:** Unresolved

The existing `InstrumentIdentity` representation may not distinguish
linear from inverse perpetual contracts when `contract_kind` is absent.

Do not assume all perpetual instruments are linear.

The configuration compiler must resolve this using authoritative catalog,
exchange profile, or configuration information.

Do not change the historical instrument catalog schema without an approved
design decision.

---

# 5. Stream Identity and Cursor Semantics

## DEC-015 — StreamId and SourceStreamKey are different

**Status:** Accepted

`SourceStreamKey` describes what market-data source is acquired.

`StreamId` is an assigned identifier for a registered logical stream.

They are not interchangeable.

`DerivedStreamId` is a separate strongly typed identifier for derived
streams.

## DEC-016 — Authoritative StreamCursor

**Status:** Accepted and implemented

A stream cursor consists of:

```text
(StreamId, LogEpoch, offset)
```

The cursor identifies an accepted logical event.

It is not:

- An exchange-native sequence number.
- A trade ID.
- A row number in a Parquet file.
- An L2 price-level index.
- A connection generation.
- A book generation.

The cursor namespace and offset allocation must preserve the approved
continuity and non-reuse guarantees.

## DEC-017 — Generations are independent

**Status:** Accepted and implemented

Maintain separate types for:

- `ConnectionGeneration`
- `SubscriptionGeneration`
- `BookGeneration`
- `DerivedGeneration`
- `LogEpoch`

These types represent different lifecycle or event-identity domains.

A WebSocket reconnection does not automatically imply a new `LogEpoch`.

A book resynchronization does not automatically imply a new `LogEpoch`.

Do not substitute `BookGeneration` for the cursor namespace.

## DEC-018 — One logical L2 event receives one cursor

**Status:** Accepted and implemented in the event model

A grouped L2 event contains ordered canonical `L2LevelUpdate` values.

The complete logical event receives exactly one accepted MarketForge cursor.

Individual price-level updates do not receive separate stream cursors.

Preserve:

- Initialization/change boundary.
- Original source operation.
- Native sequence metadata.
- Source-event ordinal where available.
- Ordered levels.
- Relevant timestamps.
- Book generation.

An initialization event must preserve the information required to
reconstruct the authoritative book state.

Do not assume a delta-diff representation is equivalent to a complete
initialization snapshot.

---

# 6. Runtime Lifecycle and Demand Ownership

## DEC-019 — StreamLifecycle represents logical runtime lifecycle

**Status:** Accepted and implemented

The approved lifecycle states are:

```rust
pub enum StreamLifecycle {
    Registered,
    Starting,
    Active,
    Idle,
    Stopping,
    Stopped,
    Failed,
}
```

Do not add `Reconnecting`, `Synchronizing`, or `Degraded` as lifecycle states.

These conditions belong to separate operational dimensions.

## DEC-020 — Operational states are independent

**Status:** Accepted and implemented

Maintain distinct models for:

- Connection state.
- Subscription state.
- Integrity state.
- Recovery state.
- Coverage state.

A logical stream can remain active while a connection reconnects,
a subscription is pending, or integrity recovery is underway.

Do not combine all operational conditions into one giant state machine.

## DEC-021 — Integrity failures are classified separately from recovery

**Status:** Accepted and implemented

The initial common categories are:

```rust
pub enum IntegrityFailure {
    DataGap,
    ContinuityViolation,
    ConflictingDuplicate,
    MalformedData,
    SnapshotFailure,
    StructuralViolation,
}
```

An integrity failure describes the detected problem.

Recovery policy determines the action taken.

Do not embed unbounded raw diagnostic messages in the integrity-state enum.

Detailed diagnostics belong in the appropriate bounded reporting system.

## DEC-022 — Demand ownership is not persistence

**Status:** Accepted and implemented

A demand registration identifies who currently requires acquisition.

The owner categories include:

- Persistent configuration.
- Recording subsystem.
- Subscriber.
- Derived stream.
- Retention session.

The lifetime of a demand registration is separate from whether its
underlying requirement survives restart.

A persistently configured derived stream may have runtime dependency
registrations that must be reconstructed after restart.

Do not infer persistence solely from the `DemandOwner` variant.

## DEC-023 — StreamHandle identifies an individual registration

**Status:** Accepted and implemented

A stream handle identifies:

```text
(stream_id, demand_id)
```

Multiple independent demands may exist for one logical stream.

Disabling persistent activation must release only the persistent
configuration demand.

It must not terminate acquisition while independent demands remain.

Avoid maintaining manual reference counters that can diverge from
the actual registration set.

---

# 7. Accepted Live Event Representation

## DEC-024 — LiveEvent owns the authoritative cursor

**Status:** Implemented; final integration verification pending

The current event model is:

```rust
pub struct LiveEvent {
    pub cursor: StreamCursor,
    pub payload: LivePayload,
    pub provenance: EventProvenance,
    pub timing: EventTiming,
}
```

The cursor is the sole authoritative MarketForge event identity.

Do not store a second independent `stream_id` field representing
the same identity.

## DEC-025 — Canonical financial payloads are reused directly

**Status:** Implemented

The Live payload is:

```rust
pub enum LivePayload {
    Trade(Trade),
    L2Group(LiveL2Group),
}
```

Live trade events reuse the existing canonical `Trade` directly.

Do not introduce a trade wrapper that duplicates the outer event's
cursor, provenance, and timing.

## DEC-026 — Grouped L2 event representation

**Status:** Implemented

The current Live L2 payload is:

```rust
pub struct LiveL2Group {
    pub grouped_boundary: DepthEventBoundary,
    pub grouped_operation: DepthSourceOperation,
    pub grouped_levels: Vec<L2LevelUpdate>,
    pub source: DepthSourceEventMetadata,
}
```

The existing depth source metadata remains authoritative for:

- Native sequence range, when provided.
- Source-event ordinal.
- Source event timestamp.
- Other supported source-level metadata.

Do not add an independent mandatory sequence range.

`BookGeneration` is carried in event provenance and is required for
accepted L2 events.

## DEC-027 — Event provenance and timing

**Status:** Implemented; downstream validation pending

Event provenance carries source and generation metadata.

The current fields include:

```text
source_stream
connection_generation
subscription_generation
native_sequence
native_trade_id
book_generation
```

Event timing includes:

```text
exchange_timestamp_ns
received_timestamp_ns
```

Different timestamps must preserve their distinct meanings.

Do not introduce redundant timestamp copies without a documented
consistency rule.

## DEC-028 — Event acceptance and immutability

**Status:** Contract accepted; runtime enforcement pending

An accepted event must not be modified by downstream consumers.

Public Rust fields alone do not enforce immutability.

The Event Router should distribute accepted events using immutable
access patterns, such as references or `Arc<LiveEvent>`, as appropriate.

Before publication, recording, or retention, required event invariants
must hold.

In particular:

- Cursor and provenance source identity must be consistent.
- Accepted L2 events must carry a `BookGeneration`.
- The complete grouped L2 event must retain one cursor.

## DEC-029 — Cursor identity does not establish exchange deduplication

**Status:** Accepted

Exchange duplicate detection and continuity validation must occur before
assigning an accepted MarketForge cursor.

Different cursor values do not prove that two incoming exchange messages
were distinct exchange events.

Trade processing must use the appropriate exchange-native identifiers,
sequence semantics, and continuity rules.

The cursor identifies the accepted event after deduplication and
integrity processing.

---

# 8. Storage, Publication, and Monitoring

## DEC-030 — Unix sockets for market-data publication

**Status:** Accepted

MarketForge Live publishes normalized events through Unix domain sockets.

Shared memory publication is not part of the initial implementation.

The publication mechanism must preserve the approved ordering,
backpressure, and subscriber-management contracts.

## DEC-031 — PostgreSQL is optional market-data recording storage

**Status:** Accepted

PostgreSQL may store selected market data when recording is enabled.

Do not use PostgreSQL as the storage system for:

- Application logs.
- Operational event history.
- General runtime diagnostics.

Those remain outside the market-data recording database.

## DEC-032 — Internal observability rather than Prometheus/Grafana

**Status:** Accepted

MarketForge Live uses:

- Internal metrics.
- Structured diagnostics.
- A simple monitoring/control API.
- A Ratatui monitoring client.

Prometheus and Grafana are not part of the approved implementation.

Any roadmap entry referring to optional Prometheus exposition must be
reconciled with this decision before implementation.

## DEC-033 — Separate monitoring TUI

**Status:** Accepted

The Live TUI is an independent client of the Control API.

It must not directly own or manipulate the Live runtime's internal
components.

The intended interface covers:

- Global health and throughput.
- Latency and errors.
- Stream status and controls.
- Recording controls.
- Derived/merged streams.
- Supported runtime settings.

## DEC-034 — Retention supports bootstrap and replay

**Status:** Accepted; implementation pending

Live retention must provide:

- RAM buffering.
- Parquet spill/persistence as required.
- Stable cursor-based replay.
- Coverage and gap reporting.
- Retention reservations/protection.
- Replay-to-live handoff support.
- Recovery after supported process or storage failures.

Historical bootstrap is implemented separately but depends on these
Live capabilities.

Retention must preserve grouped L2 event identity, boundaries,
sequences, and book generation where required for reconstruction.

---

# 9. Known Open Decisions

These items must not be silently resolved by implementation guesses.

## OPEN-001 — Perpetual contract-family resolution

Determine the authoritative mechanism for distinguishing linear and
inverse perpetual instruments when the existing catalog identity does
not provide this information.

**Owner:** Configuration/compiler integration.

## OPEN-002 — DemandId allocation and restart behavior

Define how the Stream Registry allocates unique demand registrations
and how those registrations relate to reconstructed persistent
requirements.

**Owner:** Stream Registry.

## OPEN-003 — LiveEvent invariant enforcement

Determine where complete validation and cursor assignment occur
relative to event acceptance, router publication, retention, and
recording.

**Owner:** Event acceptance / routing integration.

## OPEN-004 — Live event serialization and schema compatibility

Define versioned serialization and durable persistence contracts for
accepted Live events.

The current Serde derives do not by themselves establish a stable
versioned wire or storage format.

**Owner:** Event delivery/replay contracts.

## OPEN-005 — Complete Live retention metadata

Ensure durable retention stores sufficient information to reconstruct
the full accepted event, including cursor namespace, source provenance,
grouped L2 boundaries, ordered levels, generation, and sequence metadata.

The shared historical `TradeBatchBuilder` is a reusable payload builder;
it is not a complete Live-event persistence format.

**Owner:** Live Retention.

---

# 10. Next Implementation Position

Current progress is recorded in:

```text
.planning/live/status.md
```

The next implementation work should complete the remaining foundational
domain, delivery, replay, and service-port contracts according to
Document 17.

The current Live event envelope exists, but the distribution, replay,
retention, and recording implementations are not complete.

For future sessions:

1. Read this decision register.
2. Read `.planning/live/status.md`.
3. Consult the relevant architecture documents.
4. Inspect actual current Rust source when necessary.
5. Write complete compatible Rust code.
6. Validate using approved narrow commands.
7. Update status and decisions when work is accepted.

Do not treat an implementation report or successful compilation as
proof that runtime behavior has been verified.