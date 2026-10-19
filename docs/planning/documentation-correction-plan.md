# MarketForge Live — Documentation Consistency and Implementation Readiness Plan

## Objective

Prepare Documents 00–16 for reliable, contract-complete,
AI-assisted implementation.

The final documentation must provide:

- One authoritative definition for every shared contract.
- Consistent terminology across all documents.
- No contradictory implementation instructions.
- Explicit separation between confirmed and proposed decisions.
- Clear subsystem ownership.
- Complete v1 requirements before implementation starts.
- Defined interfaces between dependent modules.
- Testable acceptance criteria.
- No unnecessary implementation backtracking.
- Sufficient structure for autonomous coding agents.

Document 17 — Implementation Roadmap will be written last.

---

# PHASE A — Correct Existing Contradictions

## STEP 1 — Master Plan and Documentation Index

Priority: P0
Primary document: 00 — Master Plan.md

Problems identified:

1. The reference document list contains outdated document names.
2. Documents 09 and 10 are referenced under older titles.
3. Documents 16 and 17 are listed in their previous order.
4. The implementation strategy does not explicitly enforce
   complete module requirements before implementation.
5. Some high-level descriptions need to reflect decisions
   finalized in later documents.

Required changes:

- Correct the complete documentation index.
- Establish the authoritative document numbering.
- Identify which document owns each architectural decision.
- Update references to finalized subsystem designs.
- State that later documents supersede older illustrative examples
  when they define a more precise contract.
- Add the contract-complete implementation principle.
- Keep the master plan concise.

Important:

The Master Plan should summarize decisions, not duplicate
detailed subsystem specifications.

Completion criteria:

- All document references resolve correctly.
- Documents 00–16 have consistent names and numbering.
- The implementation philosophy is explicit.
- No outdated master-level implementation instructions remain.

---

## STEP 2 — Configuration Revision and Persistence Order

Priority: P0

Documents affected:

- 00 — Master Plan
- 02 — Domain Models and Contracts
- 03 — Configuration System
- 05 — Stream Registry and Subscriptions
- 13 — Runtime Configuration and Control API

Problem identified:

Document 00 describes applying runtime changes before
persisting the accepted configuration revision.

Documents 02, 03, and 13 establish the opposite order.

Authoritative contract:

    Proposed Configuration
              |
              v
    Validate
              |
              v
    Compile Effective Configuration
              |
              v
    Calculate Semantic Diff
              |
              v
    Check Expected Revision
              |
              v
    Persist Accepted Desired State
              |
              v
    Publish Accepted Revision
              |
              v
    Runtime Reconciliation
              |
              v
    Report Convergence

Required changes:

- Make the ordering identical throughout the documentation.
- Distinguish accepted desired configuration from actual state.
- Specify persistence failure behavior.
- Specify partial reconciliation behavior.
- Clarify rollback and restart recovery.
- Ensure stream commands respect configuration ownership.

Completion criteria:

No document instructs an implementation to apply a persistent
desired-state mutation before its accepted revision is committed.

---

## STEP 3 — Control API Transport Consistency

Priority: P0

Documents affected:

- 02 — Domain Models and Contracts
- 03 — Configuration System
- 05 — Stream Registry and Subscriptions
- 08 — Event Distribution and Unix Sockets
- 13 — Runtime Configuration and Control API
- 14 — Ratatui Management Interface
- 15 — Testing and Validation
- 16 — External Libraries and Code Reuse

Problem identified:

Document 05 still describes HTTP-style management endpoints.

Document 13 defines a dedicated Unix socket with
length-prefixed JSON control messages.

These are different transport contracts.

Authoritative design:

    Rust Live Daemon
           |
           v
    Unix Control Socket
           |
           v
    Versioned JSON Protocol
           |
       +---+---+
       |       |
       v       v
    Ratatui   Python

Required changes:

- Remove or mark obsolete HTTP management examples.
- Standardize control request/response terminology.
- Standardize request and operation identifiers.
- Clarify persistent versus transient commands.
- Define control protocol ownership.
- Ensure the TUI and Python use identical protocol contracts.
- Preserve the separate binary market-data socket.

Completion criteria:

All documents describe the same initial management transport.

Optional Prometheus HTTP exposition remains separate
and read-only.

---

## STEP 4 — Stream Identity and Lifecycle Consistency

Priority: P0

Documents affected:

- 02 — Domain Models and Contracts
- 03 — Configuration System
- 05 — Stream Registry and Subscriptions
- 05.1 — Connection Manager and Transport Lifecycle
- 07 — Recovery Coordination and System Resilience
- 12 — Metrics and Stream Health
- 13 — Runtime Configuration and Control API

Problems identified:

1. Stream lifecycle states are represented differently.
2. Some descriptions combine lifecycle, integrity, and health.
3. Subscription replacement and source identity changes
   require clearer separation.
4. Desired state ownership must remain unambiguous.
5. Physical connection state must remain independent
   of source-stream state.

Required changes:

Establish distinct state dimensions:

    Desired State

    Stream Lifecycle

    Connection Lifecycle

    Subscription Lifecycle

    Integrity State

    Recovery State

    Historical Coverage

    Health Assessment

Clarify:

- SourceStreamKey versus StreamId.
- Stable identity across reconnections.
- New identity requirements for semantic acquisition changes.
- SubscriptionGeneration and ConnectionGeneration.
- Demand ownership.
- Registration versus activation.
- Resource ownership.
- Deactivation and removal.

Completion criteria:

One authoritative state model exists.

Every dependent document uses compatible state terminology.

No health assessment is confused with a lifecycle transition.

---

## STEP 5 — Canonical Event and L2 Representation

Priority: P0

Documents affected:

- 02 — Domain Models and Contracts
- 06 — Normalization and Integrity
- 06.1 — L2 BookStore and Synchronization
- 08 — Event Distribution and Unix Sockets
- 09 — Live Retention and Bootstrap Buffering
- 10 — Combined Streams and Currency Conversion
- 11 — PostgreSQL Recording
- 15 — Testing and Validation

Problem identified:

Some sections describe separate canonical types:

    L2Snapshot

    L2Update

Other sections correctly reference the existing
historical MarketForge canonical representation.

Required changes:

- Standardize canonical event terminology.
- Preserve existing Trade and L2LevelUpdate semantics.
- Define how snapshot initialization is represented.
- Define atomic native depth-message boundaries.
- Separate canonical payload from Live-specific metadata.
- Clarify initialization versus incremental updates.
- Clarify book generation and reconstruction requirements.
- Remove examples that imply competing canonical models.

Completion criteria:

The same canonical event representation is used for:

    Historical Processing

    Live Processing

    Publication

    Retention

    Recording

    Derived Streams

No subsystem invents its own canonical L2 type.

---

## STEP 6 — Error, Recovery, Generation, and Cursor Contracts

Priority: P0

Documents affected:

- 01 — Architecture and Module Boundaries
- 02 — Domain Models and Contracts
- 05.1 — Connection Manager and Transport Lifecycle
- 06.1 — L2 BookStore and Synchronization
- 06.2 — Trade Processing and Continuity
- 07 — Recovery Coordination and System Resilience
- 08 — Event Distribution and Unix Sockets
- 09 — Live Retention and Bootstrap Buffering

Problems identified:

1. FailureClass examples differ between documents.
2. Several distinct generation and epoch concepts
   require consistent naming.
3. Retention uses cursor-epoch terminology that must be
   aligned with the authoritative StreamCursor contract.
4. Exact persistence and restart semantics require closure.

Required changes:

- Establish one shared failure taxonomy.
- Separate failure classification from recovery disposition.
- Standardize generation types.
- Standardize StreamCursor identity.
- Standardize LogEpoch terminology.
- Specify cursor allocation and restart-safety requirements.
- Clarify recovery ownership.
- Clarify what successful recovery actually guarantees.
- Define handling of obsolete asynchronous results.

Completion criteria:

Every module can reuse the same identity, failure,
generation, and recovery contracts.

---

## STEP 7 — Configuration Paths and Instrument Metadata

Priority: P0

Documents affected:

- 03 — Configuration System
- 04 — Exchange Protocols and Connectivity
- 06 — Normalization and Integrity
- 13 — Runtime Configuration and Control API
- 16 — External Libraries and Code Reuse

Problems identified:

1. Some examples use config/live/.
2. Other sections use data/.config/live/.
3. Persistent desired-state storage and static configuration
   need clearly separated physical locations.
4. Instrument metadata loading and Python independence
   need one consistent startup contract.

Required changes:

Standardize:

    data/.config/live/

for the selected static configuration layout.

Explicitly distinguish:

    Static Exchange Specifications

    Runtime Desired Configuration

    Runtime-Generated State

    Instrument Metadata Snapshot

Clarify:

- Python remains the authoritative instrument metadata owner.
- Rust uses validated immutable metadata snapshots.
- The Live daemon starts without Python running.
- Runtime metadata updates use explicit revision boundaries.
- Configuration compilation requires validated metadata.

Completion criteria:

There is one documented configuration layout.

Startup does not depend on a running Python process.

No module independently invents instrument metadata.

---

# PHASE B — Close Cross-Subsystem Contracts

## STEP 8 — Exchange Protocol Verification and Capability Status

Priority: P0

Documents affected:

- 03 — Configuration System
- 04 — Exchange Protocols and Connectivity
- 05.1 — Connection Manager and Transport Lifecycle
- 06 — Normalization and Integrity
- 06.1 — L2 BookStore and Synchronization
- 06.2 — Trade Processing and Continuity
- 15 — Testing and Validation

Required changes:

- Distinguish proposed protocol configurations from verified ones.
- Establish a capability verification matrix.
- Identify requirements that must be confirmed against
  official exchange specifications.
- Define fixture requirements for each exchange/API family.
- Clarify exchange-specific quantity semantics.
- Clarify supported recovery capabilities.
- Define activation restrictions for unresolved protocols.

Explicit unresolved cases:

    Bitget Inverse L2 Quantity Unit

    Gate.io Futures REST Rate Limits

    Any Unverified Exchange-Specific
    Sequencing Or Subscription Semantics

Completion criteria:

AI agents cannot treat drafted TOML configurations
as verified production protocols.

Unresolved normalization-critical requirements block activation.

---

## STEP 9 — Event Distribution and Wire Protocol Completion

Priority: P0

Documents affected:

- 02 — Domain Models and Contracts
- 08 — Event Distribution and Unix Sockets
- 09 — Live Retention and Bootstrap Buffering
- 11 — PostgreSQL Recording
- 13 — Runtime Configuration and Control API
- 15 — Testing and Validation

Required changes:

- Finalize the publication frame envelope.
- Finalize serialization strategy.
- Define canonical event grouping in IPC.
- Define event cursor serialization.
- Define protocol-version compatibility.
- Define subscriber filtering.
- Define delivery acknowledgement boundaries.
- Define replay-and-follow behavior.
- Clarify required versus best-effort delivery.
- Standardize explicit backpressure outcomes.

Important:

The data socket and control socket may share
general framing utilities.

They must not share incompatible application protocols.

Completion criteria:

Publisher and subscriber implementations can be developed
independently using one frozen wire contract.

---

## STEP 10 — Retention, Recording, and Durability Boundaries

Priority: P0

Documents affected:

- 02 — Domain Models and Contracts
- 08 — Event Distribution and Unix Sockets
- 09 — Live Retention and Bootstrap Buffering
- 11 — PostgreSQL Recording
- 15 — Testing and Validation

Required changes:

Standardize the meaning of:

    Event Accepted

    Retention Admitted

    RAM Available

    Parquet Committed

    Crash Durable

    Recording Submitted

    PostgreSQL Committed

    Contiguously Committed

Clarify:

- Cursor intervals.
- Retention reservations.
- Parquet segment commitment.
- Manifest recovery.
- Database idempotency.
- Recording commit frontiers.
- Missing coverage intervals.
- Archive-to-live compatibility.
- Crash recovery.

Do not claim durability that the selected implementation
does not provide.

Completion criteria:

Every subsystem knows exactly what each acknowledgement
and committed cursor position means.

---

## STEP 11 — Recovery Coordination and Resource Ownership

Priority: P1

Documents affected:

- 05 — Stream Registry and Subscriptions
- 05.1 — Connection Manager and Transport Lifecycle
- 06.1 — L2 BookStore and Synchronization
- 06.2 — Trade Processing and Continuity
- 07 — Recovery Coordination and System Resilience

Required changes:

Verify one authoritative owner for:

    Physical Reconnection

    Subscription Restoration

    Trade Recovery

    L2 Resynchronization

    Shared Rate Limits

    Recovery Operation Identity

    Retry Scheduling

Clarify:

- Failure reporting versus failure ownership.
- Automatic recovery versus manual recovery.
- Recovery cancellation.
- Generation fencing.
- Recovery escalation.
- REST request scheduling.
- Shared exchange cooldowns.
- Operation completion criteria.

Completion criteria:

No two modules independently supervise
the same recovery operation or physical resource.

---

## STEP 12 — Derived Streams, Ordering, and Currency Conversion

Priority: P1

Documents affected:

- 02 — Domain Models and Contracts
- 03 — Configuration System
- 10 — Combined Streams and Currency Conversion
- 11 — PostgreSQL Recording
- 12 — Metrics and Stream Health
- 15 — Testing and Validation

Required changes:

- Standardize derived-stream identity.
- Standardize derived generations.
- Clarify nested derived-stream dependencies.
- Define deterministic source ranking.
- Define watermark and finalized-frontier semantics.
- Clarify late-event handling.
- Preserve L2 message atomicity.
- Distinguish merging from order-book consolidation.
- Standardize conversion reference eligibility.
- Distinguish missing conversion paths from unavailable prices.
- Preserve historical conversion arithmetic and provenance.

Completion criteria:

Historical and Live derived processing follow
compatible economic and ordering semantics.

All supported transformations have complete dependency
and failure contracts.

---

## STEP 13 — Metrics, Health, Control, and TUI Alignment

Priority: P1

Documents affected:

- 12 — Metrics and Stream Health
- 13 — Runtime Configuration and Control API
- 14 — Ratatui Management Interface
- 15 — Testing and Validation
- 16 — External Libraries and Code Reuse

Required changes:

- Standardize metric and health snapshot terminology.
- Distinguish authoritative state from diagnostic measurements.
- Ensure TUI views use existing Control API capabilities.
- Ensure every supported TUI action has an authoritative owner.
- Standardize runtime configuration policy classifications.
- Verify settings marked hot-editable are actually supported.
- Verify restart-required settings remain restart-required.
- Preserve exactly five top-level TUI views.
- Keep optional Prometheus independent of native metrics.

Completion criteria:

No UI feature requires direct runtime-state access.

No monitoring component implements competing health logic.

Every configured operation has a consistent runtime policy.

---

# PHASE C — Prepare Documentation for Autonomous Implementation

## STEP 14 — Historical Code Reuse and Dependency Audit

Priority: P0 before implementation

Primary document:

- 16 — External Libraries and Code Reuse

Supporting documents:

- 01 — Architecture and Module Boundaries
- 02 — Domain Models and Contracts
- 06 — Normalization and Integrity
- 06.1 — L2 BookStore and Synchronization
- 10 — Combined Streams and Currency Conversion

Required changes:

Create an authoritative reuse inventory from
the actual MarketForge implementation.

For each component, identify:

    Existing Source Path

    Existing Public Types

    Existing Functions

    Existing Tests

    Actual Dependencies

    Reuse Classification

    Required Adaptations

    Compatibility Risks

Classifications:

    Reuse Unchanged

    Reuse With Adaptation

    Reference Only

    New Live Implementation

Important:

The architecture-document Repomix is insufficient
for this audit.

The actual historical Rust source and Cargo manifests
must be inspected separately.

Completion criteria:

Every module knows exactly which historical code it can reuse.

External crate choices are based on actual repository dependencies.

---

## STEP 15 — Shared Contract Freeze

Priority: P0 before implementation

Documents affected:

- 01 — Architecture and Module Boundaries
- 02 — Domain Models and Contracts
- 03 — Configuration System
- 08 — Event Distribution and Unix Sockets
- 13 — Runtime Configuration and Control API

Required changes:

Finalize implementation-critical shared contracts:

    Core Rust Identities

    StreamCursor

    Generation Types

    Canonical Event Envelope

    Error Taxonomy

    State Machines

    Configuration Schemas

    Control Wire Schema

    Publication Wire Schema

    EventSink Contract

    RetentionReader Contract

    Recording Commit Contract

    Metrics Snapshot Contract

The freeze applies to public contracts, not internal
implementation details.

Completion criteria:

Subsystem implementations do not need to invent
missing shared interfaces independently.

Any later breaking contract change requires
explicit impact review.

---

## STEP 16 — Module Completion and No-Backtracking Policy

Priority: P0 before autonomous implementation

Primary documents:

- 00 — Master Plan
- 01 — Architecture and Module Boundaries
- 15 — Testing and Validation
- 17 — Implementation Roadmap

Required changes:

Establish a formal module-completion policy.

Before implementation:

    All Approved v1 Requirements Identified

    Dependencies Identified

    Downstream Consumers Identified

    Public Interfaces Defined

    Failure Behavior Defined

    Resource Limits Defined

    Metrics Requirements Defined

    Configuration Requirements Defined

    Tests Defined

During implementation:

    Implement Complete Approved Module Scope

    Compile

    Test

    Repair

    Integrate

Before completion:

    All Approved Requirements Satisfied

    Contract Tests Pass

    Integration Tests Pass

    Failure Tests Pass

    Historical Regression Tests Pass

    No Known Required Functionality Deferred

Completion criteria:

A module cannot be marked complete while
already-planned v1 functionality is missing.

Reopening is permitted for defects,
performance improvements, or approved changes.

---

## STEP 17 — AI Agent Implementation Rules

Priority: P0 before unattended execution Supporting documents:

- 01 — Architecture and Module Boundaries
- 02 — Domain Models and Contracts
- 15 — Testing and Validation
- 16 — External Libraries and Code Reuse

Required changes:

Define explicit instructions for AI coding agents.

The instructions must establish:

    Authoritative Documentation

    Allowed File Modifications

    Module Ownership Boundaries

    Dependency Restrictions

    Code Reuse Requirements

    Testing Requirements

    Completion Criteria

    Escalation Conditions

    Progress Reporting

### Agent Responsibilities

Agents may independently:

- Inspect existing code.
- Implement approved functionality.
- Create internal implementation details.
- Add required tests.
- Run compilation and validation.
- Fix compiler errors.
- Fix test failures.
- Refactor code within approved boundaries.
- Run deterministic integration tests.
- Document implementation decisions.
- Commit completed work to isolated branches.

### Restricted Operations

Agents must not independently:

- Change authoritative canonical contracts.
- Change accepted financial semantics.
- Introduce incompatible public interfaces.
- Modify unrelated completed modules.
- Disable or remove failing tests.
- Weaken integrity or durability guarantees.
- Introduce unapproved external dependencies.
- Rewrite the architecture to simplify implementation.
- Modify production databases.
- Access production credentials.
- Declare modules complete with missing requirements.

These changes require explicit approval.

### Agent Completion Protocol

For every completed work package:

    Implementation Finished
              |
              v
    Formatting And Compilation
              |
              v
    Unit Tests
              |
              v
    Contract Tests
              |
              v
    Integration Tests
              |
              v
    Historical Regression Tests
              |
              v
    Independent Validation
              |
              v
    Work Package Completion

An AI-generated completion message is not sufficient evidence.

Completion requires verifiable test results.

### Agent Failure Protocol

If an agent cannot complete a task:

- Preserve the current branch.
- Record the failing requirement.
- Record attempted solutions.
- Record relevant compiler or test errors.
- Record any architectural uncertainty.
- Stop after the configured retry budget.
- Mark the work package as blocked.
- Do not silently reduce the scope.

Completion criteria:

An autonomous agent can work on an approved module
without repeatedly asking the user to perform ordinary
code edits, compilation, or test execution.

Architectural changes remain explicitly controlled.

---

## STEP 18 — Implementation Work Package Format

Priority: P0 before autonomous implementation

Primary document:

- 17 — Implementation Roadmap

Supporting documents:

- 00 — Master Plan
- 01 — Architecture and Module Boundaries
- 15 — Testing and Validation
- 16 — External Libraries and Code Reuse

Required changes:

Define one standard format for implementation work packages.

Each package must contain:

    Package Identifier

    Module Name

    Objective

    Approved v1 Scope

    Authoritative Documentation

    Existing Code To Reuse

    Required Dependencies

    Public Interfaces

    Internal Responsibilities

    Required Configuration

    Required Metrics

    Error And Recovery Semantics

    Resource Limits

    Downstream Consumers

    Required Tests

    Forbidden Changes

    Completion Criteria

### Module Completeness

A work package must cover the complete approved
functionality of the target module.

It may be implemented through several internal tasks.

However, those tasks must belong to the same
approved completion scope.

Example:

    Connection Manager
            |
            +-- Connection Pooling
            |
            +-- Reader / Writer Ownership
            |
            +-- Heartbeats
            |
            +-- Generation Fencing
            |
            +-- Reconnection
            |
            +-- Rate-Limit Integration
            |
            +-- Resource Limits
            |
            +-- Cancellation
            |
            +-- Shutdown
            |
            +-- Metrics
            |
            +-- Fault-Injection Tests

The module is not complete merely because
WebSocket connections can be established.

### Dependency Closure

Before starting a package, inspect all
planned downstream consumers.

For example:

    Connection Manager

    Downstream Consumers:
        Subscription Manager
        Recovery Coordinator
        Stream Registry
        Runtime Supervisor
        Metrics Registry
        Control API

The package must include the interfaces
already required by these consumers.

### Work Package Isolation

Each package should specify:

    Allowed Files

    Shared Files Requiring Approval

    Expected Public API

    Required Test Commands

    Expected Output Artifacts

This reduces accidental cross-module changes.

Completion criteria:

Every implementation package has sufficient
information to complete its approved v1 scope
without discovering previously documented
requirements after the module is considered finished.

---

## STEP 19 — Automated Verification and Progress Tracking

Priority: P1 before unattended execution

Primary documents:

- 15 — Testing and Validation
- 17 — Implementation Roadmap

Required changes:

Define a persistent implementation-progress system.

The system should track:

    Planned

    Ready

    InProgress

    Blocked

    ImplementationComplete

    VerificationFailed

    Verified

    Accepted

### Progress Persistence

Progress must survive:

- Agent context resets.
- Interrupted executions.
- Computer restarts.
- Coding-tool changes.
- Model changes.
- Failed implementation attempts.

The system must not rely exclusively
on the AI model remembering previous sessions.

### Verification Records

For every completed package, record:

    Package ID

    Git Commit

    Implementation Files

    Public Interface Changes

    Tests Executed

    Test Results

    Remaining Limitations

    Acceptance Status

### Automated Validation

The coding environment should run:

    cargo fmt --check

    cargo clippy

    cargo test

    Relevant Integration Tests

    Relevant Historical Regression Tests

Additional checks may include:

    Dependency Audits

    Architecture Boundary Checks

    Property-Based Tests

    Performance Benchmarks

The exact commands must be verified against
the actual Rust workspace.

### Failure Handling

A failed check must produce a structured result.

The agent may repair ordinary implementation defects.

The agent must not modify acceptance requirements
solely to eliminate the failure.

Completion criteria:

Progress and verification results remain available
independently of the coding agent's context window.

---

# PHASE D — Final Documentation Verification

## STEP 20 — Cross-Document Terminology and Reference Audit

Priority: P1

Documents affected:

- 00–16

Required changes:

Perform a systematic consistency audit.

### Document References

Verify:

    Document Numbers

    Document Titles

    Section References

    File Paths

    Module References

    Type Names

    Interface Names

Remove obsolete references.

### Terminology

Standardize terms such as:

    SourceStreamKey

    StreamId

    DerivedStreamId

    ConnectionId

    ConnectionGeneration

    SubscriptionGeneration

    BookGeneration

    DerivedGeneration

    LogEpoch

    StreamCursor

    ConfigRevisionId

    OperationId

    RequestId

Avoid using several names for the same concept.

### Ownership

Check that every authoritative resource
has exactly one owner.

### Configuration

Check that settings use consistent:

    Names

    Defaults

    Types

    Validation Rules

    Inheritance Rules

    Runtime Application Policies

### Cross-Subsystem Contracts

Verify that producer and consumer documents
describe compatible inputs and outputs.

Examples:

    Connection Manager
        -> Subscription Manager

    Subscription Manager
        -> Stream Processor

    Stream Processor
        -> Event Router

    Event Router
        -> Recording Manager

    Event Router
        -> Retention Manager

    Retention Manager
        -> Bootstrap Consumer

    Metrics Registry
        -> Control API

    Control API
        -> Ratatui

### Superseded Examples

Identify examples that no longer represent
the selected architecture.

Either:

    Update Example

or:

    Explicitly Mark As Historical / Superseded

Agents must not be left to choose between
two contradictory examples.

Completion criteria:

No known cross-document contradiction remains
in an implementation-critical contract.

---

## STEP 21 — Documentation Implementation-Readiness Audit

Priority: P0 before implementation begins

Documents affected:

- 00–16

Required changes:

Perform a final audit from the perspective
of an autonomous coding agent.

For every subsystem, answer:

1. What must be implemented?
2. What already exists?
3. What must be reused?
4. What is explicitly out of scope?
5. Which module owns the functionality?
6. Which interfaces must be provided?
7. Which other modules consume those interfaces?
8. Which configuration fields are required?
9. Which failures must be handled?
10. Which resource limits apply?
11. Which metrics and health information are required?
12. Which tests establish correctness?
13. What constitutes completion?
14. What decisions remain unresolved?

### Readiness Classification

Each subsystem receives one status:

    ReadyForImplementation

    ContractIncomplete

    DependencyBlocked

    ProtocolVerificationRequired

    RequiresExistingCodeAudit

    ExplicitlyDeferred

### Critical Contract Gate

A module cannot begin autonomous implementation
when its required public contract is unresolved.

It may begin when unrelated or optional
implementation details remain open.

### Dependency Gate

A module cannot be marked ready when
its known downstream interface requirements
have not been evaluated.

### Historical Compatibility Gate

Shared historical functionality must be
identified before creating replacements.

### Acceptance Gate

Every planned module must have deterministic
acceptance criteria.

Completion criteria:

Every implementation package has a documented
readiness status.

No critical unresolved contract is silently
delegated to an AI coding agent.

---

# PHASE E — Final Implementation Roadmap

## STEP 22 — Create Document 17

Priority: Final

Document:

17 — Implementation Roadmap.md

This document should be created only after
the implementation-critical consistency work
has been completed.

### Implementation Philosophy

    Contract-Complete

    Test-First

    Dependency-Aware

    Incrementally Integrated

    AI-Agent Executable

    Minimal Avoidable Backtracking

### Roadmap Requirements

The roadmap must define:

    Implementation Phases

    Module Dependencies

    Work Packages

    Package Execution Order

    Parallelizable Work

    Integration Milestones

    Verification Gates

    Acceptance Criteria

    Estimated Complexity

    External Protocol Verification Gates

### Implementation Ordering

The implementation should preserve
dependency correctness.

Foundational contracts must precede
the components that depend on them.

However, integration tests should begin early.

Do not postpone all integration until
every subsystem has been completed.

### Vertical Integration

Early milestones should establish
small working end-to-end paths.

Example:

    Fake Exchange
          |
          v
    Real Protocol Decoder
          |
          v
    Real Normalization
          |
          v
    Real Integrity Processor
          |
          v
    Real Event Router
          |
          v
    Test Subscriber

Later milestones replace infrastructure
adapters with production implementations.

### Module Completion Policy

Each module is implemented against its
complete approved v1 specification.

Integration milestones do not authorize
omitting previously documented module functionality.

### Agent Execution

The roadmap must be suitable for:

    Local Ollama Agents

    OpenAI Codex

    Cursor Agents

    Human-Supervised Development

The implementation methodology must not
depend on one proprietary coding tool.

### Final Completion Criteria

The roadmap is complete when:

- Every module has an approved implementation package.
- Package dependencies are explicit.
- Execution order is defined.
- Integration milestones are testable.
- Autonomous execution boundaries are defined.
- Verification gates are established.
- All approved v1 requirements are assigned.
- No planned functionality is silently omitted.

---

# EXECUTION ORDER

The correction plan should be executed
in the following order.

| Step | Task | Priority |
|------|------|----------|
| 1 | Master Plan and documentation index | P0 |
| 2 | Configuration revision and persistence order | P0 |
| 3 | Control API transport consistency | P0 |
| 4 | Stream identity and lifecycle consistency | P0 |
| 5 | Canonical event and L2 representation | P0 |
| 6 | Error, recovery, generation, and cursor contracts | P0 |
| 7 | Configuration paths and instrument metadata | P0 |
| 8 | Exchange protocol verification and capabilities | P0 |
| 9 | Event distribution and wire protocols | P0 |
| 10 | Retention, recording, and durability boundaries | P0 |
| 11 | Recovery coordination and resource ownership | P1 |
| 12 | Derived streams and currency conversion | P1 |
| 13 | Metrics, health, control, and TUI alignment | P1 |
| 14 | Historical code reuse and dependency audit | P0 |
| 15 | Shared contract freeze | P0 |
| 16 | Module completion and no-backtracking policy | P0 |
| 17 | AI agent implementation rules | P0 |
| 18 | Implementation work package format | P0 |
| 19 | Automated verification and progress tracking | P1 |
| 20 | Cross-document terminology and reference audit | P1 |
| 21 | Final implementation-readiness audit | P0 |
| 22 | Write final Implementation Roadmap | Final |

Priorities describe implementation importance,
not permission to skip earlier dependencies.

Some steps may be completed together when
they affect the same architectural contract.

---

# HOW WE WILL APPLY CORRECTIONS

For each step:

1. Identify the affected documents.
2. Locate contradictory or outdated sections.
3. Establish the authoritative contract.
4. Print exact replacement Markdown.
5. Identify sections that should be removed or updated.
6. Check the change against dependent documents.
7. Confirm that the correction preserves architectural intent.
8. Proceed to the next step.

We should avoid rewriting entire documents
when targeted section replacements are sufficient.

If a correction changes a foundational decision,
all dependent sections must be reviewed before
that correction is considered complete.

---

# FINAL OBJECTIVE

Before implementation begins, MarketForge Live
should have:

    Consistent Documentation
              |
              v
    Complete Shared Contracts
              |
              v
    Verified Code Reuse Opportunities
              |
              v
    Explicit Module Requirements
              |
              v
    Executable Work Packages
              |
              v
    Automated Acceptance Tests
              |
              v
    Autonomous Implementation
              |
              v
    Verified Completed Modules

The objective is not to eliminate every future
code modification.

The objective is to eliminate avoidable
backtracking caused by incomplete planning,
contradictory documentation, or forgotten
requirements.

---

**End — Documentation Consistency and Implementation Readiness Plan**