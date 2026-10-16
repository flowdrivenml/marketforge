# MarketForge Live — Architecture and Module Boundaries

## 1. Purpose

Define the internal Rust architecture of MarketForge Live, including module responsibilities, dependency boundaries, object ownership, communication interfaces, and runtime composition.

The architecture must support independent subsystem development, deterministic testing, and future extensions without requiring modifications to unrelated components.

**Primary design objective:** Establish stable contracts before implementing concrete subsystems.

MarketForge Live is a continuously running, asynchronous Rust service responsible for exchange acquisition, normalization, integrity validation, recovery, derived streams, publication, optional PostgreSQL recording, and temporary event retention.

Historical processing and bootstrap orchestration remain separate applications.

## 2. Architectural Model

MarketForge Live follows a modular, contract-driven architecture with explicit separation between the data plane, control plane, and infrastructure services.

### 2.1 Data Plane

Responsible for processing market data with minimal latency and predictable resource consumption.

```text
Exchange WebSocket / REST
           |
           v
     Transport Layer
           |
           v
    Protocol Dispatcher
           |
           v
    Generic Normalization
           |
           v
   Integrity / Book State
           |
           v
    Canonical Event Router
           |
     +-----+----------+------------+
     |     |          |            |
     v     v          v            v
   Unix  Derived   PostgreSQL   Retention
 Socket  Streams    Recorder     Manager
```

The data plane must not depend on terminal rendering, configuration persistence, or synchronous metrics queries.

### 2.2 Control Plane

Responsible for configuration, stream lifecycle management, runtime reconciliation, and operational queries.

```text
Ratatui / External Client
            |
            v
      Control Socket
            |
            v
      Command Handler
            |
            v
    Configuration Manager
            |
            v
     Runtime Reconciler
            |
            v
      Stream Registry
            |
            v
   Connection / Stream Owners
```

Control operations must not execute inside the per-event processing path.

### 2.3 Infrastructure Services

Provide shared operational capabilities:

- Metrics and health-state aggregation.
- Structured diagnostics.
- Resource accounting.
- Persistent configuration.
- PostgreSQL recording.
- Parquet-backed retention.
- Runtime supervision and shutdown.

Infrastructure services expose narrow interfaces and must not acquire ownership of unrelated domain state.

## 3. Dependency Rules

Dependencies must follow a directed hierarchy.

```text
Applications
    |
    v
Runtime / Composition
    |
    v
Subsystem Interfaces
    |
    v
Domain Models / Shared Core
```

**Mandatory constraints:**

- `canonical`, `normalize`, `book`, and `instrument` must not depend on `live`.
- Historical and Live engines may reuse shared primitives but must not depend on each other's executors.
- Transport implementations must not depend on canonical normalization logic.
- Exchange protocol adapters must not depend on PostgreSQL, Ratatui, or publication transports.
- Stream processors must not depend on concrete database or Unix-socket implementations.
- Metrics collection must not determine data-integrity decisions.
- Ratatui must communicate through the control protocol rather than accessing live runtime internals.
- Bootstrap must consume public retention and stream interfaces without accessing internal buffers.

Avoid circular dependencies and global mutable state.

## 4. Component Ownership

Each mutable resource must have one authoritative logical owner.

| Resource                              | Owner                               |
| ------------------------------------- | ----------------------------------- |
| Physical WebSocket connections        | Connection Manager                  |
| REST request scheduling               | REST Client / Rate Limiter          |
| Subscription lifecycle                | Connection Subscription Manager     |
| Source-stream identity and demand     | Stream Registry                     |
| Per-stream processing state           | Source Stream Processor             |
| Order-book state                      | Source Stream Processor / BookStore |
| Recovery coordination                 | Recovery Coordinator                |
| Derived-stream dependencies           | Derived Stream Registry             |
| Combined-stream ordering state        | Individual Merger                   |
| Conversion reference state            | Reference Registry                  |
| Subscriber connections and queues     | Unix Publisher                      |
| Recording batches and commits         | PostgreSQL Recorder                 |
| Retained batches and Parquet segments | Retention Manager                   |
| Metrics counters and snapshots        | Metrics Registry                    |
| Desired configuration                 | Configuration Manager               |
| Runtime configuration transitions     | Runtime Reconciler                  |
| Worker lifecycle                      | Runtime Supervisor                  |

Ownership must remain independent of task scheduling.

Multiple Tokio tasks may cooperate within a subsystem, but mutable stream state must have one logical owner.

A WebSocket connection may carry multiple source streams. Connection ownership and source-stream ownership must therefore remain separate.

## 5. Rust Module Structure

The Live engine remains part of the existing `marketforge_engine` crate. Shared domain primitives must remain outside `live/` to prevent duplication between historical and live processing.

```text
engine/src/
├── canonical/              # Existing canonical event models
├── normalize/              # Existing normalization primitives
├── book/                   # Existing BookStore and sequencing
├── instrument/             # Existing instrument models
├── validate/               # Existing canonical validation
├── historical/             # Existing historical engine
│
├── storage/
│   └── parquet/            # Shared Arrow/Parquet primitives
│
├── live/
│   ├── mod.rs
│   ├── model/               # Identities, cursors, states, errors
│   ├── config/              # Configuration compilation and validation
│   ├── exchange/            # Protocol definitions and strategies
│   ├── transport/           # WebSocket, REST, retries, rate limits
│   ├── registry/            # Source ownership and demand tracking
│   ├── ingest/              # Decoding, normalization, validation
│   ├── recovery/            # Trade recovery and L2 synchronization
│   ├── derived/             # Merging and currency conversion
│   ├── publish/             # Unix-socket publication
│   ├── retention/           # RAM buffering and Parquet spill
│   ├── recording/           # PostgreSQL persistence
│   ├── metrics/             # Metrics, health, diagnostics
│   ├── control/             # Commands, queries, control protocol
│   └── runtime/             # Composition, supervision, shutdown
│
├── tui/
│   ├── theme.rs
│   ├── components/
│   ├── processing.rs
│   ├── merging.rs
│   ├── conversion.rs
│   └── live/
│       ├── dashboard.rs
│       ├── streams.rs
│       ├── derived.rs
│       ├── recording.rs
│       └── settings.rs
│
└── bin/
    ├── marketforge-live.rs
    └── marketforge-live-tui.rs
```

This represents the target architecture, not a requirement to create every file immediately.

Modules should initially expose minimal public interfaces. Internal implementation details remain private or `pub(crate)`.

Shared historical/live functionality should be extracted incrementally, preserving existing behavior and regression tests.

## 6. Subsystem Interfaces

Subsystems communicate through typed contracts rather than direct access to implementation details.

### 6.1 Interface Categories

| Interface            | Responsibility                                    | Typical Implementation                   |
| -------------------- | ------------------------------------------------- | ---------------------------------------- |
| `WebSocketTransport` | Establish connections and exchange frames         | Tokio/Tungstenite                        |
| `RestTransport`      | Execute HTTP requests                             | Reqwest                                  |
| `ProtocolDecoder`    | Convert raw messages into typed exchange records  | Compiled format specification            |
| `StreamProcessor`    | Normalize and validate source records             | Trade/L2 processor                       |
| `StreamRegistry`     | Manage unique source acquisition and dependencies | Concurrent registry                      |
| `EventSink`          | Accept canonical event batches                    | Publication/recording/retention adapters |
| `RetentionReader`    | Read retained events by cursor                    | RAM/Parquet                              |
| `ConfigStore`        | Persist configuration revisions                   | Local filesystem                         |
| `MetricsReader`      | Retrieve immutable operational snapshots          | Metrics registry                         |
| `ControlClient`      | Execute management commands and queries           | Unix control socket                      |

These are conceptual interface names. Exact Rust signatures belong in `02 — Domain Models and Contracts.md`.

### 6.2 Interface Design Rules

- Use concrete structs and enums for domain models and performance-critical processing.
- Introduce traits primarily at replaceable infrastructure boundaries.
- Avoid unnecessary dynamic dispatch in per-event processing.
- Prefer immutable configuration and typed messages over shared mutable objects.
- Use bounded channels for asynchronous communication.
- Make failure, cancellation, and backpressure explicit in interface contracts.
- Avoid callbacks that expose unrelated subsystem internals.
- Define serialization boundaries independently of Rust-specific memory layouts.

Interfaces must support deterministic test implementations without requiring production infrastructure.

### 6.3 Dependency Injection

Dependencies are assembled by the runtime composition root.

```rust
let runtime = LiveRuntimeBuilder::new()
    .with_config(config)
    .with_transport(transport)
    .with_storage(storage)
    .with_metrics(metrics)
    .build()?;
```

Illustrative API; concrete signatures remain undecided.

Production implementations use actual WebSocket, REST, PostgreSQL, and filesystem adapters.

Tests substitute deterministic transports, in-memory configuration stores, event collectors, and controlled failure implementations.

The application must not require conditional production logic to support testing.

## 7. Runtime Composition

`LiveRuntime` is the top-level lifecycle coordinator.

Its responsibilities are limited to:

- Constructing and connecting subsystems.
- Starting independent services.
- Supervising long-running workers.
- Coordinating configuration reconciliation.
- Propagating shutdown and cancellation.
- Reporting fatal runtime failures.
- Ensuring bounded resource cleanup.

It must not implement exchange parsing, normalization, book synchronization, database writing, or terminal rendering.

### 7.1 Runtime Construction

```text
Validated Configuration
          |
          v
   LiveRuntimeBuilder
          |
          +-- Metrics Registry
          +-- Configuration Manager
          +-- Stream Registry
          +-- Connection Manager
          +-- Recovery Coordinator
          +-- Event Router
          +-- Derived Stream Registry
          +-- Retention Manager
          +-- PostgreSQL Recorder
          +-- Unix Publisher
          +-- Control Server
          |
          v
      LiveRuntime
          |
          v
       Supervisor
```

Optional components must support disabled configurations without requiring unrelated modules to change.

### 7.2 Worker Isolation

Workers are supervised independently according to their failure domains.

A failed exchange connection must not terminate unrelated connections.

A failed source-stream processor must not invalidate unrelated streams.

A failed subscriber must not block publication to other consumers.

A failed recorder or retention writer must report the affected delivery/durability guarantee without silently discarding required events.

Tokio task isolation does not provide protection against process-wide failures. Process restart is delegated to the operating-system service manager.

### 7.3 Shutdown

Shutdown follows a coordinated lifecycle:

```text
Shutdown Requested
        |
        v
Stop Accepting New Control Operations
        |
        v
Stop New Acquisition / Subscriptions
        |
        v
Finalize or Explicitly Abort Pending Obligations
        |
        v
Flush Recording / Retention Within Deadline
        |
        v
Close Subscriber Connections
        |
        v
Release Resources
        |
        v
Runtime Terminated
```

Shutdown deadlines and incomplete durability obligations must be reported explicitly.

Graceful shutdown does not imply that every buffered event was durably committed.

## 8. Inter-Component Communication

Communication mechanisms must reflect ownership, throughput, ordering, and reliability requirements.

### 8.1 Communication Mechanisms

| Mechanism                | Application                                          |
| ------------------------ | ---------------------------------------------------- |
| Direct function calls    | Synchronous operations within one owner              |
| Bounded Tokio channels   | Asynchronous worker communication                    |
| Immutable `Arc<T>`       | Shared configuration and read-only metadata          |
| Atomic counters          | High-frequency metrics                               |
| Typed operational events | Lifecycle transitions, integrity incidents, recovery |
| Unix domain sockets      | External publication and control                     |
| Persistent storage       | Recording, retention, configuration revisions        |

Avoid shared mutable state when ownership transfer through channels is sufficient.

### 8.2 Channel Contracts

Every asynchronous channel must define:

- Producer and consumer ownership.
- Message type and ordering guarantees.
- Maximum capacity and memory budget.
- Backpressure behavior.
- Cancellation and shutdown semantics.
- Behavior when either endpoint fails.
- Whether successful enqueueing constitutes delivery, acceptance, or durability.

**No bounded channel may silently discard canonical events.**

If capacity exhaustion prevents a required operation, the owning subsystem must report the failure and update the affected stream's integrity or durability state.

### 8.3 Event Distribution

The canonical event router distributes accepted events to independently managed consumers.

```text
                  Source Stream
                       |
                       v
                Canonical Event
                       |
                       v
                  Event Router
                       |
          +------------+------------+
          |            |            |
          v            v            v
      Publisher     Recording    Retention
          |
          v
      Subscribers
```

Derived-stream processors register as internal consumers and produce new logical streams.

Routing must preserve per-source event order and expose explicit acceptance/failure results.

Required sinks and best-effort subscribers must have distinct delivery policies.

The router must not synchronously wait for PostgreSQL transactions, Parquet compression, or slow external subscribers.

A strict durability requirement may nevertheless impose an explicit admission barrier before an event is considered durably accepted.

### 8.4 Operational Communication

High-frequency measurements use counters and bounded latency histograms.

Low-frequency operational incidents use typed events.

Examples:

```text
ConnectionEstablished
ConnectionLost
SubscriptionActivated
SequenceGapDetected
BookInvalidated
RecoveryStarted
RecoveryCompleted
RecordingFailed
RetentionCapacityExceeded
ConfigurationApplied
```

Operational events must not be confused with canonical market-data events.

Metrics collection and diagnostics must not determine whether market data is valid.

## 9. Error and Failure Boundaries

Failures must be represented through typed errors rather than string matching.

### 9.1 Error Classification

```rust
enum FailureClass {
    Transient,
    RateLimited,
    Configuration,
    Protocol,
    Integrity,
    ResourceExhaustion,
    Persistence,
    Internal,
}
```

Each failure should carry sufficient context to identify its source, affected resource, and applicable recovery policy.

### 9.2 Failure Ownership

| Failure | Responsible Component |
|---|---|
| WebSocket disconnection | Connection Supervisor |
| Heartbeat timeout | Connection Supervisor |
| Subscription rejection | Subscription Manager |
| REST rate limiting | Rate Limiter / Request Scheduler |
| Malformed exchange message | Protocol Decoder / Stream Processor |
| Canonical normalization failure | Stream Processor |
| L2 sequence discontinuity | Stream Processor / Recovery Coordinator |
| Invalid order-book state | Stream Processor |
| Trade coverage gap | Recovery Coordinator |
| Slow external subscriber | Unix Publisher |
| PostgreSQL failure | Recording Supervisor |
| Parquet spill failure | Retention Manager |
| Invalid configuration update | Configuration Manager |
| Worker termination | Runtime Supervisor |

A failure must be handled at the narrowest valid ownership boundary.

Failures may propagate to dependent resources when their guarantees are affected, but must not unnecessarily invalidate unrelated streams.

### 9.3 Failure Reporting

Every significant failure produces:

1. A typed error or operational incident.
2. An appropriate state transition.
3. Relevant metrics updates.
4. A recovery action or explicit terminal failure.
5. Diagnostic context sufficient for investigation.

Recovery must not silently erase the historical record of an integrity incident.

## 10. Extension Boundaries

The architecture must accommodate new functionality through existing contracts wherever possible.

### 10.1 New Exchange or Market Family

Adding an exchange integration should primarily require:

- Exchange protocol configuration.
- Raw-message format specifications.
- Instrument metadata resolution.
- Protocol fixtures and tests.
- Specialized strategies only when generic strategies are insufficient.

It must not require modifications to PostgreSQL recording, Unix publication, Ratatui, or canonical event routing.

### 10.2 New Derived Stream

A new transformation should integrate through the derived-stream registry.

```text
Source / Derived Dependencies
             |
             v
      Derived Processor
             |
             v
       Canonical Output
             |
             v
        Event Router
```

Derived processors must declare their dependencies, accepted event schemas, output semantics, ordering requirements, and failure behavior.

Dependency cycles must be rejected.

### 10.3 New Storage Backend

Storage implementations consume canonical events through stable interfaces.

Introducing another recording or retention backend must not require changes to exchange acquisition or normalization.

### 10.4 New Management Interface

Management clients consume the versioned control protocol.

A future HTTP interface, CLI extension, or Bootstrap TUI must not require direct access to runtime internals.

### 10.5 New Metrics

Metrics schemas and snapshots should be extensible without modifying presentation-specific code inside processing modules.

The owning subsystem remains responsible for producing measurements.

---

## 11. Testing and Verification Boundaries

Every subsystem must support independent deterministic testing without requiring live exchange connections, PostgreSQL, or an active terminal interface.

### 11.1 Testing Architecture

```text
                  TEST HARNESS
                       |
          +------------+------------+
          |            |            |
          v            v            v
     Fake Network  Fake Storage  Fake Clock
          |            |            |
          +------------+------------+
                       |
                       v
                 Live Runtime
                       |
                       v
                Event Collector
                       |
                       v
              Invariant Validation
```

Production and test implementations must satisfy the same interfaces.

Tests must exercise actual subsystem behavior rather than duplicate production logic in test-specific implementations.

### 11.2 Testing Responsibilities

| Subsystem          | Required Verification                                               |
| ------------------ | ------------------------------------------------------------------- |
| Configuration      | Schema validation, inheritance, revisions, invalid updates          |
| Transport          | Connection lifecycle, timeouts, retries, cancellation               |
| Exchange Protocols | Message classification, subscriptions, heartbeat handling           |
| Registry           | Unique acquisition, concurrent requests, demand release             |
| Ingestion          | Canonical correctness, strict parsing, instrument semantics         |
| Integrity          | Sequence continuity, duplicates, invalidation, snapshot transitions |
| Recovery           | Reconnection, stale generations, trade repair, L2 resynchronization |
| Derived Streams    | Ordering, late events, dependency validity, conversion correctness  |
| Publication        | Framing, ordering, subscriber isolation, backpressure               |
| Retention          | Buffer limits, spill correctness, cursor continuity, replay         |
| Recording          | Batch integrity, idempotency, transaction failure, recovery         |
| Metrics            | Counter correctness, health transitions, bounded overhead           |
| Control            | Command validation, concurrent revisions, reconciliation            |
| Runtime            | Startup, shutdown, worker failures, resource cleanup                |

### 11.3 Shared Test Infrastructure

Reusable testing components should include:

```text
FakeWebSocket
FakeRestClient
FakeClock
FakeConfigStore
FakeEventPublisher
FakeRecordingSink
FakeRetentionStore
EventCollector
FailureInjector
TestRuntimeBuilder
```

The fake transport must support deterministic injection of disconnections, malformed messages, sequence gaps, delayed responses, heartbeat failures, and rate-limit errors.

Timing-dependent tests should use controlled clocks rather than arbitrary real-time sleeps.

### 11.4 Historical Regression Compatibility

Shared components must preserve existing historical behavior.

Any modification to canonical models, normalization, `BookStore`, validation, or Parquet serialization requires execution of the corresponding historical regression tests.

Where equivalent source information exists, historical and live normalization must produce identical canonical values.

Live-specific metadata must not alter established historical numeric semantics.

### 11.5 Integration Testing

Subsystem integration must be validated incrementally.

```text
Fake Exchange
      |
      v
Real Protocol Decoder
      |
      v
Real Normalization
      |
      v
Real Integrity Validation
      |
      v
Real Event Router
      |
      +---- Test Publisher
      |
      +---- Test Recorder
      |
      +---- Test Retention
```

Production adapters replace test implementations without changing the underlying processing contracts.

End-to-end tests must verify event counts, identities, ordering, continuity, and failure propagation.

Performance benchmarks remain separate from deterministic correctness tests.

---

## 12. Architectural Invariants

The following invariants apply across all subsystems.

### 12.1 Acquisition

- Equivalent source requests must not create duplicate logical acquisitions.
- Every source stream has one authoritative processing-state owner.
- Connection ownership remains independent of stream ownership.
- Reconnection must not implicitly establish data continuity.
- Stale asynchronous operations must not modify newer stream generations.

### 12.2 Data Integrity

- Accepted canonical events preserve source identity and provenance.
- Required sequence discontinuities must not be silently ignored.
- Invalid order books must not publish trusted state transitions.
- L2 updates must remain logically atomic.
- Recovery must distinguish current-state validity from historical completeness.
- Missing or malformed required fields must not produce fabricated canonical values.

### 12.3 Distribution

- Per-source ordering must be preserved.
- Subscriber failures must not interrupt unrelated consumers.
- Every bounded queue must have an explicit overflow policy.
- Required delivery failures must be observable.
- Publication must not implicitly depend on PostgreSQL availability.
- A successful socket write must not be interpreted as confirmed consumer processing.

### 12.4 Retention and Persistence

- Stream cursors remain stable across RAM-to-Parquet transitions.
- Committed segments must never expose incomplete Parquet files.
- Retention must not reclaim data protected by active reservations.
- PostgreSQL replay must not create duplicate logical records.
- Uncommitted data must not be reported as durable.
- Capacity exhaustion must explicitly invalidate affected retention guarantees.

### 12.5 Configuration and Observability

- Configuration changes must pass through validation and reconciliation.
- Unrelated streams must remain unaffected by localized changes.
- Configuration revisions and stream generations must remain distinct.
- Metrics and terminal rendering must not influence canonical processing results.
- Closing the TUI must not terminate or interrupt the Live daemon.
- Runtime state must remain distinguishable from desired configuration.

---

## 13. Architectural Decisions and Deferred Details

### Established Decisions

| Area                 | Decision                                                     |
| -------------------- | ------------------------------------------------------------ |
| Language             | Rust                                                         |
| Async runtime        | Tokio                                                        |
| Architecture         | Modular, contract-driven, explicit ownership                 |
| Exchange integration | Declarative protocols with reusable strategies               |
| Acquisition          | Central registry with shared source ownership                |
| Processing           | Reuse historical canonical and normalization primitives      |
| Concurrency          | Independently supervised tasks with bounded communication    |
| Publication          | Unix domain sockets                                          |
| Recording            | Optional PostgreSQL                                          |
| Retention            | Bounded RAM with asynchronous Parquet spill                  |
| Derived streams      | Merging and currency conversion                              |
| Metrics              | Native per-stream monitoring                                 |
| Management           | Independent Ratatui client                                   |
| Control              | Versioned Unix-socket protocol                               |
| Bootstrap            | Separate application consuming Live interfaces               |
| Testing              | Deterministic, subsystem-oriented, extensive fault injection |

### Details Requiring Dedicated Specifications

The following must be resolved in their respective documents:

- Exact Rust domain types and serialization contracts.
- Cursor persistence and producer-epoch semantics.
- Concrete connection-supervisor implementation.
- Exchange-specific protocol strategy interfaces.
- Event-router delivery and acknowledgment policies.
- Backpressure behavior under required-sink failure.
- PostgreSQL transaction and replay checkpoints.
- Retention segment manifests and crash recovery.
- Configuration revision persistence and reconciliation.
- Derived-stream dependency and generation semantics.
- Final metrics snapshot schemas.
- Concrete test harness APIs and performance thresholds.

These details must not be independently invented by individual subsystem implementations.

---

## 14. Implementation Boundary Rules

Implementation should proceed through independently testable components assembled through stable interfaces.

**Before implementing a subsystem:**

- Its responsibility and ownership must be defined.
- Its public input/output contracts must be established.
- Its dependencies must be explicit.
- Its failure and cancellation behavior must be specified.
- Its resource limits and backpressure policy must be understood.
- Its deterministic tests must be planned.

**Before considering a subsystem complete:**

- Unit and contract tests must pass.
- Relevant integration tests must pass.
- Failure behavior must be verified.
- Metrics and health reporting must be implemented where applicable.
- Public interfaces must be documented.
- Existing historical regression tests must remain successful.
- Performance must be measured where the subsystem participates in the high-frequency path.

Implementation details may evolve, but changes to shared contracts require explicit review of dependent subsystems.

---

## 15. Final Architectural Rule

MarketForge Live must be assembled from independently owned, narrowly coupled subsystems.

The runtime coordinates components but does not absorb their responsibilities.

The stream registry manages acquisition ownership but does not process every market event.

The connection manager transports exchange messages but does not determine canonical market-data validity.

The stream processor owns normalization and integrity state but does not implement storage or terminal rendering.

The event router distributes accepted events but does not implement individual sink behavior.

The metrics system observes operations but does not determine processing correctness.

The control plane modifies desired runtime behavior but does not participate in per-event processing.

The Ratatui interface manages and observes the service without owning its execution lifecycle.

**Architectural completion criterion:** A new exchange adapter, derived-stream processor, storage backend, metric, or management interface can be introduced through established contracts without requiring structural changes to unrelated subsystems.

---
