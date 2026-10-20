# 16 — External Libraries and Code Reuse

## 1. Purpose

This document defines the dependency selection and code reuse strategy for MarketForge Live.

The objective is to implement a high-performance, fault-tolerant Rust market-data service while minimizing unnecessary dependencies, duplicated functionality, and long-term maintenance costs.

MarketForge already contains significant reusable infrastructure developed for historical market-data processing.

The Live implementation must reuse existing functionality wherever its semantics and performance characteristics are compatible.

External libraries should be adopted where they provide established, well-tested solutions to general infrastructure problems.

MarketForge-specific business logic, market-data integrity, and operational guarantees remain owned by MarketForge.

### 1.1 Primary Objectives

The dependency and reuse strategy must:

1. Preserve existing historical MarketForge behavior.
2. Reuse established canonical market-data models.
3. Reuse existing normalization primitives.
4. Reuse the shared BookStore implementation.
5. Reuse historical event grouping and validation.
6. Reuse compatible merge and currency-conversion logic.
7. Reuse Arrow and Parquet infrastructure.
8. Reuse existing Ratatui components and styling.
9. Select established Rust networking libraries.
10. Avoid unnecessary dependency duplication.
11. Keep external infrastructure optional where possible.
12. Preserve modularity and independent subsystem testing.
13. Minimize additional hot-path allocations and synchronization.
14. Support reproducible dependency resolution.
15. Keep the implementation maintainable as exchanges and protocols evolve.

### 1.2 Fundamental Design Principle

    Reuse Existing MarketForge Logic
                |
                v
    Reuse Established Rust Libraries
                |
                v
    Implement Missing Integration Logic
                |
                v
    Implement MarketForge-Specific Semantics

This ordering is a design preference, not an absolute rule.

An existing implementation should be reused only when its contracts match the Live requirements.

An external library should be selected only when it improves correctness, performance, maintainability, or implementation simplicity.

### 1.3 Dependency Categories

Dependencies are classified into:

    Existing MarketForge Components

    Core Rust Infrastructure

    Optional Infrastructure Adapters

    Development And Testing Dependencies

    External Reference Projects

These categories must remain distinct.

An external project used as architectural inspiration does not automatically become a runtime dependency.

### 1.4 Existing MarketForge Components

Existing components may include:

- Canonical event models.
- Exact Decimal normalization.
- Timestamp normalization.
- Instrument metadata.
- Trade processing primitives.
- BookStore.
- Sequence-validation primitives.
- Historical depth-event grouping.
- Arrow schemas and serialization.
- Parquet writers and readers.
- Historical merging.
- Currency conversion.
- Historical regression fixtures.
- Ratatui themes and widgets.

The existing repository remains the first source of implementation reuse.

### 1.5 External Infrastructure Dependencies

External libraries are appropriate for general-purpose functionality such as:

    Async Runtime

    WebSocket Transport

    HTTP Client

    TLS

    Serialization

    PostgreSQL Client

    Filesystem Utilities

    Structured Logging

    Terminal Rendering

    Metrics Exposition

    Property-Based Testing

MarketForge should not implement its own TLS stack, WebSocket protocol implementation, or PostgreSQL wire protocol.

### 1.6 MarketForge-Owned Logic

The following responsibilities must remain under MarketForge's architectural control:

    Source Stream Identity

    Subscription Deduplication

    Exchange Protocol Interpretation

    Canonical Normalization Semantics

    Trade Continuity

    L2 Synchronization

    Recovery Coordination

    Event Cursor Assignment

    Canonical Event Distribution

    Retention Coverage

    Recording Commit Coverage

    Derived Stream Ordering

    Currency Conversion Semantics

    Configuration Reconciliation

    Health Evaluation

External crates may provide useful implementation primitives.

They must not silently replace these authoritative contracts.

### 1.7 Non-Responsibilities

This document does not:

- Define canonical financial schemas.
- Redesign historical MarketForge processing.
- Implement exchange integrations.
- Establish additional external infrastructure requirements.
- Select dependencies solely by popularity.
- Require replacing existing working libraries.
- Mandate a particular crate version without compatibility verification.
- Guarantee that an external reference project can be copied directly.
- Define the final implementation schedule.

The implementation schedule belongs to the final Implementation Roadmap.

### 1.8 Fundamental Reuse Invariant

    Shared Business Semantics
    Must Have One
    Authoritative Implementation.

    Infrastructure May Have
    Replaceable Adapters.

---

## 2. Dependency Selection Philosophy

MarketForge Live is a long-running, high-throughput service.

Dependencies must be evaluated according to operational requirements rather than convenience alone.

### 2.1 Selection Criteria

Every substantial external dependency should be evaluated for:

| Criterion | Requirement |
|---|---|
| Correctness | Well-defined behavior and test coverage |
| Compatibility | Fits the existing Rust ecosystem |
| Performance | Suitable for the relevant workload |
| Maintenance | Actively maintained or otherwise stable |
| Reliability | Predictable failure behavior |
| Security | Appropriate vulnerability and update practices |
| Licensing | Compatible with MarketForge distribution |
| Integration | Does not require architectural ownership changes |
| Testability | Supports deterministic testing |
| Complexity | Provides sufficient value for its cost |
| Dependency footprint | Avoids unnecessary transitive dependencies |

No single criterion determines the decision.

### 2.2 Prefer Established Infrastructure

General-purpose infrastructure should use established implementations.

Examples:

    Tokio:
        Asynchronous Runtime

    tokio-tungstenite:
        WebSocket Protocol

    reqwest:
        HTTP Client

    serde:
        Serialization

    ratatui:
        Terminal Rendering

These are candidate or existing ecosystem dependencies, not an instruction to replace versions already working in the repository.

### 2.3 Avoid Framework-Driven Architecture

The architecture must not be redesigned merely to match an external framework.

For example, adopting an exchange-data library must not force MarketForge to:

- Replace its canonical event models.
- Abandon the existing BookStore.
- Use incompatible timestamp semantics.
- Introduce unnecessary per-event allocations.
- Duplicate connection supervisors.
- Accept weaker recovery guarantees.
- Change source identity semantics.

External libraries should fit MarketForge's contracts.

MarketForge should not surrender its contracts to an external framework.

### 2.4 Prefer Narrow Dependencies

Prefer a focused library that solves one well-defined problem over a large framework that duplicates multiple MarketForge components.

For example:

    WebSocket Library:
        Transport

    Connection Manager:
        MarketForge

    Subscription Manager:
        MarketForge

    Trade Integrity:
        MarketForge

A WebSocket library does not need to own the full market-data lifecycle.

### 2.5 Avoid Duplicate Abstractions

The runtime must avoid introducing parallel implementations of:

    Connection Supervision

    Retry Scheduling

    Rate-Limit Accounting

    Stream Ownership

    Book Synchronization

    Metrics Collection

    Configuration State

For example, a dependency that introduces its own connection supervisor must not operate independently alongside MarketForge's Connection Manager for the same connection.

### 2.6 Abstraction Cost

Every abstraction introduces potential costs:

- Runtime dispatch.
- Memory allocation.
- Additional synchronization.
- More complicated ownership.
- Increased debugging difficulty.
- Dependency coupling.
- Additional configuration.

Abstractions should solve demonstrated reuse or testing requirements.

They should not be introduced solely to make the architecture appear more generic.

### 2.7 Performance-Sensitive Dependencies

Dependencies used on the canonical event hot path require additional evaluation.

Relevant concerns include:

    Per-Event Allocations

    Parsing Overhead

    Decimal Arithmetic

    Lock Contention

    Dynamic Dispatch

    Buffer Copies

    Cache Locality

    Serialization Overhead

Performance claims must be verified through benchmarks using representative exchange messages.

### 2.8 Dependency Replacement

Infrastructure dependencies should be replaceable at explicitly defined boundaries.

Examples:

    WebSocketTransport

    RestTransport

    RecordingSink

    RetentionReader

    ConfigStore

This does not mean every library requires a dedicated trait.

Introduce replaceable interfaces where they are necessary for testing, portability, or architectural isolation.

### 2.9 Dependency Selection Invariant

    Dependencies Provide
    Implementation Primitives.

    MarketForge Preserves
    Domain Semantics
    And Operational Ownership.

---

## 3. Existing MarketForge Code Reuse

The historical MarketForge engine is the primary source of reusable domain logic.

Live processing must extend existing functionality rather than establish competing implementations.

### 3.1 Reuse Categories

Existing components are classified as:

    Directly Reusable

    Reusable With Adaptation

    Reference Implementation

    Historical-Specific

This classification must be performed per component.

A module should not be classified as reusable merely because its name matches a Live requirement.

### 3.2 Directly Reusable

A component is directly reusable when:

- Its input semantics are compatible.
- Its output semantics are compatible.
- It has no inappropriate historical-execution dependency.
- Its resource behavior is suitable.
- Its tests remain valid.
- It does not require a second canonical representation.

Examples may include canonical data types and exact numeric primitives.

### 3.3 Reusable With Adaptation

A component may require adaptation when its core algorithm is useful but its execution model differs.

Examples:

    Historical Batch Processing
        ->
    Incremental Live Processing

    Historical File-Based Input
        ->
    WebSocket Message Input

    Historical Event-Time Merge
        ->
    Bounded Live Event-Time Merge

    Historical Book Processing
        ->
    Continuous Snapshot Synchronization

Adaptation should preserve the existing algorithmic semantics whenever possible.

### 3.4 Reference Implementations

Some historical components may be better used as correctness references than directly reused executors.

For example:

    Historical Merge Executor

may provide deterministic ordering semantics while Live requires a separate incremental scheduling mechanism.

The shared ordering rules should remain consistent.

The historical execution engine itself does not necessarily need to run inside Live.

### 3.5 Historical-Specific Components

Some functionality belongs exclusively to historical processing.

Examples:

    Archive Discovery

    Historical Download Orchestration

    Archive Job Scheduling

    Historical Partition Planning

    Historical Task Execution

Live must not depend on these executors merely to reuse normalization or serialization primitives.

### 3.6 Reuse Evaluation Procedure

For each existing component:

1. Identify its public contract.
2. Identify its dependencies.
3. Identify mutable state ownership.
4. Identify historical-specific assumptions.
5. Determine Live compatibility.
6. Identify required adaptations.
7. Preserve existing regression tests.
8. Add Live-specific tests.
9. Benchmark where applicable.

The result should be an explicit reuse decision.

### 3.7 Avoid Premature Extraction

Not every shared function needs to move into a new crate.

The initial Live implementation remains inside the existing MarketForge Rust engine.

Shared functionality may be extracted into reusable internal modules as required.

Creating numerous new crates before there is a clear boundary should be avoided.

### 3.8 Reuse Invariant

    Existing Historical Logic
    Must Remain Correct.

    Live Adaptations
    Must Not Introduce
    Competing Canonical
    Interpretations.

---

## 4. Canonical Models and Normalization Reuse

Canonical market-data models are among the most important components to reuse.

They define the economic meaning of MarketForge events.

### 4.1 Canonical Event Model

The existing canonical event representation includes:

    CanonicalEvent

        Trade

        L2LevelUpdate

Live processing must reuse these types.

It must not introduce independent equivalents with different economic semantics.

### 4.2 EventEnvelope

The existing historical canonical envelope includes information such as:

    EventTimestamp

    Exchange

    InstrumentId

    Symbol

    StreamId

    OptionalSystemTimestamp

Live may attach additional operational metadata through its LiveEvent envelope.

The canonical fields must retain their established meaning.

### 4.3 Live-Specific Metadata

Live-specific metadata may include:

    StreamCursor

    ConnectionGeneration

    SubscriptionGeneration

    BookGeneration

    ReceiveTiming

    SourceProvenance

These belong to the Live processing contract.

They must not require unnecessary duplication of canonical price or quantity fields.

### 4.4 Trade Model Reuse

The historical Trade model defines the canonical trade representation.

Live must use the same:

    Price Semantics

    Quantity Semantics

    Aggressor-Side Semantics

    Instrument Identity

    Timestamp Semantics

    Native Identity Where Supported

The Live Trade Processor may implement additional continuity and recovery state.

It must not redefine what a canonical trade means.

### 4.5 L2LevelUpdate Reuse

The existing L2LevelUpdate model represents absolute price-level state changes.

Conceptually:

    Set Price Level
        To Quantity

A zero quantity represents removal according to the established canonical contract.

Live processing must preserve these semantics.

### 4.6 Exact Decimal Reuse

The historical engine's exact Decimal representation remains authoritative.

Live must reuse compatible parsing and arithmetic primitives.

Financial values must not be normalized through binary floating-point intermediates when exact Decimal semantics are required.

### 4.7 Timestamp Normalization Reuse

Live must preserve the historical canonical timestamp unit:

    Unix Nanoseconds

Existing timestamp conversion utilities should be reused.

Exchange-specific event-time field selection belongs to compiled format specifications.

### 4.8 Quantity Normalization Reuse

Quantity normalization depends on:

    Native Quantity Unit

    Instrument Metadata

    Contract Value

    Contract Value Asset

    Price

    Normalization Strategy

Existing historical quantity conversion logic should be reused where semantics match.

Live must not implement competing contract-conversion formulas for equivalent instruments.

### 4.9 Aggressor-Side Normalization

Existing historical trade-side semantics remain authoritative.

Exchange-native fields may encode aggressor direction differently.

The Live format compiler resolves these differences.

The resulting canonical side must remain compatible with historical output.

### 4.10 Instrument Metadata Reuse

The Python-managed instrument catalog remains authoritative.

Rust Live uses validated, immutable instrument contexts.

No new independent instrument metadata database is required.

Existing instrument identities and economic specifications must be reused.

### 4.11 Shared Normalization Boundary

The preferred architecture is:

    Historical Native Record
               |
               v
       Historical Adapter
               |
               |
               +-------------------+
                                   |
                                   v
                          Shared Normalization
                                   ^
                                   |
               +-------------------+
               |
       Live Native Record
               ^
               |
        Live Decoder

The diagram is conceptual.

Exact code reuse depends on the existing normalization interfaces.

### 4.12 Canonical Compatibility Tests

Shared normalization tests must compare equivalent historical and Live inputs.

Required checks include:

    Price

    Quantity

    Side

    InstrumentId

    EventTimestamp

    Native Identity

    L2 Update Semantics

### 4.13 Canonical Reuse Invariant

    One Canonical
    Economic Representation.

    Multiple Supported
    Acquisition Sources.

---

## 5. Shared BookStore and Depth Processing Reuse

The existing BookStore is the authoritative implementation for maintaining L2 order-book state.

This is a particularly important reuse decision.

### 5.1 Existing BookStore

The existing implementation uses ordered price-level storage.

Conceptually:

    BookStore

        Bids

        Asks

        Sequence State

        Segment State

        Validation

The existing code includes functionality corresponding to:

    ApplySnapshot

    ApplyBatch

    SetLevel

    EmitL2Changes

    SequenceTracker

    SegmentTracker

These components must be evaluated and reused according to their actual contracts.

### 5.2 Live BookStore Ownership

Each active L2 source stream has its own authoritative book state.

Conceptually:

    Binance BTCUSDT L2
            |
            v
        BookStore A

    Bybit BTCUSDT L2
            |
            v
        BookStore B

    OKX BTCUSDT L2
            |
            v
        BookStore C

Book state must not be combined merely because several streams represent BTCUSDT.

### 5.3 Native Synchronization

The shared BookStore manages canonical price-level state.

Exchange-specific synchronization strategies determine:

    Snapshot Acquisition

    Snapshot/Delta Bridging

    Native Sequence Validation

    Reset Conditions

    Book Generation Transitions

    Resynchronization Requirements

The live synchronization controller must translate native exchange rules into valid BookStore operations.

### 5.4 SequenceTracker Reuse

The historical implementation includes sequence-validation primitives.

These should be reused when their semantics are compatible.

However, exchange protocols may expose:

    Consecutive Sequences

    Sequence Ranges

    Previous Sequence References

    Native Resets

    Unsequenced Updates

The Live implementation must not force incompatible native semantics into one sequence strategy merely to maximize code reuse.

### 5.5 Depth Message Grouping

The historical depth pipeline preserves logical native-message grouping.

Relevant existing concepts include:

    DepthProcessingOutcome

    DepthEventBoundary

    DepthSourceOperation

    DepthSourceEventMetadata

Live must preserve equivalent grouping for:

    Event Distribution

    Parquet Retention

    PostgreSQL Recording

    Historical Reconstruction

### 5.6 Snapshot Initialization

The BookStore may emit only changed levels when applying a replacement snapshot.

This behavior is not automatically equivalent to producing a complete authoritative initialization event.

For independently reconstructable book generations, Live must support emitting the complete initialized state.

This may require an additional shared BookStore API or an appropriate initialization emission path.

### 5.7 Book Generation Management

The Live synchronization controller owns generation lifecycle.

The BookStore maintains the associated book state.

Conceptually:

    Generation 4
        |
        v
    Synchronized
        |
        v
    Sequence Gap
        |
        v
    Invalid
        |
        v
    New Snapshot
        |
        v
    Generation 5
        |
        v
    Synchronized

Obsolete generation updates must not mutate the current book.

### 5.8 Avoid Duplicate Book Implementations

Do not create:

    HistoricalBookStore

and:

    LiveBookStore

with independently maintained price-level mutation rules.

Instead:

    Shared BookStore
          |
          +-- Historical Processing
          |
          +-- Live Processing

Specialized execution controllers may differ.

The underlying canonical book-state implementation should remain shared.

### 5.9 Depth Reuse Testing

Required tests include:

- Historical BookStore regression tests.
- Snapshot initialization.
- Absolute level replacement.
- Zero-quantity deletion.
- Atomic depth updates.
- Sequence discontinuity.
- Generation invalidation.
- Snapshot replacement.
- Full initialization emission.
- Independent book reconstruction.

### 5.10 BookStore Reuse Invariant

    One Authoritative
    Book-State Implementation.

    Multiple Exchange-Specific
    Synchronization Strategies.

---

## 6. Reuse Ownership and Dependency Boundaries

Reuse must not compromise module ownership or introduce circular dependencies between historical and Live execution.

### 6.1 Dependency Direction

The preferred dependency structure is:

    Historical Engine
           |
           v
       Shared Core
           ^
           |
       Live Engine

Shared Core may include:

    Canonical Models

    Instrument Models

    Normalization

    BookStore

    Validation

    Serialization Primitives

Historical and Live executors must not depend on each other.

### 6.2 Shared Core Responsibilities

Shared components should provide domain functionality that remains valid across execution environments.

Examples:

    Canonical Trade Representation

    Decimal Arithmetic

    Timestamp Normalization

    Quantity Conversion

    Book Mutation

    Canonical Validation

    Arrow Schema Construction

They should not own Live-specific WebSocket connections or historical archive jobs.

### 6.3 Live-Specific Responsibilities

Live owns:

    Connection Manager

    Subscription Manager

    Stream Registry

    Live Processing Lifecycle

    Recovery Coordinator

    Unix Socket Publisher

    Live Retention Manager

    PostgreSQL Recording Manager

    Runtime Control API

    Metrics Registry

    HealthEvaluator

These components may call shared core functionality.

Shared core functionality must not depend on them.

### 6.4 Historical-Specific Responsibilities

Historical owns:

    Archive Acquisition

    Historical Job Scheduling

    Historical Task Execution

    Historical Output Partitioning

    Archive-Specific Coverage Processing

Existing historical execution code should not be imported wholesale into Live merely to reuse small normalization functions.

### 6.5 Internal Module Extraction

When historical code contains reusable functionality coupled to historical execution, prefer targeted extraction.

Conceptually:

    Existing Historical Module
                |
                v
    Identify Reusable Algorithm
                |
                v
    Extract Shared Primitive
                |
          +-----+-----+
          |           |
          v           v
       Historical    Live

Extraction must preserve historical behavior.

### 6.6 Trait Boundaries

Traits are appropriate when implementations genuinely vary.

Examples:

    WebSocketTransport

    RestTransport

    RecordingSink

    RetentionReader

    ConfigStore

Not every data-processing operation needs dynamic dispatch.

Concrete Rust types remain appropriate for performance-critical code.

### 6.7 Error Ownership

Shared core errors should describe shared domain failures.

Live-specific errors should describe runtime failures.

For example:

    Invalid Decimal
        -> Shared Normalization Error

    WebSocket Disconnected
        -> Live Transport Error

    PostgreSQL Commit Failed
        -> Live Recording Error

Avoid forcing shared canonical modules to depend on Live-specific error types.

### 6.8 Configuration Ownership

Shared normalization logic must not read Live TOML configuration directly.

The Live configuration compiler resolves exchange-specific configuration into typed processing plans.

Shared primitives receive validated typed inputs.

### 6.9 Metrics Ownership

Shared canonical processing must not depend on the Live Metrics Registry.

Instrumentation belongs to the appropriate Live execution boundary.

Historical processing may expose its own performance measurements without requiring Live runtime components.

### 6.10 Testing Ownership

Shared components retain shared regression tests.

Live-specific functionality adds Live-specific tests.

Changes to shared modules must run both applicable test suites.

### 6.11 Final Dependency Invariant

    Historical And Live
    Reuse Shared Domain Logic.

    They Do Not Depend
    On Each Other's
    Execution Infrastructure.

---

## 7. Tokio and Asynchronous Runtime Infrastructure

MarketForge Live uses Tokio as its primary asynchronous runtime.

Tokio provides the execution infrastructure required for continuous exchange connectivity, concurrent stream processing, asynchronous storage, and runtime supervision.

MarketForge should reuse Tokio directly rather than introduce a second asynchronous execution framework.

### 7.1 Dependency Decision

| Library | Decision | Responsibility |
|---|---|---|
| `tokio` | Required | Async runtime, tasks, timers, networking, synchronization |
| `tokio-util` | Recommended | Cancellation and selected async utilities |
| `futures-util` | Recommended | Stream/Sink utilities and combinators |
| `async-trait` | Optional | Async trait ergonomics where required |
| `rayon` | Optional | CPU-bound parallelism outside latency-sensitive async tasks |
| `crossbeam` | Evaluate only if needed | Specialized concurrent data structures |

The final dependency set should reuse existing compatible crate versions.

### 7.2 Tokio Responsibilities

Tokio provides:

    Asynchronous Tasks

    TCP Networking

    Unix Domain Sockets

    Timers

    Task Scheduling

    Bounded Channels

    Synchronization Primitives

    Async File Operations

    Runtime Shutdown Coordination

These primitives should form the foundation of MarketForge Live.

### 7.3 Runtime Ownership

MarketForge must own its Tokio runtime composition.

Conceptually:

    LiveRuntime
         |
         v
    Tokio Runtime
         |
         +-- Connection Supervisors
         |
         +-- Stream Processing Tasks
         |
         +-- Recovery Workers
         |
         +-- Event Distribution
         |
         +-- Retention Workers
         |
         +-- Recording Workers
         |
         +-- Derived Stream Workers
         |
         +-- Control Server
         |
         +-- Metrics Collection

The exact worker topology belongs to the corresponding subsystem documents.

Tokio task creation must not become an uncontrolled replacement for explicit component ownership.

### 7.4 Task Supervision

Tokio provides task execution and completion mechanisms.

MarketForge provides:

    Worker Identity

    Worker Lifecycle

    Failure Classification

    Restart Policy

    Generation Fencing

    Resource Ownership

    Shutdown Semantics

The Runtime Supervisor remains a MarketForge component.

### 7.5 JoinSet

`tokio::task::JoinSet` is a suitable primitive for supervising collections of related tasks.

Potential applications include:

    Connection Generation Tasks

    Recovery Workers

    Runtime Background Services

However, JoinSet does not independently provide MarketForge's component restart or integrity policy.

### 7.6 Task Cancellation

`tokio-util::sync::CancellationToken` is a recommended primitive for coordinated task cancellation.

Potential uses:

    Runtime Shutdown

    Connection Generation Invalidation

    Stream Processor Replacement

    Recovery Operation Cancellation

    Control Client Session Cleanup

A cancellation token must not replace generation validation.

Cancellation may race with asynchronous completion.

### 7.7 Graceful Shutdown

The runtime should use a hierarchical cancellation structure.

Conceptually:

    Runtime Cancellation
              |
       +------+------+
       |             |
       v             v
    Connections    Background Services
       |             |
       v             v
    Reader/Writer  Recording/Retention

Individual component cleanup remains explicit.

Cancellation alone does not establish that buffered events were durably committed.

### 7.8 Bounded Tokio Channels

Prefer bounded Tokio channels for asynchronous component communication.

Examples:

    Inbound Frames

    Stream Messages

    Recording Batches

    Retention Jobs

    Control Requests

    Operational Notifications

Every channel must have:

    Capacity

    Owner

    Backpressure Policy

    Shutdown Semantics

    Failure Semantics

### 7.9 Channel Selection

Recommended initial choices:

| Primitive | Appropriate Use |
|---|---|
| `tokio::sync::mpsc` | Bounded asynchronous work queues |
| `tokio::sync::oneshot` | Single operation completion |
| `tokio::sync::watch` | Latest-state observation |
| `tokio::sync::broadcast` | Bounded operational notifications |
| `tokio::sync::Semaphore` | Concurrency and admission limits |

These are implementation primitives, not domain contracts.

### 7.10 Watch Channels

A watch channel is suitable when consumers primarily need the latest state.

Examples:

    Configuration Revision

    Connection Lifecycle State

    Runtime Health Snapshot

    Metrics Snapshot

Intermediate state values may be superseded.

Watch channels are not appropriate for required canonical event delivery.

### 7.11 Broadcast Channels

Broadcast channels may support bounded operational notifications.

However, slow receivers may miss messages.

This is acceptable only when the notification contract permits gaps and consumers can refresh authoritative state.

A broadcast channel must not silently replace lossless canonical event distribution.

### 7.12 Semaphores

Tokio semaphores may enforce:

    Maximum Concurrent Connections

    Maximum REST Requests

    Maximum Control Requests

    Maximum Database Operations

Permits must be acquired and released according to the owning subsystem's resource contract.

### 7.13 Blocking Operations

Blocking operations must not execute directly on latency-sensitive Tokio worker threads.

Examples include:

    Heavy Parquet Compression

    Large Synchronous File Operations

    Expensive CPU Transformations

    Blocking Database Drivers

Use appropriate dedicated workers or `spawn_blocking` where justified.

The implementation must account for bounded concurrency and shutdown behavior.

### 7.14 Rayon Evaluation

Rayon is useful for CPU-bound parallel workloads.

Potential applications:

    Parquet Batch Processing

    Expensive Batch Transformations

    Historical Processing Reuse

It is not required for the initial live network-processing architecture.

Introducing Rayon should follow profiling evidence.

### 7.15 Tokio Invariant

    Tokio Provides
    Execution Primitives.

    MarketForge Provides
    Operational Ownership
    And Correctness Guarantees.

---

## 8. WebSocket Transport Libraries

MarketForge Live must support continuous public market-data WebSocket connections across five exchanges and their applicable API families.

A mature WebSocket implementation should be reused.

MarketForge must not implement its own WebSocket framing, masking, or control-frame protocol.

### 8.1 Dependency Decision

| Library | Decision | Responsibility |
|---|---|---|
| `tokio-tungstenite` | Recommended | Async WebSocket transport |
| `tungstenite` | Transitive/core dependency | WebSocket protocol implementation |
| `futures-util` | Recommended | Stream/Sink interfaces |
| `rustls` | Preferred TLS candidate | TLS backend |
| `native-tls` | Alternative | Platform TLS integration |

The final TLS selection must be consistent across MarketForge's networking dependencies.

### 8.2 Why tokio-tungstenite

The library provides established WebSocket transport functionality.

Relevant capabilities include:

    Async Connection Establishment

    WebSocket Handshake

    Text Frames

    Binary Frames

    Ping/Pong

    Close Frames

    Connection Errors

    Stream/Sink Integration

    Configurable WebSocket Parameters

These capabilities map naturally to the Transport Adapter defined in Document 05.1.

### 8.3 Responsibility Boundary

The library owns:

    WebSocket Protocol Implementation

MarketForge owns:

    Connection Identity

    Connection Pools

    Connection Generations

    Heartbeat Policy

    Subscription Assignments

    Exchange Rate Limits

    Reconnection

    Stream Routing

    Recovery Coordination

This distinction is mandatory.

### 8.4 Transport Adapter

Recommended conceptual architecture:

    MarketForge Connection Manager
                 |
                 v
       WebSocketTransport Adapter
                 |
                 v
           tokio-tungstenite
                 |
                 v
             TCP / TLS

The adapter should translate library-specific errors into MarketForge's typed transport errors.

### 8.5 WebSocket Configuration

The transport adapter must support validated settings for:

    Maximum Frame Size

    Maximum Message Size

    Compression Configuration

    TCP Options

    TLS Configuration

    Required Handshake Headers

These settings originate from compiled exchange profiles.

They must not be embedded as exchange-name conditionals throughout the transport implementation.

### 8.6 Custom Handshake Headers

Some exchange protocols require additional handshake headers.

For example, Gate.io Futures requires:

    X-Gate-Size-Decimal: 1

The transport adapter must support constructing validated WebSocket requests with required headers.

Tests must verify that the actual handshake includes them.

### 8.7 Text and Binary Messages

The transport adapter must preserve received payload bytes and message classification.

It must not perform canonical normalization.

The exchange protocol adapter determines how the message is interpreted.

### 8.8 Ping/Pong Handling

WebSocket libraries provide protocol-level control-frame support.

MarketForge's Heartbeat Controller remains responsible for:

    Scheduling

    Response Correlation

    Timeouts

    Liveness State

    Failure Reporting

Automatic WebSocket control-frame handling must be coordinated with MarketForge's heartbeat policy.

The implementation must verify the selected library's actual Ping/Pong behavior to avoid duplicate or conflicting responses.

### 8.9 WebSocket Writer Ownership

Each physical WebSocket connection must have one authoritative outbound write path.

Recommended architecture:

    Subscription Manager
             |
             v
    Outbound Command Queue
             |
             v
        Writer Task
             |
             v
       WebSocket Sink

Heartbeat messages and other protocol commands use this same path where required.

The library's split Stream/Sink functionality may be used without creating multiple independent writers.

### 8.10 WebSocket Reader Ownership

Each connection generation has one authoritative receive path.

Conceptually:

    WebSocket Stream
             |
             v
         Reader Task
             |
             v
    Frame Classification
             |
             v
      Bounded Routing

Received frames must remain associated with the correct connection generation.

### 8.11 WebSocket Connection Closure

The transport adapter must report:

    Normal Closure

    Unexpected Closure

    Protocol Error

    Connection Reset

    Timeout

    TLS Failure

The Connection Supervisor determines the resulting lifecycle transition.

The transport adapter must not independently create replacement connections.

### 8.12 Compression

WebSocket compression must follow the selected exchange's protocol support and the library's actual feature capabilities.

Compression can reduce transport size but introduces CPU and memory costs.

It must not be enabled indiscriminately.

Benchmarking is required when compression materially affects hot-path performance.

### 8.13 WebSocket Buffering

The implementation should minimize unnecessary payload copies.

Potential approaches include:

    Borrowed Payload Parsing

    Owned Byte Buffers

    Controlled Buffer Reuse

The selected approach must preserve safe ownership and generation isolation.

### 8.14 Connection Pooling

Connection pooling belongs to MarketForge.

The WebSocket library must not determine which subscriptions share physical connections.

Pooling decisions depend on:

    Exchange

    Endpoint

    Authentication

    Headers

    Heartbeat Compatibility

    Protocol Version

    Connection Capacity

### 8.15 WebSocket Reconnection

Reconnection must be coordinated through the Connection Manager.

Do not combine:

    Library Automatic Reconnection

with:

    MarketForge Automatic Reconnection

unless the library's behavior is explicitly integrated under one authoritative lifecycle owner.

The preferred initial model is MarketForge-owned reconnection.

### 8.16 WebSocket Testing

Required tests include:

- Handshake success.
- Handshake failure.
- Custom headers.
- Text frames.
- Binary frames.
- Ping/Pong.
- Close frames.
- Partial network delivery.
- Maximum frame size.
- Connection reset.
- Cancellation.
- Generation replacement.
- Reader and writer cleanup.

### 8.17 WebSocket Invariant

    tokio-tungstenite
    Provides WebSocket
    Communication.

    MarketForge Owns
    Connection Lifecycle
    And Stream Semantics.

---

## 9. REST and HTTP Infrastructure

MarketForge Live requires REST requests for exchange metadata, trade recovery, L2 snapshots, and other protocol-supported operations.

A reusable HTTP client should provide transport functionality.

Exchange-specific request construction and recovery behavior remain MarketForge responsibilities.

### 9.1 Dependency Decision

| Library | Decision | Responsibility |
|---|---|---|
| `reqwest` | Recommended | Async HTTP client |
| `http` | Use where needed | HTTP request/header types |
| `url` | Use where needed | URL parsing and construction |
| `serde_json` | Recommended | JSON decoding |
| `rustls` | Preferred TLS candidate | TLS transport |

Avoid introducing multiple independent HTTP client stacks without a demonstrated requirement.

### 9.2 HTTP Client Reuse

The HTTP client should be constructed and reused by appropriate exchange/API pools.

Conceptually:

    Exchange REST Pool
             |
             v
       Shared HTTP Client
             |
             v
           reqwest

Creating a new HTTP client for every REST request should be avoided.

### 9.3 REST Adapter

The REST Adapter owns general HTTP transport mechanics.

Examples:

    Request Execution

    Connection Reuse

    HTTP Timeouts

    Header Transmission

    Response Body Acquisition

    Transport Error Classification

It does not own native exchange recovery semantics.

### 9.4 Exchange Endpoint Compilation

The Configuration Compiler resolves exchange endpoint specifications before runtime use.

A compiled REST request plan may contain:

    Base URL

    Endpoint Path

    HTTP Method

    Parameter Templates

    Request Weight

    Authentication Requirements

    Response Format

    Timeout Policy

The runtime should not repeatedly parse TOML for every request.

### 9.5 REST Request Scheduling

REST operations must pass through the shared request scheduler when rate limiting or concurrency control is required.

Conceptually:

    Recovery Coordinator
             |
             v
       REST Request Plan
             |
             v
       Request Scheduler
             |
       +-----+------+
       |            |
       v            v
    Rate Limiter  Concurrency Limit
       |            |
       +-----+------+
             |
             v
         REST Adapter
             |
             v
           reqwest

### 9.6 REST Request Categories

Examples:

    L2 Snapshot

    Historical Trade Recovery

    Instrument Metadata

    Exchange Capability Query

    Recovery Verification

Different request categories may have different priorities.

They must still respect shared exchange quotas.

### 9.7 REST Timeouts

Timeouts should distinguish:

    Connection Timeout

    Request Deadline

    Response Body Timeout

    Operation Deadline

The final effective deadline must be compatible with the owning recovery or initialization operation.

### 9.8 HTTP Error Classification

The REST layer must distinguish:

    Transport Failure

    HTTP Status Failure

    Exchange Application Error

    Invalid Response Body

    Rate Limiting

    Authentication Failure

    Unsupported Endpoint

A successful HTTP status does not necessarily mean the exchange returned a valid business response.

### 9.9 HTTP 429

Rate-limit responses must be reported to the shared Rate Limiter and owning request scheduler.

MarketForge must respect valid cooldown or retry-after information.

The HTTP client must not independently retry 429 responses in a way that bypasses shared rate-limit accounting.

### 9.10 Response Size Limits

REST responses must have bounded body-size policies.

A recovery endpoint returning unexpectedly large payloads must not cause uncontrolled allocation.

Large legitimate responses may require pagination or streaming where supported.

### 9.11 Response Decoding

`serde_json` is an appropriate general-purpose JSON decoder.

However, MarketForge must preserve exact numerical semantics.

Native trade IDs, sequence identifiers, prices, quantities, and timestamps must not pass through lossy floating-point conversion.

The decoder configuration and extraction strategy must follow the existing canonical normalization contracts.

### 9.12 Authentication

Some REST endpoints require credentials or signed requests.

Authentication belongs to exchange protocol adapters and validated credential configuration.

The HTTP transport must not expose secrets through logs, metrics, or control responses.

### 9.13 REST Integration Tests

Required tests include:

- Endpoint resolution.
- Query parameter construction.
- Required headers.
- Timeout behavior.
- HTTP errors.
- Exchange application errors.
- JSON decoding.
- Pagination.
- Request weight accounting.
- Cooldown handling.
- Cancellation.
- Response-size limits.

### 9.14 REST Invariant

    reqwest Handles
    HTTP Communication.

    MarketForge Determines
    Request Meaning,
    Exchange Quotas,
    And Recovery Semantics.

---

## 10. TLS and Network Security Libraries

MarketForge communicates with external exchanges using encrypted WebSocket and HTTP connections where required.

TLS implementation should use established Rust ecosystem libraries.

### 10.1 TLS Candidates

Recommended candidates:

    rustls

    native-tls

The initial preference is `rustls`, subject to compatibility with the selected versions of `reqwest` and `tokio-tungstenite`.

### 10.2 TLS Responsibilities

The selected TLS implementation handles:

    Certificate Verification

    TLS Handshake

    Encrypted Transport

    Peer Authentication

    Protocol Negotiation

MarketForge must not implement its own cryptographic protocol.

### 10.3 TLS Backend Consistency

The dependency selection should avoid accidentally introducing unnecessary duplicate TLS backends.

For example:

    WebSocket:
        rustls

    REST:
        rustls

is preferable when both stacks support the required capabilities.

However, an existing working repository dependency should not be replaced without checking compatibility and performance.

### 10.4 Certificate Verification

Production transport must verify remote certificates.

Disabling certificate verification must not be an ordinary production configuration option.

Exceptional local testing must use explicit test-only infrastructure.

### 10.5 TLS Error Classification

TLS failures may represent:

    Transient Transport Failure

    Invalid Certificate

    Unsupported TLS Configuration

    Authentication Failure

The Connection Manager or REST request owner must determine the appropriate recovery disposition.

A persistent certificate-validation failure must not trigger unlimited immediate retries.

### 10.6 Test TLS Endpoints

Integration tests may use a local TLS server with controlled test certificates.

Tests should verify:

- Valid certificate acceptance.
- Invalid certificate rejection.
- Handshake failure.
- Timeout.
- Cancellation.
- Connection cleanup.

### 10.7 TLS Invariant

    Cryptographic
    Transport Security
    Must Use
    Established Libraries.

    MarketForge Must Not
    Weaken Validation
    For Convenience.

---

## 11. Retry and Backoff Libraries

MarketForge requires retry and backoff behavior for several independent operations.

Examples include:

    WebSocket Reconnection

    REST Recovery Requests

    PostgreSQL Reconnection

    Recording Transaction Retries

    Retention Spill Retries

Retry behavior is a cross-cutting concern, but retry ownership belongs to the component responsible for the operation.

### 11.1 Dependency Decision

| Library | Decision | Responsibility |
|---|---|---|
| `backon` | Recommended candidate | Retry/backoff primitives |
| Tokio timers | Required | Asynchronous waiting and deadlines |
| `rand` or existing RNG utilities | Conditional | Jitter generation |

The final implementation should evaluate whether one shared retry utility is sufficient.

### 11.2 Why backon

`backon` provides reusable retry machinery and exponential backoff configuration.

It can reduce duplicated low-level retry scheduling code.

However, MarketForge must supply:

    Error Classification

    Retry Eligibility

    Exchange Cooldowns

    Recovery Generation

    Operation Cancellation

    Escalation Policy

### 11.3 Retry Ownership

A library should calculate or execute a permitted retry policy.

It must not decide that every error is retryable.

For example:

    Network Timeout:
        Retry

    Exchange 429:
        Respect Cooldown

    Invalid Subscription:
        Fail Configuration

    Authentication Rejection:
        Require Correction

The owning component determines the behavior.

### 11.4 Retryable Errors

Retry eligibility must be based on typed MarketForge errors.

Avoid relying on human-readable error-string matching.

Conceptually:

    FailureClass
          |
          v
    RecoveryDisposition
          |
          v
    Retry Policy
          |
          v
    Backoff Primitive

### 11.5 Exponential Backoff

The initial policy may use:

    InitialDelay

    MultiplicationFactor

    MaximumDelay

    JitterStrategy

The values originate from validated configuration.

### 11.6 Exchange Cooldown Precedence

The effective retry deadline must respect:

    ExponentialBackoffDeadline

    ExchangeCooldownDeadline

    SharedRateLimitAvailability

Conceptually:

    NextAttempt =
        Max(
            BackoffDeadline,
            ExchangeCooldownDeadline,
            RateLimitAvailableAt
        )

The retry library must not bypass these requirements.

### 11.7 Retry Budget

Every retrying operation must have an explicit resource and scheduling policy.

Possible constraints include:

    MaximumAttempts

    MaximumElapsedDuration

    MaximumBackoff

    EscalationThreshold

    CancellationCondition

Some long-running connectivity services may retry indefinitely with bounded backoff when their configuration remains valid.

That must be an explicit operational policy rather than a library default.

### 11.8 Retry Cancellation

Retries must be cancelled when:

    Runtime Shutting Down

    Stream Removed

    Connection Generation Obsolete

    Recovery Superseded

    Configuration Invalidated

A cancelled retry must not create a new operation after its owning generation has terminated.

### 11.9 Retry Idempotency

A failed or timed-out operation may have succeeded remotely.

This is particularly important for:

    Subscription Requests

    PostgreSQL Commits

    Certain Authenticated REST Operations

The retry mechanism must honor the operation's idempotency contract.

Blind retries are prohibited when outcomes are ambiguous and reconciliation is required.

### 11.10 Retry Instrumentation

Retry behavior should update existing metrics:

    AttemptsTotal

    RetryFailuresTotal

    CurrentBackoff

    NextRetryDeadline

    RecoveryEscalations

The retry library must not establish another independent metrics registry.

### 11.11 Retry Testing

Required tests include:

- Initial delay.
- Exponential growth.
- Maximum delay.
- Jitter bounds.
- Retry eligibility.
- Cooldown precedence.
- Cancellation.
- Generation replacement.
- Maximum attempts.
- Ambiguous completion.
- Idempotent retry behavior.

### 11.12 Retry Invariant

    Retry Libraries
    Provide Scheduling
    And Backoff Primitives.

    MarketForge Owns
    Recovery Decisions
    And Completion Conditions.

---

## 12. Rate Limiting and Request Scheduling

Rate limiting is particularly important because MarketForge may issue REST requests for trade recovery, L2 synchronization, instrument metadata, and other operations simultaneously.

Different exchange APIs impose different quota models.

The rate-limiting implementation must preserve those semantics.

### 12.1 Dependency Decision

| Library | Decision | Responsibility |
|---|---|---|
| `governor` | Recommended candidate | Token/quota admission primitives |
| Tokio semaphores | Required | Concurrent request limits |
| Tokio timers | Required | Cooldown and delayed admission |
| MarketForge Rate Limiter | Required internal component | Exchange-specific shared quota ownership |

### 12.2 Governor Capabilities

`governor` supports rate-limiting primitives suitable for direct or keyed quota admission.

Its available capabilities include weighted permit checks and configurable clocks.

These may be useful for implementing exchange request budgets.

However, the chosen algorithm must match the exchange's actual quota rules.

### 12.3 Exchange Rate-Limit Semantics

MarketForge must account for:

    Exchange

    API Pool

    Endpoint

    Request Weight

    Time Window

    Response Headers

    Cooldown State

    Shared IP Or Account Budget

These semantics originate from compiled exchange configuration.

### 12.4 Rate-Limit Ownership

The MarketForge Rate Limiter owns the authoritative state of each configured quota.

Conceptually:

    ExchangeRateLimiter
          |
          +-- Binance Spot Weight Pool
          |
          +-- Binance Linear Weight Pool
          |
          +-- Binance Inverse Weight Pool
          |
          +-- Other Configured Pools

The actual grouping must follow documented exchange quota scopes.

No fixed assumption about one limiter per exchange or market family is permitted.

### 12.5 Shared Request Budget

Several operations may share one quota.

Example:

    Binance REST Weight Budget
                |
       +--------+--------+
       |        |        |
       v        v        v
    Snapshot   Trades   Metadata
       |        |        |
       +--------+--------+
                |
                v
          Rate Limiter

Independent REST clients must not each assume that they own the full exchange request quota.

### 12.6 Weighted Requests

An endpoint may consume more than one quota unit.

Conceptually:

    RequestWeight = 5

    AvailableWeight = 20

After admission:

    EstimatedRemainingWeight = 15

The exact weight must come from the compiled endpoint policy.

### 12.7 Exchange Response Headers

Some exchanges report current quota usage through response headers.

For example, Binance may provide weight-usage information.

MarketForge must reconcile local estimates with exchange-reported usage where the protocol supports it.

The rate-limiting library must not become the authoritative interpreter of exchange-specific headers.

### 12.8 Sliding Windows vs Token Buckets

A generic token-bucket or GCRA limiter does not necessarily reproduce every exchange's fixed-window or rolling-window accounting.

Before using `governor` for a quota, verify that its admission behavior is compatible with the exchange's documented rules.

Where necessary, MarketForge should implement a small exchange-compatible window-accounting layer using generic scheduling primitives.

Correctness takes precedence over uniformity.

### 12.9 Multiple Simultaneous Limits

One request may be constrained by several quotas.

For example:

    Request Weight Per Minute

    Orders Per Second

    Connection Attempts Per Interval

    IP-Wide Cooldown

The scheduler must require all applicable constraints to permit execution.

Acquiring one quota permit must not bypass another quota.

### 12.10 Request Priority

The scheduler may distinguish:

    Critical Integrity Recovery

    Snapshot Initialization

    Trade Recovery

    Instrument Metadata

    Optional Diagnostics

Priority scheduling must remain compatible with exchange quotas.

A low-priority request must not consume capacity reserved for an already admitted higher-priority recovery operation.

### 12.11 Fairness

Sustained recovery activity should not permanently starve ordinary required operations.

The scheduler should define fairness or priority aging where necessary.

The initial implementation should remain simple unless workload evidence justifies more elaborate scheduling.

### 12.12 Cooldown Handling

Exchange-imposed cooldowns override ordinary local request admission.

Conceptually:

    HTTP 429
       |
       v
    Parse Valid Cooldown
       |
       v
    Update Shared Pool State
       |
       v
    Suspend Applicable Requests
       |
       v
    Resume After Deadline

Cooldown scope must match the actual exchange restriction.

### 12.13 Cooldown Cancellation

A waiting operation must be cancellable.

For example:

    Stream Removed

while:

    Snapshot Request Waiting

Expected:

    Pending Request Cancelled

The request must not execute after the stream's generation has been invalidated.

### 12.14 Rate-Limit Metrics

The Metrics Registry should expose:

    ConfiguredQuota

    ObservedUsage

    EstimatedUsage

    RemainingCapacity

    CooldownRemaining

    DeferredRequests

    RateLimitFailures

Observed exchange-reported usage and locally estimated usage must remain distinguishable.

### 12.15 Rate-Limit Testing

Required tests include:

- Weighted request admission.
- Shared pool coordination.
- Multiple simultaneous limits.
- Window expiration.
- Exchange header reconciliation.
- Cooldown precedence.
- Concurrent requests.
- Request cancellation.
- Request priority.
- Quota exhaustion.
- Deterministic fake-clock behavior.

### 12.16 Rate-Limit Invariant

    Generic Rate-Limit
    Libraries Provide
    Admission Primitives.

    MarketForge Owns
    Exchange-Specific
    Quota Semantics.

---

## 13. Concurrency and Synchronization Libraries

MarketForge Live must support many active streams and independently supervised components without creating unnecessary lock contention.

The initial implementation should favor Rust's standard synchronization primitives and Tokio's established asynchronous utilities.

### 13.1 Dependency Decision

| Library | Decision | Intended Use |
|---|---|---|
| `std::sync` | Required | Atomics and ordinary synchronization |
| `tokio::sync` | Required | Async channels, locks, semaphores |
| `dashmap` | Optional | Concurrent keyed maps |
| `parking_lot` | Optional | Efficient synchronous locks |
| `arc-swap` | Optional | Infrequently updated shared immutable state |
| `crossbeam` | Optional | Specialized concurrent queues and coordination |

These libraries should not all be added by default.

### 13.2 Concurrency Principles

The architecture favors:

    One Logical State Owner

    Short Critical Sections

    Bounded Communication

    Immutable Shared Configuration

    Generation-Aware Operations

    Minimal Hot-Path Contention

### 13.3 Actor-Like Ownership

A stream processor may own mutable state inside one task.

For example:

    L2 Stream Processor
             |
             v
          BookStore
             |
             v
       Canonical Events

The BookStore does not necessarily require a concurrent map or mutex merely because the service has many concurrent streams.

Different streams may execute concurrently while each owns its own book.

### 13.4 Standard Mutex and RwLock

Use ordinary synchronization primitives when their semantics and contention characteristics are sufficient.

A synchronous lock must not be held across an `.await`.

An asynchronous lock must not be introduced merely because surrounding code
 merely because a component is asynchronous.

The selected lock must reflect the actual ownership and access pattern.

For example:

    Short Synchronous Registry Lookup
        -> std::sync::RwLock

    Async Resource Admission
        -> tokio::sync::Semaphore

    Single-Owner Stream Processing
        -> No Shared Lock Required

Lock selection must be justified by the component's concurrency contract.

### 13.5 DashMap Evaluation

`dashmap` provides a concurrent keyed map.

Potential applications include:

    Connection Registry Indexes

    Active Session Indexes

    Metrics Handle Lookup

However, introducing DashMap is not automatically preferable to a conventional HashMap protected by a short-lived lock.

The implementation must consider:

- Expected read/write ratio.
- Number of concurrent tasks.
- Lock contention.
- Entry lifetime.
- Allocation behavior.
- Iteration requirements.
- Potential deadlocks caused by retained map guards.

DashMap references and guards must not be retained across unrelated operations or asynchronous suspension points.

For small control-plane registries, ordinary HashMap ownership may remain simpler.

### 13.6 Arc and Immutable State

`Arc<T>` is appropriate for sharing immutable configuration and instrument contexts across tasks.

Examples:

    Arc<CompiledExchangeProfile>

    Arc<InstrumentContext>

    Arc<EffectiveStreamConfig>

    Arc<CompiledNormalizationPlan>

The underlying objects must remain immutable after publication.

Configuration updates create replacement objects rather than mutate shared semantic state unexpectedly.

### 13.7 ArcSwap Evaluation

`arc-swap` may be useful when:

- Many tasks read one immutable configuration.
- Updates occur infrequently.
- Readers require low-overhead access.
- Atomic replacement of the shared reference is sufficient.

Conceptually:

    Current Configuration Revision
               |
               v
           ArcSwap
               |
          +----+----+
          |         |
          v         v
       Reader A   Reader B

A configuration update publishes a new immutable reference.

However, `arc-swap` must not replace the Configuration Manager's revision validation or Runtime Reconciler.

A successful pointer replacement does not establish that all runtime components have applied a new configuration.

### 13.8 Atomic Counters

Use standard Rust atomics for appropriate metrics.

Examples:

    AtomicU64

    AtomicBool

    AtomicUsize

A simple independent metric counter may use relaxed ordering.

Correctness-sensitive synchronization requires ordering appropriate to the state-publication contract.

Atomic counters must not be used as substitutes for authoritative lifecycle state machines.

### 13.9 Cache-Line Contention

Frequently updated counters may cause contention when many tasks modify adjacent shared memory locations.

If profiling identifies false sharing, the implementation may evaluate:

    Cache-Line Padding

    Per-Task Counters

    Periodic Counter Aggregation

These optimizations should follow measurement rather than be introduced universally.

### 13.10 Crossbeam Evaluation

Crossbeam provides several useful concurrency primitives.

However, the initial MarketForge architecture already uses Tokio channels and explicit task ownership.

Crossbeam should be added only when a specific synchronous concurrency requirement cannot be handled efficiently by existing primitives.

Examples might include specialized worker-to-worker communication outside asynchronous Tokio tasks.

Do not introduce an additional queue implementation merely to replace working bounded Tokio channels without evidence of benefit.

### 13.11 Lock-Free Structures

Lock-free data structures may improve performance in selected workloads.

However, they introduce complexity involving:

    Memory Reclamation

    Ordering Guarantees

    ABA Problems

    Shutdown Coordination

    Resource Ownership

    Debugging Difficulty

They should not be the default choice.

MarketForge should first optimize:

    Task Ownership

    Buffer Allocation

    Batch Processing

    Unnecessary Cloning

    Contended Shared State

### 13.12 Concurrent BookStore Access

Each active L2 stream should have one authoritative BookStore owner.

The preferred model is:

    Source Stream Processor
              |
              v
          BookStore
              |
              v
    Canonical Depth Output

Other components should consume:

    Canonical Events

    Bounded Book Snapshots

    Read-Only Diagnostics

They must not concurrently mutate the same BookStore.

This avoids introducing complex synchronization into every price-level update.

### 13.13 Concurrent Stream Registry

The Stream Registry must support concurrent control operations.

However, operations targeting one logical source stream must be serialized or otherwise coordinated through an equivalent state-machine mechanism.

Independent streams may transition concurrently.

The implementation may use:

    Per-Stream Command Queues

    Short-Lived Registry Locks

    Generation Checks

The selected mechanism must prevent obsolete asynchronous completions from modifying newer state.

### 13.14 Concurrent Metrics Registry

Metrics handles should be distributed to component owners.

The canonical event hot path should not perform dynamic global metric lookup for every event.

Conceptually:

    Stream Initialization
             |
             v
    Register Metric Handle
             |
             v
    Stream Processor
             |
             v
    Direct Metric Updates

This approach limits shared registry contention.

### 13.15 Concurrent Configuration Updates

Multiple clients may submit configuration changes simultaneously.

The Configuration Manager must serialize acceptance through revision checks.

A concurrent map or atomic pointer replacement does not independently implement optimistic configuration transactions.

The established revision contract remains authoritative.

### 13.16 Concurrency Testing

Required tests include:

- Concurrent source registration.
- Duplicate subscription prevention.
- Concurrent resource reservations.
- Connection generation replacement.
- Late asynchronous completion.
- Stream removal during recovery.
- Configuration revision conflicts.
- Metrics update contention.
- Recording queue saturation.
- Runtime shutdown during active work.

Tests should use deterministic scheduling where practical.

### 13.17 Concurrency Invariant

    Prefer Single Ownership
    And Bounded Communication.

    Introduce Shared
    Mutable State Only
    When Its Benefits
    Justify The Complexity.

---

## 14. Networking and Concurrency Dependency Summary

The initial networking stack should remain relatively small.

MarketForge's complexity comes primarily from exchange protocols, integrity validation, and recovery requirements—not from needing numerous networking frameworks.

### 14.1 Recommended Core Stack

| Dependency | Decision | Primary Use |
|---|---|---|
| `tokio` | Required | Async runtime and concurrency |
| `tokio-util` | Recommended | Cancellation and async utilities |
| `futures-util` | Recommended | Stream/Sink utilities |
| `tokio-tungstenite` | Recommended | WebSocket transport |
| `reqwest` | Recommended | REST transport |
| `serde` | Required | Typed serialization |
| `serde_json` | Recommended | JSON decoding and control messages |
| `rustls` | Preferred TLS backend | Secure networking |
| `http` | Conditional | HTTP request and header construction |
| `url` | Conditional | URL validation and construction |
| `backon` | Evaluate for adoption | Retry/backoff primitives |
| `governor` | Evaluate for adoption | Generic rate-limit admission |
| `rand` | Conditional | Backoff jitter |
| `async-trait` | Conditional | Async trait ergonomics |

These are architectural recommendations.

Concrete versions and feature selections must be checked against the existing dependency graph before implementation.

### 14.2 Optional Concurrency Dependencies

| Dependency | Initial Decision | Adoption Condition |
|---|---|---|
| `dashmap` | Optional | Demonstrated concurrent-map benefit |
| `parking_lot` | Optional | Measured lock-contention or implementation benefit |
| `arc-swap` | Optional | Frequent immutable-state reads with infrequent replacement |
| `crossbeam` | Optional | Specific concurrent data-structure requirement |
| `rayon` | Optional | Demonstrated CPU-bound parallel workload |
| Specialized lock-free queues | Deferred | Profiling establishes clear benefit |

The initial implementation should avoid adding these dependencies without a concrete use case.

### 14.3 Libraries We Do Not Need to Implement Ourselves

MarketForge should not implement:

    WebSocket Protocol Framing

    TLS Cryptography

    HTTP Wire Protocol

    Generic Async Runtime

    Standard Async Channels

    Basic Synchronization Primitives

    Generic JSON Serialization

Established libraries already provide these capabilities.

### 14.4 Functionality MarketForge Must Own

MarketForge retains responsibility for:

    Connection Pooling

    Connection Lifecycle

    Connection Generations

    Subscription Admission

    Subscription Acknowledgements

    Exchange Rate-Limit Semantics

    REST Recovery Scheduling

    Trade Continuity

    L2 Synchronization

    Recovery Coordination

    Stream Identity

    Source Demand Ownership

No external networking library should silently become authoritative for these responsibilities.

### 14.5 Avoiding Dependency Duplication

The dependency graph should avoid unnecessary parallel implementations.

Examples:

    Two TLS Backends
        -> Avoid unless compatibility requires them.

    Two HTTP Clients
        -> Avoid unless distinct requirements justify them.

    Two Retry Supervisors
        -> Prohibited for the same operation.

    Two Rate-Limit Authorities
        -> Prohibited for the same quota.

    Two Connection Managers
        -> Prohibited for the same physical connection.

The objective is not to minimize the number of dependencies at any cost.

The objective is to minimize overlapping responsibilities.

### 14.6 Dependency Feature Selection

Cargo feature selection should be explicit.

For example, networking libraries may expose optional features for:

    TLS Backends

    Compression

    Proxy Support

    JSON Integration

    Additional Protocols

MarketForge should enable only capabilities required by the selected architecture.

Disabling default features may reduce unnecessary dependencies, but must not accidentally remove required transport functionality.

### 14.7 Version Compatibility

Dependencies should be selected as a compatible set.

Before upgrading a major networking dependency, verify:

- Rust compiler compatibility.
- Tokio compatibility.
- TLS backend compatibility.
- HTTP type compatibility.
- WebSocket API compatibility.
- Existing MarketForge compilation.
- Historical regression tests.
- Live transport integration tests.

Version updates must not bypass compatibility testing.

### 14.8 Performance Validation

The networking stack must be benchmarked with representative workloads.

Required measurements include:

    WebSocket Receive Throughput

    Frame Dispatch Latency

    Per-Message Allocation Count

    REST Request Concurrency

    Rate-Limit Scheduling Overhead

    Connection Memory Usage

    Reconnection Resource Cleanup

Performance results must be evaluated together with correctness.

A faster transport implementation is not acceptable if it weakens generation fencing, integrity guarantees, or backpressure behavior.

### 14.9 Final Networking Architecture

The recommended architecture is:

    MarketForge Live
          |
          v
    Runtime Supervisor
          |
          v
    Connection Manager
          |
          +--------------------------+
          |                          |
          v                          v
    WebSocket Transport         REST Scheduler
          |                          |
          v                          v
    tokio-tungstenite           reqwest
          |                          |
          v                          v
       Tokio / TLS               Tokio / TLS
          |                          |
          v                          v
    Exchange WebSockets         Exchange REST APIs

The shared REST scheduler coordinates:

    Rate Limits

    Request Priorities

    Retry Eligibility

    Cooldowns

    Concurrency Limits

    Cancellation

The Connection Manager coordinates:

    Physical Connections

    Connection Pools

    Generations

    Heartbeats

    Reader / Writer Tasks

    Reconnection

    Transport Backpressure

The Subscription Manager and Recovery Coordinator retain their own authoritative responsibilities.

### 14.10 Final Networking Invariant

    External Libraries
    Implement General-Purpose
    Networking And
    Concurrency Primitives.

    MarketForge Implements
    Exchange-Aware
    Lifecycle, Integrity,
    And Recovery Semantics.

---
## 15. PostgreSQL Client and Recording Infrastructure

MarketForge Live uses PostgreSQL for optional persistent market-data recording.

PostgreSQL is already part of the MarketForge environment.

The Live runtime must reuse the existing database infrastructure and schema conventions wherever compatible.

The Recording Manager remains responsible for batching, transaction boundaries, idempotency, committed coverage, and recovery.

### 15.1 Dependency Candidates

| Library | Decision | Responsibility |
|---|---|---|
| `sqlx` | Recommended candidate | Asynchronous PostgreSQL access |
| `tokio-postgres` | Alternative | Asynchronous PostgreSQL protocol |
| `deadpool-postgres` | Conditional | Connection pooling for tokio-postgres |
| `rust_decimal` | Existing-compatible candidate | Exact decimal representation |
| `serde` | Required | Serialization of recording metadata |
| `uuid` | Conditional | Stable identifiers where the schema requires them |

The final PostgreSQL client selection must consider existing MarketForge database dependencies.

Do not introduce both SQLx and tokio-postgres as independent recording stacks without a demonstrated requirement.

### 15.2 PostgreSQL Client Selection

The preferred initial candidate is:

    sqlx

Reasons include:

- Asynchronous PostgreSQL support.
- Connection pooling.
- Prepared queries.
- Transaction management.
- Typed database access.
- Compatibility with Tokio.
- Support for PostgreSQL numeric values.
- Straightforward testing against real PostgreSQL.

However, if MarketForge already uses a compatible asynchronous PostgreSQL client, reuse should take precedence over introducing another database stack.

### 15.3 SQLx vs tokio-postgres

Both candidates can support the required functionality.

The choice should consider:

    Existing Dependency Reuse

    Transaction Ergonomics

    Batch Insertion Performance

    NUMERIC Compatibility

    Connection Pooling

    Cancellation Behavior

    Error Classification

    Testing Simplicity

A new database client must not be selected solely because it provides more convenient application code.

### 15.4 Recording Manager Ownership

The PostgreSQL client provides database transport and query execution.

MarketForge owns:

    Recording Admission

    Batch Construction

    Event Identity

    Idempotency

    Commit Frontier Tracking

    Gap Detection

    Recording Recovery

    Archive-Aware Cleanup

    Recording Health

These responsibilities must not be delegated implicitly to the database client.

### 15.5 Database Architecture

Conceptually:

    Canonical Event Router
              |
              v
        Recording Manager
              |
              v
        Recording Batches
              |
              v
        PostgreSQL Writer
              |
              v
         SQLx Adapter
              |
              v
           PostgreSQL

The Recording Manager must remain independent of the particular PostgreSQL client API.

### 15.6 Connection Pool

The recording subsystem may use a bounded database connection pool.

Relevant configuration includes:

    MaximumConnections

    MinimumConnections

    ConnectionTimeout

    IdleTimeout

    ConnectionLifetime

    QueryTimeout

The pool must not create unlimited connections under recording pressure.

### 15.7 Database Transactions

Transactions must preserve the recording contracts established in Document 11.

A successful transaction commit establishes the applicable database persistence boundary.

A successful query execution without transaction commitment is insufficient.

### 15.8 Transaction Failure

The database adapter must classify:

    ConnectionFailure

    TransactionFailure

    ConstraintViolation

    SerializationFailure

    DeadlockOrRetryableConflict

    Timeout

    AmbiguousCommitOutcome

The Recording Manager determines recovery and retry behavior.

### 15.9 Ambiguous Commit

Consider:

    Batch Submitted
           |
           v
    PostgreSQL Commit Succeeds
           |
           v
    Connection Lost Before Confirmation

The Recording Manager must not assume that the transaction failed.

It must reconcile actual committed state using the database identity and idempotency contracts.

The client library alone cannot establish the complete logical recording outcome.

### 15.10 Batch Insertion

Market-data recording may involve substantial write throughput.

Candidate insertion strategies include:

    Prepared Multi-Row INSERT

    Parameterized Batched INSERT

    PostgreSQL COPY

    Staging Tables

    Transactional Merge

The appropriate strategy depends on the schema and required idempotency semantics.

### 15.11 COPY Evaluation

PostgreSQL COPY may offer useful throughput for large batches.

However, COPY does not independently provide the same conflict-handling semantics as an ordinary INSERT with ON CONFLICT.

When COPY is used, a possible approach is:

    COPY Into Staging Table
              |
              v
    Transactional Validation
              |
              v
    Insert Into Final Tables
              |
              v
    Apply Idempotency Rules
              |
              v
    Commit Transaction

This introduces additional complexity.

The initial implementation should benchmark simpler batched insertion before adopting a staging pipeline.

### 15.12 Batch Size

Batch size must be configurable.

Larger batches may improve throughput but increase:

    Commit Latency

    Peak Memory

    Transaction Duration

    Retry Cost

The appropriate batch size must be established through representative testing.

### 15.13 PostgreSQL NUMERIC

Canonical prices and quantities require exact decimal semantics.

PostgreSQL NUMERIC is suitable for preserving exact decimal values within the declared precision and scale.

The adapter must avoid conversion through f32 or f64.

The selected Rust decimal representation must be compatible with the database driver's NUMERIC encoding.

### 15.14 Nanosecond Timestamps

MarketForge canonical timestamps use Unix nanoseconds.

PostgreSQL timestamp types do not automatically preserve the full canonical integer-nanosecond representation.

The recording schema should preserve nanosecond timestamps using the representation established in Document 11.

Conceptually:

    BIGINT Nanoseconds

The database adapter must not silently round canonical event timestamps to microseconds.

### 15.15 Depth Recording

Document 11 defines a grouped depth-event representation.

Conceptually:

    depth_events
          |
          v
    depth_levels

The database client must preserve:

    Atomic Native Depth Group

    Event Boundary

    Source Operation

    Sequence Metadata

    Book Generation

    Ordered Canonical Level Updates

The writer must not flatten grouped L2 events in a way that loses reconstruction semantics.

### 15.16 Initialization Recording

A complete authoritative book initialization may contain many price levels.

The writer must support efficiently persisting these grouped records.

The writer must not independently reconstruct the book.

The authoritative L2 processor provides the initialization data.

### 15.17 Recording Failure Isolation

A PostgreSQL outage must not automatically terminate exchange acquisition.

The Recording Manager applies the configured recording and retention recovery policies.

The PostgreSQL client must not directly alter Stream Registry lifecycle state.

### 15.18 Database Testing

Required tests include:

- Connection pooling.
- Transaction commit.
- Transaction rollback.
- Batch insertion.
- NUMERIC round trips.
- Nanosecond timestamp preservation.
- Trade identity.
- L2 group preservation.
- Idempotent retries.
- Ambiguous commits.
- Out-of-order batch completion.
- Recording frontier reconstruction.
- Recovery after database restart.

### 15.19 PostgreSQL Performance

Benchmark:

    Trades Per Second

    Depth Messages Per Second

    Level Rows Per Second

    Transaction Commit Duration

    Database CPU Usage

    Index Maintenance

    Recording Queue Memory

    Recovery Replay Throughput

The relational L2 schema must be benchmarked using realistic depth-update sizes.

### 15.20 PostgreSQL Invariant

    PostgreSQL Libraries
    Execute Database
    Operations.

    MarketForge Owns
    Recording Identity,
    Durability Semantics,
    And Verified Coverage.

---

## 16. Arrow and Parquet Infrastructure Reuse

MarketForge already uses Arrow and Parquet for historical market-data processing.

The Live Retention subsystem should reuse compatible serialization and storage infrastructure.

It must not introduce an unrelated canonical event format solely because Live events arrive incrementally.

### 16.1 Dependency Decision

| Library | Decision | Responsibility |
|---|---|---|
| Existing Arrow crates | Reuse | Columnar arrays, schemas, record batches |
| Existing Parquet crate | Reuse | Parquet serialization and reading |
| Existing compression features | Reuse where appropriate | Parquet compression |
| `bytes` | Conditional | Efficient byte-buffer management |
| Filesystem utilities | Reuse or standard library | Temporary files and atomic publication |

Exact Arrow and Parquet crate versions must follow the existing compatible MarketForge dependency set.

### 16.2 Existing Historical Infrastructure

Reuse opportunities include:

    Arrow Schema Construction

    RecordBatch Creation

    Exact Decimal Encoding

    Canonical Event Serialization

    Parquet Writers

    Parquet Readers

    Schema Validation

    Output Validation

    Transactional File Publication

    Manifest Utilities

Not every historical executor should be reused directly.

The shared serialization and validation primitives are the primary targets.

### 16.3 Historical vs Live Execution

Historical processing typically operates on finite datasets.

Live Retention operates on continuously arriving events.

Conceptually:

    Historical:
        Finite Batch
            |
            v
        Parquet Writer

    Live:
        Continuous Events
            |
            v
        Bounded RAM Batches
            |
            v
        Parquet Writer

The writer infrastructure may be shared.

The scheduling and resource-management logic differ.

### 16.4 Live Retention Architecture

    Canonical Live Events
              |
              v
       Retention Manager
              |
              v
       Bounded RAM Batch
              |
              v
        Spill Scheduler
              |
              v
       Arrow RecordBatch
              |
              v
         Parquet Writer
              |
              v
       Committed Segment

The Retention Manager owns when and why a batch is spilled.

The Arrow/Parquet implementation owns serialization mechanics.

### 16.5 Canonical Schema Compatibility

The retained format must preserve applicable canonical information:

    Trade

    L2LevelUpdate

    EventTimestamp

    InstrumentId

    Exact Decimal Values

    Stream Identity

    Cursor

    Provenance

    Generation Boundaries

Live-specific metadata may require an additional schema layer.

The canonical financial semantics must remain unchanged.

### 16.6 Mixed Event Support

Derived streams may contain:

    Trade Events

    L2 Events

within one timeline.

The historical merge infrastructure already provides relevant mixed-event serialization conventions.

Live should reuse these conventions where compatible.

### 16.7 Event Kind

A mixed retained stream must preserve a deterministic event-kind discriminator.

Conceptually:

    EventKind

        Trade

        L2

The exact Arrow representation should reuse established historical schema definitions.

### 16.8 Decimal Encoding

The historical engine uses exact Decimal semantics.

Where the established Arrow schema uses Decimal128 or Decimal256 representations, Live must preserve the applicable precision and scale.

Live must not introduce arbitrary floating-point encodings for canonical prices or quantities.

### 16.9 Derived Decimal Precision

Historical merging and currency conversion may require wider decimal intermediate representations.

The existing historical conversion pipeline uses precision-preserving Arrow decimal conventions.

These should remain authoritative for compatible derived outputs.

A different Live storage format must not silently change rounding or overflow behavior.

### 16.10 Batch Construction

Live batching should be bounded by:

    MaximumEventCount

    MaximumEstimatedBytes

    MaximumBatchAge

    RetentionResourceBudget

Batch thresholds belong to Live Retention configuration.

Arrow does not own admission or retention resource policy.

### 16.11 Atomic L2 Groups

Logical native depth-message groups must remain atomic.

The serialization layer must preserve:

    DepthEventBoundary

    DepthSourceOperation

    DepthSourceEventMetadata

    Ordered L2LevelUpdate Collection

A Parquet row-group boundary must not cause a logically incomplete depth event to become visible as committed retained coverage.

### 16.12 Segment Publication

The existing historical infrastructure may provide reusable transactional file-publication primitives.

Live should reuse or adapt these mechanisms.

Conceptually:

    Write Temporary File
              |
              v
    Validate Output
              |
              v
    Commit Segment
              |
              v
    Publish Manifest

The Retention Manager determines when the segment becomes available for replay.

### 16.13 Parquet Compression

Compression configuration should reuse supported Parquet features.

Candidate algorithms depend on the installed Parquet implementation.

The selection should consider:

    Write Throughput

    Read Throughput

    Compression Ratio

    CPU Usage

    Memory Usage

The initial implementation should use a known-compatible codec and benchmark alternatives only when necessary.

### 16.14 Zero-Copy Considerations

Arrow provides efficient columnar representations.

However, converting individual live canonical events into Arrow arrays may still require allocation and copying.

The implementation should favor batch construction.

It must not assume that Arrow automatically makes the entire Live pipeline zero-copy.

### 16.15 Background Spill

Parquet serialization and compression may be CPU-intensive.

Spill should execute outside latency-sensitive exchange-processing tasks.

Potential implementations include:

    Dedicated Tokio Worker

    Bounded Blocking Task

    Dedicated CPU Worker

The chosen execution mechanism must enforce concurrency and memory budgets.

### 16.16 Replay Reader

Replay should consume retained events independently of physical storage location.

Conceptually:

    RetentionReader
          |
          +-- RAM Reader
          |
          +-- Parquet Reader

Both readers produce the shared retained-event contract.

The caller must not need to know whether a cursor resides in memory or Parquet.

### 16.17 Schema Evolution

Retained segments must identify the applicable serialization schema.

A newer MarketForge runtime must either:

- Read the previous schema.
- Perform an explicitly supported migration.
- Reject incompatible segments.

It must not silently reinterpret decimal fields or event identity.

### 16.18 Parquet Reuse Testing

Required tests include:

- Historical serialization compatibility.
- Exact Decimal round trips.
- Timestamp preservation.
- Trade serialization.
- L2 group serialization.
- Mixed event serialization.
- Cursor identity preservation.
- Segment validation.
- Manifest consistency.
- RAM-to-Parquet replay.
- Crash recovery.
- Corrupted segment rejection.

### 16.19 Parquet Invariant

    Arrow And Parquet
    Provide Shared
    Serialization Infrastructure.

    Live Retention Owns
    Coverage, Resource Limits,
    Spill Scheduling,
    And Replay Semantics.

---

## 17. Exact Decimal Arithmetic and Financial Numeric Reuse

MarketForge already implements exact financial normalization using decimal representations.

Live must preserve these semantics across source processing, order-book maintenance, recording, and derived transformations.

### 17.1 Primary Decision

Reuse the existing MarketForge decimal type and conversion utilities.

Do not introduce a second canonical decimal representation solely for Live.

### 17.2 Candidate Numeric Libraries

| Library | Decision | Responsibility |
|---|---|---|
| Existing MarketForge Decimal type | Authoritative | Canonical numeric values |
| `rust_decimal` | Reuse if already selected | Decimal arithmetic |
| Arrow Decimal128/Decimal256 | Reuse | Columnar decimal storage |
| PostgreSQL NUMERIC | Required database representation | Exact persistent decimals |
| `bigdecimal` | Conditional | Wider or flexible-scale database arithmetic |
| `num-bigint` | Conditional | Intermediate integer arithmetic where required |

Additional numerical libraries should be adopted only when existing precision and range requirements cannot be satisfied safely.

### 17.3 One Canonical Representation

Canonical events must use the existing financial numeric representation.

Conceptually:

    Native Value
          |
          v
    Exact Decimal Parsing
          |
          v
    Canonical Decimal
          |
       +--+----------------+
       |                   |
       v                   v
    BookStore          Derived Processing
       |                   |
       +---------+---------+
                 |
                 v
          Storage Adapters

Storage adapters may convert to database- or Arrow-specific representations.

These conversions must preserve the declared value and precision.

### 17.4 Native Values

Exchange-provided prices and quantities should remain exact whenever representable.

Examples:

    Price

    Base Quantity

    Quote Quantity

    Contract Quantity

    Contract Value

The normalizer must not silently round native values to arbitrary display precision.

### 17.5 Derived Values

Derived values may require multiplication or division.

Examples:

    Contract Quantity Conversion

    Currency Conversion

    Synthetic Reference Rates

Division may produce a nonterminating decimal expansion.

The existing historical rounding and precision policies must remain authoritative.

### 17.6 Intermediate Precision

A canonical decimal type may be sufficient for storing native values but insufficient for every intermediate multiplication.

The implementation must distinguish:

    Canonical Storage Precision

    Intermediate Arithmetic Precision

    Derived Calculation Precision

The historical engine's established precision and rounding contracts must remain authoritative.

Additional numerical capacity may be introduced when required, but it must not silently change canonical value semantics.

### 17.7 Decimal Overflow

All financial arithmetic must detect overflow.

Possible outcomes include:

    Exact Result

    Rounded Result Under Explicit Policy

    Unrepresentable Result

An unrepresentable calculation must produce an explicit failure or unavailable transformation result according to the owning component's contract.

It must not silently wrap, truncate, or convert to floating point.

### 17.8 PostgreSQL Numeric Conversion

The recording adapter must preserve exact canonical values when encoding PostgreSQL NUMERIC fields.

A conversion through the database client's numeric representation must be checked for precision and scale compatibility.

The adapter must not silently round a value to fit the database schema.

### 17.9 Arrow Decimal Conversion

Arrow Decimal128 and Decimal256 representations have fixed precision and scale constraints.

The serialization adapter must validate representability before constructing Arrow arrays.

The historical schema's declared precision and scale remain authoritative for compatible output.

### 17.10 Numerical Conversion Boundaries

The preferred design distinguishes:

    Canonical Financial Values

    Intermediate Derived Values

    Database Storage Values

    Columnar Storage Values

Each conversion boundary must be explicit and testable.

### 17.11 Avoid Duplicate Arithmetic Libraries

Introducing several decimal libraries may create:

- Different rounding behavior.
- Different scale limits.
- Different overflow semantics.
- Additional conversion overhead.
- Increased maintenance complexity.

Therefore, MarketForge should prefer one canonical decimal implementation.

Additional libraries may be used at infrastructure boundaries only when they satisfy a demonstrated requirement.

### 17.12 Decimal Compatibility Tests

Tests must cover:

    Large Coefficients

    Small Fractional Values

    High Decimal Scales

    Negative Values Where Valid

    Multiplication Overflow

    Division Rounding

    PostgreSQL Round Trips

    Arrow Round Trips

    Historical/Live Equivalence

### 17.13 Decimal Invariant

    Canonical Financial
    Semantics Must Remain
    Independent Of
    Storage And
    Execution Infrastructure.

---

## 18. Historical Merge Engine Reuse

MarketForge's historical processing engine already contains event merging and deterministic ordering functionality.

Live derived streams should reuse compatible ordering semantics and data structures.

However, historical finite-dataset execution and continuous live event processing have different scheduling requirements.

### 18.1 Primary Decision

Reuse:

    Canonical Merge Models

    Event Kind Semantics

    Deterministic Ordering Rules

    Source Ranking

    Provenance Representation

    Compatible Serialization

Adapt or implement separately:

    Continuous Input Scheduling

    Incremental Reorder Buffers

    Live Watermark Progression

    Source Liveness Handling

    Online Buffer Admission

    Recovery-Aware Merge Execution

The Live implementation must not introduce incompatible canonical output semantics.

### 18.2 Historical Merge Architecture

Historical merging operates on finite input datasets.

Conceptually:

    Historical Source A
             |
             +-----+
                   |
    Historical Source B
             |     |
             +-----+----> Historical Merge
                   |
    Historical Source C
             +-----+
                   |
                   v
           Ordered Finite Output

Historical processing can determine completion when all finite inputs have been exhausted.

### 18.3 Live Merge Architecture

Live merging operates on continuously arriving event sequences.

Conceptually:

    Live Source A
          |
          +-------+
                  |
    Live Source B |
          |       |
          +-------+----> Live Merge Engine
                  |            |
    Live Source C |            v
          +-------+      Reorder Buffer
                               |
                               v
                            Watermark
                               |
                               v
                         Derived Output

Input streams may remain active indefinitely.

The merger therefore requires explicit progress, buffering, and failure policies.

### 18.4 Shared Ordering Semantics

The historical merge pipeline uses deterministic ordering concepts corresponding to:

    Event Timestamp

    Source Rank

    Task Rank

    Event Ordinal

These establish a stable ordering when several events have equal timestamps.

Live should reuse compatible ordering keys and comparison logic.

### 18.5 MergeKey Reuse

The existing historical model includes a merge key conceptually represented as:

```rust
struct MergeKey {
    timestamp_ns: i64,
    stream_rank: usize,
    task_rank: usize,
    event_ordinal: u64,
}
```

The exact existing type should be reused when compatible.

Live must not invent a conflicting ordering rule for equivalent events.

### 18.6 Task Rank Compatibility

Historical task ranking may depend on finite processing partitions.

A Live stream does not necessarily have the same task-partition model.

Therefore, reuse must distinguish:

    Shared Deterministic Ordering Contract

from:

    Historical Execution-Specific Task Identity

Where historical task ranking is not applicable, the Live ordering contract must define an equivalent deterministic tie-breaking component without fabricating historical task identities.

### 18.7 Source Ranking

Source ranking should be resolved during derived-stream configuration compilation.

For example:

    Rank 0:
        Binance BTCUSDT Trades

    Rank 1:
        Bybit BTCUSDT Trades

    Rank 2:
        OKX BTCUSDT Trades

The ranking must remain stable within one derived generation.

A semantic rank change requires the applicable derived-generation transition.

### 18.8 Canonical Event Reuse

Live merging consumes existing canonical event types.

Relevant event categories include:

    Trade

    L2LevelUpdate

The merge must preserve native logical depth-message boundaries.

### 18.9 Mixed Event Timeline

Historical merging already supports mixed event output conventions.

Live should preserve the established event-kind discriminator and compatible output semantics.

Conceptually:

    Trade Event

    Depth Event Group

    Trade Event

    Depth Event Group

These may appear within one derived event timeline.

### 18.10 Depth Merge Semantics

A depth merge combines event timelines.

It does not automatically consolidate independent exchange order books.

For example:

    Binance Book Updates

    Bybit Book Updates

    OKX Book Updates

may produce:

    Combined L2 Event Timeline

without producing:

    One Synthetic Order Book

The Live Merge Engine must preserve each source's book identity and generation.

### 18.11 Live Reorder Buffer

The Live Merge Engine requires incremental buffering.

The initial implementation should prefer ordinary Rust collections unless profiling establishes a need for specialized structures.

Potential primitives include:

    BinaryHeap

    BTreeMap

    VecDeque

    Vec

The appropriate collection depends on the ordering and release algorithm.

### 18.12 Priority Queue Selection

A priority queue can support efficient retrieval of the next eligible event.

The selected implementation must handle:

    Event-Time Ordering

    Deterministic Tie-Breaking

    Equal Timestamps

    Source Identity

    Buffer Resource Limits

A custom queue should be introduced only when standard collections cannot satisfy the required semantics or measured performance.

### 18.13 Watermark Ownership

The Live Merge Engine owns its watermark state.

This includes:

    Per-Source Progress

    Combined Progress

    Finalized Output Frontier

    Source Liveness

    Source Failure Policy

    Late-Event Classification

No external generic sorting library can replace these MarketForge-specific guarantees.

### 18.14 Source Recovery

A source may disconnect or lose integrity.

The Live merger must respond according to its configured source-failure policy.

The historical merge executor's finite-input completion logic cannot automatically model this condition.

### 18.15 Nested Derived Streams

A derived stream may consume another derived stream.

For example:

    Binance Trades ----+
                       |
    Bybit Trades ------+----> Merge A
                       |        |
    OKX Trades --------+        |
                                +----> Merge B
    Bitget Trades --------------+

Merge B must respect Merge A's declared ordering and finalized-frontier semantics.

It must not assume that one emitted upstream event proves complete upstream progress.

### 18.16 Derived Provenance

Derived output must preserve:

    Derived Stream Identity

    Derived Generation

    Originating Source Identity

    Source Cursor

    Transformation Revision

    Ordering Metadata

These concepts should reuse historical models where compatible.

### 18.17 Merge Compatibility Testing

Required tests include:

- Historical ordering regression.
- Equal timestamp tie-breaking.
- Source rank changes.
- Mixed trade/depth events.
- L2 group atomicity.
- Live watermark progression.
- Late-event handling.
- Source failure.
- Derived generation replacement.
- Nested derived dependencies.

### 18.18 Merge Reuse Invariant

    Reuse Historical
    Ordering Semantics.

    Implement Live-Specific
    Scheduling And
    Watermark Management
    Where Required.

---

## 19. Currency Conversion Engine Reuse

MarketForge's historical engine already implements financial currency-conversion logic.

Live conversion must reuse compatible arithmetic, currency identities, path semantics, and reference-eligibility rules.

It must not create a second independent system for interpreting conversion rates.

### 19.1 Primary Decision

Reuse:

    Currency Identity

    Instrument Metadata

    Conversion Arithmetic

    Conversion Direction

    Reference Path Semantics

    Conversion Status

    Decimal Precision Policy

    Historical Compatibility Tests

Adapt:

    Incremental Reference State

    Live Reference Availability

    Reference Liveness

    Derived Stream Dependencies

    Event-Time Buffering

    Recovery-Aware State Management

### 19.2 Historical Conversion

Historical conversion operates on finite data and resolved reference streams.

Conceptually:

    Historical Source Events
              |
              v
       Reference Selection
              |
              v
        Exact Conversion
              |
              v
       Converted Output

The historical engine defines eligible reference observations and the resulting financial semantics.

### 19.3 Live Conversion

Live conversion maintains incrementally updated reference state.

Conceptually:

    Source Event
         |
         v
    Conversion Processor
         |
         +----------------+
                          |
                          v
                 Reference Registry
                          |
                          v
                 Eligible Reference
                          |
                          v
                  Exact Arithmetic
                          |
                          v
                  Derived Output

The Live Reference Registry owns current reference availability and bounded reference history.

### 19.4 Shared Conversion Arithmetic

The same underlying arithmetic should be used by historical and Live execution.

For example:

    ETH/BTC × BTC/USDT = ETH/USDT

The financial calculation must not differ merely because reference data arrives through WebSockets instead of historical archives.

### 19.5 Direct Conversion

A direct conversion may use one reference instrument.

Example:

    ETH/BTC

with:

    BTC/USDT

produces:

    ETH/USDT

The conversion must preserve the original ETH/BTC values.

### 19.6 Multi-Hop Conversion

A conversion may require multiple reference instruments.

The selected path must preserve:

    Currency Direction

    Reference Identity

    Reference Eligibility

    Exact Arithmetic

    Transformation Provenance

The Live implementation must not silently replace an unavailable reference leg with an assumed value.

### 19.7 Inverse Reference Direction

A reference may be available in the inverse direction.

For example:

    Available:
        USD/BTC

    Required:
        BTC/USD

The conversion engine must apply the correct reciprocal transformation using the established historical precision policy.

### 19.8 Reference Eligibility

The historical conversion contract distinguishes internal and external reference eligibility.

Relevant rules include:

    Internal References:
        Eligible According To
        Established Same-Event Ordering

    External References:
        Must Satisfy
        Strict Earlier-Than
        Event-Time Requirements

Live conversion must preserve these rules.

It must not automatically substitute processing arrival order for event-time eligibility.

### 19.9 Reference Freshness

Live reference observations may become stale.

The Live Reference Registry must apply the configured freshness policy.

The historical reference-selection logic should be reused where applicable.

### 19.10 Missing References

A configured reference path may be valid while its current eligible price is unavailable.

This is a runtime data-availability condition.

The conversion engine must preserve the established:

    Unavailable

conversion outcome where supported.

### 19.11 Missing Paths

A conversion path may be impossible to resolve from the validated instrument and reference graph.

This is distinct from a temporarily missing price.

The Configuration Compiler must reject unsupported required paths according to the selected path-resolution policy.

### 19.12 Stablecoin Conversion

The conversion engine must not assume that different stablecoins have equal values.

For example:

    BTC/USDC

and:

    BTC/USDT

are not automatically interchangeable.

A valid conversion requires the applicable reference relationship or explicit supported policy.

### 19.13 Conversion Status Reuse

The historical conversion contract includes:

    Identity

    Converted

    Unavailable

    Skipped

Live must retain compatible meanings.

These are event-level outcomes.

They are not equivalent to runtime health states.

### 19.14 Original Canonical Values

Conversion produces derived output.

It must not mutate the original canonical source event.

Conceptually:

    Original Event
          |
          +----------------+
                           |
                           v
                    Converted Event

Both retain their appropriate identities and provenance.

### 19.15 Conversion Reference Registry

The Live Reference Registry may use standard Rust collections.

Possible structures include:

    HashMap

    BTreeMap

    VecDeque

    Bounded Time-Ordered Buffers

The selected structure must support the required event-time lookup semantics.

A specialized database or external cache is not required for the initial implementation.

### 19.16 Reference State Boundedness

Reference state must remain bounded by:

    Required Conversion Paths

    Reference Freshness

    Event-Time Buffer Requirements

    Active Derived Dependencies

    Resource Limits

The implementation must not retain unlimited reference history.

### 19.17 Reference Acquisition

Reference streams must reuse the shared Source Stream Registry.

For example:

    Bybit ETHBTC Trades
              |
              v
    ETH/BTC -> ETH/USDT Conversion
              ^
              |
    Bybit BTCUSDT Trades

If another consumer already requires BTCUSDT trades, conversion must reuse the existing acquisition.

### 19.18 Conversion Reuse Testing

Required tests include:

- Historical conversion regression.
- Direct conversion.
- Inverse direction.
- Multi-hop paths.
- Event-time reference selection.
- Same-timestamp eligibility.
- Missing reference.
- Stale reference.
- Invalid conversion path.
- Decimal overflow.
- Rounding.
- Original source immutability.
- Reference dependency deduplication.

### 19.19 Conversion Reuse Invariant

    One Financial
    Conversion Semantics.

    Historical And Live
    Differ In Execution
    And Reference-State
    Management Only.

---

## 20. Collections, Buffers, and Memory Management

MarketForge Live processes continuously arriving data and must maintain bounded memory usage.

Standard Rust collections should remain the default.

Specialized collection libraries should be introduced only where required by measured workloads.

### 20.1 Dependency Candidates

| Library | Decision | Possible Use |
|---|---|---|
| `std::collections` | Required | Maps, queues, heaps, ordered structures |
| `bytes` | Recommended candidate | Reference-counted byte buffers |
| `smallvec` | Optional | Small collections with predictable common sizes |
| `arrayvec` | Optional | Fixed-capacity collections |
| `bumpalo` | Optional | Scoped arena allocation |
| `slab` | Optional | Indexed session/resource storage |
| `hashbrown` | Optional | Specialized hash-map requirements |

None of the optional libraries should be introduced without a specific allocation or performance justification.

### 20.2 Standard Collections First

Use standard collections where they satisfy the contract.

Examples:

    HashMap:
        Registry Indexes

    BTreeMap:
        Ordered Price Levels

    VecDeque:
        FIFO Buffers

    BinaryHeap:
        Event Ordering Queues

    Vec:
        Canonical Event Batches

### 20.3 Existing BookStore Collections

The shared BookStore already uses ordered structures suitable for maintaining price levels.

Live should preserve these structures unless benchmarks establish a clear benefit from changing them.

Any modification must preserve historical BookStore tests.

### 20.4 Bytes and Buffer Reuse

The `bytes` crate may be useful for:

    WebSocket Frame Payloads

    IPC Serialization

    Publisher Queues

    Framed Control Messages

The implementation should minimize unnecessary payload copying.

However, zero-copy aspirations must not obscure resource ownership or force unrelated components to retain large shared buffers.

### 20.5 SmallVec Evaluation

`smallvec` may help when a collection usually contains only a small number of elements.

Potential examples:

    Small Dependency Sets

    Small Protocol Field Collections

    Short Error Contexts

Its benefit must be validated against allocation measurements.

A fixed inline capacity increases the size of every containing object, even when the collection is empty.

### 20.6 ArrayVec Evaluation

`arrayvec` is useful for fixed-capacity collections with explicit maximum sizes.

Potential applications include bounded small protocol structures.

It must not be used where legitimate exchange messages may exceed the selected fixed capacity without a defined failure policy.

### 20.7 Arena Allocation

Arena allocation may reduce overhead in scoped batch-processing workloads.

However, arenas can retain memory until the entire allocation scope is released.

They are unsuitable as an unexamined replacement for bounded per-event ownership.

### 20.8 Buffer Ownership

Each bounded buffer must have:

    Owning Component

    Maximum Capacity

    Admission Policy

    Release Policy

    Failure Semantics

Examples:

    Publisher Subscriber Queue

    Merge Reorder Buffer

    Recording Batch Queue

    Retention RAM Buffer

    WebSocket Inbound Queue

### 20.9 Memory Pressure

Memory usage should be measured by the corresponding subsystem.

The Metrics Registry may expose resource summaries.

Metrics do not replace authoritative resource admission.

### 20.10 Buffer Performance Testing

Benchmarks should measure:

    Allocation Count

    Allocated Bytes

    Peak Memory

    Buffer Reuse

    Queue Contention

    Event Processing Throughput

Specialized collection adoption requires evidence of a meaningful benefit.

### 20.11 Memory Invariant

    Standard Rust
    Collections Remain
    The Default.

    Specialized Allocation
    Strategies Require
    Demonstrated Benefit.

---

## 21. Data Processing and Storage Dependency Summary

The recommended data-processing and persistence stack should primarily reuse existing MarketForge infrastructure.

### 21.1 Recommended Dependencies

| Component | Preferred Approach | Status |
|---|---|---|
| Canonical trades | Existing MarketForge models | Reuse |
| Canonical L2 | Existing MarketForge models | Reuse |
| Exact Decimal | Existing MarketForge numeric types | Reuse |
| Timestamp normalization | Existing normalization primitives | Reuse |
| Instrument metadata | Existing catalog contracts | Reuse |
| BookStore | Existing shared implementation | Reuse |
| Depth grouping | Existing historical depth contracts | Reuse |
| Merge ordering | Existing historical ordering semantics | Reuse/adapt |
| Currency conversion | Existing historical arithmetic | Reuse/adapt |
| Arrow schemas | Existing Arrow infrastructure | Reuse |
| Parquet serialization | Existing Parquet infrastructure | Reuse |
| PostgreSQL driver | Existing compatible driver, otherwise evaluate SQLx | Select |
| Database batching | MarketForge Recording Manager | Implement |
| Retention coverage | MarketForge Retention Manager | Implement |
| Live watermarks | MarketForge Merge Engine | Implement |
| Live reference state | MarketForge Reference Registry | Implement |

### 21.2 Optional Dependencies

| Library | Adoption Condition |
|---|---|
| `bigdecimal` | Required precision or scale exceeds existing compatible representation |
| `num-bigint` | Wider intermediate arithmetic is necessary |
| `smallvec` | Profiling identifies repeated small allocations |
| `arrayvec` | Fixed-capacity semantics improve a measured workload |
| `bumpalo` | Scoped batch allocation provides demonstrated value |
| `slab` | Indexed resource storage simplifies ownership |
| `hashbrown` | Hash-map behavior or performance justifies replacement |

### 21.3 Avoided Duplication

Do not introduce:

    Second Canonical Trade Model

    Second Canonical L2 Model

    Second BookStore

    Second Financial Conversion Engine

    Second Instrument Catalog Authority

    Separate Live Arrow Schema Semantics

    Independent PostgreSQL Recording Authority

    Independent Derived Ordering Semantics

### 21.4 Data-Plane Dependency Boundary

The intended structure is:

    Historical Engine
           |
           v
       Shared Core
           ^
           |
       Live Engine
           |
           v
    Infrastructure Adapters
           |
      +----+-----------+
      |                |
      v                v
    PostgreSQL       Parquet

The shared core remains independent of Live-specific connectivity, recording, and TUI infrastructure.

### 21.5 Data-Plane Performance Rule

Performance optimization should prioritize:

1. Avoiding unnecessary event copies.
2. Reusing compiled normalization plans.
3. Preserving single-owner mutable state.
4. Batching expensive storage operations.
5. Reducing unnecessary allocations.
6. Minimizing hot-path synchronization.
7. Profiling actual bottlenecks.

Optimizations must preserve correctness and historical compatibility.

### 21.6 Final Data-Processing Invariant

    Reuse MarketForge's
    Existing Financial
    And Canonical Logic.

    Build Only The
    Live-Specific Execution,
    Coordination,
    And Persistence
    Components Required.

---
## 22. Metrics Collection and Health Evaluation

MarketForge Live already defines its Metrics Registry and HealthEvaluator in Document 12.

These components remain authoritative for internal observability.

External libraries may provide measurement primitives, histogram implementations, or export formatting, but they must not create another independently maintained source of metric truth.

### 22.1 Primary Decision

The recommended architecture is:

    MarketForge Components
             |
             v
       Typed Metric Handles
             |
             v
       Metrics Registry
             |
        +----+------------------+
        |                       |
        v                       v
    HealthEvaluator       Export Adapters
        |                       |
        v                       v
    Runtime Control API    Prometheus
        |
        v
    Ratatui

The Metrics Registry remains usable without any external monitoring application.

### 22.2 Dependency Candidates

| Library | Decision | Intended Use |
|---|---|---|
| `std::sync::atomic` | Required | Lightweight counters and gauges |
| `tokio::sync` | Required | Snapshot publication and coordination |
| `hdrhistogram` | Optional | Bounded latency distributions |
| `metrics` | Evaluate only if needed | General-purpose metrics facade |
| `metrics-exporter-prometheus` | Alternative | Prometheus export using the `metrics` ecosystem |
| `prometheus-client` | Recommended exporter candidate | OpenMetrics-compatible encoding |
| Custom typed metric structures | Preferred initial approach | MarketForge Metrics Registry |

These candidates must not all be adopted simultaneously.

The initial implementation should favor the smallest set compatible with Document 12.

### 22.3 Typed Metrics

MarketForge components should receive typed metric handles.

Conceptually:

```rust
pub struct TradeMetrics {
    pub events_received: AtomicU64,
    pub events_accepted: AtomicU64,
    pub duplicates_detected: AtomicU64,
    pub continuity_failures: AtomicU64,
}
```

The actual fields and visibility should follow the Metrics Registry's implementation.

Typed handles avoid repeated metric-name lookups during canonical processing.

### 22.4 Atomic Counters

Frequently updated statistical counters may use relaxed atomic operations when the counter does not establish an additional synchronization guarantee.

For example:

```rust
metrics
    .events_accepted
    .fetch_add(1, Ordering::Relaxed);
```

Authoritative lifecycle and correctness state require their own synchronization.

A metric counter must not become the authority for trade continuity or book validity.

### 22.5 Gauges

Gauges describe current measurements such as:

    ActiveConnections

    ActiveStreams

    QueueBytes

    BufferedEvents

    RecordingPendingBatches

Gauges should be maintained by the component that owns the measured resource.

### 22.6 Latency Histograms

The runtime may use bounded histograms for:

    CanonicalProcessingDuration

    BookMutationDuration

    PublisherWriteDuration

    RecordingCommitDuration

    MergeProcessingDuration

`hdrhistogram` is a candidate when its value range, precision, memory cost, and synchronization characteristics satisfy the measurement requirements.

It is not mandatory.

### 22.7 Histogram Requirements

Histograms must have:

- Bounded memory.
- Explicit measurement units.
- Defined observation ranges.
- Documented accuracy.
- Defined aggregation windows.
- Safe concurrency behavior.

The initial implementation may instead use fixed buckets or local accumulators.

The choice should follow measurement requirements and benchmarking.

### 22.8 Metrics Facade Evaluation

The `metrics` crate provides a general-purpose instrumentation abstraction.

It may be useful for applications that want interchangeable metric recorders.

However, MarketForge already defines:

    Typed Metric Structures

    Explicit Metric Scopes

    Metrics Registry

    HealthEvaluator

    Monitoring Snapshots

Introducing another recorder could duplicate responsibilities.

Therefore, the `metrics` ecosystem should not be adopted by default.

### 22.9 Snapshot Generation

The Metrics Registry periodically produces bounded snapshots.

Snapshot consumers include:

    Runtime Monitoring API

    Ratatui

    HealthEvaluator

    Optional Exporters

Snapshot generation must not require pausing canonical processing.

### 22.10 Metric Identity

Metrics must preserve their scope:

    Runtime

    Exchange

    Connection

    Native Subscription

    Source Stream

    Derived Stream

    Subscriber Session

    Recording

    Recovery

Metric identities and lifecycle generations must remain explicit.

### 22.11 Metric Cardinality

The registry must avoid unbounded metric dimensions.

Do not register unique metric identities for every:

    Native Trade

    Order ID

    Event Cursor

    Request ID

    Arbitrary Error Message

Bounded registered stream identities may be used where appropriate.

### 22.12 HealthEvaluator Independence

Health evaluation remains MarketForge-owned.

The HealthEvaluator consumes:

    Authoritative Component State

    Required Capabilities

    Diagnostic Metrics

    Dependency Information

It must not reconstruct market-data integrity from generic metric counters.

### 22.13 Metrics Disabled

Disabling optional diagnostic collection must not disable:

    Trade Continuity Validation

    L2 Synchronization

    Recording Commit Tracking

    Recovery Coordination

Authoritative state remains available independently.

### 22.14 Metrics Testing

Required tests include:

- Counter correctness.
- Gauge correctness.
- Counter resets.
- Metric-generation changes.
- Snapshot consistency.
- Rate calculations.
- Histogram accuracy.
- Missing measurements.
- Cardinality limits.
- Health evaluation independence.
- Hot-path overhead.

### 22.15 Metrics Invariant

    MarketForge Owns
    Its Metric Semantics
    And Health Assessment.

    External Libraries
    May Provide Efficient
    Measurement Primitives.

---

## 23. Optional Prometheus Export

MarketForge Live supports optional Prometheus-compatible metrics exposition.

Prometheus and Grafana remain independently operated applications.

The exporter must not become a runtime dependency for ordinary market-data acquisition, internal monitoring, or Ratatui.

### 23.1 Primary Decision

Recommended candidate:

    prometheus-client

Its typed metric families, registry, and exposition encoder are relevant to the optional adapter.

However, its use must preserve MarketForge's existing authoritative Metrics Registry.

### 23.2 Dependency Candidates

| Library | Decision | Intended Use |
|---|---|---|
| `prometheus-client` | Recommended candidate | OpenMetrics exposition |
| `metrics-exporter-prometheus` | Alternative | Export with the `metrics` facade |
| `hyper` | Conditional | Lightweight HTTP serving |
| `axum` | Optional alternative | HTTP routing when broader HTTP services justify it |
| `tokio` | Existing | Async listener and request handling |

Only one exposition implementation should be selected.

### 23.3 Exporter Architecture

    Metrics Registry
          |
          v
    Prometheus Adapter
          |
          v
      HTTP /metrics
          |
          v
       Prometheus
          |
          v
         Grafana

The exporter translates MarketForge measurements into the selected exposition format.

### 23.4 No Duplicate Metrics Registry

A second independent registry must not collect market-data measurements separately from the MarketForge Metrics Registry.

The preferred initial implementation is:

    Existing Typed Metric State
              |
              v
    Periodic Export Snapshot
              |
              v
    Prometheus Representation

If `prometheus-client` is selected, its registry is used only as an exposition representation or adapter-managed view.

It must not become an independent authority for canonical processing measurements.

### 23.5 Encoder Compatibility

`prometheus-client` provides an OpenMetrics text encoder and typed metric abstractions.

The final implementation must confirm compatibility with the selected Prometheus server and its accepted exposition format.

Use the correct response content type for the actual format served.

The exporter must not simply label arbitrary text as Prometheus-compatible output.

### 23.6 Counters

Cumulative counters must preserve their declared lifecycle.

Examples:

    marketforge_trade_accepted_total

    marketforge_l2_sequence_gaps_total

    marketforge_connection_reconnects_total

Counter resets and generation changes must be handled consistently.

### 23.7 Gauges

Examples:

    marketforge_publisher_queue_bytes

    marketforge_retention_memory_bytes

    marketforge_recording_pending_events

    marketforge_active_source_streams

Gauge values must originate from existing authoritative metric state.

### 23.8 Histograms

Exported histogram data must preserve:

    Bucket Boundaries

    Cumulative Bucket Counts

    Observation Count

    Observation Sum

Duration histograms use seconds.

Internally sampled percentiles must not be falsely exported as ordinary histogram buckets.

### 23.9 Export Units

Prometheus-facing metric names and values must use consistent units.

Recommended:

    Time:
        Seconds

    Memory:
        Bytes

    Counts:
        Cumulative Unsigned Counts

    Ratios:
        Documented Dimensionless Values

Internal nanosecond durations may be converted at the export boundary.

### 23.10 Export Labels

Recommended bounded labels:

    exchange

    market_family

    channel

    stream_id

    failure_category

The exporter must enforce cardinality limits.

### 23.11 Removed Stream Series

When a stream is removed, exporter-specific state must eventually release the associated metric series.

The exporter must not retain an unbounded series history for obsolete streams.

### 23.12 HTTP Server Selection

The exporter requires only one read-only endpoint:

    GET /metrics

A complete application web framework is not necessary solely for this requirement.

A minimal HTTP implementation using existing compatible networking libraries is preferred.

Evaluate `hyper` or an already available HTTP server abstraction.

Use `axum` only if its broader routing capabilities provide actual value.

### 23.13 Endpoint Security

Default:

    127.0.0.1:9091

The endpoint must be disabled unless explicitly enabled.

Non-loopback exposure requires appropriate access controls.

The exporter must not expose:

    API Credentials

    Database Passwords

    Authentication Tokens

    Canonical Market-Data Payloads

    Runtime Mutation Commands

### 23.14 Request Resource Limits

The exporter must enforce:

    MaximumConcurrentScrapes

    MaximumResponseBytes

    RequestTimeout

    ExportRefreshInterval

Scraping must not require full BookStore traversal or blocking acquisition workers.

### 23.15 Exporter Failure Isolation

If the exporter fails:

    Exporter:
        Unavailable

    Metrics Registry:
        Operational

    HealthEvaluator:
        Operational

    Ratatui:
        Operational

    Canonical Processing:
        Unaffected

Exporter errors are reported through bounded local diagnostics.

### 23.16 External Deployment

MarketForge does not launch or supervise:

    Prometheus

    Grafana

Operators may configure these applications independently.

Example deployment configuration and sample dashboards may be provided separately.

### 23.17 Prometheus Testing

Tests must cover:

- Exposition formatting.
- Counters.
- Gauges.
- Histograms.
- Label escaping.
- Missing values.
- Stream removal.
- Metric-generation changes.
- Request limits.
- Loopback binding.
- Disabled exporter behavior.
- Failure isolation.

### 23.18 Prometheus Invariant

    Prometheus Export
    Is A Read-Only
    Optional Adapter.

    It Must Not Become
    A Second Source
    Of Metric Truth.

---

## 24. TOML Configuration and Serialization

MarketForge uses TOML for human-editable exchange specifications and runtime configuration.

The Configuration System remains responsible for validation, revision control, compilation, and persistence.

Serialization libraries provide parsing and encoding primitives.

### 24.1 Dependency Candidates

| Library | Decision | Responsibility |
|---|---|---|
| `serde` | Required | Typed serialization/deserialization |
| `toml` | Recommended | TOML parsing and encoding |
| `serde_json` | Recommended | JSON control messages and diagnostics |
| `toml_edit` | Optional | Preserving formatting and comments |
| `serde_path_to_error` | Optional | Structured deserialization error paths |
| `thiserror` | Recommended candidate | Typed error definitions |

Reuse existing compatible versions and serialization helpers wherever possible.

### 24.2 Configuration File Scope

The initial configuration layout includes:

    data/.config/live/
        |
        +-- live.toml
        |
        +-- exchanges/
        |
        +-- formats/
        |
        +-- runtime/
                |
                +-- sources.toml
                |
                +-- derived.toml

These files follow the schemas established in Document 03.

### 24.3 Serde Models

Use typed Rust structures for configuration.

Conceptually:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub detailed_timing: bool,
    pub snapshot_interval_ms: u64,
}
```

The actual structures must follow the validated configuration schema.

### 24.4 Strict Configuration Parsing

Configuration deserialization should reject unknown fields where silently ignoring them could produce incorrect runtime behavior.

For example, a misspelled recording setting must not silently disable or alter persistence.

The implementation should use appropriate schema validation and strict deserialization controls.

### 24.5 Parsing vs Semantic Validation

TOML parsing validates syntax and structure.

The Configuration Compiler additionally validates:

    Exchange Capabilities

    Instrument Metadata

    Stream Identity

    Derived Dependencies

    Resource Budgets

    Runtime Reconfiguration Policies

A successfully parsed file is not necessarily an executable configuration.

### 24.6 TOML Editing

`toml_edit` may be useful when the application must preserve human-authored formatting, comments, and ordering.

However, MarketForge's persisted desired-state model is revisioned and authoritative.

The runtime must not introduce two competing configuration authorities merely to preserve file formatting.

### 24.7 Configuration Persistence

The Configuration Manager owns atomic persistence.

The TOML library does not provide:

    Revision Validation

    Atomic File Publication

    Directory Synchronization

    Runtime Reconciliation

These operations remain MarketForge responsibilities.

### 24.8 Configuration Imports

Manual TOML imports must pass through the same validation and revision mechanism as Ratatui and Python updates.

Directly replacing files while the runtime is active must not bypass the Configuration Manager.

### 24.9 Configuration Diff

A semantic configuration diff compares compiled effective values.

It must not rely only on textual TOML differences.

### 24.10 Exact Numeric Configuration

Financial decimal values must not be deserialized through lossy floating-point intermediates.

Where exact decimal semantics are required, use compatible exact representations and explicit string parsing.

### 24.11 Configuration Error Reporting

Validation errors should identify:

    ConfigurationPath

    ErrorCategory

    Message

    ApplicableConstraint

These errors are exposed through the Runtime Control API.

### 24.12 Configuration Testing

Required tests include:

- Valid TOML parsing.
- Missing fields.
- Unknown fields.
- Invalid numeric ranges.
- Exchange profile references.
- Derived dependency validation.
- Configuration inheritance.
- Revision conflicts.
- Atomic persistence.
- Schema-version compatibility.

### 24.13 Configuration Invariant

    Serialization Libraries
    Parse And Encode
    Configuration.

    MarketForge Owns
    Configuration Meaning,
    Validation,
    And Runtime Application.

---

## 25. Structured Logging and Diagnostics

MarketForge Live requires structured operational logging for connection lifecycle, integrity failures, recovery operations, recording failures, configuration changes, and runtime supervision.

Operational logs remain local.

PostgreSQL is reserved for market-data recording and existing instrument metadata, not operational log history.

### 25.1 Primary Decision

Reuse the existing MarketForge logging infrastructure where compatible.

Recommended Rust ecosystem candidates:

    tracing

    tracing-subscriber

    tracing-appender

The logging implementation must remain independent of canonical event-processing correctness.

### 25.2 Dependency Candidates

| Library | Decision | Responsibility |
|---|---|---|
| `tracing` | Recommended | Structured events and spans |
| `tracing-subscriber` | Recommended | Filtering, formatting, subscribers |
| `tracing-appender` | Recommended candidate | Local file output and nonblocking writers |
| `tracing-log` | Conditional | Compatibility with `log`-based dependencies |
| `serde_json` | Existing | Structured JSON diagnostics where selected |

A second logging framework should not be introduced unnecessarily.

### 25.3 Logging Architecture

Conceptually:

    MarketForge Components
             |
             v
       Structured Events
             |
             v
      Logging Subscriber
             |
             v
       Bounded Log Writer
             |
             v
        Local Log Files

Significant operational events may additionally enter bounded diagnostic history exposed through the Control API.

This is separate from ordinary file logging.

### 25.4 Structured Context

Operational logs should include relevant fields such as:

    Component

    Exchange

    StreamId

    ConnectionId

    ConnectionGeneration

    SubscriptionGeneration

    BookGeneration

    RecoveryOperationId

    ConfigurationRevision

    FailureCategory

Only applicable fields should be present.

### 25.5 Logging Levels

Supported levels:

    Trace

    Debug

    Info

    Warn

    Error

The effective logging level should follow the runtime configuration contract.

Selected changes may apply without restarting exchange connections.

### 25.6 Per-Event Logging

Ordinary accepted trades and L2 level changes must not produce individual structured log entries by default.

At high event rates, per-event logging would introduce unnecessary:

    Formatting

    Allocation

    Queue Traffic

    Disk Writes

    CPU Consumption

Market-data throughput belongs to the Metrics Registry.

### 25.7 Operational Events

Significant events may include:

    ConnectionEstablished

    ConnectionLost

    SubscriptionRejected

    SequenceGapDetected

    BookInvalidated

    RecoveryStarted

    RecoveryCompleted

    RecordingFailed

    RetentionCapacityExceeded

    ConfigurationApplied

These events should preserve sufficient diagnostic context for investigation.

### 25.8 Nonblocking Logging

`tracing-appender` provides nonblocking writer support through a dedicated logging thread.

This is a suitable candidate for avoiding synchronous filesystem writes from processing tasks.

However, a nonblocking writer still has finite capacity.

The implementation must explicitly configure its behavior when the logging queue is full.

### 25.9 Log Queue Saturation

Possible policies include:

    Drop Low-Priority Diagnostics

    Apply Bounded Backpressure

    Escalate Diagnostic Failure

    Preserve Critical Events Through
    A Separate Bounded Mechanism

No policy may silently weaken canonical market-data integrity.

A diagnostic event that must survive process failure requires an explicitly defined persistence guarantee.

Ordinary asynchronous logging does not automatically provide that guarantee.

### 25.10 Log Rotation

Local log files require bounded retention.

Relevant policies include:

    File Rotation

    Maximum Total Disk Usage

    Retained File Count

    Retention Duration

    Cleanup Policy

The selected file appender may provide rotation primitives.

MarketForge remains responsible for enforcing any required total storage budget.

### 25.11 Logging During Recovery

Recovery may produce repeated failures.

Logs should retain useful information without generating uncontrolled repetition.

Relevant strategies include:

    Bounded Repetition

    Failure Classification

    Attempt Counts

    State Transition Logging

Repeated retry attempts should not flood storage with redundant large diagnostic payloads.

### 25.12 Sensitive Information

Logs must not expose:

    API Keys

    Authentication Tokens

    Private Credentials

    Database Passwords

    Sensitive HTTP Headers

    Unrestricted Configuration Secrets

Redaction must occur before sensitive data reaches the logging writer.

### 25.13 Metrics vs Logs

Metrics describe aggregate measurements.

Logs describe significant events and their diagnostic context.

Example:

    Metric:
        l2_sequence_gaps_total = 7

    Log:
        Sequence gap detected for
        Binance BTCUSDT Depth.

These are complementary.

Routine metric snapshots should not be continuously duplicated into ordinary logs by default.

### 25.14 Local Diagnostic History

MarketForge may maintain a bounded in-memory history of structured operational incidents.

This history supports Ratatui and Control API inspection.

It is not the authoritative source for current component state.

### 25.15 Logging Failure Isolation

A logging writer failure must not ordinarily terminate canonical acquisition.

The runtime must:

- Report logging failure where possible.
- Avoid uncontrolled retry loops.
- Preserve bounded resource consumption.
- Continue authoritative component operation.
- Avoid misrepresenting dropped diagnostics as persisted.

### 25.16 Logging Tests

Required tests include:

- Structured field correctness.
- Runtime log-level updates.
- Bounded queue behavior.
- Queue saturation.
- Log rotation.
- File write failures.
- Sensitive-field redaction.
- Recovery-log throttling.
- Graceful flush.
- Abrupt process termination.
- Canonical processing isolation.

### 25.17 Logging Invariant

    Structured Logs
    Preserve Operational
    Diagnostic Context.

    They Do Not Replace
    Metrics, Integrity State,
    Or Canonical Event Storage.

---

## 26. Runtime Control API Libraries

MarketForge Live exposes a versioned management protocol through a dedicated Unix domain socket.

The Control API contracts are defined in Document 13.

External libraries should provide framing, serialization, and asynchronous transport primitives.

MarketForge retains responsibility for request validation, authorization, revision control, command execution, and operation tracking.

### 26.1 Primary Decision

Use:

    Tokio UnixStream / UnixListener

    Serde

    Serde JSON

    Explicit Length-Prefixed Framing

No additional RPC framework is initially required.

### 26.2 Dependency Candidates

| Library | Decision | Responsibility |
|---|---|---|
| `tokio` | Required | Unix socket transport |
| `serde` | Required | Typed messages |
| `serde_json` | Recommended | Control-message encoding |
| `tokio-util` | Recommended | Optional framing utilities |
| `bytes` | Conditional | Bounded frame buffers |
| `thiserror` | Recommended candidate | Typed control errors |
| `uuid` | Conditional | Identifiers when the selected contract requires them |
| `tonic` | Not initially required | gRPC framework |
| `jsonrpsee` | Not initially required | JSON-RPC framework |

The initial interface does not justify introducing a separate RPC infrastructure stack.

### 26.3 Existing Protocol Contract

Document 13 selects:

    Unix Domain Socket

    Length-Prefixed JSON

    Typed Requests

    Typed Responses

    Explicit Protocol Versions

    Request Identifiers

    Asynchronous Operation Identifiers

The implementation must preserve that wire contract.

### 26.4 Framing Primitives

The control protocol may use `tokio-util::codec` where the selected framing implementation matches its requirements.

Otherwise, a small explicit framing implementation using Tokio and bounded buffers is sufficient.

The initial format is conceptually:

    4-Byte Length Prefix

    Big-Endian Length

    JSON Payload

The server must validate the declared length before allocating the complete message buffer.

### 26.5 Serialization

Serde provides typed Rust serialization.

The Control API must not expose arbitrary internal Rust objects directly.

The wire schema must remain independently versioned.

### 26.6 Request Correlation

MarketForge owns:

    ClientSessionId

    RequestId

    OperationId

These identities have different lifecycles.

A framing or serialization library does not implement their semantic contracts.

### 26.7 Concurrent Clients

Tokio provides:

    Concurrent Connections

    Bounded Channels

    Asynchronous Reads

    Asynchronous Writes

    Task Cancellation

MarketForge provides:

    Session Admission

    Authorization

    Request Limits

    Response Limits

    Operation Tracking

    Session Cleanup

### 26.8 Configuration Mutations

Mutating requests must pass through:

    Configuration Manager

    Runtime Reconciler

    Authoritative Component Owner

The Control Server must not directly mutate Stream Registry state without the applicable validated command contract.

### 26.9 Read-Only Queries

Monitoring queries should consume existing snapshots.

Examples:

    RuntimeSnapshot

    StreamSnapshot

    MetricsSnapshot

    HealthSnapshot

The Control Server must not reconstruct complete state by scanning event history for ordinary monitoring requests.

### 26.10 Why Not gRPC Initially

gRPC provides useful typed RPC and streaming facilities.

However, the initial MarketForge interface is:

    Local

    Unix-Socket Based

    Relatively Low Frequency

    Shared By Rust And Python

Length-prefixed JSON already satisfies the proposed requirements.

A gRPC framework would introduce additional code generation, schema management, and dependency complexity without a demonstrated initial need.

### 26.11 Why Not a REST Framework Initially

The management protocol does not require HTTP.

Adding an HTTP control server solely for Ratatui and Python would introduce another transport surface.

The optional Prometheus endpoint is read-only and remains independent.

A future HTTP management adapter may be introduced through the existing control contracts.

### 26.12 Exact Numerical Serialization

Control messages may contain:

    Cursor Offsets

    Nanosecond Timestamps

    Configuration Revisions

    Instrument Identifiers

    Exact Decimal Values

The wire schema must preserve large integers and exact decimal values.

Financial decimal values should be encoded without floating-point precision loss.

### 26.13 Control Protocol Testing

Required tests include:

- Partial frame reads.
- Partial frame writes.
- Oversized payload rejection.
- Invalid JSON.
- Protocol-version mismatch.
- Concurrent request correlation.
- Request timeout.
- Mid-response disconnect.
- Revision conflicts.
- Python interoperability.
- Authorization failures.
- Runtime isolation.

### 26.14 Control API Invariant

    Networking Libraries
    Provide Local IPC
    And Serialization.

    MarketForge Owns
    The Control Protocol,
    Authorization,
    And Runtime Semantics.

---

## 27. Ratatui and Terminal Interface Reuse

MarketForge already includes Ratatui/Crossterm infrastructure for historical processing, merging, and conversion.

The Live management interface must reuse compatible widgets, theme definitions, terminal lifecycle handling, and input patterns.

The interface architecture is defined in Document 14.

### 27.1 Primary Decision

Reuse:

    ratatui

    crossterm

    Existing MarketForge Theme

    Existing Compatible Widgets

Avoid introducing a second terminal UI framework.

### 27.2 Dependency Candidates

| Library | Decision | Responsibility |
|---|---|---|
| `ratatui` | Existing/preferred | Terminal layout and widgets |
| `crossterm` | Existing/preferred | Terminal input/output |
| Existing MarketForge UI modules | Reuse | Theme, widgets, terminal helpers |
| `tui-input` | Optional | Text-input state handling |
| `tui-textarea` | Optional | More advanced text editing |
| `unicode-width` | Conditional | Terminal text width calculations |
| `unicode-segmentation` | Conditional | Unicode-aware text editing |

Additional widget crates should be introduced only when existing components cannot satisfy the required interaction.

### 27.3 Process Separation

The intended architecture is:

    marketforge-live
           |
           v
    Runtime Control API
           |
           v
    marketforge-live-tui
           |
           v
         Ratatui

The TUI must not own or run the canonical market-data pipeline.

### 27.4 Existing Tokyo Night Theme

The existing Tokyo Night theme remains authoritative for visual styling.

Live-specific views should reuse its semantic colors and compatible widgets.

The implementation must not create a second independent theme system.

### 27.5 Shared UI Components

Reuse candidates include:

    Navigation

    Tables

    Selection State

    Status Indicators

    Dialogs

    Progress Displays

    Text Inputs

    Scrollable Panels

    Terminal Setup

    Terminal Cleanup

Existing widgets must be inspected before deciding whether adaptation is necessary.

### 27.6 Live-Specific Views

The Live TUI provides:

    Dashboard

    Streams

    Derived Streams

    Recording

    Settings

These screens consume Control API snapshots.

They do not directly access runtime-owned processing structures.

### 27.7 Event Loop

The UI should use one coherent event-driven application loop.

Conceptually:

    Crossterm Events
           |
           v
    Input Dispatcher
           |
           v
    Presentation State
           |
           v
    Ratatui Rendering

Asynchronous control responses update the presentation state through a separate bounded event path.

### 27.8 Application State

The TUI owns:

    Current View

    Selected Resource

    Focus

    Filters

    Sorting

    Dialog State

    Configuration Drafts

    Snapshot Cache

It does not own:

    Exchange Connections

    BookStore

    Recording Transactions

    Recovery Coordination

    Authoritative Stream Health

### 27.9 Table Performance

Tables may contain many registered streams.

The renderer should display only visible rows when appropriate.

Sorting and filtering should operate on bounded presentation data.

Stable StreamId and DerivedStreamId values must remain the basis of selection.

### 27.10 Form Libraries

Additional text-input libraries may simplify:

    Numeric Inputs

    Search

    Multi-Line Configuration Editing

    Cursor Navigation

However, Live's initial forms are relatively simple.

Reuse existing MarketForge input components before introducing a new widget dependency.

### 27.11 Terminal Restoration

The TUI must restore terminal state during normal exit and recoverable failures.

Relevant state includes:

    Raw Mode

    Alternate Screen

    Cursor Visibility

    Mouse Capture

Terminal cleanup should reuse existing MarketForge infrastructure.

### 27.12 Rendering Tests

Ratatui's test backend should be used where compatible.

Tests must verify:

    Layout

    Navigation

    Focus

    Tables

    Dialogs

    Status Presentation

    Narrow Terminals

    Missing Measurements

### 27.13 Historical TUI Compatibility

Changes to shared theme or widget infrastructure must preserve historical processing, merging, and conversion interfaces.

Live-specific features should extend existing UI infrastructure without unnecessarily modifying historical workflows.

### 27.14 Ratatui Invariant

    One Shared Terminal
    UI Infrastructure.

    Separate Historical
    And Live Applications.

    No Duplicate
    Presentation Framework.

---

## 28. Testing and Development Libraries

MarketForge already has a substantial historical regression suite.

The Live implementation should reuse compatible test infrastructure and add deterministic tools for asynchronous networking, failure injection, state-machine testing, and performance measurement.

The testing strategy is defined in Document 15.

### 28.1 Primary Decision

Prefer:

    Rust Built-In Test Framework

    Existing MarketForge Fixtures

    Tokio Test Utilities

    Deterministic Mock Transports

    Property-Based Testing

    Focused Benchmarks

External test libraries should solve specific verification needs.

### 28.2 Dependency Candidates

| Library | Decision | Intended Use |
|---|---|---|
| Rust `#[test]` | Required | Unit and contract tests |
| `tokio::test` | Required | Async tests |
| `proptest` | Recommended | Property-based testing |
| `criterion` | Recommended candidate | Repeatable microbenchmarks |
| `tempfile` | Recommended | Isolated temporary files/directories |
| `wiremock` | Optional | Mock HTTP endpoints |
| `mockito` | Alternative | Mock HTTP endpoints |
| `loom` | Optional | Selected concurrency-model tests |
| `testcontainers` | Conditional | PostgreSQL integration environments |
| Existing MarketForge test utilities | Reuse | Canonical, historical, and serialization fixtures |

Only one HTTP-mocking solution should be selected unless a concrete requirement justifies more.

### 28.3 Historical Regression Reuse

Existing tests remain authoritative for shared components.

Changes to:

    canonical/

    normalize/

    book/

    instrument/

    storage/parquet/

must trigger applicable historical regression suites.

### 28.4 Tokio Test Utilities

Tokio's testing facilities support asynchronous tests and controlled time.

These are useful for:

    Heartbeat Timeouts

    Reconnection Backoff

    Recording Flush Intervals

    Metrics Snapshot Timers

    Recovery Cancellation

Tests must not depend on arbitrary real-time sleeps when controlled synchronization can establish the expected behavior.

### 28.5 Property-Based Testing

`proptest` is a recommended candidate for generating operation sequences and validating invariants.

Examples:

    Stream Lifecycle Operations

    Connection Generation Changes

    Subscription Acknowledgements

    Trade Identity Sequences

    L2 Price-Level Updates

    Retention Cursor Intervals

    Derived Event Ordering

    Configuration Revisions

Failed generated cases should retain reproducible seeds or minimized examples.

### 28.6 Reference Models

Property-based tests may compare production behavior against deliberately simple reference models.

Examples:

    Simple Order Book

    Reference Cursor Interval Set

    Simplified Stream Lifecycle

    Deterministic Event Sort

The reference model must be independently specified.

It must not merely call the production implementation and compare its output to itself.

### 28.7 Temporary Files

`tempfile` is suitable for isolated:

    Configuration Files

    Retention Segments

    Manifest Files

    Unix Socket Directories

    Local Diagnostic Logs

Tests must not modify production data directories.

### 28.8 HTTP Mocking

A mock HTTP server is useful for:

    REST Snapshots

    Trade Recovery

    HTTP 429

    Retry-After

    Invalid JSON

    Timeout

    Authentication Rejection

Use a library compatible with the selected HTTP client and test runtime.

### 28.9 WebSocket Mocking

A local WebSocket test server should use compatible WebSocket protocol tooling.

Tests must control:

    Handshake

    Subscription Acknowledgements

    Native Messages

    Ping/Pong

    Disconnects

    Malformed Frames

    Delayed Responses

The mock must preserve the actual production transport boundary.

### 28.10 PostgreSQL Integration

PostgreSQL recording tests should use an isolated test database or schema.

Containerized PostgreSQL may simplify reproducible integration testing.

However, ordinary unit tests must not require PostgreSQL.

### 28.11 Criterion Benchmarks

`criterion` is a candidate for repeatable microbenchmarks.

Relevant benchmarks include:

    JSON Decoding

    Decimal Normalization

    Trade Processing

    BookStore Mutation

    Merge Ordering

    Currency Conversion

    Serialization

End-to-end throughput and soak testing require a separate harness with realistic asynchronous scheduling and resource measurements.

### 28.12 Concurrency Model Testing

`loom` may help validate selected shared-memory concurrency structures.

It should be considered for narrow correctness-critical primitives.

It is not a substitute for full Tokio runtime tests.

### 28.13 Fault Injection

MarketForge should maintain its own deterministic failure-injection interfaces.

External mocking libraries may supply network or storage failures.

The test harness must still control:

    Failure Timing

    Operation Identity

    Runtime Generation

    Expected Recovery

    Expected Integrity State

### 28.14 Test Dependency Isolation

Test-only crates belong in development dependencies where appropriate.

They must not unnecessarily increase the production runtime dependency graph.

### 28.15 Testing Invariant

    Testing Libraries
    Provide Deterministic
    Inputs And Verification
    Infrastructure.

    MarketForge Tests
    Establish Architectural
    Correctness Guarantees.

---

## 29. Observability and Management Dependency Summary

The recommended initial implementation should remain deliberately lightweight.

### 29.1 Recommended Stack

| Area | Preferred Approach |
|---|---|
| Internal metrics | Typed MarketForge metric structures |
| Metric synchronization | Standard atomics and Tokio primitives |
| Latency histograms | Fixed buckets initially; evaluate `hdrhistogram` |
| Health evaluation | MarketForge HealthEvaluator |
| Prometheus export | Optional `prometheus-client` adapter |
| Prometheus HTTP serving | Minimal HTTP implementation |
| Configuration | `serde` + `toml` |
| Configuration persistence | MarketForge Configuration Manager |
| JSON serialization | `serde_json` |
| Structured logging | `tracing` + `tracing-subscriber` |
| Local log files | Existing writer or `tracing-appender` |
| Control transport | Tokio Unix sockets |
| Control framing | Explicit length-prefixed JSON |
| Control protocol | MarketForge-owned typed messages |
| Terminal interface | Existing Ratatui/Crossterm infrastructure |
| Terminal theme | Existing Tokyo Night theme |
| Async tests | Tokio test utilities |
| Property-based tests | `proptest` |
| Benchmarks | `criterion` |
| Temporary files | `tempfile` |
| HTTP mocking | One selected mock-server library |
| PostgreSQL integration | Isolated PostgreSQL test environment |

### 29.2 Dependencies to Reuse

The following dependencies should be reused rather than replaced when existing versions satisfy the requirements:

    ratatui

    crossterm

    serde

    serde_json

    tokio

    Existing Logging Infrastructure

    Existing Test Utilities

The precise dependency inventory must be checked against the current repository.

This document does not authorize replacing working dependency versions without compatibility verification.

### 29.3 Recommended Additions

Subject to repository inspection:

    tracing

    tracing-subscriber

    toml

    proptest

    tempfile

These libraries provide useful functionality with relatively clear architectural boundaries.

However, a library already present in the repository does not need to be added again.

### 29.4 Optional Additions

Candidates requiring further evaluation:

    prometheus-client

    hdrhistogram

    tracing-appender

    tokio-util Codec Utilities

    tui-input

    tui-textarea

    criterion

    wiremock

    loom

Each optional dependency requires a documented use case.

### 29.5 Dependencies Not Initially Required

The initial implementation does not require:

    tonic

    jsonrpsee

    Full HTTP Management Framework

    Independent Metrics Recorder Framework

    Second TUI Framework

    External Logging Service

    External Monitoring Database

    Redis

    Kafka

    Dedicated Metrics Agent

These technologies may be useful in different architectures.

They do not solve a necessary initial MarketForge Live requirement.

### 29.6 Control and Data Plane Separation

External dependencies must preserve the established separation:

    MarketForge Live Runtime
                |
       +--------+---------+
       |                  |
       v                  v
    Data Plane        Control Plane
       |                  |
       v                  v
    Canonical Events  Runtime Control API
       |                  |
       v                  v
    Unix Publisher    Unix Control Socket
       |                  |
       v                  v
    Subscribers       Ratatui / Python

Monitoring is shared through read-only snapshots.

Optional Prometheus exposition remains separate:

    Metrics Registry
           |
           v
    Prometheus Adapter
           |
           v
       HTTP /metrics

The Prometheus adapter must not provide runtime mutation capabilities.

### 29.7 No Additional Service Dependencies

The ordinary Live runtime must operate without requiring:

    Prometheus Server

    Grafana

    Redis

    Kafka

    External Message Broker

    External Logging Collector

PostgreSQL is required only for the configured recording functionality.

The Python orchestration process is not required to remain active during normal Rust Live operation.

The Rust daemon uses the validated cached instrument catalog and configuration state.

### 29.8 Development Dependency Boundaries

Development tools should remain separate from runtime requirements.

Conceptually:

    Production Dependencies
             |
             v
    MarketForge Live Runtime

    Development Dependencies
             |
             v
    Tests / Benchmarks / Fixtures

Test frameworks, mock servers, and benchmark tools must not be initialized during production execution.

### 29.9 Version and Feature Discipline

The dependency graph must remain reproducible.

Before adopting a new library, verify:

- Compatibility with the project's Rust compiler.
- Compatibility with existing Tokio versions.
- Compatibility with existing Serde versions.
- Compatibility with existing Arrow and Parquet versions.
- Required Cargo features.
- Transitive dependency impact.
- Licensing.
- Maintenance status.
- Relevant security advisories.
- Existing historical regression results.

A crate appearing in this document is not automatically approval to adopt its latest release.

### 29.10 Final Observability and Management Architecture

    +--------------------------------------------------+
    |               MarketForge Live                   |
    +--------------------------------------------------+
                          |
              +-----------+-----------+
              |                       |
              v                       v
       Canonical Processing      Runtime Management
              |                       |
              v                       v
       Metrics Registry         Control API
              |                       |
       +------+--------+              |
       |               |              |
       v               v              v
    HealthEvaluator  Prometheus     Ratatui
       |             Adapter           |
       v               |               |
    Control API         v               |
       |            HTTP /metrics       |
       +---------------+---------------+
                       |
                       v
             Existing Rust Runtime

    Structured Diagnostics
              |
              v
       tracing Subscriber
              |
              v
        Local Log Files

Canonical processing, authoritative health, optional metrics exposition, and local diagnostics retain their independent responsibilities.

### 29.11 Final Part 4 Invariant

    Observability And
    Management Libraries
    Must Remain
    Infrastructure Adapters.

    They Must Not
    Replace MarketForge's
    Authoritative Runtime,
    Integrity,
    Or Configuration State.

---

## 30. Part 4 Dependency Decision Record

This section consolidates the decisions established in Sections 22–29.

### 30.1 Accepted Architectural Decisions

**Decision OBS-001 — Internal Metrics Authority**

MarketForge retains one authoritative Metrics Registry.

External instrumentation frameworks must not introduce competing metric state.

**Decision OBS-002 — Optional Prometheus Export**

Prometheus support is implemented as a read-only adapter.

The ordinary runtime does not require Prometheus or Grafana.

**Decision CFG-001 — TOML and Serde**

Human-editable configuration uses TOML.

Serde provides typed serialization.

Semantic validation remains MarketForge-owned.

**Decision LOG-001 — Structured Local Logging**

Operational diagnostics use structured local logging.

PostgreSQL does not store operational log history.

**Decision CTRL-001 — Unix Socket Management**

The management interface uses a dedicated Unix domain socket.

The initial protocol remains versioned, length-prefixed JSON.

**Decision CTRL-002 — No Additional RPC Framework**

A separate gRPC or JSON-RPC framework is not required initially.

**Decision TUI-001 — Ratatui Reuse**

The Live TUI reuses existing Ratatui/Crossterm infrastructure and the Tokyo Night theme.

**Decision TEST-001 — Historical Regression Preservation**

Shared MarketForge components retain historical regression coverage.

**Decision TEST-002 — Deterministic Async Testing**

Live uses controlled time, simulated transports, fault injection, and reproducible fixtures.

### 30.2 Decisions Requiring Implementation Evaluation

The following remain subject to code inspection or benchmarking:

| Decision | Candidate | Required Evidence |
|---|---|---|
| Latency histogram implementation | `hdrhistogram` or fixed buckets | Accuracy, memory, hot-path overhead |
| Prometheus encoder | `prometheus-client` | Compatibility with existing registry |
| Prometheus HTTP serving | Minimal Hyper-based server or existing adapter | Dependency cost and bounded serving |
| Log file writer | Existing infrastructure or `tracing-appender` | Backpressure and rotation behavior |
| TUI text input | Existing widgets or helper crate | Existing functionality and complexity |
| HTTP test mocks | `wiremock` or alternative | Determinism and reqwest compatibility |
| Concurrency-model testing | `loom` | Presence of suitable shared-state primitives |

### 30.3 Deferred Decisions

The following are deliberately deferred:

    Full HTTP Management API

    gRPC Control Plane

    Alternative TUI Framework

    External Metrics Infrastructure Management

    Distributed Logging System

    General-Purpose Plugin Architecture

    Specialized Lock-Free UI State

These features must not delay implementation of the initial Live runtime.

### 30.4 Acceptance Criteria

Part 4 is considered implemented when:

1. The existing logging infrastructure has been inspected.
2. Internal metrics remain independent of external exporters.
3. The HealthEvaluator consumes authoritative component state.
4. TOML configurations compile into validated typed structures.
5. Configuration revisions are persisted safely.
6. The Control API exchanges versioned messages over Unix sockets.
7. Rust and Python clients share protocol compatibility fixtures.
8. Ratatui reuses existing theme and widget infrastructure.
9. Operational logs remain local and bounded.
10. Tests verify monitoring and control-plane failure isolation.
11. Unnecessary frameworks are excluded from the production dependency graph.

### 30.5 Final Decision

    Reuse Existing
    MarketForge Infrastructure
    Wherever Compatible.

    Add Focused Libraries
    Only For Capabilities
    That Are Not Already
    Reliably Implemented.

    Preserve Clear Ownership
    Across Observability,
    Configuration,
    Control,
    And Presentation.

---

**## 31. External Reference Project Evaluation

External open-source projects may provide useful implementation patterns, architectural ideas, or reusable components.

However, MarketForge Live already has an established architecture and substantial historical processing infrastructure.

External project evaluation must therefore distinguish:

    Architectural Inspiration

    Algorithmic Reuse

    Selective Source Adaptation

    Direct Library Dependency

    Wholesale Framework Adoption

These are different decisions.

### 31.1 Primary Objectives

External project evaluation should:

1. Identify functionality relevant to MarketForge Live.
2. Locate reusable implementation patterns.
3. Determine compatibility with existing canonical models.
4. Evaluate ownership and lifecycle semantics.
5. Identify duplicated functionality.
6. Assess maintenance and licensing.
7. Estimate integration complexity.
8. Verify performance and correctness claims.
9. Determine whether selective adaptation is worthwhile.
10. Avoid unnecessary framework dependencies.

### 31.2 Initial Reference Projects

The initial reference set includes:

| Project | Primary Area | Initial Classification |
|---|---|---|
| Barter-Data | Exchange connectivity and streaming | High-priority reference |
| Barter-Integration | REST/WebSocket integration abstractions | High-priority reference |
| RustFinance / RustForge | Runtime composition and TUI architecture | Selective reference |
| Statui | Async monitoring and terminal dashboards | Selective reference |
| Existing MarketForge historical engine | Canonical domain and data-processing logic | Authoritative reuse source |

The external projects are not equally relevant.

The historical MarketForge implementation remains authoritative for shared financial semantics.

### 31.3 Evaluation Dimensions

Each candidate should be evaluated across:

    Relevant Functionality

    API Compatibility

    Runtime Dependencies

    Ownership Model

    Failure Handling

    Resource Boundedness

    Test Coverage

    Performance

    Maintenance

    License

    Integration Cost

    Long-Term Coupling

### 31.4 Evidence Requirements

An external project's README is useful for identifying features.

It is not sufficient evidence of:

    Production Reliability

    Absence Of Memory Leaks

    Correct Exchange Recovery

    Lossless Event Processing

    Historical Completeness

    Deterministic Ordering

    HFT Suitability

Such properties require source inspection, tests, benchmarks, or independent verification.

### 31.5 Source Reuse Levels

**Level 1 — Architectural Reference**

Study the design and implement the relevant ideas within MarketForge's existing contracts.

**Level 2 — Algorithmic Reference**

Study an algorithm or state machine, then implement or adapt it with independent tests.

**Level 3 — Selective Source Adaptation**

Adapt a clearly bounded component while preserving its licensing requirements.

**Level 4 — Direct Dependency**

Use an external crate through a stable, appropriately scoped interface.

**Level 5 — Framework Adoption**

Allow the external framework to own a substantial part of the runtime architecture.

The initial preference is Levels 1–3 for external trading frameworks.

Direct dependencies are appropriate for general-purpose infrastructure libraries.

### 31.6 Ownership Compatibility

A candidate component must not create competing authorities for:

    Source Stream Identity

    Connection Lifecycle

    Subscription State

    Trade Continuity

    Book Synchronization

    Recovery Coordination

    Recording Coverage

    Configuration Revisions

If a library owns one of these responsibilities, integration requires an explicit ownership decision.

Two independent supervisors must not control the same resource.

### 31.7 Reference Project Invariant

    External Code Reuse
    Must Reduce
    Implementation Complexity
    Without Creating
    Conflicting
    Architectural Ownership.

---

## 32. Barter-Data and Barter-Integration

The Barter ecosystem is the most directly relevant external reference for MarketForge Live's exchange connectivity.

Its public project structure separates market-data streaming, exchange integrations, instrument models, and trading-engine functionality.

MarketForge should concentrate on the market-data and lower-level integration components.

### 32.1 Reference Repositories

Primary repository:

    https://github.com/barter-rs/barter-rs

Relevant subprojects:

    barter-data

    barter-integration

    barter-instrument

Other Barter components may be inspected when they clarify lifecycle or error-handling patterns.

The trading and execution engines are not required for MarketForge Live.

### 32.2 Primary Reuse Areas

Evaluate:

    WebSocket Connection Establishment

    Exchange Connector Abstractions

    Subscription Request Construction

    Native Subscription Identification

    Stream Selection

    Message Routing

    Transport Error Handling

    Reconnection Architecture

    REST Integration Patterns

    Exchange-Specific Protocol Modules

### 32.3 Connection Management

Inspect how the project handles:

    Connection Creation

    Connection Ownership

    Reader / Writer Tasks

    Connection Termination

    Connection Replacement

    Error Propagation

    Resource Cleanup

The objective is to identify reliable patterns compatible with MarketForge's Connection Manager.

### 32.4 Reconnection Evaluation

Inspect:

    Reconnection Trigger

    Retry Backoff

    Subscription Restoration

    Failure Classification

    Cancellation

    Obsolete Task Cleanup

    Stale Message Handling

Do not assume that a library supporting reconnection also guarantees complete market-data recovery.

### 32.5 Subscription Handling

Study how the project represents:

    Exchange

    Instrument

    Channel

    Native Subscription Parameters

    Subscription Responses

    Message Routing

Compare these concepts against MarketForge's canonical source identity and Stream Registry.

### 32.6 Exchange Connector Reuse

The main reuse opportunity is a generic exchange connector pattern.

Conceptually:

    Exchange Profile
           |
           v
    Connection Requirements
           |
           v
    Native Subscription Plan
           |
           v
    WebSocket Connection
           |
           v
    Native Message Decoder
           |
           v
    MarketForge Normalization

The objective is to avoid implementing a completely different transport architecture for every exchange.

### 32.7 Connector Genericity

A successful connector abstraction should support:

    Binance

    Bybit

    OKX

    Bitget

    Gate.io

Across:

    Spot

    Linear

    Inverse

without duplicating general WebSocket lifecycle logic.

Exchange-specific protocol semantics must remain explicit.

### 32.8 Message Routing

Evaluate whether the project provides useful techniques for mapping native exchange messages to logical subscriptions.

Relevant questions include:

- How are subscription identifiers constructed?
- How are multiplexed messages routed?
- How are unsupported channels rejected?
- How are malformed messages classified?
- How are subscription acknowledgements correlated?
- How are connection generations represented?

The resulting MarketForge implementation must preserve the contracts from Documents 04–06.

### 32.9 Normalization Compatibility

Barter-Data includes its own normalized market-data representations.

MarketForge must not adopt those models as a second canonical financial schema.

If Barter-derived connector code is reused, the boundary should be:

    Native Exchange Payload
             |
             v
    Reused Transport/Decode Logic
             |
             v
    MarketForge Canonical Normalization

The MarketForge historical model remains authoritative.

### 32.10 Instrument Model Compatibility

Barter-Instrument may contain useful identification and classification ideas.

However, MarketForge already has an authoritative instrument metadata catalog.

Do not introduce another independent instrument registry.

Relevant ideas may be adapted into existing MarketForge metadata contracts.

### 32.11 Rate-Limit Compatibility

Inspect whether the project provides reusable exchange request-admission patterns.

MarketForge must retain support for:

    Weighted Requests

    Shared API Pools

    Exchange Response Headers

    Cooldowns

    Multiple Quota Windows

A generic limiter must not overwrite exchange-specific quota semantics.

### 32.12 REST Integration Patterns

Barter-Integration may provide useful approaches for:

    Request Construction

    REST Endpoint Abstractions

    WebSocket Integration

    Typed Error Handling

    Exchange-Specific Configuration

These should be compared with the selected `reqwest` and `tokio-tungstenite` adapters.

### 32.13 Integration Options

**Option A — Architectural Reference**

Study relevant source modules and implement equivalent patterns within MarketForge.

**Option B — Selective Adaptation**

Adapt focused source components with compatible licensing and independent regression tests.

**Option C — Direct Dependency**

Use Barter crates through an adapter after establishing that their ownership and APIs match MarketForge requirements.

**Option D — Wholesale Adoption**

Replace substantial MarketForge connectivity and canonical infrastructure.

Option D is not recommended for the initial implementation.

### 32.14 Preferred Approach

Initial recommendation:

    Architectural Reference
             +
    Selective Source Adaptation

A direct dependency remains possible if a narrowly scoped component proves useful and does not introduce competing ownership.

### 32.15 Required Verification

Before copying or adopting implementation details, inspect:

    Current Source Code

    Cargo Features

    Tokio Compatibility

    Error Types

    Reconnection Tests

    Subscription Tests

    Exchange Coverage

    License

    Maintenance History

The exact APIs must be validated against the selected source revision.

### 32.16 Barter Evaluation Invariant

    Borrow Reliable
    Connectivity Patterns.

    Preserve MarketForge's
    Source Identity,
    Canonical Models,
    And Recovery Guarantees.

---

## 33. RustFinance / RustForge

RustFinance is a broader Rust trading-terminal and infrastructure project.

Its documented architecture includes a daemon, terminal interface, event processing, deterministic clocks, and market-data connectivity.

MarketForge should evaluate it selectively for runtime composition and presentation patterns.

### 33.1 Reference Repository

    https://github.com/Ashutosh0x/rust-finance

### 33.2 Relevant Areas

Evaluate:

    Independent Daemon And TUI

    Runtime Event Routing

    Typed Event Envelopes

    Deterministic Clock Abstractions

    Monitoring Dashboards

    Async UI Communication

    Runtime Status Presentation

    Shutdown Patterns

### 33.3 Daemon/TUI Separation

RustFinance documents separate daemon and TUI components.

This is compatible with MarketForge's planned process separation.

MarketForge's required architecture remains:

    marketforge-live
            |
            v
    Unix Control API
            |
            v
    marketforge-live-tui

The external project's transport choices do not override MarketForge's Unix socket decision.

### 33.4 Event Envelope Patterns

Study how the project represents:

    Event Type

    Event Identity

    Timestamp

    Sequence

    Source Context

These patterns may provide useful implementation ideas.

However, MarketForge already has canonical event and envelope contracts.

No new canonical event abstraction should be introduced solely to imitate the external project.

### 33.5 Deterministic Clocks

The documented deterministic clock design is relevant to MarketForge testing.

Evaluate:

    Clock Injection

    Simulated Time

    Reproducible Execution

    Clock Separation

Potentially reusable ideas should support the testing requirements established in Document 15.

### 33.6 Runtime Status Presentation

Relevant UI ideas include:

    Connection Status

    Health Indicators

    Throughput

    Latency

    Recent Errors

    Component State

These may inform the presentation layer.

They must not change the underlying MarketForge health semantics.

### 33.7 Excluded Functionality

MarketForge Live does not require the project's:

    Trading Strategy Engine

    Order Management System

    Order Execution

    AI Analysis

    Portfolio Risk Management

    Trading Agent Infrastructure

    Prediction Market Integrations

These capabilities belong outside the MarketForge Live data-acquisition layer.

### 33.8 Performance Claims

Published performance claims must be evaluated against their workload, benchmark environment, and actual implementation.

Do not use external benchmark numbers as MarketForge acceptance thresholds without reproducing the measurements.

### 33.9 Preferred Approach

Initial recommendation:

    Architectural Reference Only

Selective adaptation may be justified for a small, well-isolated utility.

Wholesale framework adoption is not recommended.

### 33.10 RustFinance Evaluation Invariant

    Reuse Relevant
    Runtime And
    Presentation Ideas.

    Do Not Import
    An Unrelated
    Trading Platform
    Into MarketForge Live.

---

## 34. Statui

Statui is an asynchronous Rust terminal application for monitoring HTTP endpoint health.

Its primary relevance to MarketForge is the separation between background monitoring and terminal rendering.

### 34.1 Reference Repository

    https://github.com/Mohamed-Badry/statui

### 34.2 Relevant Architecture

The documented design follows:

    Tokio Monitoring Tasks
              |
              v
       Communication Channel
              |
              v
       Ratatui Frontend
              |
              v
       Terminal Rendering

This architecture is compatible with MarketForge's independent Ratatui management interface.

### 34.3 Useful Patterns

Evaluate:

    Nonblocking UI Updates

    Background Data Collection

    Channel-Based State Updates

    Snapshot Presentation

    Latency Sparklines

    Status Tables

    Detail Inspector Panels

    Terminal Rendering

### 34.4 Monitoring Ownership

Statui monitors HTTP endpoints.

MarketForge monitors market-data streams and runtime components.

These are different domains.

MarketForge must not replace its HealthEvaluator with generic HTTP availability checks.

### 34.5 UI Communication

The most useful architectural principle is:

    Background I/O
        Must Not Block
    Terminal Rendering

MarketForge already implements this through the separate daemon and Control API client.

Statui may still provide useful implementation examples for the client-side event loop.

### 34.6 Reusing Widgets

Potentially useful presentation ideas include:

    Latency History

    Status Indicators

    Endpoint/Stream Detail Panels

    Bounded Recent Error Lists

However, MarketForge should first inspect its existing Ratatui widgets and Tokyo Night theme.

### 34.7 Scope Differences

Statui is not a market-data integrity engine.

It does not replace:

    Stream Registry

    Connection Manager

    Subscription Manager

    BookStore

    Recovery Coordinator

    Metrics Registry

    HealthEvaluator

### 34.8 Preferred Approach

Initial recommendation:

    UI Architectural Inspiration

Direct dependency adoption is unnecessary for the initial implementation.

### 34.9 Statui Evaluation Invariant

    Borrow Lightweight
    Asynchronous UI
    Design Patterns.

    Preserve Existing
    Ratatui Infrastructure
    And Runtime Ownership.

---
## 35. External Code Adoption and Licensing

External source code must be evaluated before being copied, modified, or incorporated into MarketForge.

A public repository is not automatically unrestricted code.

### 35.1 Adoption Requirements

Before incorporating external code:

1. Identify the exact upstream repository.
2. Identify the source revision.
3. Review the applicable license.
4. Confirm compatibility with MarketForge distribution.
5. Identify required copyright notices.
6. Identify attribution obligations.
7. Identify relevant dependencies.
8. Inspect the implementation.
9. Create compatibility tests.
10. Document local modifications.

### 35.2 MIT-Licensed Projects

The reviewed Barter, RustFinance, and Statui repositories identify MIT licensing.

MIT licensing generally permits commercial use, modification, and redistribution subject to preserving applicable copyright and permission notices.

MarketForge must retain required notices when incorporating licensed source code.

A repository-level license must not be assumed to cover every bundled third-party file without inspection.

### 35.3 Architectural Ideas vs Source Code

Reimplementing a general architectural pattern is different from copying a project's implementation.

For example:

    One Reader Task
          +
    One Writer Task
          +
    Connection Supervisor

is an architectural pattern.

Copying a particular source module requires reviewing the original code's license and attribution requirements.

### 35.4 Source Adaptation Record

For adapted external code, record:

    Upstream Repository

    Upstream Revision

    Original Source Location

    License

    Local Destination

    Local Modifications

    Test Coverage

    Review Date

This record should accompany the implementation.

### 35.5 Direct Dependency Record

For direct dependencies, record:

    Crate Name

    Selected Version

    Enabled Features

    Dependency Purpose

    Owning MarketForge Module

    License

    Upgrade Constraints

### 35.6 Avoid Unnecessary Forks

Forking an external library creates additional maintenance responsibilities.

A fork may be justified when:

- A required defect cannot be fixed through configuration.
- Necessary functionality is unavailable upstream.
- The change must be maintained independently.
- The maintenance burden is acceptable.

Prefer upstream contributions or ordinary adapter code when practical.

### 35.7 Source Pinning

Any external Git dependency must be pinned to a reproducible revision.

Production builds must not depend on an unpinned moving branch.

### 35.8 Dependency Governance Invariant

    Every External
    Dependency Or
    Source Adaptation
    Must Have
    Explicit Ownership,
    Provenance,
    And Compatibility Evidence.

---

## 36. Dependency Security and Maintenance

MarketForge Live is expected to remain operational for extended periods.

Its dependency graph must therefore be reviewed for security, compatibility, and maintenance risk.

### 36.1 Dependency Inventory

Maintain a complete inventory of direct dependencies.

Each dependency should have:

    Name

    Version

    Features

    Purpose

    License

    Owning Module

    Update Policy

### 36.2 Cargo Lockfile

Application builds should use a committed Cargo.lock.

Release builds should resolve the intended dependency versions reproducibly.

Relevant commands include:

```bash
cargo build --locked --release
```

and:

```bash
cargo test --locked
```

### 36.3 Security Auditing

Evaluate:

    cargo-audit

    cargo-deny

for dependency vulnerability, licensing, and policy checks.

These tools are development infrastructure rather than runtime components.

### 36.4 Duplicate Dependency Versions

Review the dependency graph for multiple versions of:

    Tokio

    Rustls

    Hyper

    Serde

    Arrow

    Parquet

Some duplication may be unavoidable.

Unnecessary duplication should be eliminated where possible.

### 36.5 Native Dependencies

Prefer established pure-Rust implementations where they satisfy requirements.

Native dependencies are acceptable when justified, but their implications must be understood.

Relevant considerations include:

    Deployment

    ABI Compatibility

    Cross-Compilation

    System Libraries

    Security Updates

    Packaging

### 36.6 Rust Compiler Compatibility

New dependencies must support the project's selected Rust compiler.

The Minimum Supported Rust Version should be treated as an explicit project constraint.

Dependency upgrades must not unexpectedly force a toolchain change.

### 36.7 Maintenance Evaluation

Assess:

    Release History

    Issue Activity

    Maintainer Responsiveness

    Test Coverage

    Documentation

    Breaking Changes

    Security Advisories

A stable, infrequently updated library is not automatically unsuitable.

Maintenance assessment must consider the library's scope and maturity.

### 36.8 Upgrade Strategy

Dependency upgrades should be categorized as:

    Security Fix

    Patch Update

    Minor Update

    Major Update

    Runtime Compatibility Update

Major changes to Tokio, WebSocket, PostgreSQL, Arrow, Parquet, or Ratatui dependencies require relevant integration and regression tests.

### 36.9 Arrow/Parquet Compatibility

Arrow and Parquet dependencies deserve particular care because MarketForge's existing historical engine already depends on their schemas and APIs.

An upgrade must preserve:

    Historical Serialization

    Decimal Encoding

    RecordBatch Compatibility

    Parquet Reading

    Parquet Writing

    Existing Regression Tests

### 36.10 Dependency Failure Isolation

Optional integrations must not become mandatory merely because their crates are included.

For example:

    Prometheus Exporter Failure
             !=
    Canonical Processing Failure

    Ratatui Failure
             !=
    Live Daemon Failure

    Logging Writer Failure
             !=
    Exchange Integrity Failure

Each subsystem retains its defined failure semantics.

### 36.11 Dependency Security Invariant

    Dependency Updates
    Must Preserve
    Runtime Compatibility,
    Security Requirements,
    And Historical
    Processing Correctness.

---

## 37. Final Dependency Architecture

This section consolidates the recommended dependency architecture for MarketForge Live.

The selected implementation should remain as small as practical while satisfying the established functional and operational requirements.

### 37.1 Core Runtime

| Library | Decision | Purpose |
|---|---|---|
| `tokio` | Required | Async execution |
| `tokio-util` | Recommended | Cancellation and async utilities |
| `futures-util` | Recommended | Stream/Sink utilities |
| `serde` | Required | Typed serialization |
| `serde_json` | Required for selected protocol | JSON decoding and IPC |
| `toml` | Recommended | Configuration parsing |
| `thiserror` | Recommended candidate | Typed error definitions |

### 37.2 Networking

| Library | Decision | Purpose |
|---|---|---|
| `tokio-tungstenite` | Recommended | WebSocket transport |
| `reqwest` | Recommended | REST transport |
| `rustls` | Preferred backend | TLS |
| `http` | Conditional | HTTP types |
| `url` | Conditional | URL validation |
| `bytes` | Recommended candidate | Efficient frame buffers |

### 37.3 Retry and Rate Limiting

| Library | Decision | Purpose |
|---|---|---|
| `backon` | Evaluate | Backoff primitives |
| `governor` | Evaluate | Generic rate-limit admission |
| Tokio timers | Required | Deadlines |
| Tokio semaphores | Required | Concurrency limits |
| Existing RNG utilities | Reuse if compatible | Retry jitter |

MarketForge retains authoritative retry classification and exchange quota semantics.

### 37.4 Data Processing

| Component | Decision |
|---|---|
| Canonical models | Reuse existing MarketForge implementation |
| Exact Decimal | Reuse existing MarketForge implementation |
| Instrument metadata | Reuse existing catalog contracts |
| Trade normalization | Reuse existing normalization primitives |
| L2 BookStore | Reuse existing BookStore |
| Depth grouping | Reuse existing historical contracts |
| Merge ordering | Reuse compatible historical semantics |
| Currency conversion | Reuse compatible historical arithmetic |
| Live watermarks | Implement MarketForge-specific functionality |
| Live reference state | Implement MarketForge-specific functionality |

### 37.5 Storage

| Library/Component | Decision | Purpose |
|---|---|---|
| Existing Arrow crates | Reuse | Columnar representation |
| Existing Parquet crates | Reuse | Segment storage |
| Existing PostgreSQL driver | Prefer reuse | Database access |
| `sqlx` | Candidate if needed | Async PostgreSQL |
| `tokio-postgres` | Alternative | Async PostgreSQL |
| `tempfile` | Development dependency | Isolated tests |

Do not introduce competing PostgreSQL clients without a demonstrated requirement.

### 37.6 Observability

| Library/Component | Decision |
|---|---|
| Typed Metrics Registry | MarketForge-owned |
| HealthEvaluator | MarketForge-owned |
| `hdrhistogram` | Optional |
| `tracing` | Recommended |
| `tracing-subscriber` | Recommended |
| `tracing-appender` | Recommended candidate |
| `prometheus-client` | Optional exporter candidate |

### 37.7 Management

| Library/Component | Decision |
|---|---|
| Tokio Unix sockets | Required |
| Serde JSON | Required |
| Control protocol | MarketForge-owned |
| Ratatui | Reuse existing |
| Crossterm | Reuse existing |
| Tokyo Night theme | Reuse existing |
| Additional TUI widgets | Add only where needed |

### 37.8 Testing

| Library | Decision |
|---|---|
| Existing Rust tests | Reuse |
| Tokio test utilities | Required |
| `proptest` | Recommended |
| `criterion` | Recommended candidate |
| `tempfile` | Recommended |
| HTTP mocking library | Select one |
| `loom` | Optional |
| PostgreSQL test environment | Required for recording integration |

### 37.9 External References

| Project | Recommended Use |
|---|---|
| Barter-Data | Connectivity and subscription architecture |
| Barter-Integration | REST/WebSocket integration patterns |
| Barter-Instrument | Selective instrument-model ideas |
| RustFinance | Daemon composition, clocks, and TUI ideas |
| Statui | Async monitoring and presentation patterns |

These projects are reference sources rather than automatically adopted dependencies.

### 37.10 Explicitly Excluded Infrastructure

The initial MarketForge Live architecture does not require:

    Kafka

    Redis

    RabbitMQ

    Shared-Memory Transport

    gRPC Control Plane

    Distributed Metrics Storage

    Managed Prometheus

    Managed Grafana

    External Logging Service

    Second Canonical Data Model

    Second BookStore

    Second Instrument Catalog

This list does not prohibit future extensions.

It establishes the boundaries of the initial implementation.

### 37.11 Dependency Architecture Diagram

    +-------------------------------------------------------+
    |                   MarketForge                         |
    +-------------------------------------------------------+
                               |
                 +-------------+-------------+
                 |                           |
                 v                           v
          Historical Engine              Live Engine
                 |                           |
                 +-------------+-------------+
                               |
                               v
                       Shared Domain Core
                               |
                  +------------+------------+
                  |            |            |
                  v            v            v
             Canonical     Normalization  BookStore
                  |            |            |
                  +------------+------------+
                               |
                               v
                     Shared Data Semantics
                               ^
                               |
    +--------------------------+---------------------------+
    |                                                      |
    v                                                      v
    Live Acquisition                                  Derived Engine
    |                                                      |
    +-- Tokio                                              +-- Shared Ordering
    +-- WebSocket Transport                                +-- Shared Conversion
    +-- REST Client                                        +-- Live Watermarks
    +-- TLS                                                +-- Live Reference State
    |
    v
    Canonical Event Router
    |
    +---------------------+--------------------+
    |                     |                    |
    v                     v                    v
    Unix Publisher    Live Retention       Recording
    |                     |                    |
    +-- Tokio Sockets      +-- Arrow            +-- PostgreSQL
                           +-- Parquet          +-- Async DB Client

    Runtime Management
    |
    +-- Configuration Manager
    +-- Control API
    +-- Metrics Registry
    +-- HealthEvaluator
    +-- Structured Logging
    +-- Optional Prometheus Adapter
    |
    v
    Independent Ratatui TUI

The diagram represents architectural ownership.

It does not imply that every named component requires a separate Cargo crate.

---

## 38. Final Acceptance Criteria

Document 16 is considered implemented when the following dependency and reuse decisions have been verified against the actual MarketForge repository.

### 38.1 Existing Code Audit

Complete an inventory of:

    Canonical Models

    Instrument Metadata

    Trade Normalization

    BookStore

    Depth Grouping

    Merge Engine

    Currency Conversion

    Arrow/Parquet Infrastructure

    Existing Ratatui Widgets

    Existing Logging

    Existing Tests

Each component must receive an explicit reuse classification.

### 38.2 Dependency Inventory

Document the actual direct dependencies and selected Cargo features.

Identify:

    Already Installed

    Required Addition

    Optional Addition

    Rejected

    Pending Evaluation

Do not assume a crate is absent merely because it is not mentioned in an architecture document.

### 38.3 Compatibility Verification

Verify:

- Rust compiler compatibility.
- Tokio compatibility.
- TLS compatibility.
- Serde compatibility.
- Arrow/Parquet compatibility.
- Ratatui compatibility.
- PostgreSQL driver compatibility.
- Historical regression compatibility.

### 38.4 External Project Evaluation

Review the relevant implementation areas from:

    Barter-Data

    Barter-Integration

    RustFinance

    Statui

Record concrete findings and source references before adapting their implementations.

### 38.5 No Duplicate Authorities

Confirm there is only one authoritative implementation for:

    Canonical Event Semantics

    Instrument Metadata

    BookStore

    Source Stream Registry

    Connection Lifecycle

    Subscription Lifecycle

    Recovery Coordination

    Configuration Revisions

    Metrics Registry

    Recording Coverage

### 38.6 Performance Validation

Benchmark the selected implementation where a dependency materially affects the hot path.

Relevant areas include:

    WebSocket Frame Processing

    JSON Decoding

    Canonical Normalization

    BookStore Mutation

    Event Distribution

    Merge Ordering

    Currency Conversion

    Parquet Serialization

    PostgreSQL Recording

### 38.7 Test Preservation

All applicable historical regression tests must remain valid.

New Live tests must cover the selected adapters and their failure behavior.

### 38.8 Dependency Security

Verify:

    License Compatibility

    Dependency Vulnerabilities

    Reproducible Versions

    Enabled Cargo Features

    Transitive Dependency Footprint

    Known Maintenance Risks

### 38.9 Release Independence

Confirm that the Rust Live daemon operates without requiring:

    Active Python Orchestration

    Ratatui

    Prometheus

    Grafana

    External Message Brokers

PostgreSQL availability requirements remain conditional on the effective recording policy.

### 38.10 Final Acceptance Invariant

    External Dependencies
    Must Support
    MarketForge's Architecture.

    They Must Not
    Redefine Its
    Canonical Semantics
    Or Operational
    Guarantees.

---

## 39. Final Reuse Strategy

MarketForge Live should not be implemented as an entirely new market-data framework.

It should be implemented as an extension of the existing MarketForge engine, combining reusable historical domain logic with focused asynchronous infrastructure.

The final approach is:

    Existing MarketForge Historical Engine
                      |
                      v
              Shared Domain Logic
                      |
            +---------+---------+
            |                   |
            v                   v
    Historical Execution    Live Execution
                                |
                                v
                       Established Rust
                     Infrastructure Crates
                                |
                                v
                       MarketForge-Specific
                       Lifecycle And Recovery
                                |
                                v
                       Event Distribution,
                       Retention, Recording,
                       Derived Processing,
                       Monitoring And Control

### 39.1 Reuse First

Before implementing a new domain component:

    Inspect Existing MarketForge Code.

    Determine Whether
    Compatible Logic
    Already Exists.

### 39.2 Infrastructure Second

Before implementing general-purpose infrastructure:

    Evaluate Established
    Rust Libraries.

    Reuse Reliable
    Transport, Storage,
    And Runtime Primitives.

### 39.3 Specialized Logic Third

Implement MarketForge-specific logic where required:

    Exchange Integrity

    Native Sequence Semantics

    Connection Ownership

    Recovery Coordination

    Recording Coverage

    Retention Guarantees

    Derived Watermarks

    Runtime Reconciliation

### 39.4 Benchmark Before Optimizing

Avoid introducing specialized concurrency structures, alternative allocators, or custom network protocols without evidence of a meaningful benefit.

### 39.5 Preserve Historical Compatibility

Changes to shared financial processing must retain the historical engine's established guarantees.

### 39.6 Final Dependency Principle

    Reuse What Is Correct.

    Adopt What Is Mature.

    Implement What Is Unique.

    Test Every Boundary.

### 39.7 Completion

The resulting dependency architecture minimizes duplicated business logic and unnecessary runtime infrastructure while preserving the flexibility required for MarketForge's multi-exchange Live service.

