# 15 — Testing and Validation

## 1. Purpose

The Testing and Validation subsystem defines the verification strategy for MarketForge Live.

It establishes the testing infrastructure, test categories, fixtures, deterministic execution methods, fault-injection mechanisms, performance benchmarks, and acceptance gates required before the Live runtime is considered operationally reliable.

Testing must cover the complete architecture established in Documents 01–14.

The objective is not merely to demonstrate that the service can connect to exchanges and receive market data.

The objective is to establish confidence that MarketForge Live:

- Produces correct canonical events.
- Preserves historical/live semantic compatibility.
- Detects and reports integrity failures.
- Applies recovery policies correctly.
- Maintains valid L2 book generations.
- Preserves required event ordering.
- Prevents accidental duplicate acquisitions.
- Enforces bounded resource usage.
- Provides reliable event distribution.
- Tracks recording and retention guarantees correctly.
- Processes derived streams deterministically.
- Handles configuration changes safely.
- Remains observable during failures.
- Recovers from crashes and interrupted operations.
- Maintains acceptable performance under sustained workloads.

### 1.1 Primary Objectives

The testing architecture must support:

1. Reuse of existing historical regression tests.
2. Deterministic unit tests.
3. Exchange protocol fixture tests.
4. State-machine and property-based testing.
5. Simulated WebSocket and REST transports.
6. Controlled time and asynchronous scheduling.
7. Fault injection at subsystem boundaries.
8. Multi-component integration tests.
9. Full simulated-runtime tests.
10. PostgreSQL recording tests.
11. Parquet retention and replay tests.
12. Derived-stream correctness tests.
13. Control API and TUI tests.
14. Crash and restart validation.
15. Performance and resource benchmarks.

### 1.2 Testing Principles

MarketForge Live follows these principles:

- Test correctness independently of external exchange availability.
- Prefer deterministic fixtures over uncontrolled live responses.
- Preserve existing historical tests.
- Reuse canonical test utilities where possible.
- Test component contracts through their public interfaces.
- Model asynchronous failure and cancellation explicitly.
- Verify observable guarantees rather than implementation details alone.
- Distinguish current validity from historical completeness.
- Verify that monitoring does not influence processing correctness.
- Use bounded resources in tests.
- Make failures reproducible.
- Preserve regression fixtures for previously discovered bugs.

### 1.3 Testing Is Part of the Architecture

Every subsystem must define:

    Inputs

    Outputs

    Authoritative State

    Invariants

    Failure Conditions

    Recovery Conditions

    Resource Limits

    Observable Results

Its tests must verify these properties.

Testing must not be postponed until the complete Live runtime is implemented.

### 1.4 Verification vs Demonstration

A successful demonstration may show:

    Connect To Binance
           |
           v
    Receive BTCUSDT Trades
           |
           v
    Publish Events

This does not establish:

- Complete trade coverage.
- Correct duplicate handling.
- Reconnection safety.
- Correct canonical quantities.
- Valid L2 synchronization.
- Subscriber isolation.
- Recording durability.
- Correct behavior during overload.

A production-readiness test suite must establish the relevant guarantees individually.

### 1.5 Non-Responsibilities

The testing infrastructure must not:

- Introduce alternative canonical event definitions.
- Duplicate production integrity algorithms.
- Require Python in the Rust market-data hot path.
- Modify production recovery behavior solely to satisfy tests.
- Introduce test-only acceptance rules.
- Assume that local mock success proves exchange compatibility.
- Treat successful PostgreSQL queries as proof of complete recorded coverage.
- Treat successful snapshot application as proof of historical L2 completeness.
- Treat metrics as authoritative validation results.

### 1.6 Fundamental Testing Invariant

    Tests Must Verify
    Production Contracts.

    Production Contracts
    Must Not Be Rewritten
    Merely To Make
    Tests Pass.

---

## 2. Testing Architecture

MarketForge Live uses multiple complementary testing layers.

Each layer verifies a different class of requirements.

### 2.1 Testing Layers

The initial testing architecture contains:

    Unit Tests

    Contract Tests

    Property-Based Tests

    Component Integration Tests

    Simulated Runtime Tests

    Storage Integration Tests

    External Exchange Smoke Tests

    Performance Benchmarks

    Long-Running Stability Tests

No single layer replaces the others.

### 2.2 Verification Hierarchy

Conceptually:

    +--------------------------------------------------+
    |           Long-Running Stability Tests           |
    +--------------------------------------------------+
    |           External Exchange Smoke Tests          |
    +--------------------------------------------------+
    |           Full Simulated Runtime Tests           |
    +--------------------------------------------------+
    |           Component Integration Tests            |
    +--------------------------------------------------+
    |           Contract / Property-Based Tests        |
    +--------------------------------------------------+
    |           Deterministic Unit Tests               |
    +--------------------------------------------------+
    |           Historical Regression Tests            |
    +--------------------------------------------------+

This diagram represents verification scope, not a strict execution order.

Historical regression tests apply whenever shared historical/live components change.

### 2.3 Unit Tests

Unit tests verify narrowly scoped behavior.

Examples:

    Stream Identity Equality

    Counter Reset Handling

    Sequence Comparison

    Configuration Validation

    Backoff Calculation

    Cursor Ordering

    Decimal Normalization

    Conversion Arithmetic

Unit tests should execute quickly and require minimal external resources.

### 2.4 Contract Tests

Contract tests verify that implementations satisfy shared interface guarantees.

Examples:

    WebSocketTransport

    RestTransport

    RecordingSink

    RetentionReader

    MetricsReader

    ControlClient

Production and test implementations must satisfy the same semantic contracts.

### 2.5 Property-Based Tests

Property-based tests generate varied inputs and operation sequences to discover invalid states.

Examples:

    Random Stream Lifecycle Operations

    Random Sequence Gaps

    Random Reconnection Ordering

    Random Configuration Revisions

    Random Retention Cursor Intervals

    Random Merge Event Arrivals

Property-based tests should use reproducible seeds.

### 2.6 Component Integration Tests

Integration tests verify interactions between related components.

Examples:

    Connection Manager
        +
    Subscription Manager

    Stream Processor
        +
    BookStore

    Recording Manager
        +
    PostgreSQL

    Retention Manager
        +
    Parquet Storage

    Merge Engine
        +
    Currency Conversion

### 2.7 Simulated Runtime Tests

Simulated runtime tests exercise the complete Rust Live architecture using deterministic external inputs.

Conceptually:

    Fake Exchanges
          |
          v
    Real Connection Manager
          |
          v
    Real Protocol Adapters
          |
          v
    Real Normalization
          |
          v
    Real Integrity Processing
          |
          v
    Real Event Router
          |
     +----+------------+-------------+
     |                 |             |
     v                 v             v
    Publisher        Recording     Retention
     |                 |             |
     v                 v             v
    Test Clients     Test DB      Test Storage

Tests may replace infrastructure adapters while preserving production processing logic.

### 2.8 External Exchange Smoke Tests

Exchange smoke tests validate compatibility with real exchange endpoints.

They should verify:

- Connectivity.
- Subscription acceptance.
- Native message parsing.
- Required normalization fields.
- Quantity semantics.
- Snapshot acquisition.
- Sequence metadata.
- Heartbeat behavior.
- Rate-limit responses.

These tests depend on external systems and must not be required for ordinary deterministic test execution.

### 2.9 Performance Benchmarks

Performance tests measure:

    Throughput

    Latency

    CPU Usage

    Memory Usage

    Allocation Rate

    Queue Pressure

    Recovery Duration

    Recording Throughput

    Merge Throughput

Benchmarks must use repeatable workloads and clearly documented environments.

### 2.10 Testing Layer Invariant

    Each Testing Layer
    Establishes Different
    Evidence.

    Passing One Layer
    Does Not Imply
    Passing All Others.

---

## 3. Shared Test Infrastructure

MarketForge Live should reuse existing MarketForge test utilities wherever their contracts remain applicable.

New Live-specific test infrastructure must be generic and reusable across exchanges and subsystems.

### 3.1 Proposed Test Organization

Conceptually:

    engine/
    |
    +-- src/
    |    |
    |    +-- canonical/
    |    +-- normalize/
    |    +-- book/
    |    +-- historical/
    |    +-- live/
    |
    +-- tests/
    |    |
    |    +-- fixtures/
    |    |    |
    |    |    +-- canonical/
    |    |    +-- historical/
    |    |    +-- exchanges/
    |    |    +-- live/
    |    |
    |    +-- support/
    |    |    |
    |    |    +-- fake_clock.rs
    |    |    +-- fake_websocket.rs
    |    |    +-- fake_rest.rs
    |    |    +-- fake_storage.rs
    |    |    +-- event_collector.rs
    |    |    +-- failure_injector.rs
    |    |    +-- test_runtime.rs
    |    |
    |    +-- live/
    |         |
    |         +-- configuration.rs
    |         +-- connectivity.rs
    |         +-- subscriptions.rs
    |         +-- normalization.rs
    |         +-- trade_integrity.rs
    |         +-- book_synchronization.rs
    |         +-- recovery.rs
    |         +-- publication.rs
    |         +-- retention.rs
    |         +-- recording.rs
    |         +-- derived.rs
    |         +-- control.rs
    |         +-- metrics.rs
    |
    +-- benches/

The structure is illustrative.

Existing project test organization takes precedence where compatible.

### 3.2 Test Support Components

Recommended shared components:

| Component | Responsibility |
|---|---|
| FakeClock | Deterministic wall-clock and monotonic time |
| FakeWebSocket | Controlled WebSocket behavior |
| FakeRestClient | Controlled REST responses and failures |
| FakeExchange | Exchange protocol simulation |
| FakeConfigStore | In-memory configuration persistence |
| FakeInstrumentCatalog | Revisioned instrument metadata |
| EventCollector | Capture emitted canonical and derived events |
| FakePublisherClient | Simulate subscriber behavior |
| FakeRecordingSink | Simulate recording acknowledgements and failures |
| FakeRetentionStore | Simulate retained event storage |
| FailureInjector | Inject controlled subsystem failures |
| TestRuntimeBuilder | Assemble reproducible runtime configurations |

These components should be shared by multiple test suites.

### 3.3 TestRuntimeBuilder

The testing framework should support construction of a realistic Live runtime with substitute infrastructure.

Conceptually:

```rust
let runtime = TestRuntimeBuilder::new()
    .with_clock(fake_clock)
    .with_websocket(fake_websocket)
    .with_rest(fake_rest)
    .with_instrument_catalog(test_catalog)
    .with_recording_sink(test_recording)
    .with_retention_store(test_retention)
    .build()?;
```

The API is illustrative.

The builder must assemble actual production processing components where possible.

### 3.4 Dependency Injection

Replaceable infrastructure boundaries include:

    Network Transport

    Time Source

    Randomness Source

    Persistent Storage

    External Metadata Input

    Filesystem Operations

These boundaries should be injectable without changing canonical processing logic.

### 3.5 Avoid Duplicate Production Logic

A test fake may control transport behavior.

It must not implement a competing trade normalizer merely to make expected output easier to generate.

Similarly, an L2 test oracle must not simply call the same BookStore algorithm under test and interpret matching results as independent correctness evidence.

Expected behavior should come from independently specified fixtures, invariants, or a deliberately simple reference model.

### 3.6 Test Event Collector

The EventCollector captures events emitted by production processing components.

It should preserve:

    StreamId

    Cursor

    CanonicalPayload

    Provenance

    EventTimestamp

    PublicationOrder

    GenerationMetadata

The collector must not silently reorder events.

### 3.7 Test Assertions

Reusable assertions may include:

    AssertCanonicalEquivalent

    AssertCursorContinuous

    AssertSourceOrderPreserved

    AssertBookSynchronized

    AssertBookInvalid

    AssertCoverageGapReported

    AssertNoDuplicateSubscription

    AssertRecordingCommitFrontier

    AssertDerivedOrder

The assertions must preserve the distinctions defined in the domain contracts.

### 3.8 Test Isolation

Each test should own its required:

    Runtime State

    Temporary Files

    Socket Paths

    Database Schema Or Namespace

    Fake Clock

    Random Seed

Tests must not depend on the execution order of unrelated tests.

### 3.9 Temporary Resources

Tests creating Unix sockets, Parquet files, or configuration snapshots should use isolated temporary directories.

Cleanup must occur on normal completion and failure where practical.

A failed test must not leave resources that alter the outcome of later tests.

### 3.10 Shared Infrastructure Invariant

    Test Infrastructure
    Must Be Reusable,
    Deterministic,
    And Independent
    Of Test Execution Order.

---

## 4. Deterministic Time and Asynchronous Execution

MarketForge Live is an asynchronous Rust system.

Testing must control time-dependent behavior without relying on arbitrary real-world delays.

### 4.1 Time Domains

The runtime uses multiple distinct time domains:

    Exchange Event Time

    Local Wall-Clock Time

    Local Monotonic Time

    Runtime Scheduler Time

Tests must preserve these distinctions.

### 4.2 FakeClock

The test clock should support:

    CurrentWallClockTime

    CurrentMonotonicTime

    AdvanceDuration

    SetWallClockTime

    ScheduleDeadline

    TriggerExpiredTimers

The exact interface depends on the selected runtime abstraction.

### 4.3 Monotonic Duration Testing

Local processing durations must use monotonic time.

Tests should verify that wall-clock adjustments do not alter:

    RetryBackoff

    HeartbeatTimeout

    ProcessingDuration

    RecordingFlushDeadline

    SnapshotCollectionInterval

### 4.4 Wall-Clock Adjustments

Simulate:

    Wall Clock Moves Forward

    Wall Clock Moves Backward

    Monotonic Clock Continues

The runtime must not calculate negative local elapsed durations because the system clock changed.

### 4.5 Exchange Timestamp Behavior

Generate events with:

    Equal Event Timestamps

    Out-Of-Order Timestamps

    Future Exchange Timestamps

    Delayed Historical Timestamps

    Missing Optional Timestamps

These conditions must not automatically be classified as transport loss.

### 4.6 Controlled Tokio Time

Where applicable, the test runtime may use Tokio's controlled-time facilities.

Tests should avoid:

```rust
tokio::time::sleep(Duration::from_secs(5)).await;
```

solely to wait for an expected operation.

Instead, prefer controlled timer advancement and explicit completion synchronization.

### 4.7 Scheduler Ordering

Asynchronous tasks may complete in different orders.

Tests must cover:

    Response Before Timeout

    Timeout Before Response

    Cancellation Before Completion

    Completion After Generation Replacement

    Shutdown During Recovery

These conditions must be modeled explicitly.

### 4.8 Deterministic Randomness

Backoff jitter and randomized testing require seeded randomness.

The test harness must permit repeatable random sequences.

Conceptually:

    RandomSeed = 12345

A failed randomized test must report enough information to reproduce its generated sequence.

### 4.9 Logical Event Ordering

Tests must distinguish:

    Transport Receive Order

    Native Message Record Order

    Canonical Acceptance Order

    Derived Event-Time Order

    Subscriber Delivery Order

A test asserting event-time order must not inadvertently verify only transport arrival order.

### 4.10 Controlled Concurrency

Concurrency tests should use explicit barriers, notifications, or deterministic scheduling where possible.

They must avoid assuming that a particular Tokio task always runs first.

### 4.11 Determinism Invariant

    Identical Test Inputs,
    Configuration,
    Initial State,
    And Controlled Scheduling
    Must Produce
    Equivalent Results.

---

## 5. Exchange Protocol Fixtures

Exchange protocol fixtures verify actual native message interpretation.

The initial scope includes:

    Binance

    Bybit

    OKX

    Bitget

    Gate.io

Across:

    Spot

    Linear

    Inverse

For:

    Trades

    L2 Depth

### 5.1 Fixture Objectives

Fixtures must validate:

- Native message classification.
- Instrument routing.
- Record extraction.
- Timestamp normalization.
- Exact Decimal parsing.
- Quantity denomination.
- Aggressor-side semantics.
- Native trade identities.
- Native sequence metadata.
- Snapshot classification.
- L2 update semantics.
- Protocol error handling.
- Canonical output equivalence.

### 5.2 Exchange Coverage

The initial configuration scope contains:

    5 Exchanges
        ×
    3 Market Families
        ×
    2 Primary Channels

This produces 30 exchange/market/channel combinations.

Each combination must have representative positive and negative fixtures.

### 5.3 Fixture Organization

Conceptually:

    tests/fixtures/exchanges/
    |
    +-- binance/
    |    +-- spot/
    |    +-- linear/
    |    +-- inverse/
    |
    +-- bybit/
    |    +-- spot/
    |    +-- linear/
    |    +-- inverse/
    |
    +-- okx/
    |    +-- spot/
    |    +-- linear/
    |    +-- inverse/
    |
    +-- bitget/
    |    +-- spot/
    |    +-- linear/
    |    +-- inverse/
    |
    +-- gateio/
         +-- spot/
         +-- linear/
         +-- inverse/

### 5.4 Fixture Contents

Each fixture should identify:

    Exchange

    MarketFamily

    Channel

    FormatSpecification

    InstrumentMetadataRevision

    RawMessage

    ExpectedCanonicalOutput

    ExpectedSequenceMetadata

    ExpectedIntegrityOutcome

### 5.5 Fixture Provenance

Fixtures obtained from real exchanges should retain:

    SourceExchange

    EndpointOrChannel

    CaptureTimestamp

    ProtocolVersion

    InstrumentIdentity

    RelevantConfigurationRevision

This helps distinguish genuine exchange behavior from manually constructed examples.

### 5.6 Positive Fixtures

Positive fixtures should include:

    Valid Trade

    Valid Multi-Trade Message

    Valid L2 Snapshot

    Valid L2 Delta

    Valid Empty Update

    Valid Sequence Reset

where applicable.

### 5.7 Negative Fixtures

Negative fixtures should include:

    Missing Required Field

    Wrong Field Type

    Invalid Decimal

    Invalid Timestamp

    Unknown Instrument

    Unsupported Channel

    Malformed Native Sequence

    Unexpected Snapshot Variant

    Incompatible Quantity Metadata

### 5.8 Numeric Precision Fixtures

Include values that expose floating-point precision loss.

Examples:

    0.00000001

    65000.12345678

    Large Native Trade Identifier

    Large Native Sequence Identifier

    Fractional Contract Quantity

Expected canonical values must be exact according to the configured Decimal contracts.

### 5.9 Quantity Semantics

Fixtures must distinguish:

    Base Asset Quantity

    Quote Asset Quantity

    Contract Count

    Signed Contract Quantity

    Contract Value Denomination

The normalizer must not infer native units from exchange identity alone.

### 5.10 Unverified Protocol Requirements

A configuration containing unresolved normalization-critical semantics must fail validation.

For example:

    raw_unit = "requires_verification"

must not produce accepted canonical L2 quantities.

Tests must preserve that failure requirement rather than invent a quantity unit.

### 5.11 Native Sequence Fixtures

Sequence fixtures must represent the actual protocol semantics.

Examples:

    Consecutive Sequence IDs

    Sequence Ranges

    Previous Sequence References

    Native Resets

    Unsequenced Messages

The test harness must not assume all exchanges share one sequence strategy.

### 5.12 Fixture Compatibility

When a fixture's expected canonical output changes, the change must be reviewed.

A changed expectation may indicate:

- A corrected protocol interpretation.
- A normalization regression.
- An instrument metadata change.
- A canonical schema change.
- An invalid original fixture.

Expected-output changes must not be accepted automatically merely because new code produces different values.

### 5.13 Fixture Invariant

    Exchange Fixtures
    Must Preserve
    Native Protocol Semantics
    And Exact Canonical
    Interpretation.

---

## 6. Historical and Live Compatibility Testing

MarketForge Live must reuse existing historical canonical models, normalization primitives, BookStore logic, and relevant serialization infrastructure.

Compatibility tests ensure that Live implementation does not introduce inconsistent representations of equivalent market-data events.

### 6.1 Compatibility Objectives

Compatibility testing must verify:

- Canonical trade equivalence.
- Canonical L2-level equivalence.
- Exact Decimal semantics.
- Timestamp semantics.
- Quantity normalization.
- Instrument identity.
- Aggressor-side interpretation.
- BookStore behavior.
- Depth message boundaries.
- Derived merge ordering.
- Currency-conversion arithmetic.
- Parquet serialization compatibility where applicable.

### 6.2 Shared Canonical Models

The existing canonical event types remain authoritative.

Conceptually:

    CanonicalEvent

        Trade

        L2LevelUpdate

Live processing must not introduce a competing Trade or L2LevelUpdate representation.

Live-specific cursor, generation, and transport metadata remain separate.

### 6.3 Canonical Trade Equivalence

Given equivalent historical and live source records:

    Historical Normalization
              |
              v
        Canonical Trade

and:

    Live Normalization
              |
              v
        Canonical Trade

must produce equivalent canonical economic values and identity semantics.

Differences in transport-specific provenance may remain.

### 6.4 Depth Equivalence

The historical implementation groups L2 updates into logical native-message boundaries.

Compatibility testing must preserve:

    DepthEventBoundary

    DepthSourceOperation

    DepthSourceEventMetadata

    Vec<L2LevelUpdate>

Live processing must not flatten native depth message semantics in a way that breaks reconstruction or recording.

### 6.5 Shared BookStore

The existing BookStore is the shared authoritative implementation for maintaining L2 state.

Live tests must exercise the same core behaviors as historical processing:

    ApplySnapshot

    ApplyBatch

    SetLevel

    RemoveLevel

    ValidateSequence

    EmitChanges

Live-specific synchronization strategies may extend the surrounding protocol logic.

They must not create a second incompatible order-book implementation.

### 6.6 Full Initialization Emission

The existing BookStore may emit differences when replacing a snapshot.

That behavior is not automatically equivalent to emitting a complete initialization for a new independently reconstructable book generation.

Tests must distinguish:

    Snapshot Applied To Existing Book

from:

    Complete Initialization Emitted

A reconstruction segment requiring an independent baseline must contain sufficient complete initialized book state.

### 6.7 Book Reconstruction Tests

Given:

    Authoritative Initialization

    Ordered Valid Delta Groups

the reconstructed book must match the expected reference state.

Tests must verify:

- Bid levels.
- Ask levels.
- Level removal.
- Quantity replacement.
- Generation transitions.
- Sequence metadata.
- Native message atomicity.

### 6.8 Historical-to-Live Handoff

The future Bootstrap Engine will join:

    Historical Archives

    PostgreSQL Recording

    Live Retention

    Continuous Live Subscription

Compatibility tests should preserve the necessary contracts.

A valid handoff requires:

- Compatible canonical semantics.
- Explicit source provenance.
- Deduplication across overlaps.
- Valid generation boundaries.
- Verified continuity where supported.
- Explicit unresolved gaps.

### 6.9 Historical Coverage vs Current Validity

A reconstructed current book may be valid while historical L2 transitions remain incomplete.

Similarly, a currently healthy trade stream may have an unresolved earlier execution gap.

Tests must preserve these independent assessments.

### 6.10 Merge Compatibility

Where equivalent source events and ordering policies are available, Live derived output should remain compatible with the historical merge contracts.

Relevant properties include:

    Source Identity

    Event Kind

    Deterministic Tie-Breaking

    Original Canonical Values

    Derived Provenance

Live event-time buffering introduces bounded-lateness behavior that must remain explicitly represented.

### 6.11 Currency-Conversion Compatibility

Live currency conversion must reuse the historical arithmetic and reference-eligibility semantics.

Tests must verify:

    Identity
        Shared MarketForge
    Domain Semantics
    Must Remain Consistent
    Across Historical
    And Live Processing.

---

## 7. Configuration System Testing

The Configuration System must be tested independently of exchange connectivity and runtime execution.

Its primary responsibility is to convert declarative configuration into valid, immutable runtime plans.

Tests must verify that invalid configuration cannot partially modify running components.

### 7.1 Configuration Test Scope

Configuration testing covers:

- TOML parsing.
- Schema validation.
- Exchange profile resolution.
- Raw-format specification resolution.
- Instrument metadata references.
- Source-stream declarations.
- Derived-stream declarations.
- Configuration inheritance.
- Resource constraints.
- Runtime reconfiguration policies.
- Immutable revisions.
- Configuration persistence.
- Restart recovery.

### 7.2 Global Configuration Fixtures

Maintain representative fixtures for:

    live.toml

Tests must include configurations with:

    Default Settings

    Explicit Global Overrides

    Metrics Disabled

    Detailed Timing Enabled

    Recording Enabled

    Retention Enabled

    Prometheus Exporter Enabled

    Invalid Resource Limits

    Unsupported Configuration Version

The optional Prometheus exporter must not become a required dependency of configuration validation or runtime operation.

### 7.3 Source Configuration Fixtures

The initial source configuration should include the approximately 30 primary test streams defined in:

    runtime/sources.toml

These cover:

    Binance
    Bybit
    OKX
    Bitget
    Gate.io

Across:

    Spot
    Linear
    Inverse

For:

    Trades
    L2 Depth

Additional conversion-reference candidates include:

    Bybit ETHBTC Trades

    Bybit ETHBTC Depth

    Bybit BTCUSDC Trades

    Bybit BTCUSDC Depth

These additional streams may remain disabled until their exchange availability and instrument metadata have been verified.

### 7.4 Source Configuration Validation

Tests must verify:

- Valid source identities.
- Duplicate source detection.
- Correct exchange/market resolution.
- Correct channel resolution.
- Required acquisition parameters.
- Native instrument compatibility.
- Recording policy compilation.
- Recovery policy compilation.
- Metrics override compilation.
- Disabled source handling.

The compiler must not activate streams merely because the TOML parses successfully.

### 7.5 Inverse Instrument Resolution

Inverse markets may use native identifiers different from the logical symbols in source declarations.

For example:

    Logical Instrument:
        BTCUSD

    Native Exchange Instrument:
        Exchange-Specific Identifier

Tests must verify that the authoritative instrument catalog resolves the correct native symbol.

The configuration compiler must not assume that every exchange accepts the same textual instrument identifier.

### 7.6 Derived Configuration Fixtures

Maintain fixtures corresponding to the initial derived-stream tests:

    btc_spot_trades_merged

    btc_spot_depth_merged

    binance_btc_spot_combined

    btc_spot_trades_extended

    bybit_ethbtc_to_usdt

    bybit_btcusdc_to_usdt

These configurations exercise:

- Trade-only merges.
- Depth-only merges.
- Mixed trade/depth merges.
- Derived-on-derived dependencies.
- Currency conversion.
- Missing reference paths.
- Disabled derived streams.

### 7.7 Derived Dependency Validation

Tests must verify:

    Valid Source Dependencies

    Valid Derived Dependencies

    Missing Dependencies

    Duplicate Dependencies

    Dependency Cycles

    Unsupported Input Schemas

    Incompatible Transformation Modes

    Invalid Source Ranking

A dependency cycle must fail configuration validation before runtime activation.

### 7.8 Conversion Path Validation

Test at least two distinct conditions.

**Condition A: No valid conversion path**

    Source:
        BTC/USDC

    Target:
        USDT

    Required Reference:
        Unavailable In Catalog

Expected:

    Configuration Validation Failure

**Condition B: Valid path, missing observation**

    Source:
        ETH/BTC

    Target:
        USDT

    Reference Path:
        BTC/USDT

    Reference Observation:
        Temporarily Unavailable

Expected:

    Configuration Valid

    Runtime Conversion:
        Unavailable According To Policy

A missing conversion path is not equivalent to a temporarily missing reference price.

### 7.9 Configuration Inheritance

Verify:

    Global Default
          |
          v
    Source Override
          |
          v
    Effective Value

An absent override must remain distinguishable from an explicitly configured value.

Resetting a field to inheritance must not copy the current default into an explicit override.

### 7.10 Configuration Revision Tests

Test:

    Revision N
        |
        v
    Valid Update
        |
        v
    Revision N+1

Also test simultaneous proposals:

    Client A:
        ExpectedRevision = N

    Client B:
        ExpectedRevision = N

Only one conflicting update may commit.

The other must receive a revision conflict.

### 7.11 Persistence Fault Injection

Inject failures during:

    Serialization

    Temporary File Creation

    File Write

    File Synchronization

    Atomic Rename

    Directory Synchronization

    Revision Publication

After recovery, the configuration store must expose the last valid committed revision.

### 7.12 Hot Reconfiguration Tests

Verify supported hot settings:

    Logging Level

    Metrics Enabled

    Detailed Timing

    Metrics Snapshot Interval

    Recording Batch Size

    Recording Flush Interval

    Maximum Subscribers

Changing these settings must not unnecessarily restart source subscriptions.

### 7.13 Restart-Required Settings

Verify that unsupported hot changes are rejected or reported as restart-required.

Examples:

    Exchange Endpoint

    Transport Handshake Requirements

    Canonical Schema Changes

    Prometheus Exporter Listener Changes

The runtime must not claim that a restart-required setting was applied immediately.

### 7.14 Configuration Testing Invariant

    Configuration Validation
    Must Be Deterministic
    And Side-Effect Free.

    Invalid Configuration
    Must Not Modify
    Authoritative Runtime State.

---

## 8. Connection Manager Testing

The Connection Manager owns physical WebSocket lifecycle, transport liveness, connection pooling, and reconnection.

Tests must verify these responsibilities independently of canonical market-data processing.

### 8.1 Connection Testing Objectives

Verify:

- Connection establishment.
- Transport configuration.
- Connection pooling.
- Connection capacity.
- Reader and writer task ownership.
- Heartbeat operation.
- Transport failure detection.
- Reconnection.
- Backoff and jitter.
- Exchange rate limits.
- Connection generation fencing.
- Graceful shutdown.
- Resource cleanup.

### 8.2 Fake WebSocket Transport

The FakeWebSocket must support controlled operations:

    Connect

    SendFrame

    ReceiveFrame

    Close

    InjectError

    DelayResponse

    DropConnection

    AdvanceTime

The fake must preserve the same transport interface used by production.

### 8.3 Connection Establishment

Test:

    Connection Requested
             |
             v
    Capacity Reserved
             |
             v
    Transport Connect
             |
             v
    Handshake Complete
             |
             v
    Connection Active

Verify that the Connection Manager reports the correct lifecycle transitions.

### 8.4 Connection Failure

Inject failures including:

    DNS Failure

    TCP Connection Refused

    Connection Timeout

    TLS Handshake Failure

    WebSocket Handshake Rejection

    Connection Reset

    Unexpected EOF

    Remote Close Frame

Each failure must produce the appropriate classified result.

### 8.5 Retry Classification

Transient failures may trigger bounded reconnection.

Permanent configuration failures must not produce unlimited retries.

For example:

    Invalid WebSocket Endpoint
        -> Configuration Failure

    Temporary Connection Reset
        -> Retry With Backoff

The tests must verify the actual recovery classification.

### 8.6 Exponential Backoff

Use deterministic time and seeded randomness.

Verify:

    Initial Delay

    Exponential Growth

    Maximum Delay

    Jitter Bounds

    Retry Cancellation

    Stability Reset Interval

A short-lived successful connection must not necessarily reset a repeated-failure backoff immediately.

### 8.7 Rate-Limit Coordination

Connection establishment must respect the applicable exchange rate-limit pool.

Tests must verify:

- Shared limits across compatible connections.
- Connection-attempt limits.
- Cooldown enforcement.
- Cancellation while waiting for permits.
- Correct accounting for attempted operations.

A consumed exchange permit must not be refunded merely because the connection attempt failed.

### 8.8 Connection Pooling

Test compatible connection sharing.

Example:

    OKX Public Connection
              |
              +-- Spot Trades
              |
              +-- Linear Trades
              |
              +-- Inverse Depth

The exact sharing behavior must follow the compiled connection requirements.

### 8.9 Incompatible Connection Requirements

Verify separation when requirements differ:

    Endpoint

    Authentication Context

    Required Headers

    Protocol Version

    Connection Isolation Policy

    Incompatible Heartbeat Requirements

The Connection Manager must not pool connections whose transport requirements are incompatible.

### 8.10 Connection Generation Fencing

Simulate:

    Connection Generation 7
             |
             v
        Disconnect
             |
             v
    Connection Generation 8
             |
             v
    Late Frame From Generation 7

Expected:

    Late Frame Rejected

The obsolete frame must not reach current canonical processing.

### 8.11 Reader Task Failure

Inject a reader failure while the writer and heartbeat tasks remain active.

Verify that the Connection Supervisor:

1. Detects the failure.
2. Invalidates the connection generation.
3. Cancels associated tasks.
4. Notifies the Subscription Manager.
5. Schedules permitted recovery.
6. Releases obsolete resources.

### 8.12 Writer Task Failure

Inject an outbound write failure during subscription activation.

The test must recognize that the exchange may or may not have received the command.

The Connection Manager must not invent a confirmed native subscription outcome.

### 8.13 Shared Connection Failure

One physical connection may carry several native subscriptions.

Simulate one connection failure.

Verify that:

- The Connection Manager creates one connection recovery workflow.
- Affected subscriptions are identified.
- Unrelated connections remain operational.
- Source stream identities remain stable.
- Obsolete subscription generations are fenced.

### 8.14 Connection Shutdown

Verify:

    Graceful Shutdown

    Immediate Shutdown

    Connection Replacement

    Idle Connection Closure

Every shutdown mode must release its resources.

Shutdown must have a finite deadline.

### 8.15 Connection Testing Invariant

    Physical Reconnection
    Restores Transport.

    It Does Not
    Automatically Restore
    Subscription Readiness
    Or Market-Data Integrity.

---

## 9. Subscription Manager and Stream Registry Testing

The Stream Registry owns logical source identity, demand, and lifecycle.

The Subscription Manager owns native subscription admission, acknowledgement, and restoration.

Tests must preserve these ownership boundaries.

### 9.1 Registry Testing Objectives

Verify:

- Stable StreamId assignment.
- Canonical source identity.
- Equivalent acquisition deduplication.
- Persistent demand.
- Temporary demand.
- Subscription activation.
- Subscription deactivation.
- Stream removal.
- Dependency protection.
- Configuration revision consistency.
- Generation fencing.

### 9.2 Source Identity

Two source requests with equivalent acquisition semantics must resolve to one logical acquisition.

Conceptually:

    Source Request A
             |
             v
    Canonical Source Key
             ^
             |
    Source Request B

Expected:

    One Logical Source Stream

The requests may create separate demand handles.

They must not create separate native exchange subscriptions.

### 9.3 Distinct Acquisition Parameters

Requests with materially different acquisition parameters must not be incorrectly deduplicated.

Examples:

    Different Instrument

    Different Market Family

    Different Channel

    Different Required Depth

    Different Native Update Frequency

The canonical source identity determines equivalence.

### 9.4 Persistent Demand

A source enabled in persistent configuration contributes a persistent acquisition requirement.

Tests must verify that this demand is restored after runtime restart.

### 9.5 Temporary Demand

A temporary API or subscriber demand must not become persistent without an explicit configuration change.

After the temporary owner releases its demand, the registry reevaluates whether the source is still required.

### 9.6 Multiple Demand Owners

Example:

    Binance BTCUSDT Trades
              |
              +-- Persistent Configuration
              |
              +-- Recording
              |
              +-- Derived Stream
              |
              +-- Unix Subscriber

Releasing one demand must not terminate acquisition while others remain active.

### 9.7 Subscription Acknowledgements

Test:

    Successful Acknowledgement

    Rejected Acknowledgement

    Delayed Acknowledgement

    Duplicate Acknowledgement

    Missing Acknowledgement

    Partial Batch Success

    Ambiguous Acknowledgement

A native subscription must not be marked active merely because the outbound request was written successfully.

### 9.8 Subscription Generation Fencing

Simulate:

    Subscription Generation 10:
        Subscribe Requested

    Generation 10:
        Timeout

    Generation 11:
        Subscribe Requested

    Generation 11:
        Acknowledged

    Generation 10:
        Late Acknowledgement

The final acknowledgement must not modify generation 11.

### 9.9 Registration vs Activation

Registration creates the logical source descriptor.

Activation establishes the necessary runtime resources.

Tests must verify that a registered but disabled source does not create an active native subscription solely because its definition exists.

### 9.10 Trade Readiness

A trade stream may become operational after:

    Subscription Ready

    Processing Context Ready

    Required Validation Available

The test must separately inspect continuity state.

Successful activation does not imply complete earlier trade history.

### 9.11 L2 Readiness

An L2 stream must not become ready before authoritative book synchronization.

Test:

    Subscription Acknowledged

    Snapshot Pending

Expected:

    Stream Not Yet Ready

After successful synchronization:

    Book Synchronized

Expected:

    Stream Ready

### 9.12 Stream Removal

Removal must:

- Fence obsolete asynchronous operations.
- Validate dependencies.
- Release registered demand.
- Release native subscription resources when no longer required.
- Remove the logical descriptor according to policy.

A removed stream must not be reactivated by a delayed acknowledgement.

### 9.13 Dependency Protection

Attempt to remove a source required by an active derived stream.

Expected default:

    Removal Rejected

unless an explicitly supported cascading operation is requested.

### 9.14 Concurrent Registry Mutations

Generate concurrent operations:

    Activate

    Deactivate

    Remove

    Replace

    Update Configuration

Verify that each logical stream retains one authoritative lifecycle state.

### 9.15 Registry Testing Invariant

    One Canonical Source
    Acquisition Identity
    Must Correspond To
    One Authoritative
    Logical Source Runtime.

---

## 10. Trade Processing and Continuity Testing

Trade processing must preserve execution identity, exact economic normalization, accepted event order, and recoverable continuity.

The Trade Processor remains authoritative for execution integrity.

### 10.1 Testing Objectives

Verify:

- Individual trade normalization.
- Multi-record message processing.
- Native execution identity.
- Aggressor-side interpretation.
- Exact quantities.
- Duplicate detection.
- Source ordering.
- Continuity validation.
- Gap classification.
- Recovery reconciliation.
- Unrecoverable coverage reporting.
- Canonical cursor assignment.

### 10.2 Canonical Trade Fixtures

Fixtures must preserve:

    Exchange

    InstrumentId

    NativeTradeId

    EventTimestamp

    Price

    Quantity

    AggressorSide

    OriginalNativeSemantics

Expected canonical values must use the shared historical Trade type.

### 10.3 Individual Trade Messages

Given one valid native trade message:

    Native Trade
        |
        v
    Decode
        |
        v
    Normalize
        |
        v
    Validate
        |
        v
    Accept Canonical Trade

Expected:

    Exactly One Accepted Trade

provided the native protocol defines one execution record.

### 10.4 Multi-Record Trade Messages

One transport message may contain multiple executions.

Tests must verify:

- Each record is interpreted independently where permitted.
- Native record order is preserved.
- Duplicate identity is handled correctly.
- Accepted canonical trade count matches the valid source records.

### 10.5 Exact Quantity Semantics

Tests must cover:

    Base-Denominated Trades

    Quote-Denominated Trades

    Contract-Denominated Trades

    Signed Contract Quantities

    Inverse Contract Conversion

No exact financial quantity may be silently normalized through binary floating-point arithmetic.

### 10.6 Duplicate Trade Detection

Inject the same native execution twice.

Expected:

    One Accepted Canonical Execution

    Duplicate Detected

The test must not use matching price, size, and timestamp alone as proof of duplicate identity.

### 10.7 Execution Identity Scope

Native trade identifiers may be unique only within an exchange-defined scope.

Tests must verify that deduplication uses the appropriate:

    Exchange

    Market

    Instrument

    NativeTradeIdentity

    SourceIdentity

where required.

An identical numeric trade ID on two unrelated instruments must not automatically cause one execution to be discarded.

### 10.8 Trade Ordering

Tests must verify preservation of accepted source order.

Event timestamps may be equal or out of order.

The Trade Processor must not arbitrarily reorder executions solely to produce monotonically increasing event timestamps.

### 10.9 Continuity Verification

Where native identifiers or sequence semantics support continuity verification, test:

    Continuous Executions

    Missing Execution

    Duplicate Execution

    Out-Of-Order Execution

    Native Identifier Reset

    Unsupported Continuity Evidence

The applicable rules must come from the selected exchange protocol.

### 10.10 Unverifiable Continuity

Some feeds may not provide enough information to prove that every execution was received.

Expected classification:

    Unverified

or the applicable authoritative continuity state.

The test must not interpret an absence of detected gaps as proof of complete history.

### 10.11 Gap Detection

Inject a native continuity violation.

Expected:

    Gap Classified

    Affected Coverage Recorded

    Recovery Requirement Reported

The same incident must not be counted repeatedly as a new unique gap merely because recovery retries occur.

### 10.12 REST Trade Recovery

Simulate recovery through exchange-supported REST endpoints.

Verify:

- Correct recovery interval.
- Applicable rate-limit permits.
- Native identity preservation.
- Overlap deduplication.
- Correct event-time handling.
- Explicit recovery progress.
- Verified recovered coverage.

### 10.13 Recovery Overlap

Example:

    Live Trades:
        100, 101, 105, 106

    REST Recovery:
        101, 102, 103, 104, 105

Expected logical execution set:

    100, 101, 102, 103, 104, 105, 106

Duplicates from the overlap must not become independent executions.

The test must also verify the declared publication and cursor behavior of recovered events.

### 10.14 Partial Recovery

Simulate a REST response that cannot recover every missing execution.

Expected:

    Partial Recovery

    Remaining Gap Explicit

    Historical Coverage Incomplete

Current stream acquisition may continue when permitted by policy.

### 10.15 Unrecoverable Gap

If the exchange no longer provides the required execution history:

    Recovery:
        Unrecoverable

The gap must remain visible in authoritative coverage state.

The test must not allow ordinary subsequent trade activity to erase it.

### 10.16 Recovery Across Reconnection

Simulate:

    Active Trade Stream
           |
           v
    Connection Lost
           |
           v
    Connection Restored
           |
           v
    Subscription Restored
           |
           v
    Trade Recovery
           |
           v
    Continuity Evaluation

Connection restoration must not automatically mark earlier trade coverage complete.

### 10.17 Trade Continuity Invariant

    Accepted Trade Order
    And Exchange-Native
    Execution Completeness
    Are Distinct Guarantees.

    Recovery Must Preserve
    That Distinction.

---

## 11. L2 BookStore and Synchronization Testing

L2 processing must maintain valid source-specific order-book state.

The implementation must reuse the existing historical BookStore and its canonical depth representation.

### 11.1 Testing Objectives

Verify:

- Snapshot initialization.
- Atomic depth updates.
- Absolute price-level replacement.
- Zero-quantity deletion.
- Native sequence validation.
- Snapshot-to-delta bridging.
- Book generation transitions.
- Book invalidation.
- Resynchronization.
- Full initialization emission.
- Correct handling of missed updates.
- Historical BookStore compatibility.

### 11.2 Shared BookStore Contract

The existing BookStore remains authoritative.

Tests must use the production implementation for:

    ApplySnapshot

    ApplyBatch

    SetLevel

    RemoveLevel

    EmitL2Changes

Independent expected results should be established through fixtures or a simple reference book model.

### 11.3 Absolute Update Semantics

For a canonical L2 update:

    Price = 65000

    Quantity = 4

Expected:

    Set Level To 4

A subsequent update:

    Price = 65000

    Quantity = 7

Expected:

    Set Level To 7

The final quantity is not:

    4 + 7

unless an explicitly different native operation is normalized before the canonical update reaches BookStore.

### 11.4 Level Deletion

An accepted zero-quantity absolute update removes the applicable price level.

Test:

    Initial:
        Bid 65000 = 4

    Update:
        Bid 65000 = 0

Expected:

    Bid 65000 Absent

The final book must not retain a zero-quantity price level as an ordinary active order-book entry.

### 11.5 Logical Message Atomicity

One native depth message may update several bid and ask levels.

Tests must verify that the logical group is:

    Validated Together

    Applied Together

    Emitted With Its Boundary

A failed required validation must not leave a partially mutated book.

### 11.6 Snapshot Initialization

A valid initial snapshot must establish the book state defined by the subscribed depth scope.

Verify:

    Bid Levels

    Ask Levels

    Best Bid

    Best Ask

    Sequence Baseline

    Book Generation

The test must not claim full exchange-market depth when only a limited-depth subscription was configured.

### 11.7 Snapshot Replacement

Applying a new authoritative snapshot may replace the current book.

Tests must verify correct removal of levels absent from the replacement snapshot.

Example:

    Previous Book:
        Bid 100 = 5
        Bid 99  = 3

    New Snapshot:
        Bid 100 = 7

Expected:

        Bid 100 = 7

        Bid 99 Absent

### 11.8 Snapshot Difference vs Full Initialization

The existing BookStore snapshot application may emit only changes relative to the previous book.

This is suitable for certain incremental output contracts.

It is not necessarily sufficient to represent a complete independent initialization.

Tests must distinguish:

    Snapshot Difference Output

from:

    Full Initialization State

A new independently reconstructable book generation must expose a complete initialization through the shared canonical depth event-group contract.

### 11.9 Binance Snapshot Bridging

For Binance protocols using native update ranges, test:

    Buffer WebSocket Updates

    Obtain REST Snapshot

    Find Applicable Bridging Update

    Validate Native Sequence

    Apply Buffered Updates

    Establish Synchronized Book

Test both successful and invalid bridging conditions.

The precise sequence predicates must follow the applicable Spot, Linear, or Inverse protocol strategy.

### 11.10 WebSocket Snapshot Protocols

For exchange channels delivering native WebSocket snapshots, test:

    Snapshot Received

    Snapshot Validated

    Book Initialized

    Subsequent Deltas Applied

Examples include applicable Bybit, OKX, and Bitget depth channels.

### 11.11 Native Sequence Strategies

Test each supported strategy using exchange-specific fixtures.

Relevant patterns include:

    ConsecutiveSequence

    RangedSequence

    PreviousSequenceLink

    Unsequenced

    NativeReset

The test suite must not assume that every protocol uses identical sequence arithmetic.

### 11.12 Sequence Gap Invalidation

Simulate:

    Book Synchronized

    Valid Delta

    Sequence Gap

Expected:

    Book Invalidated

    Current Book Generation No Longer Trusted

    Recovery Requested

Invalid book state must not continue to be published as synchronized state.

### 11.13 Resynchronization

Simulate:

    Generation 4:
        Synchronized

    Sequence Gap:
        Generation 4 Invalid

    Recovery Snapshot:
        Accepted

    Generation 5:
        Synchronized

Expected:

    New Authoritative Book Generation

The previous generation must not be silently reused.

### 11.14 Obsolete Snapshot Response

Simulate:

    Snapshot Request A:
        Generation 4

    Synchronization Restart:
        Generation 5

    Snapshot Request A:
        Returns Late

Expected:

    Obsolete Snapshot Rejected

It must not overwrite the newer generation.

### 11.15 Book Reconstruction

Given:

    Complete Initialization

    Valid Ordered Delta Groups

Replay the groups into an independent reference state.

Verify that reconstructed:

    Bids

    Asks

    Quantities

    Deletions

match the expected book.

### 11.16 Missing Initialization

Attempt to reconstruct a book from ordinary absolute updates without a valid initial state.

The test must not claim complete reconstruction if some levels may never have appeared in the observed updates.

### 11.17 Book Validity vs Historical Coverage

After successful resynchronization:

    Current Book:
        Valid

Previous missing L2 transitions may remain:

    Historical Coverage:
        Incomplete

Both conditions must be represented simultaneously.

### 11.18 L2 Synchronization Invariant

    A Synchronized Book
    Requires A Valid
    Authoritative Initialization
    And Applicable
    Sequence Continuity.

    Current Book Validity
    Does Not Prove
    Complete Historical
    Depth-Event Coverage.

---

## 12. Recovery Coordination and Fault-Injection Testing

Recovery testing verifies that MarketForge responds correctly to transport failures, integrity violations, storage failures, and resource exhaustion.

The Recovery Coordinator remains authoritative for recovery workflows.

### 12.1 Recovery Testing Objectives

Verify:

- Failure classification.
- Recovery operation creation.
- Retry eligibility.
- Retry backoff.
- Shared rate-limit coordination.
- Generation fencing.
- Recovery deduplication.
- Recovery escalation.
- Successful recovery conditions.
- Partial recovery.
- Terminal failure handling.
- Recovery cancellation.
- Resource cleanup.

### 12.2 Recovery Failure Matrix

| Failure | Expected initial behavior |
|---|---|
| Network timeout | Retry according to policy |
| Connection reset | Reconnect |
| Temporary exchange outage | Backoff |
| HTTP 429 | Respect cooldown |
| Invalid subscription parameters | Fail configuration |
| Unsupported channel | Fail configuration |
| Authentication rejection | Require correction |
| Malformed market-data message | Evaluate affected integrity |
| Trade continuity gap | Attempt supported recovery |
| L2 sequence gap | Invalidate and resynchronize |
| Repeated synchronization failure | Escalate and extend backoff |
| PostgreSQL unavailable | Preserve applicable recording obligations |
| Retention capacity exhausted | Explicitly fail affected retention guarantee |

The recovery behavior must follow Documents 04–11.

### 12.3 Recovery Operation Identity

One recovery operation may include several retry attempts.

Tests must distinguish:

    RecoveryOperation

from:

    RecoveryAttempt

Repeated attempts must not create multiple independent authoritative recovery operations for the same unresolved target.

### 12.4 Duplicate Recovery Requests

Simulate several components reporting the same failure.

Example:

    Stream Processor:
        Gap Detected

    Subscription Manager:
        Connection Interrupted

    HealthEvaluator:
        Degraded Assessment

Only authoritative failure reports may initiate recovery according to the established ownership contracts.

The HealthEvaluator must not independently create another recovery operation.

### 12.5 Recovery Generation Safety

Every asynchronous recovery result must be checked against the relevant current generation.

Simulate:

    Recovery A Started

    Recovery A Superseded

    Recovery B Started

    Recovery A Completes Late

Expected:

    Recovery A Result Rejected

It must not overwrite Recovery B's state.

### 12.6 REST Recovery Rate Limits

Recovery requests must consume the applicable shared exchange rate-limit budgets.

Simulate simultaneous:

    Trade Recovery

    L2 Snapshot Request

    Metadata Request

    Ordinary REST Request

Verify that all applicable requests respect the same shared limit scope.

### 12.7 HTTP 429

Inject a rate-limit response with a valid cooldown.

Expected:

    Cooldown Registered

    Affected Requests Deferred

    Retry Not Before Deadline

The system must not retry immediately.

### 12.8 Exchange Outage

Simulate simultaneous failures across several connection pools.

Verify:

- No uncontrolled reconnect storm.
- Bounded backoff.
- Shared cooldown enforcement.
- Independent exchange failure domains.
- Gradual restoration.
- Correct source readiness evaluation.

### 12.9 Recovery Completion Criteria

A successful recovery must satisfy the specific operation's declared guarantees.

Examples:

**Connection recovery**

    Transport Operational

**Subscription recovery**

    Native Subscription Operational

**Trade continuity recovery**

    Required Execution Coverage Reconciled

**L2 recovery**

    New Book Generation Synchronized

**Recording recovery**

    Required Database Commit Coverage Restored

These conditions must not be treated as interchangeable.

### 12.10 Partial Recovery

Recovery may restore one guarantee while another remains impaired.

Example:

    Connection:
        Restored

    Subscription:
        Active

    Current Book:
        Synchronized

    Historical L2 Coverage:
        Incomplete

Expected:

    Current Operational Readiness Restored

    Historical Coverage Gap Preserved

### 12.11 Recovery Failure Escalation

Simulate repeated failed attempts.

Verify:

    Failure Count Updated

    Backoff Extended

    Escalation Reported

    Affected Health Updated

    Unrelated Components Unaffected

Permanent failures must not trigger unlimited retry loops.

### 12.12 Recovery Cancellation

Test cancellation during:

    Backoff

    REST Request

    Snapshot Synchronization

    Trade Reconciliation

    Runtime Shutdown

Cancelled operations must release resources and prevent obsolete completions from becoming authoritative.

### 12.13 Recovery and Logging

Significant recovery transitions must produce structured local diagnostics.

Examples:

    RecoveryStarted

    RecoveryAttemptFailed

    RecoveryEscalated

    RecoveryCompleted

    RecoveryFailed

These events must not be written to PostgreSQL as operational history.

### 12.14 Recovery and Metrics

Recovery attempts and outcomes must update existing metrics.

Metrics must not control recovery decisions independently.

A successful retry counter increment is not evidence that the required market-data guarantee has been restored.

### 12.15 Recovery Testing Invariant

    Recovery Success
    Must Be Established
    By The Authoritative
    Component's Completion
    Conditions.

    Retry Activity Alone
    Is Not Evidence
    Of Restored Correctness.

---

**## 13. Event Distribution and Publisher Testing

The Event Distribution subsystem routes accepted canonical and derived events to independently managed consumers.

Tests must verify event identity, ordering, admission, backpressure, failure isolation, and delivery semantics.

The Publisher and Event Router remain responsible for their respective contracts defined in Document 08.

### 13.1 Testing Objectives

Verify:

- Canonical event routing.
- Independent destination admission.
- Per-source event ordering.
- Derived-stream output ordering.
- Event identity preservation.
- Subscriber subscription filtering.
- Shared source acquisition.
- Bounded delivery queues.
- Subscriber backpressure.
- Delivery failure classification.
- Subscriber disconnection.
- Publisher restart and reconnection.
- Correct distinction between admission and delivery.

### 13.2 Event Router Fixtures

Construct canonical event sequences containing:

    Source Trades

    Source L2 Groups

    Derived Trades

    Derived L2 Groups

    Mixed Derived Events

Each event must preserve its applicable:

    StreamId

    StreamCursor

    EventTimestamp

    CanonicalPayload

    Provenance

    GenerationMetadata

The test collector must not alter payloads or reorder events.

### 13.3 Independent Routing

Given one accepted canonical source event and several configured consumers:

    Canonical Event
           |
           v
       Event Router
           |
       +---+---+-----------+
       |       |           |
       v       v           v
    Publisher Recording  Retention

Verify that each destination receives the correct logical event according to its configured delivery contract.

One destination failing must not silently change another destination's acceptance result.

### 13.4 Required vs Best-Effort Sinks

Test the distinction between:

    BestEffort

    Required

    Durable

A failed best-effort subscriber may be disconnected according to policy.

A failed required destination must produce an explicit violation of its required delivery guarantee.

A durable destination must not report success before satisfying its defined persistence commitment.

### 13.5 Admission Semantics

Test these independent boundaries:

    Canonical Event Accepted

    Router Admission Completed

    Publisher Admission Completed

    Subscriber Queue Admission Completed

    Socket Write Completed

    Downstream Application Acknowledged

The final boundary exists only where the protocol explicitly supports application acknowledgements.

The test suite must not infer consumer processing from a successful socket write.

### 13.6 Source Ordering

Provide events:

    Cursor 100

    Cursor 101

    Cursor 102

Expected publication ordering:

    100, 101, 102

The Publisher must not reorder accepted source events.

### 13.7 Derived Ordering

Derived events must follow their transformation's declared ordering contract.

The Publisher must not independently reorder derived events.

Ordering validation belongs to the Merge Engine.

### 13.8 Duplicate Routing

One source event may feed multiple downstream transformations and subscribers.

Tests must distinguish:

    One Canonical Source Event

from:

    Multiple Subscriber Deliveries

Multiple legitimate deliveries must not be counted as duplicate source acquisitions or duplicate canonical events.

### 13.9 Destination Failure Isolation

Simulate:

    Publisher:
        Temporarily Unavailable

    Recording:
        Operational

    Retention:
        Operational

Verify that recording and retention continue according to their own admission contracts.

The router must report the publication failure without fabricating recording or retention success.

### 13.10 Routing Under Load

Generate sustained source traffic while individual consumers experience different processing speeds.

Verify:

- Bounded routing queues.
- Preserved source order.
- Explicit admission failure.
- No silently discarded required events.
- No global blocking caused by one slow best-effort consumer.
- Correct resource accounting.

### 13.11 Event Distribution Invariant

    One Destination's
    Delivery Failure
    Must Not Silently Alter
    Another Destination's
    Delivery Guarantee.

---

## 14. Unix Socket Subscriber Testing

The Unix Publisher exposes canonical and derived streams through a local Unix domain socket.

Subscriber tests must verify protocol framing, stream subscriptions, per-client isolation, backpressure, and reconnection behavior.

### 14.1 Testing Objectives

Verify:

- Unix socket listener initialization.
- Client connection admission.
- Protocol version negotiation.
- Event framing.
- Partial reads and writes.
- Stream subscription filtering.
- Multiple subscribers.
- Slow subscriber handling.
- Disconnection cleanup.
- Cursor-aware replay where supported.
- Subscriber reconnection.
- Resource-limit enforcement.
- Independent TUI/control socket operation.

### 14.2 Test Subscriber

The test infrastructure should provide a reusable Unix socket client.

Conceptually:

    FakeSubscriber {
        session_id

        subscribed_streams

        read_policy

        processing_delay

        received_events

        disconnect_policy
    }

The client should support deliberate slow reads and unexpected disconnection.

### 14.3 Data Socket Framing

Use the versioned binary framing contract from Document 08.

Tests must cover:

    Complete Frame

    Partial Frame Header

    Partial Frame Payload

    Multiple Frames Per Read

    Connection Closure Mid-Frame

    Oversized Frame

The receiver must never rely on one socket read equaling one complete logical event.

### 14.4 Subscription Filtering

Suppose:

    Subscriber A:
        Binance BTCUSDT Trades

    Subscriber B:
        Binance BTCUSDT Depth

    Subscriber C:
        Both Streams

Publish one trade and one depth message.

Expected:

    Subscriber A:
        Trade Only

    Subscriber B:
        Depth Only

    Subscriber C:
        Trade And Depth

The exact depth delivery unit must preserve the established atomic message-group contract.

### 14.5 Source and Derived Subscriptions

Subscribers may independently request:

    Source Stream

    Derived Stream

One source stream may be referenced by several derived streams.

The Publisher must not create new native exchange subscriptions merely because multiple clients request related outputs.

### 14.6 Subscriber Isolation

Simulate three subscribers:

    Fast Subscriber

    Slow Subscriber

    Disconnected Subscriber

The slow subscriber must not indefinitely block the fast subscriber.

The disconnected subscriber's resources must be released.

### 14.7 Slow Consumer Backpressure

Configure a small subscriber queue.

Stop reading from that subscriber.

Continue publishing events.

Expected:

    Queue Pressure Detected

    Configured Backpressure Applied

    Subscriber Disconnected If Required

    Failure Reported

No unbounded memory growth is permitted.

### 14.8 No Silent Partial Delivery

A subscriber experiencing queue exhaustion must not receive an apparently complete contiguous cursor sequence when required events were omitted.

The implementation must preserve its declared gap, disconnection, or replay-recovery semantics.

### 14.9 Socket Write vs Application Processing

A successful socket write means the local operating system accepted the transmitted bytes according to socket semantics.

It does not prove that the subscriber application decoded or processed the event.

Tests must preserve this distinction.

### 14.10 Subscriber Reconnection

Simulate:

    Subscriber Connected

    Events Received

    Subscriber Disconnected

    Events Published During Disconnection

    Subscriber Reconnected

The resulting behavior must follow the configured replay-and-follow contract.

The Publisher must not fabricate delivery of unavailable historical events.

### 14.11 Replay-and-Follow

Where replay-and-follow is supported:

    Subscriber Requests Cursor N
                |
                v
    Retention Replay
                |
                v
    Catch Up To Live Frontier
                |
                v
    Continuous Live Delivery

Tests must verify that the transition does not omit or incorrectly duplicate logical cursor positions.

Transport retries may repeat events where permitted, but duplicate identity must remain detectable.

### 14.12 Subscriber Demand Lifecycle

A subscriber may create transient demand for a source stream.

Tests must verify:

- Demand registration.
- Shared source reuse.
- Demand release on disconnect.
- Preservation of source acquisition when other demand owners remain.
- No accidental persistence of transient subscriptions.

### 14.13 Unix Socket Resource Cleanup

After disconnect:

    Client Socket Closed

    Subscriber Queue Released

    Subscription Handles Released

    Subscriber Metrics Deregistered

    Session State Removed

Cleanup must be idempotent.

### 14.14 Publisher Isolation From Control API

The canonical data socket and management control socket remain separate.

Test that malformed data-socket messages cannot be interpreted as runtime configuration commands.

Likewise, control messages must not appear as canonical market-data events.

### 14.15 Subscriber Testing Invariant

    Subscriber Delivery
    Must Be Explicitly
    Scoped, Ordered,
    Bounded, And
    Failure-Aware.

---

## 15. Live Retention Testing

Live Retention preserves accepted canonical events for replay, recovery, and future historical bootstrap.

The Retention Manager uses bounded memory and Parquet spill according to Document 09.

Tests must verify both event preservation and accurate coverage accounting.

### 15.1 Testing Objectives

Verify:

- Retention admission.
- Cursor preservation.
- Memory buffering.
- Batch boundaries.
- Resource-budget enforcement.
- Protected intervals.
- Concurrent readers.
- RAM-to-Parquet movement.
- Replay ordering.
- Coverage intervals.
- Retention reclamation.
- Failure classification.
- Restart recovery.

### 15.2 Retention Identity

Retained events must preserve:

    StreamId

    LogEpoch

    CursorOffset

    CanonicalPayload

    Provenance

    GenerationMetadata

Moving an event between RAM and Parquet must not change its identity.

### 15.3 RAM Retention

Given an accepted event batch:

    Accepted Canonical Events
              |
              v
    Bounded RAM Retention
              |
              v
    Retained Cursor Interval

Verify that:

- All admitted events remain accessible.
- Event ordering is preserved.
- Resource accounting is updated.
- The retained cursor interval is correct.

### 15.4 Memory Capacity

Configure a small retention-memory budget.

Continue admitting events until the configured threshold is reached.

Verify that the Retention Manager applies its spill or backpressure policy.

It must not allocate memory without bounds.

### 15.5 Retention Coverage

Suppose available events occupy:

    Cursor 100 ... 150

    Cursor 180 ... 200

Expected coverage:

    Available:
        [100, 150]
        [180, 200]

    Missing:
        [151, 179]

The Retention Manager must not report one continuous interval merely because its lowest and highest available cursors are 100 and 200.

### 15.6 Epoch-Aware Coverage

Cursor ranges from different log epochs must not be combined as one comparable interval.

For example:

    Epoch 4:
        Offset 100 ... 200

    Epoch 5:
        Offset 1 ... 50

These are distinct cursor namespaces.

### 15.7 Protected Retention

A future Bootstrap session may protect a required interval.

Test:

    Protection Acquired

    Retention Pressure Increases

    Cleanup Requested

Expected:

    Protected Interval Preserved

The Retention Manager must not reclaim data covered by an active valid protection.

### 15.8 Protection Admission Failure

If the requested interval is already unavailable or cannot be protected under the declared resource guarantee:

    Protection Request

Expected:

    Explicit Failure

The Retention Manager must not return a successful reservation for coverage it cannot provide.

### 15.9 Protection Ownership

Simulate:

    Client A:
        Owns Reservation 10

    Client B:
        Attempts Release Of Reservation 10

Expected:

    Rejected Unless Authorized

Reservation ownership must not depend merely on knowing the reservation identifier.

### 15.10 Protection Expiration

Test:

    Reservation Created

    Expiration Deadline Reached

    Reservation Released

Expired reservations must not retain unlimited memory indefinitely.

The selected persistent or session-scoped ownership policy must be respected.

### 15.11 Concurrent Retention Readers

Create multiple readers requesting overlapping intervals.

Verify:

- Correct event identities.
- Correct cursor ordering.
- Safe access to committed data.
- No premature reclamation.
- Bounded reader resources.

### 15.12 Replay From RAM

Read a retained interval entirely resident in memory.

Expected:

    Original Canonical Payloads

    Original Cursor Identities

    Original Event Order

The reader must not change event timestamps or provenance.

### 15.13 Replay Across Storage Boundaries

Request an interval spanning:

    RAM Events

    Parquet Events

Expected:

    One Logical Cursor Sequence

The physical storage boundary must not introduce duplicates or gaps.

### 15.14 Retention vs Exchange Continuity

A continuous retained MarketForge cursor sequence does not prove uninterrupted native exchange acquisition.

The test suite must preserve:

    Retention Cursor Continuity

separately from:

    Trade Continuity

    L2 Synchronization

    Historical Exchange Coverage

### 15.15 Retention Testing Invariant

    Retention Guarantees
    The Availability Of
    Admitted Canonical Events
    According To Its
    Declared Storage Contract.

    It Does Not Independently
    Guarantee Complete
    Exchange History.

---

## 16. Parquet Spill and Storage Testing

Parquet spill moves retained canonical event batches from memory into immutable local storage.

Tests must verify correct serialization, transactional segment publication, replay compatibility, and failure handling.

Existing MarketForge Arrow/Parquet infrastructure should be reused wherever compatible.

### 16.1 Testing Objectives

Verify:

- Spill threshold detection.
- Batch selection.
- Canonical serialization.
- Atomic L2 group preservation.
- Temporary-file creation.
- Parquet file writing.
- Segment validation.
- Manifest publication.
- Committed-segment visibility.
- Memory reclamation.
- Spill failure recovery.
- Disk-capacity enforcement.
- Crash recovery.

### 16.2 Spill Pipeline

Conceptually:

    Retained RAM Batch
             |
             v
    Spill Batch Selected
             |
             v
    Parquet Serialization
             |
             v
    Temporary Segment Written
             |
             v
    Segment Validated
             |
             v
    Segment Committed
             |
             v
    Manifest Updated
             |
             v
    RAM Reclamation Eligible

Tests must verify the actual commitment boundary defined by the retention implementation.

### 16.3 Serialization Compatibility

The spill format must preserve:

    Canonical Event Type

    Canonical Payload

    Exact Decimal Values

    Event Timestamp

    Stream Identity

    Cursor Identity

    Source Provenance

    Applicable Generations

Existing historical Arrow/Parquet contracts remain authoritative where applicable.

### 16.4 Mixed Event Serialization

Where a retained derived stream contains both trades and depth events, its serialization must preserve the appropriate event-kind discrimination.

Tests must verify that a mixed event sequence can be reconstructed without misinterpreting an L2 record as a trade or vice versa.

### 16.5 L2 Group Atomicity

A native depth message may contain several canonical L2 level changes.

A spill boundary must not cause the logical message group to be partially visible as a committed event.

Tests must verify that grouped L2 records preserve:

    DepthEventBoundary

    SourceOperation

    SequenceMetadata

    BookGeneration

    OrderedLevelUpdates

### 16.6 Atomic Segment Publication

Simulate readers while a Parquet segment is being written.

Expected:

    Incomplete Segment:
        Invisible To Readers

    Committed Segment:
        Visible To Readers

Readers must not observe partially written files as committed retained coverage.

### 16.7 Segment Validation

Test invalid segments containing:

    Corrupted Payload

    Invalid Schema

    Missing Required Metadata

    Inconsistent Cursor Range

    Invalid Event Count

    Truncated File

The Retention Manager must reject invalid segments.

### 16.8 Manifest Consistency

A manifest must reference only segments satisfying its commitment contract.

The manifest must not claim committed coverage for a segment whose publication failed.

### 16.9 Spill Failure

Inject failures during:

    File Creation

    Serialization

    File Write

    File Flush

    File Synchronization

    Segment Rename

    Manifest Update

    Directory Synchronization

The resulting state must preserve the last established retention guarantee.

### 16.10 Crash Before Segment Commit

Simulate a crash after writing an incomplete temporary segment but before committing it.

After restart:

    Temporary Segment:
        Not Authoritative

    Manifest:
        Last Valid State

The loader must not infer committed coverage from the mere presence of a file.

### 16.11 Crash After Segment Commit

Simulate a crash after the selected commit boundary.

After restart, the Retention Manager must reconstruct the committed segment and its cursor interval according to the declared durability guarantee.

If the implementation has not established crash durability, it must not claim that the segment necessarily survives power loss.

### 16.12 Disk Full

Inject filesystem capacity exhaustion during spill.

Verify:

- Failure classification.
- Bounded retry behavior.
- No publication of incomplete segments.
- No silent deletion of protected memory.
- Explicit affected retention guarantees.

### 16.13 Memory Reclamation

RAM may be reclaimed only after the relevant retention policy permits it.

For protected intervals, reclamation must preserve replay availability through another valid storage representation.

### 16.14 Spill Performance

Benchmark:

    Serialization Throughput

    Compression Throughput

    Segment Commit Duration

    Manifest Update Duration

    Peak Spill Memory

    Replay Read Throughput

The spill process must not introduce unbounded contention with live event processing.

### 16.15 Parquet Spill Invariant

    Parquet Spill
    Must Preserve Event Identity,
    Cursor Coverage,
    And Canonical Semantics.

    Incomplete Storage
    Must Never Be Reported
    As Committed Coverage.

---

## 17. PostgreSQL Recording Testing

PostgreSQL recording provides optional persistent storage of accepted canonical and derived events.

The Recording Manager owns batching, database transactions, idempotency, committed coverage, and recording recovery.

Tests must preserve the recording contracts established in Document 11.

### 17.1 Testing Objectives

Verify:

- Recording activation.
- Recording deactivation.
- Per-stream recording policy.
- Event admission.
- Batch construction.
- Database transaction execution.
- Commit acknowledgement.
- Idempotent retries.
- Out-of-order batch completion.
- Committed cursor tracking.
- Recording gap detection.
- Recovery from retention.
- Archive-aware cleanup.
- PostgreSQL connection failure.
- Database schema compatibility.
- Correct canonical reconstruction.

### 17.2 Test Database

Recording integration tests should use a dedicated PostgreSQL test database or isolated test schema.

Tests must not modify production recording tables.

The test environment should support:

    Schema Initialization

    Transaction Rollback

    Database Connection Failure

    Query Failure

    Constraint Violation

    Idempotent Retry

    Recording Coverage Inspection

### 17.3 Recording Activation

Test enabling recording on an already active source stream.

Expected:

    Existing Source Acquisition:
        Preserved

    Recording Demand:
        Registered

    Recording Start Cursor:
        Explicit

    Recording Writer:
        Activated

Enabling recording must not recreate the native exchange subscription.

### 17.4 Recording Admission

Given a canonical event admitted to the Recording Manager:

    Event Accepted For Recording

does not immediately imply:

    Event Committed To PostgreSQL

Tests must verify that these states are independently observable.

### 17.5 Batch Construction

Test:

    Maximum Batch Records

    Maximum Batch Delay

    Batch Size Limits

    Per-Stream Ordering

    Mixed Event Types

A batch must not silently exceed configured memory budgets.

### 17.6 Transaction Commitment

Simulate:

    Batch Prepared

    Transaction Started

    Rows Written

    Transaction Committed

    Commit Acknowledged

The Recording Manager must report database durability only after the applicable transaction commitment is established.

### 17.7 Commit Frontier

Suppose batches contain:

    Batch A:
        Cursors 100 ... 149

    Batch B:
        Cursors 150 ... 199

    Batch C:
        Cursors 200 ... 249

If B and C commit but A remains unresolved:

    HighestCommittedCursor:
        249

    LastContiguousCommittedCursor:
        Before 100

relative to the preceding committed frontier.

The Recording Manager must not claim contiguous coverage through 249.

### 17.8 Out-of-Order Commits

Allow database transactions to complete out of order.

Verify that committed interval accounting remains correct.

The implementation must distinguish:

    Submitted

    TransactionallyCommitted

    ContiguouslyCommitted

### 17.9 Ambiguous Commit Outcome

Simulate:

    Batch Submitted
           |
           v
    Database Commit Succeeds
           |
           v
    Connection Lost
    Before Acknowledgement
           |
           v
    Batch Retried

Expected:

    No Duplicate Logical Records

The retry must use the established idempotency identity.

### 17.10 Recording Identity

Database records must preserve sufficient identity to distinguish:

    StreamId

    LogEpoch

    CursorOffset

    Canonical Event Identity

    Native Source Identity

    Relevant Generation

A native trade identifier is not a replacement for the MarketForge cursor identity.

### 17.11 Trade Recording

Record a batch of canonical trades.

Read the records back.

Verify equivalence of:

    InstrumentId

    EventTimestamp

    Price

    Quantities

    AggressorSide

    NativeTradeIdentity

    CanonicalCursor

    Provenance

Exact Decimal values must survive round-trip persistence.

### 17.12 L2 Recording

L2 recording must preserve native logical depth-message grouping.

Tests must verify the parent/child relational representation established in Document 11.

Conceptually:

    DepthEvents
          |
          v
    DepthLevels

The stored group must preserve:

    InitializationOrChanges

    SourceOperation

    NativeSequenceMetadata

    BookGeneration

    OrderedLevelChanges

### 17.13 Initialization Recording

Record a complete authoritative L2 initialization.

Reconstruct an independent BookStore from the recorded initialization and subsequent valid changes.

The reconstructed book must match the expected state.

### 17.14 Updates Without Initialization

Attempt independent reconstruction using only ordinary absolute L2 changes.

Expected:

    Reconstruction Not Proven Complete

unless an earlier authoritative initialization and continuous compatible history are available.

The tests must not assume that observing several days of updates necessarily reconstructs untouched book levels.

### 17.15 Cross-Storage Reconstruction

Test:

    Historical Initialization
              |
              v
    Historical L2 Changes
              |
              v
    PostgreSQL L2 Changes
              |
              v
    Live L2 Changes

When sequence compatibility and continuity are established, the pipeline may reconstruct book state without another database initialization at the storage boundary.

When continuity cannot be established, the system must report the unresolved guarantee.

### 17.16 Recording Gap Detection

Inject a failed recording interval.

Expected:

    Recording Gap Identified

    Committed Frontier Not Advanced Across Gap

    Recovery Requirement Reported

A later successful batch must not erase the unresolved earlier interval.

### 17.17 Database Outage

Simulate PostgreSQL becoming unavailable while exchange acquisition continues.

Verify:

- Recording health reflects the outage.
- Required recording obligations remain explicit.
- The Event Router follows its configured delivery policy.
- Recoverable events remain protected where retention coverage permits.
- Memory usage remains bounded.
- No uncommitted event is reported as durable.

### 17.18 Recording Recovery From Retention

Simulate:

    PostgreSQL Unavailable

    Canonical Events Retained

    PostgreSQL Restored

    Recording Recovery Started

    Retained Events Replayed

    Idempotent Commit Completed

Expected:

    Recording Coverage Reconciled

    Committed Frontier Advanced

    No Duplicate Logical Records

Recovery must fail explicitly when required retained coverage is unavailable.

### 17.19 Recording Restart Recovery

Restart the Recording Manager with existing committed data.

Verify that it reconstructs:

    Committed Cursor Intervals

    Last Contiguous Commit Frontier

    Known Recording Gaps

    Recording Generation

It must not assume that all previously submitted batches committed.

### 17.20 Recording Deactivation

Disable recording while batches remain pending.

Verify the declared policy for:

    Admission Stop Boundary

    Pending Batch Resolution

    Final Commit Frontier

    Unresolved Obligations

Disabling recording must not silently abandon events whose durability was required.

### 17.21 Derived Recording

Where derived-stream recording is supported, verify that database identities preserve:

    DerivedStreamId

    DerivedGeneration

    TransformationRevision

    OutputCursor

    SourceProvenance

A derived event must not be stored as though it were an unrelated native source event.

### 17.22 Archive-Aware Cleanup

Recording data may become eligible for cleanup after equivalent historical archive coverage has been independently validated.

Tests must verify:

    Archive Downloaded
        !=
    Archive Validated

    Archive Validated
        !=
    Recording Cleanup Automatically Safe

Cleanup eligibility must require the established provenance, interval, integrity, and continuity checks.

### 17.23 Cleanup Safety

Simulate an archive that:

- Has missing trades.
- Has incompatible instrument metadata.
- Contains incomplete depth coverage.
- Cannot establish a valid source transition.
- Fails validation.

The Recording Manager must not delete the only available recoverable recording interval merely because an archive file exists.

### 17.24 Recording Performance

Benchmark:

    EventsAdmittedPerSecond

    EventsCommittedPerSecond

    BatchPreparationDuration

    CommitDuration

    PeakRecordingQueueMemory

    RecoveryReplayThroughput

    IndexAndConstraintOverhead

Benchmark the actual proposed PostgreSQL schema and representative L2 message sizes.

### 17.25 PostgreSQL Testing Invariant

    Recording Durability
    Is Established By
    Confirmed Database
    Commitments.

    Recording Completeness
    Is Established By
    Verified Coverage,
    Not Merely By
    Stored Row Counts.

---

## 18. Crash-Recovery and Persistence Boundary Testing

MarketForge Live must distinguish normal runtime failures from abrupt process termination and power-loss scenarios.

A successful graceful shutdown does not establish that the system behaves identically after an uncontrolled crash.

### 18.1 Testing Objectives

Verify:

- Configuration recovery.
- Cursor namespace safety.
- Recording commit reconciliation.
- Retention manifest recovery.
- Incomplete Parquet segment detection.
- Stream lifecycle reconstruction.
- Subscription generation replacement.
- Book synchronization reinitialization.
- Recovery of persistent derived declarations.
- Explicit reporting of unavailable pre-crash events.
- Correct classification of durable and non-durable data.

### 18.2 Crash Categories

Testing should distinguish:

    Controlled Shutdown

    Process Termination

    Runtime Panic

    Filesystem Write Failure

    Database Connection Failure

    Simulated Power Loss

These failures have different persistence implications.

### 18.3 Graceful Shutdown

During graceful shutdown, verify:

    Stop New Acquisition

    Resolve Pending Operations

    Flush Eligible Recording Batches

    Finalize Permitted Retention Segments

    Close Subscribers

    Persist Final Configuration State

    Release Runtime Resources

A shutdown deadline may prevent some obligations from completing.

Incomplete obligations must be reported.

### 18.4 Abrupt Process Termination

Terminate the process without graceful cleanup.

On restart, verify that:

- The last committed configuration revision is restored.
- Existing PostgreSQL commitments are reconciled.
- Valid committed Parquet segments are rediscovered.
- Obsolete Unix socket endpoints are handled safely.
- Runtime generations are reestablished.
- Existing source declarations are restored.
- No previously invalid book is treated as synchronized.

### 18.5 Cursor Namespace Safety

A process restart must not cause an already committed cursor identity to be reused for a different event.

Test:

    Previous Runtime:
        Stream S
        Epoch E
        Offset 100

    Runtime Crashes

    New Runtime Starts

Expected:

    Resume At A Proven Safe Position

or:

    Establish New Log Epoch

The runtime must not reuse:

    (S, E, 100)

for an unrelated accepted event.

### 18.6 Recording Crash Reconciliation

Simulate a database transaction committing immediately before process termination.

The restarted Recording Manager must query or otherwise reconcile authoritative committed state.

It must not assume the transaction failed merely because the previous process did not receive an acknowledgement.

### 18.7 Retention Crash Recovery

Simulate crashes at:

    Before Spill Start

    During Temporary File Write

    After Temporary File Write

    Before Segment Publication

    After Segment Publication

    During Manifest Update

Recovery must follow the declared segment and manifest commitment semantics.

### 18.8 In-Memory Event Loss

Uncommitted in-memory retention may be lost after process termination.

The recovery system must not claim that such events remain available merely because their cursors appeared in a previous runtime snapshot.

### 18.9 Current Stream Restoration

Persistent source configuration may be restored automatically.

However:

    Restored Configuration
        !=
    Restored WebSocket Connection

    Restored Subscription
        !=
    Restored Trade Continuity

    Restored Book Generation
        !=
    Previously Synchronized Book

The stream must follow the appropriate activation and recovery lifecycle.

### 18.10 L2 After Restart

A previously synchronized L2 book cannot be treated as currently synchronized merely because a prior operational snapshot recorded that status.

Unless an explicitly validated persistent-book restoration contract exists, the runtime must establish a new authoritative synchronization.

### 18.11 Derived Stream Restoration

Persistently configured derived streams must be reconstructed from their accepted configuration.

They must reestablish required dependencies and transformation state.

An obsolete derived-generation identity must not be treated as an active current runtime instance.

### 18.12 Restart With PostgreSQL Unavailable

Test startup when:

    Configuration:
        Valid

    Exchange Connectivity:
        Available

    PostgreSQL:
        Unavailable

Expected behavior depends on configured recording obligations.

The runtime must not silently claim that required recording durability is available.

Unrelated acquisition may continue where permitted by policy.

### 18.13 Restart With Retention Corruption

Simulate a corrupted committed retention segment.

Expected:

    Corruption Detected

    Affected Cursor Coverage Invalidated

    Recovery Evaluated

    Unrelated Valid Segments Preserved

The runtime must not invent missing canonical events.

### 18.14 Restart With Invalid Configuration

Simulate a persisted configuration incompatible with the installed static exchange specifications.

The runtime must fail safely or enter an explicitly supported degraded management mode.

It must not silently change acquisition semantics to achieve startup.

### 18.15 Power-Loss Durability

Power-loss testing requires care because ordinary process-kill tests do not simulate loss of unflushed operating-system page-cache data.

A claimed crash-durable guarantee must be supported by the relevant:

    File Synchronization

    Directory Synchronization

    Database Commit

    Storage Device Semantics

Process termination alone is insufficient evidence of power-loss durability.

### 18.16 Crash-Recovery Matrix

| Resource              | Expected restart behavior                     |
| --------------------- | --------------------------------------------- |
| Desired configuration | Restore last committed revision               |
| Source declarations   | Reconstruct logical registrations             |
| WebSocket connections | Reestablish new connections                   |
| Native subscriptions  | Restore from desired demand                   |
| Trade continuity      | Reevaluate and recover where possible         |
| L2 BookStore          | Establish valid synchronization               |
| Derived streams       | Reconstruct and activate dependencies         |
| PostgreSQL recording  | Reconcile actual committed state              |
| Parquet retention     | Restore valid committed segments              |
| RAM-only retention    | Report lost unavailable data where applicable |
| Unix subscribers | Reconnect and reestablish subscriptions |
| Control API sessions | Reconnect and reconcile pending operations |
| Metrics snapshots | Establish a new runtime snapshot epoch |
| Operational diagnostics | Recover only explicitly committed local history |
| Prometheus exporter | Restart independently when configured |

### 18.17 Crash-Recovery Completion Criteria

A successful runtime restart must establish:

- Valid persistent configuration.
- Unambiguous runtime generations.
- Safe canonical cursor allocation.
- Correct reconstruction of desired stream ownership.
- Correct reconciliation of committed recording intervals.
- Correct identification of available retained intervals.
- No fabricated persistence guarantees.
- No restoration of obsolete connection or subscription state.
- No publication of previously invalid book state as synchronized.
- Explicit reporting of unresolved historical coverage gaps.

### 18.18 Crash-Recovery Invariant

    Process Restart
    Must Restore
    Only State Whose
    Recovery Contract
    Can Be Verified.

    Previously Observed
    Runtime State
    Is Not Proof
    Of Current Validity.

---

## 19. Combined Stream and Event-Time Ordering Testing

Combined-stream testing verifies that MarketForge can merge canonical events from multiple sources while preserving declared ordering, source identity, event-group boundaries, and failure semantics.

The Merge Engine remains authoritative for ordering and watermark state.

The initial tests should use the derived-stream definitions established in:

    runtime/derived.toml

### 19.1 Testing Objectives

Verify:

- Trade-only merging.
- Depth-only merging.
- Mixed trade/depth timelines.
- Multi-exchange source participation.
- Deterministic tie-breaking.
- Event-time ordering.
- Bounded reorder buffers.
- Watermark progression.
- Watermark stalls.
- Late-event classification.
- Source disconnection.
- Source recovery.
- Derived generation transitions.
- Nested derived dependencies.
- Preservation of canonical source events.

### 19.2 Initial Merge Fixtures

The initial integration suite contains four merge definitions:

    btc_spot_trades_merged

    btc_spot_depth_merged

    binance_btc_spot_combined

    btc_spot_trades_extended

These configurations exercise different merge contracts.

The tests must load the same proposed configuration schema used by the runtime rather than invent test-only transformation definitions.

### 19.3 Trade-Only Merge

Use:

    btc_spot_trades_merged

Inputs:

    Binance BTCUSDT Spot Trades

    Bybit BTCUSDT Spot Trades

    OKX BTCUSDT Spot Trades

The merge must preserve:

    Originating Exchange

    Source StreamId

    Source Cursor

    Native Trade Identity

    Canonical Trade Payload

    Event Timestamp

    Derived Output Identity

### 19.4 Arrival Order vs Event-Time Order

Provide events in this receive order:

    Binance:
        Timestamp 100

    Bybit:
        Timestamp 95

    OKX:
        Timestamp 98

An arrival-order merge may produce:

    100, 95, 98

An event-time merge with sufficient buffering and watermark eligibility should produce:

    95, 98, 100

The expected result must follow the selected ordering contract.

Tests must not assume that every merge mode guarantees strict chronological output under unbounded lateness.

### 19.5 Equal-Timestamp Tie-Breaking

Provide events:

    Binance:
        Timestamp 100

    Bybit:
        Timestamp 100

    OKX:
        Timestamp 100

Expected ordering must follow the deterministic tie-breaking contract.

The existing historical merge model provides relevant ordering concepts:

    EventTimestamp

    SourceRank

    TaskRank

    EventOrdinal

The Live implementation must preserve compatible semantics where applicable.

The test must verify that repeated runs produce the same output ordering for identical inputs and source-ranking configuration.

### 19.6 Source Rank Changes

Change the configured source order.

Verify that:

- The configuration change is validated.
- The applicable transformation generation changes.
- Events already published retain their original provenance.
- New events use the replacement ordering contract.
- No two generations simultaneously claim authority over the same output cursor identity.

### 19.7 Depth-Only Merge

Use:

    btc_spot_depth_merged

Inputs:

    Binance BTCUSDT Spot Depth

    Bybit BTCUSDT Spot Depth

    OKX BTCUSDT Spot Depth

The output represents:

    Combined L2 Event Timeline

It does not represent:

    One Consolidated Order Book

### 19.8 Depth Source Identity

Depth records must preserve their originating book identities.

For example:

    Binance Book Generation 4

    Bybit Book Generation 2

    OKX Book Generation 7

These generations are independent.

The merge must not interpret them as one shared order-book generation.

### 19.9 Depth Message Atomicity

A native depth message may contain multiple canonical level changes.

The merge must preserve the logical message boundary.

Tests must verify that one grouped depth event is not split into separately interleaved level changes from unrelated sources.

### 19.10 Mixed Trade and Depth Timeline

Use:

    binance_btc_spot_combined

Inputs:

    Binance BTCUSDT Spot Trades

    Binance BTCUSDT Spot Depth

Expected output supports:

    Trade

    L2LevelUpdate Group

within the same derived event timeline.

The event-kind discriminator must remain compatible with the historical canonical event representation.

### 19.11 Mixed Event Ordering

Provide:

    Trade:
        Timestamp 100

    Depth Group:
        Timestamp 99

    Trade:
        Timestamp 101

Expected event-time output, when all records are eligible for release:

    Depth Group 99

    Trade 100

    Trade 101

The depth group must remain logically atomic.

### 19.12 L2 Synchronization Failure During Mixed Merge

Simulate:

    Trades:
        Healthy

    Depth:
        Sequence Gap Detected

Expected:

    Trade Input:
        Remains Valid

    Depth Input:
        Invalidated

    Derived Health:
        Degraded Or Unhealthy
        According To Required Source Policy

The merge must not fabricate valid depth events during the invalid synchronization interval.

### 19.13 No Independent Book Consolidation

The Merge Engine must not combine bids or asks from different exchanges into one synthetic order book unless a separately defined transformation explicitly implements that operation.

Tests must reject accidental cross-exchange level aggregation.

### 19.14 Combined Stream Invariant

    Event Merging
    Preserves Source
    Identity And
    Canonical Semantics.

    It Does Not
    Implicitly Consolidate
    Independent Market State.

---

## 20. Watermark and Late-Event Testing

The Merge Engine uses bounded buffering and explicit event-time progress rules.

Watermark tests must verify both ordering correctness and behavior when input streams are delayed, idle, unavailable, or recovering.

### 20.1 Testing Objectives

Verify:

- Per-source event-time progress.
- Combined watermark calculation.
- Watermark monotonicity where required.
- Event release eligibility.
- Reorder buffer limits.
- Source inactivity handling.
- Source recovery.
- Late-event classification.
- Late-event publication policy.
- Ordering violation reporting.
- Output frontier preservation.

### 20.2 Controlled Event-Time Inputs

The test harness must permit independent control of:

    EventTimestamp

    ReceiveTime

    SourceIdentity

    ArrivalOrder

    SourceHealth

    WatermarkProgress

The Merge Engine must not assume that event timestamps equal local receive timestamps.

### 20.3 Watermark Progression

Construct a merge with three required sources.

Advance each source's event-time progress independently.

Verify that the combined watermark follows the configured source-participation and ordering policy.

The precise watermark calculation must match Document 10.

### 20.4 Slowest Required Source

Example:

    Binance Watermark:
        1000

    Bybit Watermark:
        990

    OKX Watermark:
        800

For a strict all-required-sources minimum-watermark policy:

    Combined Watermark:
        800

The test must not advance the combined watermark to 990 or 1000 while OKX remains a required limiting participant.

### 20.5 Watermark Stall

Stop advancing one required source.

Continue delivering events from other sources.

Verify:

    Watermark Progress:
        Stalled According To Policy

    Merge Buffer:
        Growing Within Limits

    Limiting Source:
        Correctly Identified

A watermark stall must not automatically be classified as lost exchange events.

### 20.6 Source Inactivity

An idle source may legitimately produce no trades.

The Merge Engine must apply its configured inactivity policy rather than assume all silent sources have failed.

Tests must distinguish:

    Legitimately Idle

    Transport Disconnected

    Subscription Failed

    Source Integrity Invalid

### 20.7 Source Failure Policy

Under:

    continue_degraded

a derived stream may continue using eligible sources according to the configured ordering contract.

Under a strict participation policy, the merger may need to pause or fail.

Tests must verify the actual selected policy.

A source must not be silently removed from the required participation set merely because its event rate is low.

### 20.8 Reorder Buffer

Configure a small buffer budget.

Inject events that cannot yet be released.

Expected:

    Buffer Occupancy Increases

    Capacity Threshold Reached

    Configured Policy Applied

The Merge Engine must not allocate unlimited memory.

### 20.9 Buffer Saturation

When the reorder buffer reaches its hard resource limit, the runtime must report the resulting operational condition.

It must not silently discard canonical events whose delivery contract requires preservation.

### 20.10 Successfully Reordered Events

Provide out-of-order arrivals that remain within the configured reorder window.

Expected:

    Events Emitted In Required Order

    No Finalized Frontier Violation

These events must not be counted as late merely because their arrival order differed from event-time order.

### 20.11 Late Event Detection

Advance the finalized output frontier.

Then deliver an event whose timestamp falls before that frontier.

Expected:

    Late Event Detected

The event must follow the configured late-event policy.

### 20.12 Emit-Late Policy

Under:

    emit_late

the Merge Engine may publish an explicitly marked late event.

The test must verify:

- Late classification is preserved.
- Source provenance remains intact.
- The event is not represented as satisfying a strict chronological output guarantee.
- The finalized frontier is not silently moved backward.

### 20.13 Strict Ordering Failure

If the configured policy prohibits late publication, an event violating the finalized frontier must produce the corresponding rejected, side-output, or failure outcome.

The implementation must not silently reorder already published events.

### 20.14 Equal Timestamps

Events with equal timestamps must be ordered deterministically according to the established tie-breaking contract.

The watermark implementation must not introduce nondeterministic ordering between equivalent inputs.

### 20.15 Watermark Recovery

Simulate:

    Source Temporarily Unavailable

    Watermark Stalled

    Source Restored

    Event-Time Progress Resumed

Verify that the Merge Engine:

- Revalidates source participation.
- Applies its recovery and lateness policy.
- Preserves the finalized output frontier.
- Releases eligible buffered events.
- Reports any unresolved ordering violations.

### 20.16 Derived Generation Reset

When a transformation generation changes, watermark state must be initialized according to the new generation's contract.

Obsolete watermark updates from the previous generation must not affect the replacement.

### 20.17 Nested Merge Watermarks

Use:

    btc_spot_trades_extended

Inputs:

    btc_spot_trades_merged

    bitget_spot_btcusdt_trades

The nested merge must respect the upstream derived stream's finalized output frontier and declared ordering guarantees.

It must not infer complete upstream progress merely because the upstream merger emitted one event.

### 20.18 Nested Merge Failure

Simulate a degraded source within the upstream merge.

Verify that the downstream merge receives the correct dependency health and ordering information.

The downstream merger must not report complete source participation merely because its direct upstream derived-stream connection remains active.

### 20.19 Watermark Testing Invariant

    Watermarks Represent
    Event-Time Progress
    Under A Defined
    Ordering Contract.

    They Are Not
    Proof Of Complete
    Exchange Acquisition.

---

## 21. Currency Conversion Testing

Currency conversion testing verifies exact numerical transformation, reference selection, path resolution, missing-reference behavior, and source-event preservation.

The Currency Conversion Engine must reuse the established historical conversion semantics.

### 21.1 Testing Objectives

Verify:

- Currency identity resolution.
- Conversion path construction.
- Direct conversion.
- Multi-hop conversion.
- Inverse reference direction.
- Event-time reference eligibility.
- Reference staleness.
- Missing reference observations.
- Missing conversion paths.
- Exact Decimal arithmetic.
- Rounding policy.
- Arithmetic overflow.
- Original canonical value preservation.
- Conversion status.
- Derived provenance.
- Conversion dependency health.

### 21.2 Initial Conversion Fixtures

The initial derived configuration includes:

    bybit_ethbtc_to_usdt

    bybit_btcusdc_to_usdt

These definitions may remain disabled in the ordinary startup configuration until their source instruments and reference paths are validated.

Deterministic tests must explicitly provision the required synthetic instrument metadata and reference events.

### 21.3 ETH/BTC Conversion

Test:

    Source:
        ETH/BTC

    Target:
        USDT

    Reference:
        BTC/USDT

Expected relationship:

    ETH/BTC × BTC/USDT = ETH/USDT

Example:

    ETH/BTC:
        0.05

    BTC/USDT:
        60000

Expected converted price:

    ETH/USDT:
        3000

All arithmetic must use exact Decimal representations.

### 21.4 Original Value Preservation

The converted event must retain:

    Original ETH/BTC Price

    Original Canonical Quantities

    Original Source Identity

    Original Event Timestamp

The conversion must not mutate the source event.

### 21.5 Conversion Reference Identity

Reference observations must preserve:

    ReferenceStreamId

    ReferenceEventTimestamp

    ReferencePrice

    ReferenceDirection

    ApplicableSourceCursor

The derived event must retain sufficient provenance to identify the reference observations used.

### 21.6 Event-Time Reference Selection

Provide:

    Reference A:
        Timestamp 100
        Price 60000

    Reference B:
        Timestamp 110
        Price 61000

    Target Event:
        Timestamp 105

Under event-time reference selection:

    Selected Reference:
        A

Reference B must not be used merely because it has already arrived when the target event is processed.

### 21.7 Equal-Timestamp References

The historical conversion contract distinguishes reference observations eligible at the same event timestamp from those requiring strictly earlier timestamps.

Tests must preserve the established internal-versus-external reference ordering rules.

A future or otherwise ineligible observation must never be selected merely because its numerical price is available.

### 21.8 Reference Staleness

Suppose:

    Reference Timestamp:
        100

    Target Timestamp:
        110

    Maximum Reference Age:
        5

Expected:

    Reference Stale

The conversion must follow the configured missing-reference policy.

Timestamp units must remain consistent.

### 21.9 Missing Reference Observation

A valid conversion path may exist while no eligible reference price is available.

Expected:

    Conversion Status:
        Unavailable

when the configured policy permits publishing unavailable conversion results.

This is a runtime data-availability condition.

### 21.10 Missing Conversion Path

A required reference instrument may be absent from the validated dependency graph.

Expected:

    Configuration Validation Failure

when the selected path-resolution policy requires that reference.

The runtime must not silently invent conversion prices.

### 21.11 BTC/USDC Conversion

Test:

    Source:
        BTC/USDC

    Target:
        USDT

Potential reference:

    USDC/USDT

Expected relationship:

    BTC/USDC × USDC/USDT = BTC/USDT

The runtime must not assume:

    USDC = USDT

without an explicitly supported conversion reference or policy.

### 21.12 Stablecoin Deviation

Use reference prices that differ from one.

Example:

    BTC/USDC:
        60000

    USDC/USDT:
        0.998

Expected:

    BTC/USDT:
        59880

The implementation must not override the observed reference price with an assumed stablecoin peg.

### 21.13 Inverse Reference Direction

A conversion path may require the inverse of an available reference pair.

Tests must verify that the engine applies the correct reciprocal relationship.

For example:

    Available Reference:
        USD/BTC

    Required Direction:
        BTC/USD

The precise arithmetic and rounding behavior must follow the historical conversion contract.

### 21.14 Multi-Hop Conversion

Construct a validated path requiring several reference instruments.

Verify:

- Path direction.
- Each reference's event-time eligibility.
- Reference freshness.
- Exact arithmetic.
- Provenance preservation.
- Correct handling of one unavailable leg.

A multi-hop conversion must not silently skip a missing intermediate reference.

### 21.15 Decimal Precision

Use values that cannot be represented exactly in binary floating point.

Verify exact Decimal arithmetic and the established rounding policy.

Tests should include:

    Small Prices

    Large Prices

    Fractional Quantities

    High-Precision Rates

    Repeating Decimal Division

    Overflow Conditions

### 21.16 Arithmetic Failure

Inject a conversion value outside the supported numerical range.

Expected:

    Explicit Arithmetic Failure

The engine must not emit infinity, NaN, or silently truncated financial values.

### 21.17 Conversion Status

Tests must preserve historical-compatible statuses:

    Identity

    Converted

    Unavailable

    Skipped

These are event-level conversion outcomes.

They are distinct from runtime health.

### 21.18 Conversion Recovery

Simulate:

    Reference Available

    Reference Becomes Stale

    Conversion Unavailable

    New Eligible Reference Arrives

    Conversion Resumes

The source stream must remain independently valid throughout.

### 21.19 Conversion and Merge Integration

Test currency conversion as part of a derived-stream dependency graph.

Verify that conversion-reference health and merge ordering health remain independently observable.

### 21.20 Currency Conversion Invariant

    Conversion Must Preserve
    Original Canonical Values,
    Exact Arithmetic,
    Eligible References,
    And Transformation Provenance.

---

## 22. Metrics Registry Testing

The Metrics Registry provides operational measurements without participating in market-data correctness decisions.

Tests must establish metric semantics, generation handling, bounded resource usage, and collection performance.

### 22.1 Testing Objectives

Verify:

- Counter updates.
- Gauge updates.
- Latency summaries.
- Metric registration.
- Metric deregistration.
- Generation-aware identity.
- Snapshot production.
- Rate calculation.
- Counter reset handling.
- Missing measurements.
- Aggregation correctness.
- Bounded metric history.
- Low hot-path overhead.

### 22.2 Counter Tests

Verify that cumulative counters:

- Increase correctly.
- Preserve their defined lifecycle.
- Handle overflow according to policy.
- Remain safe under supported concurrency.

A connection reconnect must not incorrectly reset process-lifetime counters.

### 22.3 Gauge Tests

Verify current-state measurements such as:

    ActiveConnections

    ActiveStreams

    PendingEvents

    QueueBytes

    ActiveSubscribers

Gauges may increase and decrease.

### 22.4 Rate Calculation

Given:

    Counter At T1:
        10000

    Counter At T2:
        25000

    Elapsed:
        1 second

Expected:

    15000 events/s

Rates must use the actual elapsed monotonic interval.

### 22.5 Counter Generation Changes

Simulate a metric identity changing generation.

The aggregator must not calculate a negative or artificial rate across unrelated counter generations.

### 22.6 Missing Measurements

Tests must distinguish:

    Zero

    Unavailable

    Disabled

    NotApplicable

An unavailable latency metric must not be represented as zero latency.

### 22.7 Latency Measurements

Verify:

    Local Monotonic Duration

    Exchange Event-To-Receive Delay

    Publisher Admission Duration

    Recording Commit Duration

    Merge Buffer Waiting Duration

These measurements must retain their different clock and processing semantics.

### 22.8 Histogram Tests

Verify:

- Correct observation counting.
- Correct bucket classification.
- Bounded memory.
- Compatible histogram aggregation.
- Correct unit representation.
- Stable configured boundaries.

Percentiles derived from bounded samples must not be represented as exact when they are estimates.

### 22.9 Metric Scope

Test:

    Runtime

    Exchange

    Connection

    SourceStream

    DerivedStream

    Subscriber

    Recording

    Recovery

The aggregation layer must not combine incompatible metric identities.

### 22.10 Double-Counting Prevention

Suppose:

    One Source Event

produces:

    One Source Publication

    Two Derived Publications

    Three Subscriber Deliveries

Expected:

    UniqueSourceEvents:
        1

These output and delivery counts must not be described as three or six distinct native market-data events.

### 22.11 Snapshot Consistency

Metrics snapshots may read independent counters at slightly different moments.

Tests must verify that:

- Snapshot identity is explicit.
- Capture timestamp is included.
- Generation information is preserved.
- Snapshot collection does not claim global transactional atomicity.

### 22.12 Bounded History

Generate metric snapshots for an extended simulated runtime.

Verify that memory consumption remains bounded by the configured history capacity.

### 22.13 Metrics Disabled

Disable optional detailed metrics.

Verify that:

    Trade Continuity Validation

    L2 Sequence Validation

    Recording Commit Tracking

    Recovery State

continue operating.

Disabling diagnostic timing must not weaken correctness.

### 22.14 Monitoring Overhead

Benchmark:

    Metrics Disabled

    Essential Metrics Enabled

    Detailed Timing Enabled

Measure the impact on:

    Throughput

    Processing Latency

    CPU

    Allocations

    Contention

### 22.15 Metrics Invariant

    Metrics Observe
    Processing Behavior.

    They Must Not
    Determine Canonical
    Processing Correctness.

---

## 23. HealthEvaluator Testing

The HealthEvaluator interprets authoritative component state and applicable diagnostic metrics.

Tests must verify that health reflects required operational guarantees rather than superficial activity.

### 23.1 Testing Objectives

Verify:

- Health classification.
- Multidimensional health.
- Dependency propagation.
- Optional capability handling.
- Required capability handling.
- Stale observations.
- Recovery transitions.
- Health hysteresis.
- Generation safety.
- Structured health reasons.

### 23.2 Health Statuses

The shared health model includes:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

Tests must preserve these meanings.

### 23.3 Healthy Trade Stream

Input:

    Transport:
        Healthy

    Subscription:
        Active

    Trade Continuity:
        Verified

    Publication:
        Healthy

Expected:

    Trade Stream:
        Healthy

provided all other required capabilities are satisfied.

### 23.4 Active But Incomplete Trade Stream

Input:

    Transport:
        Healthy

    Throughput:
        High

    Trade Continuity:
        Incomplete

Expected:

    Overall Health:
        Degraded Or Unhealthy
        According To Policy

High throughput must not override a confirmed integrity failure.

### 23.5 Invalid L2 Book

Input:

    Transport:
        Connected

    BookStore:
        Invalid

Expected:

    L2 Book Health:
        Unhealthy

when synchronized book state is required.

A nonempty BookStore must not override the invalid synchronization state.

### 23.6 Optional Recording

Input:

    Source:
        Healthy

    Recording:
        Disabled

    Recording Required:
        No

Expected:

    Source Health:
        Unaffected

### 23.7 Required Recording

Input:

    Source:
        Healthy

    Recording:
        Unavailable

    Recording Required:
        Yes

Expected:

    Required Durability Health:
        Impaired

The precise overall severity follows the effective policy.

### 23.8 Derived Source Failure

Input:

    Merge:
        Three Required Sources

    Two Sources:
        Healthy

    Third Source:
        Unavailable

Expected:

    Complete Participation:
        Not Healthy

The derived stream may continue producing output under a degraded-participation policy.

### 23.9 Conversion Failure Isolation

Input:

    ETHBTC Source:
        Healthy

    BTCUSDT Reference:
        Stale

Expected:

    Original ETHBTC Source:
        Healthy

    Converted ETHUSDT Derived:
        Degraded According To Policy

The conversion failure must not propagate backward into unrelated source integrity.

### 23.10 Stale Observations

Stop updating a required monitoring observation.

Advance the controlled clock beyond the freshness limit.

Expected:

    Stale Observation Detected

The HealthEvaluator must not continue presenting unsupported current health evidence.

### 23.11 Confirmed Failure With Stale Metrics

Input:

    Authoritative Book State:
        Invalid

    Metrics Snapshot:
        Stale

Expected:

    Book Health:
        Not Healthy

The confirmed integrity failure must not disappear because diagnostic metrics are stale.

### 23.12 Recovery Transition

Simulate:

    Healthy

    Failure Detected

    Recovery Started

    Transport Restored

    Integrity Restored

    Recovery Completed

The HealthEvaluator must not return to Healthy before applicable authoritative recovery conditions are satisfied.

### 23.13 Hysteresis

For threshold-based degradation, simulate measurements oscillating near the configured threshold.

Verify that hysteresis prevents excessive status flapping.

Confirmed integrity failures must bypass ordinary diagnostic stabilization.

### 23.14 Health Generation Safety

Simulate a late health observation from an obsolete stream generation.

Expected:

    Obsolete Observation Rejected

The current generation's health must remain authoritative.

### 23.15 Health Reason Propagation

A derived-stream health reason may reference an upstream root cause.

Tests must verify that dependency chains preserve:

    Original Affected Component

    Required Capability

    Root Failure Category

without generating unbounded duplicate diagnostic records.

### 23.16 Health Invariant

    Healthy Status
    Must Reflect
    Applicable Authoritative
    Operational Guarantees.

    It Must Not Be
    Inferred From
    Throughput Alone.

---

## 24. Prometheus Exporter and Monitoring Integration Testing

The optional Prometheus exporter exposes existing Metrics Registry measurements through a read-only HTTP endpoint.

Prometheus and Grafana remain external applications.

Exporter tests must verify exposition compatibility, resource bounds, failure isolation, and independence from the native monitoring system.

### 24.1 Testing Objectives

Verify:

- Disabled exporter behavior.
- Exporter startup.
- Listener configuration.
- Metric exposition.
- Counter semantics.
- Gauge semantics.
- Histogram semantics.
- Metric naming.
- Metric units.
- Metric labels.
- Series cardinality.
- Missing measurements.
- Removed-stream cleanup.
- Resource limits.
- Failure isolation.
- Exporter shutdown.

### 24.2 Disabled Exporter

Configure:

    metrics.prometheus.enabled = false

Expected:

    No Exporter Listener

    No Exporter-Specific Background Work

    Native Metrics Operational

    HealthEvaluator Operational

    Ratatui Monitoring Operational

### 24.3 Exporter Startup

Enable the exporter on a test loopback listener.

Verify that:

- The configured HTTP endpoint becomes available.
- The exporter reads existing metric state.
- No independent event counters are created.
- No canonical processing state is modified.

### 24.4 Metric Naming

Exported metric names must follow the established MarketForge namespace.

Examples:

    marketforge_trade_accepted_total

    marketforge_l2_sequence_gaps_total

    marketforge_recording_pending_events

The exporter must preserve metric meaning.

### 24.5 Duration Units

Internal durations may use nanoseconds.

Prometheus exposition must use seconds for duration measurements.

Example:

    Internal:
        250000000 ns

    Exported:
        0.25 seconds

The conversion must be tested explicitly.

### 24.6 Counter Semantics

Prometheus counters are cumulative within their exposed lifecycle.

Tests must verify correct behavior across:

    Ordinary Counter Increments

    Process Restarts

    Metric Generation Changes

    Removed Streams

### 24.7 Histogram Exposition

Verify:

- Stable bucket boundaries.
- Correct observation counts.
- Correct cumulative bucket counts.
- Correct sum units.
- Compatible aggregation semantics.

A histogram must not be serialized as an arbitrary percentile table while claiming standard histogram semantics.

### 24.8 Metric Labels

Allowed labels may include bounded dimensions such as:

    exchange

    market_family

    channel

    stream_id

    failure_category

Test that unique trade IDs, event cursors, arbitrary error messages, and recovery-operation identifiers cannot create unbounded metric series.

### 24.9 Removed Streams

Remove a registered source stream.

Verify that its exporter-specific metric series are eventually removed according to the established lifecycle.

The exporter must not retain unlimited historical series for removed streams.

### 24.10 Missing Measurements

An unavailable metric must not be exported as fabricated zero.

Tests must verify the configured omission or availability-indicator policy.

### 24.11 Health Export

If health status is exported numerically, verify that the mapping distinguishes:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

The exporter must use the HealthEvaluator's existing assessment.

### 24.12 Scrape Resource Limits

Test:

    Maximum Concurrent Requests

    Maximum Response Bytes

    Request Timeout

    Serialization Failure

    Slow HTTP Client

The exporter must remain bounded.

### 24.13 Exporter Failure Isolation

Simulate exporter failure while market-data processing continues.

Expected:

    Exporter:
        Unavailable

    Metrics Registry:
        Operational

    HealthEvaluator:
        Operational

    Canonical Processing:
        Unaffected

### 24.14 No External Monitoring Dependency

Ordinary Rust test suites must not require running:

    Prometheus Server

    Grafana

The exporter can be tested using a local HTTP client.

### 24.15 Prometheus Invariant

    External Metrics Export
    Must Remain
    An Optional Consumer
    Of Existing Runtime
    Measurements.

---

## 25. Runtime Control API Testing

The Runtime Control API provides the authoritative external management interface for MarketForge Live.

Tests must verify that configuration mutations, stream management, operational queries, and asynchronous commands preserve the contracts established in Document 13.

### 25.1 Testing Objectives

Verify:

- Unix control socket communication.
- Protocol framing.
- Protocol versioning.
- Request-response correlation.
- Multiple management clients.
- Configuration revision enforcement.
- Source-stream management.
- Derived-stream management.
- Recording and retention commands.
- Recovery commands.
- Monitoring queries.
- Authorization.
- Asynchronous operation tracking.
- Client disconnection.
- Control-plane failure isolation.

### 25.2 Control Protocol Fixtures

Maintain representative serialized fixtures for:

    GetRuntimeStatus

    ListSourceStreams

    RegisterSourceStream

    ActivateSourceStream

    DeactivateSourceStream

    RemoveSourceStream

    CreateDerivedStream

    UpdateDerivedStream

    EnableRecording

    GetStreamHealth

    GetRuntimeMetrics

    ApplyConfiguration

    GetOperation

These fixtures must use the final wire protocol rather than internal Rust memory representations.

### 25.3 Framing Tests

The initial control protocol uses length-prefixed JSON.

Tests must cover:

    Complete Frames

    Partial Length Prefixes

    Partial Payloads

    Multiple Frames Per Read

    Oversized Frames

    Invalid JSON

    Connection Closed Mid-Frame

The decoder must validate the announced length before allocating the complete payload buffer.

### 25.4 Protocol Versioning

Verify that:

- Supported versions are accepted.
- Unsupported versions are rejected.
- Required fields are validated.
- Optional fields follow compatibility rules.
- Unknown mutating commands fail explicitly.

A protocol-version mismatch must not modify runtime state.

### 25.5 Request Correlation

Send several concurrent requests.

Allow their responses to complete out of order.

Verify that each response is associated with its original:

    ClientSessionId

    RequestId

Request identity and asynchronous operation identity must remain separate.

### 25.6 Concurrent Management Clients

Run at least two independent control clients.

Example:

    Ratatui Client

    Python Client

Both attempt configuration mutations using the same expected revision.

Expected:

    One Accepted Revision

    One Revision Conflict

No silent lost update is permitted.

### 25.7 Source-Stream Commands

Test:

    Register

    Activate

    Deactivate

    Remove

    Inspect

    Reconfigure

Commands must use the Stream Registry.

The Control Server must not bypass stream identity, demand ownership, or native subscription validation.

### 25.8 Derived-Stream Commands

Test:

    Create

    Activate

    Update

    Deactivate

    Remove

The tests must verify dependency validation and appropriate derived-generation transitions.

### 25.9 Recording Commands

Test:

    EnableRecording

    DisableRecording

    RequestRecordingFlush

    GetRecordingCoverage

Recording commands must preserve admission, commitment, and durability boundaries.

### 25.10 Retention Commands

Test:

    GetRetentionCoverage

    RequestRetentionProtection

    ReleaseRetentionProtection

Retention reservations must preserve ownership and resource limits.

### 25.11 Recovery Commands

Test supported manual recovery requests.

Verify:

- Target identity.
- Authorization.
- Capability validation.
- Existing recovery detection.
- Generation fencing.
- Correct coordinator ownership.

Manual requests must not create competing authoritative recovery workflows.

### 25.12 Monitoring Queries

Test:

    GetMetricsSnapshot

    GetHealthSnapshot

    GetConnectionMetrics

    GetStreamMetrics

    GetRecordingMetrics

    GetPrometheusExporterStatus

The API must preserve missing-measurement and snapshot-freshness semantics.

### 25.13 Authorization

Use simulated clients with different permissions.

Verify that read-only clients cannot:

    Modify Configuration

    Add Or Remove Streams

    Change Recording

    Restart Connections

    Trigger Recovery

    Release Another Client's Retention Protection

Authorization must be enforced by the server.

### 25.14 Asynchronous Operation Tracking

Simulate a stream activation that requires L2 synchronization.

Expected:

    Command Accepted

    Operation Applying

    Snapshot Received

    Book Synchronized

    Operation Completed

The operation must not become completed before its declared readiness condition.

### 25.15 Ambiguous Mutation Outcome

Simulate:

    Client Sends Mutation

    Server Accepts Mutation

    Connection Lost Before Response

The client must not assume that the mutation failed.

It must reconcile the operation through its idempotency contract or authoritative runtime state.

### 25.16 Control Server Restart

Restart the Control Server while the Live data plane continues operating.

Verify that:

- Source streams remain active.
- Canonical cursors remain valid.
- BookStore state is unaffected.
- Recording and retention continue.
- Management clients can reconnect.
- Authoritative configuration revisions remain unchanged.

### 25.17 Python Interoperability

The Python client must produce and consume the same wire fixtures as the Rust client.

Tests must verify:

    Exact Integer Serialization

    Decimal Strings

    Optional Fields

    Request IDs

    Operation IDs

    Error Categories

    Protocol Versions

The Rust and Python implementations must not interpret equivalent messages differently.

### 25.18 Control API Invariant

    Control Commands
    Must Preserve
    Authoritative Ownership,
    Configuration Revisions,
    And Runtime
    Generation Safety.

---

## 26. Ratatui Management Interface Testing

The Ratatui TUI is an independent client of the Runtime Control API.

Tests must verify presentation behavior without requiring live exchange connections.

The detailed interface contracts remain defined in Document 14.

### 26.1 Testing Objectives

Verify:

- Independent TUI startup.
- Control socket connection.
- Runtime snapshot loading.
- Navigation.
- Keyboard focus.
- Table selection.
- Filtering and sorting.
- Configuration forms.
- Destructive confirmations.
- Asynchronous operation progress.
- Control connection loss.
- Snapshot freshness.
- Terminal resize.
- Terminal cleanup.

### 26.2 Fake Control Server

The testing infrastructure should provide a deterministic Control Server fixture.

It must support:

    Valid Responses

    Delayed Responses

    Out-Of-Order Responses

    Structured Errors

    Revision Conflicts

    Operation Progress

    Control Disconnection

    Protocol Mismatch

    Stale Snapshots

The fake must implement the actual control wire contract.

### 26.3 Navigation Tests

Verify navigation between:

    Dashboard

    Streams

    Derived Streams

    Recording

    Settings

Switching views should preserve applicable local presentation state.

### 26.4 Stable Selection

Select a source stream.

Then change:

    Sort Order

    Filter

    Snapshot Contents

    Table Row Position

The selected StreamId must remain stable when the resource still exists.

A destructive command must never target another stream because its row index changed.

### 26.5 Keyboard Focus

Verify that typing into an input field does not activate global shortcuts.

For example:

    Numeric Input:
        User Types 1

Expected:

    Character Inserted

Not:

    Navigate To Dashboard

### 26.6 Configuration Drafts

Open Settings and modify a local draft.

Expected:

    Runtime Configuration:
        Unchanged

After validation:

    Runtime Configuration:
        Still Unchanged

Only an accepted configuration submission may modify desired state.

### 26.7 Revision Conflict

Simulate another client modifying the configuration while a Ratatui draft is open.

Expected:

    Revision Conflict Displayed

    User Draft Preserved

    No Silent Overwrite

### 26.8 Asynchronous Operations

Submit an L2 activation.

Simulate:

    Accepted

    Applying

    Synchronizing

    Completed

The TUI must show the applicable operation state.

It must not fabricate a completion percentage when no meaningful progress denominator exists.

### 26.9 Health Display

Test the shared health statuses:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

The TUI must preserve independent lifecycle and health dimensions.

### 26.10 Missing Measurements

Test rendering:

    Latency:
        Unavailable

    Recording:
        Disabled

    Sequence Gaps:
        0

These values must remain distinguishable.

### 26.11 Disconnection

Disconnect the fake Control Server.

Expected:

    TUI Remains Running

    Connection Marked Disconnected

    Cached Snapshots Marked Stale

    Local Drafts Preserved

    Unsafe Mutations Disabled

### 26.12 Reconnection

Restore the Control Server.

Verify:

- Protocol negotiation.
- Runtime-instance identity validation.
- Snapshot refresh.
- Pending operation reconciliation.
- Stale cache invalidation.
- Resumption of permitted actions.

### 26.13 Terminal Resize

Use Ratatui's test backend to render a range of terminal dimensions.

Verify that:

- Layout dimensions remain valid.
- Text does not cause out-of-bounds rendering.
- Small terminals display the defined fallback.
- Existing user input is preserved.
- Normal rendering resumes after resizing.

### 26.14 Terminal Cleanup

Verify that normal termination restores:

    Raw Mode

    Alternate Screen

    Cursor Visibility

    Mouse Capture

where applicable.

### 26.15 Existing TUI Regression

Changes to shared Ratatui widgets or the Tokyo Night theme must preserve existing historical processing, merging, and conversion interfaces.

### 26.16 Ratatui Invariant

    TUI Testing
    Must Verify
    Presentation And
    Control Behavior
    Without Requiring
    A Live Exchange.

---

## 27. Full Simulated Runtime Testing

A full simulated-runtime test exercises MarketForge Live from exchange transport through canonical processing and downstream consumers.

This is one of the primary integration gates before real exchange testing.

### 27.1 Testing Objectives

Verify that the complete runtime can:

- Load validated configuration.
- Initialize instrument contexts.
- Register source streams.
- Establish simulated connections.
- Activate subscriptions.
- Process native exchange messages.
- Validate trades and L2 updates.
- Coordinate recovery.
- Publish canonical events.
- Retain accepted events.
- Record selected events.
- Produce derived streams.
- Expose metrics and health.
- Accept control commands.
- Shut down safely.

### 27.2 Simulated Runtime Architecture

    Validated Test Configuration
                |
                v
         TestRuntimeBuilder
                |
                v
         MarketForge Live
                |
      +---------+----------+
      |                    |
      v                    v
    Fake WebSocket      Fake REST
      |                    |
      +---------+----------+
                |
                v
        Real Protocol Adapters
                |
                v
        Real Normalization
                |
                v
        Real Integrity Processing
                |
                v
          Canonical Events
                |
       +--------+----------+----------+
       |                   |          |
       v                   v          v
    Publisher           Retention   Recording
       |                   |          |
       v                   v          v
    Test Subscriber     Test Store  Test Database

    Derived Streams:
        Source Dependencies
                |
                v
          Merge / Conversion
                |
                v
          Derived Publication

    Control Plane:
        Control API
            |
            v
        Test Clients

This test must use actual production processing logic wherever practical.

### 27.3 Initial Integration Configuration

Use a bounded subset of the source configuration.

Recommended initial sources:

    Binance Spot BTCUSDT Trades

    Binance Spot BTCUSDT Depth

    Bybit Spot BTCUSDT Trades

    Bybit Spot BTCUSDT Depth

    OKX Spot BTCUSDT Trades

    OKX Spot BTCUSDT Depth

This provides three exchanges and both primary market-data channels.

The remaining exchange-market combinations are exercised through their dedicated protocol and connectivity tests before inclusion in broader integration suites.

### 27.4 Source Acquisition Test

Start the runtime with the initial source declarations.

Verify:

    Configuration Loaded

    Sources Registered

    Connections Established

    Subscriptions Accepted

    Processors Initialized

    Source Streams Operational

L2 streams must satisfy their synchronization requirements before reporting readiness.

### 27.5 Canonical Output Test

Feed native exchange fixtures through the simulated transport.

Collect canonical output.

Compare it with independently expected canonical fixtures.

Verify:

    Exact Decimal Values

    Instrument Identity

    Event Timestamps

    Native Provenance

    Stream Cursors

    Source Ordering

### 27.6 Trade Gap Scenario

Simulate a disconnection during trade acquisition.

After reconnection, provide overlapping REST recovery data.

Verify:

- Duplicate elimination.
- Recovery classification.
- Correct accepted execution set.
- Preserved source-event identity.
- Explicit unresolved coverage if recovery is incomplete.

### 27.7 L2 Gap Scenario

Establish a synchronized book.

Inject an invalid native sequence transition.

Expected:

    Book Invalidated

    Recovery Requested

    New Snapshot Applied

    New Book Generation Established

The runtime must not publish the invalid book as synchronized.

### 27.8 Multiple Subscribers

Connect several Unix socket subscribers.

Subscribe them to overlapping source and derived streams.

Verify:

- Correct stream filtering.
- Correct event framing.
- Independent queues.
- Stable event identities.
- No duplicate native acquisition.

### 27.9 Slow Subscriber

Stop reading from one subscriber.

Continue receiving market data.

Verify that the Publisher applies its bounded backpressure policy.

Unrelated subscribers must continue according to their own delivery guarantees.

### 27.10 Retention and Replay

Retain a known canonical event sequence.

Force a controlled Parquet spill.

Replay the interval.

Expected:

    Same Canonical Events

    Same Cursor Identities

    Same Logical Ordering

The RAM/Parquet storage boundary must remain invisible to the logical replay contract.

### 27.11 Recording Integration

Enable recording for one source stream.

Verify that:

- Recording starts at the established cursor boundary.
- Accepted events are batched.
- Transactions commit.
- Committed coverage is correct.
- Source acquisition is not restarted.

### 27.12 Recording Failure

Disconnect the test PostgreSQL instance or inject equivalent recording failures.

Continue simulated exchange acquisition.

Verify the applicable recording and retention guarantees.

The runtime must not claim durability for uncommitted events.

### 27.13 Cross-Exchange Merge

Activate:

    btc_spot_trades_merged

Provide deterministic trade events from Binance, Bybit, and OKX.

Verify:

- Source identity preservation.
- Declared event-time ordering.
- Deterministic tie-breaking.
- Correct derived output identity.
- Bounded buffering.

### 27.14 Mixed Event Merge

Activate:

    binance_btc_spot_combined

Feed both Binance trades and L2 updates.

Verify that the derived output retains:

    Trade Events

    Atomic L2 Message Groups

    Event-Kind Identity

    Source Provenance

### 27.15 Nested Derived Merge

Activate:

    btc_spot_trades_extended

Verify that:

- Upstream derived output is consumed correctly.
- Underlying native sources are not acquired twice.
- Dependency health propagates correctly.
- Output ordering respects upstream finalized frontiers.

### 27.16 Currency Conversion

Provision validated synthetic instrument metadata for:

    ETHBTC

    BTCUSDT

Activate:

    bybit_ethbtc_to_usdt

Verify:

- Valid path resolution.
- Reference acquisition.
- Event-time reference selection.
- Exact Decimal conversion.
- Original source preservation.
- Missing-reference behavior.

### 27.17 Configuration Mutation

While the simulated runtime is processing events:

    Enable Recording

    Add Source Stream

    Remove Source Stream

    Change Merge Configuration

    Update Metrics Interval

Verify that accepted changes follow revision and reconciliation rules.

Unrelated streams must remain unaffected.

### 27.18 Runtime Monitoring

Query:

    Runtime Metrics

    Stream Metrics

    Stream Health

    Recovery State

    Recording State

Verify that monitoring reflects authoritative runtime state without controlling market-data correctness.

### 27.19 Complete Shutdown

Request runtime shutdown.

Verify:

- No new acquisition admitted.
- Active operations resolved according to policy.
- Recording flushes within its declared deadline.
- Retention finalizes eligible segments.
- Subscribers close cleanly.
- Runtime tasks terminate.
- Resources are released.

### 27.20 End-to-End Invariant

    The Full Runtime
    Must Preserve
    Component Contracts
    When All Components
    Operate Together.

---

## 28. Long-Running Stability and Soak Testing

Long-running tests identify problems that short deterministic tests may not expose.

Relevant risks include memory growth, task leaks, resource exhaustion, repeated recovery failures, and performance degradation.

### 28.1 Testing Objectives

Verify:

- Stable memory usage.
- Stable task counts.
- Bounded queue behavior.
- Connection lifecycle stability.
- Repeated source activation.
- Repeated source removal.
- Reconnection recovery.
- Recording backlog recovery.
- Retention spill stability.
- Metrics cardinality limits.
- TUI and control-client lifecycle.
- Graceful shutdown after sustained operation.

### 28.2 Soak Test Configurations

Recommended workloads:

    Low Throughput

    Moderate Throughput

    High Throughput

    Bursty Traffic

    Repeated Connection Failure

    Sustained Recording Load

    Derived Merge Load

    Mixed Trade And L2 Load

The workload definitions must specify event rates, message sizes, and active stream counts.

### 28.3 Duration

Use different durations for different environments.

Examples:

    Local Development:
        Short Stability Run

    Continuous Integration:
        Bounded Soak Test

    Pre-Release:
        Extended Stability Run

The exact durations are deployment decisions.

Passing a brief stability test must not be represented as proof of indefinite reliable operation.

### 28.4 Memory Growth

Measure:

    Resident Memory

    Retention Memory

    Merge Buffer Memory

    Publisher Queue Memory

    Recording Queue Memory

    Metrics Registry Memory

Stable workloads should not produce unbounded resource growth.

### 28.5 Task Leaks

Repeatedly create and remove:

    Source Streams

    Derived Streams

    Connections

    Subscribers

    Recording Sessions

    Control Sessions

Verify that completed resource lifecycles release their tasks and associated allocations.

### 28.6 Connection Churn

Inject repeated connection failures.

Verify:

- Bounded reconnection attempts.
- No duplicate supervisors.
- No orphan reader or writer tasks.
- Correct subscription restoration.
- No obsolete-generation processing.
- Correct recovery metrics.

### 28.7 Derived Stream Churn

Repeatedly activate, deactivate, and reconfigure derived streams.

Verify:

- Dependency handles are released.
- Source acquisitions remain shared.
- Derived generations are fenced.
- Merge buffers are released.
- Conversion references do not accumulate indefinitely.

### 28.8 Recording Backlog

Periodically slow or disconnect PostgreSQL.

Verify:

- Recording backlog remains within its configured resource policy.
- Required durability failures remain observable.
- Recovery replays retained events when available.
- No uncontrolled batch accumulation occurs.

### 28.9 Retention Spill Stability

Force repeated RAM-to-Parquet spill cycles.

Verify:

- Stable manifest behavior.
- Correct cursor coverage.
- No temporary-file accumulation.
- Protected intervals preserved.
- Committed segment replay remains correct.

### 28.10 Monitoring Stability

Create and remove many metric identities through ordinary component lifecycles.

Verify that the Metrics Registry and optional Prometheus exporter do not retain unbounded stale series.

### 28.11 TUI Stability

Repeatedly connect and disconnect Ratatui or a deterministic equivalent.

Verify that the daemon continues operating and control sessions are cleaned up.

### 28.12 Soak Test Acceptance

Long-running tests must report:

    Test Duration

    Workload Description

    Peak Throughput

    Mean Throughput

    Peak Memory

    Final Memory

    Task Counts

    Failure Counts

    Recovery Results

    Unresolved Integrity Incidents

The report must distinguish expected injected failures from unexpected runtime failures.

### 28.13 Stability Invariant

    Sustained Operation
    Must Preserve
    Bounded Resources
    And Explicit
    Correctness Guarantees.

---

## 29. Performance Regression Testing

MarketForge Live must be benchmarked using reproducible workloads.

Performance testing must not replace correctness testing.

### 29.1 Benchmark Objectives

Measure:

- Native message ingestion throughput.
- Decode and normalization throughput.
- Trade acceptance latency.
- L2 update processing latency.
- Event Router overhead.
- Unix socket publication throughput.
- Merge processing throughput.
- Currency conversion throughput.
- PostgreSQL recording throughput.
- Parquet spill throughput.
- Metrics overhead.
- Control-plane isolation.
- Resource usage.

### 29.2 Benchmark Environment

Every benchmark report should identify:

    CPU Model

    Logical CPU Count

    Memory Capacity

    Operating System

    Rust Compiler Version

    Build Profile

    Runtime Worker Configuration

    Storage Device

    PostgreSQL Configuration

    Workload Definition

This information is necessary for meaningful comparisons.

### 29.3 Baseline Measurements

Establish baseline performance for:

    Canonical Processing Only

    Canonical Processing + Publisher

    Canonical Processing + Retention

    Canonical Processing + Recording

    Canonical Processing + Derived Streams

    Full Runtime

These measurements help identify the overhead introduced by each subsystem.

### 29.4 Source Throughput

Benchmark individual source-stream processors using representative captured exchange messages.

Measure:

    MessagesPerSecond

    CanonicalEventsPerSecond

    ProcessingDuration

    AllocationsPerEvent

### 29.5 Trade Processing

Benchmark:

    Decode

    Normalize

    Validate

    Deduplicate

    Accept

Trade processing benchmarks should include both ordinary and duplicate-heavy workloads.

### 29.6 L2 Processing

Benchmark:

    Snapshot Initialization

    Sequence Validation

    Book Mutation

    Level Change Emission

    Resynchronization

Include varying book depths and update batch sizes.

### 29.7 Publisher Performance

Benchmark:

    Single Subscriber

    Multiple Subscribers

    Small Frames

    Large Frames

    Mixed Event Types

    Slow Subscriber

Measure queue pressure and delivery latency separately from canonical processing latency.

### 29.8 Retention Performance

Benchmark:

    RAM Admission

    Parquet Serialization

    Spill Commit

    Replay Throughput

    Memory Reclamation

### 29.9 PostgreSQL Performance

Benchmark representative recording workloads using the proposed relational schema.

Include:

    Trade-Heavy

    L2-Heavy

    Mixed Trade/L2

    Small Batches

    Large Batches

    Concurrent Streams

The parent/child depth-event representation must be evaluated with realistic L2 message sizes.

### 29.10 Merge Performance

Benchmark:

    Two Sources

    Multiple Sources

    Equal Timestamps

    Out-Of-Order Arrivals

    Watermark Stalls

    Large Reorder Buffers

    Nested Derived Streams

### 29.11 Conversion Performance

Benchmark:

    Identity Conversion

    Direct Reference Conversion

    Multi-Hop Conversion

    Missing Reference

    Stale Reference

    High-Precision Decimal Arithmetic

### 29.12 Metrics Overhead

Compare:

    Metrics Disabled

    Essential Metrics Enabled

    Detailed Timing Enabled

The tests must quantify:

    Throughput Difference

    Processing Latency Difference

    CPU Difference

    Allocation Difference

### 29.13 Prometheus Exporter Overhead

Compare exporter disabled and enabled configurations.

Generate concurrent scrape requests.

Verify that exporter load does not materially interfere with canonical event processing.

The exporter must not force expensive per-event work.

### 29.14 Control API Load

Generate frequent runtime and metrics queries while market-data processing continues.

Measure:

    Control Query Latency

    Canonical Processing Impact

    Control Queue Memory

    Response Serialization Duration

### 29.15 Regression Thresholds

Performance regression thresholds must be based on established benchmark baselines.

Conceptually:

    New Throughput
        >=
    Minimum Accepted Baseline

    New Processing Latency
        <=
    Maximum Accepted Baseline

Exact thresholds should be calibrated to the benchmark environment and measurement variability.

### 29.16 Statistical Reliability

Benchmarks should use repeated runs.

Results should report variability rather than rely on one measurement.

Performance comparisons must account for environmental noise and workload changes.

### 29.17 Correctness Under Performance Load

A benchmark result is invalid as evidence of acceptable runtime behavior if the system silently violates required event integrity or delivery contracts.

For example:

    Higher Throughput
        +
    Silent Event Loss

is not a successful optimization.

### 29.18 Performance Invariant

    Performance Improvements
    Must Preserve
    Canonical Correctness,
    Integrity Guarantees,
    And Explicit
    Delivery Semantics.

---

## 30. Continuous Integration and Release Gates

MarketForge Live should use automated verification gates throughout implementation.

The testing system must provide actionable feedback without requiring external exchanges for ordinary development.

### 30.1 Test Categories

Recommended test groups:

    Unit

    HistoricalRegression

    Contract

    PropertyBased

    ProtocolFixtures

    ComponentIntegration

    SimulatedRuntime

    StorageIntegration

    ExternalSmoke

    Performance

    Soak

### 30.2 Pull Request Gate

For changes affecting shared or Live components, the ordinary pull request gate should include:

- Formatting and linting.
- Compilation.
- Applicable unit tests.
- Applicable historical regression tests.
- Contract tests.
- Protocol fixture tests.
- Relevant property-based tests.
- Bounded component integration tests.

The selection may depend on affected modules.

### 30.3 Historical Regression Gate

Changes to:

    canonical/

    normalize/

    book/

    instrument/

    storage/parquet/

must trigger applicable historical regression tests.

The Live implementation must not destabilize historical MarketForge processing.

### 30.4 Simulated Runtime Gate

Before merging substantial subsystem integrations, execute the relevant simulated-runtime scenarios.

Examples:

    Source Acquisition

    Connection Failure

    Trade Recovery

    L2 Resynchronization

    Publisher Backpressure

    Recording Failure

    Derived Stream Ordering

### 30.5 Storage Integration Gate

PostgreSQL and Parquet tests may run in dedicated integration environments.

They must remain reproducible and isolated from production data.

### 30.6 External Exchange Smoke Gate

Real exchange tests may run:

    Before Release

    On Explicit Request

    On A Controlled Schedule

They must not be required for every ordinary local test run.

External failures must be distinguishable from implementation regressions.

### 30.7 Performance Gate

Performance regression checks should compare representative benchmark workloads against accepted baselines.

A significant regression requires investigation before release.

### 30.8 Pre-Release Gate

Before a production-oriented release, execute:

    Full Deterministic Suite

    Historical Regression Suite

    Full Simulated Runtime Suite

    Storage Integration Suite

    External Exchange Smoke Tests

    Crash-Recovery Tests

    Performance Benchmarks

    Extended Stability Tests

### 30.9 Release Evidence

The release verification report should identify:

    Software Revision

    Configuration Schema Version

    Exchange Protocol Fixture Versions

    Rust Compiler Version

    Test Suite Results

    Known Unsupported Capabilities

    Known Unresolved Protocol Requirements

    Performance Results

    Open Integrity Risks

### 30.10 CI Invariant

    Release Readiness
    Must Be Based On
    Reproducible Evidence,
    Not Merely Successful
    Manual Demonstration.

---

## 31. Test Coverage and Traceability

Testing requirements originate from the architectural contracts in Documents 01–14.

Every critical invariant should have one or more corresponding tests.

### 31.1 Contract Traceability

The project should maintain a lightweight mapping between:

    Architectural Requirement

    Responsible Subsystem

    Test Category

    Test Identifier

    Verification Result

This mapping may be maintained through test naming conventions, documentation, or generated test reports.

### 31.2 Critical Invariant Categories

Required coverage includes:

    Canonical Correctness

    Exact Numeric Semantics

    Stream Identity

    Subscription Deduplication

    Native Continuity

    Book Synchronization

    Event Ordering

    Subscriber Isolation

    Retention Coverage

    Recording Durability

    Derived Transformation Correctness

    Configuration Revision Safety

    Recovery Generation Safety

    Resource Boundedness

### 31.3 Example Traceability

| Requirement | Test |
|---|---|
| Equivalent sources share one acquisition | Concurrent registry deduplication |
| Reconnect does not prove trade continuity | Trade recovery after reconnect |
| Invalid L2 sequence invalidates book | Sequence-gap fault injection |
| Full L2 initialization reconstructs state | BookStore reconstruction |
| Slow subscriber cannot block others | Publisher backpressure integration |
| Parquet spill preserves cursors | RAM-to-Parquet replay |
| Ambiguous DB commit remains idempotent | PostgreSQL retry test |
| Derived ordering respects watermarks | Controlled multi-source merge |
| Conversion preserves exact values | Decimal conversion fixtures |
| Configuration updates are revision-safe | Concurrent client revision conflict |
| Prometheus remains optional | Disabled-exporter integration |

### 31.4 Test Naming

Test names should describe the expected guarantee.

Prefer:

    reconnect_does_not_establish_trade_continuity

    stale_book_generation_cannot_accept_depth_update

    subscriber_disconnect_releases_transient_demand

    recording_commit_does_not_cross_unresolved_gap

Avoid names that describe only implementation details without establishing meaningful behavior.

### 31.5 Regression Fixtures

Every significant discovered production defect should produce a regression fixture or deterministic reproducer where practical.

Fixing a bug without preserving its triggering conditions risks reintroducing the same failure later.

### 31.6 Coverage Metrics

Code coverage may supplement testing.

However:

    High Line Coverage
        !=
    Verified Behavioral Correctness

Critical state transitions, failure paths, and invariants matter more than achieving an arbitrary coverage percentage.

### 31.7 Traceability Invariant

    Every Critical
    Architectural Guarantee
    Must Have
    Verifiable Test Evidence.

---

## 32. Final Acceptance Criteria

MarketForge Live is considered test-ready and validation-complete when the following requirements are satisfied for the intended release scope.

### 32.1 Foundational Correctness

- Canonical model tests pass.
- Exact Decimal normalization tests pass.
- Timestamp normalization tests pass.
- Instrument metadata tests pass.
- Historical regression tests pass.
- Shared BookStore tests pass.
- Configuration compilation tests pass.

### 32.2 Exchange Connectivity

- Required exchange protocol fixtures pass.
- Connection lifecycle tests pass.
- Heartbeat tests pass.
- Reconnection tests pass.
- Rate-limit coordination tests pass.
- Subscription acknowledgement tests pass.
- Shared connection pooling tests pass.
- Generation-fencing tests pass.

### 32.3 Trade Integrity

- Trade normalization tests pass.
- Duplicate detection tests pass.
- Native identity tests pass.
- Continuity verification tests pass.
- Gap detection tests pass.
- REST recovery tests pass.
- Partial recovery tests pass.
- Unrecoverable coverage tests pass.

### 32.4 L2 Integrity

- Snapshot initialization tests pass.
- Absolute level-update tests pass.
- Level deletion tests pass.
- Atomic depth-group tests pass.
- Native sequence tests pass.
- Snapshot bridging tests pass.
- Book invalidation tests pass.
- Resynchronization tests pass.
- Full initialization emission tests pass.
- Independent book reconstruction tests pass.

### 32.5 Distribution

- Event Router tests pass.
- Publisher admission tests pass.
- Unix socket framing tests pass.
- Subscriber filtering tests pass.
- Subscriber isolation tests pass.
- Backpressure tests pass.
- Cursor-aware replay tests pass where supported.
- Delivery semantics are verified.

### 32.6 Retention and Recording

- RAM retention tests pass.
- Cursor coverage tests pass.
- Protection reservation tests pass.
- Parquet spill tests pass.
- Manifest recovery tests pass.
- PostgreSQL batching tests pass.
- Idempotent retry tests pass.
- Committed frontier tests pass.
- Recording gap tests pass.
- Recording recovery tests pass.
- Crash-recovery tests pass.

### 32.7 Derived Streams

- Trade-only merge tests pass.
- Depth-only merge tests pass.
- Mixed trade/depth tests pass.
- Deterministic tie-breaking tests pass.
- Watermark tests pass.
- Late-event policy tests pass.
- Nested derived-stream tests pass.
- Conversion path tests pass.
- Reference eligibility tests pass.
- Exact Decimal conversion tests pass.
- Missing-reference tests pass.

### 32.8 Monitoring

- Metrics Registry tests pass.
- Counter and gauge tests pass.
- Histogram tests pass.
- HealthEvaluator tests pass.
- Dependency health tests pass.
- Stale snapshot tests pass.
- Optional Prometheus exporter tests pass.
- Monitoring overhead benchmarks pass.

### 32.9 Control API

- Control protocol framing tests pass.
- Serialization compatibility tests pass.
- Configuration revision tests pass.
- Concurrent client tests pass.
- Authorization tests pass.
- Asynchronous operation tests pass.
- Python interoperability tests pass.
- Control-plane failure isolation tests pass.

### 32.10 Ratatui

- Navigation tests pass.
- Keyboard focus tests pass.
- Table selection tests pass.
- Dialog tests pass.
- Configuration draft tests pass.
- Revision conflict tests pass.
- Snapshot cache tests pass.
- Disconnection tests pass.
- Terminal rendering tests pass.
- Historical TUI regression tests pass.

### 32.11 System Integration

- Full simulated runtime tests pass.
- Multi-source acquisition tests pass.
- Cross-exchange recovery tests pass.
- Publisher/recording/retention integration passes.
- Derived-stream integration passes.
- Runtime configuration mutation tests pass.
- Graceful shutdown tests pass.
- Crash and restart tests pass.

### 32.12 Performance and Stability

- Throughput baselines are established.
- Latency baselines are established.
- Memory usage is bounded under tested workloads.
- Subscriber queues remain bounded.
- Merge buffers remain bounded.
- Recording backlogs follow their configured policies.
- Retention storage respects configured budgets.
- No unresolved resource leaks remain in required test scenarios.
- Extended stability tests satisfy release criteria.

---

## 33. Final Validation Contract

The Testing and Validation subsystem provides the verification foundation for MarketForge Live.

It combines shared historical regression testing, deterministic simulated infrastructure, exchange protocol fixtures, state-machine verification, storage integration, end-to-end runtime testing, and sustained-load validation.

The complete testing architecture is:

    Existing MarketForge Historical Tests
                     |
                     v
            Shared Canonical Models
                     |
                     v
             Live Unit Tests
                     |
                     v
             Contract Tests
                     |
                     v
           Property-Based Tests
                     |
                     v
          Component Integration
                     |
                     v
        Full Simulated Runtime
                     |
                     v
         Storage Integration
                     |
                     v
        External Exchange Smoke
                     |
                     v
       Performance / Soak Tests
                     |
                     v
           Release Validation

The diagram represents increasing integration scope.

It does not require tests to execute serially.

### 33.1 Correctness Guarantee

The testing system must establish that:

    Canonical Events
    Preserve Their
    Defined Economic
    And Identity Semantics.

### 33.2 Integrity Guarantee

The testing system must establish that:

    Missing Or Invalid
    Market-Data Coverage
    Is Detected Where Possible,
    Explicitly Classified,
    And Never Silently
    Represented As Complete.

### 33.3 Recovery Guarantee

The testing system must establish that:

    Recovery Restores
    Only Guarantees
    Actually Revalidated
    By Their Authoritative
    Components.

### 33.4 Persistence Guarantee

The testing system must establish that:

    Durable Data
    Is Reported Durable
    Only After Its
    Defined Commitment
    Boundary Is Satisfied.

### 33.5 Performance Guarantee

The testing system must establish that:

    Resource Usage
    Remains Bounded
    Under The Tested
    Workloads And
    Failure Conditions.

### 33.6 Historical Compatibility Guarantee

The testing system must establish that:

    Live Development
    Does Not Silently
    Change Existing
    Historical MarketForge
    Domain Semantics.

### 33.7 Final Completion Rule

MarketForge Live reaches its validation-complete state when:

    Shared Historical Compatibility
                 +
    Deterministic Component Correctness
                 +
    Exchange Protocol Verification
                 +
    Recovery And Failure Testing
                 +
    Storage And Delivery Verification
                 +
    Derived Transformation Correctness
                 +
    End-To-End Runtime Integration
                 +
    Performance And Stability Evidence
                 |
                 v
       Verified Release Readiness

A successful release must identify any remaining unsupported capabilities, unverified exchange behaviors, or known coverage limitations.

The absence of observed failures alone is not sufficient evidence of correctness.

---