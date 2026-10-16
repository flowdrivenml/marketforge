# MarketForge Live — Domain Models and Contracts

## 1. Purpose

Define the foundational domain models, identities, event envelopes, lifecycle states, and interface contracts shared across MarketForge Live subsystems.

These models establish the semantic boundaries between acquisition, normalization, integrity validation, recovery, derived streams, publication, recording, retention, and runtime management.

**Primary objective:** Ensure all subsystems operate on consistent, strongly typed representations without depending on one another's implementation details.

The contracts must support:

- Continuous, high-frequency market-data processing.
- Reuse of existing MarketForge canonical models.
- Multiple consumers sharing unique source acquisitions.
- Stable event identity and ordering.
- Explicit integrity and recovery semantics.
- Runtime configuration changes.
- Cursor-based retention and replay.
- Deterministic testing.
- Future Bootstrap integration.

This document defines logical contracts and proposed Rust representations. Exact field layouts, serialization formats, and implementation details may be refined during implementation, provided the established invariants remain intact.

## 2. Domain Model Organization

Domain models must remain independent of concrete infrastructure implementations.

Proposed structure:

```text
engine/src/live/model/
├── mod.rs
├── identity.rs
├── stream.rs
├── cursor.rs
├── envelope.rs
├── generation.rs
├── lifecycle.rs
├── demand.rs
├── delivery.rs
├── recovery.rs
└── error.rs
```

Existing shared models remain authoritative:

```text
engine/src/
├── canonical/
│   ├── event.rs
│   ├── envelope.rs
│   ├── trade.rs
│   ├── l2.rs
│   └── sequence.rs
│
├── instrument/
│   ├── identity.rs
│   ├── catalog.rs
│   └── currency.rs
│
├── book/
│   ├── store.rs
│   ├── sequence.rs
│   ├── snapshot.rs
│   └── validation.rs
│
└── normalize/
    ├── decimal.rs
    ├── timestamp.rs
    ├── quantity.rs
    └── side.rs
```

Live-specific models must extend existing canonical semantics rather than duplicate them.

**Boundary rule:** Canonical market-data structures describe market events. Live envelopes describe acquisition identity, runtime provenance, delivery position, and processing metadata.

## 3. Identity Model

MarketForge Live requires several independent identity domains.

They must not be conflated.

### 3.1 Exchange Identity

```rust
pub enum ExchangeId {
    Binance,
    Bybit,
    Okx,
    Bitget,
    GateIo,
}
```

Exchange identity represents the venue, not its API family or connection pool.

### 3.2 Market Identity

```rust
pub enum MarketKind {
    Spot,
    Linear,
    Inverse,
}
```

Market categories identify the relevant instrument/API family.

Specific contract semantics remain defined by the existing instrument catalog.

### 3.3 Instrument Identity

Reuse the existing MarketForge instrument identity model.

Instrument identifiers must resolve to authoritative metadata containing the information required for canonical normalization.

Exchange-native symbols are transport identifiers and must not replace canonical instrument identities.

For example:

```text
Exchange symbol: BTCUSDT
Canonical instrument: resolved catalog identity
Market category: Linear
```

Instrument metadata must be resolved before stream activation and made available to the processing path without per-event database queries.

### 3.4 Channel Identity

A channel identifies the semantic market-data feed.

```rust
pub enum ChannelKind {
    Trades,
    AggregateTrades,
    L2,
    Liquidations,
}
```

Channel capabilities may differ across exchanges.

Unsupported combinations must be rejected during configuration validation.

Acquisition parameters are part of the channel specification.

```rust
pub struct ChannelSpec {
    pub kind: ChannelKind,
    pub depth: Option<u32>,
    pub interval_ms: Option<u32>,
}
```

This is illustrative. The final representation must support validated exchange-specific parameters without treating arbitrary configuration strings as canonical identity.

### 3.5 SourceStreamKey

A source stream represents one unique logical exchange acquisition.

```rust
pub struct SourceStreamKey {
    pub exchange: ExchangeId,
    pub market: MarketKind,
    pub instrument: InstrumentId,
    pub channel: ChannelSpec,
}
```

Required properties:

- Hashable and comparable.
- Stable semantic equality.
- Independent of connection assignment.
- Independent of subscribers and recording configuration.
- Includes all acquisition parameters affecting feed semantics.
- Compatible with configuration serialization.

Two equivalent keys must resolve to one logical source acquisition.

Different acquisition semantics must produce different keys.

### 3.6 StreamId

`StreamId` is an opaque identifier assigned to a registered logical stream.

```rust
pub struct StreamId(pub String);
```

The representation is provisional.

`StreamId` must not be used as a substitute for `SourceStreamKey` when determining acquisition equivalence.

A stream identifier may represent either a source stream or a derived stream.

Internal APIs should distinguish these categories where required.

### 3.7 DerivedStreamId

```rust
pub struct DerivedStreamId(pub String);
```

Derived streams have independent identities and lifecycle state.

Their identities must incorporate or reference a validated transformation specification.

Two semantically equivalent derived-stream requests may reuse an existing processor.

A source stream and a derived stream must never accidentally share the same registry identity.

---

## 4. Stream Cursors and Event Positioning

MarketForge Live requires stable event positions independent of exchange timestamps, native exchange sequences, and physical storage locations.

Cursors support:

- Per-stream event ordering.
- Consumer continuity verification.
- RAM-to-Parquet retention transitions.
- Cursor-based replay.
- Subscriber reconnection.
- Future historical/live bootstrap handoff.

### 4.1 StreamCursor

```rust
pub struct StreamCursor {
    pub stream_id: StreamId,
    pub log_epoch: LogEpoch,
    pub offset: u64,
}
```

A cursor identifies a position within a MarketForge-managed event sequence.

The tuple `(stream_id, log_epoch, offset)` must uniquely identify a retained event position.

**Cursor invariants:**

- Offsets increase monotonically within a log epoch.
- An offset is never reused for a different event within the same epoch.
- Cursor ordering is defined only within a compatible stream and log epoch.
- Cursor assignment must preserve accepted source-event order.
- Moving an event between RAM and Parquet must not change its cursor.
- Retention deletion must not permit cursor reuse.
- A cursor must not imply historical completeness or exchange sequence continuity.

The cursor identifies MarketForge's accepted event order, not necessarily the exchange's original event sequence.

### 4.2 Cursor Assignment

Each source stream has one authoritative cursor allocator.

```text
Validated Canonical Event
           |
           v
    Source Stream Owner
           |
           v
      Cursor Assignment
           |
           v
       Event Router
           |
     +-----+---------+
     |               |
     v               v
 Publication      Retention
```

Cursor assignment occurs after canonical validation and any required stream-integrity checks.

Rejected messages do not consume canonical event offsets.

An event accepted for publication may still fail a downstream durability requirement. Such failures must be represented explicitly.

### 4.3 Cursor Continuity

For consecutive accepted events within one log epoch:

```text
offset = 100
offset = 101
offset = 102
offset = 103
```

A consumer receiving:

```text
100
101
103
```

detects a missing MarketForge delivery position.

This does not necessarily indicate an exchange-side sequence gap.

The following conditions are distinct:

| Condition | Meaning |
|---|---|
| Exchange sequence gap | Missing or inconsistent exchange-native updates |
| MarketForge cursor gap | Missing accepted events in a consumer's delivery sequence |
| Retention gap | Requested cursor interval is unavailable |
| Historical coverage gap | Required market-data history is incomplete |

These conditions must have separate error categories and metrics.

### 4.4 Cursor Persistence

Cursor allocation and persistence must have an explicit recovery contract.

A process restart must never cause previously committed cursor identities to be reused for different events.

The retention subsystem must track committed cursor ranges.

If the next safe offset cannot be reconstructed, the system must establish a new log epoch rather than continue an ambiguous sequence.

Exact offset checkpointing and crash-recovery mechanics remain implementation decisions for the retention specification.

---

## 5. Generation and Epoch Model

Different runtime lifecycles require independent generation identifiers.

A single global generation counter is insufficient.

### 5.1 Generation Types

```rust
pub struct ConnectionGeneration(pub u64);

pub struct SubscriptionGeneration(pub u64);

pub struct BookGeneration(pub u64);

pub struct LogEpoch(pub u64);

pub struct DerivedGeneration(pub u64);
```

These types represent distinct domains.

| Generation | Responsibility |
|---|---|
| `ConnectionGeneration` | Identifies one physical connection attempt |
| `SubscriptionGeneration` | Identifies one subscription activation lifecycle |
| `BookGeneration` | Identifies one synchronized order-book state |
| `LogEpoch` | Identifies one unambiguous cursor namespace |
| `DerivedGeneration` | Identifies one derived-stream semantic generation |

Configuration revisions are separate from all of these identifiers.

### 5.2 Connection Generation

Every physical connection attempt receives a new generation.

```text
Connection Generation 15
        |
        v
     Disconnect
        |
        v
Connection Generation 16
```

Delayed asynchronous operations must carry the generation that initiated them.

Results belonging to obsolete generations must not mutate current connection or subscription state.

### 5.3 Subscription Generation

A subscription may be recreated while its physical connection remains active.

Subscription generations distinguish:

- Initial subscription.
- Unsubscription and later resubscription.
- Subscription replacement.
- Rejected or obsolete acknowledgements.

A connection generation change may require new subscription generations, but the identifiers remain conceptually independent.

### 5.4 Book Generation

An order-book generation represents one authoritative synchronization lifecycle.

```text
Book Generation 7
    Snapshot sequence: 1000
    Updates: 1001 ... 1500

Sequence gap detected

Book Generation 7
    INVALID

Recovery completed

Book Generation 8
    Snapshot sequence: 2000
    Updates: 2001 ...
```

A new authoritative synchronization establishes a new book generation.

Book generation changes must be visible to downstream consumers requiring book-state continuity.

A valid current book does not imply that missing transitions from earlier generations were recovered.

### 5.5 Log Epoch

A log epoch defines the namespace in which MarketForge cursor offsets are unique and ordered.

Unlike connection generations, log epochs should not automatically change after every WebSocket reconnection.

A reconnect may interrupt exchange continuity while the retained event log remains structurally continuous.

Log epochs change when the runtime cannot safely continue the previous cursor namespace or when an explicitly defined log lifecycle transition requires it.

### 5.6 Derived Generation

Derived streams require their own generations because transformations may change without modifying source streams.

Examples:

- Changing merge ordering policy.
- Replacing merge dependencies.
- Changing conversion semantics.
- Reinitializing transformation state.
- Applying incompatible configuration changes.

A derived generation transition must identify the semantic boundary.

Operational settings that do not affect output semantics should not create unnecessary generations.

---

## 6. Canonical Live Event Envelope

Canonical market-data models remain authoritative.

Live-specific metadata must be represented through an additional envelope rather than modifying canonical trade and L2 semantics unnecessarily.

### 6.1 LiveEvent

Conceptual representation:

```rust
pub struct LiveEvent {
    pub stream_id: StreamId,
    pub cursor: StreamCursor,

    pub payload: CanonicalEvent,

    pub provenance: EventProvenance,
    pub timing: EventTiming,
}
```

The exact implementation should minimize per-event allocation and unnecessary metadata duplication.

The envelope must support all initially implemented canonical event categories.

### 6.2 EventProvenance

```rust
pub struct EventProvenance {
    pub source_stream: StreamId,

    pub connection_generation: ConnectionGeneration,
    pub subscription_generation: SubscriptionGeneration,

    pub native_sequence: Option<NativeSequence>,
    pub native_trade_id: Option<NativeTradeId>,

    pub book_generation: Option<BookGeneration>,
}
```

This is a logical model. Native identifiers may already exist inside canonical payloads and should not be duplicated physically without justification.

**Provenance invariants:**

- Native identifiers preserve exchange semantics.
- Exchange sequence identifiers are not interchangeable with MarketForge cursors.
- Connection generations identify transport provenance, not historical completeness.
- Book generations apply only to relevant stateful market-data streams.
- Missing native identifiers must remain explicitly absent.

### 6.3 EventTiming

```rust
pub struct EventTiming {
    pub exchange_timestamp_ns: Option<i64>,
    pub received_timestamp_ns: i64,
}
```

Additional processing timestamps may be captured by instrumentation without becoming mandatory serialized event fields.

Timing domains must remain explicit.

| Timestamp | Meaning |
|---|---|
| Exchange event time | Timestamp supplied by the exchange |
| Local receipt time | Time MarketForge observes the incoming message |
| Normalization time | Processing measurement |
| Publication time | Delivery-stage measurement |

Internal latency measurements must use monotonic clocks.

Wall-clock timestamps may be retained for provenance and event-time ordering.

Exchange timestamps must not be assumed perfectly synchronized across venues.

### 6.4 Canonical Payload

Reuse existing canonical structures for:

- Trade events.
- Atomic L2 updates.
- Authoritative L2 snapshots.
- Existing supported event metadata.

The live layer must not introduce a second independent representation of canonical price, quantity, side, or instrument semantics.

### 6.5 Event Immutability

Once a canonical event has been accepted and assigned a cursor, its semantic payload must remain immutable.

Downstream transformations produce new derived events rather than modifying source events in place.

This applies to:

- Currency conversion.
- Cross-exchange merging.
- Derived-stream transformations.
- Publication serialization.
- Parquet retention.

Original canonical values and provenance must remain recoverable.

---

## 7. Stream Lifecycle Models

MarketForge Live must represent connection, subscription, source-stream, and data-integrity lifecycles independently.

A stream may be connected and subscribed while its order book remains invalid or its historical coverage is incomplete.

### 7.1 SourceStreamState

```rust
pub struct SourceStreamState {
    pub lifecycle: StreamLifecycle,
    pub connection: ConnectionState,
    pub subscription: SubscriptionState,
    pub integrity: IntegrityState,
    pub recovery: RecoveryState,
}
```

Each field represents an independent operational dimension.

### 7.2 StreamLifecycle

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

The lifecycle represents resource ownership and execution state, not market-data validity.

```text
REGISTERED
     |
     v
  STARTING
     |
     v
   ACTIVE
     |
     +---- No remaining demand ----> IDLE
     |                                |
     |                                +---- New demand ---> ACTIVE
     |                                |
     |                                v
     |                             STOPPING
     |                                |
     |                                v
     |                             STOPPED
     |
     +---- Fatal failure ----------> FAILED
```

A recoverable exchange disconnection does not necessarily transition the logical stream to `Failed`.

The stream may remain active while its connection supervisor attempts recovery.

### 7.3 ConnectionState

```rust
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Failed,
}
```

Connection state belongs to the physical connection owner.

Multiple source streams may reference the same connection state.

A connected socket does not imply successful subscription or valid market data.

### 7.4 SubscriptionState

```rust
pub enum SubscriptionState {
    Pending,
    Subscribing,
    Active,
    Unsubscribing,
    Rejected,
    Failed,
}
```

Subscription activation must follow the exchange's acknowledgement semantics.

For protocols without explicit acknowledgements, activation criteria must be defined by the corresponding protocol strategy.

A subscription must not be marked active merely because a subscription request was written successfully.

### 7.5 IntegrityState

```rust
pub enum IntegrityState {
    Uninitialized,
    Synchronizing,
    Valid,
    Invalid {
        reason: IntegrityFailure,
    },
}
```

Integrity state represents whether the stream satisfies its configured data-validity requirements.

For L2 streams:

- `Uninitialized`: No authoritative state established.
- `Synchronizing`: Snapshot/update synchronization underway.
- `Valid`: Book state satisfies required continuity rules.
- `Invalid`: Required integrity conditions have been violated.

Trade streams may use different validity criteria and do not require order-book initialization.

Integrity state must not be inferred exclusively from connection status.

### 7.6 RecoveryState

```rust
pub enum RecoveryState {
    Idle,
    Pending,
    Recovering,
    WaitingForRetry,
    Exhausted,
}
```

Recovery state represents the current recovery workflow.

It must distinguish recoverable temporary failures from terminal conditions requiring intervention.

Recovery completion must not automatically imply complete historical coverage.

### 7.7 Historical Coverage State

Historical coverage is independent of current stream validity.

```rust
pub enum CoverageState {
    Unknown,
    Complete,
    Incomplete,
    Recovering,
    Unrecoverable,
}
```

Coverage status applies to a defined source and interval.

A stream may simultaneously report:

```text
Connection: ACTIVE
Integrity:  VALID
Recovery:   IDLE
Coverage:   INCOMPLETE
```

This is valid when current acquisition is healthy but an earlier interval remains unresolved.

The detailed coverage model belongs to the recovery and retention specifications.

---

## 8. Stream Demand and Ownership

Source acquisition must be shared across all consumers requiring equivalent exchange data.

The Stream Registry is the authoritative owner of source identity, demand registration, and acquisition lifecycle.

### 8.1 DemandOwner

```rust
pub enum DemandOwner {
    PersistentConfiguration,
    Recording,
    Subscriber(SubscriberId),
    DerivedStream(DerivedStreamId),
    RetentionSession(RetentionSessionId),
}
```

The final representation must uniquely identify individual demand registrations, including multiple registrations of the same owner category.

Demand ownership must not rely solely on manually incremented counters.

### 8.2 StreamHandle

```rust
pub struct StreamHandle {
    pub stream_id: StreamId,
    pub demand_id: DemandId,
}
```

A stream handle represents an active claim on a source stream.

Conceptual API:

```rust
let handle = registry
    .acquire(stream_key, demand_owner)
    .await?;
```

The registry must guarantee that concurrent equivalent requests do not create duplicate logical acquisitions.

### 8.3 Demand Lifecycle

```text
Demand Request
      |
      v
Canonical StreamKey
      |
      v
Stream Registry
      |
      +---- Existing Source ----> Attach Demand
      |
      +---- Missing Source -----> Register Source
                                      |
                                      v
                                Start Acquisition
```

Releasing demand:

```text
Demand Released
      |
      v
Registry Updates Ownership
      |
      +---- Remaining Demand ----> Keep Source Active
      |
      +---- No Demand -----------> Idle Grace Period
                                      |
                                      v
                                Stop Acquisition
```

Demand release must be idempotent.

Unexpected client disconnection must release transient subscriber demand without removing persistent acquisition requirements.

### 8.4 Persistent and Transient Demand

Persistent demand survives runtime restarts.

Examples:

- Configured acquisition.
- Enabled recording requirements.
- Persistently configured derived streams.

Transient demand exists only while its owning session remains active.

Examples:

- Connected external subscribers.
- Temporary retention reservations.
- Future bootstrap sessions.

Persistent and transient ownership must remain distinguishable.

### 8.5 Derived Dependencies

Derived streams acquire source or derived-stream handles through the same dependency-management mechanism.

```text
Source A --------+
                 |
Source B --------+----> Combined Stream
                 |            |
Source C --------+            v
                         External Consumers
```

Creating a derived stream must not create duplicate physical source acquisitions.

Dependency registration must reject cycles.

Removing a derived stream releases its dependencies without terminating sources required elsewhere.

---

## 9. Delivery Contracts

MarketForge Live must distinguish event production, routing acceptance, subscriber delivery, and durable persistence.

These are separate guarantees.

### 9.1 Delivery Stages

```text
Canonical Event Accepted
          |
          v
      Cursor Assigned
          |
          v
       Event Router
          |
    +-----+----------+-----------+
    |                |           |
    v                v           v
Publication       Recording   Retention
    |                |           |
    v                v           v
Socket Write     DB Commit   Segment Commit
    |
    v
Consumer Receive
    |
    v
Consumer Processing
```

A successful operation at one stage does not imply success at subsequent stages.

### 9.2 DeliveryStatus

```rust
pub enum DeliveryStatus {
    Accepted,
    Pending,
    Delivered,
    Failed {
        reason: DeliveryFailure,
    },
}
```

The meaning of `Delivered` must be defined by the specific destination.

For Unix sockets, it may mean successful transport write.

For PostgreSQL, it may mean transaction commit.

For retention, it may mean successful commitment under the configured durability policy.

The status must not imply consumer processing acknowledgement unless such acknowledgement was explicitly received.

### 9.3 DeliveryPolicy

```rust
pub enum DeliveryPolicy {
    BestEffort,
    Required,
    Durable,
}
```

These represent logical requirements, not complete implementations.

**BestEffort**

The destination may be disconnected or rejected when it cannot keep up.

Failures must remain observable.

**Required**

The system must not silently discard events required by the destination.

Failure to accept events must produce an explicit violation of the associated guarantee.

**Durable**

Acceptance requires the event to satisfy a defined persistence commitment.

The exact durability boundary must be specified by the storage implementation.

### 9.4 Backpressure

Every delivery destination must declare:

- Maximum pending events or bytes.
- Queue ownership.
- Overflow behavior.
- Failure propagation.
- Ordering guarantees.
- Shutdown and cancellation behavior.

```rust
pub enum BackpressureAction {
    Wait,
    DisconnectConsumer,
    FailDelivery,
    FailRetentionSession,
}
```

The selected action depends on the destination and its delivery policy.

A slow Unix-socket subscriber must not block unrelated subscribers.

A required retention failure must not be silently converted into best-effort delivery.

### 9.5 Ordering Guarantees

Source-stream delivery preserves accepted source-event order.

Derived-stream delivery preserves the ordering semantics of its transformation.

Publication must not reorder events within one logical stream unless the stream's declared transformation explicitly permits it.

Cross-stream global ordering is not guaranteed by the event router.

Combined-stream ordering is owned by the corresponding merger.

---
## 10. Error Contracts

MarketForge Live must use structured, typed errors with explicit classification, ownership, severity, and recovery semantics.

Errors must not be classified through string matching or inferred exclusively from transport-library error types.

### 10.1 Error Classification

```rust
pub enum FailureClass {
    Transport,
    Timeout,
    RateLimited,
    Configuration,
    Protocol,
    Normalization,
    Integrity,
    Recovery,
    ResourceExhaustion,
    Persistence,
    Publication,
    Internal,
}
```

Failure classification identifies the affected subsystem or failure category.

It does not independently determine whether an operation should be retried.

### 10.2 Error Severity

```rust
pub enum FailureSeverity {
    Informational,
    Warning,
    Degraded,
    Fatal,
}
```

Severity must be evaluated relative to the affected resource.

For example, a fatal failure of one source stream does not necessarily imply a fatal failure of the entire MarketForge process.

### 10.3 Recovery Disposition

```rust
pub enum RecoveryDisposition {
    Retry,
    RetryAfter,
    Resynchronize,
    Reconfigure,
    DisableResource,
    Escalate,
}
```

The recovery disposition is determined by the responsible subsystem using the failure classification, exchange protocol, runtime state, and configured policies.

Examples:

| Failure | Recovery disposition |
|---|---|
| Network timeout | Retry |
| Connection reset | Retry |
| Temporary exchange outage | Retry with backoff |
| HTTP 429 | Retry after cooldown |
| Invalid subscription parameters | Reconfigure |
| Unsupported channel | Disable affected resource |
| Authentication failure | Reconfigure |
| Malformed required market-data field | Evaluate integrity and recovery |
| L2 sequence gap | Resynchronize |
| Repeated synchronization failure | Escalate and backoff |
| Retention capacity exhaustion | Fail affected retention guarantee |

### 10.4 LiveError

Conceptual representation:

```rust
pub struct LiveError {
    pub class: FailureClass,
    pub severity: FailureSeverity,
    pub disposition: RecoveryDisposition,
    pub context: FailureContext,
}
```

```rust
pub struct FailureContext {
    pub stream_id: Option<StreamId>,
    pub connection_id: Option<ConnectionId>,
    pub operation: OperationId,
}
```

The concrete implementation should preserve the underlying error source and diagnostic context.

Error types must remain serializable into stable control-protocol responses without exposing implementation-specific internal structures.

### 10.5 Error Invariants

- Errors must identify the narrowest affected resource.
- Retryability must not be inferred from severity alone.
- Fatal stream errors must not automatically terminate unrelated streams.
- Integrity violations must not be silently converted into warnings.
- Recovery attempts must preserve the original failure context.
- Repeated failures must remain observable after successful recovery.
- Error reporting must not block the high-frequency processing path.

---

## 11. Operational Event Contracts

Operational events represent meaningful runtime transitions and incidents.

They are distinct from canonical market-data events and must never enter the canonical event sequence.

### 11.1 OperationalEvent

```rust
pub enum OperationalEvent {
    ConnectionEstablished {
        connection: ConnectionId,
    },

    ConnectionLost {
        connection: ConnectionId,
        reason: DisconnectReason,
    },

    SubscriptionActivated {
        stream: StreamId,
    },

    SubscriptionRejected {
        stream: StreamId,
    },

    SequenceGapDetected {
        stream: StreamId,
        expected: Option<u64>,
        received: Option<u64>,
    },

    BookInvalidated {
        stream: StreamId,
        generation: BookGeneration,
    },

    RecoveryStarted {
        stream: StreamId,
    },

    RecoveryCompleted {
        stream: StreamId,
    },

    RecordingFailed {
        stream: Option<StreamId>,
    },

    RetentionCapacityExceeded {
        stream: StreamId,
    },

    ConfigurationApplied {
        revision: ConfigRevisionId,
    },
}
```

The variants are illustrative and may be extended without changing the canonical event schema.

### 11.2 Operational Event Ownership

Each subsystem emits events describing operations it owns.

| Subsystem | Events |
|---|---|
| Connection Manager | Connection lifecycle, heartbeat failures |
| Subscription Manager | Subscription activation, rejection, removal |
| Stream Processor | Integrity incidents, normalization failures |
| Recovery Coordinator | Recovery attempts and outcomes |
| Recording | Persistence failures and recovery |
| Retention | Spill failures, capacity exhaustion |
| Configuration Manager | Revision acceptance and rejection |
| Runtime Reconciler | Configuration application results |

Operational events are consumed by the observability subsystem.

The TUI receives operational-event snapshots or bounded recent-event history through the control API.

### 11.3 High-Frequency Metrics Separation

Operational events must not be emitted for every ordinary market-data message.

High-frequency measurements use dedicated metrics primitives:

```text
Per-event counters
Latency histograms
Queue occupancy
Throughput measurements
```

Low-frequency incidents use typed operational events.

This avoids unnecessary allocation, synchronization, and channel traffic in the critical processing path.

### 11.4 Operational Event Guarantees

- Events identify the affected resource.
- Events include an observation timestamp.
- Significant integrity incidents remain distinguishable from routine lifecycle changes.
- Diagnostic history is bounded.
- Failure to record a diagnostic event must not corrupt canonical processing.
- Operational events do not replace authoritative runtime state.
- Metrics snapshots remain the primary source for current health queries.

Persistent operational history is a separate storage concern and is not implied by in-memory event emission.

---

## 12. Configuration Contracts

Configuration is represented as versioned desired state, compiled into validated effective runtime specifications.

### 12.1 ConfigRevisionId

```rust
pub struct ConfigRevisionId(pub u64);
```

Configuration revisions identify accepted desired-state changes.

They are independent of connection generations, book generations, derived generations, and retained-log epochs.

### 12.2 Desired Configuration

```rust
pub struct DesiredLiveConfig {
    pub revision: ConfigRevisionId,

    pub global: GlobalLiveConfig,

    pub sources: Vec<SourceStreamConfig>,

    pub derived: Vec<DerivedStreamConfig>,
}
```

Exchange protocol profiles and instrument metadata are resolved through the configuration compiler.

Desired configuration describes what should run, not what is currently operational.

### 12.3 Effective Configuration

```rust
pub struct EffectiveStreamConfig {
    pub stream: SourceStreamKey,

    pub recording: RecordingPolicy,
    pub recovery: RecoveryPolicy,
    pub metrics: MetricsPolicy,
}
```

The effective configuration is produced from:

```text
Global Defaults
       +
Exchange Protocol Constraints
       +
Stream Overrides
       |
       v
Effective Stream Configuration
```

Exchange protocol constraints are authoritative and cannot be overridden by incompatible stream-level settings.

### 12.4 Configuration Scope

```rust
pub enum ConfigScope {
    Global,
    ExchangePool,
    SourceStream,
    DerivedStream,
}
```

Each configurable field must declare its supported scope.

Not every setting is inheritable or editable at every level.

### 12.5 Reconfiguration Policy

```rust
pub enum ReconfigurationPolicy {
    Hot,
    StreamTransition,
    ConnectionReplacement,
    ServiceRestart,
    Immutable,
}
```

Initially, runtime editing is restricted to supported hot and stream-level operations.

Connection replacement and structural configuration changes remain outside the initial interactive management scope.

### 12.6 Configuration Reconciliation

Configuration changes follow:

```text
Proposed Revision
        |
        v
Validate
        |
        v
Compile Effective Configuration
        |
        v
Compare With Current Desired State
        |
        v
Generate Reconciliation Plan
        |
        v
Persist Accepted Desired Revision
        |
        v
Apply Affected Runtime Changes
        |
        v
Report Convergence Status
```

Persistence of desired state and successful runtime activation are distinct outcomes.

An exchange outage may prevent convergence without invalidating the accepted configuration.

### 12.7 Configuration Invariants

- Configuration revisions are immutable after acceptance.
- Concurrent updates must use revision-based conflict detection.
- Invalid configurations must not modify runtime state.
- Runtime reconciliation must affect only necessary resources.
- Stream identity changes require new source identities.
- Semantic transformation changes require explicit derived-generation boundaries.
- Configuration rollback does not imply event-history rollback.
- Configuration persistence must survive process restarts according to the configured storage guarantee.

---

## 13. Control Protocol Contracts

MarketForge Live exposes a versioned management protocol over a dedicated Unix domain socket.

The control protocol provides runtime configuration, stream management, health queries, and retention operations without exposing internal Rust objects.

### 13.1 ControlRequest

```rust
pub struct ControlRequest {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub command: ControlCommand,
}
```

```rust
pub enum ControlCommand {
    GetRuntimeStatus,
    ListStreams,
    GetStream(StreamId),
    AcquireStream(SourceStreamKey),
    ReleaseStream(StreamId),
    GetMetrics(MetricsQuery),
    GetConfiguration,
    ApplyConfiguration(ConfigUpdate),
    GetRetentionStatus(StreamId),
}
```

These variants define the conceptual command vocabulary.

The final protocol must distinguish persistent acquisition changes from transient subscriber demand.

### 13.2 ControlResponse

```rust
pub struct ControlResponse {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub result: ControlResult,
}
```

```rust
pub enum ControlResult {
    RuntimeStatus(RuntimeStatusSnapshot),
    Streams(Vec<StreamSnapshot>),
    Stream(StreamSnapshot),
    Metrics(MetricsSnapshot),
    Configuration(DesiredLiveConfig),
    Retention(RetentionStatus),
    Operation(OperationStatus),
    Error(ControlError),
}
```

Responses must be serializable independently of internal implementation types.

### 13.3 Asynchronous Operations

Long-running operations must not block control requests until completion.

Examples:

- Starting an exchange subscription.
- Recovering an invalid order book.
- Activating a derived stream.
- Applying a configuration revision.

Such operations return an identifier:

```rust
pub struct OperationId(pub u64);
```

The client queries the operation's progress separately.

```rust
pub enum OperationState {
    Accepted,
    Applying,
    Completed,
    Failed,
}
```

Successful command acceptance does not imply successful runtime activation.

### 13.4 Control Protocol Invariants

- Requests and responses carry explicit protocol versions.
- Request identifiers support correlation.
- Unsupported protocol versions must be rejected explicitly.
- Invalid commands must not modify runtime state.
- Mutating commands must define retry and idempotency behavior.
- Control clients must not access internal runtime locks directly.
- Slow control clients must not block market-data processing.
- Closing the TUI must not terminate the daemon.
- Unix socket permissions must restrict unauthorized configuration changes.

---

## 14. Metrics and Health Snapshot Contracts

Metrics are collected by the owning runtime components and exposed through immutable snapshots.

The TUI and external clients consume snapshots rather than reading mutable counters directly.

### 14.1 StreamMetricsSnapshot

```rust
pub struct StreamMetricsSnapshot {
    pub stream_id: StreamId,

    pub messages_received: u64,
    pub events_normalized: u64,
    pub events_published: u64,
    pub messages_rejected: u64,

    pub sequence_gap_incidents: u64,
    pub missing_sequence_units: Option<u64>,
    pub duplicate_events: u64,
    pub checksum_failures: u64,

    pub disconnects: u64,
    pub recovery_attempts: u64,
    pub successful_recoveries: u64,

    pub events_per_second: f64,

    pub normalization_latency: LatencySummary,
    pub publication_latency: LatencySummary,
}
```

This is a representative subset of the final metrics schema.

`missing_sequence_units` must remain unavailable when the exchange protocol cannot establish a meaningful count.

### 14.2 LatencySummary

```rust
pub struct LatencySummary {
    pub samples: u64,

    pub p50_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,

    pub max_ns: Option<u64>,
}
```

Latency measurements must use monotonic clocks for internal durations.

Exchange-to-arrival delay is a separate measurement involving exchange and local wall clocks.

It must not be presented as an exact network latency measurement without accounting for clock synchronization.

### 14.3 StreamHealthSnapshot

```rust
pub struct StreamHealthSnapshot {
    pub stream_id: StreamId,

    pub lifecycle: StreamLifecycle,
    pub connection: ConnectionState,
    pub subscription: SubscriptionState,
    pub integrity: IntegrityState,
    pub recovery: RecoveryState,

    pub coverage: CoverageState,

    pub last_message_at_ns: Option<i64>,
    pub last_valid_event_at_ns: Option<i64>,
}
```

Health snapshots describe observed runtime state.

They must not independently determine whether canonical events are valid.

### 14.4 MetricsReader

Conceptual interface:

```rust
pub trait MetricsReader: Send + Sync {
    fn stream_snapshot(
        &self,
        stream: &StreamId,
    ) -> Option<StreamMetricsSnapshot>;

    fn all_stream_snapshots(
        &self,
    ) -> Vec<StreamMetricsSnapshot>;
}
```

The metrics registry owns aggregation.

Individual stream processors receive lightweight metrics handles.

### 14.5 Metrics Snapshot Guarantees

- Metrics queries must not block exchange ingestion.
- Snapshots include observation timestamps.
- Counter reset boundaries must be identifiable.
- Missing measurements must not be represented as zero.
- Histograms must use bounded memory.
- High-cardinality identifiers must not create unbounded metrics structures.
- Disabled detailed instrumentation must not disable integrity validation.
- Metrics snapshots may represent slightly different observation instants across independent workers.

Strictly atomic global metrics snapshots are not required initially.

---

## 15. Retention and Replay Contracts

The retention subsystem preserves accepted canonical events across bounded memory and Parquet-backed storage.

It provides stable cursor-based access independently of physical storage location.

### 15.1 RetentionCursor

Retention uses the previously defined `StreamCursor`.

No separate cursor namespace should be introduced solely for Parquet storage.

```rust
pub struct RetainedRange {
    pub stream_id: StreamId,
    pub first: StreamCursor,
    pub last: StreamCursor,
}
```

A retained range identifies available event positions.

Availability does not independently establish historical exchange-data completeness.

### 15.2 RetentionStatus

```rust
pub struct RetentionStatus {
    pub stream_id: StreamId,

    pub oldest_available: Option<StreamCursor>,
    pub newest_available: Option<StreamCursor>,

    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub pending_spill_bytes: u64,
}
```

The status model should additionally distinguish accepted, committed, and durably committed positions when the selected retention policy requires those guarantees.

### 15.3 RetentionReader

Conceptual interface:

```rust
pub trait RetentionReader {
    async fn read_from(
        &self,
        cursor: StreamCursor,
        limit: usize,
    ) -> Result<EventBatch, RetentionError>;
}
```

The final interface may use an asynchronous stream or iterator to avoid allocating large replay batches.

Readers must not depend on whether events reside in RAM or Parquet.

### 15.4 RetentionWriter

Conceptual interface:

```rust
pub trait RetentionWriter {
    async fn append(
        &self,
        batch: EventBatch,
    ) -> Result<RetentionCommit, RetentionError>;
}
```

```rust
pub struct RetentionCommit {
    pub accepted_through: StreamCursor,
    pub durable_through: Option<StreamCursor>,
}
```

The exact append contract must specify whether the returned result represents memory acceptance, spill-queue acceptance, committed Parquet storage, or crash-durable persistence.

These guarantees must not be conflated.

### 15.5 Retention Reservations

Future Bootstrap sessions require the ability to protect retained cursor intervals against reclamation.

Conceptual model:

```rust
pub struct RetentionReservation {
    pub reservation_id: RetentionReservationId,
    pub stream_id: StreamId,
    pub from: StreamCursor,
}
```

A reservation prevents deletion of required retained data while valid.

Reservations must have explicit ownership, expiration, cancellation, and resource-accounting semantics.

The retention manager must reject reservations that cannot be satisfied under its declared guarantees.

### 15.6 Replay Continuity

Replay must preserve:

- Original canonical payload.
- Stream identity.
- Cursor identity.
- Source-event order.
- Event provenance.
- Relevant generation boundaries.

A future replay-and-follow operation must transition from retained events to current live events without a cursor gap.

Transport retries may cause repeated delivery; consumers must be able to identify duplicates.

### 15.7 Retention Failure Semantics

Retention failures include:

```rust
pub enum RetentionFailure {
    CursorUnavailable,
    CapacityExceeded,
    SpillFailed,
    SegmentCorrupted,
    StorageUnavailable,
    DurabilityUnavailable,
}
```

Failures must identify the affected stream or reservation.

Memory-first buffering must not be represented as crash-durable.

A failed strict retention obligation must remain visible even when ordinary live publication continues.

---

## 16. Derived-Stream Contracts

Derived streams transform existing canonical source or derived streams without creating duplicate exchange acquisitions.

Initially supported transformations:

- Multi-source event merging.
- Adaptive event-time ordering.
- Currency conversion.

Derived streams are registered, reference-counted, monitored, and published through the same infrastructure as source streams.

### 16.1 DerivedStreamSpec

```rust
pub struct DerivedStreamSpec {
    pub id: DerivedStreamId,
    pub kind: DerivedStreamKind,
    pub dependencies: Vec<StreamId>,
    pub generation: DerivedGeneration,
}
```

```rust
pub enum DerivedStreamKind {
    Merge(MergeSpec),
    Conversion(ConversionSpec),
}
```

The specification defines the transformation's semantics, not its runtime execution state.

### 16.2 DerivedStreamProcessor

Conceptual interface:

```rust
pub trait DerivedStreamProcessor {
    fn process(
        &mut self,
        event: &LiveEvent,
    ) -> Result<Vec<DerivedEvent>, ProcessingError>;
}
```

The final interface may support borrowed event batches, asynchronous dependencies, or output sinks to avoid unnecessary allocations.

Derived processors must not manage physical exchange connections.

### 16.3 Dependency Contracts

Every derived stream declares its required input streams.

```text
Source A --------+
                 |
Source B --------+----> Derived Stream
                 |            |
Source C --------+            v
                         Canonical Output
```

Dependency management must guarantee:

- No circular dependencies.
- No duplicate source acquisition.
- Explicit dependency ownership.
- Compatible input schemas.
- Defined behavior when dependencies become unavailable.
- Independent derived-stream lifecycle.
- Release of dependency handles when no longer required.

A derived stream may depend on another derived stream if its input contract is compatible.

### 16.4 MergeSpec

```rust
pub struct MergeSpec {
    pub sources: Vec<StreamId>,
    pub ordering: MergeOrdering,
    pub source_failure_policy: SourceFailurePolicy,
}
```

```rust
pub enum MergeOrdering {
    Arrival,
    AdaptiveEventTime(AdaptiveOrderingConfig),
}
```

```rust
pub struct AdaptiveOrderingConfig {
    pub initial_window_ms: u64,
    pub max_window_ms: u64,
    pub late_event_policy: LateEventPolicy,
}
```

The merger owns its ordering buffer, watermark state, and output sequence.

Native source streams remain unaffected by merge buffering.

Adaptive ordering improves chronological output but does not guarantee strict global event-time ordering under unbounded source lateness.

### 16.5 ConversionSpec

```rust
pub struct ConversionSpec {
    pub source: StreamId,
    pub target_currency: CurrencyId,
    pub reference_policy: ReferencePolicy,
    pub max_reference_age_ns: u64,
}
```

Currency conversion must reuse existing MarketForge conversion semantics and instrument metadata.

Initially, numerical conversion follows the established historical scope for eligible Spot instruments.

Reference acquisition must reuse the source registry.

Conversion output must preserve original canonical values and record the relevant conversion provenance.

Missing or stale references must not invalidate the original source stream.

### 16.6 Derived Event Identity

Derived events require their own stream identity and cursor namespace.

They must also preserve references to their source events.

```rust
pub struct DerivedEvent {
    pub stream_id: DerivedStreamId,
    pub generation: DerivedGeneration,
    pub provenance: DerivedProvenance,
    pub payload: CanonicalEvent,
}
```

The derived-stream owner assigns its output cursor before publication.

The exact provenance representation depends on the transformation.

For merging, provenance identifies the originating source event.

For conversion, provenance additionally identifies the conversion path and reference observations.

### 16.7 Derived-Stream Invariants

- Source events remain immutable.
- Derived outputs have independent identities and cursors.
- Dependencies cannot form cycles.
- Equivalent derived requests may share one processor.
- Derived failures must not invalidate healthy source streams.
- Semantic configuration changes require explicit generation boundaries.
- Merge ordering and conversion semantics must be independently testable.
- Derived streams must use the same publication, recording, retention, and metrics contracts as source streams.

---

## 17. PostgreSQL Recording Contracts

Recording is an optional consumer of accepted canonical events.

The recording subsystem owns batching, database transactions, commit tracking, and persistence failures.

It must not own source acquisition or canonical normalization.

### 17.1 RecordingPolicy

```rust
pub struct RecordingPolicy {
    pub enabled: bool,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub durability: RecordingDurability,
}
```

```rust
pub enum RecordingDurability {
    AvailabilityFirst,
    DurableBeforePublish,
}
```

`AvailabilityFirst` permits publication without waiting for database durability.

`DurableBeforePublish` represents a stronger ordering requirement and must not be enabled without an implementation capable of enforcing its commitment boundary.

The initial implementation may support only `AvailabilityFirst`.

### 17.2 RecordingSink

Conceptual interface:

```rust
pub trait RecordingSink {
    async fn write_batch(
        &self,
        batch: EventBatch,
    ) -> Result<RecordingCommit, RecordingError>;
}
```

```rust
pub struct RecordingCommit {
    pub stream_id: StreamId,
    pub committed_through: StreamCursor,
    pub records_committed: u64,
}
```

A successful recording commit must correspond to an acknowledged database transaction.

### 17.3 Recording Identity

Database persistence must preserve sufficient information to identify canonical events unambiguously.

Required identity components include:

```text
Stream identity
Log epoch
Event offset
Canonical event identity
Source provenance
```

Native exchange trade identifiers may additionally support source-level deduplication.

MarketForge cursor identity and native exchange identity serve different purposes.

### 17.4 Commit Continuity

A database writer must distinguish:

- Highest observed cursor.
- Highest submitted cursor.
- Highest transactionally committed cursor.
- Highest contiguous committed cursor.

The highest committed cursor does not necessarily establish contiguous recording if earlier batches failed or commits completed out of order.

Replay and bootstrap consumers must use verified committed coverage rather than infer completeness from maximum timestamps or identifiers.

### 17.5 Idempotency

Recording must tolerate retries after ambiguous transaction outcomes.

For example:

```text
Batch Submitted
      |
      v
PostgreSQL Commit Succeeds
      |
      v
Connection Lost Before Acknowledgement
      |
      v
Batch Retried
```

Retrying must not create duplicate logical records.

Idempotency constraints must be compatible with the existing PostgreSQL schema and canonical event identity.

### 17.6 Recording Failure Semantics

```rust
pub enum RecordingError {
    ConnectionUnavailable,
    TransactionFailed,
    ConstraintViolation,
    SerializationFailed,
    CapacityExceeded,
    CommitStateUnknown,
}
```

Recording failures must be classified as retryable or terminal according to their actual cause.

Database outages must not automatically terminate unrelated exchange acquisition.

When recording cannot satisfy its configured guarantee, the affected recording state must explicitly become degraded or failed.

### 17.7 Recording Invariants

- Recording is independently configurable.
- Disabling recording does not terminate sources required elsewhere.
- Database operations never execute inside normalization workers.
- Batch ordering and commit positions remain verifiable.
- Retries must be idempotent.
- Recording metrics are independent of publication metrics.
- A successful publication does not imply successful database recording.
- A committed database record does not imply complete exchange coverage.

---

## 18. Cross-Subsystem Invariants

The following invariants apply to all MarketForge Live implementations and take precedence over subsystem-specific implementation choices.

### 18.1 Identity and Ownership

- Every logical source stream has one authoritative `SourceStreamKey`.
- Equivalent acquisition requests must share one logical source stream.
- Every mutable resource has one authoritative logical owner.
- Physical connection ownership remains independent of source-stream ownership.
- Source, derived, connection, subscription, book, and retained-log identities must remain distinguishable.
- Demand release must not terminate resources required by other owners.
- Stale asynchronous operations must not mutate newer generations.

### 18.2 Canonical Data

- Existing MarketForge canonical schemas remain authoritative.
- Live-specific metadata must not alter canonical numeric semantics.
- Accepted events retain source identity, provenance, and ordering information.
- Canonical event payloads remain immutable after acceptance.
- Atomic L2 updates must remain logically indivisible.
- Invalid book state must not be published as valid.
- Missing required fields must not produce fabricated canonical values.
- Historical and live normalization must agree where source representations contain equivalent information.

### 18.3 Continuity and Ordering

Three independent continuity domains must remain explicit:

| Domain | Guarantee |
|---|---|
| Exchange continuity | Validity of native exchange sequences and source coverage |
| MarketForge continuity | Ordering and identity of accepted canonical events |
| Consumer continuity | Delivery of required MarketForge event positions |

A continuous MarketForge cursor sequence does not prove complete exchange history.

A valid current order book does not prove historical L2 completeness.

A connected subscriber does not prove that every published event was processed.

Combined-stream ordering does not replace native source sequencing.

### 18.4 Failure Isolation

- Connection failures affect only dependent subscriptions.
- Source integrity failures affect only the relevant stream and its dependent transformations.
- Derived-stream failures must not corrupt source streams.
- Subscriber failures must not block unrelated consumers.
- Recording failures must not silently invalidate publication guarantees.
- Retention failures must explicitly invalidate affected replay or durability guarantees.
- Control-plane failures must not interrupt established data-plane operation.
- TUI termination must not terminate the Live daemon.

### 18.5 Resource Safety

Every asynchronous boundary must have a defined resource budget.

```text
Connection buffers
Subscription queues
Source processing queues
Derived-stream buffers
Publication queues
Recording batches
Retention memory
Parquet spill backlog
Diagnostic history
```

Unbounded growth is prohibited unless explicitly justified and constrained by an external resource guarantee.

Capacity exhaustion must result in a defined state transition or failure response.

No subsystem may silently discard accepted canonical events when its contract requires continuity.

### 18.6 Runtime Configuration

- Configuration changes must be validated before acceptance.
- Desired state and actual runtime state remain separate.
- Configuration revisions are immutable.
- Reconciliation must minimize disruption to unaffected resources.
- Stream identity changes require new stream identities.
- Semantic transformation changes require explicit generation transitions.
- Configuration rollback must not imply event-history rollback.
- Runtime configuration changes must remain observable and auditable.

---

## 19. Contract Versioning and Compatibility

MarketForge Live must distinguish internal Rust API evolution from externally serialized protocol compatibility.

### 19.1 Version Domains

| Version Domain | Scope |
|---|---|
| Configuration schema | TOML structure and supported settings |
| Exchange protocol specification | Exchange API and message interpretation |
| Canonical schema | Normalized market-data representation |
| Publication protocol | Unix-socket market-data framing and payloads |
| Control protocol | Management requests and responses |
| Retention format | Persisted event segments and manifests |
| Derived-stream specification | Transformation semantics |

These versions must evolve independently.

An exchange payload change should not automatically require a canonical schema change.

A TUI modification should not require publication-protocol changes.

### 19.2 Internal API Compatibility

Internal Rust interfaces may evolve during development, but changes to shared contracts require evaluation of dependent modules.

Breaking changes must not be introduced solely to accommodate one concrete adapter when the existing abstraction can represent the required behavior.

Conversely, abstractions must not be generalized beyond demonstrated requirements without justification.

### 19.3 Serialized Protocol Compatibility

External protocols must include explicit version information.

Readers must reject unsupported incompatible versions rather than silently misinterpret data.

Backward-compatible additions should use optional fields or explicitly versioned variants where appropriate.

Required fields must not silently change meaning.

### 19.4 Persistent Data Compatibility

Retention segments and manifests must record sufficient schema information for deterministic interpretation.

A newer MarketForge binary must either:

- Read the existing retained format.
- Perform an explicit supported migration.
- Reject the incompatible format with a diagnostic error.

Persistent cursor identities must remain unambiguous across software upgrades.

### 19.5 Configuration Compatibility

Configuration loading must distinguish:

- Supported schema version.
- Unsupported newer version.
- Deprecated but compatible fields.
- Invalid or contradictory settings.

Automatic migration must not silently alter acquisition or canonical event semantics.

---

## 20. Contract Testing Requirements

Contracts must be verified independently of concrete production adapters.

### 20.1 Identity Tests

Verify:

- Equivalent source specifications produce equal acquisition keys.
- Different acquisition parameters produce distinct keys.
- Stream IDs cannot collide across source and derived namespaces.
- Cursor comparisons reject incompatible streams or epochs.
- Cursor offsets remain unique within their namespace.
- Generation identifiers cannot be accidentally interchanged.

### 20.2 Lifecycle Tests

Verify:

- Valid state transitions.
- Rejection of invalid transitions.
- Independent connection and integrity states.
- Recovery after disconnection.
- Book invalidation after sequence discontinuity.
- Obsolete asynchronous responses are ignored.
- Demand ownership prevents premature resource termination.

### 20.3 Delivery Tests

Verify:

- Source-event ordering.
- Correct cursor assignment.
- Subscriber isolation.
- Bounded queue behavior.
- Explicit delivery failures.
- Recording and retention acknowledgment semantics.
- No silent loss under required delivery policies.

### 20.4 Configuration Tests

Verify:

- Global default inheritance.
- Per-stream overrides.
- Exchange protocol constraints.
- Configuration revision conflicts.
- Rejection of invalid updates.
- Minimal reconciliation of affected resources.
- Persistence and restoration of accepted desired state.

### 20.5 Retention Tests

Verify:

- Cursor stability across RAM and Parquet.
- Exact event-count preservation.
- Replay ordering.
- Segment publication integrity.
- Retention reservation enforcement.
- Explicit capacity failures.
- Recovery of committed segments after restart.
- Correct distinction between accepted and durable events.

### 20.6 Historical Compatibility Tests

Shared model changes must preserve existing historical regression tests.

Where applicable, live and historical processing must produce equivalent canonical values for equivalent source records.

Tests must cover exact decimal arithmetic, timestamps, quantity semantics, trade direction, L2 atomicity, and instrument identity.

---

## 21. Open Contract Decisions

The following details require resolution before implementing their dependent subsystems.

| Contract | Open Decision | Specification |
|---|---|---|
| `StreamId` | Internal representation and stable serialization | Domain Models |
| `StreamCursor` | Offset allocation, persistence, epoch recovery | Retention |
| `ChannelSpec` | Extensible typed exchange-specific parameters | Exchange Protocols |
| `LiveEvent` | Physical envelope layout and allocation strategy | Normalization / Publication |
| `NativeSequence` | Representation of exchange-specific sequence semantics | Normalization / Integrity |
| `StreamProcessor` | Concrete enum versus trait-based dispatch | Module Boundaries |
| `EventSink` | Batch ownership, acknowledgment, backpressure | Distribution |
| `RetentionWriter` | Memory acceptance versus durable commitment | Retention |
| `RetentionReader` | Batch API versus asynchronous streaming | Retention |
| `RecordingCommit` | Contiguous committed-cursor tracking | PostgreSQL |
| `DerivedEvent` | Provenance representation and cursor assignment | Derived Streams |
| `ConfigStore` | Atomic persistence and revision recovery | Configuration |
| `ControlProtocol` | Serialization, framing, compatibility | Control API |
| `MetricsSnapshot` | Histogram representation and aggregation windows | Metrics |
| `LiveError` | Concrete Rust error composition and source chaining | Fault Tolerance |

These decisions must be resolved centrally rather than independently inside individual implementations.

---

## 22. Implementation Rules

### 22.1 Domain-First Development

Implement foundational domain models before concrete network, storage, or presentation components.

The initial domain layer should compile and support deterministic tests without requiring Tokio networking, PostgreSQL, or Ratatui.

### 22.2 Interface Stability

Subsystems must expose the smallest interface sufficient for their responsibilities.

Prefer:

- Strongly typed identities.
- Immutable event envelopes.
- Explicit state transitions.
- Typed errors.
- Bounded asynchronous communication.
- Concrete hot-path processing.
- Traits at replaceable infrastructure boundaries.

Avoid:

- Global mutable registries.
- Unstructured JSON inside the canonical processing path.
- String-based error classification.
- Direct cross-module access to mutable internals.
- Excessive trait abstraction.
- Hidden allocation or blocking behavior in performance-critical operations.

### 22.3 Testability

Every subsystem must support deterministic tests through dependency injection or controlled input fixtures.

Production behavior must not be duplicated in test-only implementations.

External exchange connections are not required for ordinary correctness tests.

### 22.4 Performance

Domain contracts must not impose unnecessary allocations, serialization, synchronization, or dynamic dispatch on every market event.

Optimization must preserve semantic correctness and be justified by profiling or benchmarks.

### 22.5 Historical Compatibility

Shared components must evolve without destabilizing the existing historical engine.

Changes to canonical schemas, numeric normalization, `BookStore`, or Parquet serialization require corresponding historical regression verification.

---

## 23. Final Contract Invariants

The architecture is considered contract-complete when the following guarantees are represented explicitly:

**Identity:** Every source, derived stream, event, generation, and retained position has an unambiguous identity.

**Ownership:** Every mutable resource has one authoritative owner and a defined lifecycle.

**Integrity:** Exchange continuity, canonical validity, and historical completeness remain independent concepts.

**Ordering:** Source order, derived order, and consumer delivery order have distinct guarantees.

**Delivery:** Acceptance, publication, persistence, and consumer processing are not conflated.

**Recovery:** Failures produce explicit state transitions and cannot silently restore invalid market state.

**Configuration:** Desired state is validated, versioned, persisted, and reconciled independently of runtime execution.

**Observability:** Metrics and diagnostics report behavior without controlling canonical processing.

**Retention:** Event identity and replay positions survive movement between memory and Parquet, subject to explicit durability guarantees.

**Extensibility:** New exchange integrations, derived processors, storage backends, and management clients use established contracts without restructuring unrelated subsystems.

**Verification:** Every contract has deterministic tests, and existing historical regression tests remain authoritative for shared semantics.

---

