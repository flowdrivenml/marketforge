## 1. Purpose

The Stream Registry is the authoritative runtime control plane for MarketForge Live source streams.

It manages stream identity, lifecycle, ownership, configuration, and dependencies without implementing exchange connectivity, market-data processing, or persistence.

Core responsibilities:

- Register, activate, modify, and remove source streams.
- Maintain authoritative stream identities and lifecycle states.
- Resolve stream configurations and instrument dependencies.
- Coordinate subscription admission and release.
- Track physical connection assignments.
- Enforce stream-level resource limits.
- Coordinate runtime configuration changes.
- Expose stream state to the TUI, API, and orchestration layer.
- Preserve lifecycle consistency across asynchronous operations.

The registry manages **desired and observed state**, not market-data events.

## 2. Architectural Boundaries

The Stream Registry must remain independent of transport implementations, exchange protocols, normalization, recording, and presentation.

| Component | Responsibility |
|---|---|
| Stream Registry | Stream identity, desired state, lifecycle, ownership |
| Subscription Manager | Subscription admission, activation, acknowledgement, removal |
| Connection Manager | Physical WebSocket connections, pooling, reconnection |
| Exchange Protocol | Exchange-specific subscription encoding and response interpretation |
| Configuration Compiler | Validated exchange, channel, and instrument configuration |
| Stream Processor | Message decoding, normalization, integrity validation |
| Recovery Coordinator | Recovery and resynchronization |
| Publisher | Canonical event distribution |
| Recording Manager | Optional PostgreSQL persistence |
| Metrics Registry | Operational counters, gauges, and measurements |
| TUI / API | Runtime inspection and control |

The registry communicates through typed commands, events, and interfaces.

It must not depend on concrete exchange clients or UI components.

### 2.1 Dependency Direction

    TUI / API / Python Orchestration
                  |
                  v
            Stream Registry
                  |
                  v
          Subscription Manager
                  |
                  v
           Connection Manager
                  |
                  v
          Exchange Protocol
                  |
                  v
            Stream Processor
                  |
                  v
          Canonical Event Bus

The diagram represents the primary control flow, not direct ownership of every component.

Runtime components may communicate through dedicated interfaces and event channels.

### 2.2 Design Constraints

- No exchange-specific logic inside the registry.
- No WebSocket operations inside registry state objects.
- No database access on market-data processing paths.
- No direct TUI dependencies.
- No synchronous waiting for network acknowledgements while holding registry locks.
- No mutable shared ownership of stream lifecycle state across independent managers.
- No implicit stream creation from incoming market-data messages.
- No stream activation without validated configuration.
- No silent loss of lifecycle transitions or failed operations.

The registry must remain independently testable using mocked subscription and connection interfaces.

## 3. Stream Identity

A source stream represents one logical market-data subscription.

Its identity must remain stable across connection failures, resubscriptions, and recovery operations.

### 3.1 Source Stream Identity

The Stream Registry maintains two distinct forms of source-stream
identity:

- `SourceStreamKey` — canonical semantic acquisition identity.
- `StreamId` — stable identifier assigned to a registered logical stream.

The authoritative `SourceStreamKey` contract is defined in
Document 02, Section 3.5.

Conceptually:

```rust
pub struct SourceStreamKey {
    pub exchange: ExchangeId,
    pub market: MarketKind,
    pub instrument: InstrumentId,
    pub channel: ChannelSpec,
}
```

`ChannelSpec` must include all parameters affecting the semantic
identity of the requested acquisition.

Examples include:

- Order-book depth.
- Update frequency.
- Exchange-native channel variant.
- Other parameters affecting acquisition semantics.

#### Canonical Acquisition Identity

Two acquisition requests resolving to semantically equivalent
`SourceStreamKey` values must resolve to the same logical
source acquisition.

The Stream Registry must not create duplicate logical source
streams merely because:

- Different clients submitted equivalent requests.
- Different subscribers require the same source data.
- Recording and derived-stream processors require the same source.
- A physical WebSocket connection was replaced.
- A native subscription was restored.
- The same stream was activated again.

The canonical acquisition identity must be independent of
connection assignment, subscription generation, subscriber
identity, and recording configuration.

#### Stable Logical Identity

`StreamId` is an opaque identifier assigned when a logical
source stream is registered.

The registry maintains the association:

    SourceStreamKey -> StreamId

Equivalent acquisition requests reuse the existing logical
stream identity according to the registry's registration and
deduplication contracts.

`StreamId` remains stable across:

- Temporary exchange disconnections.
- Physical connection replacement.
- Native subscription restoration.
- L2 resynchronization.
- Recovery operations.
- Deactivation and subsequent reactivation of the same
  registered logical stream.

A new physical connection or subscription generation must
not independently create a new `StreamId`.

#### Subscription Identity

`SubscriptionKey` represents subscription-definition identity
used by the Stream Registry and Subscription Manager.

It must be derived consistently from the authoritative
`SourceStreamKey` and its validated subscription requirements.

It must not introduce different logical acquisition-equivalence
rules.

The exact representation of `SubscriptionKey` remains an
implementation contract to finalize with the shared domain
models.

`NativeSubscriptionKey` is a separate exchange-protocol
identity defined in Section 7.3.

It identifies the compiled native subscription within its
connection context.

Multiple logical stream identities must not accidentally
produce duplicate exchange-native subscriptions.

#### Identity-Changing Configuration Updates

A configuration change that alters canonical acquisition
semantics must not silently mutate the identity represented
by an existing `SourceStreamKey`.

Such changes require a controlled replacement that:

1. Validates the new acquisition specification.
2. Resolves its canonical `SourceStreamKey`.
3. Checks for an existing equivalent logical acquisition.
4. Establishes the required replacement subscription and
   processing resources.
5. Preserves generation fencing and dependency consistency.
6. Updates registry ownership according to the approved
   replacement contract.

Whether an existing `StreamId` can be retained during a
particular replacement depends on whether the logical
acquisition identity remains semantically unchanged.

A semantic identity change must not silently reuse an
incompatible existing `StreamId`.

The Stream Registry remains authoritative for logical
source-stream identity and deduplication.

### 3.2 Identity Separation

| Identifier             | Scope                                                                                | Stability                                               |
| ---------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------- |
| StreamId               | Logical source stream                                                                | Stable across reconnects                                |
| ConnectionId           | Physical transport connection                                                        | Changes after connection replacement                    |
| ConnectionGeneration   | Physical connection incarnation                                                      | Changes on reconnection                                 |
| SubscriptionGeneration | Subscription activation incarnation                                                  | Changes on reactivation                                 |
| BookGeneration         | Valid L2 synchronization incarnation                                                 | Changes on book reinitialization                        |
| SourceStreamKey        | Canonical logical acquisition identity                                               | Stable while acquisition semantics remain unchanged     |
| SubscriptionKey        | Validated subscription-definition identity derived consistently from SourceStreamKey | Stable while subscription requirements remain unchanged |
A reconnection must not create a new `StreamId`.

Changes affecting native subscription requirements require controlled
subscription replacement. Changes affecting canonical acquisition
semantics additionally require SourceStreamKey identity validation
and may require a new logical StreamId.

Native subscription replacement alone does not imply that the
logical acquisition identity has changed.

### 3.3 Stream Types

Initial source stream types:

    Trades
    L2Depth

Combined streams, currency conversions, and other derived streams are managed through separate dependency-aware components.

They may reference source streams through stable `StreamId` values.

The Stream Registry must not embed merge or currency-conversion algorithms.

---

## 4. Registry Domain Models

The Stream Registry maintains a minimal set of authoritative runtime entities.

### 4.1 StreamDescriptor

Immutable identity and subscription definition.

| Field | Type | Purpose |
|---|---|---|
| stream_id | StreamId | Stable logical identifier |
| subscription_key | SubscriptionKey | Canonical subscription identity |
| exchange | ExchangeId | Exchange identifier |
| market | MarketCategory | Spot, Linear, Inverse |
| instrument_id | InstrumentId | Reference to instrument catalog |
| channel | ChannelKind | Trades, L2Depth |
| channel_parameters | ChannelParameters | Depth, frequency, channel variant |
| created_at | Timestamp | Registration timestamp |

`StreamDescriptor` is immutable after registration.

Changes affecting subscription identity require a new descriptor and controlled replacement.

### 4.2 StreamRuntime

Mutable operational state associated with a registered stream.

| Field                   | Type                   | Purpose                                               |
| ----------------------- | ---------------------- | ----------------------------------------------------- |
| stream_id               | StreamId               | Associated logical stream                             |
| desired_state           | DesiredState           | Requested operational state                           |
| lifecycle_state         | StreamLifecycle        | Logical runtime lifecycle defined in Document 02 §7.2 |
| connection_id           | Option<ConnectionId>   | Assigned physical connection                          |
| connection_generation   | ConnectionGeneration   | Current connection incarnation                        |
| subscription_generation | SubscriptionGeneration | Current subscription incarnation                      |
| configuration_revision  | ConfigRevisionId       | Revision adopted by this stream's runtime             |
| last_error              | Option<StreamError>    | Latest operational failure                            |
| registered_at           | Timestamp              | Registration timestamp                                |
| activated_at            | Option<Timestamp>      | Last successful acquisition activation                |
| state_changed_at        | Timestamp              | Latest logical lifecycle transition                   |

`StreamRuntime` contains authoritative Stream Registry control-plane state.

Its `lifecycle_state` uses the shared `StreamLifecycle` contract defined
in Document 02, Section 7.2.

The Stream Registry owns logical stream identity, desired activation
state, and logical runtime lifecycle. It does not independently own
physical connection, native subscription, integrity, or recovery state.

Connection and subscription generations retained in `StreamRuntime`
are references used for operation correlation and fencing. Their
authoritative lifecycle ownership remains with the Connection Manager
and Subscription Manager, respectively.

Throughput, latency, message loss, and other operational measurements
belong to the Metrics Registry.

Order-book synchronization and market-data integrity belong to the
responsible stream processor.

Health assessment belongs to the Metrics and Stream Health subsystem.

A complete source-stream status may combine observations from these
components without merging their independent state machines.

### 4.3 DesiredState

The Stream Registry distinguishes persistent acquisition intent,
effective acquisition demand, and actual runtime state.

These are separate concepts.

#### Persistent Desired State

The supported conceptual desired states are:

    Active
    Inactive
    Removed

| Desired State | Meaning |
|---|---|
| `Active` | Persistent configuration requests acquisition of this registered source. |
| `Inactive` | Persistent configuration does not independently request acquisition. Other valid demand owners may still require the source. |
| `Removed` | Removal of the persistent registration has been requested. Final removal is subject to dependency and demand-ownership constraints. |

The Configuration Manager owns the authoritative persisted desired
configuration and its accepted revisions.

The Stream Registry maintains the corresponding authoritative
runtime registration, ownership, and logical lifecycle state.

Persistent changes must pass through the configuration revision,
validation, persistence, and runtime reconciliation contracts
defined in Documents 02, 03, and 13.

The registry must not independently persist competing desired
configuration state.

#### Independent Acquisition Demand

A registered source may be required by multiple independent
demand owners.

Demand ownership follows Document 02, Section 8.

Supported conceptual owners include:

    PersistentConfiguration
    Recording
    Subscriber(SubscriberId)
    DerivedStream(DerivedStreamId)
    RetentionSession(RetentionSessionId)

Every individual demand registration has an unambiguous
`DemandId`.

The Stream Registry maintains the authoritative set of
valid demand registrations for each source.

Conceptually:

    SourceStreamKey
          |
          v
       StreamId
          |
          v
    Demand Registry
          |
          +---- Persistent Configuration
          +---- Recording
          +---- Subscriber A
          +---- Subscriber B
          +---- Derived Stream
          +---- Retention Session

Equivalent acquisition requirements share the same
registered source.

They do not create independent exchange subscriptions.

#### Effective Acquisition Demand

Effective acquisition demand is derived from the current
valid demand registrations.

Conceptually:

    EffectiveAcquisitionRequired =
        AtLeastOneValidAcquisitionDemandExists

When at least one valid demand owner requires acquisition,
the Stream Registry coordinates the necessary logical
runtime and native subscription resources.

When no valid demand remains, the source may transition
toward `Idle` or `Stopped` according to the established
resource-retention and shutdown policies.

Effective acquisition demand is not itself a
`StreamLifecycle` variant.

It is an ownership-derived condition used during
runtime reconciliation.

#### Activation and Deactivation Semantics

`ActivateStream` enables the persistent acquisition
requirement for the registered source.

`DeactivateStream` disables that persistent acquisition
requirement.

These operations must not create or release demand
registrations owned by unrelated consumers.

For example:

    Persistent Desired State: Inactive
    Recording Demand:         Present
    Subscriber Demand:        Present

    Effective Acquisition:    Required

    StreamLifecycle:          Active

This is a valid runtime state.

The source remains operational because recording and
subscriber demand still require it.

A user-issued `DeactivateStream` command must not
silently disconnect those consumers or terminate
their required acquisition.

Conversely:

    Persistent Desired State: Inactive
    Recording Demand:         Absent
    Subscriber Demand:        Absent
    Derived Demand:           Absent
    Retention Demand:         Absent

    Effective Acquisition:    Not Required

The registry may then coordinate idle retention or
shutdown according to its configured lifecycle policies.

#### Removal Semantics

`RemoveStream` requests removal of the registered
logical source.

Removal is distinct from disabling persistent
acquisition.

The registry must not remove a source while valid
protected dependencies or independent demand registrations
still require it.

A removal request with outstanding demand must be
rejected or handled through an explicitly supported,
validated coordinated-removal policy.

Removal must not silently release unrelated subscribers,
recording requirements, derived dependencies, or
retention reservations.

A removed source must not be resurrected by obsolete
asynchronous operations.

#### Reconciliation Invariants

The following invariants are mandatory:

- Persistent desired configuration has one authoritative owner.
- Equivalent acquisition requests share one logical source.
- Each demand registration has a unique ownership identity.
- Releasing one demand affects only that demand.
- Outstanding independent demand prevents premature shutdown.
- Persistent deactivation does not imply physical deactivation.
- Actual runtime lifecycle remains independent of desired state.
- Connection, subscription, integrity, recovery, and health
  states remain independently authoritative.
- Configuration acceptance does not imply runtime convergence.
- Stale generations cannot restore obsolete acquisition demand.

Temporary connection failures must not silently modify
persistent desired state or registered demand ownership.

### 4.4 StreamLifecycle

The Stream Registry uses the authoritative `StreamLifecycle` model
defined in Document 02, Section 7.2.

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

These states describe the lifecycle of the logical source-stream
runtime, not the readiness or health of its market-data feed.

| State | Meaning |
|---|---|
| `Registered` | The logical stream is registered; its runtime has not started. |
| `Starting` | Runtime resources are being initialized and activation is being coordinated. |
| `Active` | The logical runtime is running and can coordinate acquisition, processing, and recovery. |
| `Idle` | The runtime remains allocated but is not currently acquiring data because no acquisition demand requires it. |
| `Stopping` | Runtime shutdown or resource release is in progress. |
| `Stopped` | Runtime execution has stopped; the logical registration may remain available. |
| `Failed` | The logical runtime cannot continue without intervention or an authorized restart. |

#### Independent Operational States

The following conditions are not `StreamLifecycle` variants:

| Condition | Authoritative Dimension |
|---|---|
| Configuration validation | Configuration validation or operation status |
| Waiting for admission | Subscription admission or operation status |
| Subscription request in progress | `SubscriptionState` |
| Physical reconnection | `ConnectionState` |
| L2 snapshot synchronization | `IntegrityState` |
| Temporary integrity degradation | Integrity state and health assessment |
| Recovery in progress | `RecoveryState` |
| Native unsubscription in progress | `SubscriptionState` |
| Final registry removal | Registry removal operation |

A source stream may remain `StreamLifecycle::Active` while any
recoverable operational condition above is being handled.

For example:

    DesiredState:       Active
    StreamLifecycle:    Active
    ConnectionState:    Connected
    SubscriptionState:  Active
    IntegrityState:     Synchronizing
    RecoveryState:      Idle
    Health:             Degraded

This is valid for a running L2 stream whose order book has
not yet completed synchronization.

#### Lifecycle Versus Data Readiness

`StreamLifecycle::Active` confirms that the logical runtime
is running.

It does not imply that:

- The physical WebSocket connection is currently healthy.
- An exchange-native subscription has been acknowledged.
- The stream processor has completed initialization.
- An L2 order book is synchronized.
- All integrity checks have passed.
- Historical coverage is complete.

Data readiness is evaluated separately using the authoritative
subscription, processor-initialization, and integrity contracts.

A successful WebSocket subscription acknowledgement does not
imply that an L2 order book is synchronized.

For trades, acquisition readiness requires the exchange-specific
subscription confirmation and required processor initialization.

For L2, acquisition readiness additionally requires successful
snapshot and sequence initialization according to the configured
synchronization strategy.

The Stream Registry may expose aggregated readiness information
through authoritative component observations, but it must not
redefine or duplicate the underlying state machines.

#### Registration and Removal

A registered logical stream may have a stopped runtime.

`Stopped` does not imply that its registration has been removed.

Removal is a separate registry operation that releases the logical
registration and fences obsolete asynchronous completions.

A removed stream must not be resurrected by delayed connection,
subscription, or recovery events.

#### Failure Semantics

A recoverable network outage, failed heartbeat, or temporary
integrity problem does not automatically move the logical stream
to `Failed`.

`Failed` applies when the logical runtime cannot continue under
its established execution and recovery contracts.

Failures requiring configuration correction or operator intervention
must remain visible through the appropriate operation, error,
and health reporting interfaces.
---

## 5. Stream Lifecycle

### 5.1 Registration

Registration establishes the identity and immutable descriptor
of one logical source acquisition.

Registration does not independently imply that the source
must be activated.

The Stream Registry is the authoritative owner of logical
source-stream registration and acquisition deduplication.

#### Registration Sequence

Conceptually:

    Register Source Request
              |
              v
    Validate Source Definition
              |
              v
    Resolve Instrument Metadata
              |
              v
    Construct SourceStreamKey
              |
              v
    Check Existing Registrations
              |
         +----+----+
         |         |
         v         v
      Existing    Missing
         |         |
         v         v
      Reuse      Allocate
      StreamId   StreamId
         |         |
         |         v
         |      Create
         |      Descriptor
         |         |
         +----+----+
              |
              v
    Return Registered StreamId

The registration sequence does not establish physical
connections or native exchange subscriptions.

Those resources are coordinated separately when
acquisition demand requires activation.

#### Canonical Deduplication

The authoritative acquisition-equivalence contract is
`SourceStreamKey`, defined in Document 02, Section 3.5.

For any two validated requests:

    Equivalent SourceStreamKey
              |
              v
    Same Logical Source Acquisition
              |
              v
    Same Registered StreamId

The registry must prevent concurrent equivalent
registration requests from creating multiple logical
source acquisitions.

Deduplication must not depend on:

- Physical connection assignment.
- Connection or subscription generation.
- Subscriber identity.
- Recording configuration.
- Derived-stream consumer identity.
- Runtime health or recovery state.

Different acquisition semantics require distinct
`SourceStreamKey` values.

#### StreamId Assignment

`StreamId` identifies the registered logical source.

A registered source retains its identity across:

- Activation and deactivation.
- Physical connection replacement.
- Native subscription restoration.
- Recoverable exchange outages.
- L2 resynchronization.
- Temporary loss of acquisition readiness.

The registry maintains the authoritative mapping:

    SourceStreamKey -> StreamId

A persistent source registration must also preserve
its stable identity across daemon restarts according
to the configuration and registry persistence contracts.

A semantic acquisition change must not silently mutate
the identity of an existing registered source.

#### Registration Idempotency

Registration must be idempotent with respect to the
canonical `SourceStreamKey`.

An equivalent registration request returns the existing
logical source identity.

A request attempting to bind an existing `StreamId`
to an incompatible `SourceStreamKey` must be rejected
as an identity conflict.

Registration must not create an additional native
subscription merely because the same source is
requested more than once.

#### Registration Versus Demand Acquisition

Registration and demand acquisition are separate
logical operations.

Registration establishes:

    SourceStreamKey
    StreamId
    StreamDescriptor

Demand acquisition establishes an ownership claim
on the registered source.

The authoritative demand contracts are defined in
Document 02, Section 8.

Conceptually:

    SourceStreamKey
          |
          v
    Registered Source
          |
          +---- Persistent Configuration Demand
          |
          +---- Recording Demand
          |
          +---- Subscriber Demand
          |
          +---- Derived-Stream Demand
          |
          +---- Retention Session Demand

Different demand owners may reference the same
registered logical source.

Their requirements must not create duplicate
source acquisitions.

Each individual demand registration must have an
unambiguous `DemandId`.

An active `StreamHandle` identifies the logical source
and the corresponding ownership claim.

Conceptually:

```rust
pub struct StreamHandle {
    pub stream_id: StreamId,
    pub demand_id: DemandId,
}
```

The representation follows Document 02, Section 8.2.

Releasing a demand handle removes only the ownership
claim represented by that handle.

It must not remove or deactivate acquisition still
required by other valid demand owners.

#### Activation Eligibility

The Stream Registry determines whether acquisition
should remain active from the authoritative desired
configuration and outstanding demand ownership.

A registered source may remain inactive when no
acquisition demand requires it.

A registered source with active acquisition demand
must not be deactivated merely because one consumer
releases its own demand handle.

The transition between active acquisition, an idle
grace period, and stopped acquisition follows the
demand lifecycle defined in Document 02, Section 8.3.

Native subscriptions and physical connections remain
owned by their respective managers.

The Stream Registry must not independently execute
network operations during registration.

### 5.2 Activation

Activation is an asynchronous operation that coordinates logical runtime
startup, connection assignment, native subscription establishment,
processor initialization, and channel-specific data readiness.

These activities belong to separate authoritative state dimensions.

#### Logical Runtime Startup

The logical runtime follows the shared `StreamLifecycle` contract
defined in Document 02, Section 7.2.

Conceptually:

    Registered / Stopped
             |
             v
          Starting
             |
             v
           Active

`StreamLifecycle::Active` indicates that the logical runtime is
running and capable of coordinating acquisition and recovery.

It does not imply that the native subscription is established
or that the stream is ready to publish valid market data.

#### Acquisition Activation

Once the required logical runtime resources are available,
the responsible components coordinate acquisition.

Conceptually:

    Activation Requested
             |
             v
    Validate Configuration
             |
             v
    Resolve Instrument Metadata
             |
             v
    Check Resource Admission
             |
             v
    Assign Compatible Connection
             |
             v
    Establish Native Subscription
             |
             v
    Initialize Stream Processor
             |
             v
    Satisfy Channel Readiness
             |
             v
    Activation Completed

The sequence above describes logical dependencies, not a second
`StreamLifecycle` state machine.

Connection establishment, subscription acknowledgement,
processor initialization, and integrity synchronization
may proceed asynchronously according to their respective contracts.

The Subscription Manager owns native subscription admission,
activation, acknowledgement handling, and subscription generations.

The Connection Manager owns physical connection establishment
and connection lifecycle.

The responsible stream processor owns channel-specific
initialization and integrity verification.

The Stream Registry owns logical identity, desired state,
logical runtime lifecycle, and coordination of the
activation operation's authoritative outcome.

#### Activation Completion

Accepting an activation command does not imply that
activation has completed.

An activation operation completes successfully only when
all required readiness conditions defined in Section 5.3
have been satisfied.

The runtime may remain `StreamLifecycle::Active` while
the activation operation is waiting for subscription
confirmation or channel-specific initialization.

Each native subscription activation receives a new
`SubscriptionGeneration`.

All asynchronous subscription completion events must
carry the generation to which they belong.

Obsolete-generation results must not mutate the current
stream state or complete a newer activation operation.

Activation failure must be reported explicitly.

A recoverable activation problem may remain subject to
the established retry and recovery policies without
requiring the logical runtime to transition to `Failed`.

### 5.3 Logical Runtime Activity and Data Readiness

Logical runtime activity and market-data readiness are
independent concepts.

`StreamLifecycle::Active` means the logical runtime
is running.

It does not guarantee successful native subscription,
valid market data, or complete historical coverage.

#### Acquisition Readiness

A stream is considered ready for its configured acquisition
channel only when:

1. The required connection assignment is valid.
2. The native subscription has been acknowledged or
   otherwise confirmed according to the exchange protocol.
3. The stream processor has completed its required initialization.
4. Required instrument metadata is available and valid.
5. Channel-specific readiness conditions are satisfied.
6. No blocking integrity failure prevents valid event acceptance.

These requirements determine acquisition readiness and
successful activation completion.

They do not define the meaning of `StreamLifecycle::Active`.

#### Trade Readiness

For a trade stream, acquisition readiness requires:

- A valid connection assignment.
- Exchange-specific native subscription confirmation.
- Successful trade processor initialization.
- Availability of required instrument metadata.
- Satisfaction of applicable trade-integrity requirements.

Trade readiness must not be inferred solely from an
established WebSocket connection.

#### L2 Readiness

For an L2 stream, acquisition readiness additionally requires
successful order-book initialization according to its
configured snapshot and sequence synchronization strategy.

Conceptually:

    Native Subscription Established
                |
                v
       L2 Initialization
                |
                v
       Snapshot / Update
        Synchronization
                |
                v
       IntegrityState::Valid
                |
                v
         L2 Data Ready

A successful WebSocket subscription acknowledgement does
not imply that the order book is synchronized.

An L2 stream with `IntegrityState::Synchronizing` is not
yet ready to publish events requiring a valid synchronized
book, even if its logical runtime is `Active`.

#### Readiness During Recovery

A previously ready stream may temporarily lose readiness
because of:

- WebSocket disconnection.
- Native subscription interruption.
- L2 synchronization failure.
- Required metadata becoming invalid.
- A blocking integrity failure.

The logical runtime may remain `StreamLifecycle::Active`
while the responsible components attempt recovery.

The system must report readiness loss and recovery progress
through the appropriate authoritative status and health interfaces.

Previous successful activation does not authorize publication
of events that no longer satisfy their integrity requirements.

#### Historical Coverage

Acquisition readiness does not guarantee historical completeness.

A stream may be currently ready while historical intervals
remain incomplete.

Historical coverage is tracked independently through the
recovery and retention contracts.

#### Control API Semantics

The Control API must distinguish:

- Activation request accepted.
- Logical runtime started.
- Acquisition readiness achieved.
- Activation operation completed.
- Activation failed or remains pending.
- Readiness subsequently lost or restored.

A successful command acknowledgement must not be presented
as successful acquisition activation.

The Stream Registry must obtain connection, subscription,
integrity, and recovery information from the authoritative
component owners rather than maintaining competing
implementations of those state machines.

### 5.4 Deactivation

Deactivation disables the persistent acquisition requirement
associated with a registered logical source stream.

It does not automatically stop the source's logical runtime
or remove its native exchange subscription.

The authoritative persistent desired configuration is owned
by the Configuration Manager.

The Stream Registry owns the corresponding demand registration,
effective acquisition-demand calculation, and logical runtime
lifecycle.

#### Persistent Deactivation

A successful `DeactivateStream` configuration mutation changes
the persistent acquisition intent to:

    DesiredState: Inactive

The accepted change must follow the authoritative
configuration-revision lifecycle:

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
    Persist Accepted Desired Revision
       |
       v
    Publish Accepted Revision
       |
       v
    Runtime Reconciliation

The Stream Registry must not independently mutate persisted
desired configuration.

It applies the accepted revision through the Runtime Reconciler.

#### Demand Release

Once the accepted configuration revision is available for
reconciliation, the registry releases the persistent
acquisition demand associated with the source.

The release must target the correct `DemandId`.

Other demand registrations must remain unchanged.

For example:

    Before Deactivation:

        Persistent Configuration Demand: Present
        Recording Demand:                Present
        Subscriber Demand:               Present

        Effective Acquisition:           Required

    After Deactivation:

        Persistent Configuration Demand: Absent
        Recording Demand:                Present
        Subscriber Demand:               Present

        Effective Acquisition:           Required

The logical runtime and native subscription remain
available because independent demand still exists.

#### Runtime Behavior

After releasing persistent demand, the Stream Registry
recalculates effective acquisition requirements.

If other valid demand owners remain:

- Preserve the registered logical source identity.
- Preserve acquisition required by those owners.
- Do not unsubscribe merely because persistent demand
  was disabled.
- Do not invalidate a healthy L2 book unnecessarily.
- Do not reset the source cursor.
- Do not interrupt unrelated consumers.
- Report the accepted desired-state change independently
  of actual acquisition readiness.

If no valid acquisition demand remains:

- Prevent new acquisition work that is no longer required.
- Cancel or supersede unnecessary pending activation work.
- Coordinate native unsubscription when necessary.
- Fence obsolete subscription generations.
- Release stream-specific acquisition resources according
  to the applicable ownership policies.
- Transition the logical runtime toward `Idle` or
  `Stopped` according to the configured lifecycle policy.
- Preserve the logical stream registration.

Conceptually:

    Persistent Demand Released
               |
               v
    Recalculate Effective Demand
               |
          +----+----+
          |         |
          v         v
      Demand      No Demand
      Remains      Remains
          |         |
          v         v
      Preserve    Coordinate
      Acquisition Idle / Stop

#### Deactivation Completion

The control plane must distinguish:

- Persistent deactivation request accepted.
- Desired configuration revision persisted.
- Persistent demand successfully released.
- Effective acquisition remains required by other owners.
- Acquisition becomes unnecessary and begins shutdown.
- Required resource release completed.

Successful persistent deactivation does not necessarily
mean that the physical market-data subscription has stopped.

When independent demand remains, deactivation completes
after the persistent demand change has been reconciled,
without waiting for unrelated acquisition to terminate.

When no demand remains and the reconciliation plan
requires acquisition shutdown, completion must respect
the applicable resource-release requirements.

An accepted command must not be reported as fully
reconciled before its required changes have completed.

#### Generation and Resource Safety

Deactivation must be idempotent.

Releasing persistent demand more than once must not
decrement or release unrelated ownership claims.

If acquisition is no longer required, delayed subscription
acknowledgements, reconnection callbacks, or recovery
completions must not independently reactivate it.

If independent demand remains, generation fencing must
prevent obsolete operations from corrupting the
continuing acquisition.

Physical WebSocket connections must remain available
while other subscriptions require them.

### 5.5 Removal

Removal deletes a registered logical source acquisition.

It is distinct from persistent acquisition deactivation.

Deactivation releases one persistent acquisition-demand
registration while preserving the logical source.

Removal releases the logical source registration itself
and is permitted only when its ownership and dependency
constraints have been satisfied.

#### Removal Preconditions

Before removing a source, the Stream Registry must verify:

- The requested `StreamId` identifies an existing
  registered logical source.
- The associated `SourceStreamKey` is resolved.
- No protected dependent resources require the source.
- No independent acquisition-demand registrations
  would be silently destroyed.
- Pending operations can be safely cancelled or fenced.
- Required recording and retention obligations remain
  governed by their authoritative subsystems.
- Applicable configuration revisions and expected-revision
  constraints are satisfied.

An ordinary `RemoveStream` request must not silently
disconnect subscribers, disable recording, or destroy
derived-stream dependencies.

When protected demand or dependencies exist, removal
must be rejected with a structured conflict unless
an explicitly supported coordinated-removal policy
has been requested and validated.

The dependency-management policies defined in
Section 18.3 remain authoritative.

#### Persistent Configuration Removal

Removing a persistently configured source definition
is a configuration mutation.

It must follow the authoritative sequence:

    Validate Proposed Removal
              |
              v
    Check Revision and Dependencies
              |
              v
    Persist Accepted Desired Revision
              |
              v
    Publish Accepted Revision
              |
              v
    Runtime Reconciliation

The Configuration Manager owns the accepted desired revision.

The Runtime Reconciler coordinates the required changes.

The Stream Registry owns the logical registration and
demand-ownership transitions.

A request must not delete a persistent source definition
from authoritative runtime state before the accepted
configuration revision has been committed.

#### Registry Removal Sequence

Once removal is authorized:

    Removal Authorized
            |
            v
    Prevent New Demand Registration
            |
            v
    Cancel or Fence Pending Operations
            |
            v
    Release Authorized Demand Claims
            |
            v
    Coordinate Acquisition Shutdown
            |
            v
    Verify Resource Release
            |
            v
    Remove SourceStreamKey Mapping
            |
            v
    Remove StreamId Registration
            |
            v
    Removal Completed

The Stream Registry must serialize removal against
concurrent registration, demand acquisition, and
dependency mutations.

A source must not be removed after an ownership check
if another operation has concurrently registered a new
protected dependency.

#### Lifecycle During Removal

Removal is not a `StreamLifecycle` variant.

An existing logical runtime may transition through:

    StreamLifecycle::Active
              |
              v
    StreamLifecycle::Stopping
              |
              v
    StreamLifecycle::Stopped

The registration is deleted only after required
resource-release conditions have been satisfied.

If the logical runtime is already stopped, removal
may proceed directly to the remaining cleanup steps.

#### Shared Resource Safety

Removing a logical source must not terminate:

- Physical WebSocket connections required by other
  native subscriptions.
- Other registered logical source streams.
- Derived processors with independently valid dependencies.
- Recording sessions belonging to other sources.
- Retention resources still protected by valid obligations.

The responsible subsystem owner performs each resource
release operation.

The Stream Registry coordinates removal without directly
implementing network, processor, recording, or retention
cleanup logic.

#### Identity and Generation Safety

Removal must be idempotent.

A removed stream must not be resurrected by:

- Delayed native subscription acknowledgements.
- Obsolete connection events.
- Recovery completion callbacks.
- Pending activation operations.
- Stale configuration reconciliation results.

All relevant asynchronous operations must be cancelled,
completed, or generation-fenced before final removal.

A later acquisition request with the same semantic
`SourceStreamKey` may establish a new registration.

It must not reuse an obsolete runtime incarnation or
reinterpret previously accepted event cursors.

#### Removal Completion

The control plane must distinguish:

- Removal request accepted.
- Removal authorization completed.
- Required configuration revision persisted.
- Pending operations cancelled or fenced.
- Required resource cleanup completed.
- Logical registration removed.

A request acknowledgement does not imply completion
of asynchronous removal.

A rejected removal must preserve the existing source
registration and independent demand owners.

Partial cleanup failures must remain visible and must not be reported as successful removal.

---

## 6. Lifecycle Consistency

### 6.1 Generation Fencing

Every asynchronous subscription operation carries:

    StreamId
    SubscriptionGeneration
    ConnectionGeneration

A completion event is accepted only when its identifiers match the current registry state.

Example:

    Generation 7: Subscribe requested
    Generation 7: Timeout
    Generation 8: Subscribe requested
    Generation 8: Acknowledged
    Generation 7: Late acknowledgement

The final acknowledgement for generation 7 must be ignored.

This prevents stale operations from modifying newer stream state.

### 6.2 Serialized State Transitions

Lifecycle mutations for one `StreamId` must be serialized.

Independent streams may transition concurrently.

The implementation may use:

- Per-stream actors.
- Serialized command queues.
- Short-lived registry locks with generation checks.

No network operation may execute while holding a global registry write lock.

### 6.3 Failure Classification

Failures must be reported through the authoritative subsystem
responsible for the affected operation.

A failure in a physical connection, native subscription, integrity
processor, or recovery operation does not automatically imply failure
of the logical source-stream runtime.

The Stream Registry preserves logical identity and desired state
while the responsible subsystem handles recoverable failures.

| Failure                              | Responsible State or Action                                                                                                   | Logical Runtime Behavior                                                                                                                             |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Temporary network failure            | Connection Manager enters reconnection handling                                                                               | May remain `Active`                                                                                                                                  |
| Exchange outage                      | Connection Manager and Recovery Coordinator apply the appropriate backoff and escalation policies                             | May remain `Active`                                                                                                                                  |
| Subscription rate limit              | Subscription admission remains pending under the applicable shared cooldown                                                   | May remain `Starting` or `Active`                                                                                                                    |
| Invalid subscription parameters      | Subscription operation rejected; configuration correction required                                                            | Remains registered; operation fails explicitly                                                                                                       |
| Unsupported channel                  | Configuration or subscription validation rejects the request                                                                  | Remains registered; operation fails explicitly                                                                                                       |
| Missing required instrument metadata | Validation or initialization fails                                                                                            | Activation remains incomplete or fails                                                                                                               |
| Authentication failure               | Connection or subscription owner reports the failure; credential correction required                                          | Does not imply automatic logical-runtime termination                                                                                                 |
| L2 synchronization failure           | Integrity Processor invalidates readiness; Recovery Coordinator handles resynchronization                                     | May remain `Active`                                                                                                                                  |
| Repeated recovery failure            | Recovery policy escalates, exhausts retries, or requires intervention                                                         | Remains `Active` if the logical runtime can continue; otherwise may become `Failed`                                                                  |
|                                      |                                                                                                                               |                                                                                                                                                      |
| Explicit user deactivation           | Release persistent acquisition demand through accepted configuration reconciliation; recalculate effective acquisition demand | Preserve `Active` while independent demand requires acquisition; transition toward `Idle` or `Stopped` only when no valid acquisition demand remains |

#### Failure Semantics

The logical lifecycle enters `Failed` only when the logical runtime
cannot continue under its established execution and recovery contracts.

An activation operation may fail while its logical runtime remains
allocated and capable of handling subsequent configuration correction
or recovery.

The Stream Registry must not force a logical lifecycle transition
merely to represent connection, subscription, integrity, or health
conditions that already have authoritative state dimensions.

A transient failure must not destroy the stream descriptor.

A permanent configuration failure must not trigger unlimited retries.

All asynchronous failure and recovery results must respect the
applicable connection and subscription generation fences.

Failure classification and recovery disposition must follow the
shared error and recovery contracts defined in Documents 02 and 07.

### 6.4 Desired-State Reconciliation

The Stream Registry reconciles accepted persistent desired
configuration, current demand ownership, logical runtime lifecycle,
and authoritative operational observations.

Persistent desired state and effective acquisition demand are
distinct concepts.

The registry must not compare desired state and `StreamLifecycle`
as though they were values of the same enum.

#### Reconciliation Authority

The Configuration Manager owns accepted and persisted desired
configuration.

The Runtime Reconciler coordinates application of accepted
configuration revisions.

The Stream Registry owns:

- Logical source registration and identity.
- Demand registrations and ownership.
- Effective acquisition-demand calculation.
- Logical source-stream runtime lifecycle.
- Coordination of source activation and deactivation.

The Connection Manager owns physical connectivity.

The Subscription Manager owns native subscription state.

The responsible stream processor owns market-data integrity.

The Recovery Coordinator owns recovery-operation coordination.

The registry consumes authoritative component observations
without implementing competing state machines.

#### Reconciliation Inputs

For each registered source, reconciliation considers:

- `StreamId` and canonical `SourceStreamKey`.
- Accepted persistent desired configuration revision.
- Persistent acquisition intent.
- Outstanding demand registrations and their `DemandId` values.
- Current effective acquisition requirement.
- Current `StreamLifecycle`.
- Native subscription state and generation.
- Physical connection state and generation.
- Channel-specific processor initialization and integrity state.
- Recovery status and applicable retry policies.
- Outstanding control and configuration operations.
- Protected dependencies and pending resource reservations.

The current acquisition requirement must be derived from the
authoritative demand registrations rather than inferred solely
from persistent desired state.

#### Effective Acquisition Requirement

Conceptually:

    Persistent Desired Configuration
                  |
                  v
    Persistent Demand Registration
                  |
                  |
    Independent Demand Registrations
                  |
                  v
           Stream Registry
                  |
                  v
    Evaluate Valid Acquisition Demand
                  |
           +------+------+
           |             |
           v             v
       Required      Not Required
           |             |
           v             v
       Maintain       Idle / Stop
       Acquisition    Acquisition

Effective acquisition is required while at least one valid
demand registration requires the source.

A persistent `DesiredState::Active` contributes persistent
acquisition demand.

A persistent `DesiredState::Inactive` removes that particular
persistent acquisition requirement.

It does not invalidate independent recording, subscriber,
derived-stream, or retention demand.

#### Reconciliation Rules

| Persistent Desired State | Effective Acquisition Demand | Logical Runtime | Registry Action |
|---|---|---|---|
| `Active` | Required | `Registered` or `Stopped` | Start runtime and coordinate acquisition |
| `Active` | Required | `Starting` | Continue startup and activation |
| `Active` | Required | `Active` | Maintain acquisition or coordinate recovery |
| `Active` | Required | `Idle` | Resume acquisition |
| `Inactive` | Required by independent owners | `Registered` or `Stopped` | Start acquisition for the remaining demand owners |
| `Inactive` | Required by independent owners | `Active` | Preserve acquisition; do not unsubscribe |
| `Inactive` | Required by independent owners | `Idle` | Resume acquisition for remaining demand |
| `Inactive` | Not required | `Active` or `Starting` | Cancel unnecessary activation and coordinate idle retention or shutdown |
| `Inactive` | Not required | `Idle` | Apply the configured idle-retention policy |
| `Inactive` | Not required | `Stopped` | Preserve registration without acquisition |
| `Inactive` | Not required | `Failed` | Preserve registration and report failure |
| `Removed` | Independent demand or protected dependencies remain | Any registered lifecycle | Reject ordinary removal or apply an explicitly authorized coordinated-removal policy |
| `Removed` | No blocking demand or dependencies | Any registered lifecycle | Fence obsolete operations, release owned resources, and remove registration |

The table expresses logical reconciliation decisions.

It does not authorize the Stream Registry to perform
physical WebSocket operations, native subscription
commands, or integrity processing directly.

#### Lifecycle and Readiness

`StreamLifecycle::Active` means the logical runtime
is executing.

It does not independently mean that acquisition
readiness has been achieved.

A source with effective acquisition demand may report:

    Persistent Desired State: Inactive
    Independent Demand:       Recording
    Effective Acquisition:    Required
    StreamLifecycle:          Active
    ConnectionState:          Reconnecting
    SubscriptionState:        Pending
    AcquisitionReady:         false

This is valid.

The runtime remains active because recording requires
the source, while the responsible components restore
connectivity and acquisition readiness.

Successful acquisition activation must satisfy the
readiness requirements defined in Section 5.3.

Temporary loss of acquisition readiness does not
automatically change the logical lifecycle.

#### Persistent Deactivation

When persistent acquisition is disabled:

1. The Configuration Manager validates and persists the
   accepted desired configuration revision.
2. The Runtime Reconciler schedules application of the
   accepted change.
3. The Stream Registry releases only the affected
   persistent demand registration.
4. The registry recalculates effective acquisition demand.
5. Acquisition remains operational if independent
   demand still requires the source.
6. If no demand remains, the registry coordinates
   idle retention or shutdown according to policy.

Releasing persistent demand must not remove independent
demand registrations.

#### Source Removal

Removing a registered source is distinct from releasing
one demand registration.

A source cannot be removed merely because persistent
desired state is `Inactive`.

Removal must satisfy the ownership, dependency, and
resource-release requirements in Section 5.5.

The registry must serialize removal against concurrent
demand acquisition and dependency registration.

A rejected removal must preserve the existing logical
registration and outstanding independent demand.

#### Reconciliation Outcomes

The runtime must distinguish:

- Desired configuration accepted.
- Desired configuration revision persisted.
- Persistent demand added or released.
- Effective acquisition requirement changed.
- Logical runtime started or stopped.
- Native subscription established or removed.
- Acquisition readiness achieved or lost.
- Recovery pending, completed, or exhausted.
- Control operation completed or failed.
- Configuration revision partially or fully applied.

A command acknowledgement does not imply successful
runtime reconciliation.

Persistent deactivation may complete successfully
without stopping acquisition when independent
demand remains.

Conversely, a source may remain logically `Active`
while acquisition readiness is temporarily unavailable.

#### Reconciliation Execution

Reconciliation should be event-driven where possible.

Periodic reconciliation may serve as a safety mechanism
for missed control-plane events.

Mutations for one registered source must remain serialized.

Independent source streams may reconcile concurrently.

Before accepting an asynchronous completion, the registry
must validate:

- Current registered identity.
- Current desired configuration revision.
- Current effective acquisition demand.
- Applicable connection and subscription generations.
- Operation cancellation or supersession status.

Obsolete callbacks must not recreate released demand
or resurrect removed sources.

No network operation may execute while holding a
global Stream Registry write lock.

Reconciliation must be idempotent with respect to
repeated delivery of accepted configuration revisions
and equivalent demand changes.

---
## 7. Subscription Manager

The Subscription Manager translates logical stream activation requests into exchange-native subscriptions.

It owns subscription admission, connection assignment, request execution, acknowledgement tracking, and subscription release.

It does not own logical stream identity, exchange protocol implementations, or market-data processing.

### 7.1 Responsibilities

- Accept activation and deactivation requests from the Stream Registry.
- Resolve exchange-native subscription parameters.
- Validate subscription capabilities and constraints.
- Enforce subscription and connection admission limits.
- Select or request compatible physical connections.
- Encode subscription requests through exchange protocol adapters.
- Track pending subscription operations.
- Correlate acknowledgements with requests.
- Coordinate subscription restoration after reconnection.
- Release subscription resources after deactivation.
- Report lifecycle outcomes to the Stream Registry.

Subscription operations are asynchronous and generation-aware.

### 7.2 SubscriptionDescriptor

A compiled subscription definition contains:

| Field | Type | Purpose |
|---|---|---|
| stream_id | StreamId | Logical stream identity |
| subscription_key | SubscriptionKey | Canonical subscription identity |
| exchange | ExchangeId | Exchange identifier |
| market | MarketCategory | Spot, Linear, Inverse |
| instrument_id | InstrumentId | Canonical instrument |
| native_symbol | String | Exchange-native instrument identifier |
| native_market | NativeMarketType | Exchange-specific product category |
| channel | ChannelKind | Canonical channel |
| native_channel | String | Exchange subscription channel |
| parameters | SubscriptionParameters | Depth, frequency, variants |
| connection_requirements | ConnectionRequirements | Required endpoint and transport properties |
| format_id | FormatId | Compiled normalization specification |

The descriptor is immutable for one subscription activation.

Native subscription parameters are resolved before admission.

### 7.3 Subscription Identity

Logical stream identity and exchange subscription identity are distinct.

A single physical WebSocket connection may host multiple subscriptions.

A subscription is uniquely identified within its connection by its compiled native subscription key.

Conceptually:

    NativeSubscriptionKey =
        Exchange
        + Endpoint
        + NativeMarket
        + NativeSymbol
        + NativeChannel
        + NativeParameters

The Subscription Manager must prevent accidental duplicate native subscriptions on the same physical connection.

Initially, one logical source stream owns one native subscription.

Sharing one native subscription between multiple logical source streams is deferred.

---

## 8. Subscription Admission Control

Admission determines whether a requested subscription can be activated safely.

Admission must occur before issuing exchange-native subscription commands.

### 8.1 Admission Requirements

A subscription may be admitted only when:

- Its configuration is valid.
- The instrument exists in the authoritative catalog.
- The requested channel is supported.
- Channel parameters satisfy exchange constraints.
- Required normalization metadata is available.
- Required transport capabilities are supported.
- Subscription capacity is available.
- Applicable rate-limit budgets permit the operation.
- The stream has not been removed or superseded.

Admission failure must produce an explicit reason.

### 8.2 Admission Outcomes

| Outcome | Meaning |
|---|---|
| Admitted | Subscription may proceed |
| Deferred | Temporarily insufficient capacity or rate-limit budget |
| Rejected | Invalid or unsupported configuration |
| Cancelled | Request superseded by newer desired state |

Deferred requests remain eligible for later admission.

Rejected requests require configuration correction or explicit intervention.

### 8.3 Resource Constraints

Admission considers:

| Resource | Enforcement |
|---|---|
| Global source-stream limit | Stream Registry |
| Maximum subscribers | Publisher admission policy |
| Exchange connection limit | Connection Manager |
| Subscriptions per connection | Subscription Manager |
| Subscription request frequency | Exchange rate limiter |
| Connection attempt frequency | Connection Manager |
| Native channel restrictions | Configuration compiler |
| Memory and processing capacity | Runtime resource admission |

Limits must be enforced at their actual scope.

For example, an IP-wide exchange connection limit must not be independently allocated to every stream.

### 8.4 Rate-Limit Coordination

Subscription operations acquire permits from the appropriate exchange limiter.

Relevant operations include:

    Subscribe
    Unsubscribe
    Reconnect
    ConnectionEstablishment

Each operation consumes only the budgets applicable to its exchange protocol.

Rate-limit accounting must support:

- Per-connection limits.
- Per-IP limits.
- Per-exchange limits.
- Weighted operations where documented.
- Cooldown periods.
- Shared budgets across compatible market families.

A deferred operation must not hold a physical connection lock while waiting for a permit.

---

## 9. Connection Assignment

The Subscription Manager requests compatible physical connections from the Connection Manager.

Connection assignment is based on transport requirements, not merely exchange identity.

### 9.1 ConnectionRequirements

| Field | Purpose |
|---|---|
| exchange | Exchange identity |
| endpoint | Resolved WebSocket URL |
| authentication | Public or authenticated connection |
| transport_headers | Required handshake headers |
| heartbeat_strategy | Compatible heartbeat configuration |
| protocol_version | Exchange protocol generation |
| connection_class | Optional operational isolation class |

Connections may be shared only when their requirements are compatible.

### 9.2 Connection Pooling

The Connection Manager maintains physical connections independently of logical streams.

Example:

    OKX Public Connection
             |
             +-- BTC-USDT Spot L2
             |
             +-- BTC-USDT-SWAP Linear L2
             |
             +-- BTC-USD-SWAP Inverse L2

Compatible subscriptions may share one physical connection.

However:

    Binance Spot
    Binance Linear
    Binance Inverse

use distinct exchange endpoints and therefore require separate connection pools.

Similarly, Gate Futures settlement-specific endpoints must remain separate.

### 9.3 Connection Selection

Connection assignment follows:

    Subscription Request
             |
             v
    Resolve Requirements
             |
             v
    Find Compatible Connection
             |
       +-----+-----+
       |           |
     Found       Not Found
       |           |
       v           v
    Check       Request New
    Capacity    Connection
       |           |
       +-----+-----+
             |
             v
       Assign Stream

Selection should consider:

- Endpoint compatibility.
- Available subscription capacity.
- Existing connection health.
- Exchange connection limits.
- Operational isolation requirements.
- Connection load distribution.

Connection selection must not depend on TUI state or presentation concerns.

### 9.4 Connection Ownership

The Connection Manager owns:

- Physical sockets.
- Connection lifecycle.
- Connection generations.
- Heartbeats.
- Transport-level failures.
- Reconnection.
- Connection-level resource accounting.

The Subscription Manager owns:

- Subscription assignments.
- Subscription operation state.
- Native subscription acknowledgements.
- Subscription generations.
- Subscription restoration requests.

The Stream Registry owns:

- Logical stream identity.
- Desired state.
- Observed stream lifecycle.
- Stream configuration revisions.

These ownership boundaries must remain explicit.

---

## 10. Subscription Operations

Subscription operations are asynchronous, generation-scoped transactions.

The Subscription Manager coordinates their execution while the Stream Registry remains authoritative for desired state and lifecycle transitions.

Supported operations:

| Operation | Purpose |
|---|---|
| Subscribe | Activate a native market-data subscription |
| Unsubscribe | Deactivate an existing subscription |
| Restore | Reestablish a subscription after connection replacement |
| Replace | Change subscription-defining parameters |
| Cancel | Supersede an operation that is no longer required |

Operations must be idempotent at the logical control-plane level, even when the exchange protocol does not provide native idempotency guarantees.

### 10.1 SubscriptionOperation

Each operation carries:

| Field | Type | Purpose |
|---|---|---|
| operation_id | OperationId | Unique control-plane operation identity |
| stream_id | StreamId | Target logical stream |
| operation_kind | SubscriptionOperationKind | Subscribe, Unsubscribe, Restore, Replace |
| subscription_generation | u64 | Target subscription incarnation |
| connection_id | ConnectionId | Assigned physical connection |
| connection_generation | u64 | Physical connection incarnation |
| native_subscription_key | NativeSubscriptionKey | Exchange-native subscription identity |
| request_id | Option<RequestId> | Exchange request correlation identifier |
| created_at | Timestamp | Operation creation |
| deadline | Timestamp | Completion deadline |
| status | OperationStatus | Current execution status |

Operation state is maintained independently of the Stream Registry's authoritative lifecycle state.

### 10.2 Operation States

    Created
       |
       v
    Queued
       |
       v
    Sending
       |
       v
    AwaitingAcknowledgement
       |
       +----> Succeeded
       |
       +----> Failed
       |
       +----> TimedOut
       |
       +----> Cancelled

A timeout does not prove that the exchange rejected or failed to apply the subscription.

Consequently, timed-out operations must not be blindly retried when doing so could create ambiguous native subscription state.

The Subscription Manager must reconcile or replace the affected connection when necessary.

---

## 11. Subscription Acknowledgement Handling

Exchanges use different acknowledgement formats.

MarketForge must normalize them into a common internal result without embedding exchange-specific parsing inside the Subscription Manager.

### 11.1 Normalized Acknowledgement

    SubscriptionAcknowledgement {
        connection_id
        connection_generation
        request_id
        native_subscription_key
        operation_kind
        outcome
        exchange_error
    }

The Exchange Protocol Adapter extracts native acknowledgement fields and produces this normalized representation.

Possible outcomes:

| Outcome | Meaning |
|---|---|
| Accepted | Exchange confirmed the operation |
| Rejected | Exchange explicitly rejected the operation |
| Partial | Only part of a batch was accepted |
| Ambiguous | Result cannot be conclusively associated with an operation |

An acknowledgement must never be applied to an unrelated stream.

### 11.2 Correlation Strategy

Correlation is exchange-specific.

Supported mechanisms include:

- Explicit request identifiers.
- Native channel and instrument identities.
- Subscription argument matching.
- Connection-scoped operation ordering.
- Protocol-defined acknowledgement payloads.

The adapter selects the appropriate strategy from the compiled exchange profile.

If a protocol cannot distinguish concurrent acknowledgements reliably, the Subscription Manager must serialize the affected operations.

### 11.3 Acknowledgement Validation

Before accepting an acknowledgement:

1. Verify the connection identity.
2. Verify the connection generation.
3. Resolve the pending operation.
4. Verify the native subscription identity.
5. Verify the operation kind.
6. Verify the current subscription generation.
7. Confirm that the operation has not been superseded.

Only then may the operation transition to `Succeeded`.

Unknown, duplicate, or stale acknowledgements must be reported without modifying authoritative stream state.

### 11.4 Partial Batch Success

Subscription batching is permitted only when the exchange protocol supports it.

A batch may contain multiple logical stream operations.

Each stream must retain an independent outcome.

    Subscription Batch
           |
           +-- Stream A: Accepted
           |
           +-- Stream B: Rejected
           |
           +-- Stream C: Accepted

A partially successful batch must not be treated as entirely successful or entirely failed.

If the exchange returns an ambiguous batch-level response, the affected subscriptions remain unresolved until their state can be established.

---

## 12. Subscription Activation

A successful subscription acknowledgement establishes native subscription acceptance.

It does not necessarily establish stream readiness.

### 12.1 Activation Sequence

    Stream Registry
          |
          v
    Activation Requested
          |
          v
    Subscription Manager
          |
          v
    Admission Granted
          |
          v
    Connection Assigned
          |
          v
    Native Subscribe Sent
          |
          v
    Acknowledgement Accepted
          |
          v
    Processor Initialization
              |
              v
       Channel Readiness
              |
              v
    Acquisition Ready
              |
              v
    Activation Operation
         Completed

The sequence above describes acquisition activation, not the
`StreamLifecycle` state machine.

A logical source-stream runtime may already be
`StreamLifecycle::Active` while native subscription establishment
or channel-specific initialization remains incomplete.

The activation operation completes successfully only after the
configured acquisition-readiness requirements have been satisfied.

The processor must be prepared to receive messages before the
subscription can produce unbuffered market data.

Incoming data may arrive before the subscription acknowledgement.

The system must therefore establish routing and buffering before
sending the native subscription request.

Early messages must be handled according to the applicable buffering,
generation-fencing, and integrity contracts. They must not bypass
required initialization or synchronization.
### 12.2 Trade Readiness

A trade stream becomes operational when:

- The native subscription is confirmed.
- Message routing is established.
- The trade decoder is initialized.
- Instrument normalization metadata is available.
- The processor is ready to accept events.

Trade streams do not require order-book synchronization.

However, readiness does not establish historical continuity across previous disconnections.

### 12.3 L2 Readiness

An L2 stream additionally requires:

- A valid snapshot baseline.
- Successful snapshot/delta synchronization.
- Valid native sequence state.
- Initialized book storage.
- A valid book generation.

Native subscription acknowledgement does not establish L2 acquisition
readiness.

An L2 stream may already have `StreamLifecycle::Active` while its
order book remains uninitialized, synchronizing, or invalid.

L2 acquisition readiness requires successful completion of the
configured snapshot, sequence-continuity, and integrity-validation
requirements.

The Stream Processor owns authoritative book-integrity state.

The Recovery Coordinator coordinates the applicable recovery workflow.

These components report their authoritative operational outcomes
through typed events or status interfaces.

The Stream Registry uses those outcomes to determine whether
the acquisition activation operation has completed.

It must not redefine L2 integrity state as a logical
`StreamLifecycle` transition.

A recoverable synchronization failure may invalidate acquisition
readiness without terminating the logical source-stream runtime.

---

## 13. Subscription Deactivation

Deactivation must prevent obsolete messages from reaching active processing generations.

### 13.1 Deactivation Sequence

    Deactivation Requested
             |
             v
    Fence Subscription Generation
             |
             v
    Stop Stream-Specific Admission
             |
             v
    Issue Native Unsubscribe
             |
             v
    Await Acknowledgement
             |
             v
    Release Subscription Assignment
             |
             v
          Inactive

Generation fencing must occur before waiting for the exchange acknowledgement.

Messages already accepted by the previous generation are handled according to the processor's shutdown and drain policy.

No new messages from that generation may enter a replacement stream incarnation.

### 13.2 Unsubscription Failure

A failed or timed-out unsubscribe does not automatically prove that the native subscription has stopped.

Possible actions:

- Retry when the protocol safely permits it.
- Reconcile the connection's native subscription state.
- Close and replace the physical connection.
- Escalate when isolation cannot be guaranteed.

The registry must not report a cleanly completed deactivation while an uncontrolled native subscription can still deliver data into active processing.

A connection may be closed only after accounting for other streams sharing it.

---

## 14. Reconnection and Subscription Restoration

Connection recovery and subscription restoration are separate responsibilities.

The Connection Manager restores physical connectivity.

The Subscription Manager restores the native subscriptions required by the Stream Registry's desired state.

### 14.1 Connection Loss

Physical connection loss is handled by the Connection Manager.

The Connection Manager owns physical reconnection, connection
lifecycle transitions, and connection generations.

The Subscription Manager owns native subscription assignments
and subscription restoration.

The Stream Registry owns logical stream identity, desired state,
and the `StreamLifecycle` of each affected source stream.

#### Connection Failure Sequence

Conceptually:

    Connection Lost
          |
          v
    Connection Manager
          |
          v
    Fence Failed Connection Generation
          |
          v
    Enter ConnectionState::Reconnecting
          |
          v
    Notify Subscription Manager
          |
          v
    Invalidate Affected Native Assignments
          |
          v
    Fence Obsolete Subscription Operations
          |
          v
    Report Acquisition Readiness Loss
          |
          v
    Coordinate Recovery

The sequence describes the responsibilities triggered by
connection loss, not a new source-stream lifecycle transition.

#### Logical Stream Behavior

A source stream whose logical runtime remains operational
may continue reporting:

    DesiredState:       Active
    StreamLifecycle:    Active
    ConnectionState:    Reconnecting
    SubscriptionState:  Pending
    RecoveryState:      Recovering

These states belong to independent authoritative dimensions.

Connection loss must not automatically transition the
logical stream lifecycle to `Failed`, `Stopped`, or another
state merely to represent physical reconnection.

The logical `StreamId` and canonical `SourceStreamKey`
remain unchanged.

Pending operations associated with the failed connection
generation become invalid.

Stale messages, acknowledgements, or recovery completions
must not mutate current subscription or processing state.

#### Data Readiness

Connection loss may invalidate acquisition readiness even
when the logical runtime remains `Active`.

The responsible stream processor and recovery components
determine whether the interruption requires:

- Trade-continuity evaluation.
- Historical trade recovery.
- L2 book invalidation.
- Snapshot and sequence resynchronization.
- Historical coverage reporting.

A disconnected stream must not continue reporting acquisition
readiness when its required connectivity or integrity
conditions are no longer satisfied.

The Connection Manager reports the physical failure.

It does not independently determine canonical market-data
integrity or historical coverage.
### 14.2 Restoration Sequence

Physical connection restoration and source-stream acquisition
readiness are separate outcomes.

A successfully reconnected WebSocket does not automatically
restore native subscriptions or valid market-data processing.

#### Restoration Sequence

Conceptually:

    New Connection Established
              |
              v
    Publish New Connection Generation
              |
              v
    Resolve Current Required Subscriptions
              |
              v
    Revalidate Subscription Requirements
              |
              v
    Acquire Admission Permits
              |
              v
    Restore Native Subscriptions
              |
              v
    Confirm Native Subscription State
              |
              v
    Reinitialize or Restore Stream Processors
              |
              v
    Restore Channel-Specific Integrity
              |
              v
    Evaluate Acquisition Readiness
              |
              v
    Acquisition Ready

The sequence represents acquisition restoration.

It must not be interpreted as requiring the logical
source-stream runtime to leave and re-enter
`StreamLifecycle::Active`.

#### Desired-State and Demand Validation

Restoration must respect the current authoritative desired
state and acquisition-demand ownership.

The Subscription Manager must not blindly restore every
subscription associated with the previous connection.

Before restoration, the responsible components must verify:

- The logical source stream still exists.
- The stream has not been removed or superseded.
- Current effective acquisition demand requires restoration, including when persistent desired state is `Inactive` but independent demand remains valid.
- Required acquisition demand remains registered.
- The subscription definition remains valid.
- The selected connection satisfies transport requirements.
- Applicable resource and rate-limit budgets permit restoration.
- The restoration operation targets current generations.

A stream deactivated or removed while its connection was
unavailable must not be resubscribed merely because it was
active before the disconnection.

Restoration must not recreate obsolete acquisition demand.

#### Restoration Outcomes

The following outcomes must remain distinguishable:

| Outcome | Meaning |
|---|---|
| Connection restored | The physical transport satisfies its connectivity requirements. |
| Native subscription restored | The exchange-specific subscription has been established or confirmed. |
| Processor initialized | The responsible processor can accept messages under its current generation. |
| Acquisition readiness restored | All required channel-specific readiness conditions are satisfied. |
| Historical coverage restored | Previously missing market-data intervals have been recovered or otherwise resolved according to the coverage contract. |

Successful connection restoration does not imply successful
subscription restoration.

Successful subscription restoration does not imply successful
processor initialization or market-data integrity.

Successful acquisition readiness does not imply complete
historical coverage.

#### Logical Runtime During Restoration

A logical source-stream runtime may remain
`StreamLifecycle::Active` throughout restoration.

For example, an L2 stream may report:

    DesiredState:       Active
    StreamLifecycle:    Active
    ConnectionState:    Connected
    SubscriptionState:  Active
    IntegrityState:     Synchronizing
    RecoveryState:      Recovering
    AcquisitionReady:   false

Once its configured snapshot and sequence synchronization
requirements have been satisfied, acquisition readiness
may be restored without changing `StreamLifecycle`.

Recovery completion must be reported through the
authoritative recovery and integrity interfaces.

The Stream Registry must not independently declare
an order book valid or historical coverage complete.

#### Restoration Failure

A failed restoration operation must produce an explicit
operation outcome and preserve the relevant failure context.

Recoverable failures follow their configured retry,
cooldown, and escalation policies.

A temporary restoration failure must not destroy the
logical stream identity or silently change desired state.

All restoration results must respect the current
connection and subscription generations.
### 14.3 Generation Rules

A restored subscription receives a new `SubscriptionGeneration`.

The replacement physical connection has a new `ConnectionGeneration`.

The Stream Registry accepts restoration completion only when both generations match the current assignment.

This prevents delayed events from obsolete connections from reviving old subscriptions.

### 14.4 Recovery Differences

| Stream type | Restoration requirement |
|---|---|
| Trades | Resubscribe, restore processing, evaluate missing coverage |
| L2 Depth | Resubscribe, invalidate old book, establish new synchronized generation |

Trade recovery may retrieve missing executions through exchange-supported REST mechanisms.

L2 restoration normally requires a fresh authoritative snapshot.

Restoring current L2 state must not be interpreted as reconstructing every update missed during the outage.

### 14.5 Shared Connection Failure

A single physical connection failure may affect multiple source streams.

The Connection Manager emits one connection failure event.

The Subscription Manager determines the affected native assignments.

The Stream Registry updates the affected streams' authoritative
control-plane observations and coordinates reconciliation independently.

It must not treat connection failure or subscription restoration
as an automatic logical `StreamLifecycle` transition.

Connection, subscription, integrity, and recovery states
remain authoritative in their respective owning subsystems.

Recovery must avoid launching redundant physical reconnections for every affected stream.

---
## 15. Runtime Stream Mutations

The Stream Registry supports dynamic registration, activation,
deactivation, removal, replacement, and reconfiguration of logical
source streams.

These operations must respect the separation between:

- Persistent desired configuration.
- Logical source-stream identity.
- Independent acquisition-demand ownership.
- Effective acquisition requirements.
- Logical runtime lifecycle.
- Physical connection and native subscription lifecycle.
- Channel-specific acquisition readiness.

The Configuration Manager owns persistent desired configuration
and revision acceptance.

The Runtime Reconciler coordinates application of accepted
configuration revisions.

The Stream Registry owns source identity, demand registrations,
effective acquisition requirements, and logical runtime lifecycle.

### 15.1 Stream Operations

| Operation | Semantics |
|---|---|
| Register | Register or reuse a logical source identified by its canonical `SourceStreamKey`. |
| Activate | Enable persistent acquisition intent for a registered source. |
| Deactivate | Disable persistent acquisition intent without releasing independent demand. |
| Remove | Remove a logical registration only after demand and dependency constraints are satisfied. |
| Replace | Coordinate native subscription replacement or semantic source replacement according to their distinct identity contracts. |
| Reconfigure | Apply supported source configuration changes through accepted revisions and runtime reconciliation. |
| Restart | Reinitialize an existing logical runtime or its acquisition components without implicitly changing source identity or demand ownership. |

These are conceptual runtime operations.

Document 13 defines the authoritative external Control API
command vocabulary.

This section does not independently establish serialized
Control API variants.

### 15.2 Persistent Configuration Mutations

The following external operations affect persistent configuration:

    RegisterStream
    ActivateStream
    DeactivateStream
    RemoveStream

Registration and removal require Configuration Manager involvement
when persistent source definitions are created or deleted.

Activation and deactivation modify persistent acquisition intent
and must follow the authoritative configuration revision workflow.

The required ordering is:

    Validate Proposed Configuration
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
    Persist Accepted Desired Revision
              |
              v
    Publish Accepted Revision
              |
              v
    Runtime Reconciliation

The Stream Registry must not directly mutate persistent
configuration outside this workflow.

An accepted configuration revision does not imply completed
runtime reconciliation or acquisition readiness.

### 15.3 Independent Acquisition Demand

Transient and subsystem-owned acquisition requirements use the
shared demand contracts defined in Document 02, Section 8.

Conceptually:

    AcquireSourceDemand
    ReleaseSourceDemand

Each acquired demand registration has an unambiguous `DemandId`.

Releasing one demand must affect only the corresponding
ownership claim.

Persistent activation and deactivation must not be used
as substitutes for acquiring or releasing transient subscriber,
recording, derived-stream, or retention demand.

For example:

    Persistent Desired State: Inactive
    Recording Demand:         Present
    Subscriber Demand:        Present
    Effective Acquisition:    Required
    StreamLifecycle:          Active

This is valid.

The Stream Registry must preserve acquisition while independent
demand remains.

Native unsubscription and logical runtime shutdown are permitted
only when effective demand and resource-ownership policies allow them.

### 15.4 Internal Runtime Operations

The following may occur without creating a new persistent
configuration revision:

- Restoring unchanged native subscriptions after reconnection.
- Retrying eligible subscription operations.
- Resuming acquisition required by existing demand.
- Reinitializing processors during integrity recovery.
- Performing an authorized logical runtime restart.
- Releasing acquisition resources when effective demand disappears.

These operations must respect the current accepted configuration,
effective acquisition demand, and applicable generations.

They must not silently modify canonical acquisition semantics.

The Connection Manager and Subscription Manager remain
authoritative for their respective resources.

### 15.5 Command Results and Completion

Runtime operations may be asynchronous.

Command acceptance must remain distinct from:

- Configuration persistence.
- Persistent-demand reconciliation.
- Logical runtime startup.
- Native subscription establishment.
- Acquisition readiness.
- Resource cleanup.
- Source registration removal.

For persistent activation, successful reconciliation requires
the corresponding persistent demand to be established.

Acquisition readiness is a separate result unless explicitly
required by the operation's declared completion contract.

For persistent deactivation, successful reconciliation requires
the persistent demand to be released and remaining effective
acquisition demand to be evaluated.

Independent demand may keep the source operational.

For removal, successful completion requires dependency validation,
generation fencing, required resource cleanup, and deletion
of the authoritative logical registration.

Operation statuses, request correlation, and external result
schemas follow Document 13.

### 15.6 Idempotency and Concurrency

Mutations affecting the same registered source must be serialized
or coordinated through an equivalent atomic mechanism.

Independent sources may be modified concurrently.

Repeated registration of an equivalent `SourceStreamKey`
must not create duplicate logical acquisitions.

Repeated persistent activation must not create duplicate
persistent demand registrations.

Repeated persistent deactivation must not release unrelated demand.

Repeated removal must not release resources more than once
or resurrect an obsolete registration.

Concurrent source removal and demand registration must be
coordinated atomically with dependency-management operations.

An obsolete asynchronous completion must not override newer
accepted configuration or generation state.

No network operation may execute while holding a global
Stream Registry write lock.

### 15.7 Restart and Replacement Safety

Restarting a logical source runtime must not implicitly:

- Change its `SourceStreamKey`.
- Allocate a new logical `StreamId`.
- Release independent demand registrations.
- Delete previously accepted canonical events.
- Reset event cursors without an authorized log-epoch transition.
- Report historical coverage as complete.
- Treat native subscription establishment as proof of L2 integrity.

Native subscription replacement preserves the existing logical
source identity when acquisition semantics remain unchanged.

A semantic acquisition change requires the distinct identity
and dependency rules defined in Section 16.

The exact external restart command and supported restart scopes
remain unresolved Control API contracts.

This section does not introduce a `RestartStream`
wire-protocol variant.

### 15.8 Runtime Mutation Invariants

- Persistent configuration has one authoritative owner.
- Accepted desired configuration is persisted before reconciliation.
- Logical source identity is independent of connection assignment.
- Equivalent acquisition requirements share one logical source.
- Each demand owner controls only its own acquisition claims.
- Persistent deactivation must preserve independent demand.
- Connection and subscription recovery do not independently
  change persistent desired configuration.
- Logical runtime activity does not imply acquisition readiness.
- Source removal must respect protected dependencies.
- All asynchronous operations must respect revision and
  generation fencing.
- Runtime mutations must not bypass established resource limits.

## 16. Subscription Replacement

Changing subscription-defining parameters requires controlled replacement.

Examples:

- Changing L2 depth.
- Changing update frequency.
- Switching native exchange channels.
- Changing the subscribed instrument.
- Changing the native market category.

These changes must not be treated as ordinary hot-editable settings.

### 16.1 Replacement Semantics

Subscription replacement and logical source identity replacement
are distinct operations.

The authoritative logical acquisition identity is
`SourceStreamKey`, defined in Document 02, Section 3.5.

The authoritative registered logical identity is `StreamId`.

A native subscription is identified through its compiled
subscription requirements and connection-scoped native identity.

These identities must not be conflated.

#### Native Subscription Replacement

A native subscription may require replacement without changing
the semantic acquisition represented by its `SourceStreamKey`.

Examples include:

- Restoring a subscription after physical connection replacement.
- Migrating an unchanged subscription to another compatible connection.
- Replacing an ambiguous or failed native subscription incarnation.
- Reestablishing an unchanged subscription after controlled recovery.

When acquisition semantics remain unchanged:

    SourceStreamKey:        Unchanged
    StreamId:               Unchanged
    ConnectionGeneration:   Changes if the physical connection changes
    SubscriptionGeneration: New incarnation
    BookGeneration:         Changes if authoritative L2 synchronization
                            must be reestablished

The logical source stream retains its registration and identity.

Native subscription replacement must not independently create
a duplicate logical source acquisition.

The Subscription Manager owns the native replacement operation.

The Stream Registry retains authority over the logical source
identity, desired state, and lifecycle.

#### Semantic Acquisition Replacement

A change affecting the canonical acquisition specification
requires a new `SourceStreamKey`.

Examples include:

- Changing the subscribed exchange.
- Changing the market family.
- Changing the canonical instrument.
- Changing the channel's semantic type.
- Changing L2 depth when depth defines the acquired feed.
- Changing update frequency when it affects acquisition semantics.
- Changing an exchange-native channel variant that changes
  the requested market-data feed.

Such changes must not silently mutate the `SourceStreamKey`
of an existing registered source.

Conceptually:

    Existing Source
          |
          v
    Validate Proposed Acquisition
          |
          v
    Resolve New SourceStreamKey
          |
          v
    Check for Equivalent Registered Source
          |
          +---- Existing Equivalent Source
          |                |
          |                v
          |          Acquire Existing Source
          |
          +---- No Equivalent Source
                           |
                           v
                    Register New Source
                           |
                           v
                    Activate New Acquisition
          |
          v
    Reconcile Consumer Dependencies
          |
          v
    Release Superseded Source Demand

The new acquisition uses its own logical `StreamId`.

The registry must first check whether the proposed
`SourceStreamKey` already exists.

An equivalent registered source must be reused rather
than duplicated.

The old source must remain available while existing
demand owners still require it.

Replacing a configured source definition must not
silently change the acquisition identity of other
subscribers, recording requirements, or derived streams
that depend on the old source.

#### Controlled Replacement

When the exchange and available resources permit it,
replacement should prepare the new acquisition before
releasing the old one.

Conceptually:

    Validate Replacement
             |
             v
    Resolve Canonical Identity
             |
             v
    Reserve Required Resources
             |
             v
    Establish Required Native Subscription
             |
             v
    Initialize New Processor
             |
             v
    Verify Acquisition Readiness
             |
             v
    Coordinate Authoritative Switch
             |
             v
    Release Superseded Demand and Resources

The switch must preserve the applicable identity,
generation, dependency, and configuration-revision
contracts.

For a native-only replacement, the logical stream identity
remains unchanged.

For a semantic acquisition replacement, the new source
has a distinct logical identity and its own event sequence.

A replacement operation must not reuse an incompatible
source identity or silently reinterpret previously
published canonical events.

### 16.2 Replacement Policies

Two replacement policies are supported:

| Policy | Behavior |
|---|---|
| PrepareThenSwitch | Establish replacement before releasing the old subscription |
| StopThenStart | Release existing subscription before activating replacement |

`PrepareThenSwitch` minimizes downtime but temporarily consumes additional resources.

`StopThenStart` reduces resource consumption but introduces a coverage interruption.

The selected policy must be explicit.

### 16.3 Replacement Atomicity

Replacement must preserve authoritative identity,
generation, event-routing, and resource-ownership invariants.

#### Native Subscription Replacement

For a replacement that preserves `SourceStreamKey`
and `StreamId`:

- Old and new subscription generations may temporarily
  coexist internally when the selected replacement policy permits it.
- Only one subscription generation may be authoritative
  for canonical event acceptance at a time.
- Inbound messages must be validated against the
  current connection and subscription generations.
- Obsolete-generation messages must not enter current
  processing state.
- Duplicate native events must not be silently
  published as independent executions.
- L2 book generations must remain isolated.
- Failure to initialize the replacement must not
  corrupt the existing valid acquisition.
- The authoritative subscription generation changes
  only after the required replacement-readiness
  conditions have been satisfied.

Replacing a native subscription does not automatically
change `StreamLifecycle::Active`.

The logical runtime may remain active while its
subscription and integrity components coordinate
replacement.

Acquisition readiness must reflect the actual
subscription and integrity conditions throughout
the operation.

#### Semantic Acquisition Replacement

For a replacement that changes `SourceStreamKey`:

- The new acquisition must resolve to a different
  logical source identity from the superseded acquisition.
- An already registered equivalent source must be reused.
- The old and new sources have independent `StreamId`
  values and event-cursor namespaces.
- Existing consumers must not be silently redirected
  to a semantically different source.
- Persistent configuration and dependency changes
  must follow their authoritative revision contracts.
- Source-demand ownership must be transferred or
  released according to the approved reconciliation plan.
- The old source remains registered and operational
  while independent demand owners still require it.
- Failure to activate the replacement must not
  invalidate an otherwise healthy old source.
- Resource reservations and partially established
  replacement resources must be released on failure.

Switching a configured consumer from one logical source
to another is a dependency or configuration transition,
not a mutation of the old source's identity.

#### Generation and Event Safety

The system must prevent asynchronous operations from
obsolete connection or subscription generations from
modifying newer authoritative state.

Canonical events must retain their original source
identity, cursor, and generation provenance.

Replacement must not retrospectively change the meaning
of previously accepted events.

For L2 streams, new synchronization requirements must
be satisfied before publishing data requiring a valid
synchronized book.

For trade streams, the replacement must preserve
applicable native-trade deduplication and continuity
requirements.

Historical coverage across the replacement boundary
must be evaluated separately.

Successful current acquisition readiness must not be
interpreted as proof that no market-data events were
missed during replacement.

#### Resource and Failure Safety

Replacement must respect:

- Subscription admission and connection-capacity limits.
- Exchange-imposed rate limits and cooldowns.
- Explicit replacement policy.
- Current desired configuration.
- Current acquisition-demand ownership.
- Generation fencing.
- Processor and dependency ownership.
- Recording and retention guarantees.

An unsuccessful replacement must produce an explicit
operation outcome.

It must not silently destroy the previous acquisition,
leak resource reservations, or report a configuration
revision as fully applied when its required transition
has not completed.

---

## 17. Configuration Revisions

Every registered stream references a validated configuration revision.

Configuration revisions provide consistency across runtime changes and asynchronous operations.

### 17.1 Revision Model

    ConfigurationRevision {
        revision_id
        configuration_hash
        created_at
        source
        validation_status
    }

Each stream records the revision currently applied to its runtime.

A revision identifies the effective compiled configuration, including relevant exchange, channel, instrument, and runtime settings.

### 17.2 Configuration Change Classification

Configuration changes use the authoritative `ReconfigurationPolicy`
contract defined in Document 03, Section 14.1, and Document 02,
Section 12.5.

```rust
pub enum ReconfigurationPolicy {
    Hot,
    StreamTransition,
    ConnectionReplacement,
    ServiceRestart,
    Immutable,
}
```

| Policy | Semantics |
|---|---|
| `Hot` | Apply a supported setting without restarting the affected runtime resource. |
| `StreamTransition` | Create, stop, or replace affected stream resources through a controlled transition. |
| `ConnectionReplacement` | Replace affected physical connections when explicitly supported. |
| `ServiceRestart` | Persist valid desired configuration that requires a daemon restart before becoming effective. |
| `Immutable` | Reject changes that are incompatible with the current deployment or schema. |

These policies describe how an accepted configuration change
may be applied.

They are distinct from runtime operation states, subscription
operation kinds, and failure outcomes.

For example:

- A stream restart is a possible operation under `StreamTransition`.
- A native subscription replacement is a possible operation under
  `StreamTransition`.
- A physical connection replacement is governed by
  `ConnectionReplacement`.
- An invalid proposal is rejected during validation; rejection
  is not an additional `ReconfigurationPolicy` variant.

The Configuration Manager and compiler determine configuration
validity, semantic differences, and applicable policies.

The Runtime Reconciler coordinates permitted application
through the authoritative subsystem owners.

The Stream Registry coordinates only the logical stream
identity, desired-state, and lifecycle changes within its
ownership boundary.

The initial support restrictions in Document 03 remain
authoritative. Defining a policy does not imply that every
operation governed by that policy is available for hot editing.

### 17.3 Hot-Editable Settings

The initial global hot-editable settings remain those defined in **03 — Configuration System**.

| Setting | Application semantics |
|---|---|
| Logging level | Immediate |
| Metrics enabled | Immediate where supported |
| Detailed timing | Subsequent measurements |
| Metrics snapshot interval | Subsequent collection cycles |
| Recording batch size | Next batch boundary |
| Recording flush interval | Next batch boundary |
| Maximum subscribers | Future subscriber admission |

These settings must not trigger unnecessary stream resubscriptions.

Recording settings are applied through the Recording Manager.

Subscriber limits are enforced by the Publisher.

The Stream Registry coordinates only changes affecting stream lifecycle or configuration ownership.

### 17.4 Revision Application

Configuration acceptance and runtime application are separate
operations.

The Configuration Manager owns validation, revision conflict
detection, acceptance, and durable persistence of desired state.

The Runtime Reconciler coordinates subsequent application
through the authoritative subsystem owners.

#### Configuration Acceptance

The required ordering is:

    Proposed Configuration
              |
              v
       Structural and
       Semantic Validation
              |
              v
     Compile Effective Plan
              |
              v
     Calculate Semantic Diff
              |
              v
     Check Expected Revision
              |
              v
     Persist Accepted Desired
           Revision
              |
              v
     Publish Accepted Revision

No persistent desired-state mutation may be applied to runtime
resources before its accepted revision has been committed
according to the Configuration Store's durability contract.

A persistence failure must prevent publication of the candidate
as an accepted desired revision.

#### Runtime Reconciliation

After acceptance and persistence:

    Published Desired Revision
              |
              v
     Identify Affected Resources
              |
              v
     Classify Required Transitions
              |
              v
     Coordinate Subsystem Owners
              |
              v
     Evaluate Applied State
              |
              v
     Report Reconciliation Outcome

The Stream Registry participates only in transitions affecting
its authoritative resources.

Connection replacement, native subscription operations,
processing integrity, and recording changes remain owned
by their corresponding subsystems.

#### Revision Tracking

The system must distinguish:

- Accepted desired configuration revision.
- Applied configuration revision for each affected resource.
- Actual runtime state.
- Outstanding reconciliation operations.
- Failed or deferred transitions.

An accepted desired revision does not imply successful
runtime application.

A resource reports a revision as applied only when its required
transition has completed successfully.

For a `ServiceRestart` change, the accepted desired revision
may remain newer than the currently applied revision until
the required restart and subsequent reconciliation succeed.

A failed or incomplete transition must not cause the registry
to report a configuration revision that the affected resource
has not actually adopted.

#### Asynchronous Operations

Configuration reconciliation may involve long-running
operations.

Command acceptance must not be presented as operation
completion.

Pending operations must retain their relevant configuration
revision and generation context.

Delayed completion events must be checked against the current
authoritative revision, desired state, and applicable generation
fences before modifying runtime state.

Partial convergence must remain explicitly observable.

### 17.5 Configuration Failure

Configuration failures must distinguish validation,
persistence, and runtime reconciliation.

#### Validation Failure

If a proposed configuration is invalid:

- Reject the proposal before acceptance.
- Preserve the previously accepted desired revision.
- Do not mutate affected runtime resources.
- Report the specific validation failure.
- Do not interrupt unrelated streams.

#### Persistence Failure

If an otherwise valid proposed revision cannot be durably
committed:

- Do not publish it as an accepted desired revision.
- Preserve or recover the last committed desired revision.
- Do not begin runtime reconciliation for the failed candidate.
- Report the persistence failure explicitly.

The Configuration Store must recover consistently from
interrupted writes without combining partially committed
revision contents.

#### Reconciliation Failure

If an accepted revision cannot be fully applied:

- Preserve the accepted desired revision.
- Report the affected resources and failed transitions.
- Preserve previously valid runtime resources where permitted.
- Retry only according to the applicable recovery policy.
- Keep desired and applied revision information distinct.
- Do not report full convergence until all required conditions
  have been satisfied.

An exchange outage may prevent convergence without invalidating
the accepted desired configuration.

#### Rollback

Rollback is a new validated configuration change.

It creates a new revision based on the contents of an earlier
accepted configuration.

Rollback must not reuse historical revision identifiers.

Rollback must follow the same validation, persistence,
publication, and runtime reconciliation ordering as any
other configuration update.

Rollback does not reverse previously published canonical events,
database transactions, or committed retention operations.

Automatic rollback requires an explicitly defined and
authorized policy. It must not occur merely because an
otherwise valid revision has not yet converged.

---

## 18. Stream Dependencies

Source streams may be referenced by derived streams, including merges and currency-conversion pipelines.

Dependency ownership belongs to a separate dependency-aware component.

The Stream Registry exposes the necessary source-stream lifecycle information.

### 18.1 Dependency Model

A derived stream may depend on multiple source streams.

Example:

    BTCUSDT Trades ─────┐
                        |
    BTCUSDT L2 ─────────┼──> Derived Stream
                        |
    Currency Rate ──────┘

Dependencies reference stable `StreamId` values.

Physical connection identities must never be used as dependency identifiers.

### 18.2 Dependency States

The dependency manager evaluates source availability using registry state and stream-integrity information.

Possible dependency conditions:

| Condition | Meaning |
|---|---|
| Available | Required source is operational |
| Synchronizing | Source is initializing or recovering |
| Degraded | Source has reduced integrity or availability |
| Unavailable | Source is inactive, failed, or removed |

The derived-stream policy determines how these conditions affect downstream processing.

For example, an L2-dependent feature may require synchronized book state, while a trade-only calculation may tolerate a temporary coverage gap if it is explicitly represented.

### 18.3 Source Removal

Removing a source stream referenced by active derived streams requires dependency coordination.

Supported policies:

    RejectRemoval
    CascadeDeactivate
    CascadeRemove

The initial default should be:

    RejectRemoval

unless the request explicitly specifies a supported cascading operation.

This prevents accidental destruction of active merge or currency-conversion pipelines.

### 18.4 Source Recovery

When a source stream reconnects:

- Its logical `StreamId` remains stable.
- Its connection and subscription generations may change.
- Its book generation may change.
- Dependent streams receive the updated availability state.
- Derived processors decide whether to pause, resume, or reinitialize.

The Stream Registry must not implement downstream merge ordering or currency-conversion logic.

### 18.5 Dependency Consistency

Dependency mutations and stream removal must be coordinated to prevent races.

A source cannot be removed after a dependency check if another operation has concurrently registered a new dependency on it.

The dependency manager and registry must therefore use a shared revision check, serialized transaction, or equivalent atomic coordination mechanism.

---

## 19. Registry Persistence and Restart Recovery

The Stream Registry maintains authoritative runtime identity,
demand ownership, and logical lifecycle state.

Persistent desired configuration is owned by the Rust
Configuration Manager.

The Stream Registry must not create a competing persistent
configuration store.

At startup, the registry reconstructs its required
registrations and persistent demand from the last accepted,
durably committed desired configuration revision.

Runtime-only state is reconstructed through normal
initialization and reconciliation.

Python orchestration may generate configuration inputs
or submit supported management requests, but it is not
required to be running for MarketForge Live to start,
operate, or recover.

Operational logs remain local. PostgreSQL is reserved
for optional market-data recording.

### 19.1 Persistence Boundaries

The system distinguishes three categories:

1. Persistent configuration and logical identity.
2. Persistent acquisition requirements.
3. Transient runtime state.

#### Persistent Configuration and Identity

The Configuration Manager is authoritative for:

- Accepted desired configuration revisions.
- Persistently configured source definitions.
- Persistently configured acquisition intent.
- Persistently configured recording requirements.
- Persistently configured derived-stream definitions.
- Other approved persistent resource settings.

The Stream Registry must preserve stable logical
`StreamId` assignments for unchanged persistent
source acquisitions across daemon restarts.

The authoritative semantic acquisition identity
remains `SourceStreamKey`.

Persistent identity mappings must be recoverable
without depending on an obsolete runtime generation.

The exact durable identity-mapping representation
remains a shared configuration and registry contract
to finalize before implementation.

#### Persistent Acquisition Requirements

Persistent demand requirements are reconstructed
from accepted desired configuration.

Examples include:

- Enabled persistent source acquisition.
- Enabled recording that requires source acquisition.
- Persistently enabled derived-stream dependencies.

Each persistent requirement must resolve to an
unambiguous ownership claim.

Multiple persistent requirements for the same
`SourceStreamKey` must reuse one registered
logical acquisition.

The Stream Registry reconstructs demand registrations
from authoritative persistent declarations.

It must not assume that the in-memory `DemandId`
values from a previous process remain valid.

Reconstruction must not create duplicate demand
registrations when equivalent persistent requirements
are encountered repeatedly.

The precise representation and restart-stability
requirements of `DemandId` remain part of the
shared demand-ownership contract.

#### Transient Runtime State

Transient runtime state includes:

- Physical WebSocket connections.
- Connection assignments and lifecycle states.
- Native subscription assignments.
- Connection and subscription generations.
- Pending network operations.
- Processor execution state.
- L2 book synchronization state.
- Recovery operation progress.
- Transient subscriber demand.
- Temporary session-scoped acquisition claims.
- Runtime-only lifecycle observations.

These must not be restored as though they
remained operational across a process restart.

Transient subscriber demand must not become
persistent acquisition demand merely because
it existed before shutdown.

#### Ownership and Storage Boundaries

| Information | Authoritative Owner | Persistence |
|---|---|---|
| Accepted desired configuration | Configuration Manager | Durable configuration store |
| Persistent source definitions | Configuration Manager | Durable desired configuration |
| Stable logical source identity | Stream Registry | Recoverable identity mapping |
| Persistent acquisition requirements | Configuration Manager and corresponding resource owners | Durable desired configuration |
| Current demand registrations | Stream Registry | Reconstructed from persistent requirements; transient claims are not restored |
| Logical runtime lifecycle | Stream Registry | Runtime memory |
| Physical connection state | Connection Manager | Runtime memory |
| Native subscription state | Subscription Manager | Runtime memory |
| Connection and subscription generations | Respective lifecycle owners | Reestablished safely at runtime startup |
| Processor and L2 integrity state | Stream Processor | Reinitialized according to integrity contracts |
| Recovery operation state | Recovery Coordinator | Reconstructed as required by recovery contracts |
| Metrics | Metrics Registry | Runtime memory unless independently specified |
| Operational logs | Logging infrastructure | Local files |
| Optional market-data recording | Recording Manager | PostgreSQL |
| Retained market data | Retention Manager | RAM and Parquet according to retention guarantees |

The Configuration Manager owns persistence of desired
configuration.

The Stream Registry owns runtime registration and
demand reconciliation.

The Connection Manager, Subscription Manager,
Stream Processor, and Recovery Coordinator remain
authoritative for their respective runtime states.

#### Startup Reconstruction Invariants

- Accepted desired configuration must be loaded
  before persistent resource reconciliation.
- Identical persistent source definitions must
  resolve to stable logical identities.
- Equivalent acquisition requirements must not
  create duplicate source registrations.
- Persistent demand must be reconstructed from
  authoritative declarations.
- Transient subscriber demand must not be restored.
- Runtime generations must not be mistaken for
  durable source identities.
- Previously active native subscriptions must
  not be assumed to remain established.
- Previously synchronized L2 books must not
  be assumed to remain valid.
- Runtime startup must not depend on a running
  Python orchestration process.
- Reconciliation must respect current accepted
  configuration revisions and resource ownership.

A successful configuration load does not imply
that any physical connection, native subscription,
or source acquisition is ready.

Readiness is established independently through
the normal runtime activation contracts.

### 19.2 Runtime Startup

Runtime startup reconstructs the Stream Registry from authoritative,
persisted desired configuration.

Previous in-memory connection, subscription, processor, and
integrity states must not be treated as valid after a process restart.

#### Startup Sequence

Conceptually:

    Load Persisted Desired Configuration
                  |
                  v
    Validate Configuration Revision
                  |
                  v
    Compile Effective Configuration
                  |
                  v
    Resolve Instrument Metadata
                  |
                  v
    Reconstruct Source Stream Descriptors
                  |
                  v
    Initialize Stream Registry
                  |
                  v
    Restore Persistent Demand Ownership
                  |
                  v
    Reconcile Desired Acquisition State
                  |
                  v
    Start Required Logical Runtimes
                  |
                  v
    Coordinate Connection and Subscription Activation
                  |
                  v
    Restore Channel-Specific Acquisition Readiness

The sequence represents startup responsibilities and their
logical dependencies.

Connection establishment, subscription restoration,
processor initialization, and channel-specific integrity
synchronization may proceed asynchronously according
to their respective subsystem contracts.

#### Logical Runtime Lifecycle

Newly reconstructed registered streams initially follow
the authoritative `StreamLifecycle` contract in
Document 02, Section 7.2.

A stream required by current desired configuration and
demand ownership may transition through:

    StreamLifecycle::Registered
                |
                v
    StreamLifecycle::Starting
                |
                v
    StreamLifecycle::Active

`StreamLifecycle::Active` means the logical runtime is
running and capable of coordinating acquisition and recovery.

It does not mean that physical connectivity, native
subscription establishment, or market-data readiness
has completed.

#### Acquisition Readiness

A restored stream must not report acquisition readiness
until its configured requirements have been satisfied.

For trade streams, this includes native subscription
confirmation, processor initialization, required metadata,
and applicable integrity requirements.

For L2 streams, this additionally includes successful
snapshot and sequence synchronization.

A stream may therefore report:

    DesiredState:       Active
    StreamLifecycle:    Active
    ConnectionState:    Connecting
    SubscriptionState:  Pending
    IntegrityState:     Uninitialized
    AcquisitionReady:   false

Once its authoritative components satisfy the configured
readiness requirements, acquisition readiness may become
true without another logical lifecycle transition.

#### Persistent and Transient Demand

Startup restores persistent acquisition requirements
from accepted desired configuration.

Persistent recording and derived-stream dependencies
must be restored according to their ownership contracts.

Transient subscriber demand and temporary session
ownership must not be reconstructed as persistent
acquisition requirements.

Restoring one persistent demand owner must not create
a duplicate logical source acquisition when an equivalent
source is already registered.

#### Startup Safety

Startup must allocate or establish fresh runtime
connection and subscription generations.

Previously acknowledged native subscriptions must
not be assumed to remain active.

Previously synchronized L2 book state must not be
treated as valid without the required initialization
and verification procedures.

Recovery and historical coverage must be evaluated
according to their independent contracts.

A previously active stream must not be reported as
acquisition-ready merely because its persisted
configuration requested activation.

### 19.3 Restart Safety

An unexpected daemon shutdown invalidates all in-memory runtime
execution state.

Restart recovery must reconstruct authoritative runtime resources
from the last accepted and durably committed desired configuration.

It must not treat previously observed operational state as proof
that external resources remain active.

#### Persistent State Recovery

After restart, MarketForge Live must:

1. Load the last committed desired configuration revision.
2. Validate configuration compatibility with installed static
   specifications and instrument metadata.
3. Reconstruct registered logical source identities.
4. Restore stable `StreamId` assignments for unchanged
   persistent source acquisitions.
5. Reconstruct persistent acquisition-demand requirements.
6. Exclude transient subscriber and session-scoped demand
   from automatic restoration.
7. Reconcile the required logical runtimes and dependencies.

Equivalent persistent acquisition requirements must reuse the
same `SourceStreamKey` and registered logical source.

Persistent demand reconstruction must be idempotent.

A source with no reconstructed persistent demand remains
inactive unless new valid demand is subsequently acquired.

#### Runtime Resource Reconstruction

All runtime resources must be reestablished through their
authoritative owners.

Required operations include:

- Establishing physical WebSocket connections.
- Allocating fresh connection incarnations.
- Establishing native exchange subscriptions.
- Allocating fresh subscription incarnations.
- Initializing source-stream processors.
- Reconstructing L2 books through valid synchronization.
- Evaluating trade continuity and missing coverage.
- Restoring recording according to its independent policy.
- Restoring retention state according to its committed
  segments, manifests, and durability guarantees.
- Resuming permitted recovery and supervision operations.

Previously acknowledged native subscriptions must not be
assumed active after daemon restart.

Previously valid L2 books must not be assumed synchronized.

Previously running logical source streams must pass through
the appropriate startup and acquisition-readiness procedures.

#### Generation Safety

Connection, subscription, book, and log-generation domains
remain independent.

A restart must not permit an obsolete generation to be
mistaken for a current incarnation.

Fresh connection and subscription incarnations must be
established according to the authoritative generation
contracts in Document 02, Section 5.

An L2 book must establish a new authoritative synchronization
generation before synchronized-book publication resumes.

MarketForge event cursors must preserve their identity
and restart-safety requirements independently of connection
or subscription generations.

A daemon restart must not automatically establish a new
`LogEpoch` unless required by the cursor recovery contract.

If a previously used cursor namespace cannot be continued
unambiguously, the runtime must establish a new log epoch
rather than reuse an ambiguous event position.

The detailed cursor recovery contract belongs to the
retention specification.

#### Logical Runtime and Acquisition Readiness

A reconstructed source may become
`StreamLifecycle::Active` after its logical runtime
has successfully started.

This does not independently establish acquisition readiness.

For example:

    Persistent Desired State: Active
    Effective Acquisition:    Required
    StreamLifecycle:          Active
    ConnectionState:          Connecting
    SubscriptionState:        Pending
    IntegrityState:           Uninitialized
    AcquisitionReady:         false

This state is valid during startup.

Trade and L2 acquisition readiness must be restored
according to their respective channel-specific contracts.

Current acquisition readiness must remain independent
of unresolved historical coverage gaps.

#### Failure Handling

If persisted desired configuration cannot be loaded or
validated, startup must fail safely or enter an explicitly
supported degraded management mode.

The runtime must not silently rewrite acquisition semantics
to make startup succeed.

If an exchange is temporarily unavailable, the accepted
desired configuration remains authoritative.

The responsible components apply the established retry,
cooldown, and recovery policies.

Partial startup convergence must be reported explicitly.

A failure to restore one source must not unnecessarily
interrupt unrelated healthy sources.

#### Restart Invariants

- Stable logical source identity must survive ordinary restarts.
- Runtime state must be reconstructed, not blindly restored.
- Persistent demand must be rebuilt from authoritative configuration.
- Transient demand must not be resurrected.
- Native subscriptions must be reestablished.
- L2 integrity must be independently revalidated.
- Obsolete generations must not become authoritative.
- Cursor identities must never be ambiguously reused.
- Recording and retention guarantees must not be overstated.
- Historical coverage must remain independent of current readiness.
- Python orchestration must not be required for daemon recovery.

### 19.4 Runtime State Snapshots

The runtime may periodically capture operational snapshots
for diagnostics, monitoring, and post-failure investigation.

These snapshots are observational artifacts.

They are not authoritative persistent desired configuration
and must not replace the Configuration Manager's committed
revision store.

#### Snapshot Contents

A diagnostic snapshot may contain:

    StreamId
    SourceStreamKey
    PersistentDesiredState
    StreamLifecycle
    EffectiveAcquisitionRequired
    ActiveDemandCount
    ConnectionId
    ConnectionGeneration
    ConnectionState
    SubscriptionGeneration
    SubscriptionState
    IntegrityState
    RecoveryState
    AcquisitionReadiness
    AppliedConfigurationRevision
    LastError
    LastLifecycleTransition
    SnapshotTimestamp

The snapshot may additionally include aggregate demand
information and relevant recording or retention status.

Sensitive information must remain redacted.

#### Authoritative Ownership

The Stream Registry supplies logical identity, demand,
and logical runtime lifecycle observations.

The Connection Manager supplies physical connection state.

The Subscription Manager supplies native subscription state.

The responsible stream processor supplies integrity state.

The Recovery Coordinator supplies recovery observations.

The Configuration Manager supplies accepted desired revision
information.

The Metrics and Stream Health subsystem may aggregate these
observations for monitoring.

Snapshot aggregation does not transfer ownership of
authoritative state.

#### Diagnostic Consistency

Snapshots must include sufficient revision, generation,
and observation-time context to support interpretation.

Independent components may be observed at slightly
different times.

Unless explicitly supported by a stronger snapshot
contract, a diagnostic snapshot must not claim
process-wide atomic consistency.

Missing information must remain distinguishable from
an observed zero or healthy state.

#### Restart Restrictions

After daemon restart, a previous diagnostic snapshot
must not be treated as proof that:

- A WebSocket connection remains established.
- A native subscription remains active.
- An L2 order book remains synchronized.
- A recovery operation remains in progress.
- Transient acquisition demand remains valid.
- A previously observed health classification
  remains current.
- The previously applied configuration still matches
  the current runtime.

Authoritative persistent desired state must be
loaded from the Configuration Manager.

Persistent demand is reconstructed from accepted
configuration declarations.

Runtime resources are reinitialized through their
authoritative owners.

Diagnostic snapshots may assist incident investigation
and reconciliation diagnostics, but they must not
independently recreate runtime resources or bypass
normal validation and generation fencing.

#### Storage Boundary

Operational snapshots and logs remain local.

PostgreSQL is reserved for optional market-data recording.

The exact diagnostic snapshot format, retention period,
and storage location remain implementation details
subject to the established resource and logging policies.lets 

---

## 20. Registry Events

The registry emits structured lifecycle events for observability and dependent components.

Events describe completed state changes or meaningful operational conditions.

### ### 20.1 Event Types

The Stream Registry emits structured operational events describing
authoritative logical stream lifecycle transitions and significant
stream-management operations.

These events are distinct from canonical market-data events.

#### Logical Lifecycle Events

The Stream Registry owns events representing changes to the
authoritative `StreamLifecycle` defined in Document 02, Section 7.2.

Conceptual event types:

    StreamRegistered
    StreamStarting
    StreamRuntimeStarted
    StreamIdle
    StreamStopping
    StreamStopped
    StreamFailed
    StreamRemoved

`StreamRuntimeStarted` indicates that the logical runtime has
entered `StreamLifecycle::Active`.

It does not imply successful native subscription establishment
or acquisition readiness.

`StreamRemoved` describes completion of a registry removal
operation, not a `StreamLifecycle` variant.

#### Stream Management Events

The registry may additionally emit events describing
significant control-plane operations:

    StreamActivationRequested
    StreamDeactivationRequested
    StreamAdmissionDeferred
    StreamActivationCompleted
    StreamActivationFailed
    StreamDeactivationCompleted
    StreamConfigurationChanged

These events describe operation outcomes or progress.

They must not be interpreted as additional
`StreamLifecycle` states.

In particular:

- `StreamActivationCompleted` means the configured acquisition
  readiness requirements have been satisfied.
- `StreamRuntimeStarted` means the logical runtime is running.
- `StreamAdmissionDeferred` describes subscription admission
  progress, not logical runtime lifecycle.
- `StreamConfigurationChanged` describes an accepted or applied
  configuration change according to its explicit revision
  and reconciliation status.

An operation acknowledgement alone must not generate an event
claiming that asynchronous activation has completed.

#### Events Owned by Other Subsystems

The following operational conditions have separate
authoritative owners:

| Condition | Authoritative Owner |
|---|---|
| Connection established, lost, or reconnecting | Connection Manager |
| Subscription requested, confirmed, rejected, or removed | Subscription Manager |
| L2 synchronization and integrity transitions | Stream Processor / Integrity subsystem |
| Recovery attempts and completion | Recovery Coordinator |
| Historical coverage changes | Recovery / Retention subsystem |
| Health classification changes | Metrics and Stream Health subsystem |

The Stream Registry may consume these events when coordinating
activation, deactivation, and desired-state reconciliation.

It must not re-emit them as independent logical lifecycle
transitions or maintain competing authoritative state machines.

For example, a connection reconnection event may affect
acquisition readiness without changing `StreamLifecycle::Active`.

#### Event Context

Every registry event must identify:

| Field | Purpose |
|---|---|
| event_id | Unique operational-event identity |
| stream_id | Associated logical stream |
| event_type | Event classification |
| timestamp | Event occurrence time |
| reason | Optional cause or diagnostic context |

Additional context depends on the event category.

Logical lifecycle transition events include:

    previous_lifecycle
    current_lifecycle

Configuration-related events include:

    configuration_revision
    reconciliation_outcome

Asynchronous operation events include:

    operation_id
    operation_status

Subscription-related observations may reference:

    subscription_generation
    connection_generation

An event must not populate lifecycle transition fields
with connection, subscription, integrity, recovery, or
health states.

Events must be emitted after the authoritative state
mutation or operation-status change they describe.

Consumers must not be able to modify registry state directly.

#### Event Consistency

Operational events provide transition information and
diagnostic context.

They do not replace authoritative component state.

Event consumers must tolerate missing or delayed
notifications and recover their current view through
authoritative snapshots.

Generation and revision identifiers must be preserved
where needed to distinguish obsolete events from
current runtime observations.
### 20.2 Event Delivery

Registry events are distributed through a dedicated control-plane event mechanism.

They are distinct from canonical market-data events.

    Stream Registry
          |
          v
    Lifecycle Events
          |
     +----+----+---------+
     |         |         |
     v         v         v
    TUI       API      Metrics
                        |
                        v
                    Local Logs

Delivery requirements:

- Preserve per-stream transition ordering.
- Include sufficient identity and generation information.
- Avoid blocking registry mutations on slow consumers.
- Detect and report control-plane event delivery failures.
- Permit consumers to reconstruct current state from registry snapshots.

The registry remains authoritative.

Event consumers must not assume their local event history is complete.

### 20.3 Event Consistency

Lifecycle events are not a replacement for registry state.

A consumer that detects missing events must request a fresh registry snapshot.

State snapshots should include a monotonically increasing registry revision or equivalent consistency token.

This allows consumers to distinguish newer snapshots from obsolete event updates.

---

## 21. Observability

The Stream Registry exposes control-plane state and lifecycle diagnostics.

Market-data performance metrics remain owned by the Metrics Registry.

### 21.1 Registry Metrics

The Stream Registry exposes measurements describing
logical stream registration, runtime lifecycle,
demand ownership, and stream-management operations.

The Metrics Registry owns metric collection, aggregation,
and immutable snapshot publication.

The Stream Registry remains authoritative for the
underlying lifecycle and ownership information.

#### Logical Registry Metrics

| Metric | Description |
|---|---|
| `registered_streams` | Number of registered logical source streams |
| `active_stream_runtimes` | Registered source streams with `StreamLifecycle::Active` |
| `idle_stream_runtimes` | Registered source streams with `StreamLifecycle::Idle` |
| `stopped_stream_runtimes` | Registered source streams with `StreamLifecycle::Stopped` |
| `failed_stream_runtimes` | Registered source streams with `StreamLifecycle::Failed` |
| `pending_registry_operations` | Outstanding registry-owned control operations |
| `lifecycle_transitions_total` | Completed authoritative logical lifecycle transitions |
| `stream_activation_failures_total` | Failed source-acquisition activation operations |
| `stream_removals_total` | Completed source-stream removal operations |
| `configuration_conflicts_total` | Rejected conflicting configuration updates observed by the control plane |

`active_stream_runtimes` counts running logical stream runtimes.

It does not count only streams currently delivering valid
market data.

#### Acquisition Readiness Metrics

Acquisition readiness must be reported separately
from logical runtime lifecycle.

Conceptual metrics:

| Metric | Description |
|---|---|
| `acquisition_ready_streams` | Source streams satisfying their configured acquisition-readiness requirements |
| `acquisition_not_ready_streams` | Registered source streams currently required to acquire data but not satisfying readiness requirements |
| `acquisition_readiness_losses_total` | Observed transitions from acquisition-ready to not ready |
| `acquisition_readiness_restorations_total` | Observed restorations of acquisition readiness |

Readiness evaluation must use authoritative connection,
subscription, processor-initialization, and integrity
observations.

The registry must not define readiness solely from
`StreamLifecycle::Active`.

Streams that are intentionally inactive and have no
acquisition demand must not automatically be classified
as acquisition failures.

#### Component-Owned Operational Metrics

The following measurements must retain their
authoritative subsystem ownership:

| Metric or Condition | Source |
|---|---|
| Reconnecting physical connections | Connection Manager |
| Pending native subscription admission | Subscription Manager |
| Subscription acknowledgement timeouts | Subscription Manager |
| Synchronizing L2 books | Stream Processor / Integrity subsystem |
| Recovery attempts and outcomes | Recovery Coordinator |
| Historical coverage gaps | Recovery / Retention subsystem |
| Stream health classifications | Metrics and Stream Health subsystem |

These measurements may be exposed together in an
aggregated runtime or stream-health snapshot.

Aggregation does not transfer ownership of the
underlying states or counters to the Stream Registry.

#### Metric Semantics

Counters must be monotonic within their declared
runtime or metric-reset epoch.

Gauges represent observed state at the snapshot time.

Metric names and dimensions must not conflate:

- Logical runtime activity.
- Native subscription activity.
- Current acquisition readiness.
- Recovery progress.
- Historical coverage.
- Health classification.

The final metric schema and snapshot contracts belong
to Document 12 — Metrics and Stream Health.

This section defines the Registry's reporting
requirements without independently freezing a
competing metrics schema.

### 21.2 Per-Stream Diagnostics

The runtime must expose a coherent operational snapshot
for each registered logical source stream.

The snapshot combines authoritative observations from
the responsible components without transferring
ownership of their state.

#### Registry-Owned Information

The Stream Registry provides:

    StreamId
    SourceStreamKey
    DesiredState
    StreamLifecycle
    AppliedConfigurationRevision
    RegisteredAt
    LastLifecycleTransition
    LastRegistryError
    PendingRegistryOperations
    AcquisitionDemandSummary

The logical lifecycle must use the authoritative
`StreamLifecycle` contract from Document 02.

#### Referenced Connection and Subscription Information

The operational snapshot may additionally include:

    ConnectionId
    ConnectionGeneration
    ConnectionState
    SubscriptionGeneration
    SubscriptionState
    NativeSubscriptionIdentity

Connection state and generation information originate
from the Connection Manager.

Subscription state and generation information originate
from the Subscription Manager.

The Stream Registry must not independently calculate or
mutate those authoritative states.

#### Integrity, Recovery, and Coverage Information

The snapshot may include:

    IntegrityState
    RecoveryState
    CoverageState
    AcquisitionReadiness

Integrity information originates from the responsible
stream processor.

Recovery information originates from the Recovery Coordinator.

Historical coverage originates from the appropriate
recovery and retention contracts.

Acquisition readiness is evaluated from the applicable
authoritative component observations and configured
channel-readiness requirements.

A logical runtime may remain `StreamLifecycle::Active`
while acquisition readiness is false.

#### Metrics and Health Information

Additional diagnostics are provided by the relevant owners:

- Throughput and latency from the Metrics Registry.
- Message-integrity counters from the Stream Processor
  and their corresponding metrics.
- L2 synchronization status from the integrity subsystem.
- Recording status from the Recording Manager.
- Retention status from the Retention Manager.
- Aggregated health classification from the Metrics
  and Stream Health subsystem.

The Stream Registry must not duplicate these
measurements or independently evaluate stream health.

#### Snapshot Consistency

A complete operational snapshot should identify its
observation time and sufficient revision or generation
context for clients to understand which state it describes.

Independent components may be observed at slightly
different instants.

Unless a stronger consistency contract is explicitly
provided, the snapshot must not claim that all reported
component states were captured atomically.

The Control API and Ratatui must use these authoritative
runtime observations rather than infer readiness or
health solely from the logical lifecycle state.

### 21.3 TUI Integration

The Streams view consumes registry snapshots and lifecycle events.

Required functionality:

- List registered streams.
- Inspect desired and observed states.
- Add streams.
- Activate and deactivate streams.
- Remove streams.
- Inspect subscription and connection assignments.
- Display current errors.
- Display relevant health metrics.
- Control recording through the Recording Manager.
- Inspect pending operations.

The TUI must not bypass registry command validation.

### 21.4 Control API Integration

The runtime exposes stream-management operations through the versioned
Control API defined in Document 13 — Runtime Configuration and Control API.

The Ratatui interface and external clients use the same authoritative
control-plane contracts. Neither may bypass Stream Registry validation
or directly mutate internal runtime state.

The following capabilities map to the conceptual `ControlCommand`
vocabulary defined in Document 13, Section 5.3.

| Capability                             | Control API operation                                                                               |
| -------------------------------------- | --------------------------------------------------------------------------------------------------- |
| List registered streams                | `ListStreams`                                                                                       |
| Inspect an individual stream           | `GetStream`                                                                                         |
| Register a source stream               | `RegisterStream`                                                                                    |
| Activate a source stream               | `ActivateStream`                                                                                    |
| Deactivate a source stream             | `DeactivateStream`                                                                                  |
| Remove a source stream                 | `RemoveStream`                                                                                      |
| Inspect an asynchronous operation      | `GetOperation`                                                                                      |
| Update persistent stream configuration | Configuration Manager via `ApplyConfiguration`; exact resource-scoped mapping requires confirmation |
| Restart a stream                       | Exact control command and restart semantics require confirmation                                    |
| Subscribe to stream lifecycle events   | Event subscription and framing contract require confirmation                                        |

The command names above represent the conceptual vocabulary in Document 13.
They do not independently define the final serialized wire schema.

Control messages use the Unix-domain control socket and the versioned,
length-prefixed JSON framing specified in Document 13, Section 20.

The Control API is separate from the binary Unix-domain socket used
for canonical market-data distribution.

Long-running operations return an operation identifier rather than
blocking until exchange synchronization completes.

A command acknowledgement confirms acceptance or rejection, not
successful completion of asynchronous runtime work.

---

## 22. Concurrency and Resource Safety

The registry must support concurrent stream operations without global serialization of unrelated work.

### 22.1 Concurrency Model

Recommended implementation:

- Central registry for identity and indexing.
- Per-stream serialized lifecycle coordination.
- Independent Subscription Manager.
- Independent Connection Manager.
- Asynchronous operation completion events.
- Generation fencing across component boundaries.

Registry locks must protect only short state mutations.

Network operations, REST recovery, subscription acknowledgements, and processor initialization must execute outside registry locks.

### 22.2 Resource Ownership

Every allocated runtime resource must have an explicit owner.

| Resource | Owner |
|---|---|
| Stream descriptor | Stream Registry |
| Stream lifecycle state | Stream Registry |
| Subscription operation | Subscription Manager |
| Native subscription assignment | Subscription Manager |
| Physical WebSocket connection | Connection Manager |
| Stream processor | Processing Runtime |
| L2 book state | BookStore |
| Recovery operation | Recovery Coordinator |
| Recording session | Recording Manager |
| Subscriber session | Publisher |

Resource release must be idempotent.

A failed stream activation must release resources acquired before the failure.

### 22.3 Shutdown

Graceful shutdown follows:

    Stop Accepting Commands
              |
              v
    Freeze Desired-State Mutations
              |
              v
    Fence Active Generations
              |
              v
    Stop Subscription Operations
              |
              v
    Stop Stream Processors
              |
              v
    Release Native Subscriptions
              |
              v
    Close Physical Connections
              |
              v
    Flush Operational State
              |
              v
           Terminate

The Recording Manager and Publisher have independent shutdown requirements.

The registry coordinates their lifecycle dependencies without implementing their internal flush or delivery mechanisms.

---

## 23. Testing Requirements

The Stream Registry and Subscription Manager require deterministic unit tests, asynchronous integration tests, and fault-injection tests.

### 23.1 Registry Tests

Verify:

- Stable stream identity.
- Canonical subscription-key construction.
- Duplicate registration handling.
- Desired-state reconciliation.
- Valid lifecycle transitions.
- Invalid transition rejection.
- Idempotent activation and deactivation.
- Idempotent removal.
- Configuration revision conflicts.
- Stable identity after restart.

### 23.2 Subscription Tests

Verify:

- Subscription admission.
- Rate-limit deferral.
- Connection assignment.
- Native request construction.
- Acknowledgement correlation.
- Duplicate acknowledgements.
- Out-of-order acknowledgements.
- Missing acknowledgements.
- Subscription timeouts.
- Partial batch success.
- Unsubscription failures.
- Subscription restoration.

Exchange protocol adapters should be tested using recorded protocol fixtures.

### 23.3 Concurrency Tests

Verify:

- Concurrent activation requests.
- Activation followed by immediate deactivation.
- Removal during subscription.
- Configuration replacement during reconnection.
- Late acknowledgements from obsolete generations.
- Shared connection failure affecting multiple streams.
- Dependency registration racing with source removal.
- Resource release after partial activation failure.

No test may rely on arbitrary sleeps to establish correctness.

Use deterministic event scheduling, controlled clocks, and mocked asynchronous interfaces where practical.

### 23.4 Recovery Tests

Verify:

- Connection loss during subscription.
- Connection loss after acknowledgement.
- Connection loss during L2 synchronization.
- Reconnection with changed desired state.
- Repeated exchange outages.
- Rate-limit cooldown during restoration.
- Recovery completion after stream removal.
- Process restart with previously active streams.

A recovered connection must not automatically imply recovered stream readiness.

### 23.5 Property-Based Tests

Useful invariants include:

    One authoritative lifecycle state per StreamId.

    One authoritative subscription generation per StreamId.

    No stale generation may mutate current state.

    Removed streams cannot become active.

    An L2 stream reporting acquisition readiness must satisfy its
configured synchronization and integrity requirements.

StreamLifecycle::Active alone does not imply acquisition readiness.

    Native subscription capacity is never exceeded.

    Resource release is idempotent.

    Failed activation does not leak reservations.

    Desired state survives transient transport failure.

Property-based testing should explore arbitrary valid command sequences and asynchronous completion orderings.

---

## 24. Implementation Acceptance Criteria

The Stream Registry and Subscription Manager are complete when:

- Logical stream identities remain stable across reconnects.
- Registration and activation are separate operations.
- Desired and observed states are independently represented.
- All lifecycle transitions are validated.
- Subscription admission respects applicable limits.
- Physical connections can host multiple compatible streams.
- Subscription acknowledgements are correlated correctly.
- Stale asynchronous completions are rejected.
- Runtime stream mutations are generation-safe.
- Subscription replacement preserves lifecycle consistency.
- Configuration revisions are applied explicitly.
- Source dependencies are protected during removal.
- Stream restoration respects current desired state.
- L2 readiness requires successful synchronization.
- Runtime restart reconstructs persistent stream declarations.
- TUI and API commands use the same registry interface.
- Control-plane events and metrics are exposed.
- Resource cleanup is deterministic and idempotent.
- Unit, integration, concurrency, and recovery tests pass.

The implementation must remain exchange-agnostic.

Exchange-specific subscription encoding, acknowledgements, connectivity, and normalization are supplied through the compiled protocol configurations and their corresponding adapters.

---
