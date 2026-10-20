# 13 — Runtime Configuration and Control API

## 1. Purpose

The Runtime Configuration and Control API subsystem provides the authoritative external management interface for MarketForge Live.

It enables clients to inspect and modify supported runtime behavior without directly accessing internal Rust components.

The subsystem integrates:

- Configuration management.
- Runtime state inspection.
- Source-stream management.
- Derived-stream management.
- Recording controls.
- Retention inspection.
- Metrics and health queries.
- Recovery inspection.
- Operational commands.
- Configuration revision management.

The interface must be accessible independently of the Ratatui TUI.

### 1.1 Primary Objectives

The subsystem must support:

1. Managing MarketForge Live while it is running.
2. Applying validated configuration changes.
3. Preserving desired and actual state independently.
4. Executing commands asynchronously where necessary.
5. Preventing conflicting runtime modifications.
6. Exposing consistent operational snapshots.
7. Supporting multiple simultaneous management clients.
8. Preserving resource ownership boundaries.
9. Enforcing access restrictions.
10. Remaining independent of canonical event processing.

The API must not become a second implementation of the Stream Registry, Configuration Manager, or Recovery Coordinator.

### 1.2 Management Clients

Supported clients include:

    Ratatui Management TUI

    Python Orchestration

    Local CLI Tools

    Other Authorized Applications

These clients communicate through the same versioned control protocol.

A client must not receive different configuration semantics merely because it uses Python instead of Ratatui.

### 1.3 Management Model

MarketForge follows:

    Desired State
          |
          v
    Configuration Manager
          |
          v
    Runtime Reconciler
          |
          v
    Authoritative Component Owners
          |
          v
    Actual Runtime State

External clients propose changes.

The Configuration Manager validates and persists accepted desired-state revisions.

The Runtime Reconciler coordinates the corresponding component transitions.

### 1.4 Control vs Observation

The system distinguishes two operations.

**Observation**

Reads existing runtime state.

Examples:

    ListStreams

    GetStreamHealth

    GetRuntimeMetrics

    GetRecordingStatus

**Mutation**

Requests a change to runtime behavior.

Examples:

    AddStream

    RemoveStream

    EnableRecording

    CreateCombinedStream

    UpdateRuntimeSettings

Observation must not implicitly mutate desired state.

Mutation must pass through validation, authorization, and the appropriate resource owner.

### 1.5 Non-Responsibilities

The Control API must not:

- Establish exchange connections directly.
- Parse exchange-native market data.
- Normalize canonical events.
- Apply BookStore updates.
- Reconcile missing executions.
- Execute exchange recovery algorithms.
- Write market data into PostgreSQL.
- Manage subscriber event queues.
- Implement merge or conversion algorithms.
- Own the Metrics Registry.
- Render terminal interfaces.
- Launch Prometheus or Grafana.
- Participate synchronously in the canonical event hot path.

### 1.6 Fundamental Invariant

    The Control API
    Requests And Observes
    Runtime Operations.

    Authoritative Components
    Execute Those Operations.

---

## 2. Architectural Boundaries

The Control API belongs to the Rust Live control plane.

It is logically independent of market-data distribution.

### 2.1 Control Architecture

    Ratatui TUI
         |
         |
    Python Orchestration
         |
         |
    Local CLI / Applications
         |
         v
    Versioned Control Protocol
         |
         v
    Control Server
         |
         v
    Request Validation
         |
         +-----------------------+
         |                       |
         v                       v
    Read Operations         Mutating Commands
         |                       |
         v                       v
    Snapshot Providers      Configuration Manager
         |                       |
         |                       v
         |                 Runtime Reconciler
         |                       |
         |                       v
         |                 Component Owners
         |                       |
         +-----------+-----------+
                     |
                     v
              Control Response

The Control Server must not execute expensive operations while holding global configuration or registry locks.

### 2.2 Component Ownership

| Component | Responsibility |
|---|---|
| Control Server | Client sessions, request framing, dispatch |
| Configuration Manager | Desired-state revisions and validation |
| Configuration Compiler | Effective configuration compilation |
| Runtime Reconciler | Applying validated changes |
| Stream Registry | Source-stream lifecycle and demand |
| Subscription Manager | Exchange-native subscription operations |
| Connection Manager | Physical transport lifecycle |
| Derived Stream Registry | Merge and conversion definitions |
| Recording Manager | PostgreSQL recording |
| Retention Manager | Replay and retention operations |
| Recovery Coordinator | Recovery orchestration |
| Metrics Registry | Metric snapshots |
| HealthEvaluator | Health assessments |
| Runtime Supervisor | Worker lifecycle and shutdown |
| Ratatui | Presentation and user interaction |

The Control Server is not the authoritative owner of these components' internal states.

### 2.3 Data-Plane Isolation

Canonical event processing must not depend on the availability of a control client.

For example:

    Ratatui Disconnects
            |
            v
    Control Session Removed

Existing exchange acquisition, recording, retention, and publication continue operating.

A Control Server failure must not automatically invalidate established market-data processing.

Recovery of the Control Server belongs to runtime supervision.

### 2.4 Separate Socket Endpoints

MarketForge uses distinct Unix domain socket interfaces.

Conceptually:

    marketforge-data.sock

    marketforge-control.sock

The first transports canonical and derived market-data events.

The second transports management commands, queries, responses, and applicable operational notifications.

The protocols must not be mixed.

### 2.5 Monitoring API Integration

The existing monitoring interface belongs to the control plane.

Conceptually:

    Control Server
          |
          +-- Configuration Queries
          |
          +-- Stream Queries
          |
          +-- Metrics Queries
          |
          +-- Health Queries
          |
          +-- Recovery Queries

Monitoring operations consume snapshots from the Metrics Registry, HealthEvaluator, and authoritative component owners.

They must not independently calculate canonical integrity.

### 2.6 Optional Prometheus Export

Prometheus exposition is independent of the management protocol.

    Metrics Registry
          |
          +-- Control Monitoring Queries
          |
          +-- Prometheus Exporter
                    |
                    v
                 /metrics

The Prometheus exporter:

- Is optional.
- Is disabled by default.
- Provides read-only metric exposition.
- Does not implement control commands.
- Does not replace the Control API.
- Does not require Prometheus or Grafana to be running.

### 2.7 Control-Plane Failure Isolation

A malformed or slow control client must not:

- Block WebSocket readers.
- Hold stream-registry locks indefinitely.
- Block PostgreSQL batch commits.
- Interrupt canonical event publication.
- Delay L2 synchronization.
- Trigger unauthorized recovery actions.

Every client session must have explicit resource limits.

---

## 3. Control Server

The Control Server manages transport-level communication with authorized clients.

It does not own the application logic behind control commands.

### 3.1 Responsibilities

The Control Server must:

- Bind the configured Unix socket.
- Accept authorized client connections.
- Decode versioned requests.
- Validate request framing.
- Enforce message-size limits.
- Correlate responses with requests.
- Dispatch requests to appropriate handlers.
- Support bounded concurrent clients.
- Handle client cancellation.
- Report protocol errors.
- Support graceful shutdown.

### 3.2 ControlServer

Conceptual representation:

    ControlServer {
        socket_path

        protocol_version

        active_sessions

        request_dispatcher

        authentication_policy
        resource_limits

        lifecycle_state
    }

The structure is illustrative.

### 3.3 Connection Lifecycle

    Client Connects
          |
          v
    Transport Admission
          |
          v
    Authorization
          |
          v
    Protocol Negotiation
          |
          v
    Active Session
          |
          v
    Request Processing
          |
          v
    Client Disconnects
          |
          v
    Session Cleanup

Client disconnection must release session-specific resources.

It must not undo already accepted persistent configuration changes.

### 3.4 Control Transport

The initial transport is:

    Unix Domain Socket

using a versioned framed protocol.

The exact binary or structured encoding remains a contract decision.

Regardless of encoding, the protocol must support:

- Explicit message boundaries.
- Protocol versions.
- Request identifiers.
- Typed commands.
- Typed responses.
- Structured errors.
- Maximum message sizes.

### 3.5 Framing

Conceptually:

    ControlFrame {
        protocol_version

        message_type
        payload_length

        payload
    }

The frame must not rely on Unix stream read boundaries to delimit logical messages.

A partial socket read must not be interpreted as a complete request.

### 3.6 Concurrent Requests

One client session may submit multiple requests when the protocol supports concurrent request correlation.

Each request carries a unique session-scoped request identifier.

Responses may complete out of order.

The client must use request identifiers rather than assume response ordering.

### 3.7 Client Session Limits

Relevant limits include:

    MaximumControlClients

    MaximumConcurrentRequests

    MaximumRequestBytes

    MaximumResponseBytes

    RequestTimeout

    MaximumPendingResponses

Limits must prevent one client from exhausting control-plane memory.

### 3.8 Read vs Mutating Operations

Read operations may execute concurrently when their state-access contracts permit it.

Mutating operations must respect authoritative serialization and configuration revision rules.

For example, concurrent updates to the same stream may require serialized reconciliation.

Independent stream mutations may execute concurrently.

### 3.9 Client Disconnect

When a client disconnects:

- Cancel unfinished session-local reads.
- Release queued response buffers.
- Stop session-specific monitoring subscriptions.
- Release transport resources.
- Preserve already accepted persistent commands.

A command accepted by the Configuration Manager must not be silently cancelled simply because the requesting TUI was closed.

### 3.10 Socket Security

The Unix socket must use restrictive filesystem permissions.

Where supported, the server may additionally validate operating-system peer credentials.

Access to the socket must not automatically authorize every mutation.

The authorization model must distinguish read-only monitoring from runtime modification where required.

### 3.11 Control Server Invariant

    A Control Client
    Must Never Directly Own
    Or Mutate Internal
    Runtime Resources.

---

## 4. Runtime State Model

The Control API exposes desired state, observed lifecycle state, health, and configuration revisions as separate concepts.

These distinctions were established in Documents 02, 03, 05, and 12.

### 4.1 Desired State

Desired state describes the configuration that MarketForge Live
has accepted and committed as its persistent operating intent.

The Configuration Manager is authoritative for persisted
desired configuration and its revisions.

Desired state is distinct from effective acquisition demand,
actual runtime state, and health.

#### Persistent Source Acquisition

For registered source streams, the conceptual desired states are:

    Active
    Inactive
    Removed

Their meanings are:

| Desired State | Meaning |
|---|---|
| `Active` | Persistent configuration independently requires acquisition of the registered source. |
| `Inactive` | Persistent configuration does not independently require acquisition. Other valid demand owners may still require the source. |
| `Removed` | Removal of the persistent source registration is requested, subject to applicable ownership and dependency constraints. |

These semantics follow Document 05, Section 4.3.

Persistent source acquisition is only one possible
contributor to effective acquisition demand.

Other contributors may include:

- Recording requirements.
- External subscribers.
- Derived-stream dependencies.
- Retention sessions.

A source with persistent desired state `Inactive`
may therefore remain operational when independent
demand requires acquisition.

#### Other Desired Configuration

Desired configuration also describes supported
requirements for:

    Recording Enabled
    Derived Stream Enabled
    Metrics Enabled

Each resource follows its own authoritative
configuration and ownership contracts.

These settings must not be interpreted as
`StreamLifecycle` states.

#### Configuration Acceptance

Persistent desired-state changes must pass through:

    Validation
        |
        v
    Effective Configuration Compilation
        |
        v
    Semantic Diff
        |
        v
    Expected Revision Check
        |
        v
    Durable Configuration Persistence
        |
        v
    Accepted Revision Publication
        |
        v
    Runtime Reconciliation

A successfully accepted desired configuration
does not prove that the requested runtime resources
have already reached their required operational state.

The Control API must distinguish accepted desired
configuration from applied configuration and
actual runtime observations.


### 4.2 Actual State

Actual state describes the operational conditions
currently observed in MarketForge Live.

Actual state is represented through independent
authoritative dimensions rather than one combined
stream-status enum.

#### Logical Source Runtime

The Stream Registry owns the logical source-stream
lifecycle defined in Document 02, Section 7.2.

For example:

    StreamLifecycle: Active

This means the logical runtime is executing.

It does not imply successful acquisition or
valid market-data processing.

#### Physical Connection

The Connection Manager owns the authoritative
physical connection lifecycle.

For example:

    ConnectionState: Reconnecting

This describes physical transport restoration,
not a logical source-stream lifecycle transition.

A logical source runtime may remain
`StreamLifecycle::Active` while its assigned
connection is reconnecting.

#### Native Subscription

The Subscription Manager owns native subscription
state and subscription generations.

For example:

    SubscriptionState: Pending

A successfully connected WebSocket does not
independently establish native subscription readiness.

#### Market-Data Integrity

The responsible stream processor owns
market-data integrity.

For example:

    IntegrityState: Synchronizing

For L2 streams, synchronization requires
the configured snapshot and sequence-validation
procedure.

A native subscription acknowledgement does not
establish valid synchronized book state.

#### Recovery and Historical Coverage

The Recovery Coordinator owns recovery-operation
coordination.

Historical coverage is tracked through the
applicable recovery and retention contracts.

These dimensions remain distinct:

    RecoveryState: Recovering
    CoverageState: Incomplete

Recovery of current market-data integrity does
not automatically establish complete historical
coverage.

#### Effective Acquisition Demand

The Stream Registry derives effective acquisition
requirements from the current valid demand
registrations.

For example:

    Persistent Desired State: Inactive
    Recording Demand:         Present
    Effective Acquisition:    Required

Actual acquisition may remain operational
despite persistent acquisition being disabled.

#### Acquisition Readiness

Acquisition readiness describes whether a source
currently satisfies its configured channel-specific
requirements.

It must be evaluated from authoritative connection,
subscription, processor-initialization, and
integrity observations.

A source may report:

    Persistent Desired State: Inactive
    Effective Acquisition:    Required

    StreamLifecycle:          Active
    ConnectionState:          Connected
    SubscriptionState:        Active
    IntegrityState:           Synchronizing

    AcquisitionReadiness:     Not Ready

This is valid for a source whose logical runtime
and native subscription are operational while
its L2 order book is synchronizing.

#### Actual State Invariants

- Persistent desired state is not actual runtime state.
- Effective acquisition demand is not persistent desired state.
- Logical runtime activity is not acquisition readiness.
- Physical connectivity is not native subscription readiness.
- Native subscription readiness is not market-data integrity.
- Current market-data integrity is not historical completeness.
- Recovery progress is not logical runtime lifecycle.
- Health assessment is independent of authoritative lifecycle state.

Actual runtime observations must originate from
their authoritative component owners.

The Control API aggregates these observations
without independently maintaining competing
state machines.

### 4.3 Health State

Health is an independent assessment of whether a resource
currently satisfies its applicable operational guarantees.

The Metrics and Stream Health subsystem is authoritative
for health evaluation, as defined in Document 12.

Health must not be used as a replacement for logical
runtime lifecycle, physical connection state, subscription
state, acquisition readiness, or recovery progress.

A source may be `StreamLifecycle::Active` while its
health is `Degraded` or `Unhealthy`.

Likewise, a connection may be `ConnectionState::Connected`
while its independent connection-health assessment is
`Degraded`.

Health evaluation consumes authoritative observations
without modifying their underlying state machines.

### 4.4 Configuration Revision

Every accepted desired-state update receives a configuration revision.

Conceptually:

    ConfigRevisionId

The revision identifies an immutable desired configuration.

It does not imply that every runtime component has already adopted it.

### 4.5 Applied Revision

An active component may report:

    DesiredRevision

    AppliedRevision

For example:

    DesiredRevision: 15
    AppliedRevision: 14

    StreamLifecycle: Active
    ConnectionState: Reconnecting

    AcquisitionReadiness: Not Ready

This may represent a valid configuration change awaiting
runtime convergence while the logical source-stream
runtime remains operational.

The accepted desired revision does not imply that
the affected resource has adopted the new configuration.

Physical reconnection and acquisition readiness remain
independent of configuration-revision acceptance.

### 4.6 RuntimeSnapshot

Conceptual representation:

    RuntimeSnapshot {
        runtime_instance_id

        lifecycle_state

        desired_revision
        applied_revision

        active_connections

        source_stream_summary
        derived_stream_summary

        recording_summary
        retention_summary

        health_summary

        captured_at
    }

The snapshot must remain bounded.

Detailed stream and dependency queries belong to separate operations.

### 4.7 StreamSnapshot

The Control API exposes a coherent operational snapshot
for each registered logical stream.

The snapshot combines authoritative observations from
the responsible subsystems without introducing competing
state machines.

Source-stream snapshots must distinguish persistent
configuration intent, effective acquisition demand,
logical runtime lifecycle, acquisition readiness,
integrity, recovery, and health.

#### Conceptual Representation

    StreamSnapshot {
        stream_id
        stream_kind

        source_stream_key

        persistent_desired_state
        effective_acquisition_required

        stream_lifecycle
        acquisition_readiness

        connection_identity
        connection_state
        connection_generation

        subscription_state
        subscription_generation

        integrity_state
        recovery_state
        coverage_state

        health

        desired_revision
        applied_revision

        recording_state

        observed_at
    }

This representation is conceptual.

The final serialized schema belongs to the Control API
contract and must be aligned with the shared domain
models in Document 02 and the metrics and health
contracts in Document 12.

Fields that do not apply to a particular stream type
must remain explicitly unavailable.

#### Source Identity

`stream_id` identifies the registered logical stream.

For a source stream, `source_stream_key` identifies
its canonical acquisition semantics.

The Stream Registry owns logical source identity.

Physical connection replacement, native subscription
restoration, and L2 resynchronization must not
independently change the logical source identity.

Derived streams retain their distinct identity
and lifecycle contracts.

Source-specific fields must not be fabricated for
derived streams.

#### Persistent Desired State

`persistent_desired_state` describes the accepted
persistent acquisition intent associated with
a registered source.

The Configuration Manager owns the accepted
desired configuration and its revisions.

A persistent desired state of `Inactive` does not
necessarily mean the physical acquisition is stopped.

Independent demand may still require the source.

#### Effective Acquisition Demand

`effective_acquisition_required` indicates whether
the Stream Registry currently has at least one
valid acquisition-demand registration for the source.

For example:

    Persistent Desired State: Inactive
    Recording Demand:         Present
    Subscriber Demand:        Present
    Effective Acquisition:    Required

This is a valid state.

The Control API must not infer effective acquisition
demand solely from persistent desired configuration.

The Stream Registry remains authoritative for
demand registration and effective demand calculation.

#### Logical Runtime Lifecycle

`stream_lifecycle` uses the authoritative
`StreamLifecycle` contract defined in
Document 02, Section 7.2.

For example:

    StreamLifecycle: Active

This means the logical runtime is executing.

It does not independently establish native
subscription readiness or market-data integrity.

#### Connection and Subscription State

Physical connection lifecycle originates from
the Connection Manager.

Native subscription lifecycle originates from
the Subscription Manager.

Their generations must remain separate.

For example:

    ConnectionState:   Reconnecting
    SubscriptionState: Pending

These states do not automatically change
`StreamLifecycle::Active`.

A physical connection may be shared by several
source streams with different subscription,
integrity, and readiness conditions.

#### Acquisition Readiness

`acquisition_readiness` indicates whether a source
currently satisfies its configured channel-specific
readiness requirements.

Readiness is determined from authoritative connection,
subscription, processor-initialization, and integrity
observations.

For trades, readiness requires the applicable native
subscription, processing, metadata, and integrity
prerequisites.

For L2, readiness additionally requires successful
snapshot and sequence synchronization.

The Control API must not report L2 acquisition
readiness merely because a native subscription
has been acknowledged.

A source may report:

    Persistent Desired State: Active
    Effective Acquisition:    Required

    StreamLifecycle:          Active
    ConnectionState:          Connected
    SubscriptionState:        Active
    IntegrityState:           Synchronizing

    AcquisitionReadiness:     Not Ready

This is valid.

The logical runtime is running while its L2 book
is completing synchronization.

#### Integrity, Recovery, and Coverage

The responsible stream processor owns integrity state.

The Recovery Coordinator owns recovery-operation state.

Historical coverage is maintained through the
authoritative recovery and retention contracts.

These observations must remain independent.

A stream may be acquisition-ready while historical
coverage remains incomplete.

Successful current acquisition recovery must not
be reported as complete historical recovery.

#### Health Assessment

`health` is supplied by the Metrics and Stream Health
subsystem.

Health evaluation consumes authoritative operational
observations and applicable diagnostic measurements.

It must not independently mutate connection,
subscription, integrity, recovery, or logical
stream lifecycle state.

The Control API must distinguish connection health
from overall source-stream health when both are exposed.

The health classification must reflect the
configured guarantees rather than being inferred
solely from `StreamLifecycle::Active`.

#### Configuration Revisions

`desired_revision` identifies the accepted desired
configuration revision.

`applied_revision` identifies the revision whose
required runtime effects have been successfully applied
to the relevant resource.

These values may differ during reconciliation.

A persisted desired revision does not imply that
the corresponding runtime changes are already active.

Revision information must remain distinct from
connection, subscription, book, and cursor generations.

#### Recording State

`recording_state` is supplied by the Recording Manager.

Recording may be enabled independently of persistent
source-acquisition intent.

Recording requirements may contribute acquisition
demand according to the Stream Registry contracts.

The Control API must not infer recording availability
from logical runtime lifecycle or acquisition
readiness alone.

#### Snapshot Ownership and Consistency

The Control API aggregates observations from
authoritative subsystem owners.

It must not independently maintain competing copies
of their state machines.

Snapshots must include an observation timestamp
and sufficient generation and revision context
to distinguish current observations from obsolete
asynchronous results.

Component observations may originate from
different collection instants.

Unless explicitly supported, a snapshot must not
claim atomic consistency across all subsystems.

The final field types, optionality, serialization,
and snapshot-versioning rules must be established
centrally before implementation.

### 4.8 Snapshot Consistency

Control-plane snapshots may combine measurements collected at slightly different moments.

This is acceptable for ordinary inspection.

However, each authoritative component state must retain sufficient identity, generation, and revision information to prevent misinterpretation.

### 4.9 Current State vs Historical Diagnostics

The API may expose recent operational events.

However, the event history is not a replacement for authoritative state.

If a client misses lifecycle notifications, it must refresh its current-state snapshot.

### 4.10 Runtime State Invariant

    Desired State
    Is Not Actual State.

    Actual State
    Is Not Health.

    Configuration Acceptance
    Is Not Runtime Convergence.

---

## 5. Control Command Contracts

Control commands request operations against authoritative runtime components.

Commands must use typed representations rather than arbitrary string-based internal method invocation.

### 5.1 Command Categories

Supported conceptual categories:

    Runtime

    Configuration

    SourceStreams

    DerivedStreams

    Recording

    Retention

    Recovery

    Monitoring

### 5.2 ControlRequest

Conceptual Rust representation:

```rust
pub struct ControlRequest {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub command: ControlCommand,
}
```

The exact serialized representation remains a protocol decision.

### 5.3 ControlCommand

Conceptual representation:

```rust
pub enum ControlCommand {
    GetRuntimeStatus,

    ListStreams,
    GetStream(StreamId),

    RegisterStream(SourceStreamDefinition),
    ActivateStream(StreamId),
    DeactivateStream(StreamId),
    RemoveStream(StreamId),

    ListDerivedStreams,
    GetDerivedStream(DerivedStreamId),

    CreateDerivedStream(DerivedStreamDefinition),
    UpdateDerivedStream(DerivedStreamUpdate),
    RemoveDerivedStream(DerivedStreamId),

    GetConfiguration,
    ValidateConfiguration(ConfigUpdate),
    ApplyConfiguration(ConfigUpdate),

    GetMetrics(MetricsQuery),
    GetHealth(HealthQuery),

    GetRecordingStatus,
    GetRetentionStatus(StreamId),

    GetOperation(OperationId),
}
```

This is an illustrative command vocabulary.

Concrete variants may use separate request objects.

The final protocol must distinguish persistent configuration mutations
from session-scoped operations.

#### Source-Stream Command Semantics

The source-stream commands operate on authoritative logical source
identities and must respect the demand-ownership contracts defined
in Documents 02 and 05.

| Command | Responsibility |
|---|---|
| `RegisterStream` | Register or reuse a logical source identified by its canonical `SourceStreamKey`. Registration does not independently establish acquisition demand. |
| `ActivateStream` | Enable persistent acquisition intent for the registered logical source. |
| `DeactivateStream` | Disable persistent acquisition intent without releasing independent demand owned by other consumers. |
| `RemoveStream` | Request removal of the logical registration, subject to dependency, demand-ownership, and configuration constraints. |

These commands must not bypass the Configuration Manager when they
change persistent desired configuration.

A successful command acknowledgement does not imply that the
requested runtime transition has completed.

#### Persistent Acquisition Commands

`ActivateStream` and `DeactivateStream` are persistent desired-state
mutations.

They must follow the authoritative configuration transaction:

    Validate Proposed Change
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

The Configuration Manager owns revision acceptance and persistence.

The Runtime Reconciler coordinates application.

The Stream Registry adds or releases the corresponding persistent
acquisition-demand registration.

The commands must not directly mutate authoritative persisted
configuration or bypass revision conflict detection.

The conceptual `StreamId` command arguments shown above are not
a complete serialized mutation schema.

The final request contract must carry or otherwise enforce the
applicable expected configuration revision, authorization context,
request identity, and idempotency requirements.

#### Independent Acquisition Demand

Persistent acquisition intent is independent of demand owned by:

- Recording.
- External subscribers.
- Derived streams.
- Retention sessions.
- Other explicitly supported demand owners.

`ActivateStream` establishes or retains the persistent configuration
demand for the specified source.

Repeated activation must not create multiple persistent demand
registrations or duplicate exchange-native subscriptions.

`DeactivateStream` releases only the persistent configuration demand.

For example:

    Persistent Desired State: Inactive
    Recording Demand:         Present
    Subscriber Demand:        Present
    Effective Acquisition:    Required

This is a valid runtime configuration.

The physical acquisition may remain operational because independent
demand owners still require the source.

`DeactivateStream` must not silently disconnect those consumers,
release their demand registrations, or invalidate healthy market-data
processing.

The Stream Registry derives effective acquisition requirements from
all current valid demand registrations.

#### Source Removal

`RemoveStream` is distinct from `DeactivateStream`.

It requests deletion of the logical source registration rather
than merely disabling persistent acquisition.

An ordinary removal request must not silently destroy independent
subscriber, recording, derived-stream, or retention demand.

If protected dependencies or independent demand prevent removal,
the command must return a structured conflict.

A coordinated removal policy may be used only when explicitly
supported, authorized, and validated.

The default dependency-removal behavior remains the
`RejectRemoval` policy defined in Document 05, Section 18.3.

Removing a persistently configured source definition must follow
the Configuration Manager's accepted-revision and persistence
contracts.

The logical registration must not be deleted before the required
configuration mutation has been durably accepted.

The registry must fence obsolete asynchronous operations before
final removal.

#### Session-Scoped Operations

Transient demand is managed through the authoritative acquisition
and demand-handle contracts defined in Document 02, Section 8.

A session-scoped acquisition must receive an independent `DemandId`.

Releasing such demand must affect only the corresponding ownership
claim.

Persistent `ActivateStream` and `DeactivateStream` commands must not
be used as substitutes for transient demand-handle acquisition
and release.

The exact externally exposed session-scoped demand command vocabulary,
if required, remains a separate wire-contract decision.

No additional serialized commands are established by this section.

### 5.4 Read Commands

Read commands must:

- Identify their target resource.
- Apply authorization.
- Enforce response-size limits.
- Return structured results.
- Preserve observation timestamps.
- Avoid expensive hot-path synchronization.

### 5.5 Mutating Commands

Mutating commands must:

- Identify the affected resource.
- Validate input.
- Validate authorization.
- Validate applicable configuration revisions.
- Establish an operation identity.
- Dispatch through the authoritative component owner.
- Return an explicit acceptance or rejection result.

The Control Server must not bypass the Configuration Manager for persistent desired-state changes.

### 5.6 Configuration Mutations

Persistent desired-state changes follow:

    Client Proposal
          |
          v
    Configuration Validation
          |
          v
    Expected Revision Check
          |
          v
    Compile Effective Configuration
          |
          v
    Calculate Semantic Diff
          |
          v
    Persist Accepted Revision
          |
          v
    Runtime Reconciliation

The complete configuration rules remain defined in Document 03.

### 5.7 Operational Commands

Some operations may not change persistent desired state.

Examples may include:

    RequestRecordingFlush

    RequestRecoveryStatus

    RequestConnectionDiagnostics

Such commands must still use validated component interfaces.

A manual recovery request must not bypass the Recovery Coordinator.

### 5.8 Command Idempotency

Mutating commands must define explicit retry and idempotency semantics.

Command idempotency, configuration-revision conflicts, and
acquisition deduplication are separate concerns.

#### Persistent Activation

Repeated `ActivateStream` requests must not establish duplicate
persistent acquisition-demand registrations.

For example:

    ActivateStream(A)
    ActivateStream(A)

must not create two logical source acquisitions or two equivalent
exchange-native subscriptions.

If persistent acquisition is already enabled, the command may
return an appropriate already-satisfied result, subject to
the applicable expected-revision and operation contracts.

#### Persistent Deactivation

Repeated `DeactivateStream` requests must not release independent
demand registrations or decrement shared demand ownership more
than once.

If persistent acquisition is already disabled, repeating the
command must not affect recording, subscribers, derived streams,
or retention sessions.

#### Source Removal

Repeated `RemoveStream` requests must not:

- Repeat resource release.
- Reuse obsolete stream generations.
- Resurrect a removed logical registration.
- Delete an unrelated source registration.

The control plane must distinguish an already completed removal,
a currently pending removal, and a new conflicting operation.

#### Request Retries

A retried request must not create a second independent operation
when the original accepted request is still identifiable.

The Control API must preserve the distinction between:

- Request identity.
- Configuration revision.
- Asynchronous operation identity.
- Logical stream identity.
- Demand registration identity.
- Native subscription generation.

The Stream Registry remains authoritative for source deduplication
and demand ownership.

The Configuration Manager remains authoritative for persistent
configuration revisions.

The final wire contract must specify the bounded request-idempotency
retention and retry behavior.

### 5.9 Revision Conflicts

Suppose:

    Current Configuration Revision = 42

Two clients submit changes using:

    ExpectedRevision = 42

Only one conflicting revision may be accepted.

The other must receive:

    RevisionConflict

The rejected client may refresh the configuration and submit a new validated proposal.

### 5.10 ControlResponse

Conceptual representation:

```rust
pub struct ControlResponse {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub result: ControlResult,
}
```

The result must be serializable independently of internal runtime structures.

### 5.11 ControlResult

Conceptual response categories:

    Snapshot

    ValidationResult

    OperationAccepted

    OperationStatus

    ConfigurationRevision

    Rejected

    Error

A successful command acknowledgement must not imply that a long-running operation has completed.

### 5.12 Structured Errors

Control errors must include:

    ErrorCategory

    ErrorCode

    Message

    AffectedResource

    Retryability

where applicable.

Errors must not require clients to parse human-readable descriptions to determine the failure category.

### 5.13 Control Command Invariant

    A Command Is Accepted
    Only After Validation.

    Acceptance Does Not Mean
    Successful Runtime Application.

---

## 6. Asynchronous Operation Lifecycle

Many runtime operations require asynchronous work.

Examples include:

- Establishing a WebSocket connection.
- Waiting for subscription acknowledgement.
- Synchronizing an L2 book.
- Activating a derived stream.
- Replacing a stream configuration.
- Recovering a recording gap.

The Control API must not block an individual request until all downstream operations complete.

### 6.1 OperationId

Every asynchronous control operation receives an identity.

Conceptually:

```rust
pub struct OperationId(pub u64);
```

The representation is illustrative.

Operation identifiers must remain unambiguous within their declared runtime scope.

### 6.2 Operation States

Supported states:

    Accepted

    Applying

    Completed

    Failed

    Cancelled

A detailed component may expose additional internal states.

The external API should retain a stable common lifecycle.

### 6.3 OperationStatus

Conceptual representation:

    OperationStatus {
        operation_id

        operation_kind
        target_resource

        requested_revision

        state

        created_at
        started_at
        completed_at

        result
        error
    }

The operation result must identify what was actually completed.

### 6.4 Operation Submission

    Client Sends Command
             |
             v
        Validate Request
             |
             v
       Accept Operation
             |
             v
       Assign OperationId
             |
             v
       Return Acceptance
             |
             v
      Execute Asynchronously

The client may query progress using the returned operation identifier.

### 6.5 Operation Completion

A command reaches `Completed` only when its declared completion
condition has been satisfied.

Command acceptance, persistent configuration application,
logical runtime activity, and acquisition readiness are
distinct outcomes.

#### Persistent Activation

For `ActivateStream`, the persistent desired-state mutation
and its runtime reconciliation must be distinguished from
channel-specific acquisition readiness.

Conceptually:

    Persistent Activation Requested
                  |
                  v
    Desired Revision Accepted and Persisted
                  |
                  v
    Persistent Demand Registration Established
                  |
                  v
    Effective Acquisition Requirement Evaluated
                  |
                  v
    Runtime Reconciliation Applied

The persistent configuration transition is successfully applied
when its required persistent-demand and reconciliation effects
have completed.

This does not independently establish acquisition readiness.

A newly required acquisition may continue asynchronously
through connection, subscription, and integrity initialization.

#### Acquisition Readiness

When an operation explicitly requires acquisition readiness,
successful completion additionally requires the configured
channel-specific readiness conditions.

For trades:

    Native Subscription Confirmed
    Processor Initialized
    Required Metadata Available
    Applicable Integrity Requirements Satisfied

For L2:

    Native Subscription Confirmed
    Processor Initialized
    Snapshot / Sequence Synchronization Completed
    Required Book Integrity Established

`StreamLifecycle::Active` alone is insufficient to establish
acquisition readiness.

A running logical runtime may remain active while a temporary
connection outage or L2 synchronization failure prevents readiness.

The Control API must not describe persistent-demand application
as successful acquisition readiness.

#### Persistent Deactivation

For `DeactivateStream`, completion requires:

- The desired configuration revision to be accepted and persisted.
- The persistent demand registration to be released or confirmed absent.
- The effective acquisition requirement to be recalculated.
- The required reconciliation effects to be completed.

If independent demand remains, the operation may complete while
the logical runtime and native subscription continue operating.

If no demand remains, completion must respect the configured
idle-retention or acquisition-shutdown requirements.

Successful persistent deactivation must not be represented
as proof that every physical subscription has stopped.

#### Source Removal

For `RemoveStream`, successful completion requires:

- Authorization and dependency checks.
- Any required persistent configuration mutation to be committed.
- Required demand-ownership conditions to be satisfied.
- Obsolete operations and generations to be fenced.
- Required resource cleanup to complete.
- The logical registration to be removed.

A removal blocked by independent demand or protected
dependencies must not be reported as completed.

#### Configuration Updates

Configuration acceptance establishes a persisted desired revision.

Runtime convergence is evaluated separately.

For a configuration update affecting multiple resources,
successful full convergence requires all applicable
resource transitions to meet their declared completion
conditions.

A valid accepted revision may remain partially applied while
an exchange or other resource is unavailable.

#### Operation Reporting

The Control API must identify what an operation actually completed.

At minimum, clients must be able to distinguish:

- Request accepted.
- Desired revision persisted.
- Persistent demand reconciled.
- Logical runtime started or stopped.
- Acquisition readiness achieved or lost.
- Source registration removed.
- Configuration reconciliation pending or completed.
- Operation failed or cancelled.

A command acknowledgement is not an acquisition-readiness guarantee.

The final operation-result schema must preserve these distinctions
without introducing contradictory lifecycle states.

### 6.6 Partial Application

A configuration revision may affect multiple resources.

Example:

    Activate Binance BTC Trades

    Activate Bybit BTC Trades

    Activate OKX BTC Trades

Two resources may activate successfully while the third remains unavailable.

The overall operation must report partial convergence rather than claim complete success.

### 6.7 Operation Failure

Failures may originate from:

    Validation

    Authorization

    Resource Admission

    Exchange Protocol

    Runtime Reconciliation

    Component Initialization

    Persistence

    Timeout

The error must identify the narrowest relevant failing component.

### 6.8 Operation Cancellation

Cancellation is supported only when the underlying operation provides safe cancellation semantics.

For example, cancelling a pending subscription attempt may not prove that the exchange did not apply an already transmitted command.

The owning component must reconcile ambiguous native state.

Cancellation must not bypass generation fencing.

### 6.9 Client Disconnection

Once an operation has been accepted into persistent desired state, client disconnection must not automatically cancel it.

Example:

    Ratatui:
        Enable BTCUSDT Recording

    Configuration Revision:
        Accepted

    Ratatui:
        Closed

    Runtime:
        Continues Applying Revision

Transient session-scoped operations may have different cancellation rules.

Those rules must be explicit.

### 6.10 Operation Retention

Completed operation summaries may be retained in bounded local memory or operational history.

The Control API must not maintain an unlimited operation ledger.

Persistent configuration revisions remain managed separately by the Configuration System.

### 6.11 Operation Status Queries

The control protocol should support:

    GetOperation(OperationId)

and bounded queries such as:

    ListPendingOperations

    ListRecentOperations

These provide enough information for the TUI to display progress.

### 6.12 Operation Invariant

    Acknowledged
        !=
    Applied.

    Applied
        !=
    Healthy.

    Healthy Current Operation
        !=
    Complete Historical Coverage.

These guarantees must remain independently represented.

---

## 7. Configuration Transaction Architecture

MarketForge Live manages persistent runtime configuration through immutable, validated revisions.

The Configuration Manager remains the authoritative owner of desired state.

The Runtime Reconciler remains responsible for applying accepted changes to running components.

The Control API provides access to these operations without directly modifying internal runtime state.

### 7.1 Configuration Transaction Objectives

A configuration transaction must:

- Begin from a known configuration revision.
- Validate the complete proposed configuration.
- Resolve inherited and overridden settings.
- Compile affected runtime specifications.
- Calculate a semantic configuration diff.
- Identify affected resources.
- Enforce configuration revision consistency.
- Persist accepted desired state atomically.
- Schedule required runtime reconciliation.
- Report application progress.
- Preserve unaffected resources.

A configuration transaction must not require restarting unrelated exchange connections.

### 7.2 Configuration Transaction Stages

Conceptually:

    Client Configuration Proposal
                  |
                  v
         Load Current Revision
                  |
                  v
        Expected Revision Check
                  |
                  v
          Structural Validation
                  |
                  v
           Semantic Validation
                  |
                  v
      Compile Effective Configuration
                  |
                  v
         Calculate Semantic Diff
                  |
                  v
       Validate Runtime Applicability
                  |
                  v
       Persist New Desired Revision
                  |
                  v
         Publish Accepted Revision
                  |
                  v
          Runtime Reconciliation
                  |
                  v
         Report Convergence Status

The first stages determine whether the proposed configuration can be accepted.

Runtime reconciliation determines whether the system can achieve the requested operational state.

### 7.3 ConfigurationTransaction

Conceptual representation:

    ConfigurationTransaction {
        transaction_id

        expected_revision

        proposed_changes

        validation_result
        compiled_plan

        semantic_diff
        affected_resources

        accepted_revision

        transaction_state
    }

This is a control-plane model.

It must not be accessed by canonical event-processing workers.

### 7.4 Transaction States

Supported conceptual states:

    Created

    Validating

    Validated

    Persisting

    Accepted

    Rejected

A configuration transaction ends when its desired-state revision is accepted or rejected.

Runtime convergence is tracked through a separate operation.

### 7.5 Validation Is Side-Effect Free

Configuration validation must not:

- Open exchange connections.
- Create subscriptions.
- Modify BookStore state.
- Start recording.
- Remove derived streams.
- Allocate permanent runtime resources.
- Change the currently accepted desired configuration.

Validation may inspect registered capabilities, resource budgets, and authoritative metadata.

### 7.6 Transaction Acceptance

A configuration transaction becomes accepted only when:

    Validation Successful

        AND

    Expected Revision Valid

        AND

    Desired State Persisted

The Configuration Manager must not expose a new accepted revision before the configured persistence commitment is established.

### 7.7 Configuration Transaction vs Runtime Transaction

MarketForge does not provide one atomic transaction spanning:

    Local Configuration Files

    Exchange WebSockets

    PostgreSQL Transactions

    Publisher Sessions

    Derived Stream Workers

These resources belong to independent failure domains.

Therefore, configuration persistence can be atomic while runtime application remains asynchronous and partially converged.

### 7.8 Transaction Invariant

    Configuration Acceptance
    Establishes A Durable
    Desired-State Revision.

    It Does Not Guarantee
    Immediate Runtime Convergence.

---

## 8. Configuration Revision Management

Every accepted configuration change creates a new immutable revision.

Revisions provide consistency between multiple management clients and independent runtime components.

### 8.1 ConfigRevisionId

The existing domain contract is:

```rust
pub struct ConfigRevisionId(pub u64);
```

Revision identifiers must remain unique and monotonically advancing within the persistent configuration history.

They must not be reused after rollback.

### 8.2 ConfigRevision

Conceptual representation:

    ConfigRevision {
        revision_id

        parent_revision

        created_at

        desired_configuration

        configuration_hash

        change_metadata
    }

The exact serialized representation belongs to the Configuration System.

### 8.3 Expected Revision

Every persistent mutation must identify the revision against which it was prepared.

Conceptually:

```rust
pub struct ConfigUpdate {
    pub expected_revision: ConfigRevisionId,
    pub changes: Vec<ConfigChange>,
}
```

The Configuration Manager accepts the update only when its expected revision matches the current authoritative revision.

### 8.4 Concurrent Client Example

Suppose:

    Current Revision:
        42

    Ratatui:
        Change recording batch size
        ExpectedRevision = 42

    Python:
        Add BTCUSDT source stream
        ExpectedRevision = 42

If Ratatui's update commits first:

    Current Revision:
        43

Python's original update must receive:

    RevisionConflict

Python may then retrieve revision 43, incorporate the new configuration state, and submit a revised proposal.

### 8.5 Revision Conflict Response

Conceptually:

    ConfigurationRevisionConflict {
        expected_revision

        current_revision

        affected_configuration_paths
    }

The conflict must not silently overwrite the accepted revision.

### 8.6 Configuration Diff

The Configuration Manager calculates semantic differences between revisions.

Conceptually:

    ConfigurationDiff {
        previous_revision
        proposed_revision

        changes

        affected_resources

        required_reconfiguration_policies
    }

A semantic diff must distinguish meaningful runtime changes from formatting differences in TOML.

### 8.7 Example: Harmless Formatting

Changing:

    snapshot_interval_ms = 1000

to an equivalent representation must not cause unrelated resources to restart.

The compiled effective values determine whether runtime behavior changes.

### 8.8 Example: Semantic Change

Changing:

    BTCUSDT Recording:
        Disabled

to:

    BTCUSDT Recording:
        Enabled

requires recording activation at an explicit canonical cursor boundary.

It does not require recreating the BTCUSDT WebSocket connection.

### 8.9 Revision Metadata

Revision metadata should preserve:

    RevisionId

    ParentRevision

    CreationTimestamp

    ChangeOrigin

    AffectedResources

    ConfigurationHash

    ApplicationStatusReference

Sensitive configuration values must not be copied into unrestricted revision diagnostics.

### 8.10 Revision History

The Configuration Manager may maintain bounded detailed revision history while preserving the authoritative current revision.

A durable revision log or immutable revision files may support rollback and diagnostics.

Retention of historical revisions must follow an explicit local-storage policy.

### 8.11 Rollback

Rollback creates a new configuration revision based on the contents of an earlier revision.

Example:

    Revision 40
        |
        v
    Revision 41
        |
        v
    Revision 42
        |
        v
    Rollback To Revision 40
        |
        v
    Revision 43

Revision 43 contains the selected earlier desired configuration.

It does not reuse revision 40's identity.

### 8.12 Rollback Limitations

Configuration rollback does not reverse:

- Already published canonical events.
- PostgreSQL database commits.
- Parquet retention operations.
- Historical coverage changes.
- Completed exchange subscriptions.
- Previously emitted derived events.

Runtime reconciliation applies the new desired state using ordinary transition rules.

### 8.13 Revision Invariant

    Every Accepted Revision
    Is Immutable.

    Every Rollback Creates
    A New Revision.

---

## 9. Runtime Reconciliation

Runtime reconciliation transforms accepted desired-state changes into controlled operations against authoritative component owners.

The Runtime Reconciler must preserve unaffected resources whenever possible.

### 9.1 Reconciliation Responsibilities

The reconciler must:

- Compare desired and actual state.
- Determine required component changes.
- Respect dependency ordering.
- Acquire required resources.
- Dispatch operations to component owners.
- Track asynchronous completion.
- Detect partial convergence.
- Retry recoverable application failures.
- Report terminal failures.
- Preserve generation safety.
- Release obsolete resources.

It must not implement exchange protocol logic or market-data transformations.

### 9.2 ReconciliationPlan

Conceptually:

    ReconciliationPlan {
        target_revision

        operations

        dependency_order

        affected_resources

        estimated_resource_requirements

        application_policy
    }

The plan is generated from the compiled configuration diff.

### 9.3 Reconciliation Operations

Supported conceptual operations include:

    StartSourceStream

    StopSourceStream

    UpdateSourcePolicy

    StartDerivedStream

    StopDerivedStream

    ReplaceDerivedStream

    UpdateRecordingPolicy

    UpdateMetricsPolicy

    UpdatePublicationLimits

    UpdateRetentionPolicy

    UpdateExporterConfiguration

Not all operations are initially hot-editable.

Unsupported runtime transitions must be rejected before application.

### 9.4 Dependency Ordering

The reconciler must apply changes according to their dependencies.

For source activation:

    Resolve Instrument Context
              |
              v
    Validate Stream Configuration
              |
              v
    Acquire Source Demand
              |
              v
    Activate Native Subscription
              |
              v
    Initialize Stream Processor
              |
              v
    Establish Channel Readiness

For derived activation:

    Validate Derived Definition
              |
              v
    Acquire Source Dependencies
              |
              v
    Initialize Transformation Runtime
              |
              v
    Establish Output Publication
              |
              v
    Activate Derived Stream

### 9.5 Independent Resource Transitions

Independent streams may reconcile concurrently.

Example:

    Binance BTCUSDT Trades

    Bybit ETHUSDT Trades

These operations may proceed independently if resource and exchange limits permit.

However, conflicting operations targeting one logical stream must be serialized.

### 9.6 Reconciliation Generations

Every asynchronous reconciliation operation must carry sufficient identity to reject obsolete completions.

Relevant identifiers include:

    ConfigurationRevision

    StreamGeneration

    SubscriptionGeneration

    ConnectionGeneration

    DerivedGeneration

A completion from an obsolete generation must not overwrite current runtime state.

### 9.7 Partial Convergence

Suppose a configuration revision requests:

    Binance BTC Trades:
        Enabled

    Bybit BTC Trades:
        Enabled

    OKX BTC Trades:
        Enabled

After reconciliation:

    Binance BTC Trades:
        StreamLifecycle: Active
        AcquisitionReadiness: Ready

    Bybit BTC Trades:
        StreamLifecycle: Active
        AcquisitionReadiness: Ready

    OKX BTC Trades:
        StreamLifecycle: Active
        ConnectionState: Reconnecting
        AcquisitionReadiness: Not Ready

The logical runtime for the OKX stream remains operational
while its physical connection is recovering.

The configuration revision may remain valid and accepted.

Its runtime convergence status is:

    Degraded

The reconciler must not report complete acquisition
convergence merely because all logical runtimes have
entered `StreamLifecycle::Active`.

Convergence must satisfy the completion conditions
of the applicable configuration transitions.

Persistent acquisition-demand reconciliation and
channel-specific acquisition readiness remain distinct
outcomes. Their operation status must identify which
guarantees have actually been established.

### 9.8 Reconciliation State

Conceptual representation:

    ReconciliationState {
        target_revision

        overall_status

        completed_operations
        pending_operations
        failed_operations

        affected_resources

        last_error
        evaluated_at
    }

Supported statuses:

    Pending

    Applying

    Converged

    Degraded

    Failed

### 9.9 Retryable Application Failures

A temporary exchange outage may prevent activation of a valid source stream.

The reconciler may continue attempting convergence according to the appropriate component recovery policy.

It must not implement an independent exchange reconnection loop.

The Connection Manager and Recovery Coordinator remain responsible for their domains.

### 9.10 Permanent Application Failures

If runtime application reveals an unsupported or irrecoverable condition:

- Mark the affected operation failed.
- Preserve the accepted desired state.
- Report the failed resource.
- Avoid interrupting unrelated resources.
- Require an appropriate configuration correction or authorized intervention.

### 9.11 Reconciliation Invariant

    The Reconciler
    Coordinates State Changes.

    It Does Not Replace
    Authoritative Component Owners.

---

## 10. Runtime Reconfiguration Policies

Not every configuration field can be safely changed while MarketForge Live is running.

The existing Configuration System defines the allowed reconfiguration categories.

### 10.1 ReconfigurationPolicy

```rust
pub enum ReconfigurationPolicy {
    Hot,
    StreamTransition,
    ConnectionReplacement,
    ServiceRestart,
    Immutable,
}
```

These policies apply to individual configuration fields.

### 10.2 Hot

`Hot` settings can be applied without replacing the affected stream or connection.

Examples:

    Logging Level

    Metrics Enabled

    Detailed Timing Enabled

    Metrics Snapshot Interval

    Recording Batch Size

    Recording Flush Interval

    Maximum Subscribers

The application boundary must be explicit.

### 10.3 StreamTransition

A `StreamTransition` changes the lifecycle or effective processing behavior of an affected stream.

Examples:

    Enable Source Stream

    Disable Source Stream

    Enable Recording

    Disable Recording

    Create Derived Stream

    Remove Derived Stream

    Update Supported Derived Policy

A transition must preserve unaffected source acquisitions.

### 10.4 ConnectionReplacement

Some changes require replacing a physical connection.

Examples:

    WebSocket Endpoint

    Authentication Context

    Handshake Headers

    Transport Protocol Version

Connection replacement is initially deferred as an interactive hot-update capability.

Such changes require the applicable controlled deployment or restart procedure.

### 10.5 ServiceRestart

A `ServiceRestart` setting cannot be applied to the existing runtime instance.

Examples may include:

    Structural Runtime Changes

    Unsupported Listener Replacement

    Prometheus Exporter Activation

    Prometheus Exporter Listen Address

The initial optional Prometheus exporter follows the restart-required policy defined in Document 03.

### 10.6 Immutable

`Immutable` settings cannot change within the applicable deployment or schema contract.

Examples include changes to canonical field semantics that require explicit schema migration.

The system must not silently apply incompatible changes as ordinary runtime updates.

### 10.7 Reconfiguration Classification

Each proposed change must be classified before persistence.

Conceptually:

    Configuration Change
            |
            v
    Identify Field Policy
            |
       +----+----+----------------+
       |         |                |
       v         v                v
      Hot    StreamTransition   RestartRequired

A proposal containing unsupported runtime changes must not be partially applied without an explicit transaction policy.

### 10.8 Hot Application Boundaries

Initially supported settings:

| Setting | Application boundary |
|---|---|
| Logging level | Immediate |
| Metrics enabled | Subsequent metric collection |
| Detailed timing | Subsequent measurements |
| Metrics snapshot interval | Next collection cycle |
| Recording batch size | Next batch |
| Recording flush interval | Next batch |
| Maximum subscribers | Future admission |

The configuration compiler must preserve these semantics.

### 10.9 Hot Update Safety

A hot update must not:

- Recreate unrelated source subscriptions.
- Reset canonical stream cursors.
- Invalidate valid BookStore generations.
- Reorder already accepted events.
- Discard pending recording batches.
- Terminate existing subscribers unnecessarily.

### 10.10 Semantic Derived Changes

A derived-stream transformation change may affect economic output or ordering.

Examples:

    Merge Source Set Changed

    Ordering Policy Changed

    Conversion Target Changed

    Conversion Path Changed

These changes require an explicit derived-generation or stream-identity boundary.

They must not silently reinterpret previously published events.

### 10.11 Recording Changes

Enabling recording establishes an explicit recording activation boundary.

Disabling recording must handle already admitted batches according to the Recording Manager's shutdown policy.

Recording changes must not automatically restart the source WebSocket.

### 10.12 Prometheus Exporter Changes

The exporter is independently configurable.

Initial policy:

    Enable Exporter:
        ServiceRestart

    Disable Exporter:
        ServiceRestart

    Change Listen Address:
        ServiceRestart

    Change Exposition Path:
        ServiceRestart

The existing native Metrics Registry continues operating whether export is enabled or disabled.

### 10.13 Reconfiguration Invariant

    A Configuration Update
    Must Affect Only Resources
    Required By Its
    Semantic Changes.

---

## 11. Persistent Desired-State Storage

MarketForge Live must restore its accepted desired configuration after ordinary process restarts.

The Configuration Manager owns persistent desired-state storage.

### 11.1 Persistence Objectives

Persistence must provide:

- Atomic publication of accepted revisions.
- Crash recovery.
- Schema version validation.
- Revision consistency.
- Detection of incomplete writes.
- Preservation of desired source streams.
- Preservation of derived-stream definitions.
- Preservation of recording settings.
- Preservation of global runtime settings.

The persistence system must not store canonical market-data events.

### 11.2 Storage Location

Persistent runtime configuration remains under MarketForge's local data directory.

Conceptually:

    data/.config/live/

Runtime-generated state may use:

    data/.live/

The exact file organization is defined by Document 03.

The Configuration Manager must avoid conflicting configuration authorities.

### 11.3 Desired-State Snapshot

Conceptually:

    DesiredConfigurationSnapshot {
        schema_version

        revision_id

        desired_configuration

        configuration_hash

        committed_at
    }

The snapshot must contain enough information to reconstruct effective configuration after loading the required static specifications and instrument metadata.

### 11.4 Atomic Persistence

The recommended file-commit procedure is:

    Serialize Candidate
           |
           v
    Write Temporary File
           |
           v
    Flush File Contents
           |
           v
    Synchronize File
           |
           v
    Atomically Rename
           |
           v
    Synchronize Parent Directory
           |
           v
    Publish Accepted Revision

The implementation must preserve consistency between revision identity and configuration contents.

### 11.5 Persistence Failure

If configuration persistence fails:

- Reject the proposed revision.
- Preserve the previous accepted revision.
- Report the storage failure.
- Avoid applying the uncommitted configuration as accepted desired state.

A failed file write must not leave a partially accepted desired-state revision.

### 11.6 Revision Recovery

After restart, the Configuration Manager loads the last valid committed revision.

Temporary or incomplete candidate files must not be treated as accepted revisions.

The loader must validate:

    Schema Version

    Revision Identity

    Configuration Integrity

    Required References

### 11.7 Schema Compatibility

A persisted configuration created by a previous software version may require explicit migration.

Unsupported schema versions must be rejected or migrated using a validated compatibility procedure.

The Configuration Manager must not silently reinterpret unknown fields.

### 11.8 Configuration Secrets

Sensitive credentials must not be written into unrestricted diagnostic or revision-history files.

Secret references and protected storage mechanisms should be used where applicable.

The control API must redact secret values from ordinary read responses.

### 11.9 Persistence Invariant

    Accepted Desired State
    Must Survive Restart
    According To Its
    Declared Durability Contract.

---

## 12. Runtime Startup and Configuration Recovery

Runtime startup reconstructs desired state and initializes the required components.

Previously observed runtime state is not automatically valid after process restart.

### 12.1 Startup Sequence

    MarketForge Live Starts
              |
              v
    Load Last Accepted Configuration
              |
              v
    Validate Configuration Schema
              |
              v
    Load Exchange Protocol Specifications
              |
              v
    Load Instrument Metadata Snapshot
              |
              v
    Compile Effective Runtime Plan
              |
              v
    Initialize Runtime Components
              |
              v
    Start Control Server
              |
              v
    Reconcile Desired Streams
              |
              v
    Report Startup Convergence

The exact startup task ordering may be refined according to component dependencies.

### 12.2 Independent Startup

MarketForge Live must be able to start without requiring an active Ratatui instance.

It must also support the independent-startup instrument metadata snapshot contract established in Document 06.

A running Python orchestration process is not required on the canonical event hot path.

### 12.3 Persistent Stream Restoration

Persistent source-stream definitions are reconstructed from accepted configuration.

The Stream Registry reestablishes their logical identities and desired states.

It does not restore obsolete physical connection handles.

### 12.4 Runtime Generations

After process restart, runtime components establish valid new generation identities.

Relevant identities include:

    ConnectionGeneration

    SubscriptionGeneration

    BookGeneration

    DerivedGeneration

    CursorEpoch

Previous runtime snapshots must not be treated as proof that these resources remain active.

### 12.5 Source Activation

Persistent enabled sources follow ordinary activation procedures.

For trades:

    Subscription Ready

    Processing Ready

    Continuity Evaluated

For L2:

    Subscription Ready

    Snapshot Initialized

    Book Synchronized

A source must not be marked healthy merely because its configuration was restored.

### 12.6 Derived Restoration

Persistently enabled derived streams are restored after their dependencies become available.

The Derived Stream Registry must validate:

- Input identities.
- Transformation definitions.
- Ordering policies.
- Currency-conversion references.
- Required source capabilities.

A failed dependency must not cause unrelated derived streams to fail unnecessarily.

### 12.7 Recording Restoration

Recording settings are restored from desired configuration.

The Recording Manager must reconcile previous committed coverage and current source cursor boundaries.

It must not assume that events accepted before the previous process crash were durably recorded.

### 12.8 Retention Restoration

Committed Parquet segments and manifests may be recovered according to Document 09.

Uncommitted in-memory events from the previous process must not be assumed available.

Retention continuity and canonical publication cursor recovery follow their independent contracts.

### 12.9 Prometheus Exporter Startup

If the effective configuration enables the optional Prometheus exporter:

1. Validate the listener configuration.
2. Initialize the exporter from the shared Metrics Registry.
3. Bind the configured read-only HTTP endpoint.
4. Begin serving bounded exposition requests.
5. Report exporter availability independently.

Exporter failure must not invalidate exchange acquisition or native monitoring.

### 12.10 Control Server Startup

The Control Server must initialize independently of Ratatui.

If it fails to start, the runtime supervisor must report a control-plane availability failure.

The underlying data plane must not be automatically terminated unless the deployment explicitly declares control-plane availability a mandatory runtime condition.

### 12.11 Startup Convergence

Startup completion must distinguish:

    Configuration Loaded

    Runtime Constructed

    Control Plane Available

    Persistent Acquisition Demand Reconstructed

    Required Logical Source Runtimes Started

    Required Source Acquisition Readiness Established

    Derived Stream Requirements Reconciled

    Recording Requirements Reconciled

    Health Evaluated

A running process may remain partially converged while exchange connectivity is recovering.
Logical runtime startup and acquisition readiness are
separate convergence milestones.

A source may be `StreamLifecycle::Active` while its
physical connection, native subscription, or integrity
requirements are still recovering.

The startup status must report incomplete acquisition
readiness without incorrectly declaring the logical
runtime stopped or failed.

Recording reconciliation must distinguish restored
configuration from successful recovery of the required
recording commitments.

Health evaluation must reflect the currently observed
runtime guarantees rather than imply that all resources
are healthy merely because startup has completed.

### 12.12 Startup Invariant

    Restored Configuration
    Establishes Desired State.

    It Does Not Restore
    Previous Runtime Readiness
    Or Historical Completeness.

---

## 13. Source Stream Management Commands

The Control API provides a unified interface for managing canonical source streams.

Source-stream management remains owned by the Stream Registry and Subscription Manager.

The Control API must not independently create exchange connections or subscriptions.

### 13.1 Supported Operations

The initial source-stream command set includes:

    RegisterSourceStream

    ListSourceStreams

    GetSourceStream

    ActivateSourceStream

    DeactivateSourceStream

    RemoveSourceStream

    GetSourceDependencies

    GetSourceStreamHealth

    GetSourceStreamMetrics

Source registration and activation are separate operations.

A registered stream does not necessarily have an active exchange subscription.

### 13.2 RegisterSourceStream

Conceptual request:

    RegisterSourceStream {
        source_definition

        instrument_context_reference

        expected_configuration_revision

        persistence_policy
    }

The request must identify a valid canonical source acquisition.

The Stream Registry resolves the source's semantic identity.

Equivalent acquisition requests must not create duplicate logical source streams.

### 13.3 Source Definition

A source definition must provide:

    Exchange

    MarketFamily

    InstrumentIdentity

    Channel

    ChannelParameters

    DesiredState

    SupportedOperationalPolicies

The exact representation must reuse the domain contracts from Document 02.

The Control API must not invent a second source-identity model.

### 13.4 Instrument Metadata

Source activation requires validated instrument metadata.

The existing Python-managed instrument catalog remains authoritative.

The Control API may receive instrument metadata through:

    Instrument Context Registration

or:

    Source Registration With Instrument Context

Both approaches must resolve to the shared immutable Rust InstrumentContext.

The Control API must not query Python or PostgreSQL for every incoming market-data event.

### 13.5 Registration Validation

Registration must validate:

- Exchange identity.
- Market family.
- Instrument identity.
- Channel capability.
- Acquisition parameters.
- Normalization requirements.
- Instrument metadata.
- Resource admission constraints.
- Existing equivalent source registrations.

Unsupported configurations must be rejected before activation.

### 13.6 Registration Results

Possible results:

    Registered

    ExistingEquivalentStream

    InvalidConfiguration

    IdentityConflict

    UnsupportedCapability

    ResourceLimitExceeded

A request resolving to an existing equivalent source may return its registered identity rather than create another source.

### ### 13.7 ActivateSourceStream

Conceptual request:

    ActivateSourceStream {
        stream_id

        expected_configuration_revision
    }

`ActivateSourceStream` enables persistent acquisition intent
for the registered logical source.

It is a persistent configuration mutation and must follow
the Configuration Manager's validation, revision-checking,
durable persistence, and reconciliation contracts.

Conceptually:

    Validate Current Configuration
              |
              v
    Check Expected Revision
              |
              v
    Persist Accepted Desired Revision
              |
              v
    Reconcile Persistent Demand
              |
              v
    Evaluate Effective Acquisition Requirement

If independent demand already keeps the source operational,
persistent activation must not create another native subscription.

If acquisition must be started, the responsible components
coordinate connection assignment, native subscription activation,
processor initialization, and channel-specific readiness.

The Control API must distinguish:

- Persistent activation successfully applied.
- Logical runtime started.
- Acquisition readiness achieved.

`StreamLifecycle::Active` indicates that the logical runtime
is running. It does not independently establish acquisition
readiness.

The operation returns an `OperationId` when completion
requires asynchronous work.

Persistent activation completion and acquisition readiness
must follow their separately declared completion contracts.
### 13.8 Trade Stream Readiness

A trade stream achieves acquisition readiness when:

- Its subscription is operational.
- Its decoder and normalizer are initialized.
- Instrument metadata is valid.
- Canonical processing can accept executions.
- Required integrity prerequisites are satisfied.

Activation does not prove complete historical trade coverage.

That remains a separate integrity and recovery condition.

### 13.9 L2 Stream Readiness

An L2 stream requires:

- A valid native subscription.
- Successful snapshot initialization.
- Valid sequence synchronization.
- An initialized shared BookStore.
- A valid book generation.

The Control API must not report L2 acquisition readiness merely
because the WebSocket subscription was acknowledged.

Persistent activation may be applied before L2 acquisition
readiness is achieved. These outcomes must remain distinguishable.

### 13.10 DeactivateSourceStream

Deactivation releases the relevant persistent demand.

Conceptually:

    DeactivateSourceStream {
        stream_id

        expected_configuration_revision
    }

The physical acquisition must remain active if other demand owners still require it.

Possible owners include:

    Recording

    IPC Subscribers

    Derived Streams

    Retention Sessions

    Other Persistent Configuration

### 13.11 Deactivation vs Removal

Deactivation preserves the logical registration.

Removal deletes the persistent source definition and requests release of its associated resources.

The Control API must distinguish:

    DeactivateSourceStream

from:

    RemoveSourceStream

Both operations must follow dependency validation.

### 13.12 RemoveSourceStream

Before removal, the Stream Registry must identify active dependencies.

If the source is required by a derived stream or another protected resource, the operation must follow the configured removal policy.

The initial default is:

    RejectRemoval

when active dependencies would be invalidated.

Cascading removal requires an explicitly supported and authorized operation.

### 13.13 Source Mutation Invariant

    Source Control Commands
    Must Not Bypass
    Stream Registry Ownership
    Or Native Subscription
    Deduplication.

---

## 14. Derived Stream Management Commands

Derived streams are first-class logical streams managed by the Combined Stream Registry.

The Control API exposes their configuration and lifecycle operations without implementing merge or conversion logic.

### 14.1 Supported Operations

    CreateDerivedStream

    ListDerivedStreams

    GetDerivedStream

    ActivateDerivedStream

    DeactivateDerivedStream

    UpdateDerivedStream

    RemoveDerivedStream

    GetDerivedDependencies

    GetDerivedStreamHealth

    GetDerivedStreamMetrics

### 14.2 Derived Stream Definition

A derived-stream definition references the existing contracts established in Document 10.

It may specify:

    DerivedStreamId

    StreamKind

    InputStreams

    OperatorGraph

    OrderingPolicy

    SourceFailurePolicy

    ConversionPolicy

    RecordingPolicy

    RetentionPolicy

    ConfigurationRevision

The Control API must not define incompatible transformation models.

### 14.3 Supported Merge Modes

The initial merge modes remain compatible with the historical implementation:

    Trades

    Depth

    Combined

`Combined` permits trades and depth events in one chronological timeline.

The Control API must preserve the existing historical logical event schema.

### 14.4 CreateDerivedStream

Conceptual request:

    CreateDerivedStream {
        definition

        expected_configuration_revision
    }

Validation must include:

- Derived stream identity.
- Required source dependencies.
- Dependency graph acyclicity.
- Input schema compatibility.
- Transformation capabilities.
- Ordering policy.
- Conversion requirements.
- Resource budgets.

Creation does not necessarily activate the derived runtime.

### 14.5 Dependency Acquisition

Activation acquires dependencies through their authoritative registries.

For example:

    Binance BTC Trades
              |
              +----+
                   |
    Bybit BTC Trades
              |    |
              +----+----> Combined BTC Trades
                   |
    OKX BTC Trades |
              +----+

The Combined Stream Registry must reuse existing source acquisitions.

It must not create duplicate exchange subscriptions.

### 14.6 ActivateDerivedStream

Activation must:

1. Resolve dependencies.
2. Validate required source capabilities.
3. Acquire dependency handles.
4. Establish input cursor boundaries.
5. Initialize merge state.
6. Initialize conversion references where applicable.
7. Register derived publication identity.
8. Activate output processing.

The operation completes only when its declared readiness requirements are satisfied.

### 14.7 Derived Configuration Changes

Semantic changes may include:

    Source Set Changed

    Source Rank Changed

    Ordering Policy Changed

    Late Event Policy Changed

    Conversion Target Changed

    Conversion Path Changed

    Reference Staleness Changed

Such changes require an explicit transformation revision and applicable derived-generation transition.

### 14.8 UpdateDerivedStream

Conceptual request:

    UpdateDerivedStream {
        derived_stream_id

        expected_configuration_revision

        changes
    }

The Configuration Manager validates and persists the proposed revision.

The Runtime Reconciler coordinates replacement of the affected derived runtime.

### 14.9 Generation Replacement

A semantic update must prevent old and new derived generations from publishing conflicting output under the same authoritative cursor identity.

Conceptually:

    Existing Generation
              |
              v
    Compile Replacement
              |
              v
    Resolve Dependencies
              |
              v
    Establish Input Boundary
              |
              v
    Fence Previous Generation
              |
              v
    Activate Replacement Generation

Already published derived events retain their original transformation semantics.

### 14.10 DeactivateDerivedStream

Deactivation stops the derived runtime while retaining the persistent stream definition.

It must:

- Stop new derived processing admission.
- Resolve pending output according to policy.
- Fence the current runtime generation.
- Release dependency handles.
- Preserve required diagnostics.
- Report unresolved output obligations.

### 14.11 RemoveDerivedStream

Removal deletes the persistent definition after the applicable runtime and dependency transitions complete.

Removing a derived stream must not deactivate underlying source streams still required by other consumers.

### 14.12 Derived Command Invariant

    The Control API
    Manages Derived Definitions
    And Runtime Intent.

    The Merge And Conversion
    Engines Remain Responsible
    For Transformation Correctness.

---

## 15. Recording Control Commands

PostgreSQL recording is independently configurable for source and supported derived streams.

The Recording Manager owns recording admission, batching, durability, and recovery.

### 15.1 Supported Operations

    GetRecordingStatus

    ListRecordingStreams

    GetStreamRecordingStatus

    EnableStreamRecording

    DisableStreamRecording

    RequestRecordingFlush

    GetRecordingCoverage

    ListRecordingGaps

    GetRecordingRecoveryStatus

### 15.2 EnableStreamRecording

Conceptual request:

    EnableStreamRecording {
        stream_id

        expected_configuration_revision

        recording_policy
    }

Enabling recording is a persistent configuration change when requested as persistent desired state.

The Configuration Manager must validate the policy before acceptance.

### 15.3 Recording Activation

Recording activation establishes an explicit canonical cursor boundary.

Conceptually:

    Recording Enabled
           |
           v
    Validate Stream
           |
           v
    Initialize Recording Writer
           |
           v
    Establish Recording Boundary
           |
           v
    Activate Recording Admission
           |
           v
    Report Recording State

The Control API must not claim that events preceding the activation boundary were already recorded.

### 15.4 Recording Does Not Recreate Acquisition

If the source stream is already active, enabling recording should attach the recording dependency to the existing source.

It must not recreate the WebSocket connection merely because recording was enabled.

### 15.5 DisableStreamRecording

Disabling recording must:

- Stop new recording admission at the defined boundary.
- Resolve admitted batches according to policy.
- Preserve committed database records.
- Report the final committed recording frontier.
- Release recording-specific demand and resources.

The source remains active when required by other consumers.

### 15.6 RequestRecordingFlush

A flush command requests that the Recording Manager seal eligible open batches and begin persistence.

Conceptual request:

    RequestRecordingFlush {
        stream_id

        operation_id
    }

A successful flush request does not imply that every batch has committed.

The operation result must distinguish:

    FlushRequested

    BatchesSealed

    CommitCompleted

    CommitFailed

### 15.7 Recording Coverage Queries

The Control API exposes bounded coverage summaries.

Conceptual request:

    GetRecordingCoverage {
        stream_id

        interval
    }

The response may include:

    CommittedCursorIntervals

    RecordingGaps

    DurabilityStatus

    SourceIntegrityStatus

    EvaluatedAt

Detailed coverage originates from the Recording Manager.

### 15.8 Recording Gaps

Known recording gaps must remain inspectable after ordinary recording resumes.

The API must not report recording as historically complete merely because PostgreSQL is currently connected.

### 15.9 Recording Recovery Commands

Manual recovery requests, if supported, must route through the Recording Manager and Recovery Coordinator.

They must not bypass:

- Commit reconciliation.
- Cursor identity validation.
- Retention coverage checks.
- Idempotent persistence.
- Recording-generation fencing.

### 15.10 Recording Command Invariant

    Recording Control
    Must Preserve The Distinction
    Between Admission,
    Database Commitment,
    And Historical Coverage.

---

## 16. Retention and Bootstrap Support Commands

The Control API exposes Live Retention state and selected operational controls.

Historical bootstrap orchestration remains a separate subsystem.

### 16.1 Supported Operations

    GetRetentionStatus

    GetRetentionCoverage

    GetRetentionResourceUsage

    ListRetentionSessions

    GetRetentionSession

    RequestRetentionProtection

    ReleaseRetentionProtection

    GetParquetSpillStatus

The exact initial command set may be narrower.

### 16.2 Retention Status

Conceptual response:

    RetentionStatus {
        stream_id

        available_cursor_intervals

        protected_cursor_intervals

        memory_bytes
        parquet_bytes

        pending_spill_bytes

        retention_state
        coverage_state
    }

The Retention Manager remains authoritative.

### 16.3 Retention Protection

A bootstrap consumer may request protection for a required retained interval.

Conceptually:

    RequestRetentionProtection {
        stream_id

        from_cursor

        protection_policy

        requesting_session
    }

Protection must be admitted by the Retention Manager.

The Control API must not fabricate a successful reservation.

### 16.4 Protection Identity

A successful protection request returns a distinct reservation identity.

Conceptually:

    RetentionReservationId

The identity must be associated with its owner and lifecycle.

### 16.5 Protection Ownership

Retention protection must define:

    Owner

    ProtectedStream

    ProtectedCursorInterval

    ExpirationPolicy

    ReleasePolicy

    ResourceBudget

A disconnected client must not leave an unbounded permanent protection unless the reservation has an explicit durable ownership contract.

### 16.6 ReleaseRetentionProtection

A release operation must identify the reservation being released.

It must be idempotent.

It must not release another consumer's protection without authorization.

### 16.7 Retention vs Historical Bootstrap

The Control API may expose retention operations required by a future Bootstrap Engine.

However, it does not:

- Download exchange archives.
- Reconcile historical executions.
- Reconstruct historical order books.
- Coordinate historical merge tasks.
- Execute historical currency conversion.

Those responsibilities remain outside the Live Control API.

### 16.8 Retention Command Invariant

    Retention Control
    Exposes Availability
    And Protection Contracts.

    It Does Not Implement
    Historical Bootstrap Logic.

---

## 17. Recovery Control Commands

Recovery is coordinated by the Recovery Coordinator and the affected authoritative component owners.

The Control API exposes recovery state and selected operator operations.

### 17.1 Supported Queries

    ListActiveRecoveries

    GetRecoveryOperation

    GetStreamRecoveryStatus

    GetConnectionRecoveryStatus

    GetRecoveryPolicy

    GetUnresolvedIntegrityFailures

### 17.2 Recovery Status

Conceptual representation:

    RecoveryStatus {
        recovery_operation_id

        target_resource

        recovery_category

        lifecycle_state

        current_attempt
        consecutive_failures

        next_retry_at

        current_guarantees

        last_error

        evaluated_at
    }

The exact representation must reuse the Recovery Coordinator's existing contracts.

### 17.3 Manual Recovery Requests

The initial control plane may support:

    RequestStreamRecovery

    RequestConnectionRestart

    RequestBookResynchronization

These operations must be capability-checked.

They must not bypass the existing recovery machinery.

### 17.4 Connection Restart

A connection restart may affect multiple logical source streams sharing the same physical connection.

Before executing the operation, the control plane should identify:

    TargetConnection

    AffectedSubscriptions

    AffectedSourceStreams

    ExpectedIntegrityImpact

The Connection Manager remains the sole owner of physical connection replacement.

### 17.5 Book Resynchronization

A manual L2 resynchronization request must route through the authoritative synchronization and recovery controller.

It must:

- Fence obsolete synchronization work.
- Invalidate the old book generation where required.
- Establish a new authoritative snapshot.
- Validate native sequence continuity.
- Publish a new valid generation only after readiness.

The Control API must not directly call mutable BookStore operations outside the owning stream processor.

### 17.6 Trade Recovery

A manual trade-recovery request may specify a validated coverage interval or recovery target.

The Trade Processor and Recovery Coordinator remain responsible for:

- Native execution identity.
- REST recovery capabilities.
- Rate limiting.
- Duplicate detection.
- Coverage reconciliation.
- Unresolved gap classification.

### 17.7 Recovery Request Validation

Manual recovery must not start unless:

- The target exists.
- The requested operation is supported.
- The caller is authorized.
- The target generation is valid.
- Required recovery resources are available.
- The operation does not conflict with an existing authoritative recovery.

### 17.8 Recovery Idempotency

Repeated recovery requests must not create competing recovery workflows for the same target and recovery generation.

The Recovery Coordinator must consolidate equivalent requests when appropriate.

### 17.9 Recovery Completion

A recovery operation completes only when its declared recovery guarantees are established.

For example:

    Connection Restored:
        Yes

    Book Synchronized:
        No

The Control API must not report complete L2 recovery.

### 17.10 Recovery Command Invariant

    The Control API
    May Request Recovery.

    The Recovery Coordinator
    Determines And Executes
    Recovery Semantics.

---

## 18. Monitoring and Health Queries

Monitoring queries expose the Metrics Registry and HealthEvaluator without creating a second monitoring implementation.

The primary contracts remain defined in Document 12.

### 18.1 Monitoring Query Categories

    RuntimeMetrics

    RuntimeHealth

    ExchangeMetrics

    ConnectionMetrics

    SourceStreamMetrics

    SourceStreamHealth

    DerivedStreamMetrics

    DerivedStreamHealth

    PublisherMetrics

    RetentionMetrics

    RecordingMetrics

    RecoveryMetrics

    ResourceMetrics

### 18.2 GetRuntimeMetrics

The response contains a bounded global metrics snapshot.

Conceptually:

    RuntimeMetricsResponse {
        snapshot_epoch
        snapshot_id

        captured_at

        runtime_metrics

        aggregate_stream_metrics

        resource_metrics
    }

Global aggregation must avoid double-counting source events that also appear in derived outputs.

### 18.3 GetRuntimeHealth

The response contains:

    OverallHealth

    HealthDimensions

    DegradedComponents

    UnhealthyComponents

    UnknownComponents

    ActiveRecoverySummary

    EvaluatedAt

The overall health value must not conceal the reason for degradation.

### 18.4 GetStreamMetrics

A stream metrics query identifies:

    StreamId

    StreamGeneration

where applicable.

The response may include:

    AcceptedEventRate

    ProcessingLatency

    IntegrityMetrics

    PublicationMetrics

    RecordingMetrics

    RetentionMetrics

Fields not supported by the stream type remain unavailable.

### 18.5 GetStreamHealth

A health response preserves:

    DesiredState

    LifecycleState

    OverallHealth

    IndividualHealthDimensions

    StructuredHealthReasons

    ObservationTimestamp

The HealthEvaluator remains authoritative for the health assessment.

### 18.6 GetConnectionMetrics

Connection metrics may include:

    ReceivedMessages

    ReceivedBytes

    HeartbeatRTT

    ReconnectionAttempts

    CurrentQueuePressure

    LastReceiveTimestamp

Connection metrics must remain scoped to the correct connection generation.

### 18.7 GetDerivedStreamMetrics

A derived-stream response may include:

    RequiredSourceCount

    ActiveSourceCount

    MergeInputRate

    MergeOutputRate

    ReorderBufferEvents

    WatermarkProgress

    LateEventCount

    ConversionAvailability

The API must distinguish merge ordering health from conversion availability.

### 18.8 Query Time Ranges

The initial internal Metrics Registry maintains bounded recent history.

Monitoring queries may support:

    LatestSnapshot

    RecentSnapshots

    BoundedTimeWindow

The Control API must not claim unlimited historical metrics retrieval.

Prometheus may provide longer-term metric history when external collection is enabled.

### 18.9 Missing Measurements

Unavailable measurements must not be represented as numeric zero.

Examples:

    ExchangeLatency = Unavailable

    RecordingStatus = Disabled

    ConversionMetrics = NotApplicable

These conditions have different meanings.

### 18.10 Snapshot Freshness

Monitoring responses must identify their observation timestamps.

If a snapshot is stale, the API must preserve that information.

The Control Server must not label stale monitoring data as current merely because the request completed successfully.

### 18.11 Monitoring Query Limits

Monitoring queries must enforce:

    MaximumReturnedStreams

    MaximumHistorySamples

    MaximumResponseBytes

    QueryTimeout

Large diagnostic requests must not block the canonical processing hot path.

### 18.12 Prometheus Exporter Status

The Control API may expose:

    GetPrometheusExporterStatus

Conceptual response:

    PrometheusExporterStatus {
        enabled

        lifecycle_state

        listen_address
        exposition_path

        last_successful_export

        scrape_requests_total
        scrape_failures_total

        last_error
    }

The response must not expose secrets or unrestricted listener internals.

The exporter remains independently optional.

### 18.13 Prometheus vs Control Monitoring

The control protocol provides structured runtime inspection.

The Prometheus exporter provides read-only numerical metric exposition.

These interfaces have different purposes.

| Control Monitoring API | Prometheus Exporter |
|---|---|
| Structured Rust runtime snapshots | Prometheus metric families |
| Stream definitions and identities | Bounded metric labels |
| Detailed health reasons | Numeric health indicators |
| Operation progress | Metric counters and gauges |
| Recent local snapshots | Scrapeable measurements |
| Versioned control protocol | Standard HTTP exposition |
| Authorized management queries | Read-only monitoring |

Prometheus must not be used to execute stream-management operations.

### 18.14 Monitoring Invariant

    Monitoring Queries
    Observe Existing Runtime State.

    They Must Not Modify
    Canonical Processing
    Or Become Independent
    Integrity Authorities.

---

## 19. Command Authorization Boundaries

Different command categories require different authority.

The initial local control interface must distinguish observation from mutation.

### 19.1 Permission Categories

Conceptual permissions:

    Monitor

    Configure

    ManageStreams

    ManageRecording

    ManageRetention

    RequestRecovery

    ManageRuntime

These are logical authorization categories.

The initial implementation may use a simpler role model if it enforces the required boundaries.

### 19.2 Read-Only Client

A read-only client may inspect:

    Runtime Status

    Stream Status

    Metrics

    Health

    Recovery Progress

    Recording Status

It must not modify persistent desired state.

### 19.3 Management Client

An authorized management client may submit validated configuration and stream-lifecycle operations.

It must not bypass configuration revision checks.

### 19.4 Recovery Permissions

Recovery commands may affect several dependent streams.

Manual connection restart and book resynchronization should require explicit management authorization.

### 19.5 Retention Permissions

Retention protection may consume substantial memory or disk resources.

The control plane must authorize and account for reservations.

One client must not release another client's active protection without permission.

### 19.6 Transport-Level Security

Unix socket permissions provide an initial local access-control boundary.

Where supported, peer credentials may provide additional caller identity.

The server must not trust client-supplied role names without an authoritative authorization mechanism.

### 19.7 Prometheus Security

The optional Prometheus endpoint is read-only.

It is not part of the mutating Control API.

Its default listener binds to:

    127.0.0.1

Non-loopback exposure requires explicit security configuration.

### 19.8 Authorization Invariant

    Access To Metrics
    Does Not Imply
    Permission To Modify
    MarketForge Runtime State.

---

## 20. Control Protocol Transport and Framing

MarketForge Live exposes its authoritative management interface through a dedicated Unix domain socket.

The control protocol is independent of the canonical market-data publication protocol defined in Document 08.

### 20.1 Transport Requirements

The initial transport is:

    Unix Domain Socket
    Address Family: AF_UNIX
    Socket Type: SOCK_STREAM

Default conceptual endpoint:

    data/.live/marketforge-control.sock

The final path is configurable through Document 03.

The Control Server must support:

- Multiple concurrent clients.
- Independent client sessions.
- Explicit message framing.
- Request-response correlation.
- Asynchronous operation tracking.
- Bounded messages.
- Protocol version negotiation.
- Structured error responses.
- Graceful client disconnection.
- Deterministic shutdown.

The protocol must not assume that one socket read corresponds to one complete request.

### 20.2 Separation From Market-Data Publication

The runtime maintains distinct interfaces:

    marketforge-data.sock

    marketforge-control.sock

Their responsibilities are:

| Endpoint | Responsibility |
|---|---|
| Data socket | Canonical and derived market-data events |
| Control socket | Configuration, commands, queries, diagnostics |
| Prometheus HTTP endpoint | Optional read-only numerical metrics |

Control messages must never enter canonical event streams.

Canonical events must never be interpreted as control commands.

### 20.3 Protocol Encoding

The control protocol uses a structured serialization format inside an explicitly framed transport.

For the initial implementation, JSON is the recommended encoding.

Reasons:

- Control traffic is relatively low frequency.
- Python clients can implement the protocol easily.
- Ratatui can reuse the same serialized contracts.
- Messages are straightforward to inspect during debugging.
- Schema evolution is manageable.
- Control-message serialization is outside the market-data hot path.

The protocol must not use arbitrary JSON values as internal domain models.

JSON is a transport representation of typed Rust contracts.

### 20.4 Wire Framing

The recommended initial framing is:

    Length-Prefixed JSON

Conceptually:

    +----------------------+
    | Payload Length       |
    | 4 bytes, big-endian  |
    +----------------------+
    | JSON Payload         |
    | N bytes              |
    +----------------------+

The payload length must represent the number of serialized JSON bytes.

The frame prefix is not included in that length.

### 20.5 Frame Validation

Before allocating a payload buffer, the server must validate:

    DeclaredPayloadLength
        <=
    MaximumRequestBytes

Oversized frames must be rejected.

The server must not allocate memory based on an unvalidated client-controlled length.

### 20.6 Partial Reads

Unix stream sockets provide ordered bytes, not application message boundaries.

A client may send:

    Length Prefix

followed by:

    Partial JSON Payload

followed by:

    Remaining JSON Payload

The receiver must reconstruct the complete frame before decoding the request.

### 20.7 Partial Writes

Socket writes may complete partially.

The sender must ensure that an entire framed response is transmitted or explicitly report a transport failure.

A partially written response must never be treated as successfully delivered.

### 20.8 Frame Size Limits

The control protocol must define:

    MaximumRequestBytes

    MaximumResponseBytes

    MaximumBufferedInputBytes

    MaximumBufferedOutputBytes

The final limits belong to runtime configuration.

Large collections must use bounded queries and pagination rather than unrestricted responses.

### 20.9 Protocol Versioning

Every control request and response carries:

    ProtocolVersion

The server must reject unsupported incompatible versions.

Version negotiation may occur through an initial handshake or an explicit protocol-information request.

The initial implementation should remain simple and support a single current protocol version.

### 20.10 Message Categories

The wire protocol distinguishes:

    Request

    Response

    Notification

    ProtocolError

These categories must be unambiguous.

### 20.11 ControlMessage

Conceptual representation:

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ControlMessage {
    Request(ControlRequest),
    Response(ControlResponse),
    Notification(ControlNotification),
    ProtocolError(ControlProtocolError),
}
```

This representation is illustrative.

The final schema must preserve stable, documented external names.

### 20.12 Wire Format Invariant

    Control Messages
    Must Have Explicit
    Boundaries, Versions,
    Types, And Size Limits.

---

## 21. Request and Response Protocol

Every request has a unique identity within its client session.

Responses must identify the originating request.

### 21.1 Request Envelope

Conceptual representation:

```rust
pub struct ControlRequest {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub command: ControlCommand,
}
```

The request identifier is supplied by the client.

It is not a stream identifier or configuration revision.

### 21.2 Example Request

Illustrative JSON:

```json
{
  "type": "request",
  "payload": {
    "protocol_version": 1,
    "request_id": 104,
    "command": {
      "GetRuntimeStatus": null
    }
  }
}
```

The exact enum encoding must be finalized consistently across Rust and Python clients.

### 21.3 Response Envelope

Conceptual representation:

```rust
pub struct ControlResponse {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub result: ControlResult,
}
```

Every normal response must contain the originating request identifier.

### 21.4 Successful Query Response

Illustrative JSON:

```json
{
  "type": "response",
  "payload": {
    "protocol_version": 1,
    "request_id": 104,
    "result": {
      "RuntimeStatus": {
        "runtime_state": "running",
        "desired_revision": 42,
        "active_source_streams": 48,
        "active_derived_streams": 6
      }
    }
  }
}
```

The response schema is illustrative and does not replace the authoritative runtime snapshot contracts.

### 21.5 Asynchronous Command Response

A command requiring asynchronous work returns an operation identity.

Conceptually:

```json
{
  "type": "response",
  "payload": {
    "protocol_version": 1,
    "request_id": 105,
    "result": {
      "OperationAccepted": {
        "operation_id": 8501,
        "state": "accepted"
      }
    }
  }
}
```

The client subsequently queries the operation's status or receives an applicable notification.

### 21.6 Request Identity Scope

Request IDs are unique within a client session while outstanding.

Two independent clients may use the same request ID.

The server must correlate requests using:

    ClientSessionId

    RequestId

### 21.7 Request Reuse

A client must not reuse a request ID while a previous request with that ID remains unresolved.

The server must reject ambiguous simultaneous reuse.

After completion, reuse may be allowed according to the protocol contract.

### 21.8 Request Ordering

Requests received through one socket retain transport order.

However, independent asynchronous operations may complete in a different order.

Example:

    Request 100:
        GetRuntimeMetrics

    Request 101:
        GetRecordingCoverage

The server may respond:

    Response 101

    Response 100

Clients must correlate by request ID.

### 21.9 Read Consistency

A successful read response must preserve the relevant observation metadata.

Examples:

    SnapshotId

    SnapshotEpoch

    ObservedAt

    ConfigurationRevision

The response must not imply globally atomic consistency across independent components.

### 21.10 Request-Response Invariant

    Request Identity
    Correlates Communication.

    Operation Identity
    Correlates Runtime Work.

    These Are Different
    Identity Domains.

---

## 22. Client Session Management

The Control Server manages independent client sessions.

Each session has explicit admission, authorization, resource, and cleanup policies.

### 22.1 ClientSession

Conceptual representation:

    ClientSession {
        session_id

        protocol_version

        peer_identity
        permissions

        connected_at

        active_requests
        pending_responses

        active_notifications

        session_state
    }

The structure is illustrative.

### 22.2 Session Lifecycle

    Client Connected
           |
           v
    Transport Validation
           |
           v
    Peer Authorization
           |
           v
    Protocol Negotiation
           |
           v
    Session Active
           |
           v
    Request Processing
           |
           v
    Session Closing
           |
           v
    Resource Cleanup
           |
           v
    Session Closed

### 22.3 Session Identity

Each accepted connection receives a unique session identity.

Conceptually:

    ClientSessionId

Session identities must not be confused with subscriber identities from the canonical market-data Publisher.

A single application may maintain both a data-subscriber session and a control session.

These are separate transport and authorization contexts.

### 22.4 Client Types

The server may receive connections from:

    Ratatui

    Python Orchestration

    CLI

    External Authorized Application

Client type is diagnostic information.

Authorization must not depend exclusively on a client-provided type string.

### 22.5 Session Admission

Before accepting an active session, the server must enforce:

    MaximumControlClients

    AvailableSessionCapacity

    ValidPeerAuthorization

    RuntimeShutdownState

If admission fails, the connection must be rejected without affecting existing sessions.

### 22.6 Concurrent Requests

Each session has a bounded number of outstanding requests.

Conceptually:

    MaximumConcurrentRequestsPerClient

This prevents one client from submitting unlimited expensive queries.

### 22.7 Response Buffering

Outgoing responses must use bounded buffering.

A client that stops reading responses must not cause unlimited server memory growth.

Possible outcomes include:

    RequestRejected

    SessionBackpressure

    SessionDisconnected

The selected policy must not affect canonical data publication.

### 22.8 Session Idle Timeout

An idle control connection may remain open for interactive clients.

The server may apply a configurable idle timeout.

The timeout policy must account for clients that subscribe to infrequent operational notifications.

### 22.9 Client Cancellation

A client may cancel a pending request when the protocol supports cancellation.

Cancellation of a request does not automatically cancel a persistent runtime operation already accepted by the Configuration Manager.

For example:

    Cancel GetRecordingCoverage:
        Cancel pending query

    Cancel Accepted Configuration Revision:
        Not equivalent to rollback

### 22.10 Unexpected Disconnect

On unexpected disconnection:

- Cancel session-local queries.
- Remove notification subscriptions.
- Release response buffers.
- Release session-scoped resources.
- Preserve accepted persistent configuration revisions.
- Preserve authoritative runtime operations already committed.

### 22.11 Session Resource Invariant

    Every Client Session
    Must Have Bounded
    Resource Consumption
    And Deterministic Cleanup.

---

## 23. Request Dispatch and Command Execution

The Control Server routes validated requests to the appropriate component interfaces.

Request dispatch must preserve authoritative ownership.

### 23.1 Dispatcher Responsibilities

The Request Dispatcher must:

- Decode typed requests.
- Validate protocol compatibility.
- Validate authorization.
- Validate request parameters.
- Resolve target identities.
- Enforce request limits.
- Route to appropriate handlers.
- Correlate asynchronous results.
- Produce structured responses.
- Report errors consistently.

### 23.2 Dispatch Architecture

    Framed Request
          |
          v
    Protocol Decoder
          |
          v
    Request Validation
          |
          v
    Authorization
          |
          v
    Request Dispatcher
          |
     +----+-----+----------------+
     |          |                |
     v          v                v
    Read     Config/Stream    Operational
    Query      Mutation        Command
     |          |                |
     v          v                v
    Snapshot  Configuration    Component
    Provider    Manager         Owner

### 23.3 Handler Categories

Recommended categories:

    RuntimeQueryHandler

    ConfigurationHandler

    SourceStreamHandler

    DerivedStreamHandler

    RecordingHandler

    RetentionHandler

    RecoveryHandler

    MonitoringHandler

These are logical boundaries.

The implementation should not introduce unnecessary handler objects when ordinary Rust modules and functions provide sufficient separation.

### 23.4 Read-Only Dispatch

Read-only requests should use:

    Immutable Snapshots

    Component Read Interfaces

    Bounded State Queries

The dispatcher must not expose mutable references to internal runtime structures.

### 23.5 Mutating Dispatch

Persistent configuration mutations are routed through:

    Configuration Manager
             |
             v
    Runtime Reconciler
             |
             v
    Component Owner

A mutation must not bypass the Configuration Manager simply because a convenient component method exists.

### 23.6 Operational Dispatch

Transient operational commands may be dispatched to their owners through validated interfaces.

Examples:

    Recording Flush
        -> Recording Manager

    Connection Restart
        -> Connection Manager

    Recovery Request
        -> Recovery Coordinator

    Retention Reservation
        -> Retention Manager

These commands must preserve ownership and generation checks.

### 23.7 Dispatch Timeouts

A request handler must have a bounded execution deadline.

A timeout may indicate:

    QueryTimedOut

or:

    OperationStillRunning

The response must distinguish these conditions.

A control request timeout must not silently cancel an already accepted persistent operation.

### 23.8 Locking Rules

The dispatcher must not hold global registry or configuration locks while:

- Waiting for network I/O.
- Waiting for database transactions.
- Waiting for exchange acknowledgements.
- Waiting for recovery completion.
- Serializing large responses.
- Writing to slow client sockets.

### 23.9 Dispatch Invariant

    Request Dispatch
    Must Remain Bounded
    And Must Preserve
    Component Ownership.

---

## 24. Operational Notifications and Subscriptions

The Control API may provide subscriptions to low-frequency operational notifications.

Notifications allow clients to observe significant runtime changes without continuously polling every component.

They remain separate from canonical market-data events.

### 24.1 Notification Categories

Supported conceptual categories:

    RuntimeLifecycle

    StreamLifecycle

    ConnectionLifecycle

    ConfigurationRevision

    ReconciliationProgress

    HealthTransition

    RecoveryTransition

    RecordingState

    RetentionState

    ResourcePressure

### 24.2 ControlNotification

Conceptual representation:

```rust
pub struct ControlNotification {
    pub notification_id: NotificationId,
    pub category: NotificationCategory,
    pub observed_at_ns: i64,
    pub payload: NotificationPayload,
}
```

The precise serialization belongs to the versioned control protocol.

### 24.3 Notification Subscription

A client may request notifications by category or applicable resource scope.

Conceptually:

    SubscribeNotifications {
        categories

        stream_ids

        connection_ids
    }

The server validates the subscription and applies bounded delivery limits.

### 24.4 Notification Sources

Notifications originate from authoritative component state transitions.

Examples:

    Stream Registry
        -> StreamActivated

    Connection Manager
        -> ConnectionLost

    Recovery Coordinator
        -> RecoveryCompleted

    HealthEvaluator
        -> HealthStatusChanged

    Configuration Manager
        -> RevisionAccepted

The Control API must not independently manufacture authoritative state changes.

### 24.5 Notification Ordering

Notifications from one authoritative component should preserve that component's transition ordering.

Global ordering across unrelated components is not guaranteed.

Every notification must include sufficient identity and revision information to interpret it safely.

### 24.6 Notification Delivery

Operational notifications may use bounded best-effort delivery.

A slow client may miss notifications.

The server must not block authoritative component transitions indefinitely to preserve every notification for every client.

### 24.7 Notification Gap Detection

If a client misses notifications, it must be able to request an authoritative snapshot.

Where practical, notification revisions or monotonic identifiers should allow clients to detect discontinuities.

However, receiving consecutive notification IDs does not establish canonical market-data continuity.

### 24.8 Snapshot Reconciliation

Recommended client behavior:

    Connect
       |
       v
    Subscribe To Notifications
       |
       v
    Obtain Current Snapshot
       |
       v
    Apply Newer Notifications
       |
       v
    Refresh Snapshot On Gap

To avoid losing transitions between subscription establishment and snapshot retrieval, notifications must carry applicable revisions or sequence identifiers.

The client must reject obsolete notifications after applying a newer snapshot.

### 24.9 Health Notifications

Health notifications should report:

    AffectedComponent

    PreviousHealth

    CurrentHealth

    StructuredReason

    ObservationTimestamp

    HealthEvaluationRevision

They must not replace detailed health queries.

### 24.10 Configuration Notifications

Configuration notifications should distinguish:

    RevisionAccepted

    ReconciliationStarted

    ReconciliationDegraded

    ReconciliationConverged

    ReconciliationFailed

A revision acceptance notification must not claim that all affected streams are already operational.

### 24.11 Monitoring Snapshot Notifications

Periodic metrics snapshots may be exposed through notification subscriptions.

However, the initial implementation may use polling.

Continuous snapshot streaming is not required for the first Ratatui implementation.

### 24.12 Notification Invariant

    Notifications Describe
    Operational Changes.

    Authoritative Snapshots
    Remain The Source
    For Current State.

---

## 25. Control Error Contracts

The Control API must return structured errors with stable semantic categories.

Clients must not parse human-readable strings to determine error behavior.

### 25.1 Error Categories

Recommended categories:

    Protocol

    Validation

    Authorization

    RevisionConflict

    ResourceNotFound

    UnsupportedOperation

    ResourceLimit

    OperationConflict

    Timeout

    ComponentUnavailable

    Persistence

    Internal

### 25.2 ControlError

Conceptual representation:

```rust
pub struct ControlError {
    pub code: ControlErrorCode,
    pub message: String,
    pub resource: Option<ResourceId>,
    pub retryable: bool,
}
```

Additional diagnostic context may be included through structured optional fields.

### 25.3 Protocol Errors

Protocol errors include:

    UnsupportedVersion

    InvalidFrame

    MessageTooLarge

    InvalidEncoding

    InvalidMessageType

    MissingRequiredField

    DuplicateRequestId

Protocol errors must not modify runtime state.

### 25.4 Validation Errors

Validation errors include:

    UnsupportedExchange

    UnsupportedChannel

    InvalidInstrument

    InvalidSubscriptionParameters

    InvalidDerivedDependencies

    DependencyCycle

    InvalidRecordingPolicy

    InvalidMetricsConfiguration

Errors should identify the relevant configuration path when applicable.

### 25.5 Revision Conflicts

A revision conflict means the submitted expected revision does not match the current authoritative desired-state revision.

The response should include:

    ExpectedRevision

    CurrentRevision

    ConflictingResource

The client may refresh and resubmit.

The server must not silently merge conflicting changes unless an explicitly validated merge protocol exists.

### 25.6 Resource Not Found

A request targeting a nonexistent resource must receive an explicit not-found error.

Examples:

    UnknownStreamId

    UnknownDerivedStreamId

    UnknownConnectionId

    UnknownOperationId

Unknown identities must not be interpreted as requests to create new resources.

### 25.7 Operation Conflict

An operation conflict occurs when an incompatible operation is already active.

Example:

    RemoveStream

while:

    ReplaceStreamGeneration

is underway.

The authoritative owner determines whether the new operation should be serialized, rejected, or supersede the previous operation.

### 25.8 Component Unavailable

A component may temporarily be unavailable.

Examples:

    Recording Manager Recovering

    Stream Registry Initializing

    Retention Manager Unavailable

    Control Plane Shutting Down

The error must distinguish temporary unavailability from invalid configuration.

### 25.9 Timeout Semantics

Timeouts must identify the affected boundary.

Examples:

    RequestProcessingTimeout

    ComponentResponseTimeout

    OperationCompletionTimeout

    ClientTransportTimeout

A timeout does not prove that a mutating operation failed to execute.

When the outcome is ambiguous, clients must reconcile using operation identity and authoritative state rather than blindly resubmit.

### 25.10 Retryability

Retryability is not determined solely by error severity.

Examples:

| Error | Typical retry behavior |
|---|---|
| Revision conflict | Refresh revision and resubmit |
| Invalid configuration | Correct request |
| Temporary component unavailable | Retry according to policy |
| Permission denied | Do not retry without authorization change |
| Request timeout | Query operation outcome before retry |
| Resource limit exceeded | Wait or change requested resources |

### 25.11 Error Redaction

Control errors must not expose:

- API credentials.
- Authentication tokens.
- Sensitive connection headers.
- Database passwords.
- Private configuration secrets.
- Unrestricted filesystem contents.

Diagnostic context must be filtered according to caller authorization.

### 25.12 Error Invariant

    Control Errors
    Must Be Typed,
    Bounded,
    And Actionable.

    An Ambiguous Outcome
    Must Never Be Reported
    As A Confirmed Failure
    Or Success Without Evidence.

---

## 26. Control-Plane Resource Safety

The Control API must remain lightweight under normal and abnormal client behavior.

### 26.1 Resource Limits

The initial design requires explicit limits for:

    ControlClientCount

    RequestsPerClient

    PendingResponses

    RequestPayloadBytes

    ResponsePayloadBytes

    NotificationQueueBytes

    QueryResultRows

    MonitoringHistorySamples

    OperationHistoryEntries

### 26.2 Admission Control

New client sessions and requests must be admitted only when sufficient control-plane capacity exists.

Resource exhaustion must produce a bounded rejection.

It must not cause unbounded memory allocation.

### 26.3 Expensive Queries

Potentially expensive operations include:

    Full Stream Listing

    Detailed Dependency Inspection

    Large Metrics History Query

    Recording Coverage Inspection

    Retention Interval Enumeration

These should use pagination, filtering, cached snapshots, or bounded component queries.

### 26.4 Fairness

One client must not monopolize the control server.

The dispatcher should enforce per-client concurrency limits and avoid unbounded work queues.

### 26.5 Canonical Processing Isolation

The Control API must not introduce new blocking points in:

    WebSocket Receive

    Raw Message Decoding

    Canonical Normalization

    Trade Continuity Validation

    BookStore Mutation

    Event Publication

    Recording Admission

    Retention Admission

Control operations may update configuration through safe synchronization boundaries.

They must not hold locks across the entire market-data processing pipeline.

### 26.6 Monitoring Request Isolation

Monitoring clients may request metrics frequently.

Repeated requests must read existing snapshots rather than force full aggregation or expensive component inspection every time.

### 26.7 Client Failure

A client crash or disconnection must result in:

- Session cleanup.
- Response buffer release.
- Notification subscription removal.
- Cancellation of session-local reads.
- Preservation of accepted persistent operations.

### 26.8 Control Server Failure

A Control Server task failure must be detected by the Runtime Supervisor.

The supervisor may restart the control listener when safe.

The failure must not automatically invalidate exchange connections, canonical event processing, or unrelated runtime state.

### 26.9 Control Server Restart

Restarting the Control Server must not reset:

    ConfigurationRevision

    SourceStreamIdentity

    SourceStreamCursor

    BookGeneration

    RecordingCommitFrontier

    RetentionCoverage

These remain owned by their authoritative runtime components.

### 26.10 Shutdown

During runtime shutdown:

    Stop Accepting New Sessions
               |
               v
    Stop Accepting New Mutations
               |
               v
    Resolve In-Flight Requests
               |
               v
    Report Accepted Operation States
               |
               v
    Close Notification Streams
               |
               v
    Close Client Sessions
               |
               v
    Remove Socket Endpoint

The shutdown deadline must be finite.

### 26.11 Final Resource Invariant

    Control-Plane Load
    Must Not Become
    A Source Of
    Unbounded Memory Growth
    Or Canonical Processing
    Interruption.

---

## 27. Python Orchestration Integration

Python orchestration communicates with MarketForge Live through the versioned Runtime Control API.

Python remains responsible for the authoritative instrument catalog and its associated metadata-management workflows.

Rust Live remains responsible for runtime acquisition, normalization, integrity validation, publication, retention, recording, and recovery.

### 27.1 Integration Objectives

The Python client must support:

- Establishing a control connection.
- Negotiating the supported protocol version.
- Querying runtime capabilities.
- Registering validated instrument contexts.
- Creating and managing source streams.
- Creating and managing derived streams.
- Applying configuration revisions.
- Inspecting operation progress.
- Querying stream health.
- Inspecting recording and retention.
- Recovering from control connection failures.

Python must not access mutable Rust runtime internals directly.

### 27.2 Architectural Boundary

    Python Orchestration
             |
             +-- Instrument Catalog
             |
             +-- Metadata Revisions
             |
             +-- Desired Configuration
             |
             v
       Python Control Client
             |
             v
       Unix Control Socket
             |
             v
        Rust Control Server
             |
             +-- Configuration Manager
             |
             +-- Stream Registry
             |
             +-- Derived Stream Registry
             |
             +-- Runtime Reconciler

The Python control client is an external management client.

It is not part of the Rust canonical event-processing pipeline.

### 27.3 Shared Client Protocol

Python and Ratatui must use the same:

    ProtocolVersion

    RequestEnvelope

    ResponseEnvelope

    CommandVocabulary

    ErrorCategories

    ConfigurationRevisionRules

    OperationLifecycle

Different programming languages may use different client implementations, but they must preserve identical protocol semantics.

### 27.4 Python Client Structure

Conceptual organization:

    PythonControlClient
        |
        +-- Transport
        |
        +-- Framing
        |
        +-- Serialization
        |
        +-- RequestCorrelation
        |
        +-- RuntimeQueries
        |
        +-- ConfigurationCommands
        |
        +-- StreamCommands
        |
        +-- OperationTracking

This is a logical representation.

The implementation should reuse existing Python orchestration infrastructure where appropriate.

### 27.5 Connection Establishment

The client connects to the configured Unix control socket.

It must verify:

- Socket availability.
- Supported protocol version.
- Successful session establishment.
- Required operation permissions.

The client must not assume that a connected control socket means exchange acquisition is healthy.

### 27.6 Instrument Metadata Provisioning

The Python-managed instrument catalog remains authoritative.

Python may deliver validated metadata to Rust through the established instrument-context provisioning contract.

Conceptually:

    Python Instrument Catalog
              |
              v
    Resolve Instrument Revision
              |
              v
    Serialize Instrument Context
              |
              v
       Control API Request
              |
              v
    Rust Instrument Registry
              |
              v
    Validate And Publish Context

The Rust Instrument Registry stores immutable, revisioned runtime contexts.

The Control API must not create a competing instrument metadata schema.

### 27.7 Metadata Revision Updates

When Python detects an instrument metadata change:

1. Create or identify the authoritative metadata revision.
2. Determine the affected instruments.
3. Submit the updated metadata to Rust.
4. Validate the replacement context.
5. Identify affected source and derived streams.
6. Apply supported changes at safe boundaries.
7. Observe the runtime reconciliation outcome.

Normalization-critical changes must preserve the rules established in Document 06.

### 27.8 Python Runtime Queries

The client should support:

    GetRuntimeStatus

    ListSourceStreams

    ListDerivedStreams

    GetConfiguration

    GetOperation

    GetStreamHealth

    GetRecordingStatus

    GetRetentionStatus

Queries must preserve the control protocol's request and response contracts.

### 27.9 Python Stream Management

Python may submit persistent source-stream definitions.

However, source acquisition deduplication remains owned by the Rust Stream Registry.

Submitting an equivalent source request must not create duplicate native subscriptions.

### 27.10 Python Configuration Updates

Python must use expected-revision checks.

Conceptually:

    CurrentRevision = 42

    Python Proposes Change

    ExpectedRevision = 42

    Server Validates

    NewRevision = 43

A concurrent Ratatui update may produce a revision conflict.

Python must refresh and revalidate rather than silently overwrite the other client's changes.

### 27.11 Asynchronous Operation Tracking

Python should support operation handles.

Conceptually:

```python
operation = await client.activate_stream(stream_id)

status = await client.get_operation(
    operation.operation_id
)
```

The interface is illustrative.

A returned operation identifier means the command was accepted.

It does not establish successful activation.

### 27.12 Control Connection Failure

If the Unix control connection fails:

- Report control-plane unavailability.
- Reconnect according to a bounded policy.
- Reestablish protocol compatibility.
- Refresh authoritative runtime state.
- Reconcile previously submitted operations.

The client must not blindly replay non-idempotent mutations after an ambiguous connection failure.

### 27.13 Python Client Invariant

    Python Owns
    Instrument Metadata Authority.

    Rust Owns
    Live Runtime Execution.

    The Control API
    Defines Their Management
    Communication Boundary.

---

## 28. Ratatui Client Integration

The Ratatui management interface is an independent client of the Runtime Control API.

It must not own MarketForge Live's execution lifecycle.

The detailed visual interface is specified in Document 14.

### 28.1 Integration Objectives

Ratatui must support:

- Connecting to an existing Live daemon.
- Inspecting global runtime health.
- Listing source streams.
- Inspecting source-stream details.
- Managing source-stream lifecycle.
- Listing and managing derived streams.
- Inspecting recording and retention.
- Applying supported configuration changes.
- Tracking asynchronous operations.
- Displaying validation and runtime errors.
- Remaining responsive during component failures.

### 28.2 Independent Process Model

The intended architecture is:

    marketforge-live
          |
          v
    Rust Live Daemon
          |
          v
    Unix Control Socket
          |
          v
    marketforge-live-tui
          |
          v
       Ratatui UI

Closing the TUI must not terminate the daemon.

The daemon must remain operational without an attached terminal.

### 28.3 TUI Control Client

The TUI should use a dedicated Rust client implementation.

Conceptually:

    LiveControlClient {
        connection

        protocol_version

        request_correlation

        pending_operations

        current_snapshots

        notification_subscription
    }

The client must communicate through serialized protocol messages.

It must not acquire references to mutable internal daemon objects.

### 28.4 TUI Startup

Recommended sequence:

    Start Ratatui
          |
          v
    Connect To Control Socket
          |
          v
    Verify Protocol Compatibility
          |
          v
    Retrieve Runtime Snapshot
          |
          v
    Retrieve Stream Summary
          |
          v
    Retrieve Health Snapshot
          |
          v
    Render Dashboard

If the daemon is unavailable, Ratatui must display a connection failure rather than assume that no streams exist.

### 28.5 Dashboard Integration

The Dashboard consumes:

    RuntimeSnapshot

    MetricsSnapshot

    HealthSnapshot

It may display:

    Runtime State

    Active Connections

    Active Source Streams

    Active Derived Streams

    Event Throughput

    Processing Latency

    Recovery Operations

    Recording State

    Retention State

The TUI must not recalculate authoritative health from raw counters.

### 28.6 Streams View

The Streams view consumes:

    ListSourceStreams

    GetSourceStream

    GetSourceStreamHealth

    GetSourceStreamMetrics

Supported user actions may include:

    Register

    Activate

    Deactivate

    Remove

    Inspect

    Enable Recording

    Disable Recording

All mutations use the same commands available to Python and other authorized clients.

### 28.7 Derived Streams View

The Derived Streams view consumes:

    ListDerivedStreams

    GetDerivedStream

    GetDerivedDependencies

    GetDerivedStreamHealth

It may support:

    Create Merge

    Update Merge

    Remove Merge

    Configure Conversion

    Inspect Watermark State

    Inspect Source Participation

The UI must not implement merge execution or currency conversion.

### 28.8 Recording View

The Recording view consumes Recording Manager status through the Control API.

It may display:

    PostgreSQL Connectivity

    Active Recording Streams

    Recording Backlog

    Commit Latency

    Recording Gaps

    Recovery Progress

    Retention Pressure

Recording status must distinguish admission from committed durability.

### 28.9 Settings View

The Settings view consumes:

    GetConfiguration

    GetEffectiveConfiguration

    ValidateConfiguration

    ApplyConfiguration

    GetConfigurationRevision

The TUI should preserve explicit inheritance semantics.

Resetting a field to its default must remove its override rather than copy the current default value.

### 28.10 Configuration Editing

Recommended workflow:

    Open Settings
          |
          v
    Retrieve Current Revision
          |
          v
    Create Local Draft
          |
          v
    Modify Supported Fields
          |
          v
    Validate Draft
          |
          v
    Display Semantic Diff
          |
          v
    Submit Expected Revision
          |
          v
    Track Runtime Application

Draft changes must not affect the daemon before submission.

### 28.11 Validation Feedback

Validation responses should identify:

    ConfigurationPath

    ErrorCategory

    ErrorMessage

    ApplicableConstraint

The TUI may highlight the affected field.

It must not independently implement weaker configuration validation rules.

### 28.12 Revision Conflict Handling

If another client modifies configuration while Ratatui is editing:

    ExpectedRevision != CurrentRevision

Ratatui must display the conflict.

The user may reload, compare changes, and prepare a new validated draft.

The UI must not silently overwrite the newer configuration.

### 28.13 Operation Progress

Ratatui must distinguish:

    Command Accepted

    Operation Applying

    Operation Completed

    Operation Failed

For example:

    BTCUSDT L2 Activation

    Subscription:
        Active

    Book Synchronization:
        In Progress

    Overall Operation:
        Applying

The TUI must not display successful activation before required book readiness is established.

### 28.14 Notification Handling

The initial TUI may poll monitoring and operation snapshots.

Optional notification subscriptions may improve responsiveness.

When notifications are used, the client must:

- Track applicable revisions.
- Reject obsolete notifications.
- Detect gaps where supported.
- Refresh authoritative snapshots after a gap.

### 28.15 TUI Disconnection

If the control connection is lost:

- Preserve the local UI process.
- Display the disconnected state.
- Disable unsafe mutation actions.
- Attempt reconnection according to policy.
- Refresh authoritative snapshots after reconnection.

Previously displayed runtime state must not remain labeled current indefinitely.

### 28.16 Optional Prometheus Status

The Settings or Dashboard view may display:

    Prometheus Exporter:
        Disabled

or:

    Prometheus Exporter:
        Listening

The TUI does not embed Grafana or require a Prometheus server.

Exporter activation and listener changes follow their configured restart-required policy.

### 28.17 Ratatui Invariant

    Ratatui Is A Client
    Of The Runtime Control API.

    Its Lifecycle
    Is Independent Of
    The Live Daemon.

---

## 29. Shared Control Client Contracts

Rust Ratatui and Python orchestration must use interoperable control-protocol definitions.

### 29.1 Protocol Contract Ownership

The Rust Live control protocol is the authoritative wire contract.

Its schema must be documented independently of internal Rust object layouts.

The protocol must not depend on:

- Rust memory layout.
- Enum discriminant memory representation.
- Process-local pointers.
- Tokio task identities.
- Internal synchronization primitives.

### 29.2 Serialization Compatibility

The same serialized request must have equivalent meaning for:

    Rust Control Client

    Python Control Client

    Other Compatible Clients

The wire schema must define:

    Field Names

    Required Fields

    Optional Fields

    Enum Representation

    Numeric Representation

    Timestamp Representation

    Error Representation

    Versioning Rules

### 29.3 Numeric Safety

The control protocol may carry:

    InstrumentId

    EventTimestampNs

    CursorOffset

    ConfigurationRevision

    Decimal Values

Large integers and exact decimals must be serialized without loss.

For JSON control messages, exact decimal quantities should be represented as strings.

Python clients must not convert exact canonical numeric values through binary floating-point arithmetic.

### 29.4 Timestamp Semantics

Control responses may contain:

    EventTimestampNs

    ObservationTimestampNs

    MonotonicElapsedDuration

These must retain distinct semantics.

Monotonic clock readings must not be exposed as globally comparable Unix timestamps.

### 29.5 Protocol Version

Every request identifies its protocol version.

Clients must reject or explicitly handle incompatible server versions.

Backward-compatible optional fields may be added without changing existing field meanings.

### 29.6 Unknown Fields

The protocol must define whether unknown fields are:

    Ignored

    Rejected

    Preserved

Configuration mutations should reject unknown fields when silently ignoring them could change the requested behavior.

Read-only responses may permit forward-compatible optional fields.

### 29.7 Unknown Commands

Unsupported commands must receive a structured error.

They must not be silently ignored.

### 29.8 Shared Test Fixtures

The project should maintain serialized request and response fixtures.

Examples:

    runtime_status_request.json

    runtime_status_response.json

    activate_stream_request.json

    operation_accepted_response.json

    configuration_conflict_response.json

    stream_health_response.json

These fixtures must be validated by both Rust and Python client tests.

### 29.9 Contract Invariant

    One Wire Protocol.

    Multiple Client
    Implementations.

    Identical Runtime
    Command Semantics.

---

## 30. Control Protocol Testing

The Runtime Control API requires deterministic tests covering serialization, framing, request dispatch, authorization, configuration updates, and operation tracking.

### 30.1 Testing Categories

Required categories:

    Serialization

    Framing

    Protocol Versioning

    Client Sessions

    Request Dispatch

    Configuration Transactions

    Revision Conflicts

    Runtime Reconciliation

    Stream Commands

    Derived Commands

    Recording Commands

    Retention Commands

    Recovery Commands

    Monitoring Queries

    Authorization

    Failure Isolation

### 30.2 Framing Tests

Verify:

- Complete length-prefixed frames.
- Partial length-prefix reads.
- Partial payload reads.
- Multiple frames in one socket read.
- Partial writes.
- Empty or malformed payloads.
- Oversized frame rejection.
- Connection closure during frame reception.

The decoder must not allocate an unbounded buffer from an untrusted length prefix.

### 30.3 Serialization Tests

Verify:

- All supported command variants.
- All supported response variants.
- Structured errors.
- Optional fields.
- Exact numeric values.
- Timestamp serialization.
- Unknown commands.
- Unsupported protocol versions.

### 30.4 Request Correlation Tests

Send multiple concurrent requests.

Verify that responses are correctly associated with:

    ClientSessionId

    RequestId

Response completion order must not affect correlation correctness.

### 30.5 Duplicate Request Tests

Verify:

- Duplicate outstanding request identifiers are rejected.
- Completed request identifiers follow the documented reuse policy.
- Repeated mutating commands do not accidentally create duplicate runtime operations.

Request correlation is separate from command idempotency.

### 30.6 Session Lifecycle Tests

Verify:

- Client admission.
- Session authorization.
- Concurrent request limits.
- Response queue limits.
- Idle timeout.
- Unexpected disconnect.
- Session cleanup.
- Reconnection.
- Shutdown.

### 30.7 Authorization Tests

Verify that read-only clients cannot:

    ApplyConfiguration

    AddStream

    RemoveStream

    RestartConnection

    EnableRecording

    ReleaseOtherClientRetentionProtection

The server must not trust client-provided role names.

### 30.8 Configuration Transaction Tests

Verify:

    Valid Proposal
        -> Accepted Revision

    Invalid Proposal
        -> Rejected Without Mutation

    Persistence Failure
        -> Previous Revision Preserved

    Revision Conflict
        -> Rejected Without Overwrite

    Valid Revision With Runtime Failure
        -> Accepted But Not Converged

### 30.9 Reconciliation Tests

Verify:

- Independent streams reconcile concurrently.
- Conflicting mutations on one stream are serialized.
- Obsolete operation completions are rejected.
- Partial convergence is reported.
- Temporary failures are retried by the appropriate owner.
- Unaffected streams are not restarted.
- Persistent revisions survive client disconnection.

### 30.10 Source Command Tests

Verify:

- Registration.
- Duplicate acquisition deduplication.
- Activation.
- Deactivation.
- Removal.
- Dependency protection.
- Subscription-generation fencing.
- Trade readiness.
- L2 synchronization readiness.

### 30.11 Derived Command Tests

Verify:

- Valid derived definitions.
- Missing dependencies.
- Dependency cycles.
- Derived activation.
- Semantic generation replacement.
- Source reuse.
- Derived deactivation.
- Dependency release.

### 30.12 Recording Command Tests

Verify:

- Recording activation boundary.
- Recording disable boundary.
- Flush request behavior.
- Commit status distinction.
- Recording coverage queries.
- Recording gap reporting.
- Recording operation idempotency.

### 30.13 Retention Command Tests

Verify:

- Retention status queries.
- Cursor interval validation.
- Protection admission.
- Protection ownership.
- Protection release.
- Reservation expiration.
- Resource-limit enforcement.

### 30.14 Recovery Command Tests

Verify:

- Recovery status queries.
- Unsupported recovery rejection.
- Duplicate recovery request consolidation.
- Connection restart generation safety.
- L2 resynchronization routing.
- Trade-recovery capability validation.

### 30.15 Monitoring Query Tests

Verify:

- Metrics snapshot retrieval.
- Health snapshot retrieval.
- Missing measurement semantics.
- Snapshot freshness.
- Pagination.
- Response-size limits.
- Prometheus exporter status queries.

Monitoring queries must not modify authoritative runtime state.

### 30.16 Python Compatibility Tests

The Python client must successfully decode and produce the same protocol fixtures as the Rust client.

Tests must cover exact integers, decimal strings, optional fields, and version compatibility.

### 30.17 Ratatui Client Tests

Use a deterministic fake Control Server.

Verify:

- Initial connection.
- Snapshot retrieval.
- Request correlation.
- Operation progress.
- Revision conflicts.
- Control connection loss.
- Reconnection.
- Stale snapshot handling.

The tests must not require real exchange connections.

---

## 31. Fault Injection and Property-Based Testing

The control plane must remain correct under arbitrary valid sequences of concurrent requests, resource failures, and runtime state transitions.

### 31.1 Generated Operations

Property-based tests may generate:

    ConnectClient

    DisconnectClient

    SubmitReadRequest

    SubmitConfigurationUpdate

    RegisterSource

    ActivateSource

    DeactivateSource

    RemoveSource

    CreateDerived

    UpdateDerived

    RemoveDerived

    EnableRecording

    DisableRecording

    RequestRetentionProtection

    ReleaseRetentionProtection

    RequestRecovery

    ChangeConfigurationRevision

    CompleteOperation

    FailOperation

    RestartControlServer

    RestartRuntime

### 31.2 Revision Safety

For every generated operation sequence:

    Only One Authoritative
    Current Desired Revision
    May Exist.

Accepted revision identifiers must remain unique.

Conflicting proposals must not silently overwrite one another.

### 31.3 Operation Safety

An obsolete operation completion must not modify a newer resource generation.

An accepted operation must retain a discoverable outcome according to the configured operation-retention contract.

### 31.4 Source Deduplication

Repeated equivalent source requests must not create duplicate native acquisitions.

This invariant must hold under concurrent Python and Ratatui requests.

### 31.5 Resource Ownership

A client must not release resources belonging to another client without authorization.

A disconnected client must not leak unlimited session-scoped allocations.

### 31.6 Persistence Fault Injection

Inject failures during:

    Serialization

    Temporary File Creation

    File Write

    File Synchronization

    Atomic Rename

    Directory Synchronization

    Accepted Revision Publication

The system must recover the last valid committed revision or report an explicit unrecoverable persistence error.

### 31.7 Control Transport Failure

Inject:

    Partial Frame Delivery

    Mid-Request Disconnect

    Mid-Response Disconnect

    Slow Client Reads

    Oversized Requests

    Invalid Protocol Messages

    Concurrent Session Saturation

The failures must not corrupt authoritative runtime state.

### 31.8 Ambiguous Mutation Outcome

Simulate:

    Client Sends Mutation
              |
              v
    Server Accepts Operation
              |
              v
    Connection Lost Before Response

The client must not assume the operation failed.

It must reconcile against authoritative state or use the protocol's idempotency mechanism before retrying.

### 31.9 Monitoring Isolation

Generate high-rate monitoring queries while canonical processing continues.

Verify that monitoring clients cannot introduce unbounded blocking into the data plane.

### 31.10 Control-Plane Invariant

    Arbitrary Client Failures
    Must Not Corrupt
    Authoritative Runtime
    Configuration Or
    Canonical Processing.

---

## 32. Performance and Resource Requirements

The Control API is not a high-frequency market-data interface.

Its implementation should prioritize correctness, bounded resource use, and predictable latency over maximum request throughput.

### 32.1 Performance Principles

The control plane must:

- Avoid per-event participation in canonical processing.
- Use bounded client sessions.
- Use bounded request queues.
- Use bounded response buffers.
- Avoid expensive synchronous queries.
- Reuse monitoring snapshots.
- Minimize global locking.
- Preserve independent subsystem execution.
- Avoid unnecessary runtime resource restarts.

### 32.2 Query Performance

Common queries should use already available snapshots.

Examples:

    GetRuntimeStatus

    ListSourceStreams

    GetStreamHealth

    GetRuntimeMetrics

The control server should not reconstruct complete state from operational event history for every query.

### 32.3 Configuration Performance

Configuration compilation may perform relatively expensive validation.

However, it must execute outside the canonical event hot path.

A large configuration update must not block WebSocket readers or normalizers.

### 32.4 Response Size

Large collections must be bounded through:

    Pagination

    Filtering

    MaximumResultCount

    MaximumResponseBytes

The Control Server must reject requests that cannot satisfy resource limits safely.

### 32.5 Client Fairness

One client must not monopolize the control plane.

Per-session request concurrency and response-buffer limits must be enforced.

### 32.6 Monitoring Load

Repeated metrics queries should consume immutable snapshots.

The Control API must not force a full metrics aggregation cycle for every request.

### 32.7 Notification Load

Operational notifications use bounded queues.

Slow clients may lose intermediate notifications and must refresh authoritative snapshots.

The control plane must not block component state transitions indefinitely to deliver notifications.

### 32.8 Benchmark Requirements

Benchmark:

    Control Query Latency

    Concurrent Client Handling

    Configuration Validation Duration

    Configuration Diff Duration

    Snapshot Response Serialization

    Notification Delivery

    Control Server Memory Usage

    Canonical Processing Overhead

The final performance limits should be established through representative measurements.

### 32.9 Performance Invariant

    Management Activity
    Must Not Become
    A Material Bottleneck
    For Market-Data
    Acquisition Or Processing.

---

## 33. Final Acceptance Criteria

The Runtime Configuration and Control API subsystem is complete when the following requirements are satisfied.

### 33.1 Control Server

- A dedicated Unix control socket exists.
- The control protocol is versioned.
- Messages use explicit framing.
- Request sizes are bounded.
- Response sizes are bounded.
- Multiple clients are supported.
- Sessions have explicit lifecycles.
- Authorization is enforced.
- Client cleanup is deterministic.

### 33.2 Configuration Management

- Persistent desired state is authoritative.
- Configuration revisions are immutable.
- Expected-revision conflicts are detected.
- Configuration validation is side-effect free.
- Semantic configuration diffs are available.
- Accepted revisions are persisted atomically.
- Runtime convergence is tracked separately.
- Rollback creates a new revision.
- Restart recovery loads the last valid desired state.

### 33.3 Runtime Reconciliation

- Validated changes reach the appropriate component owners.
- Independent resources can reconcile concurrently.
- Conflicting mutations are serialized.
- Obsolete generations are fenced.
- Partial convergence is visible.
- Unaffected streams are preserved.
- Unsupported runtime changes are rejected.
- Restart-required settings are classified explicitly.

### 33.4 Source Stream Management

- Sources can be registered.
- Sources can be activated.
- Sources can be deactivated.
- Sources can be removed.
- Equivalent acquisitions are deduplicated.
- Instrument metadata is validated.
- Subscription readiness is respected.
- L2 synchronization readiness is respected.
- Dependencies prevent unsafe removal.

### 33.5 Derived Stream Management

- Derived streams can be created and removed.
- Existing source acquisitions are reused.
- Dependency cycles are rejected.
- Ordering policies are validated.
- Conversion policies are validated.
- Semantic changes establish generation boundaries.
- Derived lifecycle changes do not corrupt source streams.

### 33.6 Recording and Retention

- Recording can be enabled and disabled.
- Recording boundaries are explicit.
- Recording commits are distinguishable from admission.
- Recording gaps remain inspectable.
- Retention coverage can be queried.
- Retention reservations enforce ownership.
- Retention protections have bounded lifecycles.

### 33.7 Recovery and Monitoring

- Recovery operations can be inspected.
- Supported manual recovery requests use authoritative coordinators.
- Metrics snapshots can be queried.
- Health snapshots can be queried.
- Snapshot freshness is preserved.
- Optional Prometheus exporter status can be inspected.
- Monitoring queries do not modify runtime state.

### 33.8 Python Integration

- Python uses the shared control protocol.
- Instrument metadata authority remains with Python.
- Runtime execution authority remains with Rust.
- Configuration revisions are respected.
- Asynchronous operation progress is supported.
- Control reconnection is safe.
- Ambiguous mutation outcomes are reconciled.

### 33.9 Ratatui Integration

- Ratatui connects as an independent client.
- Closing Ratatui does not terminate the daemon.
- Runtime and stream snapshots are displayed.
- Supported mutations use validated control commands.
- Configuration drafts remain local until submitted.
- Revision conflicts are visible.
- Operation progress is visible.
- Control disconnections are handled safely.

### 33.10 Testing

- Framing tests pass.
- Serialization tests pass.
- Protocol compatibility tests pass.
- Authorization tests pass.
- Configuration transaction tests pass.
- Revision conflict tests pass.
- Reconciliation tests pass.
- Source and derived command tests pass.
- Recording and retention tests pass.
- Recovery command tests pass.
- Monitoring query tests pass.
- Python interoperability tests pass.
- Ratatui client tests pass.
- Fault-injection tests pass.
- Property-based tests pass.
- Performance benchmarks establish acceptable overhead.

---

## 34. Final Architectural Contract

The Runtime Configuration and Control API provides one authoritative external management interface for MarketForge Live.

It integrates configuration, stream management, operational commands, monitoring, and recovery inspection through a versioned local protocol.

The architecture is:

    Python Orchestration
            |
            |
       Ratatui TUI
            |
            |
    Authorized Applications
            |
            v
      Unix Control Socket
            |
            v
        Control Server
            |
      +-----+------------------------+
      |                              |
      v                              v
    Read Queries                 Mutating Commands
      |                              |
      v                              v
    Snapshot Providers          Configuration Manager
      |                              |
      |                              v
      |                       Runtime Reconciler
      |                              |
      |                              v
      |                       Component Owners
      |                              |
      +---------------+--------------+
                      |
                      v
               Control Responses

The diagram represents logical responsibilities, not a mandatory synchronous execution pipeline.

The subsystem guarantees:

- One consistent management protocol.
- Independent Python and Ratatui clients.
- Explicit configuration revision control.
- Atomic desired-state persistence.
- Safe runtime reconciliation.
- Asynchronous operation tracking.
- Generation-safe resource mutations.
- Structured monitoring access.
- Bounded client and query resources.
- Control-plane failure isolation.
- Explicit authorization boundaries.

It does not guarantee:

- Immediate runtime convergence after configuration acceptance.
- Successful exchange connectivity merely because a command was accepted.
- Complete historical coverage after current stream recovery.
- Atomic transactions across independent runtime subsystems.
- Unlimited operational history.
- Unbounded monitoring queries.
- Automatic reversal of already published events after configuration rollback.
- Application-level processing acknowledgement from control-socket delivery alone.

### 34.1 Principal Control Invariant

    Clients Express
    Desired Runtime Behavior.

    The Configuration Manager
    Validates And Persists
    That Intent.

    The Runtime Reconciler
    Coordinates Application.

    Authoritative Components
    Execute The Changes.

### 34.2 Runtime Safety Invariant

    Control-Plane Activity
    Must Not Corrupt,
    Reorder, Or Silently
    Discard Canonical
    Market-Data Events.

### 34.3 Independence Invariant

    MarketForge Live
    Must Operate Without
    Ratatui, Python,
    Prometheus, Or Grafana
    Processes Running.

The service may depend on previously provisioned configuration and instrument metadata, but not on a continuously running external management client.

### 34.4 Final Completion Rule

The Runtime Configuration and Control API is architecturally complete when:

    One Validated Command Contract
              |
              v
    One Authoritative Desired State
              |
              v
    One Coordinated Reconciliation Path
              |
              v
    Independently Owned Runtime Components
              |
              v
    Consistent Operational Observation

All management clients must obey this model.

---

**End of Part 5 — Python Client Integration, Ratatui Client Integration, Testing, Fault Injection, Performance, and Final Acceptance Criteria.**

**End of 13 — Runtime Configuration and Control API.**