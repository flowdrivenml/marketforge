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

A source stream is identified by:

    StreamId

`StreamId` is an opaque, stable identifier assigned when the stream is registered.

The registry additionally maintains a canonical subscription identity derived from:

    Exchange
    Market
    InstrumentId
    Channel
    ChannelParameters

Channel parameters may include:

- Order-book depth.
- Update frequency.
- Native exchange channel variant.
- Other parameters affecting the actual subscription.

Two requests resolving to the same canonical subscription identity must not accidentally create duplicate source streams.

### 3.2 Identity Separation

| Identifier | Scope | Stability |
|---|---|---|
| StreamId | Logical source stream | Stable across reconnects |
| SubscriptionKey | Canonical subscription identity | Stable while subscription parameters remain unchanged |
| ConnectionId | Physical transport connection | Changes after connection replacement |
| ConnectionGeneration | Physical connection incarnation | Changes on reconnection |
| SubscriptionGeneration | Subscription activation incarnation | Changes on reactivation |
| BookGeneration | Valid L2 synchronization incarnation | Changes on book reinitialization |

A reconnection must not create a new `StreamId`.

Changing a parameter that changes the native subscription identity requires a controlled subscription replacement.

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

| Field | Type | Purpose |
|---|---|---|
| stream_id | StreamId | Associated descriptor |
| desired_state | DesiredState | Requested operational state |
| lifecycle_state | StreamState | Observed lifecycle |
| connection_id | Option<ConnectionId> | Assigned physical connection |
| connection_generation | u64 | Current connection incarnation |
| subscription_generation | u64 | Current subscription incarnation |
| configuration_revision | u64 | Applied configuration revision |
| last_error | Option<StreamError> | Latest operational failure |
| registered_at | Timestamp | Registration time |
| activated_at | Option<Timestamp> | Last successful activation |
| state_changed_at | Timestamp | Latest lifecycle transition |

`StreamRuntime` contains control-plane state only.

Throughput, latency, message loss, and other operational measurements belong to the Metrics Registry.

Order-book synchronization state belongs to the stream processor.

### 4.3 DesiredState

The registry distinguishes user intent from actual runtime state.

Supported desired states:

    Active
    Inactive
    Removed

Semantics:

| Desired state | Meaning |
|---|---|
| Active | Stream should be running |
| Inactive | Stream remains registered but unsubscribed |
| Removed | Stream should be deactivated and deleted |

Desired state persists across temporary connection failures.

For example:

    desired_state = Active
    lifecycle_state = Reconnecting

A network failure must not silently change the user's requested state.

### 4.4 StreamState

Observed stream lifecycle:

    Registered
    Validating
    PendingAdmission
    Subscribing
    Synchronizing
    Active
    Degraded
    Reconnecting
    Unsubscribing
    Inactive
    Failed
    Removing

State semantics:

| State | Meaning |
|---|---|
| Registered | Descriptor accepted; activation not started |
| Validating | Configuration and dependencies being checked |
| PendingAdmission | Waiting for subscription or resource capacity |
| Subscribing | Subscription request issued; acknowledgement pending |
| Synchronizing | Subscription established; stream-specific initialization pending |
| Active | Stream operational and satisfying readiness requirements |
| Degraded | Stream operational with reduced integrity or service guarantees |
| Reconnecting | Transport recovery in progress |
| Unsubscribing | Subscription removal in progress |
| Inactive | Registered but not operational |
| Failed | Activation or runtime failure requiring intervention |
| Removing | Final resource release and registry removal |

`Synchronizing` is particularly important for L2 streams.

A successful WebSocket subscription acknowledgement does not imply that an order book is synchronized.

For trades, activation may complete immediately after the subscription is acknowledged and the processor is ready.

For L2, activation requires the configured snapshot and sequence initialization procedure.

---

## 5. Stream Lifecycle

### 5.1 Registration

Registration creates the logical stream without necessarily activating it.

    RegisterStream
          |
          v
    Validate Identity
          |
          v
    Resolve Configuration
          |
          v
    Check Duplicates
          |
          v
    Create Descriptor
          |
          v
       Registered

Registration must be idempotent with respect to the canonical subscription identity.

A duplicate registration request returns the existing stream identity or an explicit conflict when incompatible parameters are supplied.

### 5.2 Activation

Activation is asynchronous.

    Registered / Inactive
             |
             v
         Validating
             |
             v
      PendingAdmission
             |
             v
        Subscribing
             |
             v
       Synchronizing
             |
             v
           Active

The Subscription Manager performs admission and activation.

The Stream Registry records transitions but does not execute network operations directly.

Each activation receives a new `SubscriptionGeneration`.

All asynchronous completion events must identify the generation to which they belong.

### 5.3 Active State

A stream enters `Active` only when:

- The subscription is acknowledged or otherwise confirmed by the exchange protocol.
- The connection assignment is valid.
- The stream processor is initialized.
- Required instrument metadata is available.
- Channel-specific readiness conditions are satisfied.
- No blocking integrity failure exists.

For L2:

    Active => Book Synchronized

For trades:

    Active => Subscription Operational

`Active` does not guarantee historical completeness before the current activation.

Coverage gaps are tracked independently.

### 5.4 Deactivation

Deactivation preserves the stream descriptor.

    Active
       |
       v
    Unsubscribing
       |
       v
    Inactive

Required operations:

- Stop accepting new stream-specific work.
- Issue unsubscribe when supported and necessary.
- Fence the previous subscription generation.
- Release subscription resources.
- Terminate or suspend the stream processor.
- Preserve final operational diagnostics.

The physical WebSocket connection remains available if other subscriptions depend on it.

### 5.5 Removal

Removal permanently deletes the logical registration.

    Any Registered State
             |
             v
          Removing
             |
             v
      Release Resources
             |
             v
       Remove Registry Entry

Removal must be idempotent.

A removed stream cannot be resurrected by delayed subscription acknowledgements, connection events, or recovery completions.

Removal must coordinate with the dependency manager when derived streams reference the source.

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

Failures are classified by recovery semantics.

| Failure | Registry behavior |
|---|---|
| Temporary network failure | Reconnecting |
| Exchange outage | Reconnecting with backoff |
| Subscription rate limit | PendingAdmission with cooldown |
| Invalid subscription parameters | Failed |
| Unsupported channel | Failed |
| Missing instrument metadata | Failed |
| Authentication failure | Failed until corrected |
| L2 synchronization failure | Synchronizing or Degraded during recovery |
| Repeated recovery failure | Escalate according to retry policy |
| Explicit user deactivation | Inactive |

A transient failure must not destroy the stream descriptor.

A permanent configuration failure must not trigger unlimited retries.

### 6.4 Desired-State Reconciliation

The registry continuously reconciles desired and observed state.

Conceptually:

    desired_state != observed_state
                  |
                  v
          Reconciliation Action
                  |
                  v
           State Transition

Examples:

| Desired | Observed | Action |
|---|---|---|
| Active | Registered | Activate |
| Active | Inactive | Activate |
| Active | Reconnecting | Continue recovery |
| Inactive | Active | Deactivate |
| Inactive | Failed | Retain registration |
| Removed | Active | Deactivate and remove |
| Removed | Failed | Release and remove |

Reconciliation must be event-driven where possible.

Periodic reconciliation may serve as a safety mechanism for missed control-plane events.

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
    Stream Active

The processor must be prepared to receive messages before the subscription can produce unbuffered market data.

Incoming data may arrive before the subscription acknowledgement.

The system must therefore establish routing and buffering before sending the native subscription request.

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

The Stream Registry must not transition an L2 stream to `Active` merely because its subscription was acknowledged.

The Stream Processor or Recovery Coordinator reports synchronization readiness through a typed lifecycle event.

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

    Connection Lost
          |
          v
    Connection Manager
          |
          v
    Increment Connection Generation
          |
          v
    Notify Subscription Manager
          |
          v
    Invalidate Native Assignments
          |
          v
    Notify Stream Registry
          |
          v
    Streams Enter Reconnecting

Logical `StreamId` values remain unchanged.

Pending operations associated with the failed connection generation become invalid.

### 14.2 Restoration Sequence

    New Connection Established
              |
              v
    Resolve Desired Active Streams
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
    Await Acknowledgements
              |
              v
    Reinitialize Stream Processors
              |
              v
    Restore Channel Readiness
              |
              v
            Active

Restoration must respect current desired state.

A stream deactivated while its connection was unavailable must not be resubscribed.

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

The Stream Registry updates the corresponding logical streams independently.

Recovery must avoid launching redundant physical reconnections for every affected stream.

---
## 15. Runtime Stream Mutations

MarketForge Live supports dynamic stream management without restarting the runtime.

Supported operations:

| Operation | Semantics |
|---|---|
| Register | Create a logical stream descriptor |
| Activate | Establish the native subscription |
| Deactivate | Stop the subscription while retaining registration |
| Remove | Release resources and delete the registration |
| Replace | Change subscription-defining parameters |
| Reconfigure | Apply compatible runtime configuration changes |
| Restart | Reinitialize a stream while preserving its identity |

All mutations are coordinated through the Stream Registry.

The TUI, API, and Python orchestration layer must use the same command interface.

### 15.1 Stream Commands

The registry accepts typed commands:

    RegisterStream
    ActivateStream
    DeactivateStream
    RemoveStream
    ReplaceSubscription
    ReconfigureStream
    RestartStream

Each command contains:

| Field | Purpose |
|---|---|
| command_id | Unique command identifier |
| stream_id | Target stream, when applicable |
| expected_revision | Optional optimistic concurrency constraint |
| parameters | Operation-specific arguments |
| requested_at | Command creation timestamp |
| origin | TUI, API, orchestration, or internal recovery |

Commands are asynchronous.

A successful command submission does not imply successful completion.

### 15.2 Command Results

Commands produce:

    Accepted
    Rejected
    Conflict
    Completed
    Failed
    Cancelled

`Accepted` means the operation has entered the control plane.

`Completed` means its required lifecycle transition has finished.

The API and TUI must distinguish these outcomes.

### 15.3 Idempotency

Repeated commands must not create duplicate subscriptions or inconsistent registry state.

The registry maintains command identity and operation status for a bounded retention period.

For example:

    Activate(StreamId=15)
    Activate(StreamId=15)

If activation is already underway, the second request must not initiate another independent native subscription.

It may return the existing operation or the current desired-state result.

### 15.4 Concurrent Mutations

Conflicting operations on the same stream are serialized.

Example:

    Activate
       |
       v
    Deactivate
       |
       v
    Remove

The latest accepted desired state determines the reconciliation target.

Earlier asynchronous operations remain subject to generation fencing.

Independent streams may be modified concurrently.

---

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

A replacement creates a new subscription incarnation.

    Existing Subscription
             |
             v
    Validate Replacement
             |
             v
    Reserve Required Resources
             |
             v
    Establish New Subscription
             |
             v
    Initialize New Processor
             |
             v
    Verify Readiness
             |
             v
    Switch Active Generation
             |
             v
    Release Old Subscription

This is the preferred replacement procedure when the exchange and available resources permit overlapping subscriptions.

The logical `StreamId` may remain stable only when the replacement preserves the stream's declared identity contract.

Changes to instrument, exchange, market, or channel identity require registration of a new logical stream.

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

The registry must never expose two subscription generations as simultaneously authoritative for the same logical stream.

During replacement:

- Old and new generations may temporarily coexist internally.
- Only one generation is designated authoritative.
- Events retain their source-generation provenance.
- Duplicate native events must not be silently published as independent executions.
- L2 book generations remain isolated.
- Failure to initialize the replacement must not corrupt the existing valid stream.

The authoritative generation changes only after the replacement satisfies its readiness requirements.

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

Configuration changes are classified before application.

| Classification | Semantics |
|---|---|
| Immediate | Apply without subscription replacement |
| Deferred | Apply at a defined processing boundary |
| StreamRestart | Reinitialize the affected stream |
| SubscriptionReplacement | Establish a new native subscription incarnation |
| RuntimeRestart | Requires restarting the Live runtime |
| Rejected | Invalid or unsupported change |

The Configuration System determines change compatibility.

The Stream Registry coordinates application to affected streams.

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

    New Configuration Revision
              |
              v
       Validate Configuration
              |
              v
        Compute Change Set
              |
              v
       Identify Affected Streams
              |
              v
       Classify Required Actions
              |
              v
       Apply Changes
              |
              v
       Confirm Applied Revision

A revision is considered applied to a stream only after its required transition succeeds.

The system must distinguish:

    Requested Revision
    Applied Revision

A failed update must not cause the registry to report a revision that the stream has not actually adopted.

### 17.5 Configuration Failure

If a new configuration is invalid:

- Reject the revision before applying it.
- Preserve the last valid configuration.
- Report the validation failure.
- Do not interrupt unaffected streams.

If a valid revision fails during application, the registry must report partial application explicitly.

Automatic rollback is permitted only when the previous configuration remains available and rollback semantics are defined.

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

The Stream Registry maintains authoritative runtime state in memory.

Persistent configuration and desired-state declarations are managed through the existing Configuration System and Python orchestration layer.

Operational logs remain local. PostgreSQL is reserved for optional market-data recording.

### 19.1 Persistence Boundaries

| Data | Storage |
|---|---|
| Stream definitions | Persistent configuration |
| Desired activation state | Persistent configuration |
| Exchange protocol specifications | TOML |
| Instrument metadata | Python-managed instrument catalog |
| Runtime lifecycle state | Memory |
| Connection assignments | Memory |
| Subscription generations | Memory |
| Pending operations | Memory |
| Operational logs | Local files |
| Metrics | Memory |
| Market-data recording | PostgreSQL, optional |

Transient runtime state must not be restored as though it remained valid after process termination.

In particular:

- WebSocket connections cannot survive process restart.
- Native subscription acknowledgements become invalid.
- Pending subscription operations are abandoned.
- L2 synchronization state must be reconstructed.
- Connection and subscription generations must be reestablished.

### 19.2 Runtime Startup

Startup reconstructs the registry from validated persistent declarations.

    Load Configuration
           |
           v
    Validate Configuration
           |
           v
    Resolve Instrument Metadata
           |
           v
    Reconstruct Stream Descriptors
           |
           v
    Initialize Registry
           |
           v
    Reconcile Desired States
           |
           v
    Activate Required Streams

Previously active streams must not immediately enter `Active`.

They follow the normal subscription and readiness procedures.

### 19.3 Restart Safety

After an unexpected shutdown:

- Restore declared stream identities.
- Restore desired activation states.
- Allocate fresh runtime generations.
- Reestablish physical connections.
- Restore subscriptions.
- Reinitialize stream processors.
- Reconstruct L2 books.
- Evaluate trade coverage gaps.
- Resume recording according to its independent recovery policy.

Stable `StreamId` values require persistent identity mappings.

The registry must not generate different logical identities for unchanged persistent stream declarations on every restart.

### 19.4 Runtime State Snapshots

Periodic operational snapshots may be written locally for diagnostics.

They may contain:

    StreamId
    SubscriptionKey
    DesiredState
    LifecycleState
    ConfigurationRevision
    LastError
    StateChangedAt

These snapshots are observational artifacts.

They must not be treated as authoritative restoration of physical connections, native subscriptions, or synchronized books.

---

## 20. Registry Events

The registry emits structured lifecycle events for observability and dependent components.

Events describe completed state changes or meaningful operational conditions.

### 20.1 Event Types

    StreamRegistered
    StreamActivationRequested
    StreamAdmissionDeferred
    StreamSubscribing
    StreamSubscriptionConfirmed
    StreamSynchronizing
    StreamActivated
    StreamDegraded
    StreamReconnecting
    StreamRecovered
    StreamDeactivated
    StreamConfigurationChanged
    StreamFailed
    StreamRemoved

Each event includes:

| Field | Purpose |
|---|---|
| event_id | Unique event identity |
| stream_id | Associated logical stream |
| event_type | Lifecycle event classification |
| previous_state | Previous lifecycle state |
| current_state | New lifecycle state |
| configuration_revision | Applied configuration revision |
| subscription_generation | Associated subscription incarnation |
| timestamp | Event occurrence time |
| reason | Optional transition reason |

Events must be emitted after authoritative state mutation.

Consumers must not be able to modify registry state directly.

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

| Metric | Description |
|---|---|
| registered_streams | Total registered source streams |
| active_streams | Streams currently operational |
| inactive_streams | Registered but inactive streams |
| failed_streams | Streams in failed state |
| reconnecting_streams | Streams recovering connectivity |
| synchronizing_streams | Streams establishing readiness |
| pending_admissions | Subscriptions awaiting capacity |
| pending_operations | Outstanding subscription operations |
| activation_failures_total | Failed activations |
| subscription_timeouts_total | Subscription acknowledgement timeouts |
| configuration_conflicts_total | Rejected concurrent revisions |
| lifecycle_transitions_total | Completed state transitions |

Counters must be monotonic within one runtime process.

Gauges represent current state.

### 21.2 Per-Stream Diagnostics

The registry exposes:

    StreamId
    Exchange
    Market
    Instrument
    Channel
    DesiredState
    LifecycleState
    ConnectionId
    ConnectionGeneration
    SubscriptionGeneration
    ConfigurationRevision
    LastError
    LastTransitionTime

Additional operational measurements are retrieved from their respective owners.

Examples:

- Throughput from Metrics Registry.
- Latency from Metrics Registry.
- Message integrity from Stream Processor.
- L2 synchronization health from BookStore/Recovery Coordinator.
- Recording status from Recording Manager.

The registry should not duplicate these measurements.

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

### 21.4 API Integration

The runtime API exposes the same control-plane operations as the TUI.

Conceptual endpoints:

    GET    /streams
    GET    /streams/{stream_id}

    POST   /streams
    POST   /streams/{stream_id}/activate
    POST   /streams/{stream_id}/deactivate
    POST   /streams/{stream_id}/restart

    PATCH  /streams/{stream_id}
    DELETE /streams/{stream_id}

    GET    /streams/{stream_id}/operations
    GET    /streams/events

The exact HTTP contract is defined separately.

Long-running operations return an operation identifier rather than blocking until exchange synchronization completes.

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

    Active L2 streams require valid synchronization.

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
