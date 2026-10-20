# 12 — Metrics and Stream Health

## 1. Purpose

The Metrics and Stream Health subsystem provides a unified view of MarketForge Live's operational state.

It collects measurements from exchange connectivity, canonical processing, stream distribution, retention, merging, currency conversion, PostgreSQL recording, and recovery.

It also evaluates the operational health of individual streams and shared runtime components.

The subsystem must support:

- Internal diagnostics.
- Real-time stream health inspection.
- Performance monitoring.
- Failure detection visibility.
- Recovery progress visibility.
- Runtime resource monitoring.
- A lightweight control API.
- The Rust Ratatui TUI.

The subsystem must remain lightweight and must not interfere with market-data acquisition.

### 1.1 Primary Objectives

The subsystem must answer questions such as:

- Which streams are currently active?
- Which streams are receiving events?
- What is the current event throughput?
- How much processing latency is being introduced?
- Have trades been missed?
- Is the L2 order book synchronized?
- Are subscribers keeping up?
- Is the Merge Engine falling behind?
- Are currency-conversion references available?
- Is PostgreSQL recording current?
- Are protected retention buffers approaching capacity?
- Which components are recovering?
- Which failures require intervention?

These questions must be answerable without inspecting individual application logs.

### 1.2 Architectural Principles

The monitoring architecture follows these principles:

1. Metrics describe measurements.
2. Health describes interpreted operational state.
3. Processing components remain authoritative for their own integrity.
4. The Metrics Registry does not implement market-data processing.
5. The HealthEvaluator does not independently reconstruct source continuity.
6. Monitoring overhead must be bounded.
7. Metrics are available without external monitoring infrastructure.
8. The TUI and runtime API consume shared monitoring snapshots.
9. Monitoring remains operational when individual streams fail.
10. Optional disabled components must not be classified as failed.

### 1.3 Scope

The subsystem covers:

    Runtime

    Exchange Connections

    Rate Limiting

    Native Subscriptions

    Trade Processing

    L2 BookStore

    Canonical Streams

    Publisher

    Unix Socket Subscribers

    Live Retention

    Parquet Spill

    Combined Streams

    Currency Conversion

    PostgreSQL Recording

    Recovery Coordination

    Configuration State

Metrics may be collected globally or for individual components and streams.

### 1.4 Non-Responsibilities

The Metrics and Stream Health subsystem must not:

- Establish exchange WebSocket connections.
- Detect native sequence gaps independently.
- Reconcile missing executions.
- Reconstruct L2 order books.
- Execute market-data recovery.
- Store canonical market events.
- Manage PostgreSQL recording transactions.
- Publish canonical market data to subscribers.
- Replace structured operational logging.
- Store unlimited metric history.
- Require an external monitoring stack.
- Operate or supervise Prometheus or Grafana.
- Introduce a mandatory operational database.

The subsystem may optionally expose its existing measurements
through a Prometheus-compatible exporter.

External monitoring must remain a replaceable integration,
not a prerequisite for MarketForge Live operation.

### 1.5 Metrics vs Health

A metric represents a measurement.

Example:

    Stream:
        Binance BTCUSDT Trades

    MessagesPerSecond:
        15000

    ProcessingLatency:
        0.8 ms

    SequenceGaps:
        2

A health state represents the interpretation of relevant measurements and authoritative component states.

Example:

    Transport:
        Healthy

    TradeContinuity:
        Degraded

    Publication:
        Healthy

    Recording:
        Healthy

    Overall:
        Degraded

High throughput does not imply healthy market-data coverage.

A disconnected stream may also be intentionally disabled rather than failed.

Metrics and health must remain distinct concepts.

---

## 2. Architectural Boundaries

The Metrics Registry collects operational measurements.

The HealthEvaluator derives health assessments from metric snapshots and authoritative component state.

The runtime API and TUI expose these results.

### 2.1 Logical Architecture

    Exchange Connectivity
             |
    Canonical Processing
             |
    Stream Registry
             |
    Publisher
             |
    Retention
             |
    Merge Engine
             |
    Currency Conversion
             |
    PostgreSQL Recording
             |
    Recovery Coordinator
             |
             v
       Metrics Registry
             |
             v
       Metrics Snapshot
             |
             +------------------+
             |                  |
             v                  v
       HealthEvaluator     Runtime API
             |                  ^
             v                  |
       Health Snapshot          |
             |                  |
             +---------+--------+
                       |
                       v
                  Ratatui TUI

The diagram represents logical data dependencies.

Components do not need to synchronously report metrics to one central task for every event.

Hot-path metrics may be updated through local counters or other bounded low-overhead mechanisms.

### 2.2 Component Ownership

| Component | Owns |
|---|---|
| Connection Manager | Connection lifecycle and transport state |
| Rate Limiter | Request weights, cooldowns, and rate-limit state |
| Subscription Manager | Native subscription lifecycle |
| Trade Processor | Trade continuity and reconciliation state |
| BookStore | L2 state and synchronization |
| Stream Registry | Logical stream identity and desired state |
| Publisher | Publication admission and subscriber delivery |
| Retention Manager | Retention coverage and resource state |
| Merge Engine | Watermarks, ordering, and late-event classification |
| Currency Conversion Engine | Reference availability and conversion results |
| Recording Manager | Database recording and committed coverage |
| Recovery Coordinator | Recovery lifecycle and escalation |
| Metrics Registry | Metric storage and snapshot generation |
| HealthEvaluator | Health assessment and dependency interpretation |
| Runtime API | Monitoring and control interface |
| Ratatui TUI | User-facing presentation |

### 2.3 Authoritative Operational State

Monitoring must consume authoritative state from the component that owns it.

For example:

    Trade Processor
          |
          v
    Missing Execution Detected
          |
          +-- Update Integrity State
          |
          +-- Increment Metric
          |
          +-- Report Recovery Requirement

The HealthEvaluator reads the resulting integrity state.

It must not attempt to determine native execution continuity by interpreting trade throughput alone.

### 2.4 Metrics Are Not Recovery Commands

Metrics may indicate that recovery is underway or failing.

They do not directly initiate recovery actions.

The authoritative recovery workflow remains defined in:

    07 — Recovery Coordination and System Resilience

The HealthEvaluator may classify recovery as degraded or failed.

The Recovery Coordinator decides and executes recovery actions.

### 2.5 No Monitoring Feedback Loop

The monitoring subsystem must not create uncontrolled operational feedback loops.

For example:

    High Latency Detected
             |
             v
       Health Degraded

This must not automatically cause repeated reconnections unless an authoritative component explicitly defines that behavior.

Metrics are evidence, not an independent control authority.

### 2.6 Monitoring Failure Isolation

A metric collection failure must not ordinarily stop canonical market-data acquisition.

For example:

    Metrics Snapshot Failed

must not imply:

    Stop All WebSocket Connections

Monitoring failures must be reported separately.

If a specific operational guarantee requires a functioning diagnostic mechanism, that dependency must be explicit.

---

## 3. Metric Types and Semantics

The Metrics Registry supports a small set of well-defined metric types.

The initial implementation should avoid unnecessary general-purpose telemetry abstractions.

### 3.1 Metric Categories

Recommended metric types:

    Counter

    Gauge

    Duration

    BoundedDistribution

    State

These provide sufficient coverage for the initial Live architecture.

### 3.2 Counters

Counters represent cumulative occurrences.

Examples:

    MessagesReceivedTotal

    TradesProcessedTotal

    SequenceGapsTotal

    ReconnectAttemptsTotal

    RecordingTransactionsTotal

Counters should advance monotonically within their defined metric identity and lifecycle.

A counter reset must be distinguishable from an actual decrease in activity.

### 3.3 Gauges

Gauges represent current values.

Examples:

    ActiveConnections

    ActiveStreams

    PendingEvents

    MergeBufferBytes

    RecordingQueueBytes

    ConnectedSubscribers

Gauges may increase or decrease.

### 3.4 Durations

Durations represent measured elapsed time.

Examples:

    CanonicalProcessingDuration

    PublicationAdmissionDuration

    DatabaseCommitDuration

    RecoveryDuration

Local processing durations should use a monotonic clock.

Wall-clock timestamps must not be subtracted blindly to calculate local execution latency.

### 3.5 Bounded Distributions

A bounded distribution summarizes a collection of observations.

Examples:

    ProcessingLatencyDistribution

    PublisherWriteLatencyDistribution

    BatchCommitLatencyDistribution

The implementation may use:

- Fixed bucket histograms.
- Bounded ring-based samples.
- Lightweight accumulation with count and extrema.

The initial choice should be based on measured overhead and required UI functionality.

### 3.6 State Metrics

Some operational values represent categorical states rather than numerical measurements.

Examples:

    ConnectionState

    BookSynchronizationState

    RecordingState

    RecoveryState

These should retain explicit typed representations.

They must not be reduced to arbitrary numeric gauges without a defined mapping.

### 3.7 Metric Naming

Metric names should be consistent across components.

Conceptually:

    <subsystem>_<measurement>_<unit_or_type>

Examples:

    connection_messages_received_total

    trade_events_processed_total

    l2_sequence_gaps_total

    publisher_queue_bytes

    recording_commit_latency_ns

The specific naming convention should be centralized.

### 3.8 Units

Metrics must declare their units.

Preferred internal units include:

    Time:
        Nanoseconds

    Memory:
        Bytes

    Event Counts:
        Unsigned Integers

    Event Rates:
        Events Per Second

    Ratios:
        Explicit Dimensionless Values

The TUI may display human-readable milliseconds, MiB, or events per second.

The Metrics Registry should not store values as formatted strings.

### 3.9 Counter Overflow

High-frequency counters may grow substantially during long-running operation.

The implementation should use unsigned 64-bit counters where appropriate.

Counter overflow must not silently wrap into misleading values.

Possible policies include:

    Checked Increment

    Saturating Increment

    Explicit Counter Epoch Reset

The selected policy must be consistent and documented.

### 3.10 Metric Reset Semantics

Metrics may have different lifetimes:

    ProcessLifetime

    RuntimeGeneration

    ConnectionGeneration

    StreamGeneration

    SubscriberSession

A reconnect must not accidentally reset process-lifetime totals.

A new connection generation may legitimately start new generation-scoped counters.

### 3.11 Rates Are Derived Measurements

A rate such as:

    EventsPerSecond

should generally be calculated from counter changes over a measured interval.

Conceptually:

    EventRate =
        DeltaEventCount
        /
        DeltaElapsedSeconds

A rate must not be derived from the difference between unrelated counter generations.

### 3.12 Missing Measurements

Some metrics may not be available.

For example:

    ExchangeEventTimestamp:
        Not Provided

Therefore:

    ExchangeToReceiveLatency:
        Unavailable

The system must distinguish:

    Unavailable

from:

    Zero

A missing latency measurement must not be presented as zero latency.

---

## 4. Metrics Registry

The Metrics Registry provides a shared interface for recording and retrieving operational measurements.

It is not a general-purpose time-series database.

### 4.1 Responsibilities

The Metrics Registry must:

- Register metric scopes.
- Maintain counters and gauges.
- Store bounded latency summaries.
- Track metric lifecycle generations.
- Support snapshot creation.
- Support global aggregation.
- Expose per-stream measurements.
- Expose component-level measurements.
- Enforce memory limits.
- Handle removed streams safely.
- Minimize hot-path overhead.

### 4.2 Conceptual Representation

    MetricsRegistry {
        runtime_metrics

        connection_metrics
        subscription_metrics

        stream_metrics
        subscriber_metrics

        retention_metrics
        merge_metrics
        conversion_metrics

        recording_metrics
        recovery_metrics

        snapshot_configuration
        resource_budget
    }

This is a logical representation.

The actual implementation may use typed metric structures distributed among runtime components.

### 4.3 Typed Metric Structures

The preferred architecture uses typed metric structures rather than a dynamic metric dictionary for every hot-path operation.

For example:

```rust
struct TradeMetrics {
    events_received: AtomicU64,
    events_accepted: AtomicU64,
    duplicates_detected: AtomicU64,
    continuity_failures: AtomicU64,
    recovery_attempts: AtomicU64,
}
```

The exact structure is illustrative.

Typed fields improve:

- Compile-time validation.
- Predictable allocation.
- Hot-path performance.
- Consistent semantics.
- Reduced metric-name lookup overhead.

### 4.4 Atomic Counters

Frequently updated counters should use atomic operations where concurrent readers or writers require them.

For counters used only within one task, ordinary local accumulation may be preferable.

The implementation must not use atomic operations indiscriminately.

### 4.5 Memory Ordering

Relaxed atomic ordering may be sufficient for independent statistical counters.

However, atomics used to establish correctness or state-publication guarantees require appropriate synchronization.

The Metrics Registry must not use relaxed counters as a substitute for authoritative lifecycle synchronization.

### 4.6 Metric Registration

Metrics should be registered when their owning logical component becomes active.

Examples:

    Connection Created
        -> Connection Metrics Registered

    Stream Activated
        -> Stream Metrics Registered

    Subscriber Connected
        -> Session Metrics Registered

The registration process must remain bounded.

### 4.7 Metric Deregistration

When a stream or connection is removed:

- Stop accepting obsolete metric updates.
- Preserve required final snapshots.
- Release metric storage.
- Prevent stale-generation updates.
- Update global aggregations.

Historical metric snapshots may remain available for a short bounded diagnostic interval.

### 4.8 Stale Generation Protection

A metric update from an obsolete runtime generation must not overwrite the current generation's state.

For example:

    Stream Generation 5:
        Stopped

    Stream Generation 6:
        Active

A delayed task from generation 5 must not mark generation 6 as disconnected.

Metric generation identities must remain explicit.

### 4.9 Snapshot Frequency

Snapshot generation should run periodically.

Conceptually:

    MetricsSnapshotInterval

The interval is configurable.

The existing configuration design allows selected snapshot settings to be updated while the runtime is running.

Changes should apply at the next snapshot cycle.

### 4.10 Snapshot Collection

A snapshot process may read counters and gauges independently.

The resulting collection may not represent one globally atomic instant.

This is acceptable for ordinary diagnostics.

However, the snapshot must not falsely claim transactional consistency across unrelated components.

### 4.11 Consistency Requirements

For diagnostic measurements:

    Eventually Consistent Snapshot

is generally sufficient.

For authoritative state transitions:

    Component-Owned Synchronized State

must be used.

Examples include:

- A source book's synchronized status.
- The Recording Manager's committed cursor.
- The Merge Engine's finalized output frontier.

The Metrics Registry must not reconstruct these values by combining loosely sampled counters.

### 4.12 Global Aggregation

Global metrics may aggregate compatible stream measurements.

Examples:

    TotalAcceptedTrades

    TotalActiveConnections

    TotalBufferedEvents

However, aggregation must respect metric scope.

For example:

    SourceEventsPublished

and:

    DerivedEventsPublished

must not be combined and presented as unique native market events.

One canonical source event may appear in several derived streams.

### 4.13 Cardinality Limits

Metric identities must remain bounded.

The initial Metrics Registry should avoid arbitrary dynamic labels such as:

    NativeTradeId

    IndividualOrderId

    UniqueErrorMessage

    ArbitrarySymbolText

Metric cardinality should be controlled by registered components and stream identities.

### 4.15 Metrics Export Interfaces

The Metrics Registry exposes one authoritative collection
of operational measurements.

Multiple consumers may inspect these measurements:

    Metrics Registry
          |
          +-- HealthEvaluator
          |
          +-- Runtime Monitoring API
          |
          +-- Ratatui TUI
          |
          +-- Prometheus Exporter (optional)

Export interfaces must not maintain independent versions
of the underlying counters or gauges.

A metric retains its identity, meaning, and lifecycle
regardless of which consumer reads it.

Exporters may transform metric names, units, and labels
to satisfy their destination format.

These transformations must not change the meaning
of the underlying measurement.

Export failures must not affect the authoritative
Metrics Registry.

### 4.16 Optional Prometheus Exporter

MarketForge Live supports a Prometheus-compatible
read-only metrics exposition endpoint.

The exporter is disabled by default.

#### Architecture

    Rust Runtime Components
              |
              v
        Metrics Registry
              |
              v
       Prometheus Exporter
              |
              v
          HTTP /metrics
              |
              v
           Prometheus
              |
              v
             Grafana

Prometheus periodically scrapes the endpoint.

Grafana may query Prometheus to visualize historical
metrics and construct dashboards.

MarketForge does not manage either application.

#### Data Source

The exporter reads existing Metrics Registry measurements.

It must not:

- Increment market-data counters independently.
- Recalculate native continuity.
- Reconstruct book synchronization.
- Create independent health assessments.
- Modify stream lifecycle state.
- Reconfigure runtime components.

#### Supported Metric Types

The exporter supports:

    Counters
    Gauges
    Histograms

Durations must be exported in seconds.

Memory measurements must be exported in bytes.

Counters must follow Prometheus cumulative-counter
semantics.

Histogram bucket boundaries must be stable and
compatible across observations of the same metric.

#### Metric Naming

Exported metrics use the MarketForge namespace.

Examples:

    marketforge_trade_accepted_total

    marketforge_l2_sequence_gaps_total

    marketforge_connection_reconnects_total

    marketforge_publisher_queue_bytes

    marketforge_recording_pending_events

    marketforge_recording_commit_duration_seconds

    marketforge_merge_late_events_total

    marketforge_conversion_unavailable_total

Existing internal metric names do not need to change.

The exporter owns the translation into Prometheus
naming conventions.

#### Metric Labels

Permitted bounded labels may include:

    exchange
    market_family
    channel
    stream_id
    component
    failure_category

The exporter must never generate labels using:

    trade_id
    order_id
    event_cursor
    request_id
    arbitrary_error_message
    recovery_operation_id

Individual connection and subscriber identities
require explicit cardinality budgets and lifecycle
cleanup before being exposed.

#### Stream Cardinality

The exporter must respect the Metrics Registry's
registered metric identities.

When streams are removed, their associated export
series must eventually disappear.

The exporter must not retain unbounded historical
series for inactive streams.

#### Counter Resets

Process restarts or metric-generation changes may
reset exported counters.

The exporter must preserve valid cumulative-counter
semantics within each exposed series.

It must not artificially merge unrelated stream
generations into one numerical counter.

#### Missing Measurements

Unavailable measurements must not be exported
as fabricated zero values.

Depending on the metric contract, the exporter may:

- Omit an unavailable measurement.
- Expose a separate availability indicator.
- Export a documented categorical state gauge.

The chosen representation must remain consistent.

#### Health State Export

The HealthEvaluator remains authoritative for
health assessments.

Categorical health may be exported using a documented
numeric mapping or bounded state-indicator gauges.

The mapping must distinguish:

    Healthy
    Degraded
    Unhealthy
    Unknown
    Disabled

A missing health assessment must not be represented
as Healthy.

#### Scrape Execution

The exporter must serve requests from bounded,
read-only metric snapshots.

Scraping must not require:

- Locking the complete Stream Registry.
- Traversing full order books.
- Reading canonical market-data buffers.
- Querying PostgreSQL.
- Waiting for exchange responses.
- Blocking market-data acquisition.

The exporter may cache its serialized output for a
configurable interval.

#### Scrape Limits

The exporter must enforce:

    MaximumConcurrentRequests

    MaximumResponseBytes

    RequestTimeout

    ExportRefreshInterval

Resource exhaustion must produce a bounded HTTP
failure rather than unbounded work.

#### Security

The endpoint binds to loopback by default.

It exposes operational information and must not be
considered suitable for unrestricted public access.

Remote exposure requires explicit network security
and access-control configuration.

The endpoint is read-only and must not expose
runtime mutation commands.

#### Exporter Failure

If the Prometheus exporter fails:

    Prometheus Export = Unavailable

    Internal Metrics = Operational

    HealthEvaluator = Operational

    Ratatui = Operational

    Canonical Processing = Unaffected

Exporter errors are reported through bounded local
diagnostics.

#### Exporter Invariant

    Prometheus Export
    Must Never Become
    An Authority For
    Market-Data Correctness.

---

## 5. Metric Identity and Scope

Metrics must be associated with the component or stream that produced them.

A shared metric namespace is necessary to avoid ambiguous dashboard results.

### 5.1 Metric Scopes

Supported scopes include:

    Runtime

    Exchange

    Connection

    NativeSubscription

    CanonicalSourceStream

    DerivedStream

    SubscriberSession

    RecordingStream

    RecoveryOperation

Not every metric applies to every scope.

### 5.2 Runtime Scope

Runtime metrics describe the entire MarketForge Live process.

Examples:

    ProcessUptime

    ActiveConnections

    ActiveSourceStreams

    ActiveDerivedStreams

    TotalResidentMemory

### 5.3 Exchange Scope

Exchange metrics describe shared infrastructure for a specific exchange.

Examples:

    Binance REST Weight Usage

    Binance Active WebSockets

    Binance Connection Failures

The exchange scope may require subdivision by API pool or market family.

### 5.4 Connection Scope

Connection metrics describe one active transport instance.

Conceptually:

    ConnectionMetricIdentity {
        connection_id
        connection_generation

        exchange
        api_pool
    }

The connection generation distinguishes reconnects.

### 5.5 Native Subscription Scope

One exchange WebSocket connection may host several native subscriptions.

Subscription metrics must remain independently accessible.

Conceptually:

    NativeSubscriptionMetricIdentity {
        subscription_id
        subscription_generation

        connection_id
    }

### 5.6 Canonical Stream Scope

Canonical source-stream metrics use the authoritative Stream Registry identity.

Conceptually:

    SourceStreamMetricIdentity {
        stream_id

        stream_generation
        cursor_epoch
    }

The metrics must preserve stream identity across temporary transport reconnections.

### 5.7 Derived Stream Scope

Derived streams have independent metric identities.

Conceptually:

    DerivedStreamMetricIdentity {
        derived_stream_id

        runtime_generation
        transformation_revision
    }

Derived metrics must not be confused with the source metrics of their dependencies.

### 5.8 Subscriber Scope

Unix socket subscriber metrics are associated with a specific client session.

Conceptually:

    SubscriberMetricIdentity {
        subscriber_id
        session_generation
    }

Subscriber identities must not be reused in a way that allows stale session metrics to overwrite active sessions.

### 5.9 Recording Scope

Recording metrics may be associated with:

    RecordingManager

    RecordingWriter

    RecordingStream

    DatabaseConnection

The same stream may have healthy canonical acquisition but degraded PostgreSQL recording.

### 5.10 Recovery Scope

Recovery metrics must preserve:

    RecoveryOperationId

    TargetComponent

    RecoveryGeneration

The system must not treat multiple retry attempts as independent successful recoveries.

### 5.11 Stable vs Ephemeral Identity

Metric identities may be:

    Stable Logical Identity

or:

    Ephemeral Runtime Identity

For example:

    StreamId
        Stable Logical Identity

    ConnectionGeneration
        Ephemeral Runtime Identity

Snapshots should preserve both where relevant.

### 5.12 Metric Identity Invariant

    Every Metric Must Have
    An Unambiguous Scope
    And Lifecycle.

---

## 6. Metrics Snapshot Contracts

Monitoring consumers should read snapshots rather than repeatedly inspecting mutable runtime internals.

The Metrics Registry provides structured snapshots suitable for the runtime API and Ratatui TUI.

### 6.1 MetricsSnapshot

Conceptual representation:

    MetricsSnapshot {
        snapshot_id

        captured_at
        collection_duration

        runtime_metrics

        exchange_metrics
        connection_metrics

        source_stream_metrics
        derived_stream_metrics

        subscriber_metrics
        retention_metrics

        recording_metrics
        recovery_metrics
    }

The snapshot may contain bounded collections of component-specific measurements.

### 6.2 Snapshot Timestamp

Each snapshot must identify when it was collected.

Conceptually:

    CapturedAt

This timestamp is diagnostic.

It does not establish that every included counter was sampled at precisely the same instant.

### 6.3 Snapshot Identification

Each generated snapshot receives a monotonically advancing identifier within its snapshot epoch.

Conceptually:

    SnapshotEpoch

    SnapshotId

A process restart may establish a new snapshot epoch.

### 6.4 Per-Stream Snapshot

Conceptual representation:

    StreamMetricsSnapshot {
        stream_id
        stream_type

        runtime_generation

        events_received
        events_accepted
        events_published

        input_event_rate
        output_event_rate

        processing_latency

        latest_event_timestamp
        latest_receive_timestamp

        integrity_metrics

        publication_metrics
        recording_metrics

        captured_at
    }

Not every stream type supports every field.

Unavailable measurements must remain explicitly unavailable.

### 6.5 Connection Snapshot

Conceptual representation:

    ConnectionMetricsSnapshot {
        connection_id
        connection_generation

        exchange
        api_pool

        messages_received
        bytes_received

        receive_rate

        reconnect_attempts
        connection_duration

        last_message_at

        captured_at
    }

The authoritative connection lifecycle state is supplied separately.

### 6.6 Subscriber Snapshot

Conceptual representation:

    SubscriberMetricsSnapshot {
        subscriber_id
        session_generation

        subscribed_streams

        events_queued
        events_written

        queue_events
        queue_bytes

        write_latency
        last_delivery_at

        captured_at
    }

Subscriber metrics must distinguish queue admission from successful socket writes.

A successful socket write does not establish downstream application processing.

### 6.7 Derived Stream Snapshot

Conceptual representation:

    DerivedStreamMetricsSnapshot {
        derived_stream_id
        runtime_generation

        input_events
        output_events

        merge_buffer_events
        merge_buffer_bytes

        watermark_ns
        watermark_lag_ns

        late_events
        ordering_failures

        conversion_events
        conversion_unavailable

        captured_at
    }

The snapshot must preserve independent source-participation and reference-health information through the relevant component state.

### 6.8 Recording Snapshot

Conceptual representation:

    RecordingMetricsSnapshot {
        recording_state

        events_admitted
        events_committed

        pending_events
        pending_bytes

        commit_latency

        last_committed_cursor
        unresolved_gaps

        captured_at
    }

Committed cursor and gap information must come from the Recording Manager's authoritative state.

### 6.9 Snapshot History

The Metrics Registry may retain a bounded number of recent snapshots.

Conceptually:

    MetricsSnapshotHistory {
        maximum_snapshots
        snapshots
    }

The initial implementation should use a ring buffer or equivalent bounded structure.

Snapshot history supports:

- Short-term throughput graphs.
- Recent latency trends.
- Resource usage trends.
- Brief diagnostic inspection.

### 6.10 Snapshot Retention

Metric snapshots remain operational diagnostics.

They are not written to PostgreSQL.

The initial implementation may keep them only in memory.

Longer-term diagnostic persistence is optional and must use a separate explicitly configured local mechanism.

### 6.11 Snapshot Publication

Snapshot consumers may include:

    Runtime API

    Ratatui TUI

    Internal HealthEvaluator

These consumers should share the same snapshot model.

The system must not create separate incompatible metric representations for each UI.

### 6.12 Slow Snapshot Consumers

A slow TUI or API client must not block metrics collection indefinitely.

The recommended model is:

    Latest Snapshot Available

Consumers may skip intermediate diagnostic snapshots.

This is acceptable because metric snapshots are not canonical market-data events.

### 6.13 Snapshot Size

Snapshot size must be bounded by:

- Registered metric identities.
- Maximum active components.
- Maximum snapshot history.
- Maximum retained diagnostic detail.

High-volume market-data payloads must not be copied into monitoring snapshots.

### 6.14 Snapshot Invariant

    Monitoring Snapshots
    Are Bounded Views Of
    Operational Measurements.

    They Do Not Replace
    Authoritative Component State
    Or Canonical Event History.

---

## 7. Exchange Connectivity Metrics

Exchange connectivity metrics describe the operational behavior of REST and WebSocket connections.

The Connection Manager remains authoritative for connection lifecycle state.

The Metrics Registry records measurements and exposes snapshots.

### 7.1 Responsibilities

Connectivity metrics must provide visibility into:

- Active exchange connections.
- Connection establishment.
- WebSocket message throughput.
- Network disconnections.
- Reconnection attempts.
- Reconnection duration.
- Connection uptime.
- Message receive activity.
- Transport errors.
- Exchange API rate limits.
- REST request failures.
- Transport-level latency.

Transport metrics must not independently claim canonical market-data completeness.

### 7.2 ConnectionMetrics

Conceptual representation:

    ConnectionMetrics {
        connection_id
        connection_generation

        exchange
        market_family
        api_pool

        messages_received_total
        bytes_received_total

        reconnect_attempts_total
        disconnects_total

        transport_errors_total
        malformed_frames_total

        last_message_received_at
        connection_started_at

        connection_state
    }

The authoritative connection state comes from the Connection Manager.

### 7.3 Connection Lifecycle Measurements

Relevant metrics include:

| Metric | Type | Meaning |
|---|---|---|
| `connection_attempts_total` | Counter | Connection establishment attempts |
| `connection_successes_total` | Counter | Successful connections |
| `connection_failures_total` | Counter | Failed connection attempts |
| `connection_disconnects_total` | Counter | Unexpected disconnections |
| `connection_reconnects_total` | Counter | Successful reconnections |
| `connection_uptime_ns` | Duration | Current connection-generation uptime |
| `connection_last_message_age_ns` | Duration | Time since the last received message |
| `connection_messages_received_total` | Counter | Received WebSocket messages |
| `connection_bytes_received_total` | Counter | Received transport bytes |

Connection lifecycle counters must distinguish process-lifetime totals from individual connection-generation totals.

### 7.4 Message Throughput

Message throughput is calculated from received message counters.

Conceptually:

    MessagesPerSecond =
        DeltaMessagesReceived
        /
        DeltaElapsedSeconds

This measures transport activity.

It does not necessarily equal canonical event throughput.

One WebSocket message may contain:

- One trade.
- Multiple trades.
- One depth update.
- Multiple price-level changes.
- A heartbeat.
- A subscription acknowledgement.

### 7.5 WebSocket Receive Activity

The Connection Manager tracks the last successful transport receive operation.

However:

    No WebSocket Message
        !=
    Connection Failure

Some streams may legitimately produce no market-data events for extended periods.

Heartbeat, ping/pong, and exchange-specific liveness evidence must be considered separately.

### 7.6 Connection Latency

Useful measurements include:

    ConnectionEstablishmentDuration

    ReconnectionDuration

    WebSocketPingRoundTripTime

Ping RTT measures network and endpoint responsiveness.

It is not equivalent to exchange event-to-receive latency.

### 7.7 Transport Error Classification

Errors should be grouped into bounded categories:

    Timeout

    ConnectionReset

    RemoteClose

    ProtocolError

    AuthenticationFailure

    InvalidSubscription

    ExchangeUnavailable

    Other

The error classification must follow the connectivity contracts defined in Document 04.

### 7.8 REST Metrics

REST metrics include:

    RequestsTotal

    SuccessfulRequestsTotal

    FailedRequestsTotal

    RequestDuration

    RetriesTotal

    RateLimitResponsesTotal

    ExchangeUnavailableResponsesTotal

    ActiveRequests

The Metrics Registry must not create an independent REST retry policy.

### 7.9 API Weight Metrics

For exchanges using weighted request limits:

    ConfiguredWeightLimit

    ObservedWeightUsage

    EstimatedRemainingWeight

    CooldownRemaining

    WeightWindow

The Rate Limiter remains authoritative for these values.

Metrics must distinguish authoritative exchange-reported weight usage from locally estimated usage.

### 7.10 Subscription Metrics

Native subscriptions expose:

    SubscriptionAttemptsTotal

    SubscriptionSuccessesTotal

    SubscriptionFailuresTotal

    ActiveSubscriptions

    LastSubscriptionAcknowledgement

    SubscriptionState

A WebSocket connection may be healthy while one of its native subscriptions is invalid.

### 7.11 Shared Connections

One physical connection may serve several canonical streams.

Connection metrics must not be duplicated in global aggregation merely because multiple streams depend on the same connection.

### 7.12 Connectivity Invariant

    Transport Health
    Does Not Establish
    Market-Data Integrity.

A connected WebSocket is necessary for live acquisition but is not proof that every required trade or depth update has been received.

---

## 8. Trade Stream Metrics

Trade metrics describe canonical execution processing, continuity, deduplication, and recovery.

The Trade Processor remains authoritative for trade identity and continuity.

### 8.1 TradeMetrics

Conceptual representation:

    TradeMetrics {
        stream_id
        stream_generation

        messages_received_total

        trades_decoded_total
        trades_accepted_total
        trades_rejected_total

        duplicate_trades_total
        continuity_failures_total

        recovery_attempts_total
        recovered_trades_total

        unresolved_gap_count

        last_accepted_event_timestamp_ns
        last_accepted_receive_timestamp_ns

        continuity_state
    }

The metrics must distinguish accepted canonical trades from received transport messages.

### 8.2 Trade Processing Metrics

| Metric | Type | Meaning |
|---|---|---|
| `trade_decoded_total` | Counter | Decoded trade candidates |
| `trade_accepted_total` | Counter | Accepted canonical executions |
| `trade_rejected_total` | Counter | Rejected trade records |
| `trade_duplicates_total` | Counter | Duplicate executions detected |
| `trade_continuity_failures_total` | Counter | Detected continuity violations |
| `trade_recovery_attempts_total` | Counter | Recovery attempts |
| `trade_recovered_total` | Counter | Successfully recovered executions |
| `trade_unresolved_gaps` | Gauge | Currently unresolved continuity gaps |
| `trade_processing_latency_ns` | Distribution | Canonical processing duration |

### 8.3 Trade Continuity

The Trade Processor determines whether execution continuity is:

    Verified

    PartiallyVerified

    Unverified

    Incomplete

The Metrics Registry exposes this classification.

It must not independently infer trade continuity from timestamps or event rates.

### 8.4 Missing Executions

When missing executions can be established through native source identity or sequencing:

    Continuity Failure Detected
                |
                +-- Update Trade Integrity State
                |
                +-- Increment Continuity Metric
                |
                +-- Report Recovery Requirement

A missing execution must not be counted repeatedly merely because recovery retries occur.

The implementation should distinguish:

    UniqueDetectedGaps

from:

    GapRecoveryAttempts

### 8.5 Duplicate Detection

The duplicate counter measures canonical candidates rejected or suppressed because they represent already accepted native executions.

The counter must not treat:

    Same Price
    Same Quantity
    Same Timestamp

as sufficient duplicate evidence.

Native execution identity semantics remain authoritative.

### 8.6 Trade Recovery Metrics

Recovery measurements include:

    RecoveryRequestedTotal

    RecoveryStartedTotal

    RecoverySucceededTotal

    RecoveryFailedTotal

    RecoveredExecutionsTotal

    RecoveryDuration

    OldestUnresolvedGapAge

The Recovery Coordinator owns recovery lifecycle decisions.

### 8.7 Trade Throughput

Trade throughput should be measured using:

    AcceptedCanonicalTradesPerSecond

This is more meaningful for market-data consumers than raw WebSocket message throughput.

### 8.8 Trade Event-Time Progress

Useful timestamps include:

    LatestAcceptedEventTimestamp

    LatestAcceptedReceiveTimestamp

    LatestRecoveredEventTimestamp

These values must retain their actual semantics.

A recovered trade may have an old event timestamp despite being accepted recently.

### 8.9 Trade Latency

Trade latency measurements should distinguish:

    ExchangeEventToLocalReceive

    LocalReceiveToCanonicalAcceptance

    CanonicalAcceptanceToPublication

The first depends on exchange clock accuracy.

The latter two may be measured using local monotonic timing.

### 8.10 Trade Integrity Invariant

    High Trade Throughput
    Does Not Prove
    Complete Trade Coverage.

A trade stream may remain active while its continuity state is degraded or unverified.

---

## 9. L2 BookStore Metrics

L2 metrics describe order-book initialization, synchronization, sequence continuity, level updates, and reconstruction generations.

The shared BookStore and its synchronization controller remain authoritative.

### 9.1 L2Metrics

Conceptual representation:

    L2Metrics {
        stream_id
        stream_generation

        book_generation
        synchronization_state

        snapshots_received_total
        initializations_completed_total

        depth_updates_received_total
        depth_updates_accepted_total

        level_changes_emitted_total

        sequence_gaps_total
        synchronization_failures_total

        resynchronization_attempts_total
        resynchronization_successes_total

        bid_level_count
        ask_level_count

        last_valid_update_at
        last_initialization_at
    }

### 9.2 Synchronization States

The L2 synchronization controller exposes states corresponding to:

    Uninitialized

    Synchronizing

    Synchronized

    Invalid

    Recovering

These describe authoritative book readiness.

The Metrics Registry must not treat a nonempty BookStore as proof of synchronization.

### 9.3 Snapshot Metrics

Relevant measurements:

| Metric | Type | Meaning |
|---|---|---|
| `l2_snapshots_received_total` | Counter | Source snapshots received |
| `l2_initializations_completed_total` | Counter | Completed authoritative initializations |
| `l2_snapshot_failures_total` | Counter | Failed snapshot initialization attempts |
| `l2_snapshot_apply_latency_ns` | Distribution | Snapshot application duration |
| `l2_initialization_age_ns` | Duration | Time since the last initialization |

A snapshot received is not necessarily a successfully applied initialization.

### 9.4 Depth Update Metrics

Metrics include:

    DepthMessagesReceivedTotal

    DepthEventsAcceptedTotal

    LevelsProcessedTotal

    LevelsChangedTotal

    LevelsDeletedTotal

    LevelsUnchangedTotal

The counters must distinguish source records from resulting canonical level changes.

### 9.5 Sequence Integrity

Relevant measurements:

    SequenceGapsDetectedTotal

    SequenceValidationFailuresTotal

    SequenceResetsTotal

    SynchronizationInvalidationsTotal

    LastAcceptedSequence

Not every exchange provides comparable sequence semantics.

Missing source sequence metadata must not be represented as sequence zero.

### 9.6 Book Generations

Every authoritative book generation must be identifiable.

Metrics must expose:

    CurrentBookGeneration

    CurrentSynchronizationState

    InitializationTimestamp

    LastValidUpdateTimestamp

A new generation may begin after synchronization recovery.

### 9.7 Book Depth

The BookStore may expose:

    BidLevelCount

    AskLevelCount

    BestBid

    BestAsk

    Spread

These are diagnostic views of current book state.

Best bid and ask must not be presented as valid market data when the book is unsynchronized.

### 9.8 Book Readiness

Book readiness is determined by the synchronization controller.

A book with:

    BidLevelCount = 1000

    AskLevelCount = 1000

may still be invalid after a sequence gap.

The HealthEvaluator must honor the authoritative synchronization state.

### 9.9 Initialization Completeness

The metrics subsystem should expose whether the current book generation has a complete authoritative initialization.

This is particularly relevant for:

- PostgreSQL recording.
- Historical bootstrap.
- Reconstruction segment validation.
- Recovery after synchronization failures.

The BookStore must not be reconstructed independently inside the Metrics Registry.

### 9.10 L2 Processing Latency

Useful measurements:

    SnapshotApplyDuration

    DepthUpdateValidationDuration

    BookMutationDuration

    CanonicalLevelEmissionDuration

Detailed timing may be disabled unless explicitly configured.

### 9.11 L2 Invariant

    Book Synchronization
    Is An Authoritative State,
    Not A Metric-Derived Guess.

---

## 10. Publisher and Subscriber Metrics

The Publisher distributes canonical and derived events through Unix domain sockets.

Publisher metrics describe internal admission, queueing, delivery attempts, and subscriber health.

### 10.1 PublisherMetrics

Conceptual representation:

    PublisherMetrics {
        published_events_total

        publication_admission_failures_total

        active_subscribers

        queued_events
        queued_bytes

        disconnected_subscribers_total

        delivery_failures_total

        publication_admission_latency
    }

### 10.2 Publication Metrics

Relevant metrics:

| Metric | Type | Meaning |
|---|---|---|
| `publisher_events_admitted_total` | Counter | Events accepted for publication |
| `publisher_admission_failures_total` | Counter | Events rejected during admission |
| `publisher_active_subscribers` | Gauge | Active subscriber sessions |
| `publisher_queue_events` | Gauge | Current queued events |
| `publisher_queue_bytes` | Gauge | Current queue memory |
| `publisher_socket_writes_total` | Counter | Successful socket write operations |
| `publisher_write_failures_total` | Counter | Failed socket writes |
| `publisher_subscriber_disconnects_total` | Counter | Subscriber disconnections |

Socket write operations and logical event deliveries must remain separate metrics.

One socket write may contain multiple framed events.

### 10.3 SubscriberMetrics

Conceptual representation:

    SubscriberMetrics {
        subscriber_id
        session_generation

        subscribed_stream_count

        events_enqueued_total
        events_delivered_total

        queue_events
        queue_bytes

        slow_consumer_events_total

        socket_write_failures_total

        last_successful_write_at
    }

### 10.4 Delivery Boundaries

The Publisher must distinguish:

    Event Admitted

    Event Queued For Subscriber

    Event Written To Socket

    Event Acknowledged By Application

The final boundary exists only if the subscriber protocol explicitly supports application acknowledgements.

A successful socket write does not prove downstream application processing.

### 10.5 Slow Consumers

A subscriber may become unable to process events fast enough.

Relevant metrics include:

    SubscriberQueueOccupancy

    SubscriberQueueAge

    SubscriberWriteLatency

    SlowConsumerDisconnectsTotal

The Publisher remains responsible for enforcing subscriber queue limits.

### 10.6 Subscriber Backpressure

A slow subscriber must not cause unbounded memory growth.

The Metrics Registry reports queue pressure.

The Publisher applies the configured backpressure and disconnect policies.

### 10.7 Replay Metrics

Where replay is supported:

    ReplayRequestsTotal

    ReplayEventsDeliveredTotal

    ReplayFailuresTotal

    ReplayCoverageUnavailableTotal

    ReplayCatchUpDuration

Replay metrics must distinguish retained-event availability from subscriber delivery success.

### 10.8 Shared Source Publication

One canonical source event may be delivered to multiple subscribers.

Metrics should distinguish:

    UniqueSourceEventsPublished

from:

    SubscriberEventDeliveries

These counters must not be presented as interchangeable.

### 10.9 Publisher Invariant

    Publisher Admission
    Does Not Establish
    Subscriber Application
    Processing Success.

---

## 11. Live Retention and Parquet Spill Metrics

The Live Retention Manager preserves accepted canonical events for bootstrap, replay, and recovery.

Metrics describe retained coverage, memory usage, spill activity, and protection pressure.

### 11.1 RetentionMetrics

Conceptual representation:

    RetentionMetrics {
        retained_events
        retained_bytes

        memory_events
        memory_bytes

        parquet_segments
        parquet_bytes

        protected_intervals
        active_readers

        spill_operations_total
        spill_failures_total

        coverage_status
        retention_state
    }

### 11.2 Memory Retention

Relevant measurements:

    RetainedEvents

    RetainedBytes

    MemoryUtilizationRatio

    OldestRetainedEventAge

    ProtectedEventCount

    ProtectedBytes

Metrics must distinguish total retained data from data pinned by bootstrap protection.

### 11.3 Spill Activity

Parquet spill metrics include:

| Metric | Type | Meaning |
|---|---|---|
| `retention_spill_operations_total` | Counter | Spill operations started |
| `retention_spill_completed_total` | Counter | Successfully completed spills |
| `retention_spill_failed_total` | Counter | Failed spill operations |
| `retention_spill_events_total` | Counter | Events committed to spill segments |
| `retention_spill_bytes_total` | Counter | Bytes committed to spill storage |
| `retention_spill_latency_ns` | Distribution | Spill completion duration |
| `retention_pending_spill_bytes` | Gauge | Bytes awaiting spill completion |

A spill attempt is not equivalent to a committed Parquet segment.

### 11.4 Retention Coverage

The Retention Manager remains authoritative for:

    EarliestAvailableCursor

    LatestAvailableCursor

    AvailableCursorIntervals

    MissingCursorIntervals

    ProtectedCursorIntervals

The Metrics Registry exposes bounded summaries.

Detailed interval queries belong to the retention API.

### 11.5 Bootstrap Protection

Metrics include:

    ActiveBootstrapProtections

    ProtectedEvents

    ProtectedBytes

    OldestProtectionAge

    ProtectionAdmissionFailuresTotal

A bootstrap protection may prevent ordinary retention cleanup.

The subsystem must expose the resulting resource pressure.

### 11.6 Retention Readers

Metrics include:

    ActiveRetentionReaders

    ReplayEventsReadTotal

    ReplayReadLatency

    RetentionReadFailuresTotal

Long-running readers must not cause unlimited diagnostic state growth.

### 11.7 Disk Availability

Where the runtime has reliable filesystem information, retention metrics may expose:

    AvailableDiskBytes

    UsedRetentionDiskBytes

    ConfiguredDiskBudgetBytes

If free-space information cannot be established reliably, it must remain unavailable rather than be estimated as zero.

### 11.8 Retention Failures

Relevant failures include:

    ProtectionUnavailable

    SpillWriteFailure

    SpillCommitFailure

    RetentionCapacityExceeded

    ReplayCoverageUnavailable

    RetentionManifestInvalid

The Retention Manager owns failure classification and recovery.

### 11.9 Retention vs Recording

Retention health and PostgreSQL recording health are independent.

For example:

    Retention:
        Healthy

    Recording:
        Degraded

This may permit recording recovery from protected retention.

The reverse may also occur.

### 11.10 Retention Invariant

    Retention Metrics
    Describe Available
    Canonical Event Coverage.

    They Do Not Establish
    Complete Native Exchange
    Acquisition.

---

## 12. Combined Stream and Currency Conversion Metrics

Derived streams require measurements for input participation, ordering, buffering, watermark progress, conversion references, and transformed output.

The Merge Engine and Currency Conversion Engine remain authoritative for their respective states.

### 12.1 DerivedStreamMetrics

Conceptual representation:

    DerivedStreamMetrics {
        derived_stream_id
        runtime_generation

        input_events_total
        output_events_total

        required_source_count
        active_source_count

        merge_buffer_events
        merge_buffer_bytes

        combined_watermark_ns

        late_events_total
        ordering_failures_total

        conversion_events_total
        conversion_unavailable_total

        last_output_event_timestamp_ns
    }

### 12.2 Source Participation

Relevant measurements include:

    RequiredSourceCount

    ActiveSourceCount

    DegradedSourceCount

    UnavailableSourceCount

These values must reflect the authoritative dependency state.

A derived stream with healthy output throughput may still be degraded because a required source is unavailable.

### 12.3 Merge Throughput

Metrics include:

    MergeInputEventsPerSecond

    MergeOutputEventsPerSecond

    MergeEventsBuffered

    MergeEventsReleased

The Merge Engine may receive source events faster than it can release them due to watermark constraints.

This does not necessarily imply CPU overload.

### 12.4 Reorder Buffer

Relevant measurements:

    MergeBufferedEvents

    MergeBufferedBytes

    MergeBufferOccupancyRatio

    MergeBufferOldestEventAge

    MergeBufferSaturationEventsTotal

The Merge Engine remains responsible for enforcing buffer limits.

### 12.5 Watermark Metrics

Metrics include:

    CombinedWatermarkNs

    PerSourceWatermarkNs

    WatermarkAdvanceRate

    WatermarkStallDuration

    WatermarkLimitingSource

A watermark is an event-time progress frontier.

It must not be interpreted as a local processing timestamp.

### 12.6 Watermark Lag

Watermark lag must have an explicitly defined reference.

Possible measurements:

    LatestObservedEventTime
        -
    CombinedWatermark

or:

    CurrentWallClockTime
        -
    CombinedWatermark

These values have different interpretations.

The preferred primary metric is event-time progress lag.

Wall-clock-relative watermark age may be reported separately.

### 12.7 Late Events

Metrics include:

    LateEventsDetectedTotal

    LateEventsPublishedTotal

    LateEventsSideOutputTotal

    LateEventsCausingFailureTotal

    MaximumObservedLateness

The metrics must distinguish events reordered successfully inside the buffer from events violating the finalized output frontier.

### 12.8 Ordering Health

Ordering state must originate from the Merge Engine.

Possible classifications:

    Ordered

    Degraded

    Violated

    Recovering

The Metrics Registry must not infer ordering validity solely from whether output timestamps appear monotonic in a sampled window.

### 12.9 Conversion Metrics

Conversion measurements include:

| Metric | Type | Meaning |
|---|---|---|
| `conversion_events_total` | Counter | Events evaluated for conversion |
| `conversion_identity_total` | Counter | Identity conversions |
| `conversion_converted_total` | Counter | Successful conversions |
| `conversion_unavailable_total` | Counter | Unavailable conversions |
| `conversion_skipped_total` | Counter | Deliberately skipped conversions |
| `conversion_reference_updates_total` | Counter | Reference observations incorporated |
| `conversion_reference_stale_total` | Counter | Stale reference failures |
| `conversion_reference_missing_total` | Counter | Missing reference failures |
| `conversion_arithmetic_failures_total` | Counter | Numerical failures |
| `conversion_pending_events` | Gauge | Events waiting for reference readiness |

The conversion status definitions remain compatible with the historical conversion pipeline.

### 12.10 Reference Health

Relevant measurements:

    RequiredReferenceCount

    AvailableReferenceCount

    StaleReferenceCount

    MissingReferenceCount

    OldestRequiredReferenceAge

Reference health must be evaluated using the conversion engine's event-time eligibility rules.

A reference connection being active does not prove that its latest observation is valid for the target event.

### 12.11 Conversion Processing Latency

Possible measurements include:

    ReferenceLookupDuration

    ConversionPathEvaluationDuration

    ExactDecimalArithmeticDuration

    TotalConversionDuration

Detailed conversion timing should remain optional.

### 12.12 Derived Publication

Derived output metrics must distinguish:

    DerivedEventsProduced

    DerivedEventsAdmittedToPublisher

    DerivedEventsWrittenToSubscribers

These are separate completion boundaries.

### 12.13 Derived Stream Health

A derived stream may have:

    Sources:
        Healthy

    Merge Ordering:
        Healthy

    Conversion:
        Degraded

    Publication:
        Healthy

Its overall health may be degraded even while events continue flowing.

The HealthEvaluator must preserve the underlying reasons.

### 12.14 Derived Invariant

    Derived Output Throughput
    Does Not Prove
    Complete Input Participation,
    Correct Ordering,
    Or Successful Conversion.

---

## 13. PostgreSQL Recording Metrics

PostgreSQL metrics describe the availability, performance, durability, and coverage of optional canonical market-data recording.

The Recording Manager remains authoritative for recording state and committed event coverage.

The Metrics Registry exposes measurements and bounded summaries.

### 13.1 RecordingMetrics

Conceptual representation:

    RecordingMetrics {
        recording_manager_id

        database_state
        recording_state

        active_recording_streams

        events_admitted_total
        events_committed_total

        batches_created_total
        batches_committed_total
        batches_failed_total

        pending_events
        pending_bytes

        commit_latency

        recovery_events_total
        unresolved_gap_count

        last_successful_commit_at
    }

Per-stream recording progress must remain separately accessible.

### 13.2 Database Connectivity

Relevant measurements:

| Metric | Type | Description |
|---|---|---|
| `recording_database_connected` | State | PostgreSQL connectivity |
| `recording_database_connections` | Gauge | Active recording connections |
| `recording_database_reconnections_total` | Counter | Successful reconnections |
| `recording_database_connection_failures_total` | Counter | Connection establishment failures |
| `recording_database_request_latency_ns` | Distribution | Database operation latency |
| `recording_database_errors_total` | Counter | Classified database errors |

Database connectivity must not be interpreted as proof of successful recording.

### 13.3 Recording Throughput

The subsystem should expose:

    RecordingEventsAdmittedPerSecond

    RecordingEventsCommittedPerSecond

    RecordingBytesCommittedPerSecond

The distinction between admission and commit throughput is important.

If admission consistently exceeds commit throughput, the recording backlog grows.

### 13.4 Recording Queue

Relevant measurements:

    RecordingPendingEvents

    RecordingPendingBytes

    RecordingPendingBatches

    RecordingQueueOccupancyRatio

    RecordingOldestPendingAge

The Recording Manager remains responsible for queue admission and capacity enforcement.

### 13.5 Recording Transactions

Transaction metrics include:

    TransactionsStartedTotal

    TransactionsCommittedTotal

    TransactionsFailedTotal

    TransactionsRetriedTotal

    AmbiguousCommitOutcomesTotal

    CommitReconciliationsTotal

Idempotent retries must not inflate the count of newly recorded canonical events.

### 13.6 Recording Commit Latency

Commit latency measures the local duration of database persistence operations.

Conceptually:

    TransactionStarted
            |
            v
    Database Write
            |
            v
    Commit Confirmation
            |
            v
    TransactionCompleted

The metric may be separated into:

    BatchPreparationDuration

    DatabaseExecutionDuration

    CommitConfirmationDuration

Detailed timing should remain optional.

### 13.7 Recording Progress

Per-stream progress includes:

    LastAdmittedCursor

    LastContiguousCommittedCursor

    CommittedCursorIntervals

    UnresolvedRecordingGaps

These values come from the Recording Manager's authoritative state.

A highest committed cursor must not be substituted for the last contiguous committed cursor.

### 13.8 Recording Lag

Recording lag may be measured in multiple ways.

**Cursor lag**

    LastAdmittedCursor
        -
    LastContiguousCommittedCursor

This calculation is valid only within a compatible cursor epoch and cursor numbering contract.

**Commit age**

    CurrentMonotonicTime
        -
    OldestPendingAdmissionTime

**Event-time lag**

    LatestAcceptedEventTimestamp
        -
    LatestContiguouslyCommittedEventTimestamp

These measurements are not interchangeable.

### 13.9 Recording Coverage

Metrics may summarize:

    KnownRecordingGapCount

    UnverifiedRecordingIntervals

    RecoverableGapCount

    UnrecoverableGapCount

Detailed coverage intervals belong to the recording coverage API.

### 13.10 Archive Replacement

The Recording Manager may expose:

    ArchiveReplacementEligibleIntervals

    ArchiveReplacementPendingIntervals

    RecordingRetentionBytes

    RetentionCleanupFailuresTotal

These metrics describe storage lifecycle state.

They must not independently validate historical archive completeness.

### 13.11 Recording Health Invariant

    Database Connected
        !=
    Recording Current
        !=
    Recorded Coverage Complete.

These conditions must remain independently observable.

---

## 14. Recovery Coordination Metrics

Recovery metrics describe attempts to restore failed or degraded runtime components.

The Recovery Coordinator remains authoritative for recovery lifecycle state.

### 14.1 RecoveryMetrics

Conceptual representation:

    RecoveryMetrics {
        active_recoveries

        recovery_attempts_total
        recovery_successes_total
        recovery_failures_total

        escalations_total

        retry_backoff_duration
        recovery_duration

        oldest_active_recovery_age
    }

Individual recovery operations retain their own state and identity.

### 14.2 Recovery Categories

Metrics should distinguish recovery targets such as:

    ExchangeConnection

    NativeSubscription

    TradeContinuity

    L2Synchronization

    Publisher

    LiveRetention

    MergeRuntime

    ConversionReferences

    PostgreSQLRecording

The categories must correspond to actual recovery responsibilities.

### 14.3 Recovery Operation Identity

A recovery operation has an authoritative identifier and generation.

Conceptually:

    RecoveryMetricIdentity {
        recovery_operation_id

        target_component
        target_generation

        recovery_category
    }

A retry is not a separate successful recovery.

### 14.4 Recovery Counters

| Metric | Type | Description |
|---|---|---|
| `recovery_operations_started_total` | Counter | New recovery operations |
| `recovery_attempts_total` | Counter | Individual attempts |
| `recovery_attempts_failed_total` | Counter | Failed attempts |
| `recovery_operations_completed_total` | Counter | Completed recovery operations |
| `recovery_operations_failed_total` | Counter | Terminal failures |
| `recovery_escalations_total` | Counter | Escalations |
| `recovery_active_operations` | Gauge | Currently active recoveries |

Counters must distinguish an operation from the attempts belonging to it.

### 14.5 Retry and Backoff

Relevant measurements:

    CurrentRetryAttempt

    CurrentBackoffDuration

    NextRetryDeadline

    ConsecutiveFailures

    TotalRetryDuration

The Recovery Coordinator determines these values.

The Metrics Registry must not implement an independent retry scheduler.

### 14.6 Recovery Duration

Recovery duration is measured using a monotonic clock.

Conceptually:

    RecoveryDuration =
        RecoveryCompletionTime
        -
        RecoveryStartTime

Unfinished recoveries expose elapsed recovery age rather than completed recovery duration.

### 14.7 Recovery Completion

A successful reconnect does not necessarily mean complete recovery.

For example:

    WebSocket Reconnected:
        Yes

    Missing Trade Coverage Recovered:
        No

The corresponding recovery operation must preserve these independent guarantees.

### 14.8 Repeated Failures

Metrics should expose repeated failure patterns without storing an unbounded operation history.

Examples:

    ConsecutiveConnectionFailures

    ConsecutiveBookSynchronizationFailures

    RecoveryEscalations

    RecoveryOperationsWithinRecentWindow

The final measurement may be calculated from bounded local history.

### 14.9 Recovery Health Invariant

    Retry Activity
    Does Not Establish
    Successful Recovery.

A component remains degraded until its authoritative recovery conditions are satisfied.

---

## 15. Runtime Resource Metrics

MarketForge Live must expose global and component-level resource usage.

Resource monitoring supports capacity planning, failure detection, and runtime admission decisions.

The Runtime Resource Manager remains authoritative for configured reservations and limits.

### 15.1 Resource Categories

Relevant resources include:

    ProcessMemory

    ComponentMemory

    BufferMemory

    ActiveTasks

    ActiveConnections

    FileDescriptors

    SocketQueues

    DiskUsage

    DatabaseConnections

    ProcessingConcurrency

### 15.2 RuntimeResourceSnapshot

Conceptual representation:

    RuntimeResourceSnapshot {
        resident_memory_bytes

        configured_memory_budget_bytes

        registered_buffer_bytes

        active_connections
        active_subscribers

        active_source_streams
        active_derived_streams

        active_runtime_tasks

        open_file_descriptors

        retention_disk_bytes
        recording_queue_bytes

        captured_at
    }

Unavailable platform measurements must remain explicitly unavailable.

### 15.3 Memory Measurements

The runtime should distinguish:

    ProcessResidentMemory

    RegisteredBufferMemory

    RetentionMemory

    MergeBufferMemory

    RecordingQueueMemory

    PublisherQueueMemory

The sum of registered component memory may differ from process resident memory because of shared allocations, allocator overhead, and untracked infrastructure.

### 15.4 Memory Budget Usage

For a configured resource budget:

    MemoryUtilization =
        ReservedMemory
        /
        ConfiguredMemoryBudget

The numerator must use the resource manager's authoritative reservation accounting.

Process RSS is a separate measurement.

### 15.5 Buffer Pressure

Relevant gauges include:

    PublisherQueueOccupancy

    RetentionBufferOccupancy

    MergeBufferOccupancy

    ConversionBufferOccupancy

    RecordingQueueOccupancy

These values help identify resource pressure before hard limits are reached.

### 15.6 Task Metrics

Useful measurements:

    ActiveRuntimeTasks

    RunningProcessingTasks

    PendingProcessingTasks

    FailedTasksTotal

    RestartedTasksTotal

Task metrics must remain bounded by registered runtime components.

### 15.7 CPU Measurements

The runtime may expose:

    ProcessCpuUtilization

    ProcessingWorkerBusyRatio

    RuntimeSchedulerDelay

These measurements are optional for the initial implementation.

CPU monitoring must not require expensive sampling on every canonical event.

### 15.8 File Descriptors

On supported platforms, the runtime may expose:

    OpenFileDescriptorCount

    ConfiguredFileDescriptorBudget

    FileDescriptorPressure

The metric should include Unix sockets and other open resources.

### 15.9 Disk Measurements

Separate disk usage for:

    LiveRetention

    ParquetSpill

    LocalOperationalLogs

    OtherConfiguredLocalStorage

PostgreSQL database storage must not automatically be included in local process disk usage when the database is hosted elsewhere.

### 15.10 Resource Pressure States

Conceptual states:

    Normal

    Elevated

    Critical

    Exhausted

Thresholds are configuration-defined.

The Resource Manager remains responsible for admission control and resource-limit enforcement.

### 15.11 Resource Invariant

    Resource Metrics
    Describe Usage And Pressure.

    Resource Admission
    Remains The Responsibility
    Of The Owning Manager.

---

## 16. Latency Measurement

Latency measurements must distinguish event timestamps, local processing durations, and downstream delivery times.

These values describe different aspects of system behavior.

### 16.1 Timestamp Categories

The system uses:

    Exchange Event Timestamp

    Local Receive Timestamp

    Local Processing Timestamp

    Publication Timestamp

    Subscriber Delivery Timestamp

Each timestamp must have explicit clock semantics.

### 16.2 Exchange Event Timestamp

The canonical event timestamp originates from the exchange's event-time semantics.

It is represented as:

    event_timestamp_ns

This timestamp may be affected by:

- Exchange clock synchronization.
- Exchange timestamp precision.
- Exchange-specific timestamp definitions.
- Exchange processing delay.

It is not necessarily the exact instant a packet left the exchange.

### 16.3 Local Receive Timestamp

The local receive timestamp records when MarketForge observes an incoming transport message or accepted event.

The measurement point must be consistent.

For example:

    After WebSocket Frame Decoding

or:

    Immediately After Transport Receive

The selected boundary must be documented.

### 16.4 Event-to-Receive Delay

Conceptually:

    EventToReceiveDelay =
        LocalReceiveWallClock
        -
        ExchangeEventTimestamp

This is not a pure network latency measurement.

It includes exchange-side processing and clock-offset uncertainty.

Negative observations may indicate clock differences or timestamp semantics rather than negative physical latency.

The implementation must preserve or classify such observations rather than silently clamping them to zero.

### 16.5 Local Processing Latency

Local processing duration should be measured using a monotonic clock.

Conceptually:

    ProcessingStart
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
    CanonicalAcceptance
          |
          v
    ProcessingComplete

The elapsed monotonic duration measures local processing cost.

### 16.6 Publication Latency

Publication timing may include:

    CanonicalAcceptanceToPublisherAdmission

    PublisherAdmissionToQueue

    QueueToSocketWrite

These should remain separate when detailed timing is enabled.

### 16.7 Derived Stream Latency

Derived processing introduces additional delays:

    CanonicalSourceAccepted
              |
              v
    MergeBufferAdmission
              |
              v
    WatermarkWait
              |
              v
    MergeRelease
              |
              v
    CurrencyConversion
              |
              v
    DerivedPublication

The total derived-event delay includes intentional event-time buffering.

High watermark waiting time does not necessarily indicate inefficient CPU processing.

### 16.8 Recording Latency

Recording measurements include:

    RecordingAdmissionToBatchSeal

    BatchSealToTransactionStart

    TransactionExecutionDuration

    CommitConfirmationDuration

    TotalRecordingAdmissionToCommit

The final duration is useful for understanding how far database durability trails source acceptance.

### 16.9 Latency Sampling

Detailed timing may introduce additional overhead.

The initial implementation should support:

    MetricsEnabled

    DetailedTimingEnabled

    TimingSampleRate

The final configuration names belong to Document 03.

When detailed timing is disabled, essential counters and authoritative lifecycle states remain available.

### 16.10 Latency Distribution

For sampled durations, expose:

    ObservationCount

    Minimum

    Maximum

    Mean

    Selected Percentiles

Percentile calculations must use a clearly defined observation window or histogram.

They must not be presented as exact quantiles when estimated from bounded samples.

### 16.11 Clock Invariant

    Exchange-To-Local Delay
    Depends On Clock Semantics.

    Local Processing Duration
    Uses A Monotonic Clock.

These measurements must never be labeled interchangeably.

---

## 17. Metric Aggregation and Time Windows

Raw counters are useful for cumulative diagnostics.

The TUI and runtime API also require recent rates and trends.

These are computed through bounded aggregation.

### 17.1 Aggregation Objectives

Aggregation should support:

- Recent throughput.
- Recent error frequency.
- Queue pressure trends.
- Latency summaries.
- Recovery activity.
- Resource usage changes.

Aggregation must not retain unlimited event-level samples.

### 17.2 Snapshot-Based Rates

The recommended initial method calculates rates from consecutive metric snapshots.

Conceptually:

    Snapshot A:
        Counter = 100000

    Snapshot B:
        Counter = 115000

    Elapsed Time:
        1 second

    Rate:
        15000 events/s

The elapsed interval must use a monotonic clock.

### 17.3 Counter Generations

A counter reset invalidates an ordinary delta calculation across that reset.

The aggregation layer must track:

    MetricIdentity

    CounterEpoch

    SnapshotEpoch

A new counter generation must not produce an artificial negative rate.

### 17.4 Time Windows

The initial implementation may support:

    LatestSnapshot

    RecentShortWindow

    RecentMediumWindow

The exact durations are configurable.

A bounded ring of recent snapshots may provide the required data.

### 17.5 Moving Averages

Moving averages may smooth volatile throughput measurements.

However, a smoothed rate must remain distinguishable from the most recent instantaneous rate.

The TUI may display both where useful.

### 17.6 Error Rates

An error rate may be expressed as:

    ErrorsPerSecond

or:

    ErrorsPerAcceptedEvent

These have different meanings.

The denominator must be explicit.

### 17.7 Aggregation Across Streams

Global aggregation is valid only for compatible counters.

For example:

    TotalAcceptedSourceTrades

may aggregate accepted trade counts across source streams.

However:

    SourceEvents + DerivedEvents

does not represent unique native market-data events.

A single source event may contribute to multiple derived outputs.

### 17.8 Aggregation Across Exchanges

Exchange-level totals must avoid double-counting shared connections and subscriptions.

For example, one Binance WebSocket connection supporting ten instruments counts as:

    One Connection

not:

    Ten Connections

### 17.9 Histogram Aggregation

Histograms may be aggregated only when they use compatible bucket definitions and measurement semantics.

A processing-latency histogram must not be merged with a socket-delivery-latency histogram as though they measured the same operation.

### 17.10 Snapshot History

The aggregation layer uses a bounded history.

Conceptually:

    AggregationHistory {
        maximum_samples

        snapshots

        derived_rates
        derived_distributions
    }

The system must cap memory usage independently of runtime duration.

### 17.11 Aggregation Invariant

    Aggregated Metrics
    Must Preserve The Meaning
    Of Their Source Measurements.

---

## 18. Sampling and Collection Overhead

MarketForge Live processes high-volume market data.

Metric collection must remain efficient under sustained load.

### 18.1 Collection Principles

The initial design favors:

- Typed counters.
- Local accumulation where possible.
- Atomic counters when necessary.
- Periodic snapshot collection.
- Bounded latency histograms.
- Bounded metric identity sets.
- Optional detailed timing.

It does not require a general-purpose distributed telemetry framework.

### 18.2 Hot-Path Counters

Simple counters may be updated for every accepted event.

Example:

```rust
metrics
    .events_accepted
    .fetch_add(1, Ordering::Relaxed);
```

Relaxed ordering is appropriate for independent statistical counters when no synchronization guarantee depends on the counter.

Correctness-sensitive state must use its own synchronization contract.

### 18.3 Local Counter Aggregation

For a single processing task, local accumulation may reduce atomic contention.

Conceptually:

    Local Event Counter
            |
            v
    Periodic Metric Flush
            |
            v
    Shared Metrics Registry

The implementation must account for unflushed increments when interpreting snapshots.

Metrics are diagnostic and may tolerate bounded temporary lag.

### 18.14 Optional Export Performance

Prometheus exposition occurs outside the canonical
market-data hot path.

Exporter performance must be evaluated separately
from ordinary Metrics Registry updates.

Relevant measurements include:

    ExportRefreshDuration

    ExportSerializationDuration

    ExportResponseBytes

    ScrapeRequestsTotal

    ScrapeFailuresTotal

    ExporterMemoryBytes

Exporter diagnostics must remain bounded and must
not create recursive instrumentation.

The exporter must not allocate memory proportional
to the number of canonical market-data events.

Disabling export must eliminate exporter-specific
listener and serialization overhead.

The Metrics Registry continues operating independently.

### 18.5 Histogram Updates

Histogram updates must remain bounded in cost.

Avoid dynamically allocating one sample object per market-data event.

Fixed-size histogram buckets or bounded accumulation are preferred.

### 18.6 Snapshot Collection

Snapshot collection should run on a dedicated lightweight periodic task.

It must not require stopping exchange processing or acquiring global locks over all canonical event state.

### 18.7 Snapshot Consistency

Independent metric counters may be sampled at slightly different moments.

This is acceptable for diagnostics.

Authoritative states such as:

    BookSynchronized

    RecordingCommittedCursor

    MergeFinalizedFrontier

must be obtained from their owning components.

### 18.8 Expensive Metrics

Potentially expensive measurements include:

    Full Book Traversal

    Exact Global Memory Enumeration

    Detailed Per-Event Timing

    Large Histogram Merging

    Deep Dependency Inspection

These should be sampled, cached, or computed on demand.

For example, obtaining the current best bid and ask from an ordered BookStore is inexpensive, while repeatedly copying all price levels for monitoring is not.

### 18.9 Sampling Configuration

The configuration system may expose:

    MetricsEnabled

    MetricsSnapshotInterval

    DetailedTimingEnabled

    TimingSampleRate

    MetricsHistoryCapacity

Hot-editable settings must follow the rules established in Document 03.

### 18.10 Metrics Disabled

Disabling optional metrics must not disable authoritative integrity or lifecycle state.

For example:

    DetailedTimingEnabled = false

must not disable:

    Trade Continuity Validation

    L2 Sequence Validation

    Recovery State Tracking

    Recording Commit Tracking

The monitoring subsystem may reduce diagnostic measurement overhead without weakening correctness.

### 18.11 Monitoring Failure

If snapshot generation or metric aggregation fails:

- Report a monitoring failure.
- Preserve authoritative component operation.
- Avoid uncontrolled retry loops.
- Prevent stale snapshots from appearing current.
- Recover diagnostic functionality when possible.

### 18.12 Collection Performance

Benchmark metric overhead with:

    Metrics Disabled

    Essential Metrics Enabled

    Detailed Timing Enabled

Measure:

    Event Processing Throughput

    Processing Latency

    CPU Utilization

    Memory Usage

    Atomic Contention

    Snapshot Generation Duration

The selected implementation must demonstrate acceptable overhead under representative high-volume workloads.

### 18.13 Final Collection Invariant

    Metrics Must Observe
    MarketForge Live
    Without Becoming
    A Bottleneck For
    MarketForge Live.

---

## 19. Health State Model

MarketForge Live uses a unified health model to describe the operational condition of runtime components and logical streams.

Health must remain separate from lifecycle state, desired configuration state, and individual metrics.

### 19.1 Health Objectives

The health system must answer:

- Is the component operating according to its configured contract?
- Are its required dependencies available?
- Is canonical event integrity established?
- Is recovery currently required?
- Is output delivery functioning?
- Are relevant observations sufficiently recent?
- Are any required capabilities degraded?
- Is the component intentionally disabled?

Health assessments must identify the reason for degradation.

### 19.2 HealthStatus

The initial health classification is:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

These are operational assessments, not lifecycle states.

### 19.3 Healthy

`Healthy` means the component currently satisfies its applicable operational requirements.

For a canonical trade stream, this may require:

    Transport Available

    Subscription Active

    Trade Processing Operational

    Required Continuity Established

    Publication Operational

A stream does not need PostgreSQL recording to be healthy unless recording is required by its effective policy.

### 19.4 Degraded

`Degraded` means the component remains partially operational but cannot satisfy one or more applicable requirements.

Examples:

- A required exchange connection is reconnecting.
- Trade continuity is temporarily unverified.
- A derived stream has reduced source participation.
- A conversion reference is stale.
- Recording is lagging but retained recovery remains available.
- Publisher queues are approaching capacity.

Degraded operation must remain distinguishable from complete unavailability.

### 19.5 Unhealthy

`Unhealthy` means the component cannot satisfy an essential operational requirement.

Examples:

- An active required stream has permanently failed.
- The L2 book is invalid and no usable synchronized state exists.
- Strict derived ordering has been violated.
- Required recording has an unrecoverable persistence gap.
- Publication admission is unavailable for a required output.

An unhealthy state does not necessarily imply that the entire MarketForge process must stop.

### 19.6 Unknown

`Unknown` means there is insufficient valid evidence to establish health.

Examples:

- A stream has just started and has not completed initialization.
- Required health observations are missing.
- A monitoring snapshot has expired.
- A dependency state cannot be obtained.

`Unknown` must not be silently treated as `Healthy`.

### 19.7 Disabled

`Disabled` means the component is intentionally not operating.

Examples:

    Recording Disabled

    Derived Stream Disabled

    Optional Metrics Disabled

Disabled components are not automatically failures.

A disabled component required by an active dependent stream may nevertheless make that dependent stream unhealthy.

### 19.8 Lifecycle vs Health

Lifecycle and health are independent operational dimensions.

Lifecycle describes the execution state of a resource.

Health describes whether the resource currently satisfies
its applicable operational guarantees.

A logical source-stream runtime uses the authoritative
`StreamLifecycle` defined in Document 02, Section 7.2.

A physical connection uses the authoritative
`ConnectionState` defined in Document 02, Section 7.3.

Recovery progress is represented independently through
the recovery contracts.

#### Logical Runtime and Health

For example:

    StreamLifecycle: Active
    AcquisitionReady: false
    Health:           Degraded

This is valid when the logical runtime is running
but its acquisition requirements are temporarily
unavailable during recovery.

Another example:

    StreamLifecycle: Active
    ConnectionState: Connected
    IntegrityState:  Invalid
    Health:          Unhealthy

This represents a running logical source-stream runtime
whose market-data integrity requirements are not satisfied.

`StreamLifecycle::Active` must not be interpreted as proof
that the stream is healthy or acquisition-ready.

#### Connection Lifecycle and Health

A physical connection may report:

    ConnectionState:  Connected
    ConnectionHealth: Degraded

This indicates that the transport remains established
while its health measurements exceed configured thresholds.

Connection health degradation must not independently
introduce a `ConnectionState::Degraded` lifecycle transition.

#### Recovery Independence

Recovery progress is not a logical source-stream
lifecycle state.

For example:

    StreamLifecycle: Active
    RecoveryState:   Recovering
    Health:          Degraded

The HealthEvaluator uses authoritative recovery
observations when assessing currently available
service guarantees.

It must not independently initiate recovery,
modify logical lifecycle state, or declare
market-data integrity restored.

#### Health Invariants

- Lifecycle state is owned by the responsible
  runtime subsystem.
- Health is derived from authoritative state
  and diagnostic measurements.
- Acquisition readiness is evaluated separately.
- Recovery progress does not replace lifecycle state.
- Historical coverage is independent of current readiness.
- An active runtime may be unhealthy.
- A connected transport may have degraded health.
- A completed recovery operation does not automatically
  imply complete historical coverage.

Health evaluation must not mutate the authoritative
state dimensions it observes.

### 19.9 Desired State vs Actual State

The system must distinguish persistent desired configuration,
effective acquisition demand, logical runtime lifecycle,
acquisition readiness, and health assessment.

These dimensions are not interchangeable.

#### Persistent Desired State

Persistent desired configuration expresses whether
the configured source independently requests acquisition.

Conceptually:

    DesiredState:
        Active
        Inactive
        Removed

The Configuration Manager owns the authoritative
persisted configuration.

The Stream Registry maintains the corresponding
runtime registration and demand ownership.

`DesiredState::Inactive` means persistent configuration
does not independently require acquisition.

It does not prevent other valid demand owners from
requiring the same logical source.

#### Effective Acquisition Demand

Effective acquisition demand is derived from all
current valid demand registrations.

For example:

    Persistent Desired State: Inactive

    Recording Demand:         Present
    Subscriber Demand:        Present

    Effective Acquisition:    Required

The source remains required even though persistent
acquisition has been disabled.

The Stream Registry is authoritative for this
demand-ownership calculation.

#### Actual Runtime State

Actual runtime state is represented through independent
authoritative dimensions.

For example:

    Persistent Desired State: Inactive
    Effective Acquisition:    Required

    StreamLifecycle:          Active
    ConnectionState:          Reconnecting
    SubscriptionState:        Pending
    RecoveryState:            Recovering
    AcquisitionReadiness:     Not Ready

    HealthStatus:             Degraded

This is a valid state.

The logical runtime remains active because independent
demand owners still require acquisition.

Connection recovery and subscription restoration do
not create new logical lifecycle states.

#### Health Evaluation

The HealthEvaluator consumes authoritative operational
observations and relevant diagnostic measurements.

It must not infer actual acquisition demand solely
from persistent desired state.

It must not infer acquisition readiness solely from
`StreamLifecycle::Active`.

A source with no valid acquisition demand and an
intentionally stopped runtime must not automatically
be classified as unhealthy merely because it is
not receiving market data.

Conversely, a source required for acquisition may
be unhealthy when its configured guarantees cannot
be satisfied.

The final health classification depends on the
applicable health policies and available evidence.

#### Reporting Invariant

The following must remain independently observable:

- Persistent desired configuration.
- Effective acquisition demand.
- Logical source-stream lifecycle.
- Physical connection lifecycle.
- Native subscription state.
- Market-data integrity.
- Recovery progress.
- Acquisition readiness.
- Historical coverage.
- Health assessment.

No monitoring component may collapse these dimensions
into one ambiguous lifecycle or health value.

### 19.10 Health Reason

Every nonhealthy assessment should include a structured reason.

Conceptually:

    HealthReason {
        category

        component_id

        description

        first_observed_at
        last_observed_at

        recovery_reference
    }

The reason must be suitable for runtime API consumers.

Human-readable descriptions are supplementary.

### 19.11 Health Evaluation Timestamp

Every health assessment must identify when it was evaluated.

Conceptually:

    EvaluatedAt

A previously healthy assessment must not remain indefinitely authoritative after its supporting evidence expires.

### 19.12 Health Invariant

    Health Is An Assessment
    Of Applicable Operational
    Guarantees.

    It Is Not A Direct
    Translation Of One Metric.

---

## 20. HealthEvaluator

The HealthEvaluator combines authoritative component state, dependency requirements, and diagnostic measurements into structured health assessments.

It does not implement independent market-data integrity algorithms.

### 20.1 Responsibilities

The HealthEvaluator must:

- Evaluate component health.
- Evaluate source-stream health.
- Evaluate derived-stream health.
- Apply configured health policies.
- Interpret authoritative dependency states.
- Identify degraded requirements.
- Detect expired health observations.
- Produce bounded health snapshots.
- Expose structured health reasons.
- Preserve health evaluation revisions.
- Support runtime API and TUI consumers.

The HealthEvaluator must not:

- Reconstruct native trade continuity.
- Validate exchange sequences independently.
- Mutate BookStore state.
- Initiate exchange reconnections.
- Execute recording recovery.
- Modify source subscription configuration.
- Treat metrics as authoritative state transitions.

### 20.2 Logical Architecture

    Authoritative Components
              |
       +------+------+
       |      |      |
       v      v      v
    State  Metrics  Dependencies
       |      |      |
       +------+------+
              |
              v
       HealthEvaluator
              |
              v
       HealthSnapshot
              |
       +------+------+
       |             |
       v             v
    Runtime API    Ratatui TUI

Health evaluation should normally occur periodically or when relevant authoritative state changes.

It must not require evaluating the entire dependency graph for every market-data event.

### 20.3 HealthEvaluator Structure

Conceptually:

    HealthEvaluator {
        component_registry

        health_policies
        dependency_graph

        latest_component_states
        latest_metric_snapshots

        health_snapshot
        evaluation_revision
    }

The implementation may distribute state across existing registries.

This structure does not require introducing another authoritative stream registry.

### 20.4 HealthPolicy

Conceptual representation:

    HealthPolicy {
        required_capabilities

        degraded_thresholds
        unhealthy_thresholds

        observation_freshness

        recovery_behavior
        stabilization_policy

        policy_revision
    }

Thresholds may vary by component or stream type.

### 20.5 Authoritative Inputs

Health inputs include:

    ConnectionLifecycleState

    SubscriptionState

    TradeContinuityState

    BookSynchronizationState

    PublisherState

    RetentionCoverageState

    MergeOrderingState

    ConversionReferenceState

    RecordingDurabilityState

    RecoveryOperationState

These states must originate from their owning components.

### 20.6 Diagnostic Inputs

Metrics may supplement authoritative state.

Examples:

    QueueOccupancy

    ProcessingLatency

    MessageAge

    WatermarkStallDuration

    RecordingBacklog

    RepeatedFailureCount

These measurements help identify degradation before a hard failure occurs.

### 20.7 Evaluation Frequency

The initial implementation should support:

    Periodic Evaluation

    Significant State-Change Evaluation

Frequent canonical market-data events should not trigger complete health reevaluation.

### 20.8 Health Snapshot

Conceptually:

    HealthSnapshot {
        evaluation_id

        evaluated_at
        policy_revision

        runtime_health

        connection_health
        source_stream_health
        derived_stream_health

        publisher_health
        retention_health
        recording_health

        active_recovery_summary
    }

The snapshot must remain bounded.

### 20.9 Health Evaluation Consistency

The evaluator may receive component observations collected at slightly different times.

It must preserve:

    ObservationTimestamp

    ComponentGeneration

    StateRevision

where relevant.

It must not combine an obsolete connection state with a current stream generation without validation.

### 20.10 HealthEvaluator Invariant

    The HealthEvaluator
    Interprets Authoritative State.

    It Does Not Become
    The Authority For
    Market-Data Correctness.

---

## 21. Source Stream Health

Source-stream health combines transport, subscription, canonical processing, integrity, and publication requirements.

Different stream types have different validity conditions.

### 21.1 SourceStreamHealth

Source-stream health combines authoritative runtime observations
with the operational measurements required to evaluate the
source's configured service guarantees.

The HealthEvaluator does not own logical source identity,
acquisition demand, connection state, subscription state,
market-data integrity, or recovery state.

It consumes these observations from their authoritative owners.

#### Conceptual Health Snapshot

    SourceStreamHealth {
        stream_id
        source_stream_key

        persistent_desired_state
        effective_acquisition_required

        stream_lifecycle
        acquisition_readiness

        connection_state
        subscription_state
        integrity_state
        recovery_state
        coverage_state

        transport_health
        subscription_health

        processing_health
        integrity_health

        publication_health

        optional_recording_health
        optional_retention_health

        overall_health

        reasons
        evaluated_at
    }

This representation is conceptual.

The final snapshot schema, field types, and serialization
contracts belong to the shared domain and metrics contracts.

Not every health dimension applies to every source stream.

Unavailable observations must remain distinguishable
from healthy, unhealthy, or zero-valued observations.

#### Source Identity

`stream_id` identifies the registered logical source.

`source_stream_key` identifies its canonical acquisition
semantics.

Both are supplied by the authoritative Stream Registry.

A temporary WebSocket disconnection, native subscription
restoration, or L2 resynchronization must not change
the source's logical identity.

#### Persistent Desired State

`persistent_desired_state` represents the accepted
persistent acquisition intent.

Its authoritative configuration is owned by the
Configuration Manager.

The Stream Registry maintains the corresponding
runtime registration and persistent demand ownership.

A persistent desired state of `Inactive` does not
necessarily mean the physical acquisition is stopped.

Other valid demand owners may still require the source.

#### Effective Acquisition Demand

`effective_acquisition_required` is derived from the
valid demand registrations maintained by the
Stream Registry.

For example:

    Persistent Desired State: Inactive

    Recording Demand:         Present
    Subscriber Demand:        Present

    Effective Acquisition:    Required

The HealthEvaluator must evaluate the source against
its actual required service guarantees.

It must not infer effective acquisition demand solely
from persistent desired configuration.

A source intentionally stopped because no valid
acquisition demand remains must not automatically
be classified as unhealthy for failing to receive
market-data events.

#### Logical Runtime Lifecycle

`stream_lifecycle` uses the authoritative
`StreamLifecycle` contract defined in
Document 02, Section 7.2.

For example:

    StreamLifecycle: Active

This means the logical runtime is executing.

It does not imply that the native subscription is
established or that market data is ready.

The Stream Registry remains authoritative for
logical lifecycle transitions.

The HealthEvaluator must not introduce additional
logical lifecycle variants.

#### Acquisition Readiness

`acquisition_readiness` represents whether the
source currently satisfies its configured
channel-specific readiness requirements.

Readiness must be evaluated from authoritative
connection, subscription, processor-initialization,
and integrity observations.

A source may report:

    StreamLifecycle:      Active
    ConnectionState:      Reconnecting
    SubscriptionState:    Pending
    AcquisitionReadiness: Not Ready

This is valid while its logical runtime coordinates
recovery.

For L2 streams, acquisition readiness requires
successful synchronization according to the
configured snapshot and sequence-integrity contract.

Current acquisition readiness does not independently
establish historical coverage completeness.

#### Authoritative State Sources

| Observation | Authoritative Owner |
|---|---|
| Logical source identity | Stream Registry |
| Persistent desired configuration | Configuration Manager |
| Effective acquisition demand | Stream Registry |
| Logical runtime lifecycle | Stream Registry |
| Physical connection lifecycle | Connection Manager |
| Native subscription lifecycle | Subscription Manager |
| Market-data integrity | Stream Processor / Integrity subsystem |
| Recovery-operation state | Recovery Coordinator |
| Historical coverage | Recovery / Retention subsystems |
| Health classification | Metrics and Stream Health subsystem |

The HealthEvaluator consumes these observations
without independently mutating their authoritative
states.

#### Generation and Revision Context

Health snapshots must carry sufficient generation
and revision context to distinguish current
observations from obsolete asynchronous results.

Relevant identifiers may include:

    ConnectionGeneration
    SubscriptionGeneration
    BookGeneration
    ConfigRevisionId
    LogEpoch

These identifiers represent different domains.

They must not be replaced by one ambiguous
`stream_generation` field.

In particular:

- `ConnectionGeneration` identifies the physical
  connection incarnation.
- `SubscriptionGeneration` identifies the native
  subscription incarnation.
- `BookGeneration` identifies the authoritative
  L2 synchronization incarnation.
- `ConfigRevisionId` identifies an accepted
  desired configuration revision.
- `LogEpoch` identifies a MarketForge cursor
  namespace.

Only applicable identifiers need to appear
in a particular source-health snapshot.

The exact generation and revision fields included
in the final serialized health schema must be
established by the shared metrics contract.

#### Health Evaluation

The HealthEvaluator combines applicable component
health assessments according to the configured
service requirements.

For example:

    Persistent Desired State: Inactive
    Effective Acquisition:    Required

    StreamLifecycle:          Active
    ConnectionState:          Connected
    SubscriptionState:        Active
    IntegrityState:           Synchronizing

    AcquisitionReadiness:     Not Ready
    OverallHealth:           Degraded

This is a possible assessment for a required L2
source whose runtime and subscription are active
but whose order book is still synchronizing.

The exact health classification depends on the
configured health policy.

The HealthEvaluator must not automatically classify
every synchronizing or recovering source as
`Degraded` when a different classification is
required by its service guarantees.

#### Recording and Retention

Recording and retention health are included only
when their corresponding guarantees apply.

A source with optional recording disabled must
not become unhealthy merely because PostgreSQL
recording is unavailable.

Conversely, a required recording or retention
obligation that cannot be satisfied must affect
the applicable health assessment.

The HealthEvaluator must preserve the distinction
between acquisition readiness and downstream
delivery or durability guarantees.

#### Snapshot Consistency

Each health snapshot must include an observation
timestamp.

Component observations may originate from
different collection instants.

The evaluator must preserve relevant generation
and revision context and reject or explicitly
qualify stale observations.

A health snapshot must not claim atomic consistency
across independent components unless that guarantee
is explicitly implemented.

#### Source Health Invariants

- Logical lifecycle is independent of health.
- Connection lifecycle is independent of connection health.
- Persistent desired state is independent of effective demand.
- Effective demand determines which acquisition guarantees
  are currently required.
- Acquisition readiness is independent of historical coverage.
- Current market-data integrity remains authoritative
  in the responsible processor.
- Recovery progress remains authoritative in the
  Recovery Coordinator.
- Recording and retention guarantees remain separate
  from source acquisition.
- Health evaluation must not mutate canonical
  market-data processing state.
- Obsolete generations must not overwrite current
  authoritative observations.
### 21.2 Transport Health

Transport health reflects the connection required by the source stream.

Possible assessments:

    Healthy:
        Required connection operational.

    Degraded:
        Reconnecting or temporarily impaired.

    Unhealthy:
        Required connection unavailable
        without an active recoverable path.

    Unknown:
        Insufficient valid connection evidence.

### 21.3 Subscription Health

A transport connection may remain active while a particular subscription fails.

For example:

    WebSocket:
        Connected

    BTCUSDT Trades Subscription:
        Active

    ETHUSDT Trades Subscription:
        Invalid

The affected streams must receive independent subscription-health assessments.

### 21.4 Trade Integrity Health

Trade health must use the authoritative Trade Processor continuity state.

Conceptually:

    Verified Continuity
        -> Healthy

    Temporarily Unverified
        -> Unknown or Degraded

    Recovery In Progress
        -> Degraded

    Confirmed Unresolved Gap
        -> Degraded or Unhealthy

The precise mapping depends on the stream's required coverage contract.

### 21.5 Trade Recovery Health

A trade stream may continue accepting new executions while older missing executions are being recovered.

Example:

    Current Acquisition:
        Active

    Historical Trade Gap:
        Recovery In Progress

The stream should not be reported as fully healthy when complete trade coverage is required.

### 21.6 L2 Synchronization Health

L2 health must use the authoritative BookStore synchronization controller.

Conceptually:

    Synchronized
        -> Eligible for Healthy

    Synchronizing
        -> Unknown or Degraded

    Invalid
        -> Unhealthy for book-state consumers

    Recovering
        -> Degraded or Unhealthy,
           depending on required guarantees

A nonempty order book must not be treated as synchronized after an invalidating sequence gap.

### 21.7 L2 Generation Health

A new book generation may require:

- An authoritative initialization.
- Native sequence validation.
- Snapshot-to-delta synchronization.
- Current source readiness.

The HealthEvaluator must not declare the generation healthy before the synchronization controller confirms readiness.

### 21.8 Publication Health

Publication health reflects whether canonical events can be admitted to the required distribution infrastructure.

Individual subscriber disconnections do not necessarily invalidate the source stream.

The source stream becomes impaired when its required publication contract cannot be satisfied.

### 21.9 Optional Recording

If PostgreSQL recording is optional:

    Recording = Disabled

does not make the source unhealthy.

If recording is required for a configured durability or bootstrap guarantee:

    Recording = Unavailable

must degrade the applicable stream or guarantee health.

### 21.10 Optional Retention

Retention health is evaluated similarly.

A stream may operate without retention when no active consumer requires retained coverage.

However, failure to satisfy an active bootstrap protection must affect that bootstrap guarantee.

### 21.11 Source Stream Invariant

    A Source Stream Is Healthy
    Only When Its Required
    Processing And Delivery
    Contracts Are Satisfied.

---

## 22. Derived Stream Health

Derived streams depend on upstream canonical or derived streams.

Their health must reflect source participation, ordering, conversion, and publication requirements.

### 22.1 DerivedStreamHealth

Conceptually:

    DerivedStreamHealth {
        derived_stream_id
        runtime_generation

        lifecycle_state

        source_participation_health
        ordering_health

        conversion_health
        publication_health

        retention_health
        recording_health

        overall_health

        reasons
        evaluated_at
    }

### 22.2 Dependency Health

A derived stream may require several source streams.

Dependency availability must be evaluated against the specific
capabilities required by the derived-stream transformation.

The HealthEvaluator must not infer dependency availability solely
from `StreamLifecycle::Active`.

A source may have an operational logical runtime while its
connection is recovering, its native subscription is unavailable,
or its market-data integrity is invalid.

For example:

    SourceStream:
        StreamLifecycle:     Active
        ConnectionState:     Connected
        SubscriptionState:   Active
        IntegrityState:      Synchronizing
        AcquisitionReady:    false

    Dependent L2 Processor:
        Required Capability: BookSynchronization
        Dependency Available: false

The logical source runtime remains operational, but it does
not currently satisfy the dependent processor's requirements.

Conversely, an intentionally inactive persistent source
may remain available to dependent consumers when independent
acquisition demand keeps the source running and ready.

Persistent desired state must not be used as a substitute
for effective acquisition demand or actual source readiness.

The dependency manager remains authoritative for dependency
relationships and ownership.

The HealthEvaluator assesses whether the required capabilities
are currently satisfied.

Health evaluation must not independently activate sources,
release demand registrations, or mutate dependency relationships.

### 22.3 Strict Participation

Under strict participation:

    All Required Sources
    Must Be Available

A required source failure prevents full health.

Example:

    RequiredSources = 3

    HealthySources = 2

    UnavailableSources = 1

Overall health cannot be `Healthy`.

### 22.4 Degraded Participation

A derived stream configured to continue with partial input may remain operational.

However:

    Partial Input Participation

must remain visible.

The HealthEvaluator must distinguish:

    ProcessingOperational

from:

    CompleteSourceCoverage

### 22.5 Merge Ordering Health

Merge ordering health originates from the Merge Engine.

Possible assessments:

    Healthy:
        Ordering contract satisfied.

    Degraded:
        Watermark stalled or source
        participation incomplete.

    Unhealthy:
        Required strict ordering violated.

    Unknown:
        Insufficient ordering evidence.

The HealthEvaluator must not independently reorder events to determine correctness.

### 22.6 Watermark Stalls

A watermark stall may be caused by a required source that is idle, disconnected, or recovering.

The HealthEvaluator may consider:

    WatermarkStallDuration

    BufferedEventCount

    BufferOccupancy

    SourceParticipationState

A watermark stall does not automatically mean data loss.

Its significance depends on the configured ordering and liveness policies.

### 22.7 Late Events

Late events may be expected under a bounded-lateness policy.

Their presence does not automatically imply an unhealthy stream.

However, when the selected contract requires strict ordering, a late event that violates the finalized frontier must impair ordering health.

### 22.8 Conversion Health

Currency conversion health depends on:

- Required reference availability.
- Reference staleness.
- Conversion path validity.
- Numerical conversion success.
- Transformation revision compatibility.

A stream can produce valid merged source events while converted target values remain unavailable.

These conditions must be reported independently.

### 22.9 Conversion Status vs Conversion Health

Historical-compatible conversion statuses are:

    Identity

    Converted

    Unavailable

    Skipped

These are event-level outcomes.

They are not identical to runtime health states.

For example, one unavailable conversion event does not automatically make the entire conversion runtime unhealthy.

Health depends on the configured availability requirements and observed failure pattern.

### 22.10 Derived Publication Health

A derived stream may produce finalized events while publication admission is impaired.

The HealthEvaluator must distinguish:

    Merge Output Ready

from:

    Publisher Admission Successful

A stalled Publisher may cause the derived runtime to become degraded or unhealthy.

### 22.11 Derived Stream Invariant

    Derived Health
    Must Reflect Required
    Input Participation,
    Transformation Correctness,
    And Output Availability.

---

## 23. Dependency-Aware Health Evaluation

MarketForge components form dependency relationships.

A component's health must account for the capabilities it requires from its dependencies.

### 23.1 Dependency Principle

Consider:

    Binance Connection
            |
            v
    BTCUSDT Trade Stream
            |
            v
    Combined BTC Trades
            |
            v
    Converted BTC Trades
            |
            v
    Subscriber

A connection failure may affect several downstream components.

The HealthEvaluator must preserve the causal relationship.

### 23.2 HealthDependency

Conceptually:

    HealthDependency {
        dependent_component

        dependency_component

        required_capability

        requirement_policy
    }

Dependencies should reuse the authoritative Stream Registry and combined-stream dependency graph where possible.

### 23.3 Capability-Based Requirements

A component may require only specific capabilities from a dependency.

Capability availability must be evaluated using authoritative
operational observations and the dependent component's declared
requirements.

The logical lifecycle of a dependency is not sufficient
to establish capability availability.

In particular:

- `StreamLifecycle::Active` confirms that a logical runtime
  is executing, not that its market data is ready.
- `ConnectionState::Connected` confirms physical connectivity,
  not successful native subscription or valid market data.
- `SubscriptionState::Active` confirms native subscription
  establishment, not synchronized L2 integrity.
- `IntegrityState::Valid` establishes the applicable current
  integrity condition, not complete historical coverage.
- `RecoveryState::Recovering` describes recovery progress,
  not a logical source-stream lifecycle transition.

Dependencies must be evaluated according to the exact
capabilities required by their consumers.

Examples include:

- A live trade merger requiring current trade acquisition readiness.
- An L2-dependent processor requiring synchronized book state.
- A historical continuity consumer requiring complete coverage
  over a specified interval.
- A recording consumer requiring the configured database
  commitment guarantee.
- A currency-conversion processor requiring an eligible,
  sufficiently fresh reference observation.

A dependency may satisfy one capability while failing another.

For example, current trade acquisition may be ready while
historical coverage remains incomplete.

The HealthEvaluator must preserve this distinction rather
than assigning all downstream consumers the same status.

Health assessment must not independently determine canonical
market-data validity or alter the authoritative dependency graph.

### 23.4 Example: Optional Recording Failure

Suppose:

    Binance Trades:
        Canonical Processing Healthy

    PostgreSQL:
        Unavailable

A subscriber requiring only live trade publication may remain healthy.

A bootstrap consumer requiring durable recorded history may be degraded.

The same PostgreSQL failure affects different consumers differently.

### 23.5 Example: BookStore Failure

Suppose:

    BTCUSDT BookStore:
        Invalid

A derived stream requiring synchronized L2 state cannot be healthy.

However, a trade-only derived stream using the same exchange connection may remain healthy.

Health propagation must follow actual capabilities, not merely shared exchange identity.

### 23.6 Example: Conversion Failure

Suppose:

    BTCUSDT Trades:
        Healthy

    USDTUSD Reference:
        Stale

    Converted BTC/USD:
        Degraded

    Original BTCUSDT Trades:
        Healthy

The conversion failure must not retroactively invalidate the original trade stream.

### 23.7 Dependency Health Aggregation

A dependent component evaluates its required capabilities.

Conceptually:

    OverallHealth =
        Evaluate(
            LocalHealth,
            RequiredDependencies,
            EffectivePolicy
        )

This is a policy evaluation.

It is not necessarily the numeric minimum of all dependency statuses.

### 23.8 Dependency Cycles

The authoritative combined-stream dependency graph is acyclic.

Health evaluation should follow that graph.

If the control-plane dependency model permits other relationships, they must not introduce uncontrolled recursive evaluation.

### 23.9 Shared Dependency Failure

One failed shared connection may affect many streams.

The health system should retain the shared root cause.

Example:

    Binance WebSocket Disconnected
        |
        +-- BTCUSDT Trades Degraded
        |
        +-- ETHUSDT Trades Degraded
        |
        +-- SOLUSDT Trades Degraded

The runtime API should permit inspection of the common connection failure.

The system must not manufacture three unrelated transport failures.

### 23.10 Root-Cause Visibility

A health reason may reference another component's health reason.

Conceptually:

    Derived Stream Degraded
        |
        v
    Source Stream Unavailable
        |
        v
    Native Subscription Failed
        |
        v
    Exchange Authentication Failure

The TUI may present this dependency chain.

The HealthEvaluator must avoid storing unbounded copies of repeated descriptions.

### 23.11 Health Propagation Invariant

    Health Propagates
    Through Required Capabilities,
    Not Through Unrelated
    Component Associations.

---

## 24. Stale Health, Recovery, and Stabilization

Health observations can become stale, and components may transition rapidly between healthy and degraded states.

The HealthEvaluator must handle these conditions without hiding failures or creating misleading state changes.

### 24.1 Observation Freshness

Each authoritative observation should identify:

    ObservedAt

    ComponentGeneration

    StateRevision

where available.

The HealthEvaluator evaluates whether the observation remains valid for the current component generation.

### 24.2 Stale Metrics

A metric snapshot may become stale because:

- Snapshot collection stopped.
- The Metrics Registry is overloaded.
- A component stopped reporting.
- The monitoring task failed.

Stale metric data must not continue to appear current.

### 24.3 Stale Authoritative State

The meaning of an expired observation depends on its owner.

For example:

    Last Connection Observation:
        Connected

    Observation Age:
        Exceeds Freshness Limit

The HealthEvaluator must not automatically assume the connection remains healthy.

If an authoritative component can provide a current synchronized state, that state should take precedence over stale metric snapshots.

### 24.4 Unknown on Insufficient Evidence

When required evidence is unavailable:

    HealthStatus = Unknown

unless an independently established failure already justifies `Unhealthy`.

The system must not downgrade a confirmed unresolved integrity failure to `Unknown` merely because its monitoring snapshot expired.

### 24.5 Recovery State

Recovery state is independent of logical source-stream
lifecycle and physical connection lifecycle.

During recovery, health reflects the guarantees
currently available.

For example:

    StreamLifecycle: Active

    ConnectionState: Connected

    RecoveryState: Recovering

    TradeContinuity: Degraded

    AcquisitionReadiness: Not Ready

    OverallHealth: Degraded

This represents an operational logical runtime
whose required trade-continuity guarantees are
temporarily impaired.

Recovery progress must not be represented as
`StreamLifecycle::Recovering`.

A recovered physical connection does not automatically
restore native subscription state or complete trade
continuity.

Likewise, successful current acquisition recovery
does not imply complete historical market-data coverage.

The HealthEvaluator must consume authoritative
recovery observations from the Recovery Coordinator.

It must not independently mark recovery completed,
restore integrity state, or modify the logical runtime
lifecycle.

Health classifications must reflect current evidence
and the configured health policy.

A recovery operation may complete while unresolved
historical coverage gaps remain visible.

### 24.6 Recovery Completion

The HealthEvaluator must consume authoritative recovery completion from the Recovery Coordinator and affected component.

It must not infer recovery success solely from falling error rates.

### 24.7 Stabilization

Some diagnostic conditions fluctuate around configured thresholds.

Examples:

    Queue Occupancy

    Processing Latency

    Recording Lag

The HealthEvaluator may apply hysteresis or stabilization windows to these threshold-based assessments.

### 24.8 Hysteresis

Conceptually:

    Enter Degraded:
        Queue Occupancy >= 85%

    Return Healthy:
        Queue Occupancy <= 65%

The values are illustrative.

Actual thresholds must be configurable and based on measured runtime capacity.

### 24.9 Immediate Failures

Hysteresis must not delay recognition of confirmed correctness failures.

Examples:

    L2 Sequence Integrity Invalidated

    Strict Merge Ordering Violated

    Required Recording Gap Unrecoverable

Such authoritative failures must be reflected immediately or at the next required state-publication boundary.

### 24.10 Health Recovery Stabilization

After a transient resource-pressure condition clears, a short stabilization period may prevent excessive status flapping.

However, successful authoritative recovery operations must not be hidden behind unrelated diagnostic thresholds.

### 24.11 Health Transition

Conceptual representation:

    HealthTransition {
        component_id

        previous_status
        new_status

        reason

        observed_at

        component_generation
        policy_revision
    }

Transitions may be retained in bounded local diagnostic history.

They are not stored in PostgreSQL.

### 24.12 Health Transition Notifications

The runtime may publish health transitions through its control-plane notification interface.

Examples:

    StreamDegraded

    StreamRecovered

    BookSynchronizationLost

    RecordingUnavailable

    MergeOrderingImpaired

These are operational notifications, not canonical market-data events.

### 24.13 Health Flapping

The HealthEvaluator may detect repeated transitions between healthy and degraded states.

Relevant measurements include:

    HealthTransitionsTotal

    HealthTransitionsRecentWindow

    RepeatedDegradationCount

The implementation must avoid unbounded event history.

### 24.14 Disabled Components

An intentionally disabled component remains:

    Disabled

unless the lifecycle itself fails to honor the disable request.

If another active component requires the disabled capability, that dependent component is evaluated separately.

### 25.15 Prometheus Exposition Endpoint

The Prometheus endpoint is a separate read-only
metrics interface.

It does not replace the versioned Runtime Monitoring API.

Conceptual endpoint:

    GET /metrics

Default listener:

    127.0.0.1:9091

The endpoint exposes Prometheus-compatible metrics
using the standard text exposition contract.

Unlike the Runtime Monitoring API, it does not expose
full structured health snapshots, stream definitions,
or management commands.

The endpoint must not support:

    AddStream

    RemoveStream

    UpdateConfiguration

    RestartConnection

    EnableRecording

    DisableRecording

    TriggerRecovery

All runtime mutations remain exclusively available
through the validated Runtime Control API.

The endpoint must remain unavailable when disabled.

Prometheus and Grafana remain external applications.

The TUI must continue operating without them.
---

## 25. Runtime Monitoring API

The Runtime API provides a structured interface for inspecting metrics, health, and operational state.

It is a consumer of the Metrics Registry, HealthEvaluator, and authoritative runtime components.

The API must not introduce an independent monitoring state machine.

### 25.1 Responsibilities

The monitoring API must support:

- Global runtime metrics.
- Global runtime health.
- Exchange connectivity inspection.
- Canonical source-stream metrics.
- Derived-stream metrics.
- Per-stream health assessments.
- Dependency health inspection.
- Publisher and subscriber diagnostics.
- Live Retention health.
- PostgreSQL recording health.
- Recovery operation summaries.
- Resource utilization.
- Recent metric snapshots.

The API must preserve metric units, identity scopes, and observation timestamps.

### 25.2 Architectural Position

    Authoritative Runtime Components
                  |
         +--------+--------+
         |                 |
         v                 v
    Metrics Registry   HealthEvaluator
         |                 |
         +--------+--------+
                  |
                  v
          Monitoring Read API
                  |
           +------+------+
           |             |
           v             v
       Ratatui TUI   External Clients

The monitoring API is logically separate from the canonical market-data Publisher.

It may use a separate Unix socket endpoint or another lightweight local transport defined by the Runtime Control API.

### 25.3 Read Operations

Recommended initial operations:

    GetRuntimeMetrics

    GetRuntimeHealth

    ListConnections

    GetConnectionMetrics

    GetConnectionHealth

    ListSourceStreams

    GetSourceStreamMetrics

    GetSourceStreamHealth

    ListDerivedStreams

    GetDerivedStreamMetrics

    GetDerivedStreamHealth

    GetPublisherMetrics

    ListSubscribers

    GetSubscriberMetrics

    GetRetentionMetrics

    GetRecordingMetrics

    GetRecordingHealth

    ListActiveRecoveries

    GetResourceMetrics

    GetMetricsSnapshot

    GetHealthSnapshot

These names are conceptual.

The final API contract should use one consistent naming and versioning convention.

### 25.4 API Response Envelope

Conceptual representation:

    MonitoringResponse {
        protocol_version

        request_id

        snapshot_epoch
        snapshot_id

        captured_at

        status
        data
    }

The response must identify the data snapshot or authoritative state revision used.

### 25.5 Health Response

Conceptual representation:

    StreamHealthResponse {
        stream_id

        stream_type
        stream_generation

        desired_state
        lifecycle_state

        overall_health

        dimensions
        reasons

        evaluated_at
        policy_revision
    }

The response must preserve individual health dimensions.

Consumers must not be forced to infer health reasons from a single overall status.

### 25.6 Metric Availability

Optional or unsupported measurements must remain distinguishable from valid zero values.

For example:

    {
        "exchange_to_receive_latency_ns": null
    }

may indicate that the measurement is unavailable.

It must not imply zero latency.

If necessary, the API may also expose an explicit availability or quality indicator.

### 25.7 Filtering

List operations should support bounded filtering by applicable identities.

Examples:

    Exchange

    MarketFamily

    StreamType

    LifecycleState

    HealthStatus

    RecordingEnabled

    DerivedStreamKind

Filtering should occur without requiring clients to retrieve the complete runtime state unnecessarily.

### 25.8 Pagination

Potentially large collections should support pagination.

Examples:

    RegisteredStreams

    Subscribers

    RecentHealthTransitions

    RecoveryOperations

The API must enforce maximum response sizes.

### 25.9 Monitoring Snapshot Subscription

The runtime may support a lightweight subscription to monitoring updates.

Conceptually:

    SubscribeMetricsSnapshots

    SubscribeHealthTransitions

This is optional for the initial implementation.

A polling API is sufficient for the first Ratatui integration.

### 25.10 Slow Monitoring Clients

Slow monitoring clients may skip intermediate metric snapshots.

They must not create unbounded queues.

Health transition notifications may require a separate bounded delivery policy.

Monitoring delivery guarantees must not be confused with canonical market-data delivery guarantees.

### 25.11 API Failure Isolation

A failed monitoring request must not block:

- WebSocket acquisition.
- Canonical processing.
- BookStore updates.
- Publisher admission.
- Recording transactions.
- Recovery coordination.

Expensive queries should operate on cached snapshots or bounded authoritative state reads.

### 25.12 API Versioning

Monitoring responses must identify their protocol version.

Changes to metric naming, units, status semantics, or response structure require compatible versioning.

New optional fields should not force unrelated clients to upgrade unnecessarily.

### 25.13 Access Control

The monitoring API and runtime control API have different security requirements.

Read-only monitoring may be exposed to permitted local clients.

Mutating operations require explicit authorization.

A monitoring client must not gain stream-management permissions merely because it can inspect metrics.

Unix socket permissions and client authorization must follow the runtime security contract.

### 25.14 Monitoring API Invariant

    The Monitoring API
    Exposes Shared Runtime State.

    It Does Not Become
    An Independent Authority
    For Stream Processing,
    Integrity, Or Recovery.

---

## 26. Ratatui Monitoring Integration

The Rust Ratatui interface presents monitoring information through a small number of operational views.

The TUI consumes the same monitoring and control contracts exposed to other clients.

It must not directly own processing state.

### 26.1 TUI Objectives

The initial TUI should make it easy to:

- Identify unhealthy streams.
- Inspect transport status.
- Observe throughput and latency.
- Detect trade continuity problems.
- Inspect L2 synchronization.
- Monitor derived streams.
- Check recording and retention.
- Identify resource pressure.
- Observe recovery progress.
- Execute supported stream-management commands.

The TUI should prioritize clarity over displaying every available metric simultaneously.

### 26.2 Main Views

The recommended initial layout contains:

    Dashboard

    Streams

    Merges

    Recording

    Settings

Detailed diagnostics may appear in contextual panels or modal views.

### 26.3 Dashboard

The Dashboard provides a global runtime overview.

Recommended information:

    MarketForge Live

    Runtime:
        Running

    Connections:
        12 Active

    Source Streams:
        48 Active

    Derived Streams:
        6 Active

    Accepted Events:
        85,000/s

    Overall Health:
        Degraded

    Recovery Operations:
        2 Active

    Memory:
        3.2 GiB

    Retention:
        Healthy

    PostgreSQL:
        Connected

All values shown above are illustrative.

### 26.4 Dashboard Health Summary

The Dashboard should display counts by status:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

These counts must use one consistent aggregation scope.

For example, source-stream and derived-stream totals should remain distinguishable where necessary.

### 26.5 Streams View

The Streams view lists canonical source streams.

Recommended columns:

| Column | Meaning |
|---|---|
| Exchange | Source exchange |
| Instrument | Symbol or instrument identity |
| Type | Trades or L2 |
| State | Lifecycle state |
| Health | Overall health |
| Rate | Accepted events per second |
| Latency | Selected processing-latency metric |
| Integrity | Trade or L2 integrity status |
| Recording | Recording status |

The view should support sorting and filtering.

### 26.6 Trade Stream Details

A trade stream detail panel may display:

    Exchange

    Instrument

    StreamId

    ConnectionState

    SubscriptionState

    AcceptedTradesPerSecond

    ProcessingLatency

    ContinuityStatus

    DuplicateTrades

    UnresolvedGaps

    LastAcceptedEvent

    RecoveryStatus

Only applicable metrics should be displayed.

### 26.7 L2 Stream Details

An L2 stream detail panel may display:

    Exchange

    Instrument

    StreamId

    ConnectionState

    SynchronizationState

    BookGeneration

    BidLevels

    AskLevels

    BestBid

    BestAsk

    Spread

    SequenceGaps

    LastValidUpdate

    RecoveryStatus

Best bid, best ask, and spread must not appear authoritative when the book is unsynchronized.

### 26.8 Merges View

The Merges view lists derived streams.

Recommended columns:

    Derived Stream

    Type

    Active Sources

    Health

    Input Rate

    Output Rate

    Watermark Lag

    Buffered Events

    Late Events

    Conversion Target

A detail panel may display source dependencies, ordering policy, watermark state, and conversion-reference health.

### 26.9 Recording View

The Recording view presents PostgreSQL recording status.

Recommended information:

    Database:
        Connected

    Recording:
        Active

    Streams Recording:
        24

    Events Committed:
        45,000/s

    Pending Events:
        120,000

    Oldest Pending:
        2.7 seconds

    Known Recording Gaps:
        0

    Recovery:
        Idle

    Retention Cleanup:
        Healthy

These values are illustrative.

### 26.10 Settings View

The Settings view displays effective runtime configuration.

Relevant monitoring settings include:

    Metrics Enabled

    Detailed Timing Enabled

    Metrics Snapshot Interval

    Monitoring History Capacity

Other supported hot-editable settings remain managed by the Configuration System.

The TUI must submit validated configuration commands rather than mutating shared structures directly.

### 26.11 Health Status Presentation

Health information should use both text and visual distinction.

For example:

    Healthy

    Degraded

    Unhealthy

    Unknown

    Disabled

Color may improve readability, but status interpretation must not depend on color alone.

### 26.12 Health Reasons

Selecting a degraded stream should expose its current reasons.

Example:

    BTCUSDT Trades — Degraded

    Transport:
        Healthy

    Trade Continuity:
        Unverified

    Recovery:
        Fetching missing executions

    Recording:
        Healthy

This is more informative than displaying only:

    DEGRADED

### 26.13 TUI Refresh

The TUI should refresh from monitoring snapshots at a configurable interval.

It must not request complete market-data payloads simply to update throughput or health indicators.

### 26.14 Rendering Isolation

Ratatui rendering must remain independent of canonical event processing.

A slow terminal must not block WebSocket processing.

Rendering may skip intermediate diagnostic snapshots.

### 26.15 TUI Command Integration

The TUI may initiate supported operations such as:

    AddStream

    RemoveStream

    ActivateDerivedStream

    DeactivateDerivedStream

    EnableRecording

    DisableRecording

    UpdateRuntimeSetting

These commands belong to the Runtime Control API.

The TUI does not implement their execution logic.

### 26.16 TUI Invariant

    The TUI Is A
    Presentation And
    Control Client.

    It Must Not Become
    A Market-Data
    Processing Component.

---

## 27. Metrics and Health Testing

The subsystem requires tests for metric semantics, lifecycle isolation, health evaluation, snapshot generation, and performance overhead.

### 27.1 Testing Categories

Tests should cover:

- Counter correctness.
- Gauge correctness.
- Metric identity.
- Counter generation resets.
- Snapshot generation.
- Rate calculations.
- Latency distributions.
- Missing measurements.
- Component health evaluation.
- Dependency propagation.
- Recovery transitions.
- Stale observations.
- Resource-pressure thresholds.
- Monitoring API responses.
- TUI snapshot consumption.

### 27.2 Counter Tests

Verify:

    Counter Increment

    Counter Reset Detection

    Counter Overflow Policy

    Concurrent Counter Updates

A counter must not silently wrap or produce misleading negative rates.

### 27.3 Rate Calculation Tests

Example:

    Snapshot A:
        1000 events

    Snapshot B:
        2500 events

    Elapsed:
        0.5 seconds

Expected:

    3000 events/s

The test must use the recorded elapsed interval rather than assuming a nominal snapshot interval.

### 27.4 Generation Reset Tests

Simulate:

    Stream Generation 5:
        Counter = 10000

    Stream Generation 6:
        Counter = 100

The resulting event rate must not be calculated using:

    100 - 10000

The aggregation layer must recognize the generation change.

### 27.5 Missing Measurement Tests

Verify that an unavailable metric remains unavailable.

Examples:

    Missing Exchange Timestamp

    Unsupported RTT Measurement

    Disabled Detailed Timing

The API and TUI must not substitute zero.

### 27.6 Health Classification Tests

Construct authoritative component-state fixtures.

Examples:

    Connected + Verified + Publishing
        -> Healthy

    Connected + Trade Gap Recovering
        -> Degraded

    Connected + Invalid Book
        -> Unhealthy

    Recording Disabled + Optional
        -> Source Health Unaffected

    Recording Disabled + Required
        -> Dependent Guarantee Impaired

Expected results must follow the configured health policy.

### 27.7 Dependency Tests

Test shared dependencies.

Example:

    Connection A
        |
        +-- Trade Stream 1
        |
        +-- Trade Stream 2

When Connection A fails, both dependent streams must reflect the loss of required connectivity.

A healthy unrelated stream must remain unaffected.

### 27.8 Conversion Dependency Tests

Simulate a healthy source trade stream with a stale currency reference.

Expected:

    Source Trade Health:
        Healthy

    Converted Derived Health:
        Degraded

The conversion problem must not propagate backward into source acquisition health.

### 27.9 Recovery Tests

Simulate:

    Source Degraded

    Recovery Started

    Transport Reconnected

    Source Integrity Revalidated

    Recovery Completed

Health must return to Healthy only when the required authoritative guarantees are restored.

### 27.10 Stale Observation Tests

Stop metric snapshot generation while authoritative component state continues changing.

Verify that stale snapshots are not presented as current.

Confirmed integrity failures must not disappear because monitoring data becomes stale.

### 27.11 Hysteresis Tests

For threshold-based degradation, simulate values oscillating around the configured threshold.

Verify that stabilization prevents excessive status flapping.

Confirmed correctness failures must bypass diagnostic hysteresis.

### 27.12 TUI Tests

TUI tests should validate:

- Snapshot rendering.
- Missing metric display.
- Health reason presentation.
- Stream sorting.
- Stream filtering.
- Detail panel behavior.
- Recovery-state updates.
- Stale snapshot indicators.
- Control-command dispatch.

The tests should not require active exchange connections.

### 27.13 Prometheus Exporter Tests

The optional exporter requires deterministic tests
covering:

- Disabled exporter startup.
- Valid exposition formatting.
- Metric name conversion.
- Counter serialization.
- Gauge serialization.
- Histogram serialization.
- Nanosecond-to-second unit conversion.
- Missing metric handling.
- Counter resets.
- Stream-generation changes.
- Bounded label cardinality.
- Removed-stream series cleanup.
- Concurrent scrape limits.
- Maximum response size.
- HTTP request timeouts.
- Exporter failure isolation.
- Safe listener configuration.

Tests must verify that enabling or disabling
the exporter does not change canonical
market-data processing results.

Ordinary Metrics Registry and HealthEvaluator
tests must run without Prometheus or Grafana.

---

## 28. Fault Injection and Property-Based Testing

Health correctness must remain stable under arbitrary valid sequences of failures, recoveries, and configuration changes.

### 28.1 Generated Operations

Property-based tests may generate:

    CreateConnection

    DisconnectConnection

    ReconnectConnection

    ActivateStream

    DeactivateStream

    DetectTradeGap

    RecoverTradeGap

    InvalidateBook

    InitializeBook

    SaturatePublisherQueue

    ReleasePublisherQueue

    StallMergeWatermark

    ResumeMergeWatermark

    ExpireConversionReference

    RestoreConversionReference

    DisconnectPostgreSQL

    RestorePostgreSQL

    StartRecovery

    CompleteRecovery

    ChangeHealthPolicy

    ExpireMetricSnapshot

    ReplaceRuntimeGeneration

### 28.2 Authoritative State Invariant

A health assessment must not contradict a current authoritative component state.

Example:

    BookSynchronizationState = Invalid

cannot produce:

    L2BookHealth = Healthy

when synchronized book state is required.

### 28.3 Optional Dependency Invariant

A disabled optional capability must not automatically degrade unrelated stream health.

### 28.4 Required Dependency Invariant

A missing required capability must prevent a dependent component from being classified as fully healthy.

### 28.5 Generation Invariant

An obsolete component generation must not overwrite the current generation's health state.

### 28.6 Snapshot Invariant

Diagnostic snapshots may be eventually consistent.

However, they must retain their collection timestamps and identities.

They must not be represented as globally atomic state captures.

### 28.7 Recovery Invariant

A recovery attempt must not be counted as successful merely because the affected component reconnects.

All required recovery guarantees must be established.

### 28.8 Monitoring Failure Invariant

A monitoring failure must not silently alter canonical event processing or trigger unauthorized recovery operations.

---

## 29. Monitoring Performance Requirements

Monitoring overhead must remain small relative to canonical market-data processing.

The design prioritizes predictable resource usage and bounded work.

### 29.1 Performance Objectives

The subsystem should:

- Avoid per-event heap allocation for basic counters.
- Avoid dynamic metric-name resolution on hot paths.
- Avoid blocking global registry locks for ordinary counters.
- Avoid copying canonical event payloads into snapshots.
- Avoid full health-graph evaluation for every event.
- Support bounded metric cardinality.
- Support bounded snapshot history.
- Keep TUI rendering isolated.

### 29.2 Benchmark Modes

Benchmark MarketForge Live with:

    Monitoring Disabled

    Essential Metrics Enabled

    Detailed Timing Enabled

Compare:

    Throughput

    Processing Latency

    CPU Utilization

    Memory Utilization

    Allocation Rate

    Contention

### 29.3 Hot-Path Budget

Essential metric updates should have predictable bounded execution cost.

A typical hot-path operation should require only:

    Local Counter Increment

or:

    Atomic Counter Increment

More expensive work belongs to periodic aggregation or monitoring snapshot tasks.

### 29.4 Snapshot Cost

Measure:

    SnapshotCollectionDuration

    SnapshotAllocationBytes

    SnapshotSizeBytes

    SnapshotHistoryMemory

The collection interval must be sufficient to avoid sustained overlap between expensive snapshot operations.

### 29.5 Health Evaluation Cost

Health evaluation should operate primarily on:

    Current Component State

    Latest Diagnostic Snapshot

    Validated Dependency Graph

The evaluator must not traverse complete historical event buffers.

### 29.6 TUI Rendering Cost

Measure:

    FrameRenderDuration

    FrameRefreshRate

    SnapshotConsumptionDelay

The TUI may reduce refresh frequency when rendering becomes expensive.

This must not affect canonical processing throughput.

### 29.7 API Query Cost

Monitoring API endpoints must enforce bounded response sizes and reasonable query limits.

Expensive detail requests should be separated from lightweight dashboard polling.

### 29.8 Performance Acceptance

Concrete overhead targets should be established through representative benchmarks.

The subsystem must demonstrate acceptable overhead under sustained high-volume workloads.

No arbitrary throughput or latency guarantee should be declared without measurements.

### 29.9 Performance Invariant

    Monitoring Must Remain
    Operationally Useful
    Without Becoming
    A Material Bottleneck
    For Market-Data Acquisition.

---

## 30. Final Acceptance Criteria

The Metrics and Stream Health subsystem is complete when the following requirements are satisfied.

### 30.1 Metrics Infrastructure

- A shared Metrics Registry exists.
- Metric types have defined semantics.
- Counters and gauges use bounded storage.
- Metric identities are generation-aware.
- Counter resets are handled correctly.
- Snapshot creation is supported.
- Snapshot history is bounded.
- Global aggregation avoids double-counting.
- Optional detailed timing is supported.

### 30.2 Connectivity Metrics

- Connection lifecycle metrics are available.
- Message throughput is measurable.
- Reconnection attempts are measurable.
- REST request metrics are available.
- Rate-limit state is exposed.
- Subscription health is independently visible.
- Shared connection metrics are not duplicated incorrectly.

### 30.3 Canonical Stream Metrics

- Trade acceptance throughput is measurable.
- Trade continuity health is exposed.
- Missing executions and recovery progress are visible.
- L2 synchronization is exposed.
- Book generation is visible.
- Sequence failures are measurable.
- Book readiness uses authoritative synchronization state.

### 30.4 Distribution Metrics

- Publisher admission is measurable.
- Subscriber queue pressure is visible.
- Socket delivery metrics are available.
- Slow subscribers are detectable.
- Replay activity is measurable.
- Subscriber delivery does not imply application processing.

### 30.5 Retention and Derived Metrics

- Retention memory and disk usage are visible.
- Protected retention intervals are observable.
- Spill activity is measurable.
- Derived-stream source participation is visible.
- Merge watermark progress is exposed.
- Late events are measurable.
- Conversion availability is visible.
- Reference health is exposed.

### 30.6 Recording and Recovery Metrics

- PostgreSQL connectivity is visible.
- Recording admission and commit throughput are distinct.
- Recording backlog is measurable.
- Committed recording coverage is exposed.
- Recording gaps remain visible.
- Recovery attempts and operations are distinguished.
- Recovery completion uses authoritative state.

### 30.7 Health Evaluation

- Health is separate from metrics.
- Health is separate from lifecycle state.
- Healthy, Degraded, Unhealthy, Unknown, and Disabled are supported.
- Health reasons are structured.
- Required dependencies affect health.
- Optional disabled capabilities do not cause false failures.
- Stale observations are recognized.
- Confirmed integrity failures are not hidden by hysteresis.
- Obsolete generations cannot overwrite current health.

### 30.8 Monitoring API

- Runtime metrics can be queried.
- Stream metrics can be queried.
- Stream health can be queried.
- Dependency health can be inspected.
- Monitoring snapshots are versioned.
- Large responses are bounded.
- Missing measurements remain distinguishable from zero.
- Monitoring reads do not block market-data processing.

### 30.9 Ratatui Integration

- Dashboard displays global health.
- Streams view displays per-stream metrics.
- Trade integrity details are available.
- L2 synchronization details are available.
- Merges view displays derived-stream health.
- Recording view displays persistence health.
- Settings view exposes supported monitoring configuration.
- Health reasons are inspectable.
- TUI uses shared monitoring and control interfaces.
- Rendering remains isolated from canonical processing.

### 30.10 Testing

- Counter and gauge tests pass.
- Snapshot generation tests pass.
- Rate calculation tests pass.
- Generation-reset tests pass.
- Missing-measurement tests pass.
- Health classification tests pass.
- Dependency propagation tests pass.
- Recovery transition tests pass.
- Stale observation tests pass.
- TUI integration tests pass.
- Property-based tests pass.
- Monitoring overhead benchmarks establish acceptable performance.
### 30.11 Optional External Monitoring

- The Prometheus exporter is disabled by default.
- Exported metrics originate from the shared Metrics Registry.
- Prometheus naming and unit conventions are respected.
- Metric label cardinality remains bounded.
- Missing measurements are not fabricated as zero.
- Exporter scraping is resource-bounded.
- The endpoint is read-only.
- Loopback binding is the default.
- Exporter failures do not interrupt market-data processing.
- MarketForge does not manage Prometheus or Grafana.
- Ratatui and the internal monitoring API work independently.
- Exporter correctness and isolation tests pass.

---

## 31. Final Architectural Contract

The Metrics and Stream Health subsystem provides a shared operational view of MarketForge Live.

It observes authoritative runtime components without replacing their processing or recovery responsibilities.

The architecture is:

    Exchange Connections
             |
             v
    Canonical Processing
             |
             v
       Stream Registry
             |
             v
    Publisher / Retention
             |
             v
    Merge / Conversion
             |
             v
    PostgreSQL Recording
             |
             v
    Recovery Coordination
             |
             |
             v
       Metrics Registry
             |
             v
       Metrics Snapshots
             |
             +--------------------+
             |                    |
             v                    v
       HealthEvaluator       Monitoring API
             |                    ^
             v                    |
       Health Snapshots           |
             |                    |
             +---------+----------+
                       |
                       v
                  Ratatui TUI

The diagram is conceptual.

Metrics collection occurs throughout the runtime rather than through one serial processing pipeline.

The subsystem guarantees:

- Shared metric semantics.
- Bounded metric collection.
- Explicit metric identities and lifecycle scopes.
- Consistent monitoring snapshots.
- Authoritative health evaluation inputs.
- Multidimensional stream health.
- Dependency-aware health assessments.
- Explicit recording and recovery diagnostics.
- Lightweight runtime API access.
- Ratatui monitoring integration.
- Isolation from canonical market-data processing.

It does not guarantee:

- Complete market-data coverage from throughput metrics alone.
- Correct L2 synchronization based solely on level counts.
- Subscriber application processing from socket-write success.
- Complete recording coverage from database connectivity.
- Successful recovery from retry activity.
- Globally atomic diagnostic snapshots.
- Unlimited diagnostic history.
- Zero monitoring overhead.

The principal architectural invariant is:

    Authoritative Runtime State
                +
        Diagnostic Metrics
                |
                v
         Health Evaluation
                |
                v
      Monitoring Snapshots
                |
                v
        Runtime API / TUI

The integrity requirement is:

    Monitoring Must Never
    Replace The Components
    Responsible For
    Market-Data Correctness.

The performance requirement is:

    Monitoring Must Remain
    Bounded And Lightweight
    Under Sustained
    Market-Data Load.

The operational requirement is:

    A Healthy Dashboard
    Must Reflect Actual
    Required Guarantees,
    Not Merely The Presence
    Of Incoming Messages.

These principles provide a unified monitoring architecture for MarketForge Live without requiring external telemetry systems or introducing duplicate operational authorities.

---

**End of Part 5 — Runtime API, Ratatui Integration, Testing, Performance Requirements, and Final Acceptance Criteria.**

**End of 12 — Metrics and Stream Health.**