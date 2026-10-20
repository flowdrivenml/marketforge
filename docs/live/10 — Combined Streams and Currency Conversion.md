# 10 — Combined Streams and Currency Conversion

## 1. Purpose

The Combined Streams subsystem constructs derived market-data streams from one or more accepted canonical source streams.

It provides reusable cross-exchange, cross-market, and currency-normalized event streams without requiring downstream consumers to independently merge the same data.

The subsystem operates entirely within MarketForge Live's Rust runtime.

Its primary responsibilities include:

- Combining multiple canonical source streams.
- Preserving source-event provenance.
- Coordinating event-time ordering.
- Applying configurable reorder policies.
- Handling late and out-of-order events.
- Maintaining derived-stream identities.
- Tracking source-stream dependencies.
- Responding to source failures and recovery.
- Supporting currency conversion.
- Detecting stale or unavailable conversion rates.
- Maintaining bounded merge buffers.
- Publishing derived events through the existing Publisher.
- Supporting runtime creation, modification, and removal of combined streams.
- Exposing merge health, latency, and coverage diagnostics.

The subsystem must remain independent of exchange-native message formats.

### 1.1 Primary Objective

Suppose MarketForge receives:

    Binance BTCUSDT Trades
    Bybit BTCUSDT Trades
    OKX BTCUSDT Trades

These are three independent canonical source streams.

A consumer may request:

    Combined BTCUSDT Trades

The Combined Streams subsystem produces one logical output containing the accepted executions from all three sources under a defined ordering and conversion policy.

Conceptually:

    Binance BTCUSDT Trades ----+
                               |
    Bybit BTCUSDT Trades ------+----> Combined BTC Trades
                               |
    OKX BTCUSDT Trades --------+

The same source trades remain independently available through their original streams.

### 1.2 Why Combining Happens in Rust

Combining streams inside MarketForge Live provides:

- One shared implementation of merge logic.
- Consistent ordering across subscribers.
- Reusable derived-stream definitions.
- Centralized source-health handling.
- Consistent currency conversion.
- Lower duplication of downstream processing.
- Independent Rust and Python consumers.
- Centralized monitoring of merge correctness.

A Python or Rust consumer can subscribe to a combined stream without independently implementing its merging algorithm.

### 1.3 Scope

The Combined Streams subsystem must reuse the existing
historical merging and conversion output contracts.

Supported merge modes include:

    Trades

    Depth

    Combined

`Trades` merges canonical executions from multiple source
streams.

`Depth` merges canonical depth events while preserving
independent source-book identities and reconstruction
boundaries.

`Combined` merges trade and depth events into one
chronological timeline.

A combined timeline may contain both event types,
distinguished by the existing event-kind discriminator.

Currency conversion is a separate transformation that
may be applied to the resulting stream.

The subsystem must preserve the historical pipeline's
event representations, deterministic ordering semantics,
source provenance, and conversion status definitions.

Imbalance bars, run bars, feature engineering, and
machine-learning models remain outside MarketForge Live.

### 1.4 Combined Trades

A combined trade stream contains individual accepted executions originating from several source streams.

For example:

    Binance BTCUSDT Trade
    Bybit BTCUSDT Trade
    OKX BTCUSDT Trade

The output preserves the identity and provenance of each execution.

It must not combine multiple executions into one synthetic trade unless a separately defined aggregation operator explicitly requests that behavior.

### 1.5 Combined L2 Events

L2 combining requires additional restrictions.

A combined L2 event stream may interleave accepted events from multiple independent order books.

However:

    Combined L2 Event Stream

is not automatically equivalent to:

    Consolidated Order Book

A consolidated cross-exchange order book requires additional stateful processing, including:

- Independent source-book states.
- Compatible instrument economics.
- Currency conversion.
- Quantity normalization.
- Price-level aggregation.
- Staleness policies.
- Atomic derived-state publication.

The initial merge engine must not claim to provide a consolidated book merely by interleaving L2 deltas.

### 1.6 Currency Conversion

Currency conversion allows derived streams to express compatible financial values in a common denomination.

Example:

    BTCUSDT
    BTCUSDC
    BTCEUR

may contribute to a combined BTC trade stream denominated in USD.

However, conversion must account for:

- The actual quote asset.
- The configured target currency.
- Available conversion-rate sources.
- Event-time compatibility.
- Rate staleness.
- Numerical precision.
- Instrument metadata.
- Conversion provenance.

The system must not assume that USDT, USDC, and USD are economically identical.

### 1.7 Fundamental Invariant

    A derived stream must never
    silently change the meaning
    of its source events.

Every transformation must be explicit, versioned, and traceable.

---

## 2. Architectural Boundaries

Combined Streams operates downstream of canonical normalization and integrity validation.

It consumes accepted canonical source events and produces new derived events.

### 2.1 Component Ownership

| Component | Responsibility |
|---|---|
| Stream Registry | Source and derived logical stream identity |
| Subscription Manager | Exchange-native subscription lifecycle |
| Trade Processor | Canonical trade acceptance and continuity |
| BookStore | Canonical L2 state and synchronization |
| Combined Stream Registry | Derived-stream definitions and dependencies |
| Merge Engine | Event ordering and merge coordination |
| Currency Conversion Engine | Conversion-rate resolution and numerical transformation |
| Publisher | Source and derived event distribution |
| Live Retention Manager | Temporary retention of configured canonical and derived events |
| Recovery Coordinator | Cross-component recovery and escalation |
| Metrics Registry | Merge and conversion metrics |
| TUI / API | Derived-stream configuration and monitoring |

Combined Streams does not directly manage exchange WebSocket connections.

### 2.2 Pipeline Position

The complete logical pipeline is:

    Exchange WebSockets
            |
            v
    Exchange Connectivity
            |
            v
    Native Message Processing
            |
            v
    Canonical Normalization
            |
            v
    Integrity Validation
            |
            v
    Canonical Source Streams
            |
       +----+--------------------+
       |                         |
       v                         v
    Source Event              Merge Engine
    Distribution                 |
       |                         v
       |                  Currency Conversion
       |                         |
       |                         v
       |                  Derived Streams
       |                         |
       +------------+------------+
                    |
                    v
                 Publisher
                    |
                    v
              Unix Domain Socket
                    |
              +-----+-----+
              |           |
              v           v
          Consumer A  Consumer B

This is a logical diagram.

Currency conversion may occur before merge ordering when required by the configured transformation.

The exact processing order is defined by the derived-stream operator graph.

### 2.3 Source Streams Remain Independent

Creating a combined stream must not replace its source streams.

For example:

    Source A:
        Binance BTCUSDT Trades

    Source B:
        Bybit BTCUSDT Trades

    Derived C:
        Combined BTCUSDT Trades

All three remain independently addressable.

A subscriber may request:

    Source A

while another subscriber requests:

    Derived C

Both consume data produced from the same underlying Binance source without requiring duplicate exchange subscriptions.

### 2.4 One Source, Multiple Consumers

One canonical source event may participate in:

- Direct subscriber delivery.
- One combined trade stream.
- Another currency-converted stream.
- Temporary retention.
- Optional recording.
- Additional derived streams.

Conceptually:

    Binance BTCUSDT Trade
               |
       +-------+--------+
       |       |        |
       v       v        v
    Publisher Merge A  Merge B
       |       |        |
       v       v        v
    Raw IPC  BTC Mix  USD Mix

The original event is normalized once.

Its immutable representation may be shared across internal consumers.

### 2.5 Derived Events

A derived event is distinct from its source event.

It must preserve a reference to the original source identity.

Conceptually:

    SourceEventIdentity
            |
            v
    DerivedTransformation
            |
            v
    DerivedEventIdentity

A derived event has its own publication cursor within its derived stream.

The source event retains its original canonical identity.

### 2.6 Relationship to the Publisher

The Publisher must not implement merge algorithms.

Its responsibilities remain:

- Routing.
- Serialization.
- Subscriber queues.
- Unix socket transmission.
- Subscriber delivery tracking.
- Replay coordination.

The Merge Engine produces accepted derived events.

The Publisher distributes them using the same subscriber-facing architecture as ordinary canonical streams.

### 2.7 Relationship to Retention

Derived streams may be retained when their effective retention policy enables it.

Retention ownership remains with the Live Retention Manager.

Source retention and derived retention are independent.

A source event being retained does not automatically imply that every derived representation is retained.

Likewise, retaining a derived stream does not automatically establish complete source acquisition coverage.

### 2.8 Relationship to Recording

Derived streams may eventually support optional PostgreSQL recording.

The Recording Manager owns persistence.

The initial architecture must preserve separate recording policies for source and derived streams.

The Merge Engine must not issue PostgreSQL writes on its hot path.

---

## 3. Combined Stream Model

A combined stream is a named logical stream produced by applying a defined transformation graph to one or more input streams.

### 3.1 Stream Categories

The Stream Registry distinguishes:

    SourceStream

    DerivedStream

A source stream represents accepted canonical events from a native exchange channel.

A derived stream represents output from an internal transformation.

### 3.2 Derived Stream Types

The initial derived-stream categories are:

    TradeMerge

    DepthMerge

    CombinedMerge

    CurrencyConversion

`TradeMerge`, `DepthMerge`, and `CombinedMerge` correspond
to the existing historical merge modes.

`CombinedMerge` supports an ordered timeline containing
both canonical trades and canonical depth events.

Currency conversion may operate on trade-only,
depth-only, or combined timelines.

The existing historical output schemas and semantics
are authoritative.

The Live subsystem must not introduce incompatible
derived-event payloads for equivalent operations.

Nested derived streams may be supported through
explicitly validated dependency graphs.

Such nesting is a Live architectural extension and
must not be assumed to exist in the historical
implementation.

### 3.3 DerivedStreamId

Every derived stream receives a stable:

    DerivedStreamId

It must be distinguishable from source-stream identity while remaining compatible with the Publisher's logical StreamId routing contract.

The Stream Registry may use a unified identifier namespace with an explicit stream-kind attribute.

### 3.4 DerivedStreamDefinition

Conceptual representation:

    DerivedStreamDefinition {
        derived_stream_id

        name
        stream_kind

        input_streams
        operator_graph

        output_schema
        ordering_policy
        failure_policy

        retention_policy
        recording_policy

        configuration_revision
        enabled
    }

The actual Rust representation may use typed enums and validated operator definitions.

### 3.5 Stream Definition Identity

Two derived streams with identical input sources may still represent different outputs.

For example:

    Combined BTC Trades
        Allowed Lateness: 100 ms

    Combined BTC Trades
        Allowed Lateness: 500 ms

These definitions are not semantically equivalent.

Likewise:

    BTC Trades in USDT

and:

    BTC Trades in USD

are distinct derived products.

### 3.6 Derived Stream Output

A derived stream must declare its output contract.

Possible attributes include:

    EventType

    InstrumentEquivalenceGroup

    PriceCurrency

    QuantityUnit

    OrderingSemantics

    LatenessSemantics

    ProvenanceRequirements

A subscriber must be able to determine what the derived stream actually represents.

### 3.7 Historical/Live Derived Schema Compatibility

The existing MarketForge historical merge and conversion
schemas are authoritative for equivalent Live operations.

The Live Merge Engine must reuse the canonical Trade,
L2LevelUpdate, and CanonicalEvent representations.

Combined output must preserve the historical logical
event structure:

    Event Identity

    EventKind

    Source Provenance

    Trade Payload

    Depth Payload

The historical combined schema represents exactly one
logical trade or depth event per row.

For combined timelines:

    EventKind = Trade
        -> Trade payload present

    EventKind = Depth
        -> Depth payload present

A depth event may contain multiple canonical level
changes.

The Live implementation must preserve these event
boundaries rather than treating every depth level as an
unrelated source event.

Currency-converted outputs must preserve the original
canonical data and expose converted values separately.

The shared conversion contract includes:

    TargetQuote

    ConversionStatus

    ConversionRate

    ConversionPath

    ConversionReferences

    ConversionFailure

    TradeTarget

    DepthTarget

Conversion status semantics must match the historical
implementation:

    Identity
    Converted
    Unavailable
    Skipped

Historical conversion uses Decimal256 with precision 76
and scale 28.

Live conversion must preserve equivalent exact numerical
semantics and must not silently reduce precision to fit
ordinary rust_decimal::Decimal values.

Live-specific runtime metadata, including publication
cursors, cursor epochs, and generations, belongs in a
separate delivery or derived-event envelope.

Physical IPC encoding may differ from Parquet encoding,
but the shared logical data contract must remain
compatible.

Equivalent historical and Live transformations should
produce equivalent economic values and provenance
under the same input data and transformation policy.

### 3.8 Derived Event Immutability

Once a derived event is accepted for publication, its contents are immutable.

Later changes to conversion rates, source configuration, or merge policy must not retroactively change previously published events.

Recomputation, if supported, must use an explicit revisioned output contract.

---

## 4. Dependency Graph

Derived streams depend on their input streams.

These relationships form a directed dependency graph.

### 4.1 Dependency Example

Suppose MarketForge defines:

    Source A:
        Binance BTCUSDT Trades

    Source B:
        Bybit BTCUSDT Trades

    Source C:
        OKX BTCUSDT Trades

    Derived D:
        Combined BTCUSDT Trades

The dependency graph is:

    A ----+
          |
    B ----+----> D
          |
    C ----+

### 4.2 Nested Derived Streams

The architecture may eventually permit derived streams to depend on other derived streams.

Example:

    BTCUSDT Trades ---+
                      |
    BTCUSDC Trades ---+--> Currency-Normalized BTC Trades
                                      |
                                      v
                              Combined BTC Trades

Nested dependencies must remain explicit.

The initial implementation may restrict graph depth or supported operator combinations.

### 4.3 Dependency Graph Requirements

The graph must:

- Contain only valid stream identities.
- Reject cycles.
- Validate input/output schema compatibility.
- Validate instrument compatibility.
- Validate required conversion dependencies.
- Track configuration revisions.
- Respect Stream Registry desired state.
- Support generation-safe activation and removal.
- Avoid duplicate activation of shared sources.

### 4.4 Cyclic Dependencies

Cycles are invalid.

Example:

    Derived A depends on Derived B.

    Derived B depends on Derived A.

The graph validator must reject the configuration.

The system must not attempt recursive activation of cyclic streams.

### 4.5 Dependency Identity

Conceptually:

    DerivedDependency {
        derived_stream_id
        input_stream_id

        required
        dependency_role

        expected_schema
        expected_revision
    }

Dependencies may include ordinary market-data sources or conversion-rate sources.

### 4.6 Source Activation

Activating a derived stream may require its source streams to become operational.

The Combined Stream Registry submits the required source dependencies to the Stream Registry.

It must not directly create exchange-native subscriptions.

### 4.7 Source Deduplication

Several derived streams may depend on the same source.

Example:

    Binance BTCUSDT Trades
             |
       +-----+-----+
       |           |
       v           v
    Combined A  Combined B

Only one logical source stream is required.

The Stream Registry and Subscription Manager remain responsible for native subscription deduplication.

### 4.8 Dependency References

Source lifetime must account for derived-stream dependencies.

Conceptually:

    SourceConsumers {
        direct_subscribers
        derived_dependencies
        recording
        retention
        explicit_activation
    }

Removing one derived stream releases its dependency references.

It does not automatically deactivate source streams still required elsewhere.

### 4.9 Dependency Failure

If an input source fails:

    Source Failure
          |
          v
    Dependency State Updated
          |
          v
    Derived Failure Policy Evaluated
          |
       +--+--+
       |     |
       v     v
    Continue Suspend

Continuation requires an explicitly configured degraded-mode policy.

A derived stream must not silently omit failed sources while continuing to claim full source coverage.

### 4.10 Dependency Recovery

When a source recovers, the derived stream must reevaluate:

- Source generation.
- Processing readiness.
- Historical coverage status.
- Conversion availability.
- Merge buffer state.
- Watermark compatibility.
- Output continuity.

A restored source connection alone does not establish complete derived-stream recovery.

---

## 5. Combined Stream Registry

The Combined Stream Registry manages derived-stream definitions, dependency relationships, and runtime lifecycle state.

It does not own the physical market-data acquisition mechanisms.

### 5.1 Registry Responsibilities

The registry must:

- Create derived-stream definitions.
- Validate configuration.
- Resolve input dependencies.
- Maintain dependency references.
- Activate and deactivate derived streams.
- Track runtime generations.
- Coordinate operator initialization.
- Publish derived-stream state.
- Support controlled configuration updates.
- Expose derived-stream health.
- Reject invalid dependency graphs.

### 5.2 CombinedStreamRegistry

Conceptual representation:

    CombinedStreamRegistry {
        definitions
        runtime_states

        dependency_graph
        reverse_dependency_index

        configuration_revision
    }

The registry should support efficient lookup by derived-stream identity and source dependency.

### 5.3 Reverse Dependency Index

For each input stream, the registry tracks interested derived streams.

Example:

    Binance BTCUSDT Trades
            |
            +-- Combined BTC Trades
            |
            +-- USD-Normalized BTC Trades

This permits efficient event routing into the relevant merge operators.

### 5.4 Derived Stream Lifecycle

Supported states:

    Created
    Validating
    PendingDependencies
    Initializing
    Active
    Degraded
    Recovering
    Stopping
    Stopped
    Failed

| State | Meaning |
|---|---|
| Created | Definition registered |
| Validating | Configuration and dependencies being checked |
| PendingDependencies | Waiting for required inputs |
| Initializing | Merge operators and buffers being prepared |
| Active | Producing events under its declared contract |
| Degraded | Some required guarantees impaired |
| Recovering | Restoring state after source or operator failure |
| Stopping | Controlled deactivation underway |
| Stopped | Runtime resources released |
| Failed | Cannot operate under the configured policy |

### 5.5 Activation Sequence

    Activate Derived Stream
              |
              v
    Validate Definition
              |
              v
    Resolve Dependencies
              |
              v
    Acquire Dependency References
              |
              v
    Wait for Required Readiness
              |
              v
    Initialize Operator Graph
              |
              v
    Establish Input Boundaries
              |
              v
    Start Merge Runtime
              |
              v
    Register Derived Output
              |
              v
             Active

Input boundary establishment must prevent accepted source events from disappearing between source registration and merge activation.

### 5.6 Runtime Generation

Every activation receives a derived-stream runtime generation.

Conceptually:

    DerivedStreamGeneration

The generation fences:

- Input subscriptions.
- Merge buffers.
- Operator results.
- Conversion state.
- Publication completions.
- Recovery callbacks.

Obsolete results from a previous generation must not modify the active derived stream.

### 5.7 Deactivation

Stopping a derived stream must:

- Stop accepting new input assignments.
- Establish the output termination boundary.
- Resolve buffered events according to policy.
- Release conversion-rate references.
- Release source dependency references.
- Stop operator tasks.
- Release memory reservations.
- Update the registry state.

Deactivation must not independently terminate shared source streams.

### 5.8 Configuration Updates

Updating an active derived stream may change:

- Input sources.
- Allowed lateness.
- Ordering policy.
- Conversion target currency.
- Conversion-rate source.
- Failure policy.
- Resource limits.

Changes affecting output semantics must create a new transformation revision.

The implementation must not silently apply a new economic interpretation to an existing output sequence.

### 5.9 Registry Invariants

    Every derived stream has
    one authoritative definition.

    Every dependency references
    a valid logical stream.

    Dependency graphs contain no cycles.

    Shared source streams are not
    duplicated unnecessarily.

    Obsolete generations cannot
    publish authoritative output.

    Derived streams cannot claim
    stronger source integrity
    than their available evidence supports.

---

## 6. Derived Event Provenance

Every derived event must preserve sufficient information to identify its origin and transformation.

This is necessary for debugging, historical reconstruction, integrity validation, and downstream research.

### 6.1 Provenance Requirements

A derived event should identify:

    DerivedStreamId

    DerivedEventIdentity

    SourceStreamId

    SourceEventIdentity

    SourceExchange

    SourceInstrumentId

    TransformationRevision

    OrderingPolicyRevision

    ConversionProvenance

Not every derived operation requires every field.

The schema must distinguish applicable from nonapplicable metadata.

### 6.2 DerivedEventEnvelope

Live derived events use the existing canonical market-data
payloads and a separate runtime publication envelope.

Conceptually:

    DerivedEventEnvelope {
        derived_stream_id

        log_epoch
        derived_cursor

        source_event_identity

        transformation_revision
        ordering_policy_revision

        event
        conversion_metadata
    }

`event` preserves the existing canonical Trade or
L2LevelUpdate representation.

For combined timelines, event type and grouping must
remain compatible with the historical combined schema.

Source provenance must preserve equivalent information
to the historical merge contract:

    StreamRank

    DatasetId

    TaskId

    EventOrdinal

    CanonicalRowOffset

    CanonicalRowCount

Historical provenance fields that do not have direct Live
runtime equivalents must use an explicitly defined
mapping or remain unavailable.

The runtime must not fabricate historical dataset or
task identifiers.

Live-specific source identities and publication cursors
must remain independently available.

Conversion metadata must preserve the historical
conversion status, selected path, reference provenance,
and exact converted values.

The original source event must remain identifiable and
must not be overwritten by currency conversion.

### 6.3 Source Event Preservation

The original source event must remain identifiable.

Example:

    Source:
        Binance BTCUSDT Trade
        ExecutionId = 12345

    Derived:
        Combined BTC Trades
        SourceExecutionId = 12345
        SourceExchange = Binance

The derived event must not appear to be a new exchange execution.

### 6.4 Transformation Chain

For nested derived streams, provenance must describe the relevant transformation chain.

Example:

    Binance BTCUSDT Trade
              |
              v
    Quote Conversion: USDT -> USD
              |
              v
    Combined BTC USD Trades

The output must identify the conversion and merge revisions required to interpret its values.

### 6.5 Provenance Size

Full transformation histories may become large.

The initial implementation should use compact revision identifiers and references to immutable stream definitions.

Per-event provenance must still remain sufficient to establish source identity and the applied conversion values.

### 6.6 Provenance Invariant

    A derived event must be
    traceable to its original
    accepted canonical source event
    and the transformation contract
    used to produce it.

---

**## 7. Merge Engine Architecture

The Merge Engine combines accepted canonical events from multiple source streams into an ordered derived event sequence.

It operates downstream of canonical normalization and integrity validation.

The engine must preserve the historical pipeline's logical event representation and deterministic ordering semantics.

### 7.1 Responsibilities

The Merge Engine must:

- Consume canonical source events.
- Preserve original source identities.
- Merge trade and L2 events.
- Support trade-only, depth-only, and combined timelines.
- Apply deterministic event-time ordering.
- Maintain bounded reorder buffers.
- Track per-source event-time progress.
- Evaluate output watermarks.
- Handle late and out-of-order events.
- Detect unavailable or stalled sources.
- Respect configured source-failure policies.
- Publish derived events through the Publisher.
- Expose ordering and lateness metrics.

The Merge Engine must not:

- Normalize exchange-native messages.
- Reconstruct source order books.
- Generate artificial exchange executions.
- Modify accepted source-event identities.
- Perform source trade reconciliation.
- Establish exchange subscriptions.
- Write Parquet files directly.

### 7.2 Logical Architecture

    Canonical Source Streams
              |
       +------+------+
       |      |      |
       v      v      v
    Source A Source B Source C
       |      |      |
       +------+------+
              |
              v
       Merge Input Router
              |
              v
       Source Progress State
              |
              v
       Reorder Buffer
              |
              v
       Watermark Evaluation
              |
              v
       Ordered Event Release
              |
              v
       Derived Event Envelope
              |
              v
          Publisher

The Merge Engine must not block exchange WebSocket readers while waiting for unrelated streams to advance.

### 7.3 Merge Modes

The Merge Engine supports the existing historical merge modes:

    Trades
    Depth
    Combined

**Trades**

Produces a chronological sequence of canonical executions.

**Depth**

Produces a chronological sequence of canonical depth events while preserving independent source-book boundaries.

**Combined**

Produces one chronological sequence containing both trade and depth events.

These modes must retain the historical pipeline's logical event-kind semantics.

### 7.4 One Merge Runtime per Derived Stream

Each active merge definition has one authoritative runtime instance.

Conceptually:

    MergeRuntime {
        derived_stream_id
        runtime_generation

        input_states
        ordering_policy

        reorder_buffer
        watermark_state

        output_cursor
        output_state
    }

Multiple subscribers requesting the same derived stream share the same merge runtime.

The system must not instantiate one merge engine per Unix socket subscriber.

### 7.5 Merge Activation

Before accepting output events, a merge runtime must:

1. Resolve its configured input streams.
2. Validate compatible event schemas.
3. Acquire source dependency references.
4. Establish input subscription boundaries.
5. Initialize source progress state.
6. Initialize its reorder buffer.
7. Establish its ordering policy revision.
8. Register the derived output stream.

The initial input boundaries must be coordinated with source publication.

Otherwise, events may disappear between dependency activation and merge routing.

### 7.6 Input Immutability

The Merge Engine consumes immutable accepted canonical events.

It may attach derived-stream metadata, but it must not mutate the original source payload.

Currency conversion is a separate transformation with its own output contract.

---

## 8. Historical Ordering Compatibility

MarketForge's historical merge pipeline defines deterministic event ordering using a composite merge key.

Live merging must preserve the same logical ordering semantics wherever the required source identities are available.

### 8.1 Historical Merge Key

The existing historical ordering structure is:

    MergeKey {
        timestamp_ns
        stream_rank
        task_rank
        event_ordinal
    }

The ordering is lexicographic:

    timestamp_ns
        |
        v
    stream_rank
        |
        v
    task_rank
        |
        v
    event_ordinal

The first differing field determines which event comes first.

### 8.2 Event Timestamp

The primary ordering field is:

    event_timestamp_ns

This represents the canonical event timestamp.

The Merge Engine must not silently replace event time with socket arrival time or local processing time.

Those timestamps may be used for diagnostics or separate explicitly configured ordering policies.

### 8.3 Stream Rank

`stream_rank` provides deterministic ordering between different input streams when event timestamps are equal.

A Live merge definition must assign stable ranks to its configured inputs.

For example:

    Rank 0:
        Binance BTCUSDT Trades

    Rank 1:
        Bybit BTCUSDT Trades

    Rank 2:
        OKX BTCUSDT Trades

Ranks are assigned through the compiled merge definition.

They must not depend on the order in which asynchronous Rust tasks happen to deliver events.

### 8.4 Task Rank

Historical processing may distinguish multiple source tasks belonging to the same logical stream.

Live runtime tasks do not necessarily correspond to historical processing tasks.

Therefore, the Live implementation must not fabricate historical task identifiers.

Instead, it must define a stable Live ordering component with equivalent deterministic tie-breaking properties.

Possible inputs include:

    Source Epoch

    Source Partition Identity

    Source Event Sequence

The exact mapping must be documented and validated against the historical ordering contract.

### 8.5 Event Ordinal

Historical `event_ordinal` identifies an event's position within its source task.

Live merging must preserve an equivalent stable event ordering identity.

The preferred source is the canonical source publication cursor, scoped to its valid cursor epoch.

A cursor cannot be compared across unrelated source streams without the configured stream rank.

### 8.6 Live Merge Key

Conceptually:

    LiveMergeKey {
        event_timestamp_ns
        stream_rank
        source_ordering_identity
        source_event_ordinal
    }

This must provide a deterministic total ordering within the configured merge domain.

The finalized representation must distinguish:

- Historical task identity.
- Live source generation.
- Canonical publication identity.

These must not be conflated.

### 8.7 Equal Timestamps

Suppose three source events have the same timestamp:

    Timestamp = 1000000

    Binance Rank = 0

    Bybit Rank = 1

    OKX Rank = 2

The deterministic order is:

    Binance
    Bybit
    OKX

regardless of which event arrived at the Merge Engine first.

This ordering is guaranteed only when all relevant events are available before the applicable release boundary.

### 8.8 Historical/Live Equivalence

Given:

- The same canonical input events.
- The same source ranking.
- Equivalent source-ordering identities.
- The same merge definition.
- No events exceeding the selected lateness policy.

The historical and Live merge outputs should have equivalent logical event ordering.

Differences caused by explicitly configured Live lateness limits must remain observable.

### 8.9 Ordering Policy Revisions

Changes to:

    Stream Rankings

    Tie-Breaking Rules

    Event-Time Semantics

    Lateness Policy

must advance the ordering policy revision.

Already published derived events must retain their original ordering interpretation.

---

## 9. Reorder Buffer

Live events arrive asynchronously from independent exchange connections.

Arrival order is not necessarily event-time order.

The Reorder Buffer temporarily retains accepted input events until the Merge Engine can release them according to its watermark and lateness policy.

### 9.1 Reorder Buffer Purpose

Consider:

    Arrival 1:
        Binance Trade, EventTime 100

    Arrival 2:
        OKX Trade, EventTime 103

    Arrival 3:
        Bybit Trade, EventTime 101

Immediate publication would produce:

    100, 103, 101

The reorder buffer can instead produce:

    100, 101, 103

provided the Bybit event arrives before the output frontier advances beyond its timestamp.

### 9.2 Buffer Representation

Conceptually:

    ReorderBuffer {
        derived_stream_id

        events
        maximum_events
        maximum_bytes

        earliest_event_time
        latest_event_time

        state
    }

Events are ordered using the configured merge key.

### 9.3 Initial Data Structure

The initial recommended implementation is a priority queue ordered by the complete merge key.

Conceptually:

    MinHeap<MergeKey, CanonicalEventRef>

The smallest eligible merge key is released first.

The implementation must preserve stable deterministic ordering for equal timestamps.

### 9.4 Event Insertion

    Canonical Event
          |
          v
    Validate Input Identity
          |
          v
    Resolve Stream Rank
          |
          v
    Construct Merge Key
          |
          v
    Check Lateness
          |
          v
    Reserve Buffer Capacity
          |
          v
    Insert Event

The Merge Engine must not silently discard an event when insertion fails.

### 9.5 Buffer Limits

The Reorder Buffer must enforce:

    MaximumBufferedEvents

    MaximumBufferedBytes

    MaximumBufferResidenceTime

    MaximumAllowedLateness

The exact configuration keys belong to document 03.

### 9.6 Buffer Memory Accounting

Buffer accounting must include:

- Event references.
- Heap metadata.
- Source identity metadata.
- Ordering keys.
- Pending output references.

Shared canonical-event payloads may use immutable reference-counted storage.

The Merge Engine must not treat shared references as zero-cost resources.

### 9.7 Buffer Overflow

If the buffer cannot accept another event:

    Buffer Saturated
          |
          v
    Evaluate Eligible Releases
          |
          v
    Apply Resource Policy
          |
          v
    Report Failure if Unresolved

The Merge Engine must not force-release events under a strict ordering contract merely to free memory.

### 9.8 Reorder Buffer vs Retention

The Reorder Buffer and Live Retention Manager serve different purposes.

| Reorder Buffer | Live Retention |
|---|---|
| Orders pending source events | Preserves accepted events |
| Short-lived working state | Bounded replay and bootstrap |
| Owned by Merge Engine | Owned by Retention Manager |
| Uses ordering and watermarks | Uses canonical publication cursors |
| Generally in memory | RAM and Parquet |
| Controls derived-event release | Controls retained-event availability |

The initial Merge Engine does not automatically spill its priority queue into Parquet.

A future disk-backed merge buffer would require an explicit separate ordering and recovery contract.

---

## 10. Source Progress Tracking

Every merge input requires independent event-time progress tracking.

The engine must not assume that all source streams advance at the same rate.

### 10.1 SourceMergeState

Conceptually:

    SourceMergeState {
        stream_id
        stream_rank

        source_generation
        log_epoch

        last_accepted_cursor
        maximum_observed_event_time

        source_watermark

        last_arrival_time
        source_health

        idle_state
    }

The state belongs to one configured input stream.

### 10.2 Maximum Observed Event Time

For each source:

    MaximumObservedEventTime

tracks the largest accepted event timestamp observed by the Merge Engine.

It must not decrease when a late event arrives.

### 10.3 Out-of-Order Source Events

Example:

    Source A:

        EventTime 100
        EventTime 105
        EventTime 102

The maximum observed event time remains:

    105

after receiving the final event.

This does not prove that no additional events earlier than 105 will arrive.

### 10.4 Source Lateness

A source may permit a configured amount of out-of-order arrival.

Conceptually:

    SourceAllowedLateness

This expresses an operational tolerance, not an exchange guarantee.

The Merge Engine must measure lateness violations.

### 10.5 Source Health

Source state may be:

    Active
    Idle
    Recovering
    Unavailable
    Failed

Source health is obtained from the authoritative source lifecycle and processing components.

The Merge Engine must not infer complete source integrity solely from message arrival frequency.

### 10.6 Source Generation Changes

A source may reconnect or restart its processing generation.

The Merge Engine must validate:

    SourceGeneration

    CursorEpoch

    ProcessingReadiness

Changes may invalidate pending assumptions about source ordering or continuity.

The derived stream must respond according to its configured failure policy.

---

## 11. Event-Time Watermarks

A watermark represents the event-time frontier below which the Merge Engine is prepared to finalize output ordering.

Watermarks control when buffered events may be released.

They are not simply the latest observed event timestamps.

### 11.1 Watermark Principle

The Merge Engine must not release an event solely because it is currently the smallest item in the reorder buffer.

It must also establish that the event is eligible under the selected event-time completion policy.

### 11.2 Source Watermark

For a source using bounded lateness:

    SourceWatermark =
        MaximumObservedEventTime
        -
        AllowedLateness

This is a policy-derived watermark, not proof that earlier events cannot arrive.

Sources with stronger native progress guarantees may use protocol-specific evidence through their canonical processing contracts.

### 11.3 Combined Watermark

For a merge requiring all configured sources:

    CombinedWatermark =
        Minimum(RequiredSourceWatermarks)

The slowest required source limits output progress.

### 11.4 Example

Suppose:

    Binance:
        Watermark = 12:00:05.500

    Bybit:
        Watermark = 12:00:05.300

    OKX:
        Watermark = 12:00:05.700

Then:

    CombinedWatermark = 12:00:05.300

Events eligible under the inclusive watermark rule may be released through that frontier.

### 11.5 Release Rule

The initial recommended release rule is:

    EventTimestamp < CombinedWatermark

Events are released in complete merge-key order.

The strict inequality is required because reaching a timestamp
watermark does not, by itself, establish that every event with
that exact timestamp has already arrived.

Events at the watermark timestamp remain buffered until the
timestamp is closed under the applicable source-completeness
and ordering contracts.

An inclusive release rule:

    EventTimestamp <= CombinedWatermark

is permitted only when all relevant source contracts explicitly
establish completeness at the watermark timestamp.

A release operation must also respect the active derived-stream
generation and output admission contract.

This rule governs ordinary watermark-based release.
Events exceeding the configured lateness or buffer limits
remain subject to the separately defined late-event,
overflow, and source-failure policies.

### 11.6 Equal-Timestamp Ordering

A timestamp-only watermark is insufficient if additional events with the same timestamp may arrive after release.

For example:

    Released:
        Timestamp 100, StreamRank 2

    Arrives Later:
        Timestamp 100, StreamRank 0

The second event should precede the first under the historical merge key.

Therefore, the watermark contract must establish when a timestamp is closed for ordering.

The recommended rule is:

    Timestamp < CombinedWatermark

unless all relevant source contracts explicitly establish completeness at the watermark timestamp.

This conservative rule avoids treating an inclusive timestamp boundary as closed without evidence.

### 11.7 Watermark Monotonicity

The authoritative combined watermark must not move backward within one merge generation.

If source progress becomes invalid, the engine must enter a defined recovery or degraded state.

It must not retroactively retract previously published events.

### 11.8 Initial Watermark

At merge activation, no source watermark may exist yet.

The engine must wait until the configured required sources provide sufficient progress evidence.

It must not initialize a watermark using the current wall-clock time without an explicit policy.

### 11.9 Empty or Inactive Sources

A source may legitimately produce no events for an extended period.

The absence of trades does not necessarily indicate an exchange failure.

However, waiting indefinitely for one inactive source may prevent the combined watermark from advancing.

The idle-source policy must address this explicitly.

### 11.10 Idle Source Policy

Supported conceptual policies:

    WaitForAll

    ExcludeConfirmedIdle

    DegradeAndContinue

**WaitForAll**

The combined watermark waits for all required sources.

**ExcludeConfirmedIdle**

A source may be temporarily excluded only when its idle state is established through supported liveness or progress evidence.

**DegradeAndContinue**

The merge may continue while explicitly reporting incomplete source participation.

The initial strict policy should default to:

    WaitForAll

where complete source participation is required.

### 11.11 Idle Detection

An idle designation may consider:

- Exchange heartbeat state.
- Stream subscription health.
- Last received event time.
- Protocol-specific progress markers.
- Configured inactivity duration.

Inactivity alone must not be interpreted as proof that no earlier event can arrive later.

If an excluded source resumes with an event behind the emitted frontier, the configured late-event policy applies.

### 11.12 Watermark Stalls

A combined watermark may stop advancing because:

- One source produces no events.
- One source disconnects.
- An exchange becomes unavailable.
- Source processing stalls.
- Required progress evidence is missing.

The Merge Engine must expose the source responsible for the stall.

A watermark stall must not cause unbounded buffer growth.

---

## 12. Late Event Handling

An event is late when its merge key falls behind the derived stream's already finalized output frontier.

This is distinct from ordinary out-of-order arrival within the reorder buffer.

### 12.1 Late Event Definition

Conceptually:

    EventMergeKey < FinalizedMergeFrontier

The finalized frontier includes all relevant ordering-key components.

An event arriving behind this frontier cannot be inserted into the existing output sequence without violating ordering.

### 12.2 Late Event Causes

Possible causes include:

- Exchange network latency.
- WebSocket reconnection.
- Delayed source processing.
- Asynchronous source scheduling.
- Trade recovery.
- Incorrect source timestamps.
- Inaccurate watermark assumptions.
- Temporarily excluded source resumption.

### 12.3 Late Event Policies

Supported conceptual policies:

    FailStrictOrdering

    PublishAsLate

    SideOutput

The selected policy belongs to the derived-stream configuration.

### 12.4 FailStrictOrdering

Under strict ordering:

    Late Event Detected
           |
           v
    Reject Normal Ordered Admission
           |
           v
    Mark Derived Continuity Impaired
           |
           v
    Report Ordering Failure

The source event remains valid.

It must not be silently discarded.

Recovery may require replay, a new derived-stream generation, or another explicitly supported procedure.

### 12.5 PublishAsLate

The event may be emitted after the finalized frontier with explicit late-event metadata.

This preserves event availability but weakens chronological ordering.

The derived stream must advertise that its ordering contract permits late output.

It must not continue claiming globally monotonic event-time ordering.

### 12.6 SideOutput

The event is routed to a separately identified late-event output.

Conceptually:

    Merge Engine
         |
         +-- Ordered Output
         |
         +-- Late Event Output

The late-event output must have its own routing identity and publication contract.

A subscriber interested only in ordered output must be informed that late source events were excluded from that output.

### 12.7 No Silent Drop

The initial MarketForge integrity policy is:

    Never Silently Drop Accepted Events

Therefore, a late event must produce:

- A valid explicitly classified publication.
- A separate side output.
- Or an explicit failure requiring recovery.

It must not disappear without an observable outcome.

### 12.8 Late Trade Recovery

Recovered trades may have event timestamps older than the current derived output frontier.

Example:

    Combined Stream Published Through:
        12:00:10

    Trade Reconciliation Recovers:
        EventTime 12:00:07

The Merge Engine must not silently insert the recovered trade into an already finalized event-time sequence.

The selected late-event policy determines its handling.

### 12.9 Late L2 Events

Late L2 events require additional care.

A source L2 update must already have passed source-book integrity validation.

However, publishing it late in a merged event-time timeline may violate that timeline's declared ordering contract.

The Merge Engine must preserve the source book generation and committed transition identity.

It must not rewrite source-book state to accommodate the combined ordering.

### 12.10 Lateness Metrics

The subsystem must measure:

    LateEventsTotal

    MaximumObservedLateness

    LatenessDistribution

    LateEventsBySource

    LateEventsByReason

Metrics must distinguish ordinary buffered reordering from events violating the finalized frontier.

---

## 13. Ordering Guarantees

The Merge Engine must expose its actual ordering guarantees to downstream consumers.

### 13.1 Ordering Modes

The initial conceptual modes are:

    StrictEventTime

    BoundedEventTime

    ArrivalOrder

**StrictEventTime**

Events are published in deterministic merge-key order.

Any event that would violate the finalized frontier causes an explicit failure or requires a separate output.

**BoundedEventTime**

Events are reordered within a configured lateness budget.

Events arriving after finalization follow the selected late-event policy.

**ArrivalOrder**

Events are published according to Merge Engine admission order.

This mode is explicitly not equivalent to the historical chronological merge.

### 13.2 Historical Equivalence

The historical merge pipeline operates on finite, completed inputs.

It can determine deterministic ordering without live uncertainty.

The Live Merge Engine can reproduce that ordering only when its watermark and lateness assumptions are sufficient for the observed event stream.

It must not claim unconditional equivalence.

### 13.3 Output Cursor

Every released derived event receives a monotonically advancing publication cursor within the derived stream's cursor epoch.

The output cursor represents publication order.

It is not the original source cursor.

### 13.4 Source Identity

Every derived event preserves the original canonical source identity.

The Merge Engine must not create new exchange-native trade identities.

### 13.5 Ordering Across Restart

A Merge Engine restart requires:

- A new runtime generation.
- Validation of source cursor boundaries.
- Recovery or invalidation of pending reorder state.
- A restart-safe output cursor identity.
- Explicit handling of unfinished ordering intervals.

A new merge generation must not claim seamless continuity without supporting evidence.

### 13.6 Required Invariants

    The Merge Engine does not mutate
    accepted canonical source events.

    Derived output ordering is deterministic
    under the selected ordering contract.

    Source ranks are configuration-defined.

    The reorder buffer remains bounded.

    Watermarks never regress within
    an authoritative merge generation.

    Late events never disappear silently.

    An event behind the finalized frontier
    cannot be inserted into strict ordered
    output without explicit recovery.

    Historical-equivalent ordering is claimed
    only when its required assumptions hold.

---

## 14. Currency Conversion Architecture

The Currency Conversion Engine transforms canonical market-data values into a configured target quotation currency.

It operates within the Rust derived-stream subsystem.

Currency conversion must preserve original canonical source values and attach separately represented converted values.

The existing historical conversion implementation is the reference contract.

### 14.1 Responsibilities

The Currency Conversion Engine must:

- Resolve source and target currencies.
- Validate instrument currency metadata.
- Resolve configured conversion paths.
- Consume internal and external reference streams.
- Maintain reference-price state.
- Evaluate references at the applicable event-time boundary.
- Enforce reference staleness limits.
- Prevent future-data leakage.
- Perform exact numerical transformations.
- Preserve original canonical values.
- Preserve conversion provenance.
- Report unavailable conversion results.
- Support trade, depth, and combined timelines.
- Remain compatible with historical conversion semantics.

The engine must not independently establish exchange connections or query PostgreSQL during event processing.

### 14.2 Architectural Position

Conceptually:

    Canonical Source Streams
              |
              v
         Merge Engine
              |
              v
       Ordered Event Stream
              |
              v
    Currency Conversion Engine
              |
              v
      Converted Derived Stream
              |
              v
           Publisher
              |
              v
        Unix Domain Socket

This is the default architecture for combined chronological conversion.

A currency conversion operator may also operate on an individual source stream without requiring a multi-source merge.

### 14.3 Historical Compatibility

The historical implementation already provides:

    Conversion Planning

    Reference Stream Selection

    Reference State

    Reference Checkpoints

    Conversion Path Evaluation

    Exact Decimal Arithmetic

    Trade Conversion

    Depth Conversion

    Combined Conversion

The Live implementation must reuse the applicable pure domain algorithms and economic semantics wherever possible.

Historical file readers, partition planners, and Parquet writers are not required in the Live conversion hot path.

The objective is shared conversion logic, not duplication of historical batch infrastructure.

### 14.4 Original Values

The conversion engine must not overwrite source canonical values.

For example:

    Original:
        BTCUSDT
        Price = 65000 USDT

    Converted:
        Target = USD
        Price = 64980 USD

Both representations must remain available.

The converted price is a derived value.

The original price remains the source of truth.

### 14.5 Conversion Runtime

Conceptual representation:

    ConversionRuntime {
        derived_stream_id
        runtime_generation

        target_currency
        conversion_plan

        reference_state
        reference_dependencies

        precision_policy
        staleness_policy

        configuration_revision
        transformation_revision
    }

Each active conversion runtime owns its mutable reference state.

Immutable conversion plans may be shared.

---

## 15. Currency and Instrument Identity

Currency conversion depends on the economic identity of the instrument.

Instrument symbols alone are insufficient.

### 15.1 Currency Metadata

The conversion engine must receive validated instrument metadata identifying the relevant assets.

Conceptually:

    InstrumentCurrencyContext {
        instrument_id

        base_asset
        quote_asset
        settlement_asset

        instrument_kind
        quantity_semantics

        metadata_revision
    }

Not every instrument uses every field.

The context must preserve the distinction between spot, linear, and inverse contracts.

### 15.2 Authoritative Metadata

The Python-managed instrument catalog remains the authoritative source for instrument metadata.

MarketForge Live receives validated, revisioned metadata through its existing initialization contract.

The conversion engine uses locally available immutable metadata.

It must not perform Python or PostgreSQL lookups for individual market-data events.

### 15.3 Symbol Independence

The conversion engine must not infer currency identities solely by splitting strings such as:

    BTCUSDT

    BTC-USD-SWAP

    BTCUSDC

Instrument symbols vary by exchange and market family.

Currency identities must come from validated metadata.

### 15.4 Target Currency

Every converted derived stream declares:

    TargetCurrency

Examples:

    USD
    EUR
    USDT
    USDC
    BTC

A target currency is an explicit economic denomination.

The engine must not assume stablecoin parity with fiat currencies.

### 15.5 Identity Conversion

When:

    SourceQuoteCurrency == TargetCurrency

the conversion may use an identity transformation.

Conceptually:

    ConversionRate = 1

The historical implementation already distinguishes identity conversion from ordinary conversion.

Live must preserve that distinction.

### 15.6 Unsupported Instruments

Some instrument economics may not support a requested conversion under the available metadata and conversion operators.

Examples include:

- Missing quote-asset metadata.
- Unsupported contract-value semantics.
- Unavailable quantity representation.
- Unsupported inverse-price transformation.
- Incompatible underlying assets.

The engine must reject unsupported configurations or report explicitly unavailable conversion results.

It must not apply spot conversion formulas indiscriminately to derivatives.

---

## 16. Conversion Graph

Currency conversion uses a directed graph of available currency relationships.

### 16.1 Graph Model

Each currency is represented as a vertex.

Each supported conversion relationship is represented as a directed edge.

Conceptually:

    BTC ---- USDT
     |        |
     |        |
    USD ---- USDC
     |
     |
    EUR

The diagram illustrates possible relationships.

Only configured and validated reference instruments establish usable conversion edges.

### 16.2 Conversion Edge

Conceptual representation:

    ConversionEdge {
        source_currency
        target_currency

        reference_stream_id
        direction

        instrument_id
        conversion_semantics

        metadata_revision
    }

An edge must define how a reference observation produces its conversion ratio.

### 16.3 Direct Conversion

A direct conversion requires one compatible reference relationship.

Example:

    EUR -> USD

using a validated EUR/USD reference stream.

If the reference price expresses USD per EUR:

    ValueUSD = ValueEUR × RateEURUSD

The engine must determine multiplication or division from the edge's explicit direction.

### 16.4 Inverse Conversion

If the available reference expresses the opposite currency relationship:

    USD -> EUR

the engine may use an inverse edge when mathematically and economically valid.

Conceptually:

    RateUSDToEUR = 1 / RateEURToUSD

The calculation must use exact checked arithmetic.

Division by zero is invalid.

### 16.5 Multi-Hop Conversion

A conversion may require several edges.

Example:

    EUR
     |
     v
    USDT
     |
     v
    USD

The conversion rate is derived from the ordered composition of the selected edge ratios.

A multi-hop path must preserve:

- Every reference source.
- Every reference timestamp.
- Every conversion direction.
- The selected path.
- The applied numerical result.

### 16.6 ConversionPath

Conceptual representation:

    ConversionPath {
        source_currency
        target_currency

        currencies
        edges

        path_revision
    }

The path must be validated before activation.

### 16.7 Path Validation

Validation must reject:

- Unknown currency identities.
- Missing reference-stream definitions.
- Unsupported edge directions.
- Invalid instrument metadata.
- Cyclic paths where prohibited by policy.
- Paths exceeding the configured maximum length.
- Incompatible reference units.

### 16.8 Path Selection

The Conversion Planner determines which paths are eligible for each input stream.

The historical implementation already contains conversion planning and path-evaluation mechanisms.

Live must preserve compatible selection semantics.

If multiple paths are available, the selected policy must be explicit.

The implementation must not silently select whichever reference update happened to arrive first.

### 16.9 Multiple Conversion Paths

A derived stream may eventually support:

    PreferredPath

    OrderedFallbackPaths

    StrictSinglePath

The current historical materialization behavior must be treated as the compatibility baseline.

Any additional Live path-selection behavior is a separately versioned extension.

### 16.10 Dependency Graph Integration

Conversion-rate reference streams become dependencies of the converted derived stream.

Example:

    BTCUSDT Trades -----------+
                              |
    USDTUSD Reference --------+---> BTC Trades in USD

The Combined Stream Registry acquires these dependencies through the Stream Registry.

It must not create duplicate source subscriptions.

---

## 17. Reference Stream Architecture

A reference stream provides market observations used to evaluate a currency conversion path.

Reference data may originate from the same combined timeline or an independent source stream.

### 17.1 Reference Sources

The historical implementation distinguishes:

    InternalReferences

    ExternalReferences

Live must preserve these logical categories.

**Internal references** belong to the input timeline being processed.

**External references** arrive through separately identified reference streams.

Both contribute to one compatible conversion-state model.

### 17.2 ReferenceObservation

Conceptual representation:

    ReferenceObservation {
        reference_stream_id

        source_event_identity
        reference_instrument_id

        event_timestamp_ns
        ordering_key

        price

        source_generation
        metadata_revision
    }

The observation must retain enough information to establish its event-time eligibility and provenance.

### 17.3 ReferenceState

The conversion runtime maintains the latest eligible reference observations required by the selected conversion paths.

Conceptually:

    ReferenceState {
        reference_streams
        latest_observations
        progress_state
        configuration_revision
    }

The state must be updated according to the historical reference-ordering contract.

It must not simply use the latest value received by wall-clock arrival order.

### 17.4 Internal References

An internal reference observation may be produced by a trade that also appears in the merged event timeline.

Example:

    Combined Timeline:
        BTCUSDT Trade
        ETHUSDT Trade
        USDTUSD Trade

The USDTUSD trade may update conversion reference state used by subsequent events.

The fact that a trade serves as a reference does not remove it from the combined event sequence.

### 17.5 External References

External reference streams are separately acquired canonical streams.

Example:

    Combined Input:
        BTCUSDT Trades

    External Reference:
        USDTUSD Trades

The reference stream may be shared by several conversion runtimes.

However, each runtime must evaluate reference state according to its own target event ordering and conversion policy.

### 17.6 Shared Reference Acquisition

One reference stream may support many derived streams.

Conceptually:

    USDTUSD Trades
          |
       +--+--+
       |     |
       v     v
    BTC/USD ETH/USD

Only one underlying canonical source stream is required.

The Stream Registry manages shared dependency references.

### 17.7 Reference Lifecycle

Reference dependencies must be activated before the conversion runtime declares full readiness.

Reference availability must be tracked independently from the primary market-data stream.

A healthy BTCUSDT trade stream does not prove that the USDTUSD conversion reference is healthy.

### 17.8 Reference Recovery

After a reference-stream disconnect:

- The reference state remains explicitly classified.
- Existing observations retain their original timestamps.
- Staleness continues increasing.
- New conversions use only references permitted by policy.
- Missing required references produce unavailable outcomes.
- Reference recovery does not retroactively rewrite published converted events.

---

## 18. Event-Time Reference Alignment

Reference alignment is one of the most important correctness requirements.

Currency conversion must not use future observations when evaluating past events.

### 18.1 Historical Ordering Principle

The historical combined conversion implementation advances external reference observations strictly before the target event timestamp.

Conceptually:

    ReferenceTimestamp < EventTimestamp

The current event is then evaluated.

Applicable internal reference observations are incorporated afterward.

Live conversion must preserve this behavior for equivalent conversion policies.

### 18.2 Example

Suppose:

    Reference:
        USDTUSD = 0.9995
        Timestamp = 12:00:00.100

    Target Event:
        BTCUSDT Trade
        Timestamp = 12:00:00.200

The reference may be eligible.

Now suppose:

    Reference:
        USDTUSD = 0.9998
        Timestamp = 12:00:00.300

That later observation must not be used to convert the trade at 12:00:00.200.

### 18.3 Equal Timestamps

The historical contract requires careful handling of equal timestamps.

An external reference observation with the same timestamp as a target event is not automatically eligible merely because its merge rank is earlier.

For historical-compatible external reference alignment:

    ReferenceTimestamp < TargetTimestamp

remains the default requirement.

Any alternative equal-timestamp policy must be explicitly versioned.

### 18.4 Internal Reference Ordering

For internal references:

    Evaluate Current Event
             |
             v
    Incorporate Current Reference Observation

This prevents a trade from using its own price as a newly available conversion reference before the event has been evaluated.

### 18.5 Live Arrival Disorder

Consider:

    BTCUSDT Trade:
        EventTime 12:00:00.200
        Arrives First

    USDTUSD Reference:
        EventTime 12:00:00.100
        Arrives Later

Immediately converting the BTCUSDT trade upon arrival may use stale reference state.

The conversion engine must coordinate with event-time ordering so eligible reference observations are considered before final conversion.

### 18.6 Conversion Readiness Frontier

Each required reference stream must expose sufficient event-time progress to establish which observations are eligible.

Conceptually:

    ConversionReadinessFrontier

A target event may be finalized only when the conversion policy can safely evaluate its required reference state.

This may depend on source watermarks established by the Merge Engine.

### 18.7 Reference Watermarks

For strict event-time alignment, the conversion engine should wait until required reference streams have advanced beyond the target timestamp.

Conceptually:

    ReferenceWatermark > TargetEventTimestamp

A timestamp watermark that only reaches the target timestamp may not establish that all earlier reference observations have arrived, depending on the watermark contract.

The implementation must use the precise inclusive or exclusive semantics established in Part 2.

### 18.8 Waiting Limits

Reference alignment cannot wait indefinitely.

The runtime must enforce:

    MaximumReferenceWait

    MaximumConversionBufferEvents

    MaximumConversionBufferBytes

    ReferenceStalenessLimit

If readiness cannot be established, the conversion must follow an explicit unavailable or failure policy.

### 18.9 Conversion Finality

Once a converted event is published, its conversion result is final for that transformation revision.

A later reference observation must not silently modify the published event.

Reprocessing with improved references requires a separately identified correction or recomputation contract.

---

## 19. Conversion Staleness and Availability

A reference observation may exist but be too old to support a valid conversion.

The conversion engine must distinguish missing data from stale data.

### 19.1 Reference Age

Conceptually:

    ReferenceAge =
        TargetEventTimestamp
        -
        ReferenceEventTimestamp

For an eligible historical-compatible reference:

    ReferenceAge > 0

under the strict timestamp rule.

### 19.2 Maximum Staleness

Each conversion policy defines:

    MaximumReferenceStaleness

A reference is eligible only if its age satisfies the configured bound.

The initial implementation should preserve the historical configuration's staleness semantics.

### 19.3 Staleness Example

Suppose:

    ReferenceTimestamp:
        12:00:00

    TargetEventTimestamp:
        12:00:10

    MaximumStaleness:
        5 seconds

The reference is too old.

The conversion result must be classified as unavailable.

### 19.4 Conversion Outcomes

The historical conversion contract distinguishes:

    Identity

    Converted

    Unavailable

    Skipped

Live must preserve these statuses.

### 19.5 Failure Reasons

The historical conversion implementation includes failure classifications corresponding to:

    NoPath

    MissingReference

    StaleReference

    FutureReference

    ReferenceIdentityMismatch

    ArithmeticFailure

Live must preserve compatible failure semantics.

### 19.6 Unavailable Conversion

When conversion is unavailable:

- Original canonical values remain valid.
- Target converted values remain unavailable.
- A structured failure reason is attached.
- The source event is not silently discarded.
- The derived event follows the configured output policy.

### 19.7 Skipped Conversion

A skipped event is different from an unavailable conversion.

**Skipped** means the transformation policy deliberately excluded the event.

**Unavailable** means conversion was requested but could not be completed.

The system must preserve that distinction.

### 19.8 Source Failure

Reference disconnection does not immediately invalidate previously observed reference prices.

However, those observations become stale as target event time advances.

The engine must not use them after they exceed the configured staleness limit.

### 19.9 Availability Invariant

    A converted value must not
    be published as valid unless
    every required conversion
    reference satisfies the
    configured eligibility rules.

---

## 20. Numerical Precision and Conversion Arithmetic

The existing historical conversion pipeline uses higher-precision decimal arithmetic than the ordinary canonical Rust event types.

Live conversion must not silently reduce that precision.

### 20.1 Existing Numerical Models

The canonical source types use:

    rust_decimal::Decimal

The historical conversion pipeline uses:

    Decimal256

with a fixed output scale of:

    28

and precision of:

    76

These representations serve different purposes.

### 20.2 Conversion Precision

The Live Currency Conversion Engine must preserve numerical semantics compatible with the historical conversion implementation.

A converted value must not be forced into `rust_decimal::Decimal` if doing so would lose required precision.

### 20.3 Exact Ratio Arithmetic

The historical implementation evaluates conversion paths using exact rational arithmetic before producing the target decimal result.

The Live implementation must preserve equivalent behavior.

Conceptually:

    ConvertedValue =
        OriginalValue
        ×
        ConversionNumerator
        /
        ConversionDenominator

Intermediate rounding must not be introduced merely because a path contains multiple edges.

### 20.4 Multi-Hop Precision

For:

    EUR -> USDT -> USD

the conversion engine must avoid rounding the EUR/USDT result before applying the USDT/USD relationship unless the shared historical contract explicitly requires it.

The final result must follow the historical numerical precision policy.

### 20.5 Checked Arithmetic

All conversion operations must detect:

- Overflow.
- Division by zero.
- Unsupported decimal scale.
- Unrepresentable output.
- Invalid negative prices.
- Invalid conversion ratios.

Arithmetic failure must produce a structured unavailable result or an explicit transformation failure, according to the conversion contract.

### 20.6 Trade Conversion

Trade conversion may produce:

    PriceTarget

    QuantityQuoteTarget

The original trade price and quantities remain unchanged.

The output must preserve the distinction between source and target quotation currencies.

### 20.7 Depth Conversion

For a canonical depth event, conversion applies to the appropriate economic values of each level.

The historical implementation transforms:

    Price

and:

    QuantityQuote

into their target-currency equivalents.

Base quantity and contract quantity are not automatically converted into the target quote currency.

### 20.8 Atomic Depth Conversion

A depth event may contain multiple level changes.

The conversion operation must preserve event grouping.

If the conversion contract requires event-level atomicity, failure to convert one required level must prevent the event from being falsely classified as fully converted.

The historical depth-conversion behavior is authoritative.

### 20.9 Precision Invariant

    Source Values
        +
    Exact Conversion References
        +
    Shared Numerical Policy
        |
        v
    Historical-Compatible
    Converted Values

The Live implementation must not replace this with ordinary floating-point multiplication.

---

## 21. Conversion Output Contract

The Currency Conversion Engine produces output compatible with the historical converted merge representation.

### 21.1 Output Principles

A converted derived event must preserve:

- Original canonical event.
- Original source provenance.
- Target quote currency.
- Conversion status.
- Conversion failure reason.
- Applied conversion rate.
- Selected conversion path.
- Reference provenance.
- Converted trade or depth values.

### 21.2 Conceptual Output

    ConvertedDerivedEvent {
        derived_event_envelope

        source_event

        conversion {
            target_quote

            status
            failure

            rate
            path
            references

            trade_target
            depth_target
        }
    }

This is a logical representation, not a replacement for the existing historical physical schema.

### 21.3 Original Payload

The original canonical event is immutable.

Currency conversion must not overwrite:

    Trade.price

or:

    L2LevelUpdate.price

inside the source event.

Converted values are represented separately.

### 21.4 Conversion Status

Every conversion attempt produces one applicable status:

    Identity
    Converted
    Unavailable
    Skipped

The status determines which converted fields may be present.

### 21.5 Conversion Provenance

For successful nonidentity conversion, provenance must identify the reference observations used.

Relevant fields include:

    ReferenceStreamId

    ReferenceEventIdentity

    ReferenceTimestamp

    ReferencePrice

    ConversionDirection

    ConversionPath

The historical implementation's conversion-reference contract is authoritative.

Live-specific source cursors and runtime generations may supplement this information.

They must not replace the historical conversion-reference semantics.

### 21.6 Conversion Rate

The output must preserve the conversion rate actually applied to the event.

Conceptually:

    ConversionRate {
        numerator
        denominator
        evaluated_decimal
    }

The exact representation must remain compatible with the historical conversion contract.

The engine should preserve the exact ratio internally until the final numerical result is produced.

The exposed decimal rate must use the defined conversion precision policy.

### 21.7 Converted Trade Values

The existing historical trade-conversion output includes:

    PriceTarget

    QuantityQuoteTarget

These values must remain separate from:

    Trade.price

    Trade.quantity_quote

For example:

    Original Trade:

        Instrument: BTCUSDT

        Price: 65000 USDT

        QuantityBase: 0.5 BTC

        QuantityQuote: 32500 USDT

    Conversion:

        TargetQuote: USD

        ConversionRate: 0.9995

        Status: Converted

    Converted Values:

        PriceTarget: 64967.5 USD

        QuantityQuoteTarget: 32483.75 USD

The source trade remains unchanged.

### 21.8 Converted Depth Values

The existing historical depth-conversion representation preserves converted values for individual price levels.

Conceptually:

    ConvertedDepthLevel {
        price_target
        quantity_quote_target
    }

A depth event may contain multiple level changes.

The converted representation must preserve the event's original grouping and level ordering.

For example:

    Original Depth Event:

        Bid:
            Price: 65000 USDT
            QuantityBase: 2 BTC

        Ask:
            Price: 65010 USDT
            QuantityBase: 3 BTC

    Conversion:

        TargetQuote: USD
        ConversionRate: 0.9995

    Converted Levels:

        Bid:
            PriceTarget: 64967.5 USD

        Ask:
            PriceTarget: 64977.495 USD

The conversion operation must not merge independent source books or aggregate their liquidity.

### 21.9 Unavailable Conversion Output

When conversion is unavailable:

    ConversionStatus = Unavailable

The original canonical event remains valid.

Converted values must be absent or marked unavailable according to the shared historical schema.

The output must preserve:

    TargetQuote

    FailureReason

    OriginalCanonicalEvent

A missing conversion reference must not cause the original trade or depth event to disappear silently.

### 21.10 Skipped Conversion Output

When a configured policy excludes an event:

    ConversionStatus = Skipped

The output must preserve the original event identity and applicable provenance.

Skipped events and unavailable conversions must remain distinguishable.

### 21.11 Historical/Live Output Equivalence

For equivalent input events, reference observations, conversion paths, and numerical policies:

    Historical Conversion
              |
              v
       Converted Output A

    Live Conversion
              |
              v
       Converted Output B

The resulting economic values and conversion metadata must be equivalent.

Differences in:

    Runtime Publication Cursor

    Runtime Generation

    Unix Socket Framing

    Parquet File Layout

do not constitute differences in canonical conversion semantics.

### 21.12 Output Invariant

    Original Canonical Values
              +
    Historical-Compatible
    Conversion Metadata
              +
    Exact Converted Values
              |
              v
    One Derived Event Contract

No conversion may silently overwrite the original source event.

---

## 22. Currency Conversion Configuration

Conversion behavior must be defined explicitly and compiled before activation.

### 22.1 ConversionDefinition

Conceptual representation:

    ConversionDefinition {
        target_currency

        eligible_source_streams

        conversion_paths
        reference_streams

        maximum_reference_staleness

        unavailable_policy
        precision_policy

        configuration_revision
    }

The exact TOML structure belongs to **03 — Configuration System**.

### 22.2 Configuration Validation

The configuration compiler must validate:

- Target currency identity.
- Source instrument metadata.
- Reference stream identities.
- Conversion path directions.
- Supported conversion operators.
- Numerical precision requirements.
- Reference staleness limits.
- Unavailable conversion policies.
- Dependency graph compatibility.

Invalid conversion definitions must fail before activation.

### 22.3 Runtime Configuration Updates

Changes to the following affect transformation semantics:

    Target Currency

    Conversion Path

    Reference Selection

    Reference Staleness

    Numerical Precision

    Unavailable Policy

Such changes must advance the transformation revision.

The implementation must establish a controlled boundary between old and new conversion behavior.

### 22.4 Conversion Configuration Revisions

Every derived event must identify the transformation revision used to produce it.

For example:

    TransformationRevision 5:
        BTCUSDT -> USD
        Reference: USDTUSD Stream A

    TransformationRevision 6:
        BTCUSDT -> USD
        Reference: USDTUSD Stream B

Events produced under revision 5 must not be reinterpreted as revision 6.

### 22.5 Reference Dependency Updates

If a conversion definition adds or removes reference streams, the Combined Stream Registry must update its dependency graph.

Reference acquisition remains owned by the Stream Registry and Subscription Manager.

The conversion engine must not independently create exchange-native subscriptions.

---

## 23. Currency Conversion Invariants

The following invariants apply to all conversion-enabled derived streams.

### 23.1 Canonical Preservation

    Original canonical events
    remain immutable.

    Converted values are represented
    separately from source values.

    Original exchange and instrument
    identities remain available.

### 23.2 Historical Compatibility

    Live conversion follows the
    existing historical conversion
    semantics for equivalent operations.

    Conversion status definitions
    remain compatible.

    Conversion reference provenance
    remains compatible.

    Exact numerical behavior must not
    be silently weakened.

### 23.3 Temporal Integrity

    Future reference observations
    must not be used.

    Reference eligibility follows
    the configured event-time policy.

    Internal references are incorporated
    only after the current event has
    been evaluated when required by
    the historical contract.

    Published conversion results
    are immutable.

### 23.4 Resource Safety

    Reference-state memory is bounded.

    Pending conversion buffers are bounded.

    Missing references cannot cause
    indefinite accumulation.

    Reference acquisition is deduplicated
    through the Stream Registry.

### 23.5 Failure Semantics

    Unavailable conversion is explicit.

    Skipped conversion is distinct
    from unavailable conversion.

    Original source-event integrity
    is independent of conversion availability.

    Conversion failure must not
    silently remove accepted source events.

### 23.6 Derived Output

    Every converted event has a
    valid derived-stream identity.

    Every converted event retains
    its source-event identity.

    Conversion revisions are explicit.

    Output publication follows the
    selected derived-stream ordering contract.

---

## 24. Combined Stream Runtime Lifecycle

Each active combined stream is managed by one authoritative runtime instance.

The Combined Stream Registry owns its definition and lifecycle.

The Merge Engine owns ordering state.

The Currency Conversion Engine owns conversion state.

The Publisher owns downstream distribution.

These responsibilities must remain separate.

### 24.1 Runtime Lifecycle

Supported states:

    Created
    Validating
    PendingDependencies
    Initializing
    Active
    Degraded
    Recovering
    Stopping
    Stopped
    Failed

The lifecycle follows:

    Stream Definition
           |
           v
       Validation
           |
           v
    Dependency Resolution
           |
           v
       Initialization
           |
           v
         Active
           |
      +----+----+
      |         |
      v         v
    Degraded  Stopping
      |         |
      v         v
    Recovering Stopped
      |
      v
     Active

A failed recovery may transition the stream to `Failed`.

### 24.2 Runtime State

Conceptual representation:

    CombinedStreamRuntime {
        derived_stream_id
        runtime_generation

        definition_revision

        lifecycle_state

        source_dependencies
        reference_dependencies

        merge_state
        conversion_state

        input_frontiers
        output_frontier

        resource_reservations
        last_error
    }

The actual Rust implementation may distribute these fields among separate components.

### 24.3 Activation Requirements

A combined stream may become active only when:

- Its definition is valid.
- Its dependency graph is acyclic.
- Required source streams are registered.
- Input schemas are compatible.
- Required instrument metadata is available.
- Input routing boundaries are established.
- Merge buffers are initialized.
- Ordering policies are compiled.
- Required conversion dependencies are resolved.
- Output publication identity is established.
- Resource admission succeeds.

A source stream being connected does not automatically establish its processing readiness.

### 24.4 Activation Boundary

The Combined Stream Registry must coordinate input activation with the Publisher or internal canonical-event distribution mechanism.

Conceptually:

    Resolve Source Streams
             |
             v
    Establish Input Cursor Boundaries
             |
             v
    Activate Internal Routing
             |
             v
    Initialize Merge Runtime
             |
             v
    Begin Ordered Processing

No accepted source event required by the derived stream may disappear between boundary establishment and routing activation.

The implementation may use bounded buffering or the Live Retention Manager to bridge this interval.

### 24.5 Activation Without Historical Replay

A newly created combined stream may begin from:

    LatestAvailable

This establishes a new live input boundary.

It does not imply historical completeness before activation.

### 24.6 Activation With Historical Replay

A combined stream may request retained source events from an explicit starting cursor.

Each input stream has its own cursor domain.

The runtime must establish independent replay boundaries for all required sources.

The historical-to-live coordination rules from Document 09 apply.

### 24.7 Output Publication

Once a derived event is finalized:

    Merge Output
          |
          v
    Optional Currency Conversion
          |
          v
    Derived Event Validation
          |
          v
    Derived Publication Admission
          |
          v
    Publisher

The event receives a derived-stream publication identity.

The original source-event identity remains unchanged.

### 24.8 Stopping

A controlled stop must:

- Stop accepting new input assignments.
- Establish a termination boundary.
- Resolve buffered events according to policy.
- Stop conversion processing.
- Release input routing references.
- Release conversion-reference dependencies.
- Release resource reservations.
- Update the Stream Registry.
- Report unresolved output coverage.

The runtime must not silently discard buffered events while reporting a clean completion.

---

## 25. Source Failure Handling

Combined streams depend on independently managed source streams.

A source failure may impair ordering, conversion, or completeness without requiring the entire MarketForge runtime to stop.

### 25.1 Source Failure Categories

Relevant failures include:

    TransportUnavailable

    SubscriptionUnavailable

    ProcessingUnavailable

    TradeCoverageUnverified

    TradeCoverageIncomplete

    L2BookInvalid

    SourceCursorDiscontinuity

    SourceGenerationChanged

    SourceRemoved

The Combined Stream Registry receives authoritative source-health information.

It must not classify exchange failures solely from the absence of arriving events.

### 25.2 Required vs Optional Sources

Each input dependency declares whether it is required.

Conceptually:

    SourceDependency {
        stream_id

        required

        failure_policy
        recovery_policy
    }

A derived stream requiring all three exchanges has a different contract from one that permits temporary operation with only two.

### 25.3 Strict Source Policy

Under strict operation:

    Required Source Unavailable
               |
               v
    Derived Stream Degraded
               |
               v
    Watermark Progress Restricted
               |
               v
    Output Paused if Necessary

The runtime must not silently remove the failed source from its required input set.

### 25.4 Degraded Source Policy

A configured derived stream may permit continued operation using available sources.

Example:

    Binance = Active

    Bybit = Active

    OKX = Unavailable

The derived output may continue with:

    ActiveSources = 2

    RequiredSources = 3

However, its health must indicate incomplete source participation.

### 25.5 Degraded Output Metadata

A degraded derived stream must expose:

    RequiredSourceSet

    ActiveSourceSet

    UnavailableSourceSet

    CoverageStatus

    OrderingStatus

This information may be represented through runtime state and control-plane notifications rather than repeated in every data event.

The output must remain interpretable by downstream consumers.

### 25.6 Failure During Reordering

Suppose:

    Binance Watermark = 100

    Bybit Watermark = 105

    OKX Watermark = 90

Then OKX disconnects.

Under strict policy, the combined watermark cannot safely advance beyond its established frontier.

Under degraded operation, the runtime may exclude OKX according to the configured policy.

This weakens complete-source ordering guarantees.

If OKX later produces events behind the finalized output frontier, those events must follow the late-event policy.

### 25.7 Source Integrity Failure

A source may remain connected but become invalid.

For example:

    Binance L2 Sequence Gap

The BookStore invalidates its synchronized generation.

Any derived stream requiring that book must update its dependency state.

The Merge Engine must not treat the last accepted L2 state as indefinitely authoritative.

### 25.8 Source Recovery

After a source recovers, the derived runtime reevaluates:

- Source generation.
- Canonical cursor epoch.
- Current processing readiness.
- L2 book generation.
- Source event-time frontier.
- Missing coverage intervals.
- Retained replay availability.
- Merge ordering compatibility.

Source reconnection alone is insufficient to declare full derived-stream recovery.

---

## 26. Source Reintroduction

A previously unavailable source may resume after the combined stream has continued operating.

Reintroducing that source must not violate already finalized derived output ordering.

### 26.1 Reintroduction Problem

Consider:

    Combined Output Frontier:
        EventTime = 12:00:10

    OKX Recovers.

    First Recovered Event:
        EventTime = 12:00:07

The recovered event belongs before the finalized output frontier.

It cannot be inserted into the existing strict chronological sequence without violating ordering.

### 26.2 Reintroduction Policies

Supported conceptual policies:

    RequireCatchUp

    ResumeWithLateHandling

    RestartDerivedGeneration

### 26.3 RequireCatchUp

The source is reconciled through available retained or recovered events before rejoining ordinary derived output.

This is possible only when source coverage and merge ordering can be established.

If some events remain behind an already finalized strict output frontier, catch-up alone cannot repair the published sequence.

A new derived generation or separately versioned replay may be required.

### 26.4 ResumeWithLateHandling

The source rejoins current processing.

Events behind the finalized frontier follow the configured late-event policy.

The derived stream remains explicitly classified according to the resulting ordering guarantee.

### 26.5 RestartDerivedGeneration

The runtime establishes a new derived-stream generation.

Conceptually:

    Generation 5:
        Partial Source Participation

    Generation 6:
        Restored Source Participation

The new generation must establish a valid input boundary and output cursor identity.

It must not claim that generation 5 was retrospectively complete.

### 26.6 Reintroduction Validation

Before rejoining:

- Validate source identity.
- Validate source generation.
- Establish a fresh input cursor boundary.
- Evaluate retained historical coverage.
- Evaluate the current merge watermark.
- Determine whether pending events are late.
- Apply the configured reintroduction policy.

### 26.7 Source Reintroduction Invariant

    A recovered source cannot
    retroactively change already
    published derived events.

Any correction requires an explicit new output contract.

---

## 27. Conversion Reference Failures

Currency conversion introduces dependencies beyond the market-data sources being merged.

A reference stream may fail independently.

### 27.1 Independent Failure Domains

Example:

    Binance BTCUSDT Trades:
        Healthy

    USDTUSD Reference:
        Unavailable

The BTCUSDT source trade remains valid.

Its USD conversion may become unavailable.

### 27.2 Reference Failure Conditions

Relevant conditions include:

    ReferenceDisconnected

    ReferenceUnavailable

    ReferenceStale

    ReferenceCursorDiscontinuity

    ReferenceMetadataInvalid

    ConversionPathUnavailable

    ArithmeticFailure

These conditions must not be reported as native trade-integrity failures.

### 27.3 Reference Staleness

When a reference stops updating, its last observation remains in reference state.

However, conversion eligibility depends on event-time staleness.

Conceptually:

    ReferenceAge =
        TargetEventTimestamp
        -
        ReferenceEventTimestamp

A disconnected reference is not automatically unusable if the existing observation remains eligible.

A connected reference is not automatically usable if its last observation is stale.

### 27.4 Conversion Failure Policy

Supported conceptual policies:

    EmitUnavailable

    PauseConversion

    FailDerivedStream

**EmitUnavailable**

Preserve the source event with:

    ConversionStatus = Unavailable

**PauseConversion**

Temporarily delay output while waiting for reference readiness.

Waiting must remain bounded.

**FailDerivedStream**

Stop output when the required conversion contract cannot be satisfied.

### 27.5 Historical Compatibility

The default conversion-result representation must preserve the historical statuses:

    Identity
    Converted
    Unavailable
    Skipped

Live-specific failure escalation must not redefine these conversion statuses.

### 27.6 Reference Recovery

After a reference stream recovers:

- Validate its current generation.
- Establish ordering compatibility.
- Update reference state through eligible observations.
- Reevaluate conversion readiness.
- Resume normal conversion where permitted.

Previously published unavailable conversions must not be silently rewritten.

### 27.7 Reference Recovery Invariant

    Currency conversion availability
    is independent of original
    source-event validity.

---

## 28. Dynamic Combined Stream Configuration

MarketForge must support controlled creation, modification, and removal of derived streams while the runtime remains operational.

These operations are initiated through the runtime control interface.

The Ratatui TUI is a client of that interface.

### 28.1 Supported Operations

The initial control plane supports:

    CreateCombinedStream

    ActivateCombinedStream

    DeactivateCombinedStream

    UpdateCombinedStream

    RemoveCombinedStream

    GetCombinedStream

    ListCombinedStreams

    GetCombinedStreamHealth

### 28.2 Configuration Ownership

The Combined Stream Registry owns runtime derived-stream definitions.

The Configuration System owns validation and compilation of persistent configuration.

The Stream Registry remains authoritative for logical stream identity.

No TUI component may directly mutate live merge buffers.

### 28.3 Creation

Conceptually:

    Create Request
          |
          v
    Validate Definition
          |
          v
    Validate Input Streams
          |
          v
    Validate Dependency Graph
          |
          v
    Validate Conversion Rules
          |
          v
    Reserve Configuration Identity
          |
          v
    Register Derived Stream

Creating a definition does not necessarily activate it.

### 28.4 Activation

Activation acquires source dependencies and initializes the processing graph.

Shared source streams must be reused.

The registry must not create duplicate exchange-native subscriptions.

### 28.5 Update Categories

Configuration updates are classified as:

    MetadataOnly

    Operational

    Semantic

**MetadataOnly**

Examples:

    Display Name
    Description

These may apply without replacing the merge runtime.

**Operational**

Examples:

    Metrics Enabled
    Diagnostic Sampling

These may apply at defined safe boundaries.

**Semantic**

Examples:

    Input Source Set
    Source Ranking
    Ordering Policy
    Target Currency
    Conversion Path
    Conversion Staleness Policy
    Late Event Policy

These affect output interpretation.

They require a controlled transformation revision.

### 28.6 Semantic Update Procedure

    Proposed Update
          |
          v
    Validate New Definition
          |
          v
    Construct Replacement Runtime
          |
          v
    Resolve New Dependencies
          |
          v
    Establish New Input Boundaries
          |
          v
    Fence Previous Generation
          |
          v
    Activate Replacement Generation
          |
          v
    Release Obsolete Resources

The transition must not expose two authoritative runtimes publishing conflicting events under the same derived cursor identity.

### 28.7 Output Continuity

A semantic update may establish a new output epoch.

Example:

    Before:
        Combined BTC Trades in USDT

    After:
        Combined BTC Trades in USD

These are economically different products.

The implementation should normally treat them as distinct stream definitions or explicitly revisioned derived generations.

It must not silently change quotation units in the middle of an otherwise unchanged output sequence.

### 28.8 Input Set Changes

Adding or removing a source changes the merge domain.

Example:

    Revision 1:
        Binance + Bybit

    Revision 2:
        Binance + Bybit + OKX

The new definition must establish a valid input boundary for OKX.

Previously published events remain associated with revision 1.

### 28.9 Removal

Removing a combined stream must:

- Mark its desired state as removed.
- Stop its runtime.
- Fence obsolete generations.
- Release merge buffers.
- Release conversion dependencies.
- Release source references.
- Remove downstream availability.
- Preserve required operational diagnostics.

Source streams required elsewhere must remain active.

---

## 29. Merge and Conversion Resource Management

Combined streams require bounded memory and processing capacity.

The subsystem must remain isolated from latency-sensitive exchange acquisition.

### 29.1 Resource Categories

Relevant resources include:

    MergeBufferMemory

    PendingInputEvents

    ConversionReferenceMemory

    PendingConversionEvents

    DerivedOutputQueueMemory

    MergeProcessingConcurrency

    ConversionProcessingConcurrency

    ActiveDerivedStreams

### 29.2 ResourceBudget

Conceptual representation:

    DerivedResourceBudget {
        maximum_derived_streams

        maximum_merge_buffer_bytes
        maximum_merge_buffer_events

        maximum_conversion_buffer_bytes
        maximum_conversion_buffer_events

        maximum_reference_state_bytes

        maximum_processing_concurrency
    }

The exact configuration keys belong to Document 03.

### 29.3 Global and Per-Stream Budgets

The runtime must enforce both:

    Global Derived Resource Limits

and:

    Per-Derived-Stream Limits

A single high-volume combined stream must not consume all available merge memory without an explicit policy.

### 29.4 Shared Event References

The Merge Engine should use immutable references to canonical source events where practical.

Conceptually:

    Arc<CanonicalEvent>

This avoids unnecessarily copying large depth payloads across derived-stream dependencies.

Each retained reference must remain accounted for under the applicable resource budget.

### 29.5 Merge Buffer Saturation

When the reorder buffer becomes saturated:

    Buffer Saturated
           |
           v
    Evaluate Watermark
           |
           v
    Release Eligible Events
           |
           v
    Reevaluate Capacity

If no events are eligible for release, the runtime must apply its configured failure or resource policy.

It must not force-release events while continuing to claim strict chronological ordering.

### 29.6 Conversion Buffer Saturation

A conversion buffer may fill while waiting for reference-stream progress.

Possible responses include:

- Rejecting additional optional work.
- Reporting unavailable conversion.
- Failing the affected derived stream.
- Applying bounded upstream pressure.

The selected response must follow the conversion policy.

### 29.7 Backpressure Limitations

Backpressure inside the Merge Engine does not guarantee that an exchange will stop generating data.

If source processing is delayed long enough, exchange-side acquisition may become impaired.

Therefore, the runtime should:

- Use bounded internal queues.
- Prioritize essential source processing.
- Isolate derived workloads.
- Avoid blocking WebSocket readers.
- Report resource pressure before exhaustion.

### 29.8 Retention Relationship

The Live Retention Manager may preserve accepted source or derived events for replay.

However, it is not an implicit unbounded extension of the Merge Engine's reorder buffer.

Recovery of a failed merge runtime requires explicit source cursor and retention-coverage validation.

### 29.9 Resource Exhaustion

If a derived stream cannot maintain its required resource guarantees:

    Resource Exhaustion
           |
           v
    Derived Stream Degraded
           |
           v
    Failure Report
           |
           v
    Recovery Coordinator

Healthy source streams should continue operating whenever possible.

### 29.10 Resource Isolation Invariant

    Derived-stream processing
    must not introduce unbounded
    resource consumption or
    uncontrolled blocking into
    canonical source acquisition.

---

## 30. Recovery Coordination

Derived-stream failures are reported to the Recovery Coordinator using the shared failure-reporting contract from Document 07.

The Combined Stream Registry remains responsible for implementing derived-stream lifecycle transitions.

### 30.1 Recovery Targets

Supported conceptual targets:

    DerivedStream

    MergeRuntime

    ConversionRuntime

    SourceDependency

    ReferenceDependency

### 30.2 Recovery Actions

Relevant actions include:

    RestartMergeRuntime

    ReestablishInputBoundaries

    ReplayRetainedInputs

    RestoreConversionReferences

    RebuildConversionState

    StartNewDerivedGeneration

    EscalateDerivedFailure

The coordinator orchestrates these actions through their owning components.

### 30.3 Recovery Inputs

Recovery planning considers:

- Desired derived-stream state.
- Current runtime generation.
- Definition revision.
- Source dependency health.
- Reference dependency health.
- Available retention coverage.
- Merge output frontier.
- Ordering policy.
- Conversion state.
- Available resource capacity.

### 30.4 Merge Runtime Failure

If the Merge Engine fails unexpectedly:

- Fence the failed runtime generation.
- Preserve the last authoritative output frontier.
- Identify unprocessed accepted source inputs.
- Validate available replay coverage.
- Reconstruct the merge state when possible.
- Establish a replacement runtime generation.
- Resume output under the applicable ordering contract.

The runtime must not assume that buffered but unpublished events survived the failure.

### 30.5 Merge Recovery From Retention

Replay may reconstruct a derived timeline when the required source events remain available.

However, successful reconstruction requires:

- Complete relevant source coverage.
- Compatible source identities.
- Compatible ordering definitions.
- Compatible conversion references.
- Correct initialization state.
- A valid output publication boundary.

Retention availability alone does not prove these requirements.

### 30.6 Conversion State Recovery

A conversion runtime may need to reconstruct reference state from historical or retained observations.

The historical implementation already uses reference checkpoints for parallel processing.

Live may use analogous checkpointing when necessary.

A checkpoint must preserve sufficient temporal and identity information to avoid look-ahead bias.

### 30.7 Partial Recovery

A derived stream may restore current operation while historical derived coverage remains incomplete.

Example:

    CurrentInputDelivery = Restored

    MergeRuntime = Active

    HistoricalDerivedCoverage = Incomplete

The system must preserve these independent outcomes.

### 30.8 Recovery Completion

A derived recovery operation is complete only when the required guarantees are established.

Possible guarantees include:

    DependenciesRestored

    OrderingRestored

    ConversionRestored

    PublicationRestored

    HistoricalCoverageRestored

The Recovery Coordinator must not treat a restarted merge task as proof of complete derived-stream recovery.

---

## 31. Derived Stream Publication

Derived streams publish through the same Publisher infrastructure as ordinary canonical source streams.

They do not require separate Unix socket servers or subscriber-delivery implementations.

### 31.1 Publication Architecture

    Canonical Source Streams
              |
       +------+------+
       |      |      |
       v      v      v
    Binance Bybit   OKX
       |      |      |
       +------+------+
              |
              v
         Merge Engine
              |
              v
    Optional Currency Conversion
              |
              v
     Derived Event Validation
              |
              v
     Derived Stream Publisher
              |
              v
       Shared IPC Publisher
              |
       +------+------+
       |             |
       v             v
    Subscriber A  Subscriber B

The Derived Stream Publisher is a logical publication interface, not necessarily a separate physical component.

### 31.2 Source and Derived Stream Routing

The existing Publisher routes using logical stream identities.

For example:

    Source Stream 101:
        Binance BTCUSDT Trades

    Source Stream 102:
        Bybit BTCUSDT Trades

    Derived Stream 501:
        Combined BTCUSDT Trades

A subscriber may request any permitted combination:

    Subscriber A:
        Stream 101

    Subscriber B:
        Stream 501

    Subscriber C:
        Stream 101
        Stream 501

The same source event may appear in both direct and derived subscriptions.

This is intentional.

The representations have distinct publication identities and contracts.

### 31.3 One Merge Runtime, Multiple Subscribers

Multiple consumers subscribing to one derived stream must share the same authoritative merge runtime.

Conceptually:

    Combined BTC Trades
             |
             v
      One Merge Runtime
             |
             v
          Publisher
             |
       +-----+-----+
       |     |     |
       v     v     v
       A     B     C

The runtime must not create an independent merge operation for each subscriber.

This preserves consistency and reduces duplicated computation.

### 31.4 Derived Publication Identity

Every accepted derived event receives:

    DerivedStreamId

    CursorEpoch

    DerivedStreamCursor

These identifiers describe the derived publication sequence.

The original source event identity must remain separately available.

### 31.5 Publication Ordering

Within a derived stream, publication must preserve the order finalized by the Merge Engine.

The Publisher must not independently reorder derived events.

If the derived stream uses:

    StrictEventTime

the Publisher must preserve its finalized merge-key order.

If the derived stream permits late events, the Publisher must preserve the resulting declared output order and late-event metadata.

### 31.6 Publication Admission

A derived event is committed only after:

- The merge operator finalizes its position.
- Required conversion processing completes.
- The output schema validates.
- The current runtime generation is valid.
- A publication identity is assigned.
- The Publisher accepts the event.

The Merge Engine must not advance its authoritative output frontier beyond an event whose required publication admission has failed.

### 31.7 Publication Failure

If the Publisher cannot accept a derived event:

    Derived Event Ready
              |
              v
    Publication Admission Failed
              |
              v
    Preserve Pending Output State
              |
              v
    Apply Bounded Recovery Policy

The event must not disappear silently.

The runtime must avoid assigning the same derived publication cursor to incompatible event payloads during retry.

### 31.8 Derived Stream Availability

A derived stream may be:

    Active

while its Publisher delivery is:

    Degraded

The runtime must distinguish:

    Merge Processing Health

from:

    Subscriber Delivery Health

Successful merging does not prove successful Unix socket delivery.

### 31.9 Publication Invariant

    Each accepted derived event
    has one authoritative
    derived publication identity.

    The original source identity
    remains independently available.

    The Publisher preserves the
    Merge Engine's finalized order.

---

## 32. Derived Stream Retention

Derived streams may use the Live Retention Manager defined in Document 09.

Retention is optional unless a consumer or processing contract requires it.

### 32.1 Retention Categories

A derived stream may use:

    NoRetention

    MemoryRetention

    MemoryAndParquetRetention

The effective policy is defined by the Configuration System.

### 32.2 Retention Independence

Source and derived retention are separate.

Example:

    Binance BTCUSDT Trades
        Retention Enabled

    Bybit BTCUSDT Trades
        Retention Enabled

    Combined BTCUSDT Trades
        Retention Disabled

This is valid.

The combined stream can still publish live events.

However, its own derived publication history is not automatically available for cursor replay.

### 32.3 Derived Retention Identity

Retained derived events preserve:

    DerivedStreamId

    DerivedCursorEpoch

    DerivedCursor

    SourceEventIdentity

    TransformationRevision

    OrderingPolicyRevision

    ConversionMetadata

These identities must remain recoverable through the retention contract.

### 32.4 Source Retention vs Derived Retention

Retaining source events may permit recomputing a derived stream.

However:

    Source Events Available
        !=
    Derived Replay Automatically Available

Reconstruction also requires:

- The original derived-stream definition.
- Compatible source rankings.
- Applicable ordering policy.
- Required conversion references.
- Historical conversion state.
- Source coverage.
- Deterministic replay logic.

The Publisher must not claim that source retention automatically guarantees derived replay.

### 32.5 Historical Parquet Compatibility

When derived events are spilled to Parquet, their market-data representation must remain compatible with the corresponding historical merged or converted dataset schema.

The existing historical implementation is authoritative for:

    Trades

    Depth

    Combined

and their corresponding currency-converted outputs.

Live-specific cursor and retention metadata must remain separate where possible.

Per-event identity information required for exact replay must remain recoverable.

### 32.6 Recomputed Derived Streams

A derived stream may be reconstructed from retained inputs when the necessary history and transformation state are available.

Recomputation must identify:

    Input Coverage

    Transformation Revision

    Ordering Revision

    Reference State

    Output Generation

A newly recomputed sequence must not silently overwrite an existing authoritative derived sequence with different results.

### 32.7 Retention Failure

Derived retention failure does not automatically invalidate canonical source streams.

However, it may prevent:

- Subscriber replay.
- Derived-stream recovery.
- Historical bootstrap.
- Reconstruction after process restart.

The runtime must report the affected guarantees independently.

---

## 33. Derived Stream Recording

Derived streams may optionally be recorded through the existing PostgreSQL Recording Manager.

Recording is independent of both merging and Unix socket publication.

### 33.1 Recording Policy

Each derived stream may declare:

    RecordingEnabled

    RecordingBatchPolicy

    RecordingRetentionPolicy

The exact configuration belongs to Document 03.

### 33.2 Recording Ownership

The Recording Manager owns:

- Database connectivity.
- Recording batches.
- Persistence identities.
- Commit reconciliation.
- Recording failure handling.

The Merge Engine must not directly write to PostgreSQL.

### 33.3 Recording Identity

Recorded derived events must preserve:

    DerivedStreamId

    DerivedEventIdentity

    SourceEventIdentity

    TransformationRevision

The database persistence identity must support idempotent retries according to the Recording Manager's contract.

### 33.4 Recording Failure Isolation

A PostgreSQL outage may produce:

    Derived Merge = Active

    Derived Publication = Active

    Derived Recording = Degraded

This is valid when recording is optional and the configured persistence policy permits continued operation.

### 33.5 Original Source Preservation

Recording a converted derived event must preserve the distinction between:

    Original Canonical Values

and:

    Converted Target Values

The database schema must not silently overwrite source prices or quantities with converted values.

---

## 34. Derived Stream Observability

Combined streams require independent visibility into input progress, output ordering, conversion availability, and resource usage.

The Metrics Registry remains the authoritative metrics infrastructure.

### 34.1 DerivedStreamHealthSnapshot

Conceptual representation:

    DerivedStreamHealthSnapshot {
        derived_stream_id
        runtime_generation

        lifecycle_state

        required_sources
        active_sources
        degraded_sources

        combined_watermark
        last_output_event_time

        buffered_events
        buffered_bytes

        late_events
        ordering_status

        conversion_status
        reference_health

        publication_status
        retention_status
        recording_status

        last_error
        evaluated_at
    }

This snapshot is diagnostic.

It must not require copying every buffered event.

### 34.2 Merge Metrics

| Metric | Description |
|---|---|
| `merge_events_received_total` | Source events admitted to merge |
| `merge_events_emitted_total` | Derived events emitted |
| `merge_buffer_events` | Current buffered event count |
| `merge_buffer_bytes` | Current reorder-buffer memory |
| `merge_watermark_ns` | Current combined watermark |
| `merge_watermark_lag_ns` | Diagnostic event-time lag |
| `merge_late_events_total` | Events behind finalized frontier |
| `merge_ordering_failures_total` | Ordering contract violations |
| `merge_buffer_overflows_total` | Buffer capacity failures |
| `merge_source_stalls_total` | Detected source-progress stalls |

Metrics must distinguish source-level and derived-stream-level states.

### 34.3 Conversion Metrics

| Metric | Description |
|---|---|
| `conversion_events_total` | Events evaluated |
| `conversion_identity_total` | Identity conversion results |
| `conversion_converted_total` | Successful conversions |
| `conversion_unavailable_total` | Unavailable conversions |
| `conversion_skipped_total` | Deliberately skipped conversions |
| `conversion_reference_updates_total` | Reference observations incorporated |
| `conversion_reference_stale_total` | Stale-reference failures |
| `conversion_reference_missing_total` | Missing-reference failures |
| `conversion_arithmetic_failures_total` | Numerical conversion failures |
| `conversion_pending_events` | Events awaiting conversion readiness |
| `conversion_processing_latency` | Conversion processing latency |

Conversion statuses must match the historical contract.

### 34.4 Source Participation Metrics

A combined stream should expose:

    ConfiguredSourceCount

    ActiveSourceCount

    DegradedSourceCount

    UnavailableSourceCount

The system should also identify which required source is limiting the combined watermark.

### 34.5 Resource Metrics

Relevant measurements include:

    DerivedRuntimeMemoryBytes

    MergeBufferBytes

    ConversionBufferBytes

    ReferenceStateBytes

    ActiveDerivedStreams

    PendingDerivedEvents

These values support the runtime-wide resource manager.

### 34.6 TUI Integration

The Ratatui Merges view should display:

    Derived Stream

    Type

    Active Sources

    Ordering Policy

    Watermark Lag

    Buffered Events

    Late Events

    Conversion Target

    Conversion Health

    Runtime State

A detail view may show:

    DerivedStreamId
    RuntimeGeneration

    Input Dependencies
    Reference Dependencies

    Source Watermarks
    Combined Watermark

    Output Cursor
    Ordering Revision

    Conversion Path
    Reference Staleness

    Buffer Usage
    Last Error

The TUI must not implement merge or conversion algorithms.

### 34.7 Runtime API

Conceptual control operations include:

    ListCombinedStreams

    GetCombinedStream

    CreateCombinedStream

    UpdateCombinedStream

    ActivateCombinedStream

    DeactivateCombinedStream

    RemoveCombinedStream

    GetMergeHealth

    GetConversionHealth

    GetMergeDependencies

    GetMergeMetrics

All mutations must pass through validated control-plane commands.

### 34.8 Local Structured Logging

Relevant events include:

    CombinedStreamCreated

    CombinedStreamActivated

    CombinedStreamDegraded

    CombinedStreamRecovered

    CombinedStreamStopped

    SourceDependencyUnavailable

    MergeWatermarkStalled

    MergeLateEventDetected

    MergeBufferSaturated

    ConversionReferenceUnavailable

    ConversionReferenceStale

    ConversionFailed

    CombinedStreamGenerationReplaced

Operational logs remain local.

PostgreSQL must not be used for operational logging.

---

## 35. Historical/Live Equivalence Testing

The historical merge and conversion implementations provide reference behavior for equivalent Live operations.

The Live subsystem must be tested against that reference.

### 35.1 Testing Objective

Given identical canonical source inputs and equivalent transformation definitions, the Live implementation should produce economically and semantically equivalent output.

Differences caused by bounded lateness, incomplete Live source coverage, or explicitly configured degraded operation must remain observable.

### 35.2 Reference Implementations

Relevant historical components include:

    historical/merge/core/

    historical/merge/materialize/

    historical/convert/execution/combined/

    historical/convert/reference/

    historical/convert/output/schema/

These establish the current reference behavior for:

- Merge ordering.
- Source identity.
- Trade/depth grouping.
- Currency conversion.
- Reference selection.
- Exact numerical arithmetic.
- Conversion status.
- Output provenance.

### 35.3 Canonical Input Fixtures

Tests must use identical canonical inputs for historical and Live processing.

Fixtures should include:

    Multiple Exchanges

    Multiple Instruments

    Equal Timestamps

    Out-of-Order Arrival

    Trade Events

    Depth Events

    Currency References

    Missing References

    Stale References

    Source Generation Changes

### 35.4 Ordering Equivalence

The historical merge uses deterministic composite ordering keys.

The Live implementation must reproduce the equivalent ordering for events admitted before their applicable finalization boundaries.

Example:

    Event A:
        Timestamp = 100
        StreamRank = 0

    Event B:
        Timestamp = 100
        StreamRank = 1

    Event C:
        Timestamp = 101
        StreamRank = 0

Expected:

    A
    B
    C

Arrival order must not determine the final ordering when all events are within the permitted lateness window.

### 35.5 Mixed Event Equivalence

The historical combined format supports trade and depth events in one chronological timeline.

Tests must verify:

- Correct event-kind discrimination.
- Correct trade payloads.
- Correct depth-event grouping.
- Correct source provenance.
- Deterministic tie-breaking.

Depth events containing multiple level changes must not be flattened into unrelated events during logical comparison.

### 35.6 Conversion Equivalence

For identical input values and reference observations, verify:

    Target Currency

    Conversion Status

    Conversion Failure

    Conversion Rate

    Conversion Path

    Conversion References

    Converted Price

    Converted Quote Quantity

The expected numerical results must match the historical precision and rounding contract.

### 35.7 Internal Reference Ordering

Simulate one combined timeline containing internal conversion-reference trades.

Verify:

    Evaluate Current Event
             |
             v
    Update Internal Reference State

The current event must not use its own reference observation before the historical ordering contract permits it.

### 35.8 External Reference Ordering

Simulate external reference observations before, equal to, and after the target event timestamp.

Verify that the Live implementation reproduces the historical strict-before alignment rule.

### 35.9 Precision Tests

Include:

- Very small conversion rates.
- Large decimal values.
- Multi-hop conversion.
- Inverse conversion.
- Contract-derived quote quantities.
- Exact identity conversion.
- Arithmetic overflow.
- Unsupported scale.
- Unavailable conversion.

The Live implementation must not silently reduce historical Decimal256 precision to ordinary Decimal precision.

### 35.10 Equivalence Limitations

Historical processing has access to completed datasets.

Live processing uses bounded buffering and watermarks.

Therefore, equivalence requires controlled input conditions.

The test suite must not claim identical output for arbitrary unbounded-lateness arrival patterns.

Instead, it must verify that deviations are classified correctly.

---

## 36. Fault-Injection Testing

Derived-stream processing must remain correct when sources fail, references become stale, or runtime generations change.

### 36.1 Source Disconnection

Simulate:

    Binance Active

    Bybit Active

    OKX Disconnects

Verify:

- Required-source policy is applied.
- Combined watermark behavior is correct.
- Buffer limits remain enforced.
- Degraded source participation is visible.
- Healthy source streams remain operational.

### 36.2 Source Reconnection

Reconnect the failed source with events older than the derived output frontier.

Verify:

- Late-event policy is applied.
- Already published output remains immutable.
- Source generation changes are validated.
- No silent insertion into finalized strict output occurs.

### 36.3 L2 Synchronization Failure

Invalidate one source BookStore generation.

Verify:

- Affected derived dependencies become degraded.
- Invalid source L2 state is not treated as synchronized.
- Other source books remain independently valid.
- Recovery requires a new compatible generation.

### 36.4 Conversion Reference Outage

Disable one required conversion reference.

Verify:

- Source events remain valid.
- Conversion availability changes according to reference staleness.
- Unavailable results preserve original payloads.
- Recovery does not rewrite previously published conversions.

### 36.5 Merge Buffer Saturation

Prevent one required watermark from advancing while other sources continue producing events.

Verify:

- Buffer capacity is enforced.
- Memory remains bounded.
- Strict ordering is not silently weakened.
- Resource failure is reported.
- Other unrelated derived streams remain operational where possible.

### 36.6 Publisher Failure

Fail derived publication admission.

Verify:

- Output frontier does not falsely advance.
- Pending output remains bounded.
- Duplicate cursor assignments are prevented.
- Recovery follows the Publisher contract.

### 36.7 Runtime Replacement

Start a replacement merge generation while an obsolete generation has pending asynchronous completions.

Verify that obsolete results cannot publish authoritative events.

---

## 37. Property-Based Testing

Property-based tests should generate arbitrary sequences of source events, failures, and configuration changes.

### 37.1 Operations

Generated operations may include:

    AddSourceEvent

    AdvanceSourceWatermark

    DisconnectSource

    ReconnectSource

    ChangeSourceGeneration

    AddReferenceObservation

    ExpireReference

    UpdateMergeDefinition

    ChangeOrderingPolicy

    ChangeConversionPath

    SaturateMergeBuffer

    FailPublication

    RestartMergeRuntime

### 37.2 Ordering Invariants

    Finalized output is deterministic
    under the selected ordering policy.

    Strict output never accepts an event
    behind its finalized frontier.

    Watermarks never regress within
    one authoritative merge generation.

    Source ranking remains stable
    within one definition revision.

### 37.3 Identity Invariants

    Original canonical events
    retain their source identity.

    Derived events receive separate
    publication identities.

    Shared source subscriptions
    are not duplicated unnecessarily.

    Obsolete generations cannot
    modify current output.

### 37.4 Conversion Invariants

    Source values remain immutable.

    Future references are not used.

    Stale references produce explicit
    unavailable results.

    Conversion statuses match the
    historical contract.

    Exact arithmetic remains compatible
    with historical conversion semantics.

### 37.5 Resource Invariants

    Merge buffers remain bounded.

    Conversion buffers remain bounded.

    Reference state remains bounded.

    Failed operations release resources.

    One stalled derived stream cannot
    consume unlimited runtime memory.

---

## 38. Performance and Benchmarking

The Combined Streams subsystem must support sustained high-throughput processing without becoming a bottleneck for exchange acquisition.

### 38.1 Benchmark Categories

Benchmark:

- Merge input routing.
- Merge-key construction.
- Priority-queue insertion.
- Watermark evaluation.
- Ordered event release.
- Conversion reference updates.
- Conversion path evaluation.
- Exact decimal arithmetic.
- Derived publication admission.
- Multi-subscriber fan-out.
- Configuration generation replacement.

### 38.2 Scaling Dimensions

Benchmarks should vary:

    InputStreamCount

    EventRate

    TradeToDepthRatio

    AverageDepthEventSize

    ReorderBufferSize

    AllowedLateness

    ReferenceStreamCount

    ConversionPathLength

    DerivedStreamCount

    SubscriberCount

### 38.3 Historical Benchmark Comparison

The historical merge implementation provides useful throughput and memory baselines.

However, historical batch throughput is not directly equivalent to Live streaming latency.

Benchmarks must distinguish:

    Offline Batch Throughput

from:

    Live End-to-End Latency

### 38.4 Latency Measurements

Measure:

    Source Event Accepted
              |
              v
    Merge Buffer Admission
              |
              v
    Watermark Release
              |
              v
    Conversion Completion
              |
              v
    Publisher Admission
              |
              v
    Unix Socket Write

The measured end-to-end delay includes intentional event-time buffering.

It must not be interpreted solely as CPU processing latency.

### 38.5 Performance Targets

Concrete throughput and latency targets must be established through representative benchmarks.

The system must not assume arbitrary performance guarantees.

Optimization must preserve correctness, precision, and ordering.

---

## 39. Implementation Acceptance Criteria

The Combined Streams subsystem is complete when the following requirements are satisfied.

### 39.1 Architecture

- Derived streams are first-class logical streams.
- Source and derived streams remain independently subscribable.
- One merge runtime can serve multiple subscribers.
- Shared source subscriptions are deduplicated.
- Dependency graphs are validated.
- Derived-stream lifecycles are generation-safe.

### 39.2 Historical Compatibility

- Existing canonical event types are reused.
- Historical merge modes are supported.
- Historical combined-event grouping is preserved.
- Merge-key semantics remain compatible.
- Historical conversion statuses are preserved.
- Conversion provenance remains compatible.
- Exact converted values follow the shared numerical contract.
- No incompatible duplicate canonical schema is introduced.

### 39.3 Ordering

- Per-source event identity is preserved.
- Deterministic stream ranking is supported.
- Reorder buffers remain bounded.
- Watermark semantics are explicit.
- Equal-timestamp ordering is deterministic.
- Late events are handled explicitly.
- Strict ordering violations cannot be hidden.
- Historical equivalence is claimed only when its assumptions hold.

### 39.4 Currency Conversion

- Conversion paths are validated.
- Internal and external references are supported.
- Future references are rejected.
- Reference staleness is enforced.
- Original market-data values remain unchanged.
- Converted values are represented separately.
- Missing references produce explicit outcomes.
- Decimal256-equivalent precision is preserved.

### 39.5 Failure Recovery

- Source failures update derived-stream health.
- Reference failures remain independently classified.
- Source reintroduction follows a defined policy.
- Merge runtime crashes are recoverable where retention permits.
- Obsolete runtime generations cannot publish.
- Partial historical coverage remains visible.

### 39.6 Resource Safety

- Merge memory is bounded.
- Conversion memory is bounded.
- Reference state is bounded.
- Publisher admission failures are explicit.
- Runtime resource budgets are enforced.
- Healthy source acquisition remains isolated from optional derived workloads where possible.

### 39.7 Observability

- Merge watermark state is exposed.
- Buffered-event counts are available.
- Late events are measurable.
- Source participation is visible.
- Conversion failure reasons are reported.
- Derived publication health is exposed.
- TUI and runtime API support combined-stream management.
- Operational logs remain local.

### 39.8 Testing

- Merge ordering tests pass.
- Historical equivalence tests pass.
- Mixed trade/depth tests pass.
- Currency conversion precision tests pass.
- Reference alignment tests pass.
- Late-event tests pass.
- Source failure tests pass.
- Runtime generation tests pass.
- Resource exhaustion tests pass.
- Property-based tests pass.
- Performance benchmarks establish acceptable operating characteristics.

---

## 40. Final Architectural Contract

The Combined Streams subsystem transforms accepted canonical source events into reusable derived streams.

It operates entirely inside the Rust MarketForge Live runtime.

The architecture is:

    Exchange Connections
             |
             v
    Canonical Normalization
             |
             v
      Integrity Validation
             |
             v
    Canonical Source Streams
             |
       +-----+-----+
       |           |
       v           v
    Direct       Merge Engine
    Delivery         |
       |             v
       |       Event-Time Ordering
       |             |
       |             v
       |       Currency Conversion
       |             |
       |             v
       |       Derived Streams
       |             |
       +------+------+ 
              |
              v
           Publisher
              |
              v
       Unix Domain Sockets
              |
       +------+------+
       |             |
       v             v
    Python         Rust
    Consumers      Consumers

The subsystem guarantees:

- Reuse of existing canonical source events.
- Compatibility with historical merge and conversion semantics.
- Explicit derived-stream identities.
- Deterministic ordering under the configured policy.
- Bounded reorder buffers.
- Event-time watermark coordination.
- Explicit late-event handling.
- Original source-event provenance.
- Historical-compatible currency conversion.
- Independent source and derived subscriptions.
- Resource-bounded runtime execution.
- Observable degradation and recovery.

It does not guarantee:

- Unconditional chronological completeness under arbitrary Live lateness.
- Recovery of source events never acquired.
- Currency conversion without valid references.
- Automatic equivalence between different instrument economics.
- Aggregated cross-exchange order books from simple event merging.
- Exactly-once downstream application processing.
- Unlimited buffering or replay availability.

The essential invariants are:

    One Canonical Source Event
              |
              v
    Zero or More Derived Representations

and:

    Every Derived Representation
    Preserves Its Source Identity
    and Transformation Semantics.

Historical and Live pipelines must interpret equivalent events consistently.

The Live implementation may require additional runtime state for watermarks, generations, buffering, and recovery.

That additional state must not redefine the existing historical economic data contracts.

---
