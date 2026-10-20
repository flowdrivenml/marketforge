# 11 — PostgreSQL Recording

## 1. Purpose

The PostgreSQL Recording subsystem durably stores selected canonical market-data events acquired by MarketForge Live.

Its primary purpose is to preserve market data that has not yet become available through the exchanges' historical archive infrastructure.

The subsystem bridges the interval between:

- The latest available historical archive coverage.
- Market data acquired and recorded by MarketForge Live.
- The current continuously arriving live event stream.

This enables historical bootstrap consumers to access recent market data without waiting for exchanges to publish their next archive.

PostgreSQL recording is optional at the runtime level.

However, when a stream's configured bootstrap guarantee depends on PostgreSQL coverage, recording becomes a required dependency for that guarantee.

### 1.1 The Historical Archive Availability Gap

Historical exchange archives are generally published after the market events they contain.

For exchanges publishing daily archives, the newest available archive may cover only the preceding day or an earlier interval.

Consider:

    Current Time:
        October 19, 14:00

    Latest Historical Archive:
        October 18, 00:00–24:00

    Missing Recent Archive Coverage:
        October 19, 00:00–14:00

The exact archive publication schedule, coverage, and availability depend on the exchange.

MarketForge must not assume that every exchange publishes a complete daily archive at the same time.

Without recent recording, a bootstrap consumer may be unable to reconstruct the interval between the latest archive and current live acquisition.

### 1.2 The Recording Solution

MarketForge Live records accepted canonical events as they arrive.

Conceptually:

    HISTORICAL ARCHIVES
    ──────────────────────────────┐
                                  |
    Older Market Data             |
                                  |
                                  v
                           Historical Reader
                                  |
                                  |
    POSTGRESQL                    |
    ──────────────────────────────┤
                                  |
    Recently Recorded Events      |
                                  |
                                  v
                           Coverage Reconciliation
                                  |
                                  |
    LIVE STREAM                   |
    ──────────────────────────────┤
                                  |
    Current Canonical Events      |
                                  |
                                  v
                           Bootstrap Consumer

The historical bootstrap process combines the available sources according to their actual coverage and identity guarantees.

### 1.3 Example Timeline

Suppose:

    October 18
    ─────────────────────────────────────────

    Exchange Historical Archive:
        Complete through 23:59:59

    October 19
    ─────────────────────────────────────────

    PostgreSQL Recording:
        00:00:00 -> 14:00:00

    Live Acquisition:
        Continuing after 14:00:00

The desired logical coverage is:

    Historical Archives
    |-----------------------|

                          PostgreSQL
                          |--------------------|

                                               Live
                                               |------->

The source intervals should overlap where possible.

Reconciliation must prevent duplicate logical executions while preserving distinct events.

### 1.4 Primary Responsibilities

The Recording subsystem must:

- Accept selected canonical market-data events.
- Preserve existing canonical event identities.
- Record trades and configured L2 events.
- Use historical-compatible market-data schemas.
- Batch database writes.
- Enforce bounded recording buffers.
- Support idempotent persistence.
- Track committed recording boundaries.
- Expose recorded coverage intervals.
- Support bounded reads for bootstrap.
- Coordinate recorded-data retention.
- Detect database outages.
- Recover ambiguous transaction outcomes.
- Report recording gaps.
- Support concurrent live writing and historical reading.
- Remain independent of Unix socket subscribers.
- Expose recording health and performance metrics.

### 1.5 Non-Responsibilities

The Recording subsystem does not:

- Download historical exchange archives.
- Normalize exchange-native messages.
- Validate native trade continuity independently.
- Reconstruct exchange order books.
- Perform currency conversion.
- Merge cross-exchange streams.
- Provide the primary live subscriber transport.
- Replace the Live Retention Manager.
- Store application logs.
- Store operational failure history.
- Guarantee recovery of events never acquired.
- Decide that a historical interval is complete without supporting evidence.

### 1.6 Recording vs Live Retention

PostgreSQL recording and temporary Live Retention serve different purposes.

| Property | Live Retention | PostgreSQL Recording |
|---|---|---|
| Primary purpose | Bootstrap buffering and subscriber replay | Durable recent market-data coverage |
| Storage | RAM and temporary Parquet | PostgreSQL |
| Expected lifetime | Temporary | Configurable, potentially longer |
| Crash durability | Depends on committed tier | Depends on committed transaction |
| Primary access | Canonical publication cursor | Event-time ranges and canonical identities |
| Historical integration | Live handoff | Recent historical coverage |
| Source recovery | Limited by retained events | Limited by recorded events |
| Owner | Live Retention Manager | Recording Manager |

Both may retain the same canonical event.

This is intentional.

### 1.7 Recording vs Unix Socket Publication

The Publisher distributes accepted events to active local consumers.

The Recording Manager persists selected events for later retrieval.

These operations have independent completion boundaries.

    Canonical Event Accepted
               |
       +-------+-------+
       |               |
       v               v
    Publisher      Recording Manager
       |               |
       v               v
    Unix Sockets    PostgreSQL

A successful socket write does not prove database persistence.

A successful database commit does not prove subscriber delivery.

### 1.8 Fundamental Invariant

    PostgreSQL Recording
    Must Preserve Canonical Events
    Without Changing Their
    Economic Meaning Or Identity.

The Recording Manager must not silently rewrite timestamps, prices, quantities, native execution identifiers, or source provenance.

---

## 2. Architectural Boundaries

The PostgreSQL Recording subsystem operates downstream of canonical event acceptance.

It is a storage consumer, not an exchange connectivity or normalization component.

### 2.1 Component Ownership

| Component | Responsibility |
|---|---|
| Connection Manager | Exchange network connections |
| Subscription Manager | Native exchange subscriptions |
| Trade Processor | Canonical trade processing and continuity |
| BookStore | Canonical L2 state |
| Stream Registry | Logical source-stream identity |
| Publisher | Unix socket event distribution |
| Live Retention Manager | Temporary RAM/Parquet buffering |
| Recording Manager | PostgreSQL recording lifecycle |
| Recording Writer | Batching and database writes |
| Recording Coverage Index | Recorded event availability |
| Historical Data Subsystem | Exchange archive acquisition |
| Bootstrap Coordinator | Historical/recorded/live reconciliation |
| Recovery Coordinator | Cross-component failure recovery |
| Runtime Resource Manager | Shared resource budgets |
| Metrics Registry | Recording health and performance |

The Recording Manager must not duplicate the source-processing responsibilities of other components.

### 2.2 Data Flow

    Exchange WebSocket
           |
           v
    Native Processing
           |
           v
    Canonical Normalization
           |
           v
    Integrity Validation
           |
           v
    Accepted Canonical Event
           |
       +---+----------------+
       |                    |
       v                    v
    Publisher         Recording Manager
       |                    |
       v                    v
    Unix IPC            Bounded Queue
                            |
                            v
                       Batch Builder
                            |
                            v
                     PostgreSQL Writer
                            |
                            v
                      Commit Tracking

The Recording Manager receives already accepted canonical events.

It must not reinterpret native exchange payloads.

### 2.3 Recording Is Configurable

Recording can be enabled or disabled independently for configured streams.

Examples:

    Binance BTCUSDT Trades:
        Recording Enabled

    Binance ETHUSDT Trades:
        Recording Disabled

    Bybit BTCUSDT L2:
        Recording Enabled

    Combined BTC Trades:
        Recording Disabled

The Recording Manager must respect effective stream configuration.

### 2.4 Source and Derived Recording

Both source and derived streams may be recorded when explicitly enabled.

However, their identities must remain separate.

Example:

    Source:
        Binance BTCUSDT Trades

    Derived:
        Combined BTC Trades in USD

Recording both produces two distinct logical datasets.

The Recording Manager must not treat a derived event as another native exchange execution.

### 2.5 Historical Schema Compatibility

Recorded market-data values must be compatible with the existing historical canonical schema.

The historical pipeline remains authoritative for:

- Trade field semantics.
- Depth field semantics.
- Decimal precision.
- Timestamp units.
- Instrument identities.
- Event provenance.
- Combined-stream output semantics where applicable.

Database tables may use a different physical representation from Parquet.

However, reading recorded events must recover equivalent canonical values without lossy conversion.

### 2.6 Instrument Metadata

Instrument metadata originates from the Python-managed authoritative catalog.

MarketForge Live receives a validated metadata snapshot or initialization context.

The Recording Manager must not perform per-event Python calls or instrument-catalog lookups.

Relevant metadata revisions must remain recoverable for recorded events.

Historical readers must be able to interpret recorded values under the applicable metadata version.

### 2.7 PostgreSQL Independence

When recording is disabled:

    Live Acquisition = Operational

    Publisher = Operational

    PostgreSQL = Not Required

The Rust Live runtime must not require an active PostgreSQL connection for ordinary source acquisition.

When recording is enabled and required for a specific guarantee, database unavailability must be reflected in that guarantee's health.

### 2.8 No Operational Database

PostgreSQL must not store:

    Application Logs

    Recovery Operation History

    Runtime Error Logs

    TUI Events

    Operational Metrics History

These remain managed through local logging and runtime diagnostics.

PostgreSQL is reserved for its explicitly defined market-data and existing instrument-catalog responsibilities.

---

## 3. Historical, Recorded, and Live Coverage

Bootstrap requires one consistent view of the available market-data history.

The existence of three data sources does not automatically establish continuous coverage.

### 3.1 Coverage Layers

MarketForge distinguishes:

    HistoricalArchiveCoverage

    PostgreSQLRecordedCoverage

    LiveRetentionCoverage

    CurrentLiveAcquisition

Each layer has separate availability and integrity evidence.

### 3.2 Historical Archive Coverage

Historical archives provide event data for intervals published by the exchange.

The Historical Data Subsystem determines:

- Which archives are available.
- Which time intervals they claim to cover.
- Which instruments they contain.
- Which native identities are present.
- Whether integrity verification succeeded.

An archive's nominal date range is not by itself proof that every native event is present.

### 3.3 PostgreSQL Recorded Coverage

Recorded coverage represents accepted canonical events successfully persisted in PostgreSQL.

A recorded time interval may contain gaps caused by:

- MarketForge being offline.
- Exchange disconnections.
- Subscription failures.
- Failed database writes.
- Recording being disabled.
- Missing source recovery.
- Retention or resource failures.

The Recording Manager must not infer completeness merely because records exist near both ends of an interval.

### 3.4 Live Retention Coverage

Live Retention provides accepted canonical events preserved in RAM or temporary Parquet.

It supports the final transition to continuous live processing.

Unlike PostgreSQL, it is bounded primarily by temporary retention and protection policies.

### 3.5 Combined Coverage Timeline

Conceptually:

    Time ───────────────────────────────────────>

    Historical:
    [====================]

    PostgreSQL:
                    [====================]

    Live Retention:
                                  [===========]

    Current Live:
                                            ---->

Overlapping intervals are expected.

They support reconciliation.

### 3.6 Why Overlap Matters

Suppose:

    Historical Archive:
        Trades 100–200

    PostgreSQL:
        Trades 195–250

Both sources contain:

    Trades 195–200

The bootstrap process must recognize these as duplicate representations of the same native executions.

It should preserve one logical execution per valid native execution identity.

### 3.7 Missing Intervals

Suppose:

    Historical:
        Trades 100–200

    PostgreSQL:
        Trades 210–250

If native identifiers establish that executions 201–209 are missing, the combined coverage is incomplete.

If the identifier system does not support such continuity conclusions, the interval must remain unverified rather than being declared complete or incomplete solely from identifier differences.

### 3.8 Coverage vs Market Inactivity

The absence of recorded trades does not necessarily indicate missing data.

A market may have no executions during a period.

Therefore:

    No Rows
        !=
    Recording Failure

Likewise:

    No Rows
        !=
    Proven Complete Coverage

Completeness may require verified source continuity, acquisition health, and recording-watermark evidence.

### 3.9 RecordingCoverage

Conceptual representation:

    RecordingCoverage {
        stream_id

        event_time_interval

        committed_cursor_intervals
        source_coverage_status

        recording_status
        durability_status

        evidence
        evaluated_at
    }

The exact schema may be implemented using database metadata tables and runtime indexes.

### 3.10 Coverage Status

Supported statuses:

    VerifiedComplete

    PartiallyVerified

    VerifiedIncomplete

    Unverified

    Unavailable

The Recording Manager determines recording durability.

The source-processing components provide source-integrity evidence.

The Bootstrap Coordinator evaluates combined historical coverage.

### 3.11 Coverage Invariant

    Successful Database Persistence
        !=
    Complete Exchange Acquisition.

PostgreSQL may contain every event delivered to the Recording Manager while MarketForge still lacks executions that occurred during an upstream outage.

---

## 4. Recording Domain Models

The Recording subsystem maintains explicit runtime state, batch identities, persistence progress, and recording coverage.

### 4.1 RecordingManager

Conceptual representation:

    RecordingManager {
        runtime_instance_id

        stream_states
        recording_policies

        batch_scheduler
        database_writer
        coverage_tracker

        resource_budget

        configuration_revision
    }

The Recording Manager coordinates recording lifecycle and persistence operations.

### 4.2 RecordingStreamState

    RecordingStreamState {
        stream_id

        recording_enabled
        recording_generation

        configuration_revision
        metadata_revision

        last_admitted_cursor
        last_committed_cursor

        pending_batches

        recording_status
        last_error
    }

A single latest committed cursor does not establish complete coverage when earlier cursor intervals are missing.

The implementation must preserve authoritative committed ranges or equivalent gap evidence.

### 4.3 RecordingPolicy

    RecordingPolicy {
        enabled

        batch_max_events
        batch_max_bytes
        batch_flush_interval

        maximum_pending_bytes
        maximum_pending_batches

        database_write_timeout

        retry_policy
        durability_policy

        retention_policy
    }

Configuration keys are finalized in Document 03.

The existing hot-editable recording batch size and flush interval apply at safe batch boundaries.

### 4.4 RecordedEvent

Conceptual representation:

    RecordedEvent {
        canonical_event_identity

        stream_id
        event_type

        event_timestamp_ns
        received_timestamp_ns

        configuration_revision
        metadata_revision

        canonical_payload
        provenance
    }

The database representation must preserve the shared canonical economic data contract.

### 4.5 RecordingBatch

    RecordingBatch {
        batch_id

        stream_id
        recording_generation

        first_cursor
        last_cursor

        event_count
        estimated_bytes

        events
        state

        created_at
        committed_at
    }

Batches may be organized by stream or another validated persistence partitioning policy.

A batch must not mix incompatible canonical schemas without an explicit encoding contract.

### 4.6 RecordingBatchState

Supported states:

    Open
    Sealed
    PendingWrite
    Writing
    CommitUncertain
    Committed
    Failed
    Cancelled

`CommitUncertain` is required because a database connection may fail after the transaction commits but before the client receives confirmation.

### 4.7 Recording Identity

A durable event identity must be sufficient to prevent logical duplication during retries.

Conceptually:

    RecordedEventKey {
        stream_id
        log_epoch
        offset
    }

This key is the relational representation of the
authoritative `StreamCursor` defined in Document 02.

The existing database column name `stream_cursor`,
where retained for schema compatibility, represents
the numeric `offset` component.

It must not be interpreted as a second complete
`StreamCursor` or an independent cursor namespace.

The canonical uniqueness constraint is logically:

    UNIQUE (stream_id, log_epoch, offset)

Equivalent physical column names are permitted when
required for compatibility with the existing database.

This identifies a canonical publication event.

For native trades, the database may additionally maintain a separate native execution identity for cross-source reconciliation.

The two identities must not be conflated.

### 4.8 Native Execution Identity

For trades, a native execution identity may include:

    Exchange

    Market

    InstrumentId

    NativeExecutionId

    NativeIdentityScope

The exact semantics belong to Document 06.2.

A native trade identifier must not be assumed globally unique across exchanges, instruments, or exchange-defined identity epochs.

### 4.9 RecordingCommitResult

    RecordingCommitResult {
        batch_id

        committed_event_count
        committed_cursor_intervals

        database_transaction_identity

        committed_at
        durability_status
    }

A successful result must reflect an authoritative database transaction outcome.

### 4.10 Recording Status

Supported states:

    Disabled
    Initializing
    Recording
    Degraded
    Recovering
    Stopping
    Stopped
    Failed

| State | Meaning |
|---|---|
| Disabled | Recording not enabled |
| Initializing | Database session and recording state being prepared |
| Recording | Writes operating according to configured policy |
| Degraded | Recording guarantee impaired |
| Recovering | Database or pending-batch recovery underway |
| Stopping | Graceful recording shutdown |
| Stopped | Recording resources released |
| Failed | Cannot satisfy current recording configuration |

### 4.11 Recording Generations

Each recording activation receives a generation.

Conceptually:

    RecordingGeneration

The generation fences:

- Pending batch operations.
- Database write completions.
- Recording configuration updates.
- Recovery callbacks.
- Shutdown actions.

However, obsolete runtime generations must not cause valid already-committed database records to be deleted.

Generation fencing protects mutable runtime state, not the existence of valid durable market data.

---

## 5. Recording Activation and Lifecycle

Recording may be enabled during runtime operation.

Activation must establish a valid persistence boundary before reporting that recording is operational.

### 5.1 Activation Sequence

    Enable Recording
           |
           v
    Validate Stream
           |
           v
    Validate Recording Policy
           |
           v
    Validate Database Availability
           |
           v
    Initialize Database Writer
           |
           v
    Establish Source Cursor Boundary
           |
           v
    Activate Recording Admission
           |
           v
        Recording

The system must define precisely which source events are eligible for recording after activation.

### 5.2 Recording Activation Boundary

A stream may already be producing events when recording is enabled.

Example:

    Current Source Cursor:
        1000

    Recording Enabled

The system must establish an explicit starting cursor.

It must not assume that events 1–999 are already recorded.

### 5.3 Recording Historical Backfill

If retention contains earlier accepted canonical events, the Recording Manager may request a controlled backfill.

Conceptually:

    Recorded Through:
        Cursor 1000

    Retention Available:
        Cursor 1001–1500

    Current Live:
        Cursor 1501+

The Recording Manager may reconcile the missing retained interval before switching to ordinary live recording.

Backfill requires compatible cursor identities and actual retained coverage.

### 5.4 Continuous Recording

After activation:

    Canonical Event
          |
          v
    Recording Admission
          |
          v
    Bounded Batch Buffer
          |
          v
    PostgreSQL Transaction
          |
          v
    Commit Confirmation
          |
          v
    Recording Progress Update

Database writes occur asynchronously relative to exchange acquisition.

### 5.5 Recording Disable

When recording is disabled:

- Stop admitting new events at the defined boundary.
- Resolve pending batches according to policy.
- Preserve already committed records.
- Report the final committed cursor.
- Release database resources.
- Update stream recording state.

Disabling recording must not automatically remove the source stream.

### 5.6 Recording and Source Lifetime

A recording-enabled stream may require the corresponding source to remain active even when it has no Unix socket subscribers.

The Stream Registry must account for recording as a source dependency.

Example:

    Binance BTCUSDT Trades
             |
             +-- PostgreSQL Recording
             |
             +-- No IPC Subscribers

The source remains active while its recording requirement is enabled.

### 5.7 Recording Shutdown

During graceful shutdown, the Recording Manager must attempt to flush admitted batches within the configured deadline.

It must report batches whose durability could not be established.

A clean process shutdown does not automatically imply every admitted event was committed.

### 5.8 Crash Semantics

Events that were only held in volatile recording buffers when the process crashed may be lost.

Therefore:

    RecordingAdmitted
        !=
    DatabaseCommitted

The runtime must preserve this distinction.

Where bootstrap requires stronger durability, the configuration must provide an appropriate durable admission, spill, or recovery mechanism.

### 5.9 Lifecycle Invariant

    Recording Progress
    May Advance Only After
    Authoritative Database
    Commit Confirmation.

Ambiguous transaction outcomes must be reconciled before the corresponding events are classified as durably recorded.

---

## 6. PostgreSQL Data Model

The Recording subsystem stores canonical market-data events using a schema that preserves the existing MarketForge historical data contracts.

The database representation may differ physically from the historical Parquet representation.

However, all recorded events must be recoverable without losing canonical field semantics, numerical precision, source identity, or event grouping.

### 6.1 Database Responsibilities

The PostgreSQL schema must support:

- Canonical trade recording.
- Canonical L2 event recording.
- Source-stream identification.
- Derived-stream identification where enabled.
- Stable event identities.
- Idempotent inserts.
- Event-time range queries.
- Stream-specific queries.
- Recording progress tracking.
- Durable coverage metadata.
- Historical bootstrap reads.
- Controlled retention and deletion.

The database must not become an operational logging system.

### 6.2 Database Organization

The initial recommended organization is:

    PostgreSQL
        |
        +-- marketforge_live schema
        |       |
        |       +-- trades
        |       |
        |       +-- depth_events
        |       |
        |       +-- depth_levels
        |       |
        |       +-- recording_batches
        |       |
        |       +-- recording_coverage
        |
        +-- Existing Instrument Catalog

The exact schema name is configurable.

The Recording subsystem must not modify the authoritative instrument catalog's ownership or lifecycle.

### 6.3 Logical vs Physical Representation

The existing canonical Rust types remain authoritative for event semantics.

For trades:

    canonical::Trade

For depth:

    canonical::L2LevelUpdate

For combined timelines:

    Historical-Compatible Combined Event Contract

PostgreSQL may use relational tables and indexes optimized for retrieval.

It must not define a second competing canonical market-data model.

### 6.4 Physical Storage Strategy

The initial recommended strategy is:

    Typed Relational Tables
             +
    Stable Canonical Identities
             +
    Separate Recording Metadata

Trades and depth events may use separate physical tables.

This permits different indexing and storage strategies without requiring every event row to contain nullable columns for unrelated event types.

### 6.5 Schema Versioning

Database schema versioning is distinct from canonical event schema versioning.

Conceptually:

    DatabaseSchemaVersion

    CanonicalSchemaVersion

    RecordingContractVersion

A database migration must not silently change the economic meaning of existing records.

The Recording Manager must validate schema compatibility before beginning writes.

---

## 7. Canonical Trade Storage

Trade recording must preserve the existing canonical `Trade` representation.

### 7.1 Trade Fields

The logical trade representation includes:

    EventEnvelope

    TradeId

    Sequence

    Side

    Price

    QuantityBase

    QuantityQuote

    QuantityContracts

    IsRpi

    TradeIv

    MarkIv

    IndexPrice

    MarkPrice

The PostgreSQL schema must preserve applicable optional fields.

It must distinguish:

    NULL

from:

    Numeric Zero

This distinction is particularly important for optional quantities and derivative-specific fields.

### 7.2 Trade Table

Conceptual relational structure:

    trades {
        stream_id

        cursor_epoch
        stream_cursor

        exchange
        instrument_id
        symbol

        event_timestamp_ns
        system_timestamp_ns

        trade_id
        sequence

        side
        price

        quantity_base
        quantity_quote
        quantity_contracts

        is_rpi

        trade_iv
        mark_iv

        index_price
        mark_price

        canonical_schema_version
        metadata_revision
        configuration_revision
    }

This is a logical specification, not finalized PostgreSQL DDL.

### 7.3 Exact Decimal Values

Prices and quantities must preserve canonical decimal precision.

The initial recommendation is to use PostgreSQL:

    NUMERIC

with appropriate validated constraints.

The Recording Manager must not convert exact canonical values through floating-point representations.

For example:

    rust_decimal::Decimal
             |
             v
    PostgreSQL NUMERIC

The transformation must be lossless for supported canonical values.

### 7.4 Timestamp Precision

The canonical timestamp contract uses:

    Unix Nanoseconds

The recommended authoritative database representation is:

    BIGINT

for nanosecond timestamps.

A PostgreSQL `TIMESTAMPTZ` column may be introduced as a convenience for indexing or inspection.

However, it must not replace the original nanosecond integer because PostgreSQL timestamp precision does not preserve arbitrary nanosecond values.

### 7.5 Native Trade Identifiers

Native execution identifiers must preserve their original semantics.

The existing canonical model uses:

    TradeId: Option<String>

Therefore, the database should not assume that every trade identifier is a 64-bit integer.

A text representation is appropriate for opaque native identifiers.

### 7.6 Trade Side

The existing canonical trade side contract is:

    Buy

    Sell

The database may store these as constrained values or compact numerical codes.

The mapping must be versioned and unambiguous.

### 7.7 Source Provenance

The database must preserve:

    Exchange

    InstrumentId

    StreamId

    NativeExecutionId

    EventTimestamp

Relevant source-generation and recovery provenance may be stored separately.

Recording must not invent missing native identifiers.

### 7.8 Trade Schema Invariant

    PostgreSQL Trade
           |
           v
    Canonical Trade Decoder
           |
           v
    Equivalent Canonical Trade

The decoded trade must preserve the original canonical economic values.

---

## 8. Canonical L2 Storage

L2 recording requires particular care because one native depth message may produce several canonical price-level changes.

The database must preserve event grouping and authoritative book-generation information.

### 8.1 L2 Event Semantics

The existing canonical type:

    L2LevelUpdate

represents an absolute price-level state.

A zero quantity indicates deletion.

A missing optional quantity does not mean deletion.

These semantics must remain unchanged.

### 8.2 Depth Event Grouping

One accepted L2 event may contain multiple level changes.

Conceptually:

    L2 Event 100
          |
          +-- Bid 65000 -> Quantity 2
          |
          +-- Bid 64990 -> Quantity 4
          |
          +-- Ask 65010 -> Quantity 3

The historical merged depth representation preserves
this grouping.

PostgreSQL recording must do the same.

One accepted logical L2 event receives exactly one
MarketForge `StreamCursor`, as defined in
Document 02, Sections 4.1 and 6.4.

Its constituent `L2LevelUpdate` values do not
receive independent MarketForge event cursors.

The `depth_events` row identifies the parent
logical event and preserves its cursor.

The associated `depth_levels` rows preserve
the ordered canonical level changes.

Recording and replay must reconstruct the complete
logical L2 event without changing its original
cursor identity or event boundaries.

### 8.3 Recommended Relational Representation

The initial PostgreSQL implementation uses two tables:

    depth_events

    depth_levels

A separate table for order-book snapshots is not required.

Both authoritative initialization events and ordinary
incremental events use the same relational representation.

The distinction is expressed through:

    EventBoundary

    BookGeneration

    SourceOperation

    SequenceMetadata

An initialization event contains the complete authoritative
book state for its generation.

An ordinary change event contains only the absolute
level changes produced by canonical depth processing.

Both use the existing canonical `L2LevelUpdate`
representation.

The Recording Manager must preserve logical event
grouping and level ordering.

PostgreSQL must not implement a second order-book
state machine.

The shared BookStore remains responsible for maintaining
book state and providing initialization data.

This representation supports reconstruction by applying
the stored events in their validated order.

### 8.4 Depth Event Table

Conceptual:

    depth_events {
        depth_event_id

        stream_id

        cursor_epoch
        stream_cursor

        exchange
        instrument_id
        symbol

        event_timestamp_ns
        system_timestamp_ns

        book_generation
        book_version

        event_boundary
        source_operation

        sequence_first
        sequence_last

        level_count

        canonical_schema_version
        metadata_revision
        configuration_revision
    }

The event boundary distinguishes initialization from ordinary updates.

The source operation may distinguish snapshot, absolute update, and relative update when applicable.

These fields must follow the existing depth-processing contract.

### 8.5 Depth Level Table

Conceptual:

    depth_levels {
        depth_event_id

        level_ordinal

        side
        price

        quantity_base
        quantity_quote
        quantity_contracts

        order_count
    }

`level_ordinal` preserves the deterministic order of level changes within the parent event.

### 8.6 Atomic Depth Recording

A depth event and all its associated levels must be committed atomically.

Conceptually:

    BEGIN TRANSACTION

        Insert Depth Event

        Insert All Associated Levels

    COMMIT

The Recording Manager must not expose a partially persisted depth event as complete.

### 8.7 Initialization Events

An L2 initialization event establishes the complete
authoritative starting state of an independently
reconstructable book generation.

An initialization uses the same absolute level-update
representation as ordinary depth changes.

Example:

    INITIALIZATION — Generation 5

        SET BID 65000 -> 5
        SET BID 64990 -> 3

        SET ASK 65010 -> 2
        SET ASK 65020 -> 4

    CHANGES — Generation 5

        SET BID 65000 -> 7
        SET ASK 65020 -> 0

The initialization event must contain every level
required to reconstruct the authoritative starting
book state.

It must not contain only the differences between
the new snapshot and a previous book generation.

The existing `BookStore::apply_snapshot()` computes
changed levels relative to the previous state.

Therefore, the Live depth processor must provide a
separate complete-initialization emission path when
starting a new independently reconstructable segment.

The Recording Manager persists the resulting canonical
initialization event without independently reconstructing
the order book.

The database must preserve:

- The initialization boundary.
- The book generation.
- The complete set of initialization levels.
- The deterministic level ordering.
- Applicable native sequence metadata.
- Original canonical event provenance.

Ordinary subsequent events contain only changed levels.

No separate physical snapshot table is necessary.

An independently reconstructable segment consists of:

    Complete Initialization
             |
             v
    Ordered Absolute Changes
             |
             v
    Reconstructed Book State

Periodic complete-state checkpoints may be introduced
later to reduce bootstrap replay time.

Such checkpoints may use the same event representation,
but they must be explicitly distinguished from
exchange-originated initialization events when their
provenance or sequencing semantics differ.
### 8.8 Book Generation Changes

After a sequence gap, the BookStore may establish a new generation.

Example:

    Generation 5
        Delta Events

    Sequence Gap

    Generation 6
        Initialization
        Delta Events

The database must preserve this transition.

A historical reader must not join generation-5 deltas to generation-6 state without the required initialization.

### 8.9 L2 Reconstruction

Recorded L2 data can support historical reconstruction only when:

- A valid initialization is available.
- Required subsequent events are available.
- Event grouping is preserved.
- Book generations are compatible.
- Sequence continuity is established.

Recorded level changes alone do not prove reconstructable historical book coverage.

### 8.10 Depth Schema Invariant

    Recorded Depth Event
              +
    Ordered Recorded Levels
              |
              v
    Historical-Compatible
    Canonical Depth Event

No level changes may be lost or duplicated during reconstruction.

---

## 9. Canonical Event Identity

PostgreSQL recording must distinguish canonical publication identity from native exchange event identity.

Both are important, but they solve different problems.

### 9.1 Canonical Publication Identity

The canonical publication identity is:

    StreamId

    `LogEpoch`

    StreamCursor

This identifies one accepted event within MarketForge's publication system.

### 9.2 Native Execution Identity

A native trade identity may contain:

    Exchange

    Market

    InstrumentId

    NativeExecutionId

    NativeIdentityScope

The actual scope depends on the exchange and instrument.

### 9.3 Why Both Are Necessary

Suppose one trade is received live and later found in an exchange archive.

Its native execution identity may be the same in both sources.

However, only the live representation has the original MarketForge publication cursor.

Conceptually:

    Historical Archive:
        NativeExecutionId = 12345

    PostgreSQL:
        NativeExecutionId = 12345
        Cursor = 500

These are two representations of one native execution.

Bootstrap reconciliation must identify that relationship.

### 9.4 Publication Identity Constraints

The database must enforce uniqueness for the authoritative canonical publication identity.

Conceptually:

    UNIQUE (
        stream_id,
        cursor_epoch,
        stream_cursor
    )

The exact key structure may depend on the final unified stream-identity schema.

### 9.5 Native Identity Constraints

A separate native trade identity index may support reconciliation.

However, uniqueness must follow the actual exchange-specific identity scope.

The database must not impose a globally unique `trade_id`.

### 9.6 Duplicate Recording Attempts

A database retry may attempt to insert an already committed event.

The Recording Manager must distinguish:

    Same Identity + Same Payload

from:

    Same Identity + Conflicting Payload

The first may be treated as an idempotent duplicate.

The second is an integrity failure.

### 9.7 Avoiding Silent Conflict Suppression

The implementation must not use unconditional conflict ignoring as proof of successful recording.

For example:

    INSERT ... ON CONFLICT DO NOTHING

can prevent duplicate rows.

However, it does not establish whether a conflicting existing row contains the same canonical event.

The recording contract must define how conflicting identities are validated.

### 9.8 Idempotent Write Invariant

    Retrying The Same Canonical Event
    Must Not Create A Second
    Logical Database Event.

An incompatible payload with the same identity must produce an explicit error.

---

## 10. PostgreSQL Partitioning

Market-data recording may produce substantial database volume.

The schema must support efficient inserts, range queries, retention, and cleanup.

### 10.1 Partitioning Objectives

Partitioning should:

- Keep historical range queries efficient.
- Support large recorded datasets.
- Avoid oversized indexes.
- Enable controlled retention.
- Support concurrent ingestion.
- Permit efficient archival and cleanup.

### 10.2 Initial Partitioning Strategy

The recommended initial strategy is:

    Time-Based Partitioning

using event-time intervals.

For example:

    trades_2026_10_19

    trades_2026_10_20

    depth_events_2026_10_19

    depth_events_2026_10_20

The exact partition duration must be configurable.

Daily partitions are a reasonable initial option for high-volume market data.

### 10.3 Event Time vs Recording Time

The partition key should reflect the intended historical query domain.

For ordinary historical reconstruction:

    EventTimestamp

is generally the relevant dimension.

However, late recovered events may be recorded long after their event timestamps.

Example:

    Event Time:
        October 18

    Recorded At:
        October 19

The database must route the event to the correct event-time partition.

### 10.4 Partition Identity Constraints

PostgreSQL's partitioned-table uniqueness rules must be considered explicitly.

If a table is partitioned by event time, a parent-table unique constraint generally must include the partition key.

Therefore, a constraint such as:

    UNIQUE (
        stream_id,
        cursor_epoch,
        stream_cursor
    )

cannot automatically be assumed to provide globally enforced uniqueness on an event-time-partitioned table.

The schema must resolve this limitation before implementation.

### 10.5 Recommended Identity Architecture

For the initial design, use a separate authoritative recording-identity registry or an equivalent transactionally enforced mechanism when global canonical identity uniqueness is required across event-time partitions.

Conceptually:

    recording_identities {
        stream_id
        cursor_epoch
        stream_cursor

        event_timestamp_ns
        event_type

        payload_identity
    }

The Recording Manager must commit the identity reservation and corresponding market-data row atomically.

The precise mechanism should be benchmarked before finalization.

Alternative partition strategies may be selected if they offer simpler correctness and better measured performance.

### 10.6 Trade Indexes

Trade queries commonly filter by:

    StreamId

    EventTimestampRange

Useful conceptual index:

    (stream_id, event_timestamp_ns)

A separate native execution identity index may support overlap reconciliation.

### 10.7 Depth Indexes

Depth queries commonly filter by:

    StreamId

    EventTimestampRange

    BookGeneration

Useful conceptual indexes include:

    (stream_id, event_timestamp_ns)

    (stream_id, book_generation, event_timestamp_ns)

Child level rows require efficient lookup by:

    depth_event_id

### 10.8 Partition Creation

Future partitions must be created before incoming writes require them.

Partition management may be performed through:

- Startup validation.
- Scheduled maintenance.
- Controlled administrative operations.

The Recording Manager must not repeatedly perform expensive DDL operations on the market-data hot path.

### 10.9 Missing Partitions

If an event cannot be inserted because its required partition does not exist:

    RecordingPartitionUnavailable

The Recording Manager must report the failure and follow its retry or configuration policy.

The event must not be silently discarded.

### 10.10 Partitioning Invariant

    Physical Partitioning
    Must Not Change
    Canonical Event Identity
    Or Historical Query Semantics.

---

## 11. Recording Coverage Metadata

PostgreSQL must provide sufficient information to determine which canonical publication intervals were durably recorded.

Coverage cannot be inferred solely from minimum and maximum event timestamps.

### 11.1 Recording Coverage Requirements

The coverage model must support:

- Per-stream committed positions.
- Cursor epochs.
- Known missing intervals.
- Recording activation boundaries.
- Recording disable boundaries.
- Database outage intervals.
- Recording recovery.
- Source continuity evidence.
- Bootstrap range queries.

### 11.2 Coverage Table

Conceptual:

    recording_coverage {
        coverage_id

        stream_id
        cursor_epoch

        first_cursor
        last_cursor

        first_event_timestamp_ns
        last_event_timestamp_ns

        recording_generation

        recording_status
        source_integrity_status

        committed_at
    }

The table represents recording coverage, not generic application history.

### 11.3 Coverage Intervals

A recording interval should identify a contiguous accepted publication range when the cursor contract supports contiguous positions.

Example:

    Stream 15
    Epoch 7

    Recorded:
        Cursor 100–500

If cursors 200–220 are missing, the system must represent the gap explicitly.

It must not report the entire interval as complete.

### 11.4 Coverage vs Timestamp Range

Suppose the database contains:

    Trade at 12:00:00

    Trade at 12:10:00

This does not establish that every execution between those timestamps was recorded.

Likewise, an empty time range may represent legitimate market inactivity.

Source integrity evidence is needed to distinguish these cases.

### 11.5 Recording Activation Boundary

When recording begins:

    RecordingStartCursor

must be preserved.

The database must not imply that earlier accepted events were recorded.

### 11.6 Recording Disable Boundary

When recording stops:

    RecordingStopCursor

must be preserved.

The coverage index must not extend beyond the last durably committed event.

### 11.7 Transactional Coverage Updates

Coverage metadata should be committed consistently with the corresponding market-data batch.

Conceptually:

    BEGIN TRANSACTION

        Insert Market Events

        Update Recording Coverage

        Confirm Recording Identity

    COMMIT

If the transaction fails, neither the event rows nor their corresponding committed coverage assertions may become authoritative.

### 11.8 Recording Gaps

Known recording gaps must remain identifiable.

Conceptually:

    RecordingGap {
        stream_id
        cursor_epoch

        affected_interval

        reason
        detection_time

        recovery_status
    }

A gap may be recoverable through Live Retention or another validated source.

The Recording Manager must not silently mark the gap resolved until the missing events have been durably reconciled.

### 11.9 Source Acquisition Gaps

Recording gaps and source acquisition gaps are independent.

Example:

    Source Acquisition:
        Complete

    Recording:
        Missing Cursor 200–220

This may be recoverable from retention.

Another example:

    Source Acquisition:
        Missing Executions

    Recording:
        All Accepted Events Persisted

The recording subsystem may be healthy even though the overall market-data history is incomplete.

### 11.10 Coverage Status

The coverage API must distinguish:

    RecordedComplete

    RecordedPartial

    RecordingGap

    RecordingUnverified

    RecordingUnavailable

These statuses describe recording coverage.

They must not replace the separate source-integrity classifications.

### 11.11 Coverage Invariant

    A Recording Coverage Claim
    Must Be Supported By
    Committed Database State
    And Applicable Cursor Evidence.

---

## 12. Historical-Compatible Database Reads

The Recording subsystem must expose recorded data through an interface usable by the existing historical processing and bootstrap architecture.

### 12.1 Read Responsibilities

The read interface must:

- Select recorded streams.
- Query bounded event-time ranges.
- Preserve canonical identities.
- Preserve exact decimal values.
- Reconstruct grouped depth events.
- Return original source provenance.
- Support pagination.
- Provide recording coverage information.
- Avoid blocking database writers unnecessarily.

### 12.2 ReadRequest

Conceptual:

    RecordingReadRequest {
        stream_id

        start_timestamp_ns
        end_timestamp_ns

        event_types

        maximum_events
        maximum_bytes

        continuation_token
    }

The recommended interval convention is:

    [start_timestamp_ns, end_timestamp_ns)

### 12.3 ReadResult

Conceptual:

    RecordingReadResult {
        events

        returned_interval
        recording_coverage

        next_token
        status
    }

A completed query is not necessarily proof that the requested interval has complete market-data coverage.

### 12.4 Historical Canonical Reconstruction

A database reader must reconstruct:

    Trade

    L2LevelUpdate

or the equivalent grouped canonical depth-event representation.

The result must remain compatible with the historical merge and conversion pipelines.

### 12.5 Deterministic Ordering

Queries must use a deterministic ordering contract.

For example:

    event_timestamp_ns

    source_stream_rank

    source_event_identity

When multiple events share the same timestamp, ordering must use stable tie-breaking information.

Database row insertion order must not determine historical event order.

### 12.6 Cursor Pagination

For cursor-based recording replay, queries must preserve the canonical publication order.

Conceptually:

    StreamId
    `LogEpoch`
    StreamCursor

Event-time pagination and publication-cursor pagination serve different purposes.

The API should distinguish them explicitly.

### 12.7 Historical Archive Integration

The Bootstrap Coordinator combines:

    Historical Archive Reader

    PostgreSQL Recording Reader

    Live Retention Reader

Each source reports its own availability and coverage evidence.

The coordinator reconciles overlap using canonical and native identities.

### 12.8 Reader Isolation

Long-running historical queries must not indefinitely block recording transactions.

The implementation should use bounded queries and appropriate database isolation levels.

The exact transaction isolation policy must be selected based on the required consistency guarantees.

### 12.9 Historical Read Invariant

    PostgreSQL Recorded Events
              |
              v
    Historical-Compatible Decoder
              |
              v
    Canonical Events
              |
              v
    Existing Merge and
    Conversion Contracts

The Recording subsystem must not require a second set of economic transformation rules.

---

## 13. Recording Writer Architecture

The Recording Writer persists accepted canonical market-data events into PostgreSQL using bounded asynchronous batching.

It is owned by the Recording Manager.

The writer must preserve canonical event identity, maintain accurate durability boundaries, and support safe retry after database failures.

### 13.1 Responsibilities

The Recording Writer must:

- Accept validated canonical recording events.
- Preserve their original event identities.
- Group compatible events into batches.
- Enforce memory and concurrency limits.
- Serialize events into PostgreSQL-compatible representations.
- Execute database transactions.
- Confirm transaction outcomes.
- Maintain idempotent persistence.
- Update committed recording coverage.
- Detect ambiguous transaction results.
- Support bounded retries.
- Report permanent recording failures.
- Expose writer throughput and latency metrics.

The writer must not:

- Normalize exchange-native payloads.
- Modify canonical prices or quantities.
- Assign replacement native execution identities.
- Reconstruct L2 order books.
- Determine exchange-side historical completeness.
- Block WebSocket reader tasks on ordinary database writes.

### 13.2 Writer Architecture

Conceptually:

    Accepted Canonical Events
               |
               v
        Recording Admission
               |
               v
         Bounded Input Queue
               |
               v
          Batch Scheduler
               |
         +-----+-----+
         |           |
         v           v
    Trade Batches  Depth Batches
         |           |
         +-----+-----+
               |
               v
       PostgreSQL Writer Pool
               |
               v
        Transaction Execution
               |
               v
        Commit Reconciliation
               |
               v
       Recording Coverage Update

The physical implementation may use different worker pools for trades and depth.

Concurrency must remain bounded.

### 13.3 Writer Ownership

Each recording operation belongs to one authoritative Recording Manager generation.

A batch has one authoritative writer operation at a time.

A failed operation may be retried only after its previous outcome has been classified or reconciled.

Concurrent writers must not execute conflicting versions of the same batch.

### 13.4 Writer Pool

The writer pool maintains a configurable number of PostgreSQL connections.

Conceptually:

    RecordingWriterPool {
        database_connections

        maximum_concurrent_transactions

        pending_batches
        active_batches

        connection_health
        recording_generation
    }

The pool must not create an unlimited number of database connections during recovery.

### 13.5 Connection Independence

PostgreSQL connection management is independent of exchange connectivity.

A database reconnection must not cause exchange WebSocket subscriptions to be recreated.

Likewise, exchange reconnection must not unnecessarily restart healthy recording connections.

### 13.6 Dedicated Execution

Database operations may involve blocking or asynchronous waits.

They must execute through dedicated recording tasks.

The canonical source-processing hot path must interact with recording through a bounded admission interface.

### 13.7 Writer Failure

A writer failure affects the corresponding batch and recording guarantee.

The Recording Manager must preserve the distinction between:

    Failed Before Transaction

    Transaction Rolled Back

    Commit Outcome Unknown

    Transaction Committed

These outcomes require different recovery behavior.

---

## 14. Recording Batching

Recording batches reduce database transaction overhead and improve write throughput.

The batching policy must balance latency, memory usage, and transaction size.

### 14.1 Batch Objectives

Batching should:

- Reduce per-event database overhead.
- Limit transaction frequency.
- Improve bulk insertion throughput.
- Bound pending memory.
- Preserve event identities.
- Support efficient idempotent retries.
- Minimize recording lag.
- Preserve required depth-event atomicity.

### 14.2 Batch Triggers

A recording batch may be sealed when:

    MaximumBatchEvents Reached

    MaximumBatchBytes Reached

    FlushInterval Elapsed

    GracefulShutdown Requested

    ExplicitFlush Requested

All limits are configurable.

### 14.3 RecordingBatch

Conceptual representation:

    RecordingBatch {
        batch_id

        recording_generation

        stream_id
        cursor_epoch

        first_cursor
        last_cursor

        events

        event_count
        estimated_bytes

        created_at
        sealed_at

        state
    }

The batch must preserve enough information to reconcile committed events after a retry or process interruption.

### 14.4 Batch Partitioning

The initial recommended strategy is:

    Per-Stream Logical Batches

This simplifies:

- Cursor tracking.
- Recording coverage.
- Replay.
- Failure isolation.
- Idempotent recovery.

Multiple batches from different streams may be written concurrently.

A later optimization may combine compatible streams into one physical transaction when this provides measurable benefits.

### 14.5 Trade and Depth Separation

Trade and depth events may use different batch builders because their relational representations differ.

Trade batches may insert one row per canonical trade.

Depth batches may insert:

    One Depth Event Row

and:

    Zero or More Depth Level Rows

per accepted depth event.

The parent and child records must be committed atomically.

### 14.6 Batch Ordering

Within one stream, recording input follows canonical publication order.

However, successful database transactions may complete out of order when multiple batches execute concurrently.

Therefore:

    Batch 10 Committed

does not automatically imply:

    Batch 9 Committed

The Recording Manager must track committed intervals independently.

### 14.7 Commit Frontier

The authoritative committed frontier represents the highest confirmed continuous recorded position under the canonical cursor contract.

Example:

    Batch A:
        Cursor 100–199
        Committed

    Batch B:
        Cursor 200–299
        Pending

    Batch C:
        Cursor 300–399
        Committed

The contiguous committed frontier remains:

    Cursor 199

until Batch B commits.

The Recording Manager may separately report that Batch C is durable.

### 14.8 Batch Size Configuration

Relevant settings include:

    recording_batch_max_events

    recording_batch_max_bytes

    recording_flush_interval

The exact configuration keys belong to Document 03.

The existing hot-editable batch-size and flush-interval settings must apply at safe batch boundaries.

### 14.9 Batch Resource Reservations

Before creating or enlarging a batch, the Recording Manager must reserve sufficient memory capacity.

Memory accounting includes:

- Event references.
- Batch metadata.
- Serialized database parameters.
- Pending retry state.
- Temporary conversion buffers.
- Transaction result metadata.

Shared immutable event references must be accounted for consistently.

### 14.10 Batch Invariants

    Batch contents are immutable
    after sealing.

    Batches retain canonical
    event identities.

    Database completion order
    does not determine source
    event ordering.

    Batch memory remains bounded.

    One batch has one authoritative
    persistence operation at a time.

---

## 15. PostgreSQL Transaction Semantics

Market-data recording requires atomic persistence of event rows, identity metadata, and applicable coverage updates.

### 15.1 Transaction Boundary

The recommended transaction structure is:

    BEGIN

        Register Or Validate Event Identities

        Insert Canonical Event Rows

        Insert Depth Level Rows Where Applicable

        Update Recording Batch State

        Update Recording Coverage

    COMMIT

The transaction must not publish committed coverage before the corresponding event data is durable.

### 15.2 Atomicity Requirement

Either:

    All Required Transaction Changes Commit

or:

    None Become Committed

The Recording Manager must not expose partial successful persistence as a complete batch result.

### 15.3 Depth Event Atomicity

For an L2 event with multiple price-level changes:

    Depth Event
         |
         +-- Level 1
         +-- Level 2
         +-- Level 3

all required rows must be inserted within the same transaction.

A failed insertion of Level 3 must not leave the event classified as fully recorded.

### 15.4 Identity Reservation

When using the separate recording-identity registry described in Section 10.5, event identity registration and market-data insertion must occur within one transaction.

Conceptually:

    BEGIN

        Reserve Canonical Identity

        Validate Existing Identity If Present

        Insert Event Data

        Commit Coverage

    COMMIT

This prevents durable identity records from referencing market-data rows that never committed.

### 15.5 Existing Identity

If a recording identity already exists, the writer must determine whether the existing record represents the same canonical event.

Possible outcomes:

    Identical:
        Idempotent Success

    Conflicting:
        Integrity Failure

A conflict must not be silently interpreted as successful persistence.

### 15.6 Transaction Isolation

The transaction isolation level must support the selected identity and coverage consistency contract.

The initial implementation may use PostgreSQL's ordinary transaction isolation together with explicit uniqueness constraints and carefully designed concurrency control.

Stronger isolation must be introduced when required by actual correctness conditions.

The database schema must not depend on undocumented assumptions about concurrent insert ordering.

### 15.7 Transaction Durability

A successful database commit provides durability according to PostgreSQL's configured transaction and storage guarantees.

The Recording Manager must not claim stronger durability than the active database configuration supports.

For example, configurations that relax synchronous commit behavior may weaken crash-durability guarantees.

Such behavior must be represented explicitly in the recording durability policy.

### 15.8 Commit Result

Conceptually:

    RecordingCommitResult {
        batch_id

        transaction_identity

        committed_events
        committed_cursor_intervals

        committed_at

        durability_status
    }

The result must correspond to an authoritative transaction outcome.

### 15.9 Transaction Failure

A failed transaction may result from:

- Constraint violation.
- Invalid data.
- Database connection failure.
- Statement timeout.
- Transaction deadlock.
- Serialization conflict.
- Insufficient database storage.
- Database restart.

The Recording Manager must classify errors before retrying.

### 15.10 Transaction Invariant

    Committed Recording Coverage
    Must Correspond To
    Committed Canonical Event Data.

---

## 16. Idempotent Recording

Database retries are unavoidable.

The Recording Manager must ensure that retries do not create duplicate logical market-data events.

### 16.1 Idempotency Identity

The primary recording identity is:

    StreamId

    `LogEpoch`

    StreamCursor

Native trade identifiers provide an additional reconciliation dimension.

The database must preserve both where applicable.

### 16.2 Retry Example

Suppose:

    Batch 100:
        Events 1000–1099

The transaction commits, but the client loses its connection before receiving the confirmation.

The Recording Manager retries.

Without idempotency, the database could contain two copies of each event.

With idempotent persistence, the writer can identify that the events already exist.

### 16.3 Idempotent Outcomes

Supported outcomes:

    NewlyInserted

    AlreadyCommittedEquivalent

    IdentityConflict

    PersistenceFailed

The first two satisfy the event-persistence requirement.

The third is an integrity failure.

### 16.4 Payload Equivalence

Two records with the same canonical identity are equivalent only when their canonical data contracts match.

The comparison must account for:

- Event type.
- Source identity.
- Event timestamp.
- Exact numerical values.
- Applicable optional fields.
- Depth-event grouping.
- Canonical schema version.
- Required provenance.

A stable payload fingerprint may accelerate equivalence checks.

The fingerprint representation must be versioned and collision-resistant enough for the selected integrity policy.

A fingerprint alone must not replace validation where stronger evidence is required.

### 16.5 Concurrent Duplicate Attempts

Two writers may attempt to persist the same canonical identity concurrently.

The database must enforce one authoritative result.

The implementation must not rely solely on application-side checks such as:

    SELECT existing event

followed by:

    INSERT new event

without a suitable concurrency-control mechanism.

### 16.6 Event-Time Partitions

If market-data tables use event-time partitioning, global canonical identity enforcement must account for PostgreSQL partitioned uniqueness restrictions.

The identity registry described in Section 10.5 is one possible solution.

Its transaction and index costs must be benchmarked.

### 16.7 Native Trade Deduplication

Canonical recording idempotency is distinct from native execution deduplication.

Example:

    Canonical Event A:
        Native Trade 500

    Canonical Event B:
        Native Trade 500

The Trade Processor should ordinarily prevent duplicate native executions from being accepted into the same authoritative source stream.

The Recording Manager must not silently discard conflicting canonical publication events based solely on matching native trade identifiers.

It must report the inconsistency through the integrity interface.

### 16.8 Idempotency Invariant

    One Canonical Publication Identity
    Must Resolve To
    One Authoritative Recorded Event.

---

## 17. Recording Queue and Backpressure

Database recording must remain asynchronous relative to canonical event processing.

However, asynchronous recording does not permit unbounded buffering.

### 17.1 Recording Queue

Conceptually:

    RecordingQueue {
        maximum_events
        maximum_bytes

        queued_events
        queued_bytes

        state
    }

The queue contains accepted event references or sealed recording batches.

### 17.2 Queue States

Supported states:

    Normal
    Elevated
    Saturated
    Failed

The Recording Manager must expose queue occupancy and recording lag.

### 17.3 Recording Admission

An event may enter the recording queue only when:

- Recording is enabled.
- Its stream is eligible.
- Its canonical identity is valid.
- Its recording generation is current.
- Required memory capacity is reserved.
- The event is compatible with the database schema.

Admission must produce an explicit result.

### 17.4 Queue Saturation

When the recording queue becomes full:

    Recording Queue Saturated
                |
                v
    Restrict New Recording Admission
                |
                v
    Apply Configured Overflow Policy
                |
                v
    Report Recording Degradation

The Recording Manager must not silently discard admitted events.

### 17.5 Overflow Policies

Supported conceptual policies:

    RequireDurability

    RecoverFromRetention

    AllowExplicitRecordingGap

These policies represent different service guarantees.

### 17.6 RequireDurability

Under strict durability:

    Recording Cannot Accept Event
                |
                v
    Required Recording Guarantee Impaired

The runtime must not report successful recording.

A strict configuration may propagate backpressure or suspend the affected operation.

However, blocking exchange acquisition does not guarantee that the exchange will stop producing events.

The system must therefore prioritize bounded buffering and early escalation.

### 17.7 RecoverFromRetention

If the Live Retention Manager holds the required accepted canonical events, the Recording Manager may record a durable recovery cursor and resume database writing from retention.

Conceptually:

    PostgreSQL Slow
           |
           v
    Recording Queue Saturated
           |
           v
    Establish Retention Protection
           |
           v
    Release Direct Queue Pressure
           |
           v
    Record Missing Events Later

This mechanism requires an authoritative protection and cursor boundary.

It must not assume retention can recover events that have already expired.

### 17.8 Retention Protection Admission

The protection must be active before the Recording Manager relies on retention as an alternative storage tier.

If protection admission fails, the system must not claim that the recording backlog is safely recoverable.

### 17.9 Explicit Recording Gap

A permissive recording policy may allow live acquisition to continue while acknowledging that some events cannot be recorded.

The system must report:

    RecordingGap

including the affected cursor interval when known.

This policy is incompatible with an unconditional claim of complete recording coverage.

### 17.10 Subscriber Independence

Recording queue saturation must not automatically block healthy Unix socket subscribers.

The Publisher and Recording Manager maintain independent delivery and durability guarantees.

### 17.11 Backpressure Invariant

    Recording Backpressure
    Must Not Become
    Silent Recording Loss.

---

## 18. Database Outage Recovery

PostgreSQL may become temporarily unavailable while MarketForge continues receiving market data.

The Recording Manager must recover safely without creating duplicate events or fabricating coverage.

### 18.1 Failure Categories

Possible database failures include:

    ConnectionLost

    ConnectionTimeout

    DatabaseUnavailable

    DatabaseRestart

    StatementTimeout

    TransactionDeadlock

    SerializationFailure

    AuthenticationFailure

    PermissionDenied

    SchemaIncompatible

    DiskExhausted

    CommitOutcomeUnknown

### 18.2 Recoverable Errors

Temporary failures may trigger bounded retry.

Examples:

    Connection Reset

    Temporary Database Restart

    Transient Connection Failure

    Retryable Transaction Conflict

The retry policy must account for PostgreSQL-specific error classifications.

### 18.3 Permanent Errors

Errors requiring configuration or operator correction must not trigger unlimited retries.

Examples:

    Invalid Credentials

    Missing Permissions

    Unsupported Database Schema

    Invalid Table Definition

These failures require explicit escalation.

### 18.4 Outage Sequence

    PostgreSQL Available
             |
             v
    Recording Active
             |
             v
    Database Connection Lost
             |
             v
    Recording Degraded
             |
             v
    Preserve Pending Work
             |
             v
    Respect Buffer and Retention Limits
             |
             v
    Retry Database Connectivity
             |
             v
    Reconcile Commit State
             |
             v
    Resume Missing Writes
             |
             v
    Validate Coverage
             |
             v
    Recording Restored

### 18.5 Pending Work

During an outage, events may remain in:

    Recording Queue

    Sealed Batches

    Protected Live Retention

These storage locations have different durability guarantees.

Memory-only pending batches are not crash-durable.

Disk-committed retention segments may provide stronger recovery guarantees.

### 18.6 Database Reconnection

After reconnecting, the writer must validate:

- Database identity.
- Schema compatibility.
- Required table availability.
- Recording permissions.
- Current transaction capabilities.
- Existing batch commit state.
- Available recording capacity.

A successful TCP connection alone is not evidence that recording can resume safely.

### 18.7 Recovery Ordering

The Recording Manager should prioritize unresolved earlier cursor intervals before newer recording backlogs when this is necessary to restore contiguous coverage.

However, recording across independent streams may proceed concurrently.

### 18.8 Outage Duration

The runtime must measure:

    RecordingOutageDuration

    PendingRecordingEvents

    PendingRecordingBytes

    OldestUncommittedEventAge

    RecoverableRetentionCoverage

These measurements help determine whether the available recovery window remains sufficient.

### 18.9 Outage Recovery Completion

Recording recovery is complete only when the required pending events have been durably reconciled.

For example:

    DatabaseConnected = true

    PendingBatchCount = 0

does not prove complete coverage if earlier admitted events were lost.

The Recording Manager must validate committed intervals and known gaps.

### 18.10 Outage Invariant

    Database Connectivity Restored
        !=
    Recording Coverage Restored.

---

## 19. Ambiguous Commit Reconciliation

A database commit may succeed even when the application fails to receive the confirmation.

This is an essential case for idempotent recording.

### 19.1 Ambiguous Commit Scenario

    Recording Writer
          |
          v
    BEGIN TRANSACTION
          |
          v
    INSERT EVENTS
          |
          v
    COMMIT
          |
          v
    Connection Lost

At this point, the client may not know whether the transaction committed.

### 19.2 CommitUncertain State

The affected batch enters:

    CommitUncertain

The writer must not immediately classify it as failed.

It must also not assume that the transaction succeeded.

### 19.3 Reconciliation Procedure

    Commit Outcome Unknown
              |
              v
    Reestablish Database Session
              |
              v
    Query Authoritative Batch Identity
              |
              v
    Validate Existing Event Identities
              |
          +---+---+
          |       |
          v       v
       Present   Absent
          |       |
          v       v
      Validate   Retry
      Payloads   Transaction
          |
          v
    Confirm Committed Coverage

A partial or inconsistent result must trigger an integrity failure or a defined repair procedure.

### 19.4 Batch Commit Identity

A stable batch identity may be used to support reconciliation.

Conceptually:

    BatchCommitIdentity {
        recording_instance
        recording_generation
        batch_id
    }

Batch identity is operational metadata.

It must not replace canonical event identity.

### 19.5 Transactional Batch Marker

The database may maintain:

    recording_batches

A batch marker must be inserted or updated in the same transaction as the corresponding event rows and committed coverage.

This permits reliable determination of whether a particular batch transaction committed.

### 19.6 Reconciliation After Process Restart

The previous runtime's batch identity may be unavailable after a crash unless it was persisted.

Restart recovery must therefore be able to reconcile through canonical event identities and durable recording coverage.

The system must not depend exclusively on volatile batch identifiers.

### 19.7 Ambiguous Commit Invariant

    Unknown Commit Outcome
    Requires Reconciliation,
    Not Blind Duplication.

---

## 20. Recording Recovery From Live Retention

Live Retention may provide accepted canonical events that were not successfully recorded during a PostgreSQL outage.

This is especially valuable for preserving the archive-to-live bridge.

### 20.1 Recovery Preconditions

Retention-based recovery requires:

- A valid source stream.
- Known missing recording cursors.
- Compatible cursor epochs.
- Available retained events.
- Valid canonical event identities.
- Sufficient recording capacity.
- A valid protection or read lease.

### 20.2 Recovery Procedure

    Identify Recording Gap
              |
              v
    Query Retention Coverage
              |
              v
    Validate Required Cursor Interval
              |
              v
    Acquire Retention Read Protection
              |
              v
    Read Canonical Events
              |
              v
    Build Recovery Batches
              |
              v
    Persist Through Recording Writer
              |
              v
    Confirm Database Commits
              |
              v
    Update Recording Coverage
              |
              v
    Release Retention Protection

The Recording Manager must use the same idempotent database persistence interface for ordinary and recovered events.

### 20.3 Recovery Event Identity

A recovered retained event preserves:

    Original StreamId

    Original `LogEpoch`

    Original StreamCursor

    Original Native Identity

    Original Canonical Payload

The Recording Manager must not assign new canonical publication identities during backfill.

### 20.4 Overlap With Ordinary Recording

During recovery, new live events may continue entering ordinary recording queues.

A recovery event may already exist in PostgreSQL because an earlier ambiguous transaction succeeded.

Idempotent recording must handle this overlap.

### 20.5 Partial Retention Availability

Suppose:

    Recording Missing:
        Cursor 100–200

    Retention Available:
        Cursor 150–200

Only the available portion can be recovered through retention.

The Recording Manager must not mark the full interval complete.

### 20.6 Recovery Expiration

If retained events expire before recording recovery can consume them:

    RecordingGapUnrecoverableFromRetention

The system may evaluate another supported recovery source.

For example, a later historical archive may contain the corresponding native executions.

However, historical archive recovery requires native identity reconciliation rather than assuming publication cursor equivalence.

### 20.7 Recording Recovery Invariant

    Retention Can Recover
    Only The Canonical Events
    It Actually Preserved.

---

## 21. Recording Guarantees and Failure Outcomes

The Recording Manager must independently expose database connectivity, write progress, durability, and coverage.

### 21.1 Recording Guarantees

Conceptual guarantees:

    DatabaseAvailable

    RecordingAdmissionAvailable

    PendingWritesRecoverable

    RecordingDurabilityEstablished

    RecordedCoverageVerified

These guarantees are independently evaluated.

### 21.2 Example: Healthy Recording

    DatabaseAvailable = Satisfied

    RecordingAdmissionAvailable = Satisfied

    RecordingDurabilityEstablished = Satisfied

    RecordedCoverageVerified = Satisfied

The final guarantee applies only to the verified interval.

It does not imply complete native exchange acquisition.

### 21.3 Example: Database Outage

    DatabaseAvailable = Unsatisfied

    RecordingAdmissionAvailable = Degraded

    PendingWritesRecoverable = Satisfied

    RecordingDurabilityEstablished = Recovering

    RecordedCoverageVerified = Unverified

Live acquisition may remain operational.

### 21.4 Example: Unrecoverable Recording Gap

    DatabaseAvailable = Satisfied

    RecordingAdmissionAvailable = Satisfied

    RecordingDurabilityEstablished = Satisfied

    RecordedCoverageVerified = Unsatisfied

This represents restored current recording with an unresolved historical recording gap.

### 21.5 Required Recording Policy

When PostgreSQL recording is required to guarantee bootstrap coverage, a recording failure must degrade the corresponding bootstrap guarantee.

The runtime must not report the archive-to-live bridge as complete merely because live acquisition continues.

### 21.6 Optional Recording Policy

When recording is optional, PostgreSQL failure must not automatically terminate otherwise healthy canonical source processing or Unix socket delivery.

Any recording gap remains visible.

### 21.7 Final Recording Invariant

    Successful Recording
    Requires Authoritative
    Database Durability Evidence.

    Complete Historical Coverage
    Requires Additional
    Source Integrity Evidence.

These guarantees must never be conflated.

---

## 22. Archive-to-PostgreSQL-to-Live Bootstrap

MarketForge's historical bootstrap must support three complementary market-data sources:

    Historical Exchange Archives

    PostgreSQL Recorded Events

    Live Retention and Continuous Acquisition

These sources provide different portions of the market-data timeline.

The Bootstrap Coordinator is responsible for selecting and reconciling them.

PostgreSQL provides durable recent-event coverage but does not independently determine historical completeness.

### 22.1 Bootstrap Objective

The objective is to construct a logically continuous market-data history from the requested historical starting point to current live acquisition.

Conceptually:

    Historical Starting Time
              |
              v
    Historical Exchange Archives
              |
              v
    PostgreSQL Recorded Events
              |
              v
    Live Retention
              |
              v
    Continuous Live Streaming

The transition must preserve canonical economic values, source identity, and applicable integrity guarantees.

### 22.2 Three-Layer Architecture

    HISTORICAL DATA SUBSYSTEM
              |
              v
       Exchange Archives
              |
              v
      Historical Processing
              |
              |
              +-----------------------------+
                                            |
                                            v
                                   Bootstrap Coordinator
                                            ^
                                            |
              +-----------------------------+
              |
              |
        PostgreSQL Reader
              ^
              |
      PostgreSQL Recording
              ^
              |
              +-----------------------------+
                                            |
                                            |
                                    Live Retention
                                            ^
                                            |
                                    Canonical Processing
                                            ^
                                            |
                                      Live Exchange

The bootstrap coordinator consumes validated canonical events from the available sources.

It must not independently reimplement native exchange normalization.

### 22.3 Example Coverage Timeline

Suppose the current time is:

    October 19, 14:00

The exchange's latest available daily archive covers:

    October 18

PostgreSQL contains events recorded since:

    October 18, 23:50

Live Retention contains events beginning:

    October 19, 13:45

The timeline becomes:

    October 18                  October 19
    |------------------------------|------------------->

    Historical Archives
    [===========================]

    PostgreSQL Recording
                              [=======================]

    Live Retention
                                              [=======]

    Current Live Acquisition
                                                     --->

The overlaps are intentional.

They allow the Bootstrap Coordinator to validate continuity and avoid duplicate logical executions.

### 22.4 Bootstrap Source Selection

The coordinator determines which source should supply each requested interval.

Possible inputs include:

- Historical archive availability.
- Historical archive integrity status.
- PostgreSQL recording coverage.
- Live Retention availability.
- Native execution identity.
- Source sequence evidence.
- Instrument metadata revision.
- Requested bootstrap time range.
- Consumer completeness requirements.

The coordinator must not select a source solely because its nominal date range contains the requested timestamp.

### 22.5 Preferred Source Policy

The initial recommended policy is:

    Historical Archives
        -> Preferred for older verified history.

    PostgreSQL
        -> Preferred for recent durable events
           not yet covered by verified archives.

    Live Retention
        -> Preferred for protected bootstrap
           overlap and live handoff.

This is a source-selection preference, not a rigid partition by timestamp.

The coordinator may require overlapping reads from multiple sources to establish continuity.

### 22.6 Recording Availability

A bootstrap may begin when PostgreSQL is unavailable if another source provides the required coverage.

Conversely, PostgreSQL may provide the recent interval when live retention no longer contains it.

The required guarantee determines whether bootstrap may proceed.

### 22.7 Source Completeness

Each source must report its own coverage evidence.

Conceptually:

    Historical:
        Verified / Partial / Unverified

    PostgreSQL:
        Recorded / Missing / Unverified

    Live Retention:
        Available / Partial / Unavailable

The coordinator evaluates overall continuity without treating these classifications as interchangeable.

### 22.8 Bootstrap Invariant

    The Bootstrap Coordinator
    Must Never Infer Complete
    Historical Coverage Solely
    From The Presence Of Data
    In Multiple Storage Systems.

---

## 23. Bootstrap Coverage Planning

Before reading the full requested history, the Bootstrap Coordinator constructs a coverage plan.

The plan identifies which source intervals are required and where reconciliation must occur.

### 23.1 CoveragePlan

Conceptual representation:

    BootstrapCoveragePlan {
        bootstrap_id

        stream_id
        requested_time_interval

        historical_intervals
        recorded_intervals
        live_retention_intervals

        overlap_intervals
        unresolved_intervals

        source_priority_policy
        completeness_policy

        configuration_revision
    }

The exact representation belongs to the Bootstrap Coordinator.

### 23.2 Planning Inputs

The coordinator queries:

    Historical Archive Inventory

    Recording Coverage Index

    Live Retention Coverage Index

    Source Integrity State

These interfaces must return explicit coverage information.

### 23.3 Time Interval Convention

The recommended internal convention is:

    [start_timestamp_ns, end_timestamp_ns)

This avoids ambiguity at adjacent interval boundaries.

Native exchange archive boundaries may require normalization into this convention.

### 23.4 Coverage Classification

A planned interval may be:

    HistoricalAvailable

    RecordedAvailable

    LiveRetainedAvailable

    MultipleSourcesAvailable

    Missing

    Unverified

These classifications describe source availability.

They do not automatically establish native event continuity.

### 23.5 Coverage Gaps

Suppose:

    Historical:
        [00:00, 12:00)

    PostgreSQL:
        [12:05, 14:00)

The interval:

    [12:00, 12:05)

requires further evaluation.

The coordinator must determine whether:

- Another archive exists.
- Live retention covers the interval.
- Native trade recovery can provide it.
- The interval contains verified market inactivity.
- The interval remains unverified or incomplete.

The coordinator must not fill the interval with fabricated records.

### 23.6 Coverage Planning vs Execution

Planning identifies potential sources.

Execution validates actual records and their identities.

Therefore:

    CoveragePlanAvailable
        !=
    HistoricalCoverageVerified

The bootstrap result must be based on execution evidence.

---

## 24. Historical and Recorded Trade Overlap

Historical archives and PostgreSQL may contain the same native trades.

The bootstrap process must reconcile those representations without deleting distinct executions.

### 24.1 Overlap Example

Historical archive:

    Trade 100
    Trade 101
    Trade 102
    Trade 103
    Trade 104

PostgreSQL:

    Trade 103
    Trade 104
    Trade 105
    Trade 106

Expected logical output:

    Trade 100
    Trade 101
    Trade 102
    Trade 103
    Trade 104
    Trade 105
    Trade 106

This requires valid native execution identity semantics.

### 24.2 Native Identity

Trade reconciliation follows **06.2 — Trade Processing and Continuity**.

The identity may depend on:

    Exchange

    Market

    InstrumentId

    NativeExecutionId

    NativeIdentityScope

A trade identifier must not be assumed globally unique.

### 24.3 Duplicate Representations

When historical and recorded trades represent the same native execution, the coordinator emits one logical execution into the bootstrap result.

Both representations may contribute provenance evidence.

The original database row remains unchanged.

### 24.4 Conflicting Representations

If matching native identities contain conflicting economic values:

    Historical:
        TradeId = 100
        Price = 65000

    PostgreSQL:
        TradeId = 100
        Price = 65100

the coordinator must report:

    HistoricalRecordingConflict

It must not silently choose one value based solely on source preference.

### 24.5 Missing Native Identifiers

Some source records may lack native execution identifiers.

In that case, deduplication requires another explicitly supported identity or continuity mechanism.

The coordinator must not assume:

    Same Timestamp
        +
    Same Price
        +
    Same Quantity

implies the same execution.

Distinct trades may share those values.

### 24.6 Event Ordering

After deduplication, historical bootstrap applies the established historical merge-ordering contract.

The database's row insertion order must not determine historical event ordering.

The existing historical deterministic ordering rules remain authoritative.

### 24.7 Recording Provenance

A PostgreSQL record should preserve:

    Original Source Stream

    Native Execution Identity

    Canonical Event Identity

    Acquisition Provenance

    Recording Commit Evidence

These permit reconciliation without modifying the event's original economic values.

### 24.8 Trade Overlap Invariant

    One Native Execution
    May Exist In Multiple
    Physical Data Sources.

    It Must Appear Once
    In The Reconciled
    Logical Execution History.

This guarantee applies only where equivalent executions can be identified reliably.

---

## 25. Historical and Recorded L2 Reconciliation

L2 reconciliation is more complex than trade reconciliation because book state depends on valid initialization and ordered updates.

### 25.1 L2 Source Data

Historical archives may contain:

    Snapshots

    Incremental Updates

    Absolute Level Updates

    Relative Level Updates

PostgreSQL stores accepted canonical L2 events produced by the Live BookStore and processing pipeline.

The sources may therefore have different original acquisition representations while sharing a canonical output contract.

### 25.2 L2 Boundary Requirement

A historical L2 sequence may connect to PostgreSQL-recorded events only when the applicable source protocol establishes a valid transition.

Required evidence may include:

- Compatible sequence identifiers.
- Valid initialization snapshots.
- Matching book generations where applicable.
- Verified native update continuity.
- Compatible instrument metadata.
- Preserved event boundaries.

Timestamp overlap alone is insufficient.

### 25.3 Recording Initialization

PostgreSQL recording does not require an independent
initialization snapshot at every storage boundary.

Historical bootstrap may reconstruct the current book
by replaying:

    Historical Initialization
              |
              v
    Historical Absolute Updates
              |
              v
    PostgreSQL Absolute Updates
              |
              v
    Live Absolute Updates
              |
              v
       Current BookStore

This is valid only when the required source continuity,
event ordering, and book-generation compatibility
have been established.

A transition from historical archives to PostgreSQL
does not itself require a new snapshot.

Likewise, a transition from PostgreSQL to continuous
Live processing does not require a new snapshot
when continuity is verified.

However, if PostgreSQL recording begins while a
BookStore is already synchronized, the recording
subsystem must not assume that subsequently recorded
changes are independently reconstructable.

There are two supported situations.

**Situation A — Continuous Historical Coverage**

A valid historical initialization exists, and the
historical-to-recorded boundary can be verified.

PostgreSQL may record subsequent absolute updates
without a new initialization.

Bootstrap reconstructs the existing book generation
by replaying historical and recorded events.

**Situation B — Independent Recording Coverage**

No compatible historical initialization or verified
continuous transition is available.

The Live depth processor must provide a complete
authoritative initialization of its current book state
before the recorded segment can be treated as
independently reconstructable.

The Recording Manager persists this initialization
using the existing depth-event and depth-level tables.

It does not reconstruct the book independently.

A new initialization is also required after a
synchronization failure when previous book state
can no longer be trusted.

An initialization event must contain the complete
authoritative book state rather than only the
differences from the preceding generation.

Periodic initialization checkpoints are optional.

They may improve bootstrap performance but are not
required merely because a storage boundary occurs.

The fundamental invariant is:

    A Storage Boundary
    Does Not Require
    A New Book Initialization.

    A Reconstruction Boundary
    Requires A Valid
    Starting Book State.

### 25.4 Historical-to-Recorded L2 Join

Conceptually:

    Historical Snapshot
             |
             v
    Historical Deltas
             |
             v
    Validate Boundary
             |
             v
    PostgreSQL Recorded Deltas
             |
             v
    Reconstructed Book

This is valid only when all required synchronization evidence exists.

### 25.5 Unbridgeable L2 History

If sequence continuity cannot be established, the coordinator must not concatenate historical and recorded L2 deltas as though they formed one valid book sequence.

A newer authoritative snapshot may establish a new reconstruction segment.

Example:

    Historical L2
    [==============]

    Missing Deltas
                   [???]

    PostgreSQL Snapshot
                        |
                        v
    New Book Generation
                        [==============]

The newer snapshot restores current state from its initialization point.

It does not reconstruct the missing historical transitions.

### 25.6 Book Generation Identity

Live BookStore generations identify authoritative synchronization epochs.

Historical reconstruction segments may use different locally assigned identifiers.

The coordinator must not compare these identifiers numerically without an explicit mapping.

### 25.7 L2 Overlap Deduplication

Equivalent L2 messages may require:

- Native sequence identity.
- Source operation identity.
- Canonical event grouping.
- Source timestamp.
- Compatible payload evidence.

Individual level equality alone does not establish that two depth events are duplicates.

### 25.8 L2 Reconstruction Invariant

    Historical L2
        +
    PostgreSQL L2
        !=
    Automatically Continuous L2.

A valid reconstruction requires protocol-compatible synchronization evidence.

---

## 26. PostgreSQL-to-Live Handoff

After reading recent recorded data, a bootstrap consumer must transition to live retention and continuous acquisition.

The handoff follows the principles defined in **09 — Live Retention and Parquet Spill**.

### 26.1 Handoff Objective

The consumer must not miss accepted canonical events between its final PostgreSQL read and the beginning of continuous live delivery.

### 26.2 Handoff Race

Consider:

    PostgreSQL Query Completes:
        Cursor 1000

    Live Stream Already At:
        Cursor 1050

If the consumer immediately subscribes to the latest live frontier, cursors 1001–1049 may be missed.

The handoff must account for this interval.

### 26.3 Recommended Sequence

    Bootstrap Begins
          |
          v
    Establish Live Retention Protection
          |
          v
    Capture Protected Live Boundary
          |
          v
    Read Historical Archives
          |
          v
    Read PostgreSQL Coverage
          |
          v
    Reconcile Archive/Database Overlap
          |
          v
    Reconcile Database/Retention Overlap
          |
          v
    Replay Protected Live Events
          |
          v
    Establish Continuous Delivery Boundary
          |
          v
    Activate Live Consumption
          |
          v
    Release Bootstrap Protection

The retention protection must be established before the long-running historical read begins.

### 26.4 Database/Retention Overlap

Suppose PostgreSQL contains:

    Cursor 900–1100

Live Retention contains:

    Cursor 1000–1200

The bootstrap consumer should avoid replaying cursors 1000–1100 twice.

Canonical publication identities provide the primary reconciliation mechanism when both sources preserve the same cursor epoch.

### 26.5 Cursor Epoch Compatibility

A PostgreSQL record from a previous runtime may use a different cursor epoch from the current live retention stream.

In that case, numeric cursor values must not be compared directly.

The coordinator must use supported source identity and coverage evidence to reconcile the boundary.

### 26.6 Database Lag

PostgreSQL recording may lag behind live acquisition.

Example:

    Last Committed Database Cursor:
        1000

    Current Live Cursor:
        1500

The Bootstrap Coordinator must not wait for recording to catch up if the protected Live Retention interval provides a valid continuous handoff.

It may read the missing interval directly from retention.

### 26.7 Recording Outage During Bootstrap

If PostgreSQL becomes unavailable during bootstrap:

- The coordinator evaluates already acquired data.
- It checks alternative coverage sources.
- It preserves active Live Retention protection.
- It may retry the database read.
- It must not declare completion without satisfying required coverage guarantees.

### 26.8 Final Handoff Evidence

A successful handoff requires:

    Historical Coverage Evaluated

    Database Coverage Evaluated

    Overlap Reconciled

    Protected Live Events Consumed

    Continuous Delivery Established

The coordinator must not release protection before the consumer safely commits its processing frontier.

---

## 27. Historical Archive Publication

Exchanges may publish historical archives after MarketForge has already recorded the corresponding events.

This changes which data source is needed for future bootstrap operations.

### 27.1 Archive Availability

Suppose:

    October 19:
        PostgreSQL records live events.

    October 20:
        Exchange publishes October 19 archive.

The archive may now provide historical coverage that previously depended on PostgreSQL.

However, archive publication alone does not prove that recorded data is safe to delete.

### 27.2 Archive Discovery

The Historical Data Subsystem owns archive discovery and validation.

The Recording Manager must not independently scrape exchange archive websites.

The historical subsystem reports:

    ArchiveAvailable

    ArchiveValidated

    ArchiveCoverage

to the relevant coverage-coordination interface.

### 27.3 Archive Validation

Before archived data can replace recorded coverage, the historical subsystem must validate:

- Archive availability.
- Expected instrument and market.
- Archive format compatibility.
- Data integrity.
- Relevant native event identities.
- Required time coverage.
- Applicable continuity evidence.

### 27.4 Source Preference Transition

After an archive becomes validated:

    Previous Bootstrap Preference:

        Historical Archives
        +
        PostgreSQL Recent Data

    New Bootstrap Preference:

        Historical Archives
        +
        Smaller PostgreSQL Recent Interval

The transition reduces reliance on database-recorded history.

It does not require rewriting previously recorded events.

### 27.5 Archive/Recording Reconciliation

Before declaring recorded intervals replaceable, the coordinator must verify that the corresponding archive provides the required coverage.

Where native execution identities permit, it should compare archive and recording overlap.

Conflicts must be reported.

### 27.6 Archive Coverage State

Conceptually:

    ArchiveCoverageState {
        exchange
        market
        instrument_id

        interval

        availability_status
        validation_status
        integrity_status

        evaluated_at
    }

This state is owned by the Historical Data Subsystem.

### 27.7 Publication Delay

Archive publication may be delayed.

Some exchanges may publish incomplete or corrected archives.

Therefore, the Recording Manager must not use a fixed assumption such as:

    Delete Records After 24 Hours

unless the configured retention policy explicitly accepts the associated coverage risk.

### 27.8 Archive Replacement Invariant

    Archive Published
        !=
    Recorded Data Safe To Delete.

Deletion requires the configured replacement and retention conditions to be satisfied.

---

## 28. Recording Retention and Cleanup

PostgreSQL recording must not grow indefinitely unless explicitly configured and provisioned for that purpose.

The initial recommended strategy is to retain recent data until corresponding historical archive coverage has been validated.

### 28.1 Retention Objectives

The retention policy should:

- Bound database growth.
- Preserve recent bootstrap coverage.
- Account for delayed archive publication.
- Prevent premature deletion.
- Support efficient partition cleanup.
- Respect active bootstrap readers.
- Preserve unresolved coverage diagnostics.
- Avoid unnecessary duplicate long-term storage.

### 28.2 RetentionPolicy

Conceptual representation:

    RecordingRetentionPolicy {
        minimum_retention_duration

        archive_replacement_required

        archive_validation_required

        safety_overlap_duration

        maximum_retention_duration

        cleanup_interval
    }

The exact configuration keys belong to Document 03.

### 28.3 Minimum Retention

Recorded events must remain available for at least the configured minimum retention period unless an explicit exceptional policy permits earlier deletion.

This provides a safety window for:

- Late archive publication.
- Historical bootstrap retries.
- Archive validation.
- Recovery from temporary failures.

### 28.4 Archive-Aware Retention

The recommended default policy is:

    Retain Until:

        Minimum Retention Satisfied

        AND

        Corresponding Archive Coverage Validated

        AND

        Required Safety Overlap Satisfied

        AND

        No Active Bootstrap Protection Requires Data

Only then may recorded events become deletion-eligible.

### 28.5 Maximum Retention

A maximum retention duration may be configured to enforce storage limits.

However, an unconditional maximum age can conflict with archive-replacement requirements.

For example:

    Maximum Retention = 7 days

    Exchange Archive Delayed = 10 days

Deleting the PostgreSQL interval after seven days would create a potential bootstrap coverage gap.

The policy must resolve this conflict explicitly.

### 28.6 Retention Modes

Supported conceptual modes:

    ArchiveVerified

    FixedDuration

    Manual

**ArchiveVerified**

Deletes only after validated archive replacement and minimum-retention conditions.

**FixedDuration**

Deletes according to configured age, potentially accepting historical coverage risk.

**Manual**

Requires explicit operator action.

The selected mode must be visible in runtime configuration.

### 28.7 ArchiveVerified Default

The initial recommended default is:

    ArchiveVerified

This matches PostgreSQL's primary role as a temporary durable bridge to historical archives.

Storage exhaustion under this policy must trigger explicit escalation rather than silently deleting unverified coverage.

### 28.8 Cleanup Eligibility

Before deleting records, the cleanup process must verify:

- Effective retention policy.
- Archive coverage evidence.
- Minimum retention age.
- Safety overlap requirements.
- Active bootstrap readers.
- Active recovery operations.
- Database partition boundaries.
- Applicable legal or operational retention constraints.

### 28.9 Cleanup and Partitioning

Time-based PostgreSQL partitions may support efficient bulk deletion.

However, a partition may be dropped only when its entire contents are deletion-eligible.

If one partition contains both replaceable and required data, dropping it is unsafe.

The implementation may use finer partitioning or controlled row deletion.

### 28.10 Identity Registry Cleanup

If canonical recording identities are stored separately from event-time partitions, their lifecycle must be coordinated with recorded-data deletion.

The system must not leave permanent identity records referencing deleted market-data rows unless explicitly designed as tombstones.

Likewise, identity deletion must not permit unsafe duplicate recording during active retry or recovery.

### 28.11 Active Bootstrap Readers

A bootstrap consumer may be reading PostgreSQL while cleanup begins.

The cleanup system must protect the required interval through an explicit read lease, coverage pin, or another validated mechanism.

A query transaction alone should not be assumed to provide indefinite retention protection.

### 28.12 Cleanup Transaction

Cleanup operations must update:

    Recorded Event Availability

    Recording Coverage Metadata

    Identity Registry State

consistently.

A deletion operation must not leave the coverage index advertising records that no longer exist.

### 28.13 Cleanup Failure

Cleanup may fail because of:

- Database connection loss.
- Lock contention.
- Active readers.
- Partition dependency errors.
- Permission failures.
- Transaction failure.

A failed cleanup must not be classified as successful disk reclamation.

### 28.14 Cleanup Invariant

    Recorded Data May Be Deleted
    Only When The Selected
    Retention Contract
    Explicitly Permits It.

---

## 29. Archive Coverage and Recording Lifecycle Coordination

The Recording Manager and Historical Data Subsystem must coordinate through explicit interfaces.

They must not directly mutate each other's internal state.

### 29.1 Coordination Responsibilities

The Historical Data Subsystem provides:

    Available Archive Intervals

    Validated Archive Intervals

    Archive Integrity Results

    Archive Identity Evidence

The Recording Manager provides:

    Recorded Coverage

    Recording Gaps

    Committed Cursor Intervals

    Retention Eligibility

The Bootstrap Coordinator evaluates their combined use.

### 29.2 Coordination Flow

    Exchange Archive Published
              |
              v
    Historical Archive Discovery
              |
              v
    Historical Archive Validation
              |
              v
    Coverage Coordination
              |
              v
    Compare With PostgreSQL Coverage
              |
              v
    Determine Replaceable Intervals
              |
              v
    Apply Recording Retention Policy
              |
              v
    Schedule Eligible Cleanup

No cleanup occurs merely because an archive filename exists.

### 29.3 Coverage Replacement Result

Conceptually:

    RecordingReplacementAssessment {
        stream_id

        recorded_interval
        historical_interval

        historical_validation_status
        reconciliation_status

        replacement_eligible
        unresolved_conflicts

        evaluated_at
    }

The assessment must be based on authoritative coverage evidence.

### 29.4 Partial Archive Replacement

An archive may replace only part of a recorded interval.

Example:

    PostgreSQL:
        [00:00, 24:00)

    Validated Archive:
        [00:00, 18:00)

Only the verified corresponding interval may become replacement-eligible.

The remaining recorded interval must be preserved when required by the retention contract.

### 29.5 Recording After Archive Availability

MarketForge may continue recording events even when historical archive coverage exists.

This can occur because:

- Recording was already active.
- An archive was published late.
- Historical integrity verification is pending.
- The source remains configured for recording.

The Recording Manager must not automatically disable recording solely because an archive becomes available.

Recording configuration and recording retention are separate concerns.

### 29.6 Historical Archive Corrections

An exchange may publish corrected or replacement historical data.

The historical subsystem must track the relevant archive version or validation identity.

If a previously validated archive is later found invalid, the coverage coordinator must reevaluate replacement eligibility.

Already deleted PostgreSQL data cannot be assumed recoverable.

This risk must be considered when configuring safety overlap and retention duration.

### 29.7 Coordination Invariant

    PostgreSQL Is The
    Durable Recent-Data Bridge.

    Historical Archives Are The
    Preferred Long-Term Source.

    The Transition Between Them
    Requires Verified Coverage,
    Not Merely Elapsed Time.

---

## 30. Recording Observability

The Recording subsystem must expose operational state through MarketForge's internal Metrics Registry, runtime API, and Ratatui TUI.

No Prometheus or Grafana dependency is required.

Operational logs remain local.

### 30.1 Observability Objectives

Recording observability must distinguish:

- Database connectivity.
- Recording admission.
- Pending event volume.
- Transaction throughput.
- Commit latency.
- Recording durability.
- Recorded coverage.
- Known recording gaps.
- Retention-based recovery.
- Archive-replacement readiness.
- Database capacity and storage pressure.

The system must not present a healthy database connection as proof that complete market-data coverage exists.

### 30.2 RecordingHealthSnapshot

Conceptual representation:

    RecordingHealthSnapshot {
        runtime_instance_id

        recording_state
        database_state

        active_recording_streams
        pending_batches
        pending_events
        pending_bytes

        last_commit_at
        oldest_uncommitted_event_age

        committed_coverage
        unresolved_recording_gaps

        database_capacity
        retention_recovery_state

        last_error
        evaluated_at
    }

The snapshot must remain bounded.

Detailed interval information should be available through separate queries rather than copied into every global health snapshot.

### 30.3 Recording Metrics

| Metric | Description |
|---|---|
| `recording_events_admitted_total` | Events admitted to recording |
| `recording_events_committed_total` | Events durably committed |
| `recording_batches_created_total` | Recording batches created |
| `recording_batches_committed_total` | Successfully committed batches |
| `recording_batches_failed_total` | Failed recording batches |
| `recording_batches_pending` | Pending batches |
| `recording_queue_events` | Events awaiting persistence |
| `recording_queue_bytes` | Memory occupied by pending work |
| `recording_commit_latency` | Database transaction completion latency |
| `recording_batch_size_events` | Events per committed batch |
| `recording_batch_size_bytes` | Bytes per committed batch |
| `recording_oldest_pending_age` | Age of oldest uncommitted event |

Counters must distinguish newly committed events from idempotent retries of existing records.

### 30.4 Database Metrics

| Metric | Description |
|---|---|
| `recording_database_connected` | Database connectivity state |
| `recording_database_connections` | Active recording connections |
| `recording_database_reconnections_total` | Database reconnections |
| `recording_transactions_total` | Attempted transactions |
| `recording_transactions_failed_total` | Failed transactions |
| `recording_commit_uncertain_total` | Ambiguous commit outcomes |
| `recording_commit_reconciled_total` | Successfully reconciled uncertain commits |
| `recording_database_write_bytes_total` | Approximate bytes written |
| `recording_database_query_latency` | Database operation latency |

High-cardinality values such as individual batch identifiers must not become unbounded metric labels.

### 30.5 Coverage Metrics

The Recording Manager should expose:

    ActiveRecordedStreams

    KnownRecordingGaps

    UnverifiedRecordingIntervals

    LastContiguousCommittedCursor

    LastCommittedEventTimestamp

    RecordingLag

Recording lag must distinguish:

    Event-Time Lag

from:

    Publication-Cursor Lag

The two measure different properties.

### 30.6 Recovery Metrics

Relevant measurements include:

| Metric | Description |
|---|---|
| `recording_recovery_operations_total` | Recording recovery attempts |
| `recording_recovery_completed_total` | Completed recovery operations |
| `recording_recovery_failed_total` | Failed recovery operations |
| `recording_recovered_events_total` | Events recovered from retention |
| `recording_recovery_pending_events` | Outstanding recovery backlog |
| `recording_gaps_detected_total` | Recording gaps detected |
| `recording_gaps_resolved_total` | Recording gaps resolved |
| `recording_unrecoverable_gaps_total` | Gaps classified as unrecoverable |

A recovered database connection must not increment the complete-coverage metric until the relevant gaps have been reconciled.

### 30.7 TUI Integration

The Ratatui Recording view should display:

    PostgreSQL:
        Connected / Disconnected

    Recording:
        Active / Degraded / Disabled

    Streams Recording:
        N

    Write Throughput:
        Events/s

    Pending Events:
        N

    Pending Batches:
        N

    Oldest Pending Age:
        Duration

    Last Commit:
        Timestamp

    Recording Gaps:
        N

    Disk Usage:
        Estimated / Available

A stream-specific detail view may display:

    StreamId
    RecordingGeneration

    Recording Enabled

    Last Admitted Cursor
    Last Committed Cursor

    Unresolved Intervals

    Last Database Error

The TUI must use the runtime control interface.

It must not execute recording SQL directly.

### 30.8 Runtime API

Conceptual operations:

    GetRecordingHealth

    ListRecordingStreams

    GetStreamRecordingStatus

    GetRecordingCoverage

    ListRecordingGaps

    EnableStreamRecording

    DisableStreamRecording

    RequestRecordingFlush

    RequestRecordingRecovery

    GetRecordingRetentionStatus

Mutating operations must pass through the Recording Manager's validated control plane.

### 30.9 Local Logging

Relevant structured events include:

    RecordingStarted

    RecordingStopped

    RecordingAdmissionFailed

    RecordingBatchSealed

    RecordingBatchCommitted

    RecordingBatchFailed

    RecordingCommitUncertain

    RecordingCommitReconciled

    RecordingGapDetected

    RecordingGapResolved

    RecordingDatabaseDisconnected

    RecordingDatabaseReconnected

    RecordingRetentionCleanupStarted

    RecordingRetentionCleanupCompleted

Operational logs remain local.

PostgreSQL must not become the application logging database.

---

## 31. PostgreSQL Capacity Planning

Recording volume depends on the selected streams, event rates, event sizes, indexes, and retention duration.

The system must enforce finite resource budgets and provide enough capacity information to detect recording risk before storage exhaustion.

### 31.1 Capacity Dimensions

Relevant dimensions include:

    RecordedEventsPerSecond

    AverageTradeRowSize

    AverageDepthEventSize

    AverageDepthLevelCount

    RecordingRetentionDuration

    IndexOverhead

    TransactionLogVolume

    DatabaseMaintenanceOverhead

    ConcurrentBootstrapReaders

### 31.2 Storage Estimation

An approximate storage model is:

    DailyStorageBytes =
        DailyTradeStorage
        +
        DailyDepthStorage
        +
        IndexOverhead
        +
        DatabaseOverhead

For trades:

    DailyTradeStorage =
        TradeEventsPerSecond
        ×
        SecondsPerDay
        ×
        AverageStoredTradeBytes

For depth:

    DailyDepthStorage =
        DepthEventsPerSecond
        ×
        SecondsPerDay
        ×
        AverageStoredDepthEventBytes

The depth estimate must include child level rows.

These formulas are planning approximations.

Actual storage consumption must be measured.

### 31.3 Recording Throughput

The database must sustain:

    AverageRecordingRate

and accommodate:

    PeakRecordingRate

while supporting:

- Concurrent bootstrap reads.
- Index maintenance.
- Transaction logging.
- Recovery backlog processing.
- Retention cleanup.

The system must not size recording solely for average event throughput.

### 31.4 Recovery Headroom

Following a database outage, the Recording Manager may need to persist older retained events while continuing to accept new live events.

Therefore:

    RequiredRecoveryWriteRate
        >
    CurrentLiveRecordingRate

may be necessary to reduce the backlog.

If database write throughput merely matches incoming live volume, the recovery backlog may never shrink.

### 31.5 Capacity Example

Suppose:

    Live Recording Rate:
        100,000 events/s

    Database Sustainable Rate:
        150,000 events/s

The theoretical capacity available for recovery is:

    50,000 events/s

This ignores transaction contention, payload-size differences, and other database workloads.

Actual recovery performance must be measured.

### 31.6 Database Storage Budget

The Recording Manager should monitor available database storage where the deployment and database permissions permit it.

However, ordinary PostgreSQL connections may not expose authoritative host-filesystem free space.

Therefore, the implementation must distinguish:

    Database Size

    Configured Storage Budget

    Verified Free Capacity

    Unknown Physical Capacity

It must not invent free-space estimates from database table sizes alone.

### 31.7 Storage Pressure

Conceptual states:

    Healthy
    Warning
    Critical
    Exhausted

Appropriate responses may include:

- Accelerating eligible retention cleanup.
- Restricting optional recording.
- Reporting database capacity pressure.
- Applying recording admission limits.
- Escalating required recording failures.

The system must not delete unverified recent market-data coverage merely to recover capacity.

### 31.8 Index Costs

Indexes improve historical queries and identity reconciliation but increase write overhead.

Index design should be benchmarked using representative:

- Trade ingestion workloads.
- Depth ingestion workloads.
- Bootstrap range queries.
- Native identity lookups.
- Partition cleanup operations.

The initial implementation should avoid excessive indexes before workload measurements justify them.

### 31.9 Database Maintenance

PostgreSQL maintenance requirements may include:

- Vacuuming.
- Statistics updates.
- Index maintenance.
- Partition management.
- Transaction-log management.
- Backup and recovery configuration.

These are infrastructure responsibilities.

The Recording Manager must expose relevant operational failures without attempting to replace PostgreSQL administration.

### 31.10 Capacity Invariant

    Recording Capacity Must Be
    Sufficient For Both
    Current Ingestion And
    Required Recovery Work.

A database operating successfully at its absolute write limit has no spare capacity to recover an accumulated backlog.

---

## 32. Recording Testing Strategy

The Recording subsystem requires unit tests, PostgreSQL integration tests, crash simulations, and historical bootstrap reconciliation tests.

Tests must verify both persistence correctness and the absence of false coverage claims.

### 32.1 Testing Categories

| Category | Scope |
|---|---|
| Schema tests | Canonical field compatibility |
| Numerical tests | Exact decimal preservation |
| Timestamp tests | Nanosecond timestamp preservation |
| Identity tests | Canonical and native identity behavior |
| Transaction tests | Atomic event and coverage commits |
| Batching tests | Batch boundaries and ordering |
| Concurrency tests | Multiple recording writers |
| Idempotency tests | Duplicate write attempts |
| Outage tests | Database disconnect and recovery |
| Crash tests | Process termination during recording |
| Retention tests | Recording backfill from retained events |
| Bootstrap tests | Archive/PostgreSQL/live reconciliation |
| Cleanup tests | Archive-aware retention |
| Property-based tests | Arbitrary recording and failure sequences |
| Performance tests | Throughput, latency, storage growth |

### 32.2 Test Infrastructure

Tests should use:

- Isolated PostgreSQL instances.
- Temporary test schemas.
- Deterministic canonical-event fixtures.
- Mock Stream Registry interfaces.
- Mock Live Retention Manager.
- Mock Historical Archive Reader.
- Controlled database connection failures.
- Deterministic retry scheduling.
- Injected commit ambiguity.
- Forced process termination.

Tests must not depend on production market-data tables.

### 32.3 Test Isolation

Every test must use an isolated database namespace or disposable PostgreSQL instance.

Cleanup must not affect unrelated recording data.

The test suite must never assume that a shared development database is empty.

---

## 33. Schema and Identity Tests

### 33.1 Canonical Trade Round Trip

Insert and retrieve canonical trades containing:

- Optional native trade IDs.
- Native sequence values.
- Exact prices.
- Base quantities.
- Quote quantities.
- Contract quantities.
- RPI indicators.
- Optional derivatives metadata.
- Nanosecond timestamps.

Verify exact canonical equivalence.

### 33.2 Depth Round Trip

Insert one grouped depth event with several level changes.

Verify:

- Parent event identity.
- Complete child level set.
- Level ordering.
- Exact decimal values.
- Zero-quantity deletion semantics.
- Initialization boundary.
- Book generation.
- Sequence metadata.

### 33.3 Missing vs Zero Quantities

Verify:

    NULL != 0

For example:

    quantity_base = NULL

must remain distinguishable from:

    quantity_base = 0

This is essential for canonical quantity semantics.

### 33.4 Timestamp Precision

Test nanosecond timestamps that are not representable exactly by PostgreSQL `TIMESTAMPTZ`.

Verify that the authoritative BIGINT timestamp survives without precision loss.

### 33.5 Canonical Identity

Attempt to insert the same canonical publication identity twice.

Expected:

    Identical Payload:
        Idempotent Success

    Conflicting Payload:
        Integrity Failure

### 33.6 Partition Identity

Verify that canonical identity uniqueness remains enforced across event-time partitions.

A retry using the same publication identity but a conflicting event timestamp must not create a second authoritative event.

### 33.7 Native Execution Identity

Verify that equal native trade IDs belonging to different valid identity scopes are not incorrectly deduplicated.

The database must not assume native identifiers are globally unique.

---

## 34. Transaction and Batching Tests

### 34.1 Batch Commit

Write a valid batch containing several canonical events.

Expected:

- All event rows committed.
- Identity records committed.
- Recording coverage updated.
- Batch state marked committed.

### 34.2 Atomic Depth Failure

Attempt to write a grouped depth event where one child level insertion fails.

Expected:

- No partial depth event committed.
- No invalid coverage update.
- Transaction failure reported.

### 34.3 Out-of-Order Commit Completion

Simulate:

    Batch A:
        Cursor 100–199

    Batch B:
        Cursor 200–299

    Batch C:
        Cursor 300–399

Commit:

    A
    C
    B

Verify that the contiguous committed frontier does not advance through the missing Batch B interval before its commit.

### 34.4 Batch Size Limits

Verify:

- Maximum event count.
- Maximum encoded bytes.
- Flush interval.
- Explicit flush.
- Graceful shutdown flush.
- Memory reservation accounting.

### 34.5 Concurrent Writers

Run multiple recording writers against independent streams.

Verify:

- No cross-stream identity collisions.
- No inconsistent coverage frontiers.
- No duplicate event acceptance.
- Correct transaction isolation.

### 34.6 Hot Configuration Changes

Change recording batch size and flush interval during active recording.

Verify that changes apply at safe batch boundaries.

Already sealed batches must retain their original execution contract.

---

## 35. Failure and Crash Recovery Tests

### 35.1 Database Disconnect

Disconnect PostgreSQL during recording.

Expected:

- Recording becomes degraded.
- Pending work remains bounded.
- Source acquisition remains independently classified.
- Recovery begins according to policy.
- Recording gaps remain visible.

### 35.2 Database Reconnection

Restore PostgreSQL availability.

Verify:

- Schema compatibility is checked.
- Pending transactions are reconciled.
- Backlog recording resumes.
- Commit frontiers advance correctly.
- Recording health updates only after validation.

### 35.3 Ambiguous Commit

Simulate:

    Transaction Commits
          |
          v
    Connection Lost
          |
          v
    Client Receives No Confirmation

Expected:

- Batch becomes `CommitUncertain`.
- Existing durable records are discovered.
- Equivalent records are accepted idempotently.
- No duplicate logical events are created.

### 35.4 Crash Before Commit

Terminate MarketForge after batch creation but before database commit.

After restart:

- Uncommitted events are not claimed durable.
- Database state is inspected.
- Available retention may provide missing events.
- Unavailable coverage remains explicit.

### 35.5 Crash After Commit

Terminate MarketForge immediately after database commit but before the Recording Manager updates its in-memory frontier.

After restart:

- Committed rows remain available.
- Recording coverage is reconstructed.
- No duplicate events are inserted.
- The old runtime generation cannot corrupt current state.

### 35.6 Database Storage Exhaustion

Simulate PostgreSQL rejecting writes because of storage exhaustion.

Verify:

- Recording failures are explicit.
- Retry policy does not create an uncontrolled loop.
- Live acquisition remains isolated where policy permits.
- Protected retention is used only when available.
- No false durability claims occur.

### 35.7 Retention-Based Backfill

Simulate:

    PostgreSQL Recorded:
        Cursor 100–200

    Database Outage:
        Cursor 201–300

    Live Retention:
        Cursor 201–400

Restore PostgreSQL.

Expected:

- Cursors 201–300 are recovered.
- Already recorded events are deduplicated.
- Current recording continues.
- Coverage becomes complete only after commit validation.

### 35.8 Partial Retention Recovery

Simulate missing recording cursors 201–300, but retention contains only 250–300.

Expected:

- Available events are recovered.
- Remaining missing coverage is reported.
- No fabricated events are inserted.
- Recording does not claim the full interval is complete.

---

## 36. Historical Bootstrap Integration Tests

The most important end-to-end tests validate the complete historical-to-live data bridge.

### 36.1 Archive-to-Recording Join

Historical archive contains native executions:

    100–200

PostgreSQL contains:

    195–300

Expected reconciled output:

    100–300

Each native execution appears once under the validated identity contract.

### 36.2 Archive-to-Recording Conflict

Historical and database records contain the same native execution identity but conflicting canonical prices.

Expected:

    HistoricalRecordingConflict

The Bootstrap Coordinator must not silently choose one representation.

### 36.3 Archive-to-Recording Gap

Historical archive ends at execution 200.

PostgreSQL begins at execution 210.

Where native execution identity semantics establish a missing interval, the bootstrap result must report incomplete coverage.

Otherwise, the boundary remains unverified until additional evidence is available.

### 36.4 Recording-to-Live Join

PostgreSQL contains:

    Cursor 100–300

Live Retention contains:

    Cursor 250–400

Expected:

- Overlapping events reconciled.
- Each canonical event appears once.
- Live handoff begins at the correct boundary.
- No accepted event disappears between database read and live subscription.

### 36.5 Recording Lag

Simulate:

    Last Database Commit:
        Cursor 1000

    Current Live:
        Cursor 1500

Live Retention contains:

    Cursor 1001–1500

Expected:

- Bootstrap reads PostgreSQL through cursor 1000.
- Bootstrap reads the retained continuation.
- It transitions to current live processing.
- It does not require PostgreSQL to catch up first.

### 36.6 Historical Archive Published

Begin with:

    Historical Archives:
        October 18

    PostgreSQL:
        October 19

Then introduce a validated October 19 archive.

Expected:

- Historical coverage is reevaluated.
- Corresponding PostgreSQL data becomes potentially replaceable.
- No deletion occurs before retention conditions are satisfied.

### 36.7 Delayed Archive Publication

Simulate the exchange failing to publish the expected daily archive.

Expected:

- PostgreSQL retains the required recent interval.
- ArchiveVerified cleanup does not delete it.
- Database capacity pressure is reported when necessary.

### 36.8 Corrected Historical Archive

Simulate an exchange replacing an earlier historical archive.

Verify that archive-replacement eligibility is reevaluated.

Previously validated coverage must not remain trusted when its supporting evidence becomes invalid.

### 36.9 L2 Historical Handoff

Construct:

    Historical L2 Snapshot

    Historical L2 Deltas

    PostgreSQL L2 Events

    Live L2 Events

Verify valid generation and sequence boundaries.

If historical continuity cannot be proven, the bootstrap must start a new reconstruction segment rather than pretending the full historical book path is continuous.

---

## 37. Property-Based Testing

Property-based tests should generate arbitrary valid recording operations and failures.

### 37.1 Generated Operations

    AdmitEvent

    SealBatch

    BeginTransaction

    CommitTransaction

    FailTransaction

    LoseCommitAcknowledgement

    DisconnectDatabase

    ReconnectDatabase

    RetryBatch

    CrashRuntime

    RestartRuntime

    RecoverFromRetention

    AdvanceRecordingCoverage

    PublishHistoricalArchive

    ValidateArchive

    ExpireRecordedPartition

    DeleteRecordedPartition

### 37.2 Identity Invariants

    One canonical publication identity
    resolves to one authoritative
    recorded event.

    Equivalent retries do not
    create logical duplicates.

    Conflicting payloads are
    reported as integrity failures.

### 37.3 Durability Invariants

    A database commit must precede
    successful durability confirmation.

    Unknown commit outcomes
    require reconciliation.

    Uncommitted volatile batches
    are not assumed crash-durable.

### 37.4 Coverage Invariants

    Committed coverage must correspond
    to committed market-data rows.

    Out-of-order batch completion
    cannot advance coverage through
    unresolved intervals.

    Recording completeness does not
    imply source acquisition completeness.

### 37.5 Bootstrap Invariants

    Historical and recorded duplicates
    are reconciled by valid identities.

    Bootstrap protection precedes
    long-running historical reads.

    Database-to-live handoff cannot
    silently skip accepted events.

### 37.6 Cleanup Invariants

    Active bootstrap protection
    prevents unauthorized deletion.

    ArchiveVerified retention requires
    validated replacement evidence.

    Cleanup cannot leave coverage
    metadata advertising deleted data.

---

## 38. Performance Benchmarking

Recording performance must be measured using representative MarketForge workloads.

### 38.1 Benchmark Categories

Benchmark:

- Trade batch construction.
- Depth batch construction.
- PostgreSQL bulk insertion.
- Identity conflict validation.
- Transaction commit latency.
- Recording coverage updates.
- Concurrent writer throughput.
- Historical range queries.
- Bootstrap read throughput.
- Retention-based recovery.
- Partition cleanup.
- Restart reconciliation.

### 38.2 Workload Variation

Benchmarks should vary:

    StreamCount

    EventsPerSecond

    TradeToDepthRatio

    AverageDepthLevelsPerEvent

    BatchSize

    FlushInterval

    WriterConcurrency

    IndexCount

    PartitionDuration

    RecordingRetentionDuration

### 38.3 Sustained Recording Benchmark

Simulate sustained market-data ingestion while:

- Recording remains enabled.
- Bootstrap queries execute.
- Database indexes are maintained.
- Retention cleanup runs.

Measure:

    Ingestion Throughput

    Commit Latency

    Recording Lag

    Queue Memory

    Database CPU

    Database Disk Usage

    Transaction Log Volume

### 38.4 Outage Recovery Benchmark

Simulate a recording outage of a representative duration.

During the outage, preserve the accepted events through the configured retention mechanism.

After recovery, measure:

- Backlog size.
- Recovery write throughput.
- Time to restore contiguous recording coverage.
- Impact on current recording.
- Memory and disk pressure.
- Idempotency overhead.

### 38.5 Performance Targets

Concrete performance targets must be established from measurements.

The architecture must not assume that PostgreSQL can sustain arbitrary high-frequency depth-event recording without benchmarking.

Recording policies may differ by source stream and event type.

---

## 39. Implementation Acceptance Criteria

The PostgreSQL Recording subsystem is complete when the following requirements are satisfied.

Bootstrap-related requirements in this section define
the recording and query capabilities required by the
separate Bootstrap Engine.

MarketForge Live must implement and validate:

- Durable canonical event recording.
- Preservation of native trade identities.
- Preservation of logical L2 event boundaries.
- Recording commit and coverage tracking.
- Historical-compatible database reads.
- Recording availability and gap reporting.
- Recording retention and protected-reader guarantees.
- Safe coexistence with Live Retention.

Bootstrap integration tests must verify that recorded
events can be consumed correctly by historical/live
reconciliation logic.

The separate Bootstrap Engine owns the orchestration
that combines exchange archives, PostgreSQL recordings,
and live retained events.

It also owns final historical/live dataset assembly
and bootstrap completion decisions.

These responsibilities do not become part of the
PostgreSQL Recording Manager.

The Bootstrap requirements and tests below remain
valid and must not be removed.

### 39.1 Architecture

- PostgreSQL recording is independently configurable.
- Recording remains downstream of canonical acceptance.
- Recording does not replace Unix socket distribution.
- Recording does not replace temporary Live Retention.
- Source streams may remain active solely for recording.
- Database outages remain isolated where configured policy permits.

### 39.2 Historical Compatibility

- Trade data preserves the canonical schema.
- L2 data preserves event grouping and generation boundaries.
- Decimal precision is preserved.
- Nanosecond timestamps are preserved.
- Native identifiers remain lossless.
- Database reads reconstruct historical-compatible canonical events.
- Historical merge and conversion contracts remain reusable.

### 39.3 Persistence Correctness

- Recording queues are bounded.
- Batch sizes are bounded.
- Database transactions are atomic.
- Recording identities are stable.
- Idempotent retries are supported.
- Conflicting duplicate identities are detected.
- Ambiguous commits are reconciled.
- Committed coverage matches durable records.

### 39.4 Recovery

- Database connection failures are classified.
- Transient failures use bounded retry.
- Permanent failures trigger explicit escalation.
- Retention-based recovery is supported when coverage exists.
- Partial recovery remains visible.
- Process restart reconciles existing durable data.
- Recording health does not falsely imply historical completeness.

### 39.5 Bootstrap

- Archive/PostgreSQL overlap is reconciled.
- Native trade identities are preserved.
- Missing archive-to-recording intervals are detected where evidence permits.
- L2 synchronization boundaries remain valid.
- PostgreSQL-to-live handoff is continuous under the selected contract.
- Recording lag does not force unnecessary bootstrap delay when protected live coverage exists.
- Bootstrap completion requires authoritative evidence.

### 39.6 Retention

- Recording retention is configurable.
- Archive-aware retention is supported.
- Validated archive replacement is required under ArchiveVerified policy.
- Active bootstrap readers are protected.
- Partition cleanup is safe.
- Deletion updates availability metadata.
- Delayed archives cannot trigger silent recording loss.

### 39.7 Observability

- Recording throughput is measurable.
- Batch commit latency is measurable.
- Recording lag is visible.
- Database connectivity is exposed.
- Pending recording memory is exposed.
- Recording gaps are inspectable.
- Archive replacement eligibility is available.
- TUI and runtime API support recording management.
- Operational logs remain local.

### 39.8 Testing

- Partition uniqueness tests pass.
- Transaction atomicity tests pass.
- Idempotency tests pass.
- Database outage tests pass.
- Crash recovery tests pass.
- Retention backfill tests pass.
- Archive/recording/live bootstrap tests pass.
- Archive-aware cleanup tests pass.
- Property-based tests pass.
- Sustained-load benchmarks establish acceptable performance.

---

## 40. Final Architectural Contract

The PostgreSQL Recording subsystem durably stores selected canonical market-data events to bridge the period between the latest available historical exchange archives and continuous live acquisition.

The architecture is:

    Historical Exchange Archives
                |
                v
       Historical Data Subsystem
                |
                |
                +--------------------------------+
                                                 |
                                                 v
                                          Bootstrap Coordinator
                                                 ^
                                                 |
    PostgreSQL                                   |
         ^                                       |
         |                                       |
    Recording Writer                             |
         ^                                       |
         |                                       |
    Recording Manager                            |
         ^                                       |
         |                                       |
    Accepted Canonical Events                    |
         |                                       |
         +------------------+--------------------+
                            |
                            v
                    Live Retention
                            |
                            v
                    Continuous Live

The subsystem guarantees:

- Optional canonical market-data recording.
- Historical-compatible trade and depth schemas.
- Stable canonical publication identities.
- Exact numerical representation.
- Atomic database transactions.
- Idempotent recording retries.
- Explicit committed recording frontiers.
- Bounded recording queues.
- Database outage detection and recovery.
- Retention-assisted recording backfill.
- Recording coverage diagnostics.
- Historical bootstrap read access.
- Archive-aware recording retention.
- Safe removal of replaceable recorded data.

It does not guarantee:

- Complete acquisition of events missed during upstream exchange outages.
- Unlimited recording capacity.
- Crash durability for uncommitted volatile buffers.
- Automatic reconstruction of incomplete historical L2 sequences.
- Exactly-once downstream subscriber processing.
- Immediate availability of exchange historical archives.
- Correctness of historical archive coverage without validation.

The principal architectural invariant is:

    Historical Archives
           +
    PostgreSQL Recording
           +
    Live Retention
           |
           v
    One Reconciled Market-Data History

The correctness requirement is:

    No Historical Coverage
    May Be Claimed Complete
    Without Supporting Evidence.

The durability requirement is:

    No Event May Be Reported
    As Recorded Before Its
    Authoritative Database
    Commit Has Been Established.

The retention requirement is:

    Recorded Data Must Not Be
    Deleted Under ArchiveVerified
    Policy Until Validated Archive
    Coverage Can Replace It.

These guarantees preserve MarketForge's separation between source acquisition, canonical processing, local event distribution, temporary retention, and durable recent-history recording.

---

**End of Part 5 — Recording Observability, Capacity Planning, Fault Injection, Bootstrap Testing, and Final Acceptance Criteria.**

**End of 11 — PostgreSQL Recording.**
