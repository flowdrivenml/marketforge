//! Live-specific event envelope types consumed by all MarketForge Live
//! downstream subsystems (publication, PostgreSQL recording, Parquet
//! retention, replay, derived-stream processing).
//!
//! Authoritative reference: 02 — Domain Models and Contracts.md,
//! Sections 4–6 and 9; Document 06 normalization and integrity
//! requirements; Document 06.1 L2 event grouping semantics.
//!
//! Live envelopes wrap the canonical historical types without modifying
//! them. The grouped L2 payload preserves the depth event boundary,
//! source operation, and source sequence range; BookGeneration is
//! placed on the Live wrapper than the shared historical
//! `DepthProcessingOutcome`.

use serde::{Deserialize, Serialize};

use crate::canonical::{L2LevelUpdate, Trade};
use crate::formats::depth::{DepthEventBoundary, DepthSourceEventMetadata, DepthSourceOperation};

use super::cursor::*;
use super::identity::StreamId;

/// Immutable representation of one accepted live event.
///
/// Every accepted canonical event — trade or logical L2 group — is
/// wrapped in a `LiveEvent` that carries its StreamCursor assignment,
/// source provenance, and timing metadata.
///
/// # Immutable cursor contract
///
/// Once assigned, the cursor is **not** modified. It uniquely identifies
/// this event within its stream and log epoch namespace. Downstream
/// systems (Unix publication, PostgreSQL recording, Parquet retention,
/// replay, derived-stream processing) rely on the cursor for position
/// tracking and ordering.
///
/// # Ownership invariants (post-correction)
///
/// - **`LiveEvent` is the sole owner** of `cursor`, `provenance`, and
///   `timing`. No inner payload type carries independent copies.
/// - `LivePayload` wraps canonical types **directly** (not through a
///   wrapper struct that re-duplicates metadata).
/// - BookGeneration lives in `EventProvenance`; `LiveL2Group` has no
///   redundant copy.
///
/// # Field-level provenance
///
/// - `stream_id`: The logical stream on which the event was accepted.
/// - `cursor`: The MarketForge cursor assigned at acceptance time.
/// - `payload`: The underlying canonical trade or grouped L2 event.
/// - `provenance`: Source stream identity, connection/subscription
///   generations, native exchange identifiers.
/// - `timing`: Exchange event timestamp and local receipt timestamp.
///
/// # Downstream consumption
///
/// The envelope retains sufficient metadata for all initial downstream
/// subsystems without information loss:
///
/// - **Unix publication**: `stream_id`, cursor, payload, and timing
///   are sufficient to reconstruct the event and its position.
/// - **PostgreSQL recording**: Logical L2 event boundaries are
///   preserved through `LiveL2Group::grouped_boundary` and
///   `LiveL2Group::grouped_levels`. One `depth_events` row plus
///   multiple `depth_levels` rows per event.
/// - **Parquet retention**: All provenance and timing fields are
///   serializable and sufficient for column-store retention.
/// - **Derived-stream processing**: `provenance` identifies the
///   originating event; the cursor preserves ordering across
///   derivations.
///
/// # Memory efficiency
///
/// The envelope minimizes per-event allocation. Shared fields
/// (StreamId, StreamCursor) may be `Arc`-backed in future revisions.
/// Financial values inside `Trade` and `L2LevelUpdate` are reused
/// from the canonical payload without duplication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveEvent {
    /// The logical stream on which this event was accepted.
    pub stream_id: StreamId,

    /// The MarketForge cursor assigned at acceptance time.
    /// Immutable after construction.
    pub cursor: StreamCursor,

    /// The underlying canonical trade or grouped L2 event.
    pub payload: LivePayload,

    /// Source and generation provenance for this event.
    pub provenance: EventProvenance,

    /// Exchange event time and local receipt time.
    pub timing: EventTiming,
}

impl LiveEvent {
    /// All variants are immutable after cursor assignment.
    fn new(
        stream_id: StreamId,
        cursor: StreamCursor,
        payload: LivePayload,
        provenance: EventProvenance,
        timing: EventTiming,
    ) -> Self {
        Self {
            stream_id,
            cursor,
            payload,
            provenance,
            timing,
        }
    }

    /// The exchange-reported event timestamp, if available.
    ///
    /// For trades, delegates to the canonical `Trade`'s envelope.
    /// For L2 events, uses the group source timestamp.
    pub fn event_timestamp_ns(&self) -> Option<i64> {
        match &self.payload {
            LivePayload::Trade(trade) => trade.envelope.event_timestamp_ns.into(),
            LivePayload::L2Group(group) => group.source_timestamp_ns,
        }
    }
}

/// Canonical payload for one accepted live event.
///
/// One accepted trade event corresponds to one `LiveEvent` carrying
/// the canonical `Trade` directly (no wrapper that duplicates
/// cursor/provenance/timing). One accepted logical L2 event corresponds
/// to one `LiveEvent` carrying one `LiveL2Group` (which itself wraps
/// multiple ordered `L2LevelUpdate` values).
///
/// # Correction history
///
/// Previously `LivePayload::Trade` wrapped a `LiveTrade` that
/// redundantly carried `cursor`, `provenance`, and `timing`.
/// Those fields are owned exclusively by `LiveEvent`. The
/// `LiveTrade` wrapper was removed to eliminate duplication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LivePayload {
    /// One accepted trade event.
    ///
    /// The canonical `Trade` is carried directly — cursor,
    /// provenance, and timing are owned by `LiveEvent`, not
    /// duplicated on the payload.
    ///
    /// One accepted trade event receives one `StreamCursor`.
    Trade(Trade),

    /// One accepted logical L2 event (grouped depth update).
    ///
    /// One accepted logical L2 event receives **one** `StreamCursor`,
    /// regardless of the number of price levels contained within
    /// `grouped_levels`. Individual level changes within the event do
    /// **not** receive independent cursors.
    ///
    /// The complete `LiveL2Group` is the unit of canonical event
    /// acceptance, cursor assignment, event routing, publication,
    /// and retention.
    L2Group(LiveL2Group),
}

/// One accepted logical L2 event (grouped canonical depth update).
///
/// A `LiveL2Group` represents one atomic depth-processing boundary —
/// an initialization snapshot or an incremental delta — that has
/// passed canonical validation and integrity checks.
///
/// # L2 grouping contract
///
/// - `grouped_levels` contains the canonical ordered `L2LevelUpdate`
///   values produced by the depth processor. The ordering within the
///   group must be preserved by all downstream consumers.
///
/// - `grouped_boundary` identifies whether this event establishes
///   the authoritative book (initialization) or modifies it
///   (delta). This must not be interpreted as a native snapshot or
///   delta — it describes the canonical processing boundary.
///
/// - `grouped_operation` identifies the source event type
///   (snapshot, absolute update, relative update) before canonical
///   normalization.
///
/// - Native sequence identifiers are preserved through `source`
///   (`DepthSourceEventMetadata::sequence_start` / `sequence_end`).
///   There is **no** separate `EventRange` field to avoid duplication.
///
/// - `book_generation` is removed because it is already on
///   `EventProvenance` — the envelope's provenance is the
///   authoritative source for book state identity.
///
/// - `source_timestamp_ns` is the exchange event timestamp for the
///   source message from which these levels were produced. If the
///   source did not provide an event timestamp, this is `None`.
///
/// # Downstream preservation
///
/// PostgreSQL recording preserves the group boundary: one `depth_events`
/// row plus multiple `depth_levels` rows. Reconstructing the group
/// from storage must recover the original cursor, book generation,
/// level ordering, and boundary type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveL2Group {
    /// Canonical depth event boundary (Initialization or Changes).
    pub grouped_boundary: DepthEventBoundary,

    /// Source event operation (Snapshot, AbsoluteUpdate, RelativeUpdate).
    pub grouped_operation: DepthSourceOperation,

    /// Ordered canonical level updates.
    pub grouped_levels: Vec<L2LevelUpdate>,

    /// Exchange event timestamp for the source message.
    pub source_timestamp_ns: Option<i64>,

    /// Source-level metadata (including optional sequence range).
    pub source: DepthSourceEventMetadata,
}

impl LiveL2Group {
    /// Create a new grouped L2 event.
    ///
    /// `source` defaults to `DepthSourceEventMetadata::default()`
    /// when the caller does not supply exchange-level metadata.
    pub fn new(
        boundary: DepthEventBoundary,
        operation: DepthSourceOperation,
        levels: Vec<L2LevelUpdate>,
        source_timestamp_ns: Option<i64>,
        source: DepthSourceEventMetadata,
    ) -> Self {
        Self {
            grouped_boundary: boundary,
            grouped_operation: operation,
            grouped_levels: levels,
            source_timestamp_ns,
            source,
        }
    }
}

/// Opaque exchange identity for a sequence identifier.
///
/// Each exchange uses its own sequence format. The Live layer
/// preserves the original opaque identity; interpretation is a
/// document-10 concern.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NativeSequence(pub u64);

/// Opaque exchange identity for a trade or event identifier.
///
/// Preserved as opaque — not interpreted by the Live envelope.
/// Trade deduplication and identity conflict resolution work on
/// this value through the trade identity contract, not the envelope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NativeTradeId(pub String);

/// Source and generation provenance for an accepted event.
///
/// The provenance identifies the transport, acquisition, and source
/// context for this event. It is **not** a financial or structural
/// representation — it answers "where did this event come from?"
///
/// # Provenance invariants
///
/// - Native identifiers preserve exchange semantics.
/// - Exchange sequence identifiers are **not** interchangeable with
///   MarketForge cursor offsets.
/// - Connection generations identify transport provenance, not
///   historical completeness.
/// - Book generations apply only to L2 streams — for trade events
///   they remain `None`.
/// - Missing native identifiers remain explicitly absent (`None`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventProvenance {
    /// The source stream identity for this event.
    pub source_stream: StreamId,

    /// Physical connection attempt that delivered this event.
    pub connection_generation: ConnectionGeneration,

    /// Subscription activation lifecycle that admitted this event.
    pub subscription_generation: SubscriptionGeneration,

    /// Native exchange sequence identifier, if provided by the source.
    pub native_sequence: Option<NativeSequence>,

    /// Native trade/event identifier from the source, if known.
    pub native_trade_id: Option<NativeTradeId>,

    /// Authorized order-book synchronization iteration.
    /// Present only for L2 events.
    pub book_generation: Option<BookGeneration>,
}

/// Exchange and local-receipt timing for an accepted event.
///
/// Timing domains are explicit — they are not conflated with the
/// canonical event's own timestamps, and they do not carry processing
/// or publication timestamps.
///
/// Internal latency measurements use monotonic clocks and are captured
/// by instrumentation, not stored on accepted events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTiming {
    /// Wall-clock timestamp in nanoseconds, as reported by the exchange.
    ///
    /// `None` when the source message did not carry a event timestamp.
    pub exchange_timestamp_ns: Option<i64>,

    /// Wall-clock timestamp in nanoseconds, when MarketForge received
    /// the message on the local instance.
    pub received_timestamp_ns: i64,
}

impl EventTiming {
    /// Retrieve the exchange timestamp as `i64`.
    pub fn event_timestamp_ns(&self) -> Option<i64> {
        self.exchange_timestamp_ns
    }

    /// Retrieve the local receipt timestamp as i64.
    pub fn received_timestamp_ns(&self) -> i64 {
        self.received_timestamp_ns
    }
}
