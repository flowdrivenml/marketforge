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
/// Once assigned, cursor is **not** modified. It uniquely identifies
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
/// - Stream identity is owned by `cursor.stream_id`. `stream_id` field
///   is **removed** to eliminate the redundant copy.
/// - BookGeneration lives in `EventProvenance`; `LiveL2Group` has no
///   redundant copy.
/// - Exchange event timestamp for L2 events is owned by
///   `source.event_timestamp_ns` (non-nullable). `source_timestamp_ns`
///   field is **removed** to eliminate the redundant nullable copy.
///
/// # Timestamp field semantics
///
/// - **Trades**: exchange event time is at `trade.envelope.event_timestamp_ns`
///   (always present). system_timestamp is at `trade.envelope.system_timestamp_ns`
///   (optional — may be None when the historical schema had no system time).
/// - **L2 events**: exchange event time is at `group.source.event_timestamp_ns`
///   (always present via DepthSourceEventMetadata).
/// - **EventTiming**: `exchange_timestamp_ns` carries the live-layer
///   exchange time (same observation as above); `received_timestamp_ns`
///   carries local receipt time (distinct observation).
///
/// # Downstream immutability contract
///
/// **Downstream consumers must not modify the fields of an accepted
/// `LiveEvent`.** Although fields are `pub` (allowing inspection), the
/// accepted event must be treated as immutable. Mutation should be done
/// via construction of a new `LiveEvent` rather than in-place field
/// assignment. Cursor ordering, provenance consistency, and event
/// identity all depend on the envelope remaining unmodified after
/// `LiveEvent::new`.
///
/// # Downstream consumption
///
/// The envelope retains sufficient metadata for all initial downstream
/// subsystems without information loss:
///
/// - **Unix publication**: stream identity from `cursor`, payload, and
///   timing are sufficient to reconstruct the event and its position.
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
///
///
/// Validation errors when constructing an accepted Live event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveEventError {
    StreamIdentityMismatch,
    MissingBookGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveEvent {
    /// The MarketForge cursor assigned at acceptance time.
    ///
    /// The authoritative stream identity is `cursor.stream_id`.
    /// `stream_id` was removed to eliminate the redundant copy.
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
    /// Construct a validated Live event.
    ///
    /// The cursor is the authoritative stream identity.
    /// The event must preserve the canonical payload and its
    /// source provenance without introducing redundant fields.
    pub fn try_new(
        cursor: StreamCursor,
        payload: LivePayload,
        provenance: EventProvenance,
        timing: EventTiming,
    ) -> Result<Self, LiveEventError> {
        if cursor.stream_id != provenance.source_stream {
            return Err(LiveEventError::StreamIdentityMismatch);
        }

        if matches!(&payload, LivePayload::L2Group(_)) && provenance.book_generation.is_none() {
            return Err(LiveEventError::MissingBookGeneration);
        }

        Ok(Self {
            cursor,
            payload,
            provenance,
            timing,
        })
    }

    /// The exchange-reported event timestamp, if available.
    ///
    /// For trades, delegates to the canonical `Trade`'s envelope
    /// `event_timestamp_ns` (always present in the canonical model).
    /// For L2 events, uses `group.source.event_timestamp_ns`.
    pub fn event_timestamp_ns(&self) -> Option<i64> {
        match &self.payload {
            LivePayload::Trade(trade) => {
                // Trade envelope always provides event_timestamp_ns
                // (non-nullable in the canonical model).
                Some(trade.envelope.event_timestamp_ns)
            }
            LivePayload::L2Group(group) => Some(group.timestamp_ns()),
        }
    }

    /// Validate the event envelope's invariants.
    ///
    /// Returns `true` if:
    /// - The cursor's `stream_id` matches the provenance's `source_stream`.
    /// - For L2 events, the provenance contains a valid `book_generation`.
    pub fn validate(&self) -> bool {
        // Cursor stream identity must match provenance source.
        if self.cursor.stream_id != self.provenance.source_stream {
            return false;
        }

        // L2 events require a book generation.
        if matches!(&self.payload, LivePayload::L2Group(_)) {
            if self.provenance.book_generation.is_none() {
                return false;
            }
        }

        true
    }

    /// Assert that the event envelope's invariants hold.
    ///
    /// Panics if `validate()` would return `false`.
    /// This is intended for internal checks, not public API use.
    pub fn assert_valid(&self) {
        assert!(self.validate(), "LiveEvent invariant violation");
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
/// - `book_generation` is authoritative on `EventProvenance` — the
///   envelope's provenance. `LiveL2Group` carries no redundant copy.
///
/// - Exchange event timestamp: `source.event_timestamp_ns`.
///   `source_timestamp_ns` was **removed** because `DepthSourceEventMetadata`
///   already provides the exchange timestamp as a non-nullable field.
///   This eliminates duplicate storage without losing any information.
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

    /// Source-level metadata (including exchange event timestamp and
    /// optional sequence range).
    ///
    /// `source_timestamp_ns` was **removed** because
    /// `source.event_timestamp_ns` is always available (non-nullable
    /// in `DepthSourceEventMetadata`), eliminating a redundant nullable
    /// copy.
    pub source: DepthSourceEventMetadata,
}

impl LiveL2Group {
    /// Create a new grouped L2 event.
    ///
    /// `source` contains the exchange event timestamp; `source_timestamp_ns`
    /// is no longer a separate field.
    pub fn new(
        boundary: DepthEventBoundary,
        operation: DepthSourceOperation,
        levels: Vec<L2LevelUpdate>,
        source: DepthSourceEventMetadata,
    ) -> Self {
        Self {
            grouped_boundary: boundary,
            grouped_operation: operation,
            grouped_levels: levels,
            source,
        }
    }

    /// Exchange event timestamp for the source message.
    ///
    /// Always returns Some because `DepthSourceEventMetadata` carries
    /// a non-nullable `event_timestamp_ns` for depth events.
    pub fn timestamp_ns(&self) -> i64 {
        self.source.event_timestamp_ns
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
    /// For trade events, this is the same observation as
    /// `Trade::envelope.event_timestamp_ns`.
    /// For L2 events, this is the same observation as
    /// `LiveL2Group::source.event_timestamp_ns`.
    ///
    /// `None` when the source message did not carry an event timestamp.
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
