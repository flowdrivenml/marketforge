use crate::shared::domain::book::BookChange;

use crate::live::integrity::depth::continuity::DepthSequenceBaseline;
use crate::live::integrity::depth::controller::{
    AcceptedDepthOutput, DepthControllerOutcome, LiveDepthController,
};
use crate::live::integrity::depth::timestamps::DepthAcceptedTimestamp;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_PUBLICATION_FENCE_ID: AtomicU64 = AtomicU64::new(1);

fn next_publication_fence_id() -> Result<u64, String> {
    NEXT_PUBLICATION_FENCE_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| "publication fence instance ID space exhausted".to_string())
}
// ============================================================
// CONSUMER-FACING EVENTS
// ============================================================

#[derive(Debug, Clone, Copy)]
pub enum DepthPublicationEvent<'a> {
    Invalidated {
        stream_generation: u64,
        book_generation: u64,
    },

    Snapshot {
        stream_generation: u64,
        book_generation: u64,
        baseline: DepthSequenceBaseline,
        levels: &'a [BookChange],
        timestamp: DepthAcceptedTimestamp,
    },

    Delta {
        stream_generation: u64,
        book_generation: u64,
        baseline: DepthSequenceBaseline,
        changes: &'a [BookChange],
        timestamp: DepthAcceptedTimestamp,
    },
}

// ============================================================
// ACKNOWLEDGED STATE
// ============================================================

/// Last transition successfully handed to the downstream
/// publication component.
///
/// `publication_revision` advances on every acknowledged
/// nonempty publication transition, including invalidation.
///
/// It is independent of `book_generation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthPublicationState {
    pub stream_generation: u64,
    pub published_book_generation: Option<u64>,
    pub publication_revision: u64,
}

// ============================================================
// DELIVERY PHASE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthPublicationPhase {
    /// No preparation is outstanding.
    Ready,

    /// One preparation is outstanding.
    ///
    /// A second preparation is forbidden.
    Pending,

    /// A handoff was unsuccessful or has uncertain delivery.
    ///
    /// Only the explicit reset/reinitialization protocol
    /// may restore normal delta publication.
    ResetRequired,
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepthPublicationError {
    WrongStream,
    ObsoleteGeneration,
    InvalidAcceptedState,
    DeltaWithoutSnapshot,
    WrongBookGeneration,
    NonIncreasingBookGeneration,
    StalePreparation,

    PreparationOutstanding,
    NoOutstandingPreparation,
    WrongPreparation,

    ResetRequired,
    ResetInvalidationRequired,

    PreparationIdExhausted,
    PublicationRevisionExhausted,
    ForeignPreparation,
}

// ============================================================
// PREPARATION IDENTITY
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparationToken {
    id: u64,
}

/// Type of transition waiting for acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparationKind {
    Normal,
    ResetInvalidation,
    ResetSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BookIdentity {
    stream_generation: u64,
    book_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutstandingPreparation {
    token: PreparationToken,
    kind: PreparationKind,

    previous: DepthPublicationState,
    next: DepthPublicationState,

    /// Book identities potentially affected by delivery.
    ///
    /// This includes replacement snapshots, which may
    /// have been partially delivered before failure.
    touched_book: Option<BookIdentity>,
}

// ============================================================
// PREPARED TRANSITION
// ============================================================

/// Single-use publication transition.
///
/// Creating this value reserves the fence.
///
/// Dropping it does not release that reservation.
/// The caller must acknowledge success or explicitly
/// report uncertain/failed delivery.
///
/// If this value is lost, the runtime can invoke
/// `mark_outstanding_handoff_uncertain()`.
#[derive(Debug)]
pub struct PreparedDepthPublication<'a> {
    stream_id: String,

    token: PreparationToken,

    previous: DepthPublicationState,
    next: DepthPublicationState,

    events: Vec<DepthPublicationEvent<'a>>,
    fence_id: u64,
}

impl<'a> PreparedDepthPublication<'a> {
    pub fn events(&self) -> &[DepthPublicationEvent<'a>] {
        &self.events
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn next_state(&self) -> DepthPublicationState {
        self.next
    }
}

// ============================================================
// PUBLICATION FENCE
// ============================================================

/// Single-flight integrity-to-publication fence.
///
/// The authoritative source actor must serialize:
///
/// 1. Integrity-controller transitions.
/// 2. Publication preparation.
/// 3. Ordered downstream queue handoff.
/// 4. Acknowledgement or failure reporting.
///
/// Only one prepared transition can be outstanding.
///
/// A failed handoff makes delta publication impossible
/// until the consumer has been invalidated and a new
/// complete snapshot has been acknowledged.
///
/// This is not durable or end-to-end exactly-once delivery.
#[derive(Debug)]
pub struct DepthPublicationFence {
    stream_id: String,

    state: DepthPublicationState,
    phase: DepthPublicationPhase,

    next_preparation_id: u64,

    outstanding: Option<OutstandingPreparation>,

    /// A book possibly exposed by a failed handoff.
    ///
    /// This is distinct from the last acknowledged book.
    uncertain_book: Option<BookIdentity>,
    fence_id: u64,
}

impl DepthPublicationFence {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(stream_id: impl Into<String>, stream_generation: u64) -> Result<Self, String> {
        let stream_id = stream_id.into();

        if stream_id.trim().is_empty() {
            return Err("depth publication fence requires a stream ID".into());
        }
        let fence_id = next_publication_fence_id()?;
        Ok(Self {
            stream_id,

            state: DepthPublicationState {
                stream_generation,
                published_book_generation: None,
                publication_revision: 0,
            },

            phase: DepthPublicationPhase::Ready,

            next_preparation_id: 1,

            outstanding: None,
            uncertain_book: None,
            fence_id,
        })
    }
    /// Whether a failed publication has been fully invalidated
    /// at the downstream publication boundary.
    ///
    /// This does not mean the book is synchronized or ready.
    /// A new authoritative snapshot is still required.
    pub fn reset_invalidation_complete(&self) -> bool {
        self.phase == DepthPublicationPhase::ResetRequired
            && self.outstanding.is_none()
            && self.state.published_book_generation.is_none()
            && self.uncertain_book.is_none()
    }
    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn state(&self) -> DepthPublicationState {
        self.state
    }

    pub fn phase(&self) -> DepthPublicationPhase {
        self.phase
    }

    pub fn has_published_book(&self) -> bool {
        self.state.published_book_generation.is_some()
    }

    pub fn has_outstanding_preparation(&self) -> bool {
        self.outstanding.is_some()
    }

    // ========================================================
    // NORMAL PREPARATION
    // ========================================================

    pub fn prepare<'a>(
        &mut self,
        controller: &LiveDepthController,
        outcome: &'a DepthControllerOutcome,
    ) -> Result<PreparedDepthPublication<'a>, DepthPublicationError> {
        self.prepare_inner(controller, Some(outcome))
    }

    pub fn prepare_state_change(
        &mut self,
        controller: &LiveDepthController,
    ) -> Result<PreparedDepthPublication<'static>, DepthPublicationError> {
        self.prepare_inner(controller, None)
    }

    fn prepare_inner<'a>(
        &mut self,
        controller: &LiveDepthController,
        outcome: Option<&'a DepthControllerOutcome>,
    ) -> Result<PreparedDepthPublication<'a>, DepthPublicationError> {
        self.require_available()?;

        if controller.stream_id() != self.stream_id {
            return Err(DepthPublicationError::WrongStream);
        }

        let current_generation = controller.stream_generation();

        if current_generation < self.state.stream_generation {
            return Err(DepthPublicationError::ObsoleteGeneration);
        }

        let mut events = Vec::new();
        let mut next = self.state;

        // ----------------------------------------------------
        // STREAM GENERATION
        // ----------------------------------------------------

        if current_generation > self.state.stream_generation {
            if let Some(previous_book) = self.state.published_book_generation {
                events.push(DepthPublicationEvent::Invalidated {
                    stream_generation: self.state.stream_generation,
                    book_generation: previous_book,
                });
            }

            next.stream_generation = current_generation;
            next.published_book_generation = None;
        }

        // ----------------------------------------------------
        // ACCEPTED OUTPUT
        // ----------------------------------------------------

        if let Some(outcome) = outcome {
            if let Some(accepted) = outcome.accepted_output() {
                if !controller.is_synchronized() {
                    return Err(DepthPublicationError::InvalidAcceptedState);
                }

                match accepted {
                    AcceptedDepthOutput::Snapshot {
                        book_generation,
                        baseline,
                        levels,
                        timestamp,
                    } => {
                        if controller.book_generation() != book_generation
                            || controller.baseline() != Some(baseline)
                        {
                            return Err(DepthPublicationError::InvalidAcceptedState);
                        }

                        if let Some(previous_book) = next.published_book_generation {
                            if book_generation <= previous_book {
                                return Err(DepthPublicationError::NonIncreasingBookGeneration);
                            }

                            events.push(DepthPublicationEvent::Invalidated {
                                stream_generation: current_generation,
                                book_generation: previous_book,
                            });
                        }

                        events.push(DepthPublicationEvent::Snapshot {
                            stream_generation: current_generation,
                            book_generation,
                            baseline,
                            levels,
                            timestamp,
                        });

                        next.published_book_generation = Some(book_generation);
                    }

                    AcceptedDepthOutput::Delta {
                        book_generation,
                        baseline,
                        changes,
                        timestamp,
                    } => {
                        if controller.book_generation() != book_generation
                            || controller.baseline() != Some(baseline)
                        {
                            return Err(DepthPublicationError::InvalidAcceptedState);
                        }

                        let Some(previous_book) = next.published_book_generation else {
                            return Err(DepthPublicationError::DeltaWithoutSnapshot);
                        };

                        if previous_book != book_generation {
                            return Err(DepthPublicationError::WrongBookGeneration);
                        }

                        events.push(DepthPublicationEvent::Delta {
                            stream_generation: current_generation,
                            book_generation,
                            baseline,
                            changes,
                            timestamp,
                        });
                    }
                }

                return self.reserve(next, events, PreparationKind::Normal);
            }
        }

        // ----------------------------------------------------
        // INVALIDATION / EXPLICIT LIFECYCLE
        // ----------------------------------------------------

        if !controller.is_synchronized() {
            if let Some(previous_book) = next.published_book_generation.take() {
                events.push(DepthPublicationEvent::Invalidated {
                    stream_generation: current_generation,
                    book_generation: previous_book,
                });
            }
        }

        self.reserve(next, events, PreparationKind::Normal)
    }
    fn validate_preparation_owner(
        &self,
        prepared: &PreparedDepthPublication<'_>,
    ) -> Result<(), DepthPublicationError> {
        if prepared.fence_id != self.fence_id {
            return Err(DepthPublicationError::ForeignPreparation);
        }

        Ok(())
    }
    // ========================================================
    // RESET AFTER UNCERTAIN DELIVERY
    // ========================================================

    /// Prepare consumer invalidation after a failed handoff.
    ///
    /// This can revoke:
    /// - The last acknowledged book.
    /// - A replacement snapshot that may have been partially
    ///   delivered but never acknowledged.
    ///
    /// Delivery must be ordered and atomic at the receiving
    /// publication boundary.
    ///
    /// The fence remains ResetRequired after acknowledgement.
    pub fn prepare_reset_invalidation(
        &mut self,
    ) -> Result<PreparedDepthPublication<'static>, DepthPublicationError> {
        if self.outstanding.is_some() {
            return Err(DepthPublicationError::PreparationOutstanding);
        }

        if self.phase != DepthPublicationPhase::ResetRequired {
            return Err(DepthPublicationError::ResetRequired);
        }

        let mut events = Vec::new();

        if let Some(previous_book) = self.state.published_book_generation {
            events.push(DepthPublicationEvent::Invalidated {
                stream_generation: self.state.stream_generation,
                book_generation: previous_book,
            });
        }

        if let Some(uncertain) = self.uncertain_book {
            let already_covered =
                self.state
                    .published_book_generation
                    .is_some_and(|book_generation| {
                        self.state.stream_generation == uncertain.stream_generation
                            && book_generation == uncertain.book_generation
                    });

            if !already_covered {
                events.push(DepthPublicationEvent::Invalidated {
                    stream_generation: uncertain.stream_generation,
                    book_generation: uncertain.book_generation,
                });
            }
        }

        let mut next = self.state;
        next.published_book_generation = None;

        self.reserve(next, events, PreparationKind::ResetInvalidation)
    }

    /// Prepare a new complete snapshot after reset
    /// invalidation was acknowledged.
    ///
    /// The snapshot must be an accepted current controller
    /// outcome, not a fabricated delta or cached partial book.
    ///
    /// It is the caller's responsibility to establish a fresh
    /// authoritative snapshot after the failed handoff.
    pub fn prepare_reset_snapshot<'a>(
        &mut self,
        controller: &LiveDepthController,
        outcome: &'a DepthControllerOutcome,
    ) -> Result<PreparedDepthPublication<'a>, DepthPublicationError> {
        if self.outstanding.is_some() {
            return Err(DepthPublicationError::PreparationOutstanding);
        }

        if self.phase != DepthPublicationPhase::ResetRequired {
            return Err(DepthPublicationError::ResetRequired);
        }

        if self.state.published_book_generation.is_some() || self.uncertain_book.is_some() {
            return Err(DepthPublicationError::ResetInvalidationRequired);
        }

        if controller.stream_id() != self.stream_id {
            return Err(DepthPublicationError::WrongStream);
        }

        if controller.stream_generation() < self.state.stream_generation {
            return Err(DepthPublicationError::ObsoleteGeneration);
        }

        if !controller.is_synchronized() {
            return Err(DepthPublicationError::InvalidAcceptedState);
        }

        let Some(AcceptedDepthOutput::Snapshot {
            book_generation,
            baseline,
            levels,
            timestamp,
        }) = outcome.accepted_output()
        else {
            return Err(DepthPublicationError::DeltaWithoutSnapshot);
        };

        if controller.book_generation() != book_generation
            || controller.baseline() != Some(baseline)
        {
            return Err(DepthPublicationError::InvalidAcceptedState);
        }

        let generation = controller.stream_generation();

        let next = DepthPublicationState {
            stream_generation: generation,
            published_book_generation: Some(book_generation),
            publication_revision: self.state.publication_revision,
        };

        self.reserve(
            next,
            vec![DepthPublicationEvent::Snapshot {
                stream_generation: generation,
                book_generation,
                baseline,
                levels,
                timestamp,
            }],
            PreparationKind::ResetSnapshot,
        )
    }

    // ========================================================
    // ACKNOWLEDGEMENT
    // ========================================================

    pub fn acknowledge(
        &mut self,
        prepared: PreparedDepthPublication<'_>,
    ) -> Result<(), DepthPublicationError> {
        self.validate_preparation_owner(&prepared)?;
        let pending = self.verify_prepared(&prepared)?;

        let next_revision = self
            .state
            .publication_revision
            .checked_add(u64::from(!prepared.is_empty()))
            .ok_or(DepthPublicationError::PublicationRevisionExhausted)?;

        let mut next = pending.next;
        next.publication_revision = next_revision;

        self.state = next;
        self.outstanding = None;

        match pending.kind {
            PreparationKind::Normal | PreparationKind::ResetSnapshot => {
                self.phase = DepthPublicationPhase::Ready;
            }

            PreparationKind::ResetInvalidation => {
                // Consumer invalidation has been handed off.
                // A complete replacement snapshot is still
                // required before deltas may resume.
                self.phase = DepthPublicationPhase::ResetRequired;

                self.uncertain_book = None;
            }
        }

        Ok(())
    }

    // ========================================================
    // FAILED / UNCERTAIN HANDOFF
    // ========================================================

    /// Explicitly report that a prepared handoff failed or
    /// its delivery outcome cannot be established.
    ///
    /// No subsequent delta may be published until reset.
    pub fn handoff_failed(
        &mut self,
        prepared: PreparedDepthPublication<'_>,
    ) -> Result<(), DepthPublicationError> {
        self.validate_preparation_owner(&prepared)?;
        self.verify_prepared(&prepared)?;

        self.mark_outstanding_handoff_uncertain()
    }

    /// Fail closed if the PreparedDepthPublication was
    /// dropped or otherwise lost before acknowledgement.
    ///
    /// This operation is idempotent once ResetRequired
    /// has been established.
    pub fn mark_outstanding_handoff_uncertain(&mut self) -> Result<(), DepthPublicationError> {
        let Some(pending) = self.outstanding.take() else {
            if self.phase == DepthPublicationPhase::ResetRequired {
                return Ok(());
            }

            return Err(DepthPublicationError::NoOutstandingPreparation);
        };

        if let Some(book) = pending.touched_book {
            self.uncertain_book = Some(book);
        }

        self.phase = DepthPublicationPhase::ResetRequired;

        Ok(())
    }

    // ========================================================
    // INTERNAL RESERVATION
    // ========================================================

    fn require_available(&self) -> Result<(), DepthPublicationError> {
        if self.outstanding.is_some() {
            return Err(DepthPublicationError::PreparationOutstanding);
        }

        if self.phase == DepthPublicationPhase::ResetRequired {
            return Err(DepthPublicationError::ResetRequired);
        }

        Ok(())
    }

    fn reserve<'a>(
        &mut self,
        next: DepthPublicationState,
        events: Vec<DepthPublicationEvent<'a>>,
        kind: PreparationKind,
    ) -> Result<PreparedDepthPublication<'a>, DepthPublicationError> {
        if self.outstanding.is_some() {
            return Err(DepthPublicationError::PreparationOutstanding);
        }

        // Reserve both IDs before mutating fence state.
        let next_id = self
            .next_preparation_id
            .checked_add(1)
            .ok_or(DepthPublicationError::PreparationIdExhausted)?;

        if !events.is_empty() {
            self.state
                .publication_revision
                .checked_add(1)
                .ok_or(DepthPublicationError::PublicationRevisionExhausted)?;
        }

        let token = PreparationToken {
            id: self.next_preparation_id,
        };

        let touched_book = events.iter().rev().find_map(|event| match event {
            DepthPublicationEvent::Snapshot {
                stream_generation,
                book_generation,
                ..
            }
            | DepthPublicationEvent::Delta {
                stream_generation,
                book_generation,
                ..
            }
            | DepthPublicationEvent::Invalidated {
                stream_generation,
                book_generation,
            } => Some(BookIdentity {
                stream_generation: *stream_generation,
                book_generation: *book_generation,
            }),
        });

        let previous = self.state;

        self.outstanding = Some(OutstandingPreparation {
            token,
            kind,
            previous,
            next,
            touched_book,
        });

        self.next_preparation_id = next_id;
        self.phase = DepthPublicationPhase::Pending;

        Ok(PreparedDepthPublication {
            stream_id: self.stream_id.clone(),
            token,
            previous,
            next,
            events,
            fence_id: self.fence_id,
        })
    }

    fn verify_prepared(
        &self,
        prepared: &PreparedDepthPublication<'_>,
    ) -> Result<OutstandingPreparation, DepthPublicationError> {
        if prepared.stream_id != self.stream_id {
            return Err(DepthPublicationError::WrongStream);
        }

        let Some(pending) = self.outstanding else {
            return Err(DepthPublicationError::NoOutstandingPreparation);
        };

        if pending.token != prepared.token {
            return Err(DepthPublicationError::WrongPreparation);
        }

        if self.state != pending.previous
            || pending.previous != prepared.previous
            || pending.next != prepared.next
        {
            return Err(DepthPublicationError::StalePreparation);
        }

        Ok(pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use rust_decimal::Decimal;

    use crate::{
        live::{
            config::format::DepthFormatConfig,
            infrastructure::normalization::{
                DepthMessageKind, NormalizationSource, NormalizedDepth,
            },
            integrity::DepthContinuityCompiler,
        },
        shared::domain::canonical::{
            BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata,
        },
    };

    const STREAM: &str = "stream-42";

    fn controller() -> LiveDepthController {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth/bybit_spot.toml");

        let config = DepthFormatConfig::load(path).unwrap();

        let plan = DepthContinuityCompiler::compile(&config).unwrap();

        LiveDepthController::new(STREAM, plan, 10).unwrap()
    }

    fn fence() -> DepthPublicationFence {
        DepthPublicationFence::new(STREAM, 10).unwrap()
    }

    fn level(quantity: i64) -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Bybit,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: STREAM.into(),
            },

            side: BookSide::Bid,

            price: Decimal::from(80_000),

            quantity_base: Some(Decimal::from(quantity)),

            quantity_quote: None,
            quantity_contracts: None,

            order_count: None,
        }
    }

    fn depth(kind: DepthMessageKind, update: u64, cross: u64, quantity: i64) -> NormalizedDepth {
        NormalizedDepth::new(
            STREAM,
            NormalizationSource::WebSocket,
            kind,
            SequenceMetadata {
                sequence_first: None,
                sequence_last: Some(update),
                sequence_previous: None,
                cross_sequence: Some(cross),
            },
            vec![level(quantity)],
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap()
    }

    fn snapshot(update: u64, cross: u64) -> NormalizedDepth {
        depth(DepthMessageKind::Snapshot, update, cross, 3)
    }

    fn delta(update: u64, cross: u64) -> NormalizedDepth {
        depth(DepthMessageKind::Delta, update, cross, 5)
    }

    // ========================================================
    // INITIAL SNAPSHOT
    // ========================================================

    #[test]
    fn first_snapshot_establishes_publication_generation() {
        let mut controller = controller();
        let mut fence = fence();

        let outcome = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &outcome).unwrap();

        assert_eq!(prepared.events().len(), 1);

        assert!(matches!(
            prepared.events()[0],
            DepthPublicationEvent::Snapshot {
                stream_generation: 10,
                book_generation: 1,
                ..
            }
        ));

        // Preparation alone does not advance the fence.
        assert!(!fence.has_published_book());

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().published_book_generation, Some(1),);
    }

    // ========================================================
    // DELTA REQUIRES SNAPSHOT
    // ========================================================

    #[test]
    fn delta_cannot_publish_before_snapshot_acknowledgement() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        // No acknowledgement of the initial snapshot.
        let _prepared = fence.prepare(&controller, &initial).unwrap();

        let update = controller.process(10, &delta(101, 201));

        assert!(matches!(
            fence.prepare(&controller, &update),
            Err(DepthPublicationError::PreparationOutstanding)
        ));
    }

    // ========================================================
    // ACKNOWLEDGED DELTA
    // ========================================================

    #[test]
    fn acknowledged_snapshot_allows_matching_delta() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        let update = controller.process(10, &delta(101, 201));

        let prepared = fence.prepare(&controller, &update).unwrap();

        assert_eq!(prepared.events().len(), 1);

        assert!(matches!(
            prepared.events()[0],
            DepthPublicationEvent::Delta {
                stream_generation: 10,
                book_generation: 1,
                ..
            }
        ));

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().published_book_generation, Some(1),);
    }

    // ========================================================
    // REPLACEMENT SNAPSHOT
    // ========================================================

    #[test]
    fn replacement_snapshot_invalidates_previous_book_first() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        let replacement = controller.process(10, &snapshot(200, 300));

        let prepared = fence.prepare(&controller, &replacement).unwrap();

        assert_eq!(prepared.events().len(), 2);

        assert!(matches!(
            prepared.events()[0],
            DepthPublicationEvent::Invalidated {
                stream_generation: 10,
                book_generation: 1,
            }
        ));

        assert!(matches!(
            prepared.events()[1],
            DepthPublicationEvent::Snapshot {
                stream_generation: 10,
                book_generation: 2,
                ..
            }
        ));

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().published_book_generation, Some(2),);
    }

    // ========================================================
    // SEQUENCE FAILURE
    // ========================================================

    #[test]
    fn sequence_failure_invalidates_published_book() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        // Cross-sequence regression invalidates the book.
        let failure = controller.process(10, &delta(101, 199));

        assert!(matches!(
            failure,
            DepthControllerOutcome::RecoveryRequired { .. }
        ));

        let prepared = fence.prepare(&controller, &failure).unwrap();

        assert_eq!(prepared.events().len(), 1);

        assert!(matches!(
            prepared.events()[0],
            DepthPublicationEvent::Invalidated {
                stream_generation: 10,
                book_generation: 1,
            }
        ));

        fence.acknowledge(prepared).unwrap();

        assert!(!fence.has_published_book());
    }

    // ========================================================
    // EXPLICIT INVALIDATION
    // ========================================================

    #[test]
    fn explicit_invalidation_produces_lifecycle_event() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        controller.invalidate();

        let prepared = fence.prepare_state_change(&controller).unwrap();

        assert!(matches!(
            prepared.events()[0],
            DepthPublicationEvent::Invalidated {
                book_generation: 1,
                ..
            }
        ));

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().published_book_generation, None,);
    }

    // ========================================================
    // GENERATION TRANSITION
    // ========================================================

    #[test]
    fn new_stream_generation_invalidates_old_book() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        controller.begin_generation(11).unwrap();

        let prepared = fence.prepare_state_change(&controller).unwrap();

        assert_eq!(prepared.events().len(), 1);

        assert!(matches!(
            prepared.events()[0],
            DepthPublicationEvent::Invalidated {
                stream_generation: 10,
                book_generation: 1,
            }
        ));

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().stream_generation, 11,);

        assert!(!fence.has_published_book());
    }

    // ========================================================
    // DELIVERY FAILURE
    // ========================================================

    #[test]
    fn failed_handoff_does_not_advance_publication_state() {
        let mut controller = controller();
        let mut fence = fence();
        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        // Simulate a failed publication attempt by dropping
        // the prepared transition without acknowledgement.
        drop(prepared);

        assert_eq!(fence.state().published_book_generation, None,);
    }

    // ========================================================
    // STALE PREPARATION
    // ========================================================

    #[test]
    fn second_preparation_is_rejected_until_first_is_resolved() {
        let mut controller = controller();
        let mut fence = fence();

        let outcome = controller.process(10, &snapshot(100, 200));

        let first = fence.prepare(&controller, &outcome).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::Pending,);

        assert!(matches!(
            fence.prepare(&controller, &outcome),
            Err(DepthPublicationError::PreparationOutstanding)
        ));

        fence.acknowledge(first).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::Ready,);

        assert_eq!(fence.state().published_book_generation, Some(1),);
    }

    // ========================================================
    // WRONG STREAM
    // ========================================================

    #[test]
    fn wrong_stream_cannot_cross_publication_boundary() {
        let controller = controller();

        let mut fence = DepthPublicationFence::new("another-stream", 10).unwrap();

        assert!(matches!(
            fence.prepare_state_change(&controller),
            Err(DepthPublicationError::WrongStream)
        ));
    }

    // ========================================================
    // NO DUPLICATE INVALIDATION
    // ========================================================

    #[test]
    fn invalidation_is_not_repeated_after_acknowledgement() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        controller.invalidate();

        let first = fence.prepare_state_change(&controller).unwrap();

        assert_eq!(first.events().len(), 1);

        fence.acknowledge(first).unwrap();

        let second = fence.prepare_state_change(&controller).unwrap();

        assert!(second.is_empty());

        fence.acknowledge(second).unwrap();
    }
    // ============================================================
    // N5N.7.7a — TRANSACTIONAL PUBLICATION FENCE
    // ============================================================

    #[test]
    fn successive_deltas_have_distinct_acknowledged_revisions() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().publication_revision, 1,);

        let first = controller.process(10, &delta(101, 201));

        let prepared = fence.prepare(&controller, &first).unwrap();

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().publication_revision, 2,);

        assert_eq!(fence.state().published_book_generation, Some(1),);

        let second = controller.process(10, &delta(102, 202));

        let prepared = fence.prepare(&controller, &second).unwrap();

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().publication_revision, 3,);

        // Deltas advanced publication revision without
        // changing the authoritative book generation.
        assert_eq!(fence.state().published_book_generation, Some(1),);
    }

    #[test]
    fn later_delta_cannot_prepare_while_earlier_is_pending() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        let first = controller.process(10, &delta(101, 201));

        let pending = fence.prepare(&controller, &first).unwrap();

        let second = controller.process(10, &delta(102, 202));

        assert!(matches!(
            fence.prepare(&controller, &second),
            Err(DepthPublicationError::PreparationOutstanding)
        ));

        fence.acknowledge(pending).unwrap();

        let prepared = fence.prepare(&controller, &second).unwrap();

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().publication_revision, 3,);
    }

    #[test]
    fn dropped_preparation_blocks_future_publication() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        drop(prepared);

        // Dropping a preparation cannot silently undo
        // the reservation.
        assert!(fence.has_outstanding_preparation());

        let later = controller.process(10, &delta(101, 201));

        assert!(matches!(
            fence.prepare(&controller, &later),
            Err(DepthPublicationError::PreparationOutstanding)
        ));

        // Runtime discovers that the prepared transition
        // was lost or delivery status is unknown.
        fence.mark_outstanding_handoff_uncertain().unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);

        assert!(!fence.has_outstanding_preparation());

        assert!(matches!(
            fence.prepare(&controller, &later),
            Err(DepthPublicationError::ResetRequired)
        ));
    }

    #[test]
    fn failed_delta_requires_explicit_reset() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        let failed = controller.process(10, &delta(101, 201));

        let prepared = fence.prepare(&controller, &failed).unwrap();

        fence.handoff_failed(prepared).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);

        assert_eq!(fence.state().published_book_generation, Some(1),);

        let later = controller.process(10, &delta(102, 202));

        assert!(matches!(
            fence.prepare(&controller, &later),
            Err(DepthPublicationError::ResetRequired)
        ));

        let invalidation = fence.prepare_reset_invalidation().unwrap();

        assert_eq!(invalidation.events().len(), 1);

        assert!(matches!(
            invalidation.events()[0],
            DepthPublicationEvent::Invalidated {
                stream_generation: 10,
                book_generation: 1,
            }
        ));

        fence.acknowledge(invalidation).unwrap();

        assert_eq!(fence.state().published_book_generation, None,);

        // The consumer has been invalidated, but ordinary
        // delta publication still cannot resume.
        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);

        assert!(matches!(
            fence.prepare(&controller, &later),
            Err(DepthPublicationError::ResetRequired)
        ));
    }

    #[test]
    fn failed_replacement_revokes_old_and_uncertain_new_book() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        // Publication of the replacement includes:
        // invalidate book 1, then snapshot book 2.
        let replacement = controller.process(10, &snapshot(200, 300));

        let prepared = fence.prepare(&controller, &replacement).unwrap();

        assert_eq!(prepared.events().len(), 2);

        // We cannot know whether the receiving component
        // accepted zero, one, or both events.
        fence.handoff_failed(prepared).unwrap();

        let reset = fence.prepare_reset_invalidation().unwrap();

        assert_eq!(reset.events().len(), 2);

        assert!(matches!(
            reset.events()[0],
            DepthPublicationEvent::Invalidated {
                stream_generation: 10,
                book_generation: 1,
            }
        ));

        assert!(matches!(
            reset.events()[1],
            DepthPublicationEvent::Invalidated {
                stream_generation: 10,
                book_generation: 2,
            }
        ));

        fence.acknowledge(reset).unwrap();

        assert!(!fence.has_published_book());

        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);
    }

    #[test]
    fn full_snapshot_restores_publication_after_reset() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.handoff_failed(prepared).unwrap();

        // The initial snapshot may have been partially
        // delivered, despite not being acknowledged.
        let invalidation = fence.prepare_reset_invalidation().unwrap();

        assert_eq!(invalidation.events().len(), 1);

        assert!(matches!(
            invalidation.events()[0],
            DepthPublicationEvent::Invalidated {
                stream_generation: 10,
                book_generation: 1,
            }
        ));

        fence.acknowledge(invalidation).unwrap();

        // Delta-only recovery is forbidden.
        let delta_outcome = controller.process(10, &delta(101, 201));

        assert!(matches!(
            fence.prepare_reset_snapshot(&controller, &delta_outcome,),
            Err(DepthPublicationError::DeltaWithoutSnapshot)
        ));

        // The source establishes a fresh full snapshot.
        let replacement = controller.process(10, &snapshot(200, 300));

        let prepared = fence
            .prepare_reset_snapshot(&controller, &replacement)
            .unwrap();

        assert_eq!(prepared.events().len(), 1);

        assert!(matches!(
            prepared.events()[0],
            DepthPublicationEvent::Snapshot {
                stream_generation: 10,
                book_generation: 2,
                ..
            }
        ));

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::Ready,);

        assert_eq!(fence.state().published_book_generation, Some(2),);

        let next = controller.process(10, &delta(201, 301));

        let prepared = fence.prepare(&controller, &next).unwrap();

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().publication_revision, 3,);
    }

    #[test]
    fn reset_invalidation_failure_remains_fail_closed() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.handoff_failed(prepared).unwrap();

        let reset = fence.prepare_reset_invalidation().unwrap();

        // Even invalidation delivery may fail.
        fence.handoff_failed(reset).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);

        assert!(matches!(
            fence.prepare_reset_snapshot(&controller, &initial,),
            Err(DepthPublicationError::ResetInvalidationRequired)
        ));

        // Another invalidation attempt is allowed.
        let retry = fence.prepare_reset_invalidation().unwrap();

        assert!(!retry.events().is_empty());

        fence.acknowledge(retry).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);
    }

    #[test]
    fn invalidation_acknowledgement_advances_revision() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().publication_revision, 1,);

        controller.invalidate();

        let prepared = fence.prepare_state_change(&controller).unwrap();

        fence.acknowledge(prepared).unwrap();

        assert_eq!(fence.state().publication_revision, 2,);

        assert_eq!(fence.state().published_book_generation, None,);
    }

    #[test]
    fn pending_preparation_blocks_generation_transition_delivery() {
        let mut controller = controller();
        let mut fence = fence();

        let initial = controller.process(10, &snapshot(100, 200));

        let prepared = fence.prepare(&controller, &initial).unwrap();

        controller.begin_generation(11).unwrap();

        assert!(matches!(
            fence.prepare_state_change(&controller),
            Err(DepthPublicationError::PreparationOutstanding)
        ));

        // The pending old-generation handoff cannot be
        // treated as safely resolved merely because the
        // integrity controller advanced its generation.
        fence.handoff_failed(prepared).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);

        let invalidation = fence.prepare_reset_invalidation().unwrap();

        fence.acknowledge(invalidation).unwrap();

        assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);
    }
}
