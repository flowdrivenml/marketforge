use super::{
    CompiledDepthContinuityPlan, DepthContinuityDecision, DepthContinuityFamily,
    DepthContinuityValidator, DepthIgnoreReason, DepthSequenceBaseline, DepthViolationReason,
    to_book_changes,
};
use super::{DepthBookBudget, DepthBookLimits};
use super::{DepthBridgeRule, DepthSnapshotSource, DepthSyncToken, PreSnapshotDepthBuffer};
use super::{DepthResourceAccounting, DepthResourceLimits};
use crate::{
    book::{BookChange, BookStore},
    canonical::BookSide,
    live::infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
};
use std::time::Instant;

// ============================================================
// PROCESSING OUTCOMES
// ============================================================

/// Results of processing one normalized depth message.
/// Accepted means the complete book operation succeeded.
/// Ignored means no book mutation occurred.
/// RecoveryRequired means current book synchronization
/// cannot be trusted and the owning runtime must recover.
/// These results do not imply successful downstream
/// publication or persistence.
#[derive(Debug)]
pub enum DepthControllerOutcome {
    Accepted {
        changes: Vec<BookChange>,
        snapshot: bool,
        book_generation: u64,
        baseline: DepthSequenceBaseline,
    },

    Ignored {
        reason: DepthControllerIgnoreReason,
    },

    RecoveryRequired {
        reason: DepthControllerRecoveryReason,
    },
    /// An authoritative Bitget snapshot was validated and
    /// stored privately.
    /// The snapshot has NOT established synchronization.
    /// No book publication is authorized.
    SnapshotStaged {
        snapshot_sequence: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthControllerIgnoreReason {
    ObsoleteGeneration,
    StaleSequence,
    Keepalive,
    /// The delta was retained for future snapshot reconciliation.
    /// It has not been applied to the authoritative book.
    AwaitingSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepthControllerRecoveryReason {
    SnapshotRequired,
    InvalidSequence(DepthViolationReason),
    InvalidBook(String),
    InvalidSnapshot(String),
    WrongStream,
    InvalidSource,
    BookGenerationExhausted,
    SnapshotBridgeFailed,
}

/// Private, uncommitted Bitget snapshot.
/// A snapshot alone does not establish a synchronized
/// Bitget V3 book. The first delta must bridge its sequence.
/// No caller may access this candidate through book().
#[derive(Debug)]
struct PendingBitgetSnapshot {
    book: BookStore,
    snapshot_sequence: u64,

    /// Monotonic local clock at which the snapshot
    /// entered the pending-bridge state.
    staged_at: Instant,
}

// ============================================================
// LIVE DEPTH CONTROLLER
// ============================================================

/// Owns synchronized L2 state for one source stream.
/// This controller owns:
/// - One mutable BookStore.
/// - The accepted sequence baseline.
/// - The current stream generation.
/// - The current book generation.
/// It does not own:
/// - WebSocket connections.
/// - REST snapshot requests.
/// - Recovery scheduling.
/// - GapTracker history.
/// - Event publication.
/// - PostgreSQL persistence.
/// The surrounding runtime must serialize calls to this
/// controller for its source stream.

#[derive(Debug)]
pub struct LiveDepthController {
    stream_id: String,

    /// Authoritative subscribed order-book depth.
    ///
    /// This is source-runtime configuration, not a value
    /// inferred from the number of levels in a message.
    subscribed_depth: Option<u64>,

    plan: CompiledDepthContinuityPlan,

    stream_generation: u64,

    book_generation: u64,

    book: BookStore,

    baseline: Option<DepthSequenceBaseline>,
    pending_bitget_snapshot: Option<PendingBitgetSnapshot>,
    /// The REST synchronization attempt currently authorized
    /// to establish the authoritative book.
    active_rest_attempt: Option<DepthSyncToken>,
    /// Most recently started REST synchronization attempt.
    /// This survives ordinary invalidation, preventing an old
    /// attempt from being reactivated within the same stream
    /// generation.
    last_rest_attempt: Option<DepthSyncToken>,
    resources: DepthResourceLimits,
    book_limits: DepthBookLimits,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl LiveDepthController {
    pub(super) fn new(
        stream_id: impl Into<String>,
        plan: CompiledDepthContinuityPlan,
        stream_generation: u64,
    ) -> Result<Self, String> {
        let stream_id = stream_id.into();

        if stream_id.trim().is_empty() {
            return Err("depth controller: empty stream_id".into());
        }

        if !plan.requires_generation_fencing {
            return Err("depth controller requires generation fencing".into());
        }

        Ok(Self {
            stream_id,
            plan,
            stream_generation,
            book_generation: 0,
            book: BookStore::new(),
            baseline: None,
            pending_bitget_snapshot: None,
            active_rest_attempt: None,
            last_rest_attempt: None,
            subscribed_depth: None,
            resources: DepthResourceLimits::default(),
            book_limits: DepthBookLimits::default(),
        })
    }
    /// Expire a staged Bitget snapshot if its bridging
    /// deadline has elapsed.
    ///
    /// The source runtime should call this from its
    /// periodic integrity timer.
    ///
    /// Returns None when:
    /// - No Bitget snapshot is pending.
    /// - The pending snapshot has not expired.
    ///
    /// Returns RecoveryRequired only when an existing
    /// pending candidate actually expires.
    ///
    /// This method does not schedule a new snapshot.
    pub fn expire_pending_snapshot(&mut self) -> Option<DepthControllerOutcome> {
        let expired = self
            .pending_bitget_snapshot
            .as_ref()
            .is_some_and(|pending| {
                pending.staged_at.elapsed() >= self.resources.max_pending_snapshot_age
            });

        if !expired {
            return None;
        }

        self.invalidate();

        Some(DepthControllerOutcome::RecoveryRequired {
            reason: DepthControllerRecoveryReason::SnapshotBridgeFailed,
        })
    }
    /// Configure the authoritative book's retained-state
    /// budget before synchronization starts.
    ///
    /// Changing this policy while holding an authoritative
    /// book, a staged snapshot, or an active REST attempt
    /// is forbidden.
    ///
    /// A new budget may be installed after synchronization
    /// has been explicitly invalidated.
    pub(super) fn set_book_limits(&mut self, limits: DepthBookLimits) -> Result<(), String> {
        let limits = limits
            .validate()
            .map_err(|error| format!("invalid retained book limits: {error:?}"))?;

        if self.is_synchronized()
            || self.pending_bitget_snapshot.is_some()
            || self.active_rest_attempt.is_some()
        {
            return Err("cannot change retained book limits during \
             active synchronization"
                .into());
        }

        self.book_limits = limits;

        Ok(())
    }

    pub fn book_limits(&self) -> DepthBookLimits {
        self.book_limits
    }
    /// Configure depth admission limits before the controller
    /// begins accepting market data.
    ///
    /// Resource policy cannot be changed while synchronized,
    /// while holding a Bitget snapshot, or while a REST
    /// synchronization attempt is active.
    pub(super) fn set_resource_limits(
        &mut self,
        limits: DepthResourceLimits,
    ) -> Result<(), String> {
        let limits = limits
            .validate()
            .map_err(|error| format!("invalid depth resource limits: {error:?}"))?;

        if self.is_synchronized()
            || self.pending_bitget_snapshot.is_some()
            || self.active_rest_attempt.is_some()
        {
            return Err("cannot change depth resource limits during \
             active synchronization"
                .into());
        }

        self.resources = limits;

        Ok(())
    }

    pub fn resource_limits(&self) -> DepthResourceLimits {
        self.resources
    }
    // ============================================================
    // REST SYNCHRONIZATION ATTEMPT OWNERSHIP
    // ============================================================

    /// Authorize a new REST snapshot synchronization attempt.
    /// The caller must start its bounded WebSocket buffer before
    /// issuing the corresponding REST snapshot request.
    /// Every attempt must have an identifier greater than the
    /// previously started attempt within the same stream generation.
    /// Starting a new attempt invalidates the previous book and
    /// cancels any older synchronization attempt.
    /// This method does not issue HTTP requests, allocate a buffer,
    /// or schedule recovery.
    ///
    ///
    pub(super) fn begin_rest_attempt(&mut self, token: DepthSyncToken) -> Result<(), String> {
        if self.plan.snapshot_source != DepthSnapshotSource::Rest
            || !self.plan.requires_update_buffer
        {
            return Err("this depth profile does not support REST snapshot \
             synchronization"
                .into());
        }

        if token.stream_generation != self.stream_generation {
            return Err("REST attempt belongs to an obsolete or unknown \
             stream generation"
                .into());
        }

        if token.attempt == 0 {
            return Err("REST synchronization attempt ID must be positive".into());
        }

        if let Some(previous) = self.last_rest_attempt {
            if previous.stream_generation == token.stream_generation
                && token.attempt <= previous.attempt
            {
                return Err("REST synchronization attempt must advance \
                 monotonically"
                    .into());
            }
        }

        // Discard any previous synchronized state or pending
        // Bitget candidate before installing the new attempt.
        self.invalidate();

        self.active_rest_attempt = Some(token);
        self.last_rest_attempt = Some(token);

        Ok(())
    }

    /// Return the currently authorized REST synchronization token.
    /// None means the controller has no active REST attempt.
    pub fn active_rest_attempt(&self) -> Option<DepthSyncToken> {
        self.active_rest_attempt
    }
    /// Construct a depth controller with an authoritative
    /// subscription-depth binding.
    ///
    /// The source runtime should prefer this constructor
    /// whenever the exchange channel declares a fixed depth.
    ///
    /// A value of 1 represents the best bid/ask level,
    /// not unrestricted book coverage.
    pub(super) fn new_with_subscription_depth(
        stream_id: impl Into<String>,
        plan: CompiledDepthContinuityPlan,
        stream_generation: u64,
        subscribed_depth: u64,
    ) -> Result<Self, String> {
        if subscribed_depth == 0 {
            return Err("subscribed depth must be greater than zero".into());
        }

        let mut controller = Self::new(stream_id, plan, stream_generation)?;

        controller.subscribed_depth = Some(subscribed_depth);

        Ok(controller)
    }

    pub fn subscribed_depth(&self) -> Option<u64> {
        self.subscribed_depth
    }
    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn stream_generation(&self) -> u64 {
        self.stream_generation
    }

    pub fn book_generation(&self) -> u64 {
        self.book_generation
    }

    pub fn book(&self) -> &BookStore {
        &self.book
    }

    pub fn baseline(&self) -> Option<DepthSequenceBaseline> {
        self.baseline
    }

    pub fn is_synchronized(&self) -> bool {
        self.book.is_initialized() && self.baseline.is_some_and(|baseline| baseline.synchronized)
    }

    pub fn plan(&self) -> &CompiledDepthContinuityPlan {
        &self.plan
    }

    // ========================================================
    // STREAM GENERATION
    // ========================================================

    /// Install a newer stream generation.
    /// The caller is responsible for assigning authoritative,
    /// monotonically increasing generation identifiers.
    /// Any previously synchronized book becomes invalid.
    pub(super) fn begin_generation(&mut self, generation: u64) -> Result<(), String> {
        if generation <= self.stream_generation {
            return Err("new stream generation must be greater \
                 than the current generation"
                .into());
        }

        self.stream_generation = generation;

        self.invalidate();

        Ok(())
    }

    // ========================================================
    // INVALIDATION
    // ========================================================

    /// Invalidate current synchronization state.
    ///
    /// This clears the book rather than leaving a stale
    /// book accessible as if it were authoritative.
    pub(super) fn invalidate(&mut self) {
        self.baseline = None;
        self.pending_bitget_snapshot = None;

        // Invalidation cancels any pending REST synchronization.
        self.active_rest_attempt = None;

        self.book.clear();
    }

    // ========================================================
    // NORMALIZED DEPTH PROCESSING
    // ========================================================

    /// Process a complete normalized depth message.
    /// `generation` identifies the authoritative source
    /// runtime generation that produced the message.
    ///
    /// Obsolete generations cannot modify current state.
    ///
    /// This stage supports only WebSocket messages.
    ///
    /// REST snapshots require a separately verified
    /// snapshot-bridge procedure and are intentionally
    /// rejected here.
    pub(super) fn process(
        &mut self,
        generation: u64,
        message: &NormalizedDepth,
    ) -> DepthControllerOutcome {
        // ----------------------------------------------------
        // GENERATION FENCING
        // ----------------------------------------------------

        if generation != self.stream_generation {
            return DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration,
            };
        }

        // ----------------------------------------------------
        // STREAM IDENTITY
        // ----------------------------------------------------

        if message.stream_id() != self.stream_id {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::WrongStream,
            };
        }
        // ----------------------------------------------------
        // RESOURCE ADMISSION
        // ----------------------------------------------------

        if let Err(error) = DepthResourceAccounting::validate_message(message, self.resources) {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(format!(
                    "depth message exceeds resource limits: \
                     {error:?}"
                )),
            };
        }
        // ----------------------------------------------------
        // SOURCE
        // ----------------------------------------------------

        // Replay requires its own explicit lifecycle contract.
        //
        // REST snapshots cannot be safely joined to an
        // incremental WebSocket stream here.

        if message.source() != NormalizationSource::WebSocket {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSource,
            };
        }

        // ----------------------------------------------------
        // MESSAGE KIND
        // ----------------------------------------------------
        // ----------------------------------------------------
        // SUBSCRIPTION DEPTH VALIDATION
        // ----------------------------------------------------

        if let Some(subscribed_depth) = self.subscribed_depth {
            // If the normalized message supplies its own declared
            // depth, it must agree with the authoritative binding.
            //
            // Absence of per-message depth is acceptable because
            // the source runtime has already bound the subscription.
            if let Some(message_depth) = message.declared_depth() {
                if message_depth != subscribed_depth {
                    self.invalidate();

                    return DepthControllerOutcome::RecoveryRequired {
                        reason: DepthControllerRecoveryReason::InvalidSnapshot(format!(
                            "message depth {message_depth} \
                             does not match subscribed depth \
                             {subscribed_depth}"
                        )),
                    };
                }
            }
        }

        // ----------------------------------------------------
        // BYBIT DEPTH-1 SNAPSHOT-ONLY CONTRACT
        // ----------------------------------------------------

        if self.plan.family == DepthContinuityFamily::BybitV5
            && message.kind() == DepthMessageKind::Delta
        {
            // Prefer the authoritative subscription binding.
            //
            // Where the binding is unavailable, the normalized
            // message's declared depth can still identify a
            // forbidden depth-1 delta.
            let effective_depth = self.subscribed_depth.or(message.declared_depth());

            if effective_depth == Some(1) {
                self.invalidate();

                return DepthControllerOutcome::RecoveryRequired {
                    reason: DepthControllerRecoveryReason::InvalidSequence(
                        DepthViolationReason::UnsupportedMetadata,
                    ),
                };
            }

            // Fail closed when neither the runtime nor the message
            // establishes what Bybit depth this stream represents.
            //
            // Otherwise an unbound level-1 stream could reach the
            // ordinary delta evaluator.
            if effective_depth.is_none() {
                self.invalidate();

                return DepthControllerOutcome::RecoveryRequired {
                    reason: DepthControllerRecoveryReason::InvalidSnapshot(
                        "Bybit delta has no authoritative \
                     subscription-depth binding"
                            .into(),
                    ),
                };
            }
        }
        match message.kind() {
            DepthMessageKind::Snapshot => self.process_snapshot(message),

            DepthMessageKind::Delta => self.process_delta(message),
        }
    }

    // ========================================================
    // WEBSOCKET SNAPSHOT
    // ========================================================

    fn process_snapshot(&mut self, message: &NormalizedDepth) -> DepthControllerOutcome {
        // ----------------------------------------------------
        // SNAPSHOT CAPABILITY
        // ----------------------------------------------------

        if !self.plan.accepts_websocket_snapshots {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSource,
            };
        }
        // An authoritative WebSocket snapshot supersedes any
        // in-flight REST synchronization attempt.
        //
        // Do not allow a delayed REST response to overwrite the
        // book established by this WebSocket snapshot.
        self.active_rest_attempt = None;

        // Bitget V3 snapshots require a validated first-delta
        // bridge before the candidate book can be committed.
        if self.plan.family == DepthContinuityFamily::BitgetV3Previous {
            return self.stage_bitget_snapshot(message);
        }

        // ----------------------------------------------------
        // SEQUENCE BASELINE
        // ----------------------------------------------------

        let Some(last_sequence) = message.sequence().sequence_last else {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(
                    "snapshot has no sequence baseline".into(),
                ),
            };
        };

        // ----------------------------------------------------
        // NEXT BOOK GENERATION
        // ----------------------------------------------------

        let Some(next_generation) = self.book_generation.checked_add(1) else {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::BookGenerationExhausted,
            };
        };

        // ----------------------------------------------------
        // PREPARE SNAPSHOT LEVELS
        // ----------------------------------------------------
        // ----------------------------------------------------
        // RETAINED BOOK BUDGET
        // ----------------------------------------------------

        // A complete replacement snapshot establishes a new
        // authoritative book generation.
        //
        // Reject an oversized snapshot before constructing
        // temporary bid/ask maps or mutating BookStore.
        if let Err(error) = DepthBookBudget::validate_snapshot(message.levels(), self.book_limits) {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "WebSocket snapshot exceeds retained \
                     book budget: {error:?}"
                )),
            };
        }
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        for (side, price, level) in to_book_changes(message.levels()) {
            match side {
                BookSide::Bid => bids.push((price, level)),
                BookSide::Ask => asks.push((price, level)),
            }
        }

        // ----------------------------------------------------
        // ATOMIC BOOK REPLACEMENT
        // ----------------------------------------------------

        // BookStore validates the complete snapshot before
        // mutating its internal bid and ask maps.

        if let Err(error) = self.book.apply_snapshot(bids, asks) {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(error.to_string()),
            };
        }

        // The shared BookStore returns a diff from apply_snapshot().
        //
        // A newly synchronized book generation must instead
        // provide its complete retained state.
        let changes = full_book_changes(&self.book);

        // ----------------------------------------------------
        // COMMIT SYNCHRONIZATION STATE
        // ----------------------------------------------------

        let baseline = DepthSequenceBaseline {
            last_sequence,

            cross_sequence: message.sequence().cross_sequence,

            synchronized: true,
        };

        self.book_generation = next_generation;

        self.baseline = Some(baseline);

        DepthControllerOutcome::Accepted {
            changes,
            snapshot: true,
            book_generation: self.book_generation,
            baseline,
        }
    }
    // ========================================================
    // BITGET SNAPSHOT STAGING
    // ========================================================

    /// Build an isolated candidate book from a Bitget V3
    /// WebSocket snapshot.
    ///
    /// The candidate is not exposed through book(), and
    /// synchronization remains false until the first delta
    /// bridges the snapshot sequence.
    /// Repeated snapshots replace the previous candidate.
    fn stage_bitget_snapshot(&mut self, message: &NormalizedDepth) -> DepthControllerOutcome {
        // A new snapshot supersedes any previous candidate
        // and invalidates the previously committed book.
        self.invalidate();
        if let Err(error) = DepthBookBudget::validate_snapshot(message.levels(), self.book_limits) {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "Bitget snapshot exceeds retained \
                     book budget: {error:?}"
                )),
            };
        }
        if let Err(error) = DepthResourceAccounting::validate_pending_snapshot(
            message.level_count(),
            self.resources,
        ) {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "Bitget snapshot exceeds pending \
                     snapshot limits: {error:?}"
                )),
            };
        }
        let Some(snapshot_sequence) = message.sequence().sequence_last else {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(
                    "Bitget snapshot is missing seq".into(),
                ),
            };
        };

        // Native Bitget snapshots do not carry U/u ranges
        // or Bybit cross-sequence metadata.
        if message.sequence().sequence_first.is_some()
            || message.sequence().cross_sequence.is_some()
        {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(
                    "Bitget snapshot contains incompatible \
                 sequence metadata"
                        .into(),
                ),
            };
        }

        // Bitget may use pseq=0 in an initial snapshot.
        // Do not interpret that snapshot sentinel as a
        // continuity violation.
        //
        // Other snapshot pseq values remain metadata;
        // the snapshot's seq establishes the baseline.

        // Reserve the next book-generation value before
        // allocating or preparing the candidate.
        if self.book_generation.checked_add(1).is_none() {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::BookGenerationExhausted,
            };
        }

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        for (side, price, level) in to_book_changes(message.levels()) {
            match side {
                BookSide::Bid => bids.push((price, level)),
                BookSide::Ask => asks.push((price, level)),
            }
        }

        let mut candidate = BookStore::new();

        if let Err(error) = candidate.apply_snapshot(bids, asks) {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(error.to_string()),
            };
        }
        // Defense in depth: validate actual candidate size
        // before retaining the staged snapshot.
        if let Err(error) = DepthBookBudget::validate_book(&candidate, self.book_limits) {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "Bitget candidate exceeds retained \
                     book budget: {error:?}"
                )),
            };
        }
        let retained_levels = candidate.bids().len() + candidate.asks().len();

        if let Err(error) =
            DepthResourceAccounting::validate_pending_snapshot(retained_levels, self.resources)
        {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "Bitget candidate exceeds retained \
                     level limits: {error:?}"
                )),
            };
        }
        // Commit only the pending candidate.
        //
        // The authoritative book remains empty/uninitialized.
        // The accepted sequence baseline remains absent.
        self.pending_bitget_snapshot = Some(PendingBitgetSnapshot {
            book: candidate,
            snapshot_sequence,
            staged_at: Instant::now(),
        });

        DepthControllerOutcome::SnapshotStaged { snapshot_sequence }
    }
    // ========================================================
    // INCREMENTAL UPDATE
    // ========================================================
    fn process_delta(&mut self, message: &NormalizedDepth) -> DepthControllerOutcome {
        // The first Bitget delta is evaluated against the
        // uncommitted snapshot, not the ordinary sequence
        // baseline.
        // ----------------------------------------------------
        // PENDING BITGET SNAPSHOT EXPIRY
        // ----------------------------------------------------

        if self
            .pending_bitget_snapshot
            .as_ref()
            .is_some_and(|pending| {
                pending.staged_at.elapsed() >= self.resources.max_pending_snapshot_age
            })
        {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotBridgeFailed,
            };
        }
        if self.plan.family == DepthContinuityFamily::BitgetV3Previous
            && self.pending_bitget_snapshot.is_some()
        {
            return self.process_bitget_bridge(message);
        }
        // ----------------------------------------------------
        // REQUIRE SYNCHRONIZED BASELINE
        // ----------------------------------------------------

        if !self.is_synchronized() {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotRequired,
            };
        }

        let baseline = self
            .baseline
            .expect("synchronized controller must have a baseline");

        // ----------------------------------------------------
        // CONTINUITY VALIDATION
        // ----------------------------------------------------

        let decision = DepthContinuityValidator::evaluate(
            &self.plan,
            baseline,
            message.sequence(),
            !message.is_empty(),
        );

        let next_sequence = match decision {
            DepthContinuityDecision::Accept { next_sequence } => next_sequence,

            DepthContinuityDecision::Ignore { reason } => {
                return DepthControllerOutcome::Ignored {
                    reason: match reason {
                        DepthIgnoreReason::Stale => DepthControllerIgnoreReason::StaleSequence,

                        DepthIgnoreReason::Keepalive => DepthControllerIgnoreReason::Keepalive,
                    },
                };
            }

            DepthContinuityDecision::Resynchronize { reason } => {
                self.invalidate();

                return DepthControllerOutcome::RecoveryRequired {
                    reason: DepthControllerRecoveryReason::InvalidSequence(reason),
                };
            }
        };

        // ----------------------------------------------------
        // ATOMIC BOOK UPDATE
        // ----------------------------------------------------

        let proposed_changes = to_book_changes(message.levels());

        // ----------------------------------------------------
        // RETAINED BOOK BUDGET PREFLIGHT
        // ----------------------------------------------------

        if let Err(error) =
            DepthBookBudget::validate_delta(&self.book, &proposed_changes, self.book_limits)
        {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(format!(
                    "depth delta exceeds retained book \
                     budget: {error:?}"
                )),
            };
        }

        // ----------------------------------------------------
        // ATOMIC BOOK APPLICATION
        // ----------------------------------------------------

        let changes = match self.book.apply_batch(proposed_changes) {
            Ok(changes) => changes,

            Err(error) => {
                self.invalidate();

                return DepthControllerOutcome::RecoveryRequired {
                    reason: DepthControllerRecoveryReason::InvalidBook(error.to_string()),
                };
            }
        };

        // ----------------------------------------------------
        // ADVANCE ACCEPTED BASELINE
        // ----------------------------------------------------

        // The sequence baseline advances only after the
        // complete book batch was applied successfully.

        let next_baseline = DepthSequenceBaseline {
            last_sequence: next_sequence,

            cross_sequence: message
                .sequence()
                .cross_sequence
                .or(baseline.cross_sequence),

            synchronized: true,
        };

        self.baseline = Some(next_baseline);

        DepthControllerOutcome::Accepted {
            changes,
            snapshot: false,
            book_generation: self.book_generation,
            baseline: next_baseline,
        }
    }
    // ========================================================
    // BITGET INITIAL SNAPSHOT BRIDGE
    // ========================================================

    /// Validate the first delta after a Bitget V3 snapshot.
    ///
    /// Required:
    ///
    ///     delta.pseq <= snapshot.seq <= delta.seq
    ///
    /// A valid bridge is applied to the private candidate
    /// book before either the book or sequence baseline
    /// becomes authoritative.
    ///
    fn process_bitget_bridge(&mut self, message: &NormalizedDepth) -> DepthControllerOutcome {
        let Some(pending) = self.pending_bitget_snapshot.as_ref() else {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotRequired,
            };
        };

        let snapshot_sequence = pending.snapshot_sequence;

        let sequence = message.sequence();

        // The Bitget first-delta contract does not include
        // U/u ranges or cross-sequence metadata.
        if sequence.sequence_first.is_some() || sequence.cross_sequence.is_some() {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::UnsupportedMetadata,
                ),
            };
        }

        let (Some(current), Some(previous)) = (sequence.sequence_last, sequence.sequence_previous)
        else {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::MissingSequence,
                ),
            };
        };

        // A reset-marked delta cannot establish continuity
        // with the staged snapshot.
        //
        // This restriction applies to deltas, not to the
        // initial snapshot's own pseq=0.
        if previous == 0 {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::ResetRequiresSnapshot,
                ),
            };
        }

        // A delta whose sequence ends before the snapshot
        // cannot contribute to its synchronization bridge.
        //
        // Ignore it without discarding the candidate, since
        // a later delta may bridge the snapshot.
        if current < snapshot_sequence {
            return DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::StaleSequence,
            };
        }

        // Validate the actual Bitget bridge:
        //
        //   previous <= snapshot_sequence <= current
        //
        // A delta that starts after the snapshot sequence
        // cannot be safely joined to the staged book.
        if previous > snapshot_sequence {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotBridgeFailed,
            };
        }

        let Some(next_generation) = self.book_generation.checked_add(1) else {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::BookGenerationExhausted,
            };
        };

        let changes = to_book_changes(message.levels());
        // ----------------------------------------------------
        // STAGED BOOK BUDGET PREFLIGHT
        // ----------------------------------------------------

        let budget_result = {
            let pending = self
                .pending_bitget_snapshot
                .as_ref()
                .expect("pending Bitget snapshot was checked");

            DepthBookBudget::validate_delta(&pending.book, &changes, self.book_limits)
        };

        if let Err(error) = budget_result {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(format!(
                    "Bitget bridge exceeds retained book \
                     budget: {error:?}"
                )),
            };
        }
        // Apply to the private candidate first.
        //
        // A failed batch cannot partially mutate the
        // authoritative book.
        let applied = {
            let candidate = &mut self
                .pending_bitget_snapshot
                .as_mut()
                .expect("pending Bitget snapshot was checked")
                .book;

            candidate.apply_batch(changes)
        };

        if let Err(error) = applied {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(error.to_string()),
            };
        }

        // All validation and book application succeeded.
        //
        // Transfer ownership of the completed candidate
        // into the authoritative book.
        let candidate = self
            .pending_bitget_snapshot
            .take()
            .expect("pending Bitget snapshot was checked");

        // The staged snapshot and first bridging delta have
        // both been accepted.
        //
        // The resulting candidate is the first authoritative
        // book of this generation.
        self.book = candidate.book;

        let baseline = DepthSequenceBaseline {
            last_sequence: current,
            cross_sequence: None,
            synchronized: true,
        };

        self.baseline = Some(baseline);
        self.book_generation = next_generation;

        // Initial synchronization must deliver the complete book,
        // not merely the bridging delta.
        let changes = full_book_changes(&self.book);

        DepthControllerOutcome::Accepted {
            changes,
            snapshot: true,
            book_generation: self.book_generation,
            baseline,
        }
    }
    // ============================================================
    // REST SNAPSHOT SYNCHRONIZATION
    // ============================================================

    /// Complete one REST snapshot synchronization attempt.
    ///
    /// The caller must:
    /// 1. Start buffering WebSocket deltas before requesting REST.
    /// 2. Associate the REST request with `token`.
    /// 3. Serialize buffer admission and snapshot completion.
    /// 4. Never feed an obsolete response under a newer token.
    ///
    /// A successful result contains the FULL synchronized book
    /// state, not merely the changes applied during bridging.
    ///
    /// This operation does not perform HTTP requests or publish
    /// market data.

    pub(super) fn complete_rest_snapshot(
        &mut self,
        token: DepthSyncToken,
        buffer: &mut PreSnapshotDepthBuffer<NormalizedDepth>,
        snapshot: &NormalizedDepth,
    ) -> DepthControllerOutcome {
        // --------------------------------------------------------
        // VERIFY SYNCHRONIZATION ATTEMPT
        // --------------------------------------------------------

        if !self.plan.requires_update_buffer
            || self.plan.snapshot_source != DepthSnapshotSource::Rest
        {
            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSource,
            };
        }

        // Reject obsolete responses without modifying the
        // authoritative controller or current buffer.
        // The controller, not the caller or buffer, owns
        // synchronization-attempt authority.
        // An obsolete attempt must never invalidate a newer
        // synchronized book or cancel a newer REST attempt.
        if token.stream_generation != self.stream_generation
            || self.active_rest_attempt != Some(token)
            || buffer.token() != token
        {
            return DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration,
            };
        }

        if buffer.is_invalidated() {
            self.invalidate();

            return DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(
                    "REST synchronization buffer was invalidated".into(),
                ),
            };
        }

        if snapshot.stream_id() != self.stream_id {
            return self.fail_rest_sync(buffer, DepthControllerRecoveryReason::WrongStream);
        }

        if snapshot.source() != NormalizationSource::Rest
            || snapshot.kind() != DepthMessageKind::Snapshot
        {
            return self.fail_rest_sync(buffer, DepthControllerRecoveryReason::InvalidSource);
        }
        if let Err(error) = DepthBookBudget::validate_snapshot(snapshot.levels(), self.book_limits)
        {
            return self.fail_rest_sync(
                buffer,
                DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "REST snapshot exceeds retained book \
                 budget: {error:?}"
                )),
            );
        }
        let Some(snapshot_sequence) = snapshot.sequence().sequence_last else {
            return self.fail_rest_sync(
                buffer,
                DepthControllerRecoveryReason::InvalidSnapshot(
                    "REST snapshot lacks an authoritative sequence ID".into(),
                ),
            );
        };

        let Some(next_book_generation) = self.book_generation.checked_add(1) else {
            return self.fail_rest_sync(
                buffer,
                DepthControllerRecoveryReason::BookGenerationExhausted,
            );
        };
        if let Err(error) = DepthResourceAccounting::validate_message(snapshot, self.resources) {
            return self.fail_rest_sync(
                buffer,
                DepthControllerRecoveryReason::InvalidSnapshot(format!(
                    "REST snapshot exceeds resource limits: \
                 {error:?}"
                )),
            );
        }
        // --------------------------------------------------------
        // BUILD PRIVATE CANDIDATE BOOK
        // --------------------------------------------------------

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        for (side, price, level) in to_book_changes(snapshot.levels()) {
            match side {
                BookSide::Bid => bids.push((price, level)),
                BookSide::Ask => asks.push((price, level)),
            }
        }

        let mut candidate = BookStore::new();

        if let Err(error) = candidate.apply_snapshot(bids, asks) {
            return self.fail_rest_sync(
                buffer,
                DepthControllerRecoveryReason::InvalidSnapshot(error.to_string()),
            );
        }

        // --------------------------------------------------------
        // TRANSFER BUFFERED DELTAS
        // --------------------------------------------------------

        // The outer runtime must serialize this operation
        // with WebSocket message admission.
        //
        // A message arriving after drain cannot be allowed
        // to bypass either the remaining replay or the
        // controller's synchronized state transition.

        let updates = match buffer.drain(token) {
            Ok(updates) => updates,

            Err(error) => {
                return self.fail_rest_sync(
                    buffer,
                    DepthControllerRecoveryReason::InvalidSnapshot(format!(
                        "buffer handoff failed: {error:?}"
                    )),
                );
            }
        };

        // --------------------------------------------------------
        // FIND INITIAL BRIDGE
        // --------------------------------------------------------

        let mut baseline = DepthSequenceBaseline {
            last_sequence: snapshot_sequence,
            cross_sequence: None,
            synchronized: true,
        };

        let mut bridged = false;

        for message in updates {
            if message.stream_id() != self.stream_id
                || message.source() != NormalizationSource::WebSocket
                || message.kind() != DepthMessageKind::Delta
            {
                return self.fail_rest_sync(buffer, DepthControllerRecoveryReason::InvalidSource);
            }

            if !bridged {
                match self.evaluate_rest_bridge(snapshot_sequence, message.sequence()) {
                    RestBridgeDecision::Stale => {
                        continue;
                    }

                    RestBridgeDecision::Accept { next_sequence } => {
                        let changes = to_book_changes(message.levels());

                        if let Err(error) =
                            DepthBookBudget::validate_delta(&candidate, &changes, self.book_limits)
                        {
                            return self.fail_rest_sync(
                                buffer,
                                DepthControllerRecoveryReason::InvalidBook(format!(
                                    "REST bridge exceeds retained book \
                     budget: {error:?}"
                                )),
                            );
                        }

                        if let Err(error) = candidate.apply_batch(changes) {
                            return self.fail_rest_sync(
                                buffer,
                                DepthControllerRecoveryReason::InvalidBook(error.to_string()),
                            );
                        }

                        baseline.last_sequence = next_sequence;
                        bridged = true;
                    }

                    RestBridgeDecision::Invalid(reason) => {
                        return self.fail_rest_sync(
                            buffer,
                            DepthControllerRecoveryReason::InvalidSequence(reason),
                        );
                    }
                }

                continue;
            }

            // ----------------------------------------------------
            // VALIDATE SUBSEQUENT DELTAS
            // ----------------------------------------------------

            match DepthContinuityValidator::evaluate(
                &self.plan,
                baseline,
                message.sequence(),
                !message.is_empty(),
            ) {
                DepthContinuityDecision::Accept { next_sequence } => {
                    let changes = to_book_changes(message.levels());

                    if let Err(error) =
                        DepthBookBudget::validate_delta(&candidate, &changes, self.book_limits)
                    {
                        return self.fail_rest_sync(
                            buffer,
                            DepthControllerRecoveryReason::InvalidBook(format!(
                                "REST replay exceeds retained book \
                     budget: {error:?}"
                            )),
                        );
                    }

                    if let Err(error) = candidate.apply_batch(changes) {
                        return self.fail_rest_sync(
                            buffer,
                            DepthControllerRecoveryReason::InvalidBook(error.to_string()),
                        );
                    }

                    baseline.last_sequence = next_sequence;
                }

                DepthContinuityDecision::Ignore { .. } => {
                    // The message is stale or otherwise
                    // explicitly ignorable under its protocol.
                }

                DepthContinuityDecision::Resynchronize { reason } => {
                    return self.fail_rest_sync(
                        buffer,
                        DepthControllerRecoveryReason::InvalidSequence(reason),
                    );
                }
            }
        }

        // --------------------------------------------------------
        // REQUIRE SUCCESSFUL INITIAL BRIDGE
        // --------------------------------------------------------

        if !bridged {
            return self
                .fail_rest_sync(buffer, DepthControllerRecoveryReason::SnapshotBridgeFailed);
        }

        // --------------------------------------------------------
        // PREPARE FULL SYNCHRONIZED BOOK OUTPUT
        // --------------------------------------------------------

        // A consumer must initialize from the complete final
        // candidate state, not just the last applied delta.
        if let Err(error) = DepthBookBudget::validate_book(&candidate, self.book_limits) {
            return self.fail_rest_sync(
                buffer,
                DepthControllerRecoveryReason::InvalidBook(format!(
                    "completed REST book exceeds retained \
                 budget: {error:?}"
                )),
            );
        }
        let changes = full_book_changes(&candidate);

        // --------------------------------------------------------
        // ATOMIC AUTHORITATIVE COMMIT
        // --------------------------------------------------------

        self.book = candidate;
        self.baseline = Some(baseline);
        self.book_generation = next_book_generation;
        // The synchronization attempt has completed.
        //
        // A delayed duplicate response carrying the same token
        // must not be able to replace this accepted book.
        self.active_rest_attempt = None;
        DepthControllerOutcome::Accepted {
            changes,
            snapshot: true,
            book_generation: self.book_generation,
            baseline,
        }
    }

    // ============================================================
    // REST BRIDGE VALIDATION
    // ============================================================

    /// Evaluate the first applicable WebSocket delta against
    /// the authoritative REST snapshot sequence.
    ///
    /// This is intentionally separate from ordinary delta
    /// continuity because the initial joining rules differ.
    fn evaluate_rest_bridge(
        &self,
        snapshot_sequence: u64,
        sequence: &crate::canonical::SequenceMetadata,
    ) -> RestBridgeDecision {
        match self.plan.initial_bridge {
            DepthBridgeRule::BinanceSpotRangeContainsSnapshot => {
                // Binance Spot:
                //
                // Discard:
                //     u <= snapshot.lastUpdateId
                //
                // First applicable:
                //     U <= snapshot.lastUpdateId + 1 <= u
                //
                // The latter follows the current documented
                // diff-depth initialization boundary.

                let (Some(first), Some(last)) = (sequence.sequence_first, sequence.sequence_last)
                else {
                    return RestBridgeDecision::Invalid(DepthViolationReason::MissingSequence);
                };

                if sequence.sequence_previous.is_some() || sequence.cross_sequence.is_some() {
                    return RestBridgeDecision::Invalid(DepthViolationReason::UnsupportedMetadata);
                }

                if first > last {
                    return RestBridgeDecision::Invalid(DepthViolationReason::InvalidRange);
                }

                if last <= snapshot_sequence {
                    return RestBridgeDecision::Stale;
                }

                let Some(required) = snapshot_sequence.checked_add(1) else {
                    return RestBridgeDecision::Invalid(DepthViolationReason::SequenceOverflow);
                };

                if first > required {
                    return RestBridgeDecision::Invalid(DepthViolationReason::SequenceGap);
                }

                RestBridgeDecision::Accept {
                    next_sequence: last,
                }
            }

            DepthBridgeRule::BinanceFuturesSnapshotBridge => {
                let (Some(first), Some(last), Some(previous)) = (
                    sequence.sequence_first,
                    sequence.sequence_last,
                    sequence.sequence_previous,
                ) else {
                    return RestBridgeDecision::Invalid(DepthViolationReason::MissingSequence);
                };

                if sequence.cross_sequence.is_some() {
                    return RestBridgeDecision::Invalid(DepthViolationReason::UnsupportedMetadata);
                }

                if first > last {
                    return RestBridgeDecision::Invalid(DepthViolationReason::InvalidRange);
                }

                // The REST snapshot already contains every update
                // through snapshot_sequence.
                //
                // Never replay an update whose final sequence is
                // equal to or older than that snapshot.
                //
                // In particular, an event ending exactly at the
                // snapshot boundary may be valid protocol evidence,
                // but its absolute quantities must not overwrite
                // the newer REST snapshot.
                if last <= snapshot_sequence {
                    return RestBridgeDecision::Stale;
                }

                // The first applicable buffered range may cross the
                // snapshot boundary and extend beyond it.
                let crosses_snapshot = first <= snapshot_sequence && snapshot_sequence < last;

                // Alternatively, the first applicable update may
                // continue directly after the snapshot.
                //
                // The explicit previous-update linkage is required
                // for this case.
                let directly_continues = previous == snapshot_sequence
                    && snapshot_sequence
                        .checked_add(1)
                        .is_some_and(|required| first <= required && required <= last);

                if !crosses_snapshot && !directly_continues {
                    return RestBridgeDecision::Invalid(DepthViolationReason::SequenceGap);
                }

                RestBridgeDecision::Accept {
                    next_sequence: last,
                }
            }

            DepthBridgeRule::NextRequiredInRange => {
                // Gate.io:
                //
                // required = snapshot.id + 1
                //
                // Stale:
                //     u < required
                //
                // Bridge:
                //     U <= required <= u

                let (Some(first), Some(last)) = (sequence.sequence_first, sequence.sequence_last)
                else {
                    return RestBridgeDecision::Invalid(DepthViolationReason::MissingSequence);
                };

                if sequence.sequence_previous.is_some() || sequence.cross_sequence.is_some() {
                    return RestBridgeDecision::Invalid(DepthViolationReason::UnsupportedMetadata);
                }

                if first > last {
                    return RestBridgeDecision::Invalid(DepthViolationReason::InvalidRange);
                }

                let Some(required) = snapshot_sequence.checked_add(1) else {
                    return RestBridgeDecision::Invalid(DepthViolationReason::SequenceOverflow);
                };

                if last < required {
                    return RestBridgeDecision::Stale;
                }

                if first > required {
                    return RestBridgeDecision::Invalid(DepthViolationReason::SequenceGap);
                }

                RestBridgeDecision::Accept {
                    next_sequence: last,
                }
            }

            _ => RestBridgeDecision::Invalid(DepthViolationReason::UnsupportedMetadata),
        }
    }

    // ============================================================
    // FAILED SYNCHRONIZATION
    // ============================================================

    /// Invalidate both authoritative book synchronization
    /// and the current temporary REST synchronization attempt.
    ///
    /// This does not schedule recovery; the runtime uses the
    /// returned outcome to request a new attempt.
    fn fail_rest_sync(
        &mut self,
        buffer: &mut PreSnapshotDepthBuffer<NormalizedDepth>,
        reason: DepthControllerRecoveryReason,
    ) -> DepthControllerOutcome {
        self.invalidate();
        buffer.invalidate();

        DepthControllerOutcome::RecoveryRequired { reason }
    }
}

/// Export every currently retained level of an initialized book.
///
/// This is a complete snapshot of the BookStore's retained
/// coverage, not a diff against a previous book.
///
/// Used only when establishing a new synchronized
/// book generation.
fn full_book_changes(book: &BookStore) -> Vec<BookChange> {
    let mut levels = Vec::with_capacity(book.bids().len() + book.asks().len());

    for (price, level) in book.bids() {
        levels.push((BookSide::Bid, *price, level.clone()));
    }

    for (price, level) in book.asks() {
        levels.push((BookSide::Ask, *price, level.clone()));
    }

    levels
}

/// A publication-eligible book operation.
///
/// Snapshot establishes a complete retained book for
/// a new book generation.
///
/// Delta changes an already initialized book generation.
///
/// Neither form represents durable persistence or
/// successful downstream delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptedDepthOutput<'a> {
    Snapshot {
        book_generation: u64,
        baseline: DepthSequenceBaseline,
        levels: &'a [BookChange],
    },

    Delta {
        book_generation: u64,
        baseline: DepthSequenceBaseline,
        changes: &'a [BookChange],
    },
}

impl DepthControllerOutcome {
    /// Return a typed, publication-eligible book operation.
    ///
    /// Buffered updates, staged snapshots, stale messages,
    /// and recovery-required outcomes return None.
    ///
    /// This is the preferred boundary for downstream
    /// publication adapters.
    pub fn accepted_output(&self) -> Option<AcceptedDepthOutput<'_>> {
        match self {
            Self::Accepted {
                changes,
                snapshot: true,
                book_generation,
                baseline,
            } => Some(AcceptedDepthOutput::Snapshot {
                book_generation: *book_generation,
                baseline: *baseline,
                levels: changes,
            }),

            Self::Accepted {
                changes,
                snapshot: false,
                book_generation,
                baseline,
            } => Some(AcceptedDepthOutput::Delta {
                book_generation: *book_generation,
                baseline: *baseline,
                changes,
            }),

            Self::Ignored { .. } | Self::RecoveryRequired { .. } | Self::SnapshotStaged { .. } => {
                None
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestBridgeDecision {
    Stale,

    Accept { next_sequence: u64 },

    Invalid(DepthViolationReason),
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use rust_decimal::Decimal;

    use crate::{
        canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata},
        live::{config::format::DepthFormatConfig, integrity::DepthContinuityCompiler},
    };

    fn begin_rest(controller: &mut LiveDepthController) {
        controller.begin_rest_attempt(rest_token()).unwrap();
    }

    fn plan(profile: &str) -> CompiledDepthContinuityPlan {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/formats/depth")
            .join(format!("{profile}.toml"));

        let config = DepthFormatConfig::load(path).unwrap();

        DepthContinuityCompiler::compile(&config).unwrap()
    }

    fn controller(profile: &str) -> LiveDepthController {
        LiveDepthController::new("stream-42", plan(profile), 10).unwrap()
    }

    fn level(side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
        L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Bybit,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: "stream-42".into(),
            },

            side,

            price: Decimal::from(price),

            quantity_base: Some(Decimal::from(quantity)),
            quantity_quote: None,
            quantity_contracts: None,

            order_count: None,
        }
    }

    fn sequence(
        first: Option<u64>,
        last: Option<u64>,
        previous: Option<u64>,
        cross: Option<u64>,
    ) -> SequenceMetadata {
        SequenceMetadata {
            sequence_first: first,
            sequence_last: last,
            sequence_previous: previous,
            cross_sequence: cross,
        }
    }

    fn depth(
        kind: DepthMessageKind,
        sequence: SequenceMetadata,
        levels: Vec<L2LevelUpdate>,
    ) -> NormalizedDepth {
        NormalizedDepth::new(
            "stream-42",
            NormalizationSource::WebSocket,
            kind,
            sequence,
            levels,
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap()
    }

    fn bybit_snapshot(update_id: u64, cross_sequence: u64, quantity: i64) -> NormalizedDepth {
        depth(
            DepthMessageKind::Snapshot,
            sequence(None, Some(update_id), None, Some(cross_sequence)),
            vec![
                level(BookSide::Bid, 80_000, quantity),
                level(BookSide::Ask, 80_010, 2),
            ],
        )
    }

    fn bybit_delta(update_id: u64, cross_sequence: u64, quantity: i64) -> NormalizedDepth {
        depth(
            DepthMessageKind::Delta,
            sequence(None, Some(update_id), None, Some(cross_sequence)),
            vec![level(BookSide::Bid, 80_000, quantity)],
        )
    }

    // ========================================================
    // SNAPSHOT INITIALIZATION
    // ========================================================

    #[test]
    fn snapshot_initializes_book_and_sequence() {
        let mut controller = controller("bybit_spot");

        let outcome = controller.process(10, &bybit_snapshot(100, 200, 3));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: true,
                book_generation: 1,
                ..
            }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(controller.baseline().unwrap().last_sequence, 100,);

        assert_eq!(controller.baseline().unwrap().cross_sequence, Some(200),);

        assert_eq!(
            controller
                .book()
                .bids()
                .get(&Decimal::from(80_000))
                .unwrap()
                .quantity_base,
            Some(Decimal::from(3)),
        );
    }

    // ========================================================
    // INCREMENTAL UPDATES
    // ========================================================

    #[test]
    fn valid_delta_advances_book_and_sequence() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let outcome = controller.process(10, &bybit_delta(101, 201, 5));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: false,
                book_generation: 1,
                ..
            }
        ));

        assert_eq!(controller.baseline().unwrap().last_sequence, 101,);

        assert_eq!(controller.baseline().unwrap().cross_sequence, Some(201),);

        assert_eq!(
            controller
                .book()
                .bids()
                .get(&Decimal::from(80_000))
                .unwrap()
                .quantity_base,
            Some(Decimal::from(5)),
        );
    }

    // ========================================================
    // STALE UPDATES
    // ========================================================

    #[test]
    fn stale_delta_does_not_mutate_book() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let outcome = controller.process(10, &bybit_delta(99, 199, 99));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::StaleSequence
            }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(controller.baseline().unwrap().last_sequence, 100,);

        assert_eq!(
            controller
                .book()
                .bids()
                .get(&Decimal::from(80_000))
                .unwrap()
                .quantity_base,
            Some(Decimal::from(3)),
        );
    }

    // ========================================================
    // SEQUENCE VIOLATIONS
    // ========================================================

    #[test]
    fn invalid_sequence_invalidates_book() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let outcome = controller.process(10, &bybit_delta(101, 199, 5));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::SequenceRegression
                )
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());

        assert!(controller.book().bids().is_empty());
        assert!(controller.book().asks().is_empty());

        assert_eq!(controller.baseline(), None);
    }

    // ========================================================
    // ATOMIC BOOK APPLICATION
    // ========================================================

    #[test]
    fn malformed_batch_never_partially_mutates_book() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let invalid = depth(
            DepthMessageKind::Delta,
            sequence(None, Some(101), None, Some(201)),
            vec![
                level(BookSide::Bid, 80_000, 5),
                level(BookSide::Ask, 80_020, -1),
            ],
        );

        let outcome = controller.process(10, &invalid);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(_)
            }
        ));

        // The controller invalidates the complete book.
        // A partially applied update must not survive.

        assert!(!controller.is_synchronized());
        assert!(controller.book().bids().is_empty());
        assert!(controller.book().asks().is_empty());
    }

    // ========================================================
    // SNAPSHOT REPLACEMENT
    // ========================================================

    #[test]
    fn replacement_snapshot_removes_obsolete_levels() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let replacement = depth(
            DepthMessageKind::Snapshot,
            sequence(None, Some(200), None, Some(300)),
            vec![level(BookSide::Bid, 79_990, 7)],
        );

        let outcome = controller.process(10, &replacement);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: true,
                book_generation: 2,
                ..
            }
        ));

        assert!(
            controller
                .book()
                .bids()
                .get(&Decimal::from(80_000))
                .is_none()
        );

        assert!(
            controller
                .book()
                .asks()
                .get(&Decimal::from(80_010))
                .is_none()
        );

        assert!(
            controller
                .book()
                .bids()
                .contains_key(&Decimal::from(79_990))
        );

        assert_eq!(controller.baseline().unwrap().last_sequence, 200,);
    }

    // ========================================================
    // OBSOLETE GENERATIONS
    // ========================================================

    #[test]
    fn obsolete_generation_cannot_modify_book() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        controller.begin_generation(11).unwrap();

        let outcome = controller.process(10, &bybit_delta(101, 201, 99));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert_eq!(controller.stream_generation(), 11);

        assert!(!controller.is_synchronized());
        assert!(controller.book().bids().is_empty());
    }

    // ========================================================
    // DELTA BEFORE SNAPSHOT
    // ========================================================

    #[test]
    fn delta_before_snapshot_requires_recovery() {
        let mut controller = controller("bybit_spot");

        let outcome = controller.process(10, &bybit_delta(101, 201, 5));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotRequired
            }
        ));

        assert!(!controller.is_synchronized());
        assert_eq!(controller.baseline(), None);
    }

    // ========================================================
    // REST SNAPSHOT STRATEGIES
    // ========================================================

    #[test]
    fn binance_spot_rejects_unbridged_ws_snapshot() {
        let mut controller = controller("binance_spot");

        let outcome = controller.process(10, &bybit_snapshot(100, 200, 3));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSource
            }
        ));

        assert!(!controller.is_synchronized());
    }

    // ========================================================
    // GATE.IO WEBSOCKET FULL SNAPSHOT
    // ========================================================

    #[test]
    fn gate_websocket_snapshot_establishes_new_book() {
        let mut controller = controller("gateio_spot");

        let snapshot = depth(
            DepthMessageKind::Snapshot,
            sequence(None, Some(100), None, None),
            vec![level(BookSide::Bid, 80_000, 3)],
        );

        let outcome = controller.process(10, &snapshot);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: true,
                book_generation: 1,
                ..
            }
        ));

        assert!(controller.is_synchronized());
    }
    fn bitget_snapshot(sequence_id: u64) -> NormalizedDepth {
        depth(
            DepthMessageKind::Snapshot,
            sequence(None, Some(sequence_id), Some(0), None),
            vec![
                level(BookSide::Bid, 80_000, 3),
                level(BookSide::Ask, 80_010, 2),
            ],
        )
    }

    fn bitget_delta(previous: u64, current: u64, quantity: i64) -> NormalizedDepth {
        depth(
            DepthMessageKind::Delta,
            sequence(None, Some(current), Some(previous), None),
            vec![level(BookSide::Bid, 80_000, quantity)],
        )
    }

    #[test]
    fn bitget_snapshot_is_staged_not_synchronized() {
        let mut controller = controller("bitget_spot");

        let outcome = controller.process(10, &bitget_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::SnapshotStaged {
                snapshot_sequence: 100
            }
        ));

        assert!(!controller.is_synchronized());

        assert_eq!(controller.baseline(), None);

        // The private candidate must not be visible through
        // the authoritative book accessor.
        assert!(!controller.book().is_initialized());

        assert!(controller.book().bids().is_empty());
        assert!(controller.book().asks().is_empty());

        assert_eq!(controller.book_generation(), 0);
    }

    #[test]
    fn bitget_valid_bridge_commits_candidate_atomically() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        let outcome = controller.process(10, &bitget_delta(99, 105, 7));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: true,
                book_generation: 1,
                ..
            }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(controller.baseline().unwrap().last_sequence, 105);

        assert_eq!(
            controller
                .book()
                .bids()
                .get(&Decimal::from(80_000))
                .unwrap()
                .quantity_base,
            Some(Decimal::from(7))
        );

        // The snapshot's ask level must still be present.
        assert!(
            controller
                .book()
                .asks()
                .contains_key(&Decimal::from(80_010))
        );
    }

    #[test]
    fn bitget_stale_delta_preserves_pending_snapshot() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        let outcome = controller.process(10, &bitget_delta(90, 95, 7));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::StaleSequence
            }
        ));

        assert!(!controller.is_synchronized());

        // A later valid bridge can still activate the book.
        let outcome = controller.process(10, &bitget_delta(99, 105, 7));

        assert!(matches!(outcome, DepthControllerOutcome::Accepted { .. }));
    }

    #[test]
    fn bitget_failed_bridge_invalidates_candidate() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        let outcome = controller.process(10, &bitget_delta(102, 105, 7));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotBridgeFailed
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());

        assert_eq!(controller.baseline(), None);
        assert_eq!(controller.book_generation(), 0);
    }

    #[test]
    fn bitget_reset_delta_requires_new_snapshot() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        let outcome = controller.process(10, &bitget_delta(0, 105, 7));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::ResetRequiresSnapshot
                )
            }
        ));

        assert!(!controller.is_synchronized());
    }

    #[test]
    fn bitget_invalid_bridge_batch_does_not_commit() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        let invalid = depth(
            DepthMessageKind::Delta,
            sequence(None, Some(105), Some(99), None),
            vec![
                level(BookSide::Bid, 80_000, 7),
                level(BookSide::Ask, 80_020, -1),
            ],
        );

        let outcome = controller.process(10, &invalid);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());

        assert_eq!(controller.baseline(), None);
    }

    #[test]
    fn bitget_new_generation_discards_pending_snapshot() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        controller.begin_generation(11).unwrap();

        let obsolete = controller.process(10, &bitget_delta(99, 105, 7));

        assert!(matches!(
            obsolete,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        let current = controller.process(11, &bitget_delta(99, 105, 7));

        assert!(matches!(
            current,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotRequired
            }
        ));

        assert!(!controller.is_synchronized());
    }

    #[test]
    fn bitget_repeated_snapshot_replaces_candidate() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        controller.process(10, &bitget_snapshot(200));

        // This delta bridges the old snapshot but cannot
        // bridge the newer candidate.
        let stale = controller.process(10, &bitget_delta(99, 105, 7));

        assert!(matches!(
            stale,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::StaleSequence
            }
        ));

        let accepted = controller.process(10, &bitget_delta(199, 205, 8));

        assert!(matches!(
            accepted,
            DepthControllerOutcome::Accepted {
                book_generation: 1,
                ..
            }
        ));

        assert_eq!(controller.baseline().unwrap().last_sequence, 205);
    }
    use crate::live::integrity::{DepthBufferLimits, DepthSyncToken, PreSnapshotDepthBuffer};

    fn rest_token() -> DepthSyncToken {
        DepthSyncToken {
            stream_generation: 10,
            attempt: 1,
        }
    }

    fn rest_buffer() -> PreSnapshotDepthBuffer<NormalizedDepth> {
        PreSnapshotDepthBuffer::new(
            rest_token(),
            DepthBufferLimits {
                max_messages: 32,
                max_bytes: 65_536,
            },
        )
        .unwrap()
    }

    fn rest_snapshot(sequence_id: u64) -> NormalizedDepth {
        NormalizedDepth::new(
            "stream-42",
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            sequence(None, Some(sequence_id), None, None),
            vec![
                level(BookSide::Bid, 80_000, 3),
                level(BookSide::Ask, 80_010, 2),
            ],
            Some(1_000_000_000),
            Some(200),
        )
        .unwrap()
    }

    fn range_delta(first: u64, last: u64) -> NormalizedDepth {
        depth(
            DepthMessageKind::Delta,
            sequence(Some(first), Some(last), None, None),
            vec![level(BookSide::Bid, 80_000, 7)],
        )
    }

    fn futures_delta(first: u64, last: u64, previous: u64) -> NormalizedDepth {
        depth(
            DepthMessageKind::Delta,
            sequence(Some(first), Some(last), Some(previous), None),
            vec![level(BookSide::Bid, 80_000, 7)],
        )
    }

    fn push_buffered(
        buffer: &mut PreSnapshotDepthBuffer<NormalizedDepth>,
        message: NormalizedDepth,
    ) {
        // The test supplies a conservative fixed allocation
        // allowance. Production must use authoritative
        // retained-message memory accounting.
        buffer.push(rest_token(), message, 1024).unwrap();
    }

    // ============================================================
    // N5N.7.4b — AUTHORITATIVE BOOK BUDGET TESTS
    // ============================================================

    fn book_limits(max_levels: usize) -> DepthBookLimits {
        DepthBookLimits {
            max_levels,
            max_estimated_bytes: 32 * 1024 * 1024,
        }
    }

    #[test]
    fn websocket_snapshot_rejects_excessive_book_size() {
        let mut controller = controller("bybit_spot");

        controller.set_book_limits(book_limits(1)).unwrap();

        // Existing fixture contains one bid and one ask.
        let outcome = controller.process(10, &bybit_snapshot(100, 200, 3));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
    }

    #[test]
    fn cumulative_delta_growth_is_rejected() {
        let mut controller = controller("bybit_spot");

        controller.set_book_limits(book_limits(3)).unwrap();

        controller.process(10, &bybit_snapshot(100, 200, 3));

        // The initial book contains two levels.
        let first_delta = depth(
            DepthMessageKind::Delta,
            sequence(None, Some(101), None, Some(201)),
            vec![level(BookSide::Bid, 79_990, 2)],
        );

        assert!(matches!(
            controller.process(10, &first_delta),
            DepthControllerOutcome::Accepted {
                snapshot: false,
                ..
            }
        ));

        assert_eq!(
            controller.book().bids().len() + controller.book().asks().len(),
            3,
        );

        // Each message is individually small, but this
        // second delta would exceed the cumulative limit.
        let second_delta = depth(
            DepthMessageKind::Delta,
            sequence(None, Some(102), None, Some(202)),
            vec![level(BookSide::Ask, 80_020, 5)],
        );

        let outcome = controller.process(10, &second_delta);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
        assert_eq!(controller.baseline(), None);
    }

    #[test]
    fn replacement_level_does_not_consume_extra_capacity() {
        let mut controller = controller("bybit_spot");

        controller.set_book_limits(book_limits(2)).unwrap();

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let outcome = controller.process(10, &bybit_delta(101, 201, 5));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: false,
                ..
            }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(
            controller.book().bids().len() + controller.book().asks().len(),
            2,
        );
    }

    #[test]
    fn deletion_and_insertion_can_share_full_capacity() {
        let mut controller = controller("bybit_spot");

        controller.set_book_limits(book_limits(2)).unwrap();

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let delta = depth(
            DepthMessageKind::Delta,
            sequence(None, Some(101), None, Some(201)),
            vec![
                level(BookSide::Ask, 80_010, 0),
                level(BookSide::Ask, 80_020, 4),
            ],
        );

        let outcome = controller.process(10, &delta);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted {
                snapshot: false,
                ..
            }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(
            controller.book().bids().len() + controller.book().asks().len(),
            2,
        );

        assert!(
            controller
                .book()
                .asks()
                .contains_key(&Decimal::from(80_020))
        );

        assert!(
            !controller
                .book()
                .asks()
                .contains_key(&Decimal::from(80_010))
        );
    }

    #[test]
    fn bitget_bridge_cannot_exceed_retained_book_budget() {
        let mut controller = controller("bitget_spot");

        controller.set_book_limits(book_limits(2)).unwrap();

        assert!(matches!(
            controller.process(10, &bitget_snapshot(100)),
            DepthControllerOutcome::SnapshotStaged { .. }
        ));

        let expanding_delta = depth(
            DepthMessageKind::Delta,
            sequence(None, Some(105), Some(99), None),
            vec![level(BookSide::Bid, 79_990, 5)],
        );

        let outcome = controller.process(10, &expanding_delta);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
        assert!(controller.pending_bitget_snapshot.is_none());
    }

    #[test]
    fn rest_snapshot_rejects_retained_book_overflow() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        controller.set_book_limits(book_limits(1)).unwrap();

        begin_rest(&mut controller);

        let outcome =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
    }

    #[test]
    fn rest_replay_cannot_expand_candidate_beyond_budget() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        controller.set_book_limits(book_limits(2)).unwrap();

        begin_rest(&mut controller);

        // The REST snapshot has two price levels.
        // This bridge introduces a third level.
        let expanding_delta = depth(
            DepthMessageKind::Delta,
            sequence(Some(101), Some(105), None, None),
            vec![level(BookSide::Bid, 79_990, 5)],
        );

        push_buffered(&mut buffer, expanding_delta);

        let outcome =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
    }

    #[test]
    fn book_budget_cannot_change_while_synchronized() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        assert!(controller.set_book_limits(book_limits(10)).is_err());

        assert!(controller.is_synchronized());
    }

    #[test]
    fn book_budget_can_change_after_invalidation() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        controller.invalidate();

        controller.set_book_limits(book_limits(10)).unwrap();

        assert_eq!(controller.book_limits().max_levels, 10);
    }

    #[test]
    fn binance_spot_rest_snapshot_bridges_buffered_updates() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, range_delta(90, 95));
        push_buffered(&mut buffer, range_delta(96, 105));
        push_buffered(&mut buffer, range_delta(106, 110));

        let result =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            result,
            DepthControllerOutcome::Accepted {
                snapshot: true,
                book_generation: 1,
                ..
            }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(controller.baseline().unwrap().last_sequence, 110,);

        assert_eq!(
            controller
                .book()
                .bids()
                .get(&Decimal::from(80_000))
                .unwrap()
                .quantity_base,
            Some(Decimal::from(7)),
        );
    }

    #[test]
    fn binance_spot_rejects_missing_initial_bridge() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, range_delta(105, 110));

        let result =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            result,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::SequenceGap
                )
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
        assert!(buffer.is_invalidated());
    }

    #[test]
    fn binance_futures_rest_snapshot_uses_previous_link() {
        let mut controller = controller("binance_linear");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, futures_delta(101, 105, 100));

        push_buffered(&mut buffer, futures_delta(106, 110, 105));

        let result =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            result,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        assert_eq!(controller.baseline().unwrap().last_sequence, 110,);
    }

    #[test]
    fn gate_rest_snapshot_accepts_overlapping_range() {
        let mut controller = controller("gateio_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, range_delta(98, 105));
        push_buffered(&mut buffer, range_delta(104, 110));

        let result =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            result,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        assert_eq!(controller.baseline().unwrap().last_sequence, 110,);
    }

    #[test]
    fn obsolete_rest_snapshot_cannot_modify_current_book() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        controller.begin_generation(11).unwrap();

        let result =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            result,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert!(!controller.is_synchronized());
    }

    #[test]
    fn rest_snapshot_without_bridge_is_not_synchronized() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        let result =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            result,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotBridgeFailed
            }
        ));

        assert!(!controller.is_synchronized());
        assert_eq!(controller.baseline(), None);
    }

    #[test]
    fn bad_replayed_delta_never_commits_partial_book() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, range_delta(101, 105));

        let invalid = depth(
            DepthMessageKind::Delta,
            sequence(Some(106), Some(110), None, None),
            vec![
                level(BookSide::Bid, 80_000, 9),
                level(BookSide::Ask, 80_020, -1),
            ],
        );

        push_buffered(&mut buffer, invalid);

        let result =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            result,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
        assert_eq!(controller.baseline(), None);
    }
    #[test]
    fn obsolete_rest_attempt_cannot_replace_newer_book() {
        let mut controller = controller("binance_spot");

        let token_1 = DepthSyncToken {
            stream_generation: 10,
            attempt: 1,
        };

        let token_2 = DepthSyncToken {
            stream_generation: 10,
            attempt: 2,
        };

        let limits = DepthBufferLimits {
            max_messages: 32,
            max_bytes: 65_536,
        };

        let mut old_buffer = PreSnapshotDepthBuffer::new(token_1, limits).unwrap();

        let mut new_buffer = PreSnapshotDepthBuffer::new(token_2, limits).unwrap();

        // Attempt 1 starts.
        controller.begin_rest_attempt(token_1).unwrap();

        old_buffer
            .push(token_1, range_delta(101, 105), 1024)
            .unwrap();

        // Attempt 2 supersedes attempt 1.
        controller.begin_rest_attempt(token_2).unwrap();

        new_buffer
            .push(token_2, range_delta(201, 205), 1024)
            .unwrap();

        // Attempt 2 successfully synchronizes the book.
        let current =
            controller.complete_rest_snapshot(token_2, &mut new_buffer, &rest_snapshot(200));

        assert!(matches!(
            current,
            DepthControllerOutcome::Accepted {
                book_generation: 1,
                ..
            }
        ));

        assert_eq!(controller.baseline().unwrap().last_sequence, 205);

        // Delayed response from attempt 1 arrives.
        //
        // It carries a matching old buffer and valid old
        // snapshot, but its attempt is no longer authorized.
        let obsolete =
            controller.complete_rest_snapshot(token_1, &mut old_buffer, &rest_snapshot(100));

        assert!(matches!(
            obsolete,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        // The newer synchronized book must remain intact.
        assert!(controller.is_synchronized());

        assert_eq!(controller.baseline().unwrap().last_sequence, 205);

        assert_eq!(controller.book_generation(), 1);
    }
    #[test]
    fn duplicate_rest_response_cannot_commit_twice() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, range_delta(101, 105));

        let first =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            first,
            DepthControllerOutcome::Accepted {
                book_generation: 1,
                ..
            }
        ));

        let second =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            second,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(controller.book_generation(), 1);

        assert_eq!(controller.baseline().unwrap().last_sequence, 105);
    }
    #[test]
    fn rest_attempt_ids_must_increase() {
        let mut controller = controller("binance_spot");

        let first = DepthSyncToken {
            stream_generation: 10,
            attempt: 1,
        };

        let second = DepthSyncToken {
            stream_generation: 10,
            attempt: 2,
        };

        controller.begin_rest_attempt(first).unwrap();

        assert!(controller.begin_rest_attempt(first).is_err());

        controller.begin_rest_attempt(second).unwrap();

        assert!(controller.begin_rest_attempt(first).is_err());

        assert_eq!(controller.active_rest_attempt(), Some(second));
    }
    #[test]
    fn gate_websocket_snapshot_supersedes_rest_attempt() {
        let mut controller = controller("gateio_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, range_delta(101, 105));

        // An authoritative Gate.io full snapshot arrives
        // through the WebSocket feed.
        let websocket_snapshot = depth(
            DepthMessageKind::Snapshot,
            sequence(None, Some(200), None, None),
            vec![level(BookSide::Bid, 80_000, 5)],
        );

        let accepted = controller.process(10, &websocket_snapshot);

        assert!(matches!(
            accepted,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        assert_eq!(controller.active_rest_attempt(), None);

        // The older REST response must not overwrite it.
        let obsolete =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            obsolete,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ));

        assert_eq!(controller.baseline().unwrap().last_sequence, 200);

        assert!(controller.is_synchronized());
    }
    #[test]
    fn websocket_snapshot_output_contains_complete_book() {
        let mut controller = controller("bybit_spot");

        let outcome = controller.process(10, &bybit_snapshot(100, 200, 3));

        let Some(AcceptedDepthOutput::Snapshot {
            levels,
            book_generation,
            baseline,
        }) = outcome.accepted_output()
        else {
            panic!("expected full synchronized snapshot");
        };

        assert_eq!(book_generation, 1);
        assert_eq!(baseline.last_sequence, 100);

        assert_eq!(
            levels.len(),
            controller.book().bids().len() + controller.book().asks().len(),
        );

        assert!(levels.iter().any(|(side, price, level)| {
            *side == BookSide::Bid
                && *price == Decimal::from(80_000)
                && level.quantity_base == Some(Decimal::from(3))
        }));

        assert!(levels.iter().any(|(side, price, level)| {
            *side == BookSide::Ask
                && *price == Decimal::from(80_010)
                && level.quantity_base == Some(Decimal::from(2))
        }));
    }
    #[test]
    fn bitget_bridge_output_is_full_snapshot_not_delta() {
        let mut controller = controller("bitget_spot");

        let staged = controller.process(10, &bitget_snapshot(100));

        assert!(staged.accepted_output().is_none());

        let outcome = controller.process(10, &bitget_delta(99, 105, 7));

        let Some(AcceptedDepthOutput::Snapshot {
            levels,
            book_generation,
            baseline,
        }) = outcome.accepted_output()
        else {
            panic!("Bitget bridge must emit synchronized snapshot");
        };

        assert_eq!(book_generation, 1);
        assert_eq!(baseline.last_sequence, 105);

        // The bridging delta changed only the bid.
        // The initial output must also include the ask
        // retained from the staged snapshot.
        assert_eq!(levels.len(), 2);

        assert!(levels.iter().any(|(side, price, level)| {
            *side == BookSide::Bid
                && *price == Decimal::from(80_000)
                && level.quantity_base == Some(Decimal::from(7))
        }));

        assert!(levels.iter().any(|(side, price, level)| {
            *side == BookSide::Ask
                && *price == Decimal::from(80_010)
                && level.quantity_base == Some(Decimal::from(2))
        }));
    }
    #[test]
    fn ordinary_delta_output_contains_only_changes() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let outcome = controller.process(10, &bybit_delta(101, 201, 5));

        let Some(AcceptedDepthOutput::Delta {
            changes,
            book_generation,
            baseline,
        }) = outcome.accepted_output()
        else {
            panic!("expected incremental depth output");
        };

        assert_eq!(book_generation, 1);
        assert_eq!(baseline.last_sequence, 101);

        // Only the modified bid should be emitted.
        assert_eq!(changes.len(), 1);

        assert_eq!(changes[0].0, BookSide::Bid);
        assert_eq!(changes[0].1, Decimal::from(80_000));

        assert_eq!(changes[0].2.quantity_base, Some(Decimal::from(5)),);
    }
    #[test]
    fn replacement_snapshot_output_is_complete_new_generation() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let replacement = depth(
            DepthMessageKind::Snapshot,
            sequence(None, Some(200), None, Some(300)),
            vec![
                level(BookSide::Bid, 80_000, 3),
                level(BookSide::Bid, 79_990, 7),
                level(BookSide::Ask, 80_020, 4),
            ],
        );

        let outcome = controller.process(10, &replacement);

        let Some(AcceptedDepthOutput::Snapshot {
            levels,
            book_generation,
            ..
        }) = outcome.accepted_output()
        else {
            panic!("replacement must emit full snapshot");
        };

        assert_eq!(book_generation, 2);
        assert_eq!(levels.len(), 3);

        // This level was unchanged from the previous book,
        // but it must still appear in the new snapshot.
        assert!(levels.iter().any(|(side, price, level)| {
            *side == BookSide::Bid
                && *price == Decimal::from(80_000)
                && level.quantity_base == Some(Decimal::from(3))
        }));

        // Removed levels must not appear as old positive levels
        // or deletion markers in the new full snapshot.
        assert!(
            !levels
                .iter()
                .any(|(_, price, _)| { *price == Decimal::from(80_010) })
        );
    }
    #[test]
    fn failed_depth_operation_has_no_publishable_output() {
        let mut controller = controller("bybit_spot");

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let outcome = controller.process(10, &bybit_delta(101, 199, 5));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired { .. }
        ));

        assert!(outcome.accepted_output().is_none());
        assert!(!controller.is_synchronized());
    }
    #[test]
    fn rest_sync_output_uses_typed_full_snapshot() {
        let mut controller = controller("binance_spot");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, range_delta(101, 105));

        let outcome =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        let Some(AcceptedDepthOutput::Snapshot {
            levels,
            book_generation,
            baseline,
        }) = outcome.accepted_output()
        else {
            panic!("REST bridge must produce full book snapshot");
        };

        assert_eq!(book_generation, 1);
        assert_eq!(baseline.last_sequence, 105);

        assert_eq!(
            levels.len(),
            controller.book().bids().len() + controller.book().asks().len(),
        );
    }
    // ============================================================
    // N5N.7.2 — BINANCE FUTURES BOOTSTRAP
    // ============================================================

    #[test]
    fn futures_equal_snapshot_boundary_does_not_overwrite_book() {
        let mut controller = controller("binance_linear");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        // This update ends exactly at the REST snapshot.
        // Its quantity must not overwrite snapshot state.
        push_buffered(&mut buffer, futures_delta(95, 100, 90));

        // The next event continues from the snapshot.
        push_buffered(&mut buffer, futures_delta(101, 105, 100));

        let outcome =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        assert!(controller.is_synchronized());

        assert_eq!(controller.baseline().unwrap().last_sequence, 105,);
    }

    #[test]
    fn futures_equal_boundary_alone_does_not_complete_bridge() {
        let mut controller = controller("binance_linear");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, futures_delta(95, 100, 90));

        let outcome =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotBridgeFailed
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
    }

    #[test]
    fn futures_bridge_rejects_missing_continuation() {
        let mut controller = controller("binance_linear");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, futures_delta(110, 115, 109));

        let outcome =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::SequenceGap
                )
            }
        ));

        assert!(!controller.is_synchronized());
    }

    #[test]
    fn coin_m_futures_uses_same_safe_snapshot_boundary() {
        let mut controller = controller("binance_inverse");
        let mut buffer = rest_buffer();

        begin_rest(&mut controller);

        push_buffered(&mut buffer, futures_delta(95, 100, 90));

        push_buffered(&mut buffer, futures_delta(101, 105, 100));

        let outcome =
            controller.complete_rest_snapshot(rest_token(), &mut buffer, &rest_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        assert_eq!(controller.baseline().unwrap().last_sequence, 105,);
    }
    // ============================================================
    // N5N.7.2 — BYBIT SUBSCRIPTION DEPTH
    // ============================================================

    #[test]
    fn bybit_depth_one_rejects_delta() {
        let mut controller = LiveDepthController::new_with_subscription_depth(
            "stream-42",
            plan("bybit_spot"),
            10,
            1,
        )
        .unwrap();

        // The test helper creates messages with declared_depth=200.
        // Construct depth-1 messages explicitly.
        let snapshot = NormalizedDepth::new(
            "stream-42",
            NormalizationSource::WebSocket,
            DepthMessageKind::Snapshot,
            sequence(None, Some(100), None, Some(200)),
            vec![level(BookSide::Bid, 80_000, 3)],
            Some(1_000_000_000),
            Some(1),
        )
        .unwrap();

        assert!(matches!(
            controller.process(10, &snapshot),
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ));

        let delta = NormalizedDepth::new(
            "stream-42",
            NormalizationSource::WebSocket,
            DepthMessageKind::Delta,
            sequence(None, Some(101), None, Some(201)),
            vec![level(BookSide::Bid, 80_000, 5)],
            Some(1_000_000_000),
            Some(1),
        )
        .unwrap();

        let outcome = controller.process(10, &delta);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSequence(
                    DepthViolationReason::UnsupportedMetadata
                )
            }
        ));

        assert!(!controller.is_synchronized());
    }

    #[test]
    fn bybit_depth_one_accepts_repeated_snapshot_id() {
        let mut controller = LiveDepthController::new_with_subscription_depth(
            "stream-42",
            plan("bybit_spot"),
            10,
            1,
        )
        .unwrap();

        let make_snapshot = |quantity| {
            NormalizedDepth::new(
                "stream-42",
                NormalizationSource::WebSocket,
                DepthMessageKind::Snapshot,
                sequence(None, Some(100), None, Some(200)),
                vec![level(BookSide::Bid, 80_000, quantity)],
                Some(1_000_000_000),
                Some(1),
            )
            .unwrap()
        };

        let first = controller.process(10, &make_snapshot(3));

        assert!(matches!(
            first,
            DepthControllerOutcome::Accepted {
                snapshot: true,
                book_generation: 1,
                ..
            }
        ));

        let second = controller.process(10, &make_snapshot(3));

        assert!(matches!(
            second,
            DepthControllerOutcome::Accepted {
                snapshot: true,
                book_generation: 2,
                ..
            }
        ));

        assert!(controller.is_synchronized());
    }

    #[test]
    fn bybit_rejects_subscription_depth_mismatch() {
        let mut controller = LiveDepthController::new_with_subscription_depth(
            "stream-42",
            plan("bybit_spot"),
            10,
            1,
        )
        .unwrap();

        // The existing helper declares depth=200.
        let outcome = controller.process(10, &bybit_snapshot(100, 200, 3));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(_)
            }
        ));

        assert!(!controller.is_synchronized());
    }

    #[test]
    fn bybit_unbound_delta_with_unknown_depth_fails_closed() {
        let mut controller = controller("bybit_spot");

        // Snapshot declares its depth, but the controller
        // has not bound the subscription.
        controller.process(10, &bybit_snapshot(100, 200, 3));

        let delta = NormalizedDepth::new(
            "stream-42",
            NormalizationSource::WebSocket,
            DepthMessageKind::Delta,
            sequence(None, Some(101), None, Some(201)),
            vec![level(BookSide::Bid, 80_000, 5)],
            Some(1_000_000_000),
            None,
        )
        .unwrap();

        let outcome = controller.process(10, &delta);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(_)
            }
        ));

        assert!(!controller.is_synchronized());
    }

    #[test]
    fn bybit_bound_depth_accepts_delta_without_message_depth() {
        let mut controller = LiveDepthController::new_with_subscription_depth(
            "stream-42",
            plan("bybit_spot"),
            10,
            200,
        )
        .unwrap();

        controller.process(10, &bybit_snapshot(100, 200, 3));

        let delta = NormalizedDepth::new(
            "stream-42",
            NormalizationSource::WebSocket,
            DepthMessageKind::Delta,
            sequence(None, Some(101), None, Some(201)),
            vec![level(BookSide::Bid, 80_000, 5)],
            Some(1_000_000_000),
            None,
        )
        .unwrap();

        assert!(matches!(
            controller.process(10, &delta),
            DepthControllerOutcome::Accepted {
                snapshot: false,
                ..
            }
        ));
    }
    // ============================================================
    // N5N.7.4 — RESOURCE SAFETY
    // ============================================================

    #[test]
    fn rejects_oversized_bitget_snapshot() {
        let mut controller = controller("bitget_spot");

        controller
            .set_resource_limits(DepthResourceLimits {
                max_levels_per_message: 10,
                max_pending_snapshot_levels: 1,
                max_pending_snapshot_age: std::time::Duration::from_secs(30),
            })
            .unwrap();

        // Existing fixture contains one bid and one ask.
        let outcome = controller.process(10, &bitget_snapshot(100));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidSnapshot(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
        assert!(controller.pending_bitget_snapshot.is_none());
    }

    #[test]
    fn oversized_delta_invalidates_synchronized_book() {
        let mut controller = controller("bybit_spot");

        controller
            .set_resource_limits(DepthResourceLimits {
                max_levels_per_message: 1,
                max_pending_snapshot_levels: 10,
                max_pending_snapshot_age: std::time::Duration::from_secs(30),
            })
            .unwrap();

        // One level is within the configured limit.
        let initial = depth(
            DepthMessageKind::Snapshot,
            sequence(None, Some(100), None, Some(200)),
            vec![level(BookSide::Bid, 80_000, 3)],
        );

        assert!(matches!(
            controller.process(10, &initial),
            DepthControllerOutcome::Accepted { .. }
        ));

        let oversized = depth(
            DepthMessageKind::Delta,
            sequence(None, Some(101), None, Some(201)),
            vec![
                level(BookSide::Bid, 80_000, 5),
                level(BookSide::Ask, 80_010, 2),
            ],
        );

        let outcome = controller.process(10, &oversized);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::InvalidBook(_)
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(!controller.book().is_initialized());
    }

    #[test]
    fn expired_bitget_snapshot_cannot_bridge() {
        let mut controller = controller("bitget_spot");

        controller
            .set_resource_limits(DepthResourceLimits {
                max_levels_per_message: 10,
                max_pending_snapshot_levels: 10,
                max_pending_snapshot_age: std::time::Duration::from_secs(1),
            })
            .unwrap();

        controller.process(10, &bitget_snapshot(100));

        // Simulate a snapshot staged in the past.
        //
        // The private pending field is accessible because
        // this test belongs to depth_controller.rs.
        controller
            .pending_bitget_snapshot
            .as_mut()
            .unwrap()
            .staged_at = Instant::now() - std::time::Duration::from_secs(2);

        let outcome = controller.process(10, &bitget_delta(99, 105, 7));

        assert!(matches!(
            outcome,
            DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotBridgeFailed
            }
        ));

        assert!(!controller.is_synchronized());
        assert!(controller.pending_bitget_snapshot.is_none());
    }

    #[test]
    fn timer_expires_abandoned_bitget_snapshot() {
        let mut controller = controller("bitget_spot");

        controller
            .set_resource_limits(DepthResourceLimits {
                max_levels_per_message: 10,
                max_pending_snapshot_levels: 10,
                max_pending_snapshot_age: std::time::Duration::from_secs(1),
            })
            .unwrap();

        controller.process(10, &bitget_snapshot(100));

        controller
            .pending_bitget_snapshot
            .as_mut()
            .unwrap()
            .staged_at = Instant::now() - std::time::Duration::from_secs(2);

        let outcome = controller.expire_pending_snapshot();

        assert!(matches!(
            outcome,
            Some(DepthControllerOutcome::RecoveryRequired {
                reason: DepthControllerRecoveryReason::SnapshotBridgeFailed
            })
        ));

        assert!(controller.pending_bitget_snapshot.is_none());

        // Repeated timer ticks cannot report the same
        // expiration more than once.
        assert!(controller.expire_pending_snapshot().is_none());
    }

    #[test]
    fn active_resource_policy_cannot_be_changed() {
        let mut controller = controller("bitget_spot");

        controller.process(10, &bitget_snapshot(100));

        assert!(
            controller
                .set_resource_limits(DepthResourceLimits::default())
                .is_err()
        );

        assert!(controller.pending_bitget_snapshot.is_some());
    }
}
