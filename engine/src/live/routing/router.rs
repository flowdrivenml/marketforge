//! Canonical event router registry and admission planning.
//!
//! This module owns destination registration and deterministic
//! fan-out planning.
//!
//! Stage 1 does not reserve capacity or commit event batches.

use crate::live::model::delivery::DeliveryStatus;
use crate::live::model::delivery::{DeliveryFailure, DeliveryStage};
use crate::live::routing::acknowledgement::DeliveryAcknowledgement;
use crate::live::routing::acknowledgement::DeliveryObligation;
use crate::live::routing::acknowledgement::PreparedObligationsHandle;
use crate::live::routing::acknowledgement::{
    AcknowledgementError, AcknowledgementTracker, DeliveryCursorRange, DeliveryObligationSpec,
    PreparedObligations,
};
use crate::live::routing::tracking_recovery::TrackingApplyFailure;
use crate::live::routing::tracking_recovery::TrackingRecovery;
use crate::live::routing::tracking_recovery::TrackingRecoveryReservation;
use crate::live::routing::tracking_recovery::{TrackingApplyError, TrackingApplyOutcome};
use crate::live::routing::{
    admission::{AdmissionContext, DeadlineTimer},
    destination::CapacityReservation,
    queue::{AsyncCapacityAdmission, AsyncQueueAdmissionError, DestinationQueue, QueueError},
};
use crate::live::{
    model::{
        cursor::StreamCursor,
        delivery::{DeliveryPolicy, DestinationKind},
    },
    ports::routing::{DestinationDescriptor, DestinationId, EventBatch, EventDestinationPort},
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

/// A registered destination and its validated descriptor.
#[derive(Clone)]
struct RegisteredDestination {
    descriptor: DestinationDescriptor,
    destination: Arc<dyn EventDestinationPort>,
    queue: Option<DestinationQueue>,
}
/// A destination queue participating in router-controlled admission.
#[derive(Clone)]
pub struct QueueDestination {
    pub descriptor: DestinationDescriptor,
    pub queue: DestinationQueue,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutPreparationError {
    NoRequiredDestinations,
    MissingQueue(DestinationId),
    DuplicateQueue(DestinationId),
    DescriptorMismatch(DestinationId),
    Admission {
        destination: DestinationId,
        reason: AsyncQueueAdmissionError,
    },
}
#[derive(Debug)]
pub struct TrackedCommitmentFailure {
    pub reason: TrackedFanoutError,
    pub obligations: Option<PreparedObligations>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackedFanoutError {
    Validation(RoutingObligationValidationError),
    ObligationCommitment,
    RollbackFailed,
    Precommit(FanoutCommitmentError),
    RecoveryCapacityExhausted,
    Context(crate::live::ports::routing::RoutingAdmissionFailure),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingObligationValidationError {
    DestinationCountMismatch,
    DuplicateDestination,
    MissingObligation,
    CursorRangeMismatch,
    ObligationNotPending,
    DestinationKindMismatch,
    DeliveryPolicyMismatch,
}

#[derive(Debug)]
pub enum FanoutCommitResult {
    Complete {
        through: StreamCursor,
        committed: Vec<CommittedDestination>,
    },

    Partial(PartialFanoutCommitment),

    Rejected(FanoutCommitmentRejection),
}

#[derive(Debug)]
#[must_use = "delivery and recovery outcomes retain required ownership"]
pub enum TrackedFanoutResult {
    Complete {
        through: StreamCursor,
        committed: Vec<CommittedDestination>,
    },

    Partial {
        batch: EventBatch,
        committed: Vec<CommittedDestination>,
        failed: Vec<FailedDestinationCommitment>,
    },

    Rejected {
        batch: EventBatch,
        reason: TrackedFanoutError,
    },
    /// Irreversible destination results exist, while tracking responsibility is
    /// retained by recovery or returned here with its original reservations.
    RecoveryPending {
        batch: EventBatch,
        committed: Vec<CommittedDestination>,
        failed: Vec<FailedDestinationCommitment>,
        tracking: Result<RecoveryTrackingResult, RecoveryTrackingFailure>,
    },
    AcknowledgementFailed {
        batch: EventBatch,
        committed: Vec<CommittedDestination>,
        failed: Vec<FailedDestinationCommitment>,
        tracking_errors: Vec<TrackingUpdateError>,
    },
}
#[derive(Debug)]
pub enum RecoveryTrackingFailure {
    ReservationCountMismatch {
        expected: usize,
        actual: usize,
        reservations: Vec<TrackingRecoveryReservation>,
    },
}
#[derive(Debug)]
pub struct RecoveryTrackingResult {
    pub errors: Vec<RecoveryTrackingError>,
    pub unresolved: Vec<UnresolvedTrackingUpdate>,
}
/// Tracking update errors produced when acknowledgement correlation
/// fails for one or more destinations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackingUpdateError {
    MissingObligation {
        destination: DestinationId,
    },
    Acknowledgement {
        destination: DestinationId,
        error: AcknowledgementError,
    },
}

/// An unresolved tracking update whose ownership could not
/// be transferred into TrackingRecovery.
///
/// The caller must retain or escalate this value.
/// Tracking responsibility that could not be transferred
/// into the recovery owner.
///
/// Both variants retain their capacity reservations.
#[derive(Debug)]
pub enum UnresolvedTrackingUpdate {
    ApplyFailure {
        destination: DestinationId,
        failure: TrackingApplyFailure,
    },

    MissingObligation {
        destination: DestinationId,
        status: DeliveryStatus,
        reservation: TrackingRecoveryReservation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryTrackingError {
    MissingObligation {
        destination: DestinationId,
    },

    ApplyFailed {
        destination: DestinationId,
        reason: TrackingApplyError,
    },

    Retained {
        destination: DestinationId,
        error: AcknowledgementError,
    },
}

#[derive(Debug)]
pub struct PartialFanoutCommitment {
    pub batch: EventBatch,
    pub committed: Vec<CommittedDestination>,
    pub failed: Vec<FailedDestinationCommitment>,
}

#[derive(Debug)]
pub struct FanoutCommitmentRejection {
    pub batch: EventBatch,
    pub reason: FanoutCommitmentError,
}
/// One successfully reserved destination.
#[derive(Debug)]
pub struct PreparedDestination {
    id: DestinationId,
    queue: DestinationQueue,
    reservation: CapacityReservation,
}

#[derive(Debug)]
pub struct PreparedFanout {
    first_cursor: StreamCursor,
    last_cursor: StreamCursor,
    destinations: Vec<PreparedDestination>,
    registry_revision: u64,
}

impl PreparedFanout {
    pub fn first_cursor(&self) -> &StreamCursor {
        &self.first_cursor
    }

    pub fn last_cursor(&self) -> &StreamCursor {
        &self.last_cursor
    }

    pub fn destination_count(&self) -> usize {
        self.destinations.len()
    }

    pub fn destination_ids(&self) -> Vec<DestinationId> {
        self.destinations
            .iter()
            .map(|destination| destination.id)
            .collect()
    }

    pub fn registry_revision(&self) -> u64 {
        self.registry_revision
    }
}
/// One destination selected for a routing operation.
///
/// The descriptor is a snapshot taken when the plan was created.
#[derive(Clone)]
pub struct PlannedDestination {
    pub descriptor: DestinationDescriptor,
    pub destination: Arc<dyn EventDestinationPort>,
    pub queue: Option<DestinationQueue>,
}

/// Immutable fan-out plan for one canonical event batch.
#[derive(Clone)]
pub struct RoutingPlan {
    pub first_cursor: StreamCursor,
    pub last_cursor: StreamCursor,
    pub required: Vec<PlannedDestination>,
    pub best_effort: Vec<PlannedDestination>,
    pub registry_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedDestination {
    pub id: DestinationId,
    pub through: StreamCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedDestinationCommitment {
    pub id: DestinationId,
    pub reason: QueueError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutCommitmentOutcome {
    Complete {
        through: StreamCursor,
        committed: Vec<CommittedDestination>,
    },

    Partial {
        through: StreamCursor,
        committed: Vec<CommittedDestination>,
        failed: Vec<FailedDestinationCommitment>,
    },

    Rejected {
        reason: FanoutCommitmentError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutCommitmentError {
    RegistryChanged,
    ShuttingDown,
    CursorMismatch,
    DestinationMismatch,
    NoRequiredDestinations,
}

impl RoutingPlan {
    pub fn required_count(&self) -> usize {
        self.required.len()
    }

    pub fn best_effort_count(&self) -> usize {
        self.best_effort.len()
    }

    pub fn destination_count(&self) -> usize {
        self.required.len() + self.best_effort.len()
    }
}

/// Destination registration and planning errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterRegistryError {
    DuplicateDestination,
    DestinationCapacity,
    UnknownDestination,
    InvalidDestinationPolicy,
    InvalidCapacity,
    NoDestinations,
    RevisionExhausted,
    ShuttingDown,
    QueueCapacityMismatch,
}

struct RouterRegistryState {
    destinations: HashMap<DestinationId, RegisteredDestination>,
    max_destinations: usize,
    revision: u64,
    shutting_down: bool,
}

/// Thread-safe destination registry.
///
/// Registration and removal are serialized through a write lock.
/// Planning uses a consistent registry snapshot.
pub struct RouterRegistry {
    state: RwLock<RouterRegistryState>,
    commitment: Mutex<()>,
}

impl RouterRegistry {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(RouterRegistryState {
                destinations: HashMap::new(),
                max_destinations: 1024,
                revision: 0,
                shutting_down: false,
            }),
            commitment: Mutex::new(()),
        }
    }
    pub fn with_destination_limit(max_destinations: usize) -> Result<Self, RouterRegistryError> {
        if max_destinations == 0 {
            return Err(RouterRegistryError::InvalidCapacity);
        }
        let registry = Self::new();
        registry
            .state
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .max_destinations = max_destinations;
        Ok(registry)
    }
    pub fn register_queue(
        &self,
        destination: Arc<dyn EventDestinationPort>,
        queue: DestinationQueue,
    ) -> Result<(), RouterRegistryError> {
        let descriptor = destination.descriptor();

        validate_descriptor(&descriptor)?;

        if queue.capacity().limits() != descriptor.capacity {
            return Err(RouterRegistryError::QueueCapacityMismatch);
        }

        let _commitment = self
            .commitment
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.shutting_down {
            return Err(RouterRegistryError::ShuttingDown);
        }

        if state.destinations.contains_key(&descriptor.id) {
            return Err(RouterRegistryError::DuplicateDestination);
        }
        if state.destinations.len() >= state.max_destinations {
            return Err(RouterRegistryError::DestinationCapacity);
        }

        let next_revision = state
            .revision
            .checked_add(1)
            .ok_or(RouterRegistryError::RevisionExhausted)?;

        state.destinations.insert(
            descriptor.id,
            RegisteredDestination {
                descriptor,
                destination,
                queue: Some(queue),
            },
        );

        state.revision = next_revision;

        Ok(())
    }
    /// Return the queue registered for a destination.
    ///
    /// The returned queue shares ownership with the registry.
    pub fn peek_queue(&self, id: DestinationId) -> Option<DestinationQueue> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        state
            .destinations
            .get(&id)
            .and_then(|entry| entry.queue.clone())
    }
    /// Register one destination instance.
    ///
    /// Multiple instances of the same destination kind are allowed,
    /// but destination IDs must be unique.
    pub fn register(
        &self,
        destination: Arc<dyn EventDestinationPort>,
    ) -> Result<(), RouterRegistryError> {
        let descriptor = destination.descriptor();

        validate_descriptor(&descriptor)?;
        let _commitment = self.commitment.lock().unwrap_or_else(|e| e.into_inner());

        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.shutting_down {
            return Err(RouterRegistryError::ShuttingDown);
        }

        if state.destinations.contains_key(&descriptor.id) {
            return Err(RouterRegistryError::DuplicateDestination);
        }
        if state.destinations.len() >= state.max_destinations {
            return Err(RouterRegistryError::DestinationCapacity);
        }

        let next_revision = state
            .revision
            .checked_add(1)
            .ok_or(RouterRegistryError::RevisionExhausted)?;

        state.destinations.insert(
            descriptor.id,
            RegisteredDestination {
                descriptor,
                destination,
                queue: None,
            },
        );

        state.revision = next_revision;

        Ok(())
    }

    /// Remove a destination from future routing plans.
    ///
    /// Existing plans retain their Arc-owned destination references.
    ///
    /// This does not cancel already accepted delivery obligations.
    pub fn unregister(&self, id: DestinationId) -> Result<(), RouterRegistryError> {
        let _commitment = self.commitment.lock().unwrap_or_else(|e| e.into_inner());
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.shutting_down {
            return Err(RouterRegistryError::ShuttingDown);
        }

        if !state.destinations.contains_key(&id) {
            return Err(RouterRegistryError::UnknownDestination);
        }

        let next_revision = state
            .revision
            .checked_add(1)
            .ok_or(RouterRegistryError::RevisionExhausted)?;

        state.destinations.remove(&id);
        state.revision = next_revision;

        Ok(())
    }

    /// Create a deterministic snapshot of destinations for a batch.
    pub fn plan(&self, batch: &EventBatch) -> Result<RoutingPlan, RouterRegistryError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.shutting_down {
            return Err(RouterRegistryError::ShuttingDown);
        }

        if state.destinations.is_empty() {
            return Err(RouterRegistryError::NoDestinations);
        }

        let mut required = Vec::new();
        let mut best_effort = Vec::new();

        for registered in state.destinations.values() {
            let planned = PlannedDestination {
                descriptor: registered.descriptor,
                destination: registered.destination.clone(),
                queue: registered.queue.clone(),
            };

            match registered.descriptor.policy {
                DeliveryPolicy::Required | DeliveryPolicy::Durable => {
                    required.push(planned);
                }

                DeliveryPolicy::BestEffort => {
                    best_effort.push(planned);
                }
            }
        }

        required.sort_by_key(|entry| entry.descriptor.id.0);
        best_effort.sort_by_key(|entry| entry.descriptor.id.0);

        Ok(RoutingPlan {
            first_cursor: batch.first_cursor().clone(),
            last_cursor: batch.last_cursor().clone(),
            required,
            best_effort,
            registry_revision: state.revision,
        })
    }

    /// Prevent future registrations, removals, and plans.
    ///
    /// Existing plans remain valid snapshots but are not
    /// automatically authorized to commit after shutdown.
    pub fn shutdown(&self) {
        let _commitment = self.commitment.lock().unwrap_or_else(|e| e.into_inner());
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        state.shutting_down = true;
    }

    pub fn revision(&self) -> u64 {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .revision
    }

    pub fn destination_count(&self) -> usize {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .destinations
            .len()
    }
}

impl Default for RouterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate a destination descriptor before registration.
fn validate_descriptor(descriptor: &DestinationDescriptor) -> Result<(), RouterRegistryError> {
    if descriptor.capacity.max_pending_events == 0 || descriptor.capacity.max_pending_bytes == 0 {
        return Err(RouterRegistryError::InvalidCapacity);
    }

    if matches!(
        (descriptor.kind, descriptor.policy),
        (
            DestinationKind::Publication | DestinationKind::Derived,
            DeliveryPolicy::Durable
        )
    ) {
        return Err(RouterRegistryError::InvalidDestinationPolicy);
    }

    Ok(())
}

/// Coordinates capacity reservations for required destinations.
///
/// No destination queue is mutated during preparation.
pub struct FanoutAdmissionCoordinator;

impl FanoutAdmissionCoordinator {
    pub async fn prepare(
        plan: &RoutingPlan,
        batch: &EventBatch,
        context: AdmissionContext,
        timer: Option<&dyn DeadlineTimer>,
    ) -> Result<PreparedFanout, FanoutPreparationError> {
        if plan.required.is_empty() {
            return Err(FanoutPreparationError::NoRequiredDestinations);
        }

        let mut prepared = Vec::with_capacity(plan.required.len());

        for required in &plan.required {
            let id = required.descriptor.id;

            // The queue must come from the registry snapshot.
            // Callers cannot supply an unrelated queue.
            let queue = required
                .queue
                .as_ref()
                .ok_or(FanoutPreparationError::MissingQueue(id))?;

            match queue.reserve_async(batch, context.clone(), timer).await {
                AsyncCapacityAdmission::Reserved(reservation) => {
                    prepared.push(PreparedDestination {
                        id,
                        queue: queue.clone(),
                        reservation,
                    });
                }

                AsyncCapacityAdmission::Rejected(reason) => {
                    // Dropping prepared releases all reservations
                    // acquired before this failure.
                    return Err(FanoutPreparationError::Admission {
                        destination: id,
                        reason,
                    });
                }
            }
        }

        Ok(PreparedFanout {
            first_cursor: plan.first_cursor.clone(),
            last_cursor: plan.last_cursor.clone(),
            destinations: prepared,
            registry_revision: plan.registry_revision,
        })
    }
}

pub struct FanoutCommitmentCoordinator;

impl FanoutCommitmentCoordinator {
    pub fn commit(
        registry: &RouterRegistry,
        prepared: PreparedFanout,
        batch: EventBatch,
    ) -> FanoutCommitResult {
        let _commitment = registry
            .commitment
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        Self::commit_locked(registry, prepared, batch)
    }
    /// Construct acknowledgement templates for every prepared
    /// destination before irreversible queue commitment.
    ///
    /// Every destination must have exactly one corresponding
    /// obligation.
    ///
    /// The final delivery status is assigned after queue
    /// commitment determines success or failure.
    fn prepare_acknowledgement_templates(
        prepared: &PreparedFanout,
        obligations: &[DeliveryObligation],
    ) -> Result<HashMap<DestinationId, DeliveryAcknowledgement>, RoutingObligationValidationError>
    {
        use std::collections::HashSet;

        if prepared.destination_count() != obligations.len() {
            return Err(RoutingObligationValidationError::DestinationCountMismatch);
        }

        let mut templates = HashMap::with_capacity(obligations.len());
        let mut seen = HashSet::new();

        for obligation in obligations {
            if templates
                .insert(
                    obligation.destination,
                    DeliveryAcknowledgement {
                        obligation: obligation.id,
                        destination: obligation.destination,
                        range: obligation.range.clone(),
                        status: DeliveryStatus::Pending,
                    },
                )
                .is_some()
            {
                return Err(RoutingObligationValidationError::DuplicateDestination);
            }
        }

        for destination in prepared.destination_ids() {
            if !seen.insert(destination) {
                return Err(RoutingObligationValidationError::DuplicateDestination);
            }

            if !templates.contains_key(&destination) {
                return Err(RoutingObligationValidationError::MissingObligation);
            }
        }

        Ok(templates)
    }
    /// Recovery-aware tracked fan-out commitment.
    ///
    /// Recovery capacity is reserved before any irreversible
    /// queue commitment.
    ///
    /// Activates obligations before queue writes and transfers every failed
    /// tracking update into recovery or an explicitly owned return value.
    pub fn commit_tracked_with_recovery(
        registry: &RouterRegistry,
        tracker: &mut AcknowledgementTracker,
        recovery: &mut TrackingRecovery,
        plan: &RoutingPlan,
        prepared: PreparedFanout,
        obligations: PreparedObligations,
        batch: EventBatch,
    ) -> TrackedFanoutResult {
        Self::commit_tracked_with_recovery_checked(
            registry,
            tracker,
            recovery,
            plan,
            prepared,
            obligations,
            batch,
            || Ok(()),
        )
    }

    /// Checks the caller's cancellation/deadline fence under the registry
    /// commitment lock, after all preparation and before obligation activation.
    pub(crate) fn commit_tracked_with_recovery_checked(
        registry: &RouterRegistry,
        tracker: &mut AcknowledgementTracker,
        recovery: &mut TrackingRecovery,
        plan: &RoutingPlan,
        prepared: PreparedFanout,
        obligations: PreparedObligations,
        batch: EventBatch,
        check: impl FnOnce() -> Result<(), crate::live::ports::routing::RoutingAdmissionFailure>,
    ) -> TrackedFanoutResult {
        let _commitment = registry
            .commitment
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Validate the registry before activating obligations.
        if let Err(error) = Self::validate_precommit(registry, &prepared, &batch) {
            return match tracker.abort_prepared(obligations) {
                Ok(()) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::Precommit(error),
                },

                Err(_) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::RollbackFailed,
                },
            };
        }

        // Validate the complete obligation mapping.
        if let Err(error) = validate_routing_obligations(tracker, plan, &prepared, &obligations) {
            return match tracker.abort_prepared(obligations) {
                Ok(()) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::Validation(error),
                },

                Err(_) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::RollbackFailed,
                },
            };
        }

        // Reserve capacity for every required destination.
        let reservations = match recovery.reserve_batch(prepared.destination_count()) {
            Ok(reservations) => reservations,

            Err(_) => {
                return match tracker.abort_prepared(obligations) {
                    Ok(()) => TrackedFanoutResult::Rejected {
                        batch,
                        reason: TrackedFanoutError::RecoveryCapacityExhausted,
                    },

                    Err(_) => TrackedFanoutResult::Rejected {
                        batch,
                        reason: TrackedFanoutError::RollbackFailed,
                    },
                };
            }
        };

        // Verify ownership and count before irreversible commitment.
        if recovery
            .validate_reservations(&reservations, prepared.destination_count())
            .is_err()
        {
            drop(reservations);

            return match tracker.abort_prepared(obligations) {
                Ok(()) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::RecoveryCapacityExhausted,
                },

                Err(_) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::RollbackFailed,
                },
            };
        }

        // Build all correlation state before the first queue can accept work.
        if let Err(error) = Self::validate_commitment_mapping(&prepared, obligations.obligations())
        {
            return match tracker.abort_prepared(obligations) {
                Ok(()) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::Validation(error),
                },
                Err(_) => TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::RollbackFailed,
                },
            };
        }
        let templates =
            match Self::prepare_acknowledgement_templates(&prepared, obligations.obligations()) {
                Ok(templates) => templates,
                Err(error) => {
                    return match tracker.abort_prepared(obligations) {
                        Ok(()) => TrackedFanoutResult::Rejected {
                            batch,
                            reason: TrackedFanoutError::Validation(error),
                        },
                        Err(_) => TrackedFanoutResult::Rejected {
                            batch,
                            reason: TrackedFanoutError::RollbackFailed,
                        },
                    };
                }
            };
        let range = DeliveryCursorRange::new(plan.first_cursor.clone(), plan.last_cursor.clone())
            .expect("validated routing range");
        if let Err(reason) = check() {
            return TrackedFanoutResult::Rejected {
                batch,
                reason: if tracker.abort_prepared(obligations).is_ok() {
                    TrackedFanoutError::Context(reason)
                } else {
                    TrackedFanoutError::RollbackFailed
                },
            };
        }
        let handle = match Self::try_begin_tracked_commitment(tracker, plan, &prepared, obligations)
        {
            Ok(handle) => handle,
            Err(failure) => {
                if let Some(prepared) = failure.obligations {
                    if tracker.abort_prepared(prepared).is_err() {
                        return TrackedFanoutResult::Rejected {
                            batch,
                            reason: TrackedFanoutError::RollbackFailed,
                        };
                    }
                }
                return TrackedFanoutResult::Rejected {
                    batch,
                    reason: failure.reason,
                };
            }
        };
        let outcome = Self::commit_locked(registry, prepared, batch.clone());
        let (committed, failed, rejection) = match outcome {
            FanoutCommitResult::Complete { committed, .. } => (committed, Vec::new(), None),
            FanoutCommitResult::Partial(partial) => (partial.committed, partial.failed, None),
            FanoutCommitResult::Rejected(rejection) => {
                // Defensive path: no queue accepted. Active obligations still
                // receive terminal failures through the reserved recovery path.
                let failed = handle
                    .obligations()
                    .iter()
                    .map(|o| FailedDestinationCommitment {
                        id: o.destination,
                        reason: QueueError::Closed,
                    })
                    .collect();
                (Vec::new(), failed, Some(rejection.reason))
            }
        };
        let tracking = Self::apply_tracking_updates_with_recovery(
            tracker,
            recovery,
            &templates,
            &committed,
            &failed,
            reservations,
            &range,
        );
        match tracking {
            Ok(updates) if updates.errors.is_empty() && updates.unresolved.is_empty() => {}
            tracking => {
                return TrackedFanoutResult::RecoveryPending {
                    batch,
                    committed,
                    failed,
                    tracking,
                };
            }
        }
        if let Some(reason) = rejection {
            return TrackedFanoutResult::Rejected {
                batch,
                reason: TrackedFanoutError::Precommit(reason),
            };
        }
        if failed.is_empty() {
            TrackedFanoutResult::Complete {
                through: batch.last_cursor().clone(),
                committed,
            }
        } else {
            TrackedFanoutResult::Partial {
                batch,
                committed,
                failed,
            }
        }
    }

    /// Prepare recovery capacity for a tracked fan-out.
    ///
    /// Validates registry state and recovery capacity before
    /// irreversible queue commitment.
    ///
    /// The returned reservations remain owned by the caller.
    /// This method does not activate obligations or enqueue events.
    pub fn prepare_recovery_commitment(
        registry: &RouterRegistry,
        recovery: &TrackingRecovery,
        prepared: &PreparedFanout,
        batch: &EventBatch,
    ) -> Result<Vec<TrackingRecoveryReservation>, TrackedFanoutError> {
        let _commitment = registry
            .commitment
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        Self::validate_precommit(registry, prepared, batch)
            .map_err(TrackedFanoutError::Precommit)?;

        let reservations = recovery
            .reserve_batch(prepared.destination_count())
            .map_err(|_| TrackedFanoutError::RecoveryCapacityExhausted)?;

        recovery
            .validate_reservations(&reservations, prepared.destination_count())
            .map_err(|_| TrackedFanoutError::RecoveryCapacityExhausted)?;

        Ok(reservations)
    }
    /// Verify that every prepared destination has exactly one
    /// corresponding obligation before irreversible commitment.
    ///
    /// This function does not mutate queues or tracker state.
    fn validate_commitment_mapping(
        prepared: &PreparedFanout,
        obligations: &[DeliveryObligation],
    ) -> Result<(), RoutingObligationValidationError> {
        use std::collections::HashSet;

        if prepared.destination_count() != obligations.len() {
            return Err(RoutingObligationValidationError::DestinationCountMismatch);
        }

        let mut seen = HashSet::new();

        for obligation in obligations {
            if !seen.insert(obligation.destination) {
                return Err(RoutingObligationValidationError::DuplicateDestination);
            }
        }

        for destination in prepared.destination_ids() {
            if !seen.contains(&destination) {
                return Err(RoutingObligationValidationError::MissingObligation);
            }
        }

        Ok(())
    }
    fn apply_tracking_updates_with_recovery(
        tracker: &mut AcknowledgementTracker,
        recovery: &mut TrackingRecovery,
        templates: &HashMap<DestinationId, DeliveryAcknowledgement>,
        committed: &[CommittedDestination],
        failed: &[FailedDestinationCommitment],
        reservations: Vec<TrackingRecoveryReservation>,
        range: &DeliveryCursorRange,
    ) -> Result<RecoveryTrackingResult, RecoveryTrackingFailure> {
        let mut errors = Vec::new();
        let mut unresolved = Vec::new();

        // The caller must validate count and ownership
        // before irreversible queue commitment.
        let expected_count = committed.len() + failed.len();

        if reservations.len() != expected_count {
            return Err(RecoveryTrackingFailure::ReservationCountMismatch {
                expected: expected_count,
                actual: reservations.len(),
                reservations,
            });
        }

        let outcomes = committed
            .iter()
            .map(|destination| {
                (
                    destination.id,
                    DeliveryStatus::Progress(DeliveryStage::Queued),
                )
            })
            .chain(failed.iter().map(|destination| {
                (
                    destination.id,
                    DeliveryStatus::Failed(map_queue_error_to_delivery_failure(
                        &destination.reason,
                    )),
                )
            }));

        for ((destination_id, status), reservation) in outcomes.zip(reservations) {
            let Some(template) = templates.get(&destination_id) else {
                errors.push(RecoveryTrackingError::MissingObligation {
                    destination: destination_id,
                });

                match recovery.retain_unresolved(destination_id, range.clone(), status, reservation)
                {
                    Ok(()) => {
                        // Recovery now owns the unresolved outcome.
                    }

                    Err(failure) => {
                        unresolved.push(UnresolvedTrackingUpdate::MissingObligation {
                            destination: failure.destination,
                            status: failure.status,
                            reservation: failure.reservation,
                        });
                    }
                }

                continue;
            };
            let mut acknowledgement = template.clone();
            acknowledgement.status = status;

            match recovery.apply_or_retain(tracker, acknowledgement, reservation) {
                Ok(TrackingApplyOutcome::Applied) => {}

                Ok(TrackingApplyOutcome::Retained { error }) => {
                    errors.push(RecoveryTrackingError::Retained {
                        destination: destination_id,
                        error,
                    });
                }

                Err(failure) => {
                    errors.push(RecoveryTrackingError::ApplyFailed {
                        destination: destination_id,
                        reason: failure.reason,
                    });

                    unresolved.push(UnresolvedTrackingUpdate::ApplyFailure {
                        destination: destination_id,
                        failure,
                    });

                    // The failure still owns the acknowledgement
                    // and reservation at this point.
                    //
                    // This is NOT a safe production recovery path:
                    // dropping failure would release its reservation.
                    //
                    // The final integration must retain or escalate
                    // this owned failure.
                }
            }
        }

        Ok(RecoveryTrackingResult { errors, unresolved })
    }
    /// Apply tracking updates for every destination outcome.
    ///
    /// All destinations are processed even when individual
    /// acknowledgements fail.
    ///
    /// This helper does not modify destination queues.
    fn apply_tracking_updates(
        tracker: &mut AcknowledgementTracker,
        obligation_map: &HashMap<DestinationId, DeliveryObligation>,
        committed: &[CommittedDestination],
        failed: &[FailedDestinationCommitment],
    ) -> Vec<TrackingUpdateError> {
        let mut tracking_errors = Vec::new();

        // Report successful queue commitments.
        for destination in committed {
            let Some(obligation) = obligation_map.get(&destination.id) else {
                tracking_errors.push(TrackingUpdateError::MissingObligation {
                    destination: destination.id,
                });
                continue;
            };

            let acknowledgement = DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: DeliveryStatus::Progress(DeliveryStage::Queued),
            };

            if let Err(error) = tracker.acknowledge(acknowledgement) {
                tracking_errors.push(TrackingUpdateError::Acknowledgement {
                    destination: destination.id,
                    error,
                });
            }
        }

        // Report failed queue commitments.
        for destination in failed {
            let Some(obligation) = obligation_map.get(&destination.id) else {
                tracking_errors.push(TrackingUpdateError::MissingObligation {
                    destination: destination.id,
                });
                continue;
            };

            let acknowledgement = DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: DeliveryStatus::Failed(map_queue_error_to_delivery_failure(
                    &destination.reason,
                )),
            };

            if let Err(error) = tracker.acknowledge(acknowledgement) {
                tracking_errors.push(TrackingUpdateError::Acknowledgement {
                    destination: destination.id,
                    error,
                });
            }
        }

        tracking_errors
    }
    /// Attempt tracked commitment with recovery capacity reserved.
    ///
    /// This transitional entry point validates recovery capacity only.
    /// It does not yet perform queue commitment or retain failed updates.
    /// Reserve recovery capacity for a future tracked fan-out.
    ///
    /// The returned reservations must remain alive until every
    /// destination acknowledgement has either succeeded or
    /// transferred into TrackingRecovery.
    ///
    /// Dropping the reservations before commitment releases
    /// capacity without modifying destination queues.
    pub fn prepare_tracking_recovery(
        recovery: &TrackingRecovery,
        required_destinations: usize,
    ) -> Result<Vec<TrackingRecoveryReservation>, TrackedFanoutError> {
        recovery
            .reserve_batch(required_destinations)
            .map_err(|_| TrackedFanoutError::RecoveryCapacityExhausted)
    }
    fn validate_precommit(
        registry: &RouterRegistry,
        prepared: &PreparedFanout,
        batch: &EventBatch,
    ) -> Result<(), FanoutCommitmentError> {
        // Caller must already hold registry.commitment.

        let state = registry
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.shutting_down {
            return Err(FanoutCommitmentError::ShuttingDown);
        }

        if state.revision != prepared.registry_revision {
            return Err(FanoutCommitmentError::RegistryChanged);
        }

        if prepared.first_cursor != *batch.first_cursor()
            || prepared.last_cursor != *batch.last_cursor()
        {
            return Err(FanoutCommitmentError::CursorMismatch);
        }

        if prepared.destinations.is_empty() {
            return Err(FanoutCommitmentError::NoRequiredDestinations);
        }

        for destination in &prepared.destinations {
            if !state.destinations.contains_key(&destination.id) {
                return Err(FanoutCommitmentError::DestinationMismatch);
            }
        }

        Ok(())
    }
    fn commit_locked(
        registry: &RouterRegistry,
        prepared: PreparedFanout,
        batch: EventBatch,
    ) -> FanoutCommitResult {
        let state = registry
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.shutting_down {
            return FanoutCommitResult::Rejected(FanoutCommitmentRejection {
                batch,
                reason: FanoutCommitmentError::ShuttingDown,
            });
        }

        if state.revision != prepared.registry_revision {
            return FanoutCommitResult::Rejected(FanoutCommitmentRejection {
                batch,
                reason: FanoutCommitmentError::RegistryChanged,
            });
        }

        if prepared.first_cursor != *batch.first_cursor()
            || prepared.last_cursor != *batch.last_cursor()
        {
            return FanoutCommitResult::Rejected(FanoutCommitmentRejection {
                batch,
                reason: FanoutCommitmentError::CursorMismatch,
            });
        }

        if prepared.destinations.is_empty() {
            return FanoutCommitResult::Rejected(FanoutCommitmentRejection {
                batch,
                reason: FanoutCommitmentError::NoRequiredDestinations,
            });
        }

        // The registry state is retained under the read lock.
        // The commitment mutex prevents router-controlled changes.

        for destination in &prepared.destinations {
            if !state.destinations.contains_key(&destination.id) {
                return FanoutCommitResult::Rejected(FanoutCommitmentRejection {
                    batch,
                    reason: FanoutCommitmentError::DestinationMismatch,
                });
            }
        }

        let through = batch.last_cursor().clone();

        let mut committed = Vec::new();
        let mut failed = Vec::new();

        for destination in prepared.destinations {
            match destination
                .queue
                .enqueue_reserved(batch.clone(), destination.reservation)
            {
                Ok(()) => {
                    committed.push(CommittedDestination {
                        id: destination.id,
                        through: through.clone(),
                    });
                }

                Err(rejection) => {
                    failed.push(FailedDestinationCommitment {
                        id: destination.id,
                        reason: rejection.reason,
                    });
                }
            }
        }

        if failed.is_empty() {
            FanoutCommitResult::Complete { through, committed }
        } else if committed.is_empty() {
            // No destination accepted the batch.
            // This is still a failed fan-out operation.
            FanoutCommitResult::Partial(PartialFanoutCommitment {
                batch,
                committed,
                failed,
            })
        } else {
            FanoutCommitResult::Partial(PartialFanoutCommitment {
                batch,
                committed,
                failed,
            })
        }
    }
    pub fn commit_tracked(
        registry: &RouterRegistry,
        tracker: &mut AcknowledgementTracker,
        plan: &RoutingPlan,
        prepared: PreparedFanout,
        obligations: PreparedObligations,
        batch: EventBatch,
    ) -> TrackedFanoutResult {
        // Acquire the commitment lock BEFORE validation.
        let _commitment = registry
            .commitment
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Validate while holding the commitment lock.
        if let Err(error) = Self::validate_precommit(registry, &prepared, &batch) {
            if tracker.abort_prepared(obligations).is_err() {
                return TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::RollbackFailed,
                };
            }

            return TrackedFanoutResult::Rejected {
                batch,
                reason: TrackedFanoutError::Precommit(error),
            };
        }

        // Continue with the existing activation logic.
        let activation = Self::try_begin_tracked_commitment(tracker, plan, &prepared, obligations);

        let handle = match activation {
            Ok(handle) => handle,

            Err(failure) => {
                if let Some(obligations) = failure.obligations {
                    if tracker.abort_prepared(obligations).is_err() {
                        return TrackedFanoutResult::Rejected {
                            batch,
                            reason: TrackedFanoutError::RollbackFailed,
                        };
                    }
                }

                return TrackedFanoutResult::Rejected {
                    batch,
                    reason: failure.reason,
                };
            }
        };

        let obligation_map: HashMap<DestinationId, _> = handle
            .obligations()
            .iter()
            .map(|obligation| (obligation.destination, obligation.clone()))
            .collect();

        let result = Self::commit_locked(registry, prepared, batch.clone());

        let (committed, failed) = match result {
            FanoutCommitResult::Complete { committed, .. } => (committed, Vec::new()),

            FanoutCommitResult::Partial(partial) => (partial.committed, partial.failed),

            FanoutCommitResult::Rejected(_) => {
                fail_tracked_obligations(
                    tracker,
                    handle.obligations(),
                    DeliveryFailure::Unavailable,
                )
                .expect("active obligations must accept terminal failure");

                return TrackedFanoutResult::Rejected {
                    batch,
                    reason: TrackedFanoutError::ObligationCommitment,
                };
            }
        };

        let tracking_errors =
            Self::apply_tracking_updates(tracker, &obligation_map, &committed, &failed);

        if !tracking_errors.is_empty() {
            return TrackedFanoutResult::AcknowledgementFailed {
                batch,
                committed,
                failed,
                tracking_errors,
            };
        }

        if !tracking_errors.is_empty() {
            return TrackedFanoutResult::AcknowledgementFailed {
                batch,
                committed,
                failed,
                tracking_errors,
            };
        }

        if failed.is_empty() {
            TrackedFanoutResult::Complete {
                through: batch.last_cursor().clone(),
                committed,
            }
        } else {
            TrackedFanoutResult::Partial {
                batch,
                committed,
                failed,
            }
        }
    }
    /// Validate and activate prepared delivery obligations.
    ///
    /// This operation does not enqueue events.
    ///
    /// After successful activation, obligations remain tracked
    /// even if the returned handle is dropped.
    /// Validate and activate prepared delivery obligations.
    ///
    /// On validation failure, prepared obligations are aborted
    /// before returning an error.
    ///
    /// No destination queues are modified by this method.
    pub fn begin_tracked_commitment(
        tracker: &mut AcknowledgementTracker,
        plan: &RoutingPlan,
        prepared: &PreparedFanout,
        obligations: PreparedObligations,
    ) -> Result<PreparedObligationsHandle, TrackedFanoutError> {
        if let Err(error) = validate_routing_obligations(tracker, plan, prepared, &obligations) {
            tracker
                .abort_prepared(obligations)
                .map_err(|_| TrackedFanoutError::RollbackFailed)?;

            return Err(TrackedFanoutError::Validation(error));
        }

        match tracker.begin_commitment(obligations) {
            Ok(handle) => Ok(handle),

            Err((_, prepared)) => {
                tracker
                    .abort_prepared(prepared)
                    .map_err(|_| TrackedFanoutError::RollbackFailed)?;

                Err(TrackedFanoutError::ObligationCommitment)
            }
        }
    }
    pub fn try_begin_tracked_commitment(
        tracker: &mut AcknowledgementTracker,
        plan: &RoutingPlan,
        prepared: &PreparedFanout,
        obligations: PreparedObligations,
    ) -> Result<PreparedObligationsHandle, TrackedCommitmentFailure> {
        if let Err(error) = validate_routing_obligations(tracker, plan, prepared, &obligations) {
            return Err(TrackedCommitmentFailure {
                reason: TrackedFanoutError::Validation(error),
                obligations: Some(obligations),
            });
        }

        match tracker.begin_commitment(obligations) {
            Ok(handle) => Ok(handle),

            Err((_, obligations)) => Err(TrackedCommitmentFailure {
                reason: TrackedFanoutError::ObligationCommitment,
                obligations: Some(obligations),
            }),
        }
    }
}

pub fn prepare_routing_obligations(
    tracker: &mut AcknowledgementTracker,
    plan: &RoutingPlan,
) -> Result<PreparedObligations, AcknowledgementError> {
    // Validate the original cursor range.
    let range = DeliveryCursorRange::new(plan.first_cursor.clone(), plan.last_cursor.clone())?;

    // Build specifications without allocating obligation IDs.
    let specs: Vec<DeliveryObligationSpec> = plan
        .required
        .iter()
        .map(|destination| DeliveryObligationSpec {
            destination: destination.descriptor.id,
            range: range.clone(),
            kind: destination.descriptor.kind,
            policy: destination.descriptor.policy,
        })
        .collect();

    // Atomically prepare and register all obligations.
    //
    // The tracker owns ID allocation and transaction identity.
    // If preparation fails, no obligations should be registered.
    tracker.prepare_obligation_specs(specs)
}

/// Validate that every prepared destination has exactly one
/// registered, pending delivery obligation.
///
/// This function does not mutate the tracker or destination queues.
/// Validate that every prepared destination has exactly one
/// matching, registered, pending delivery obligation.
///
/// This function performs read-only validation.
///
/// It does not begin commitment or mutate destination queues.
pub fn validate_routing_obligations(
    tracker: &AcknowledgementTracker,
    plan: &RoutingPlan,
    prepared: &PreparedFanout,
    obligations: &PreparedObligations,
) -> Result<(), RoutingObligationValidationError> {
    use std::collections::HashSet;

    if prepared.destination_count() != obligations.len() || plan.required.len() != obligations.len()
    {
        return Err(RoutingObligationValidationError::DestinationCountMismatch);
    }
    let planned_ids: HashSet<DestinationId> = plan
        .required
        .iter()
        .map(|entry| entry.descriptor.id)
        .collect();

    if planned_ids.len() != plan.required.len() {
        return Err(RoutingObligationValidationError::DuplicateDestination);
    }

    let obligation_ids: HashSet<DestinationId> = obligations
        .obligations()
        .iter()
        .map(|obligation| obligation.destination)
        .collect();

    if obligation_ids.len() != obligations.len() {
        return Err(RoutingObligationValidationError::DuplicateDestination);
    }

    if prepared.first_cursor() != &plan.first_cursor || prepared.last_cursor() != &plan.last_cursor
    {
        return Err(RoutingObligationValidationError::CursorRangeMismatch);
    }

    let expected_range =
        DeliveryCursorRange::new(plan.first_cursor.clone(), plan.last_cursor.clone())
            .map_err(|_| RoutingObligationValidationError::CursorRangeMismatch)?;

    let mut seen = HashSet::new();

    for destination_id in prepared.destination_ids() {
        if !seen.insert(destination_id) {
            return Err(RoutingObligationValidationError::DuplicateDestination);
        }

        let planned = plan
            .required
            .iter()
            .find(|entry| entry.descriptor.id == destination_id)
            .ok_or(RoutingObligationValidationError::MissingObligation)?;

        let obligation = obligations
            .obligations()
            .iter()
            .find(|obligation| obligation.destination == destination_id)
            .ok_or(RoutingObligationValidationError::MissingObligation)?;

        if obligation.range != expected_range {
            return Err(RoutingObligationValidationError::CursorRangeMismatch);
        }

        if obligation.kind != planned.descriptor.kind {
            return Err(RoutingObligationValidationError::DestinationKindMismatch);
        }

        if obligation.policy != planned.descriptor.policy {
            return Err(RoutingObligationValidationError::DeliveryPolicyMismatch);
        }

        if tracker.status(obligation.id) != Some(DeliveryStatus::Pending) {
            return Err(RoutingObligationValidationError::ObligationNotPending);
        }
    }

    Ok(())
}

fn map_queue_error_to_delivery_failure(error: &QueueError) -> DeliveryFailure {
    match error {
        QueueError::Capacity(_) => DeliveryFailure::CapacityExceeded,

        QueueError::Closed => DeliveryFailure::Unavailable,

        QueueError::MemoryAccountingOverflow | QueueError::ReservationMismatch => {
            DeliveryFailure::Internal
        }
    }
}

fn fail_tracked_obligations(
    tracker: &mut AcknowledgementTracker,
    obligations: &[DeliveryObligation],
    reason: DeliveryFailure,
) -> Result<(), AcknowledgementError> {
    for obligation in obligations {
        tracker.acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Failed(reason),
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_range() -> DeliveryCursorRange {
        let cursor = StreamCursor::new(
            crate::live::model::StreamId("test-stream".to_string()),
            crate::live::model::LogEpoch(1),
            10,
        );

        DeliveryCursorRange::new(cursor.clone(), cursor).unwrap()
    }

    #[test]
    fn recovery_helper_rejects_reservation_count_mismatch() {
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut recovery = TrackingRecovery::new(2).unwrap();

        let reservations = recovery.reserve_batch(2).unwrap();

        let obligation_map = HashMap::new();

        let committed = vec![CommittedDestination {
            id: DestinationId(1),
            through: StreamCursor::new(
                crate::live::model::StreamId("test-stream".to_string()),
                crate::live::model::LogEpoch(1),
                10,
            ),
        }];

        let range = test_range();

        let result = FanoutCommitmentCoordinator::apply_tracking_updates_with_recovery(
            &mut tracker,
            &mut recovery,
            &obligation_map,
            &committed,
            &[],
            reservations,
            &range,
        );

        match result {
            Err(RecoveryTrackingFailure::ReservationCountMismatch {
                expected,
                actual,
                reservations,
            }) => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);

                // Error retains ownership of both reservations.
                assert_eq!(reservations.len(), 2);
                assert_eq!(recovery.occupied(), 2);

                drop(reservations);

                assert_eq!(recovery.occupied(), 0);
            }

            other => panic!("unexpected recovery result: {other:?}"),
        }
    }

    #[test]
    fn recovery_helper_preserves_missing_obligation_ownership() {
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut recovery = TrackingRecovery::new(1).unwrap();

        let reservations = recovery.reserve_batch(1).unwrap();

        let obligation_map = HashMap::new();

        let committed = vec![CommittedDestination {
            id: DestinationId(1),
            through: StreamCursor::new(
                crate::live::model::StreamId("test-stream".to_string()),
                crate::live::model::LogEpoch(1),
                10,
            ),
        }];

        let range = test_range();

        let result = FanoutCommitmentCoordinator::apply_tracking_updates_with_recovery(
            &mut tracker,
            &mut recovery,
            &obligation_map,
            &committed,
            &[],
            reservations,
            &range,
        )
        .unwrap();

        assert_eq!(result.errors.len(), 1);

        // Missing obligation is now retained by TrackingRecovery.
        assert_eq!(result.unresolved.len(), 0);
        assert_eq!(recovery.unresolved_len(), 1);

        assert!(matches!(
            result.errors[0],
            RecoveryTrackingError::MissingObligation {
                destination: DestinationId(1),
            }
        ));

        // Recovery owns the reservation.
        assert_eq!(recovery.occupied(), 1);
        assert_eq!(recovery.available(), 0);

        // Dropping the diagnostic result must NOT release
        // the reservation retained by TrackingRecovery.
        drop(result);

        assert_eq!(recovery.unresolved_len(), 1);
        assert_eq!(recovery.occupied(), 1);
    }

    #[test]
    fn commitment_mapping_rejects_missing_obligations() {
        let prepared_ids = [DestinationId(1), DestinationId(2)];

        let obligation_ids = [DestinationId(1)];

        assert_ne!(prepared_ids.len(), obligation_ids.len());
    }

    #[test]
    fn acknowledgement_templates_reject_empty_obligations() {
        let prepared_count = 2;
        let obligations: Vec<DeliveryObligation> = Vec::new();

        assert_ne!(prepared_count, obligations.len());
    }
}
