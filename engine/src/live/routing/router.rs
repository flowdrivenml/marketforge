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
    AcknowledgementFailed {
        batch: EventBatch,
        committed: Vec<CommittedDestination>,
        failed: Vec<FailedDestinationCommitment>,
        acknowledgement_errors: Vec<(DestinationId, AcknowledgementError)>,
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
                revision: 0,
                shutting_down: false,
            }),
            commitment: Mutex::new(()),
        }
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
        let mut acknowledgement_errors = Vec::new();

        // Report successful queue commitments.
        for destination in &committed {
            let Some(obligation) = obligation_map.get(&destination.id) else {
                // This should be impossible after successful validation,
                // but do not panic after irreversible queue commitment.
                continue;
            };

            if let Err(error) = tracker.acknowledge(DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: DeliveryStatus::Progress(DeliveryStage::Queued),
            }) {
                acknowledgement_errors.push((destination.id, error));
            }
        }

        // Report failed queue commitments.
        for destination in &failed {
            let Some(obligation) = obligation_map.get(&destination.id) else {
                continue;
            };

            if let Err(error) = tracker.acknowledge(DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: DeliveryStatus::Failed(map_queue_error_to_delivery_failure(
                    &destination.reason,
                )),
            }) {
                acknowledgement_errors.push((destination.id, error));
            }
        }

        // All destination acknowledgements have been attempted.
        if !acknowledgement_errors.is_empty() {
            return TrackedFanoutResult::AcknowledgementFailed {
                batch,
                committed,
                failed,
                acknowledgement_errors,
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
