use crate::live::model::delivery::DeliveryCapacity;
use crate::live::routing::acknowledgement::AcknowledgementError;
use crate::live::routing::acknowledgement::AcknowledgementTracker;
use crate::live::routing::acknowledgement::DeliveryAcknowledgement;
use crate::live::routing::acknowledgement::DeliveryCursorRange;
use crate::live::routing::acknowledgement::DeliveryObligationId;
use crate::live::routing::destination::{CapacityError, CapacityReservation, DestinationCapacity};
use crate::live::{model::delivery::DeliveryStatus, ports::routing::DestinationId};
use std::collections::VecDeque;
/// Bounded capacity owner for unresolved tracking updates.
///
/// One reservation represents one possible pending update.
///
/// Reservations must be acquired before irreversible
/// destination queue commitment.
#[derive(Debug, Clone)]
pub struct TrackingRecoveryCapacity {
    capacity: DestinationCapacity,
}
/// Outcome of one deterministic tracking recovery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingRecoveryOutcome {
    /// No pending updates exist.
    Empty,

    /// The oldest pending update was successfully applied.
    Recovered { remaining: usize },

    /// The update could not be applied and remains retained.
    RetryRequired {
        error: AcknowledgementError,
        remaining: usize,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingRecoveryValidationError {
    CountMismatch,
    ForeignReservation,
    InvalidReservationSize,
}

/// A destination outcome that could not be correlated with
/// a valid delivery obligation.
///
/// Retains recovery capacity until explicitly resolved.
/// A destination outcome that could not be correlated
/// with a registered delivery obligation.
///
/// Retains the original event cursor range and recovery
/// capacity until reconciliation is completed.
#[derive(Debug)]
pub struct UnresolvedRecoveryOutcome {
    destination: DestinationId,
    range: DeliveryCursorRange,
    status: DeliveryStatus,
    reservation: TrackingRecoveryReservation,
}

impl UnresolvedRecoveryOutcome {
    pub fn destination(&self) -> DestinationId {
        self.destination
    }

    pub fn range(&self) -> &DeliveryCursorRange {
        &self.range
    }

    pub fn status(&self) -> DeliveryStatus {
        self.status
    }
}
/// Capacity reserved specifically for a tracking recovery update.
///
/// This wrapper prevents callers from supplying arbitrary
/// destination-capacity reservations.
#[derive(Debug)]
pub struct TrackingRecoveryReservation {
    reservation: CapacityReservation,
}

impl TrackingRecoveryCapacity {
    pub fn new(max_pending_updates: usize) -> Result<Self, CapacityError> {
        let limits = DeliveryCapacity::new(max_pending_updates, max_pending_updates as u64)
            .map_err(|_| CapacityError::RequestTooLarge)?;

        Ok(Self {
            capacity: DestinationCapacity::new(limits, 1)?,
        })
    }

    /// Reserve capacity for one potential tracking failure.
    ///
    /// Dropping the returned reservation releases capacity.
    pub fn reserve(&self) -> Result<TrackingRecoveryReservation, CapacityError> {
        Ok(TrackingRecoveryReservation {
            reservation: self.capacity.try_reserve(1, 1)?,
        })
    }

    pub fn occupied(&self) -> usize {
        self.capacity.usage().occupied_events
    }

    pub fn available(&self) -> usize {
        self.capacity
            .limits()
            .max_pending_events
            .saturating_sub(self.occupied())
    }
}

/// A tracking update retained for later recovery.
///
/// Owns both the acknowledgement and its capacity reservation.
/// Dropping this value releases the reservation.
#[derive(Debug)]
pub struct PendingTrackingUpdate {
    acknowledgement: DeliveryAcknowledgement,
    reservation: TrackingRecoveryReservation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingRecoveryError {
    ForeignReservation,
}
impl PendingTrackingUpdate {
    fn new(
        acknowledgement: DeliveryAcknowledgement,
        reservation: TrackingRecoveryReservation,
    ) -> Self {
        Self {
            acknowledgement,
            reservation,
        }
    }

    pub fn acknowledgement(&self) -> &DeliveryAcknowledgement {
        &self.acknowledgement
    }

    pub fn reserved_events(&self) -> usize {
        self.reservation.reservation.events()
    }
}
/// Bounded in-process storage for failed tracking updates.
///
/// Capacity is accounted for by the reservations owned
/// by individual PendingTrackingUpdate values.
#[derive(Debug)]
pub struct TrackingRecovery {
    capacity: TrackingRecoveryCapacity,
    pending: VecDeque<PendingTrackingUpdate>,
    unresolved: VecDeque<UnresolvedRecoveryOutcome>,
}

impl TrackingRecovery {
    pub fn new(max_pending_updates: usize) -> Result<Self, CapacityError> {
        Ok(Self {
            capacity: TrackingRecoveryCapacity::new(max_pending_updates)?,
            pending: VecDeque::new(),
            unresolved: VecDeque::new(),
        })
    }
    /// Reserve one recovery slot for each required destination.
    ///
    /// If any reservation fails, all previously acquired
    /// reservations are released automatically.
    pub fn reserve_batch(
        &self,
        count: usize,
    ) -> Result<Vec<TrackingRecoveryReservation>, CapacityError> {
        // Reject before allocating: public callers may supply arbitrary counts,
        // including counts whose Vec layout would overflow. Availability is a
        // preflight only; individual reservations still enforce shared capacity.
        if count > self.available() {
            return Err(CapacityError::Full);
        }
        let mut reservations = Vec::new();
        reservations
            .try_reserve_exact(count)
            .map_err(|_| CapacityError::AccountingOverflow)?;

        for _ in 0..count {
            reservations.push(self.reserve()?);
        }

        Ok(reservations)
    }
    /// Reserve one slot before queue commitment.
    pub fn reserve(&self) -> Result<TrackingRecoveryReservation, CapacityError> {
        self.capacity.reserve()
    }
    /// Attempt recovery of the oldest pending tracking update.
    ///
    /// On success, the update is removed and its capacity
    /// reservation is released.
    ///
    /// On failure, the update remains retained for a later retry.
    ///
    /// This method does not perform automatic retries or sleep.
    pub fn retry_next(&mut self, tracker: &mut AcknowledgementTracker) -> TrackingRecoveryOutcome {
        let Some(update) = self.pending.front() else {
            return TrackingRecoveryOutcome::Empty;
        };

        let result = tracker.acknowledge(update.acknowledgement().clone());

        match result {
            Ok(()) => {
                // Dropping the update releases its reservation.
                self.pending.pop_front();

                TrackingRecoveryOutcome::Recovered {
                    remaining: self.pending.len(),
                }
            }

            Err(error) => {
                // Keep both the acknowledgement and reservation.
                TrackingRecoveryOutcome::RetryRequired {
                    error,
                    remaining: self.pending.len(),
                }
            }
        }
    }

    pub fn retain_unresolved(
        &mut self,
        destination: DestinationId,
        range: DeliveryCursorRange,
        status: DeliveryStatus,
        reservation: TrackingRecoveryReservation,
    ) -> Result<(), UnresolvedRetentionFailure> {
        if !reservation.reservation.belongs_to(&self.capacity.capacity) {
            return Err(UnresolvedRetentionFailure {
                destination,
                range,
                status,
                reservation,
                reason: TrackingApplyError::ForeignReservation,
            });
        }

        if reservation.reservation.events() != 1 || reservation.reservation.bytes() != 1 {
            return Err(UnresolvedRetentionFailure {
                destination,
                range,
                status,
                reservation,
                reason: TrackingApplyError::InvalidReservationSize,
            });
        }

        self.unresolved.push_back(UnresolvedRecoveryOutcome {
            destination,
            range,
            status,
            reservation,
        });

        Ok(())
    }
    pub fn unresolved_len(&self) -> usize {
        self.unresolved.len()
    }

    /// Inspect the oldest uncorrelated outcome without transferring responsibility.
    pub fn next_unresolved(&self) -> Option<&UnresolvedRecoveryOutcome> {
        self.unresolved.front()
    }

    /// Reconcile the oldest uncorrelated outcome with a known obligation.
    ///
    /// The authoritative tracker checks destination, complete cursor range and
    /// progress semantics before either owner releases responsibility. Rejection
    /// leaves the outcome and its reservation intact. Success releases only the
    /// recovery slot; terminal obligation finalization remains the tracker's job.
    pub fn reconcile_next_unresolved(
        &mut self,
        tracker: &mut AcknowledgementTracker,
        obligation: DeliveryObligationId,
    ) -> TrackingRecoveryOutcome {
        let Some(outcome) = self.unresolved.front() else {
            return TrackingRecoveryOutcome::Empty;
        };
        let acknowledgement = DeliveryAcknowledgement {
            obligation,
            destination: outcome.destination,
            range: outcome.range.clone(),
            status: outcome.status,
        };
        match tracker.acknowledge(acknowledgement) {
            Ok(()) => {
                self.unresolved.pop_front();
                TrackingRecoveryOutcome::Recovered {
                    remaining: self.unresolved.len(),
                }
            }
            Err(error) => TrackingRecoveryOutcome::RetryRequired {
                error,
                remaining: self.unresolved.len(),
            },
        }
    }
    /// Validate reservations intended for one tracked fan-out.
    ///
    /// This operation does not consume reservations or mutate
    /// recovery state.
    pub fn validate_reservations(
        &self,
        reservations: &[TrackingRecoveryReservation],
        expected_count: usize,
    ) -> Result<(), TrackingRecoveryValidationError> {
        if reservations.len() != expected_count {
            return Err(TrackingRecoveryValidationError::CountMismatch);
        }

        for reservation in reservations {
            if !reservation.reservation.belongs_to(&self.capacity.capacity) {
                return Err(TrackingRecoveryValidationError::ForeignReservation);
            }

            if reservation.reservation.events() != 1 || reservation.reservation.bytes() != 1 {
                return Err(TrackingRecoveryValidationError::InvalidReservationSize);
            }
        }

        Ok(())
    }
    /// Retain a failed acknowledgement with its reservation.
    ///
    /// The reservation remains occupied until the update
    /// is removed or the recovery owner is dropped.
    /// Retain an acknowledgement using capacity reserved
    /// from this recovery owner.
    ///
    /// Foreign reservations are rejected without inserting
    /// the acknowledgement into the recovery collection.
    pub fn retain(
        &mut self,
        acknowledgement: DeliveryAcknowledgement,
        reservation: TrackingRecoveryReservation,
    ) -> Result<(), TrackingRecoveryError> {
        if !reservation.reservation.belongs_to(&self.capacity.capacity) {
            return Err(TrackingRecoveryError::ForeignReservation);
        }

        self.pending
            .push_back(PendingTrackingUpdate::new(acknowledgement, reservation));

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn occupied(&self) -> usize {
        self.capacity.occupied()
    }

    pub fn available(&self) -> usize {
        self.capacity.available()
    }
    /// Reserve recovery capacity for an entire fan-out.
    ///
    /// All reservations are acquired before returning the bundle.
    ///
    /// If any reservation fails, previously acquired capacity
    /// is released automatically.
    pub fn prepare_batch(
        &self,
        required_destinations: usize,
    ) -> Result<TrackingRecoveryBatch, CapacityError> {
        let reservations = self.reserve_batch(required_destinations)?;

        Ok(TrackingRecoveryBatch { reservations })
    }
    /// Apply a delivery acknowledgement or retain it for recovery.
    ///
    /// The caller must supply a reservation acquired from this
    /// recovery owner before irreversible queue commitment.
    ///
    /// Successful application releases capacity.
    ///
    /// Failed application retains both the acknowledgement and
    /// its reservation.
    /// Apply an acknowledgement or retain it for recovery.
    ///
    /// Validation failures return the original acknowledgement
    /// and reservation without modifying the tracker.
    ///
    /// Successful acknowledgements release recovery capacity.
    ///
    /// Failed acknowledgements remain owned by TrackingRecovery.
    pub fn apply_or_retain(
        &mut self,
        tracker: &mut AcknowledgementTracker,
        acknowledgement: DeliveryAcknowledgement,
        reservation: TrackingRecoveryReservation,
    ) -> Result<TrackingApplyOutcome, TrackingApplyFailure> {
        // Validate recovery ownership before touching the tracker.
        if !reservation.reservation.belongs_to(&self.capacity.capacity) {
            return Err(TrackingApplyFailure {
                reason: TrackingApplyError::ForeignReservation,
                acknowledgement,
                reservation,
            });
        }

        // Validate the reservation size.
        if reservation.reservation.events() != 1 || reservation.reservation.bytes() != 1 {
            return Err(TrackingApplyFailure {
                reason: TrackingApplyError::InvalidReservationSize,
                acknowledgement,
                reservation,
            });
        }

        match tracker.acknowledge(acknowledgement.clone()) {
            Ok(()) => {
                // Reservation is dropped, releasing capacity.
                Ok(TrackingApplyOutcome::Applied)
            }

            Err(error) => {
                // Transfer ownership into bounded recovery storage.
                self.pending
                    .push_back(PendingTrackingUpdate::new(acknowledgement, reservation));

                Ok(TrackingApplyOutcome::Retained { error })
            }
        }
    }
}

/// Recovery capacity reserved for one complete fan-out.
///
/// Owns all reservations until they are consumed or dropped.
///
/// Dropping the bundle before commitment releases every
/// reservation through RAII.
#[derive(Debug)]
pub struct TrackingRecoveryBatch {
    reservations: Vec<TrackingRecoveryReservation>,
}

impl TrackingRecoveryBatch {
    pub fn len(&self) -> usize {
        self.reservations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty()
    }

    pub fn into_reservations(self) -> Vec<TrackingRecoveryReservation> {
        self.reservations
    }
}

/// Outcome of applying an acknowledgement with reserved
/// recovery capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingApplyOutcome {
    /// Acknowledgement succeeded; recovery capacity released.
    Applied,

    /// Acknowledgement failed and was retained for recovery.
    Retained { error: AcknowledgementError },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackingApplyError {
    ForeignReservation,
    InvalidReservationSize,
}

/// A failed tracking application that preserves ownership
/// of both the acknowledgement and recovery reservation.
#[derive(Debug)]
pub struct TrackingApplyFailure {
    pub reason: TrackingApplyError,
    pub acknowledgement: DeliveryAcknowledgement,
    pub reservation: TrackingRecoveryReservation,
}

#[derive(Debug)]
pub struct UnresolvedRetentionFailure {
    pub destination: DestinationId,
    pub range: DeliveryCursorRange,
    pub status: DeliveryStatus,
    pub reservation: TrackingRecoveryReservation,
    pub reason: TrackingApplyError,
}
