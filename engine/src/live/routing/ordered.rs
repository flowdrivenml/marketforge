//! Ordered required-destination admission through the existing tracked fan-out.
//! No obligations are allocated while awaiting capacity: dropping a preparation
//! future releases its order permit and every acquired queue reservation.
use std::sync::Arc;

use super::{
    acknowledgement::{AcknowledgementError, AcknowledgementTracker},
    admission::{AdmissionContext, DeadlineTimer},
    cursor::{CursorOrderError, CursorOrderPermit, StreamAdmissionOrder},
    router::{
        FanoutAdmissionCoordinator, FanoutCommitmentCoordinator, FanoutPreparationError,
        PreparedFanout, RouterRegistry, RouterRegistryError, RoutingPlan, TrackedFanoutResult,
        prepare_routing_obligations,
    },
    tracking_recovery::TrackingRecovery,
};
use crate::live::{
    foundation::MonotonicClock,
    ports::routing::{EventBatch, RoutingAdmissionFailure},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderedPreparationError {
    Context(RoutingAdmissionFailure),
    Order(CursorOrderError),
    Registry(RouterRegistryError),
    Capacity(FanoutPreparationError),
}

/// Owns capacity and the exclusive namespace permit; has no tracker side effects
/// until synchronous commitment. Dropping it is a complete precommit rollback.
#[must_use = "commit or drop to release the prepared capacity and order permit"]
pub struct OrderedFanout<'a> {
    registry: &'a RouterRegistry,
    batch: EventBatch,
    plan: RoutingPlan,
    prepared: PreparedFanout,
    order: CursorOrderPermit,
    context: AdmissionContext,
    clock: Arc<dyn MonotonicClock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderedCommitFailure {
    Context(RoutingAdmissionFailure),
    Obligations(AcknowledgementError),
}

#[derive(Debug)]
#[must_use = "retain partial delivery and recovery responsibility"]
pub enum OrderedCommitOutcome {
    Rejected {
        batch: EventBatch,
        reason: OrderedCommitFailure,
    },
    Tracked(TrackedFanoutResult),
}

fn check(
    context: &AdmissionContext,
    clock: &dyn MonotonicClock,
) -> Result<(), RoutingAdmissionFailure> {
    if context.cancellation.is_cancelled() {
        return Err(RoutingAdmissionFailure::Cancelled);
    }
    if context
        .deadline
        .is_some_and(|deadline| clock.now() >= deadline)
    {
        return Err(RoutingAdmissionFailure::DeadlineExceeded);
    }
    Ok(())
}

impl<'a> OrderedFanout<'a> {
    pub async fn prepare(
        registry: &'a RouterRegistry,
        order: &StreamAdmissionOrder,
        batch: EventBatch,
        context: AdmissionContext,
        clock: Arc<dyn MonotonicClock>,
        timer: Option<&dyn DeadlineTimer>,
    ) -> Result<Self, OrderedPreparationError> {
        check(&context, clock.as_ref()).map_err(OrderedPreparationError::Context)?;
        let order = order
            .prepare(&batch)
            .map_err(OrderedPreparationError::Order)?;
        let plan = registry
            .plan(&batch)
            .map_err(OrderedPreparationError::Registry)?;
        let prepared = FanoutAdmissionCoordinator::prepare(&plan, &batch, context.clone(), timer)
            .await
            .map_err(OrderedPreparationError::Capacity)?;
        check(&context, clock.as_ref()).map_err(OrderedPreparationError::Context)?;
        Ok(Self {
            registry,
            batch,
            plan,
            prepared,
            order,
            context,
            clock,
        })
    }

    /// No await points occur after the final context check. Cancellation after
    /// responsibility transfer never rewinds the authoritative frontier.
    pub fn commit(
        self,
        tracker: &mut AcknowledgementTracker,
        recovery: &mut TrackingRecovery,
    ) -> OrderedCommitOutcome {
        if let Err(reason) = check(&self.context, self.clock.as_ref()) {
            return OrderedCommitOutcome::Rejected {
                batch: self.batch,
                reason: OrderedCommitFailure::Context(reason),
            };
        }
        let obligations = match prepare_routing_obligations(tracker, &self.plan) {
            Ok(obligations) => obligations,
            Err(reason) => {
                return OrderedCommitOutcome::Rejected {
                    batch: self.batch,
                    reason: OrderedCommitFailure::Obligations(reason),
                };
            }
        };
        let result = FanoutCommitmentCoordinator::commit_tracked_with_recovery_checked(
            self.registry,
            tracker,
            recovery,
            &self.plan,
            self.prepared,
            obligations,
            self.batch,
            || check(&self.context, self.clock.as_ref()),
        );
        if !matches!(result, TrackedFanoutResult::Rejected { .. }) {
            self.order.commit();
        }
        OrderedCommitOutcome::Tracked(result)
    }
}
