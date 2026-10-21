//! Ordered required-destination admission through the existing tracked fan-out.
//! No obligations are allocated while awaiting capacity: dropping a preparation
//! future releases its order permit and every acquired queue reservation.
use std::sync::Arc;

use super::{
    acknowledgement::{AcknowledgementError, AcknowledgementTracker},
    admission::{AdmissionContext, DeadlineTimer},
    cursor::{CursorOrderError, CursorOrderPermit, StreamAdmissionOrder},
    namespaces::{AdmissionNamespaces, NamespaceError},
    router::{
        FanoutAdmissionCoordinator, FanoutCommitmentCoordinator, FanoutPreparationError,
        PreparedFanout, RouterRegistry, RouterRegistryError, RoutingPlan, TrackedFanoutResult,
        prepare_routing_obligations,
    },
    tracking_recovery::TrackingRecovery,
};
use crate::live::{
    foundation::{
        MonotonicClock,
        resources::{ResourceBudget, ResourceReservation, ResourceUsage},
    },
    model::{error::ErrorCode, memory::RetainedSize},
    ports::routing::{EventBatch, RoutingAdmissionFailure},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderedPreparationError {
    Context(RoutingAdmissionFailure),
    Order(CursorOrderError),
    Registry(RouterRegistryError),
    Capacity(FanoutPreparationError),
    Namespace(NamespaceError),
    Budget(ErrorCode),
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
    _admission: Option<ResourceReservation>,
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
    /// Attempt optional queues only after required responsibility transferred.
    /// Optional rejection never rewinds required commitment or its cursor.
    pub fn commit_with_best_effort(
        self,
        tracker: &mut AcknowledgementTracker,
        recovery: &mut TrackingRecovery,
    ) -> (
        OrderedCommitOutcome,
        Option<Result<super::router::BestEffortFanout, super::router::BestEffortError>>,
    ) {
        let mut optional = None;
        let required = self.commit_inner(tracker, recovery, Some(&mut optional));
        (required, optional)
    }

    /// Resolve the pre-registered authoritative frontier. An incoming batch
    /// cannot implicitly establish or reset a namespace.
    pub async fn prepare_registered(
        registry: &'a RouterRegistry,
        namespaces: &AdmissionNamespaces,
        batch: EventBatch,
        context: AdmissionContext,
        clock: Arc<dyn MonotonicClock>,
        timer: Option<&dyn DeadlineTimer>,
    ) -> Result<Self, OrderedPreparationError> {
        check(&context, clock.as_ref()).map_err(OrderedPreparationError::Context)?;
        let cursor = batch.first_cursor();
        let order = namespaces
            .get(&cursor.stream_id, cursor.log_epoch)
            .map_err(OrderedPreparationError::Namespace)?;
        Self::prepare(registry, &order, batch, context, clock, timer).await
    }

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
            _admission: None,
        })
    }

    /// No await points occur after the final context check. Cancellation after
    /// responsibility transfer never rewinds the authoritative frontier.
    pub fn commit(
        self,
        tracker: &mut AcknowledgementTracker,
        recovery: &mut TrackingRecovery,
    ) -> OrderedCommitOutcome {
        self.commit_inner(tracker, recovery, None)
    }

    fn commit_inner(
        self,
        tracker: &mut AcknowledgementTracker,
        recovery: &mut TrackingRecovery,
        optional: Option<
            &mut Option<Result<super::router::BestEffortFanout, super::router::BestEffortError>>,
        >,
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
        let optional_batch = optional.as_ref().map(|_| self.batch.clone());
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
            if let (Some(output), Some(batch)) = (optional, optional_batch) {
                *output = Some(self.registry.try_best_effort(&self.plan, &batch));
            }
            self.order.commit();
        }
        OrderedCommitOutcome::Tracked(result)
    }
}

/// Global count/byte bound for preparations, including futures waiting on slow
/// destinations. Capacity exhaustion is explicit rejection; no waiter queue is
/// allocated. A prepared operation retains its charge until commitment or drop.
#[derive(Debug)]
pub struct OrderedAdmissionBudget {
    budget: ResourceBudget,
}

impl OrderedAdmissionBudget {
    pub fn new(limit: ResourceUsage) -> Result<Self, ErrorCode> {
        Ok(Self {
            budget: ResourceBudget::new(limit)?,
        })
    }

    pub fn usage(&self) -> ResourceUsage {
        self.budget.usage()
    }

    /// Stop new preparations without revoking already acquired responsibility.
    /// Runtime shutdown must also close namespaces and cancel waiting contexts.
    pub fn close(&self) {
        self.budget.close();
    }

    pub async fn prepare<'a>(
        &self,
        registry: &'a RouterRegistry,
        namespaces: &AdmissionNamespaces,
        batch: EventBatch,
        context: AdmissionContext,
        clock: Arc<dyn MonotonicClock>,
        timer: Option<&dyn DeadlineTimer>,
    ) -> Result<OrderedFanout<'a>, OrderedPreparationError> {
        check(&context, clock.as_ref()).map_err(OrderedPreparationError::Context)?;
        let bytes = batch
            .retained_size_bytes()
            .map_err(|_| OrderedPreparationError::Budget(ErrorCode::Capacity))?;
        let reservation = self
            .budget
            .reserve(ResourceUsage {
                items: batch.len(),
                bytes: usize::try_from(bytes)
                    .map_err(|_| OrderedPreparationError::Budget(ErrorCode::Capacity))?,
            })
            .map_err(OrderedPreparationError::Budget)?;
        let mut prepared =
            OrderedFanout::prepare_registered(registry, namespaces, batch, context, clock, timer)
                .await?;
        prepared._admission = Some(reservation);
        Ok(prepared)
    }
}
