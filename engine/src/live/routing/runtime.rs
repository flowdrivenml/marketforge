//! Owned composition of the existing router admission and delivery owners.
//! The executor schedules bounded turns; adapters retain their frozen ports.
//! Keep this owner after a shutdown deadline: expiry never waives obligations.
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use super::{
    admission::{DeadlineFuture, DeadlineTimer},
    driver::{DeliveryDriver, RegisteredDeliveryDrive},
    ingress::RouterIngress,
    ingress_reporting::{
        IngressOutcomes, IngressReporter, ScheduledIngressReporting, ScheduledIngressStep,
        TrackingIngressReporter,
    },
    metrics::{RouterMetricResults, RouterMetricSampler},
    namespaces::{AdmissionNamespaces, NamespaceError},
    ordered::OrderedAdmissionBudget,
    port::RouterPort,
    reporting::{
        FinalizationReporter, FinalizationReportingConsumer, ReportingCoordinatorError,
        ReportingStep,
    },
    router::{RouterRegistry, RouterRegistryError},
    shutdown::{CompleteRouterDrainSnapshot, RouterDrainSnapshot},
};
use crate::live::{
    foundation::{
        cancellation::CancellationToken,
        clock::{MonotonicClock, WallClock},
    },
    model::{cursor::StreamCursor, error::ErrorCode},
    ports::{
        observability::ScopedMetricsPort,
        routing::{
            DestinationCompletion, DestinationId, EventBatch, EventDestinationPort,
            EventRouterPort, RoutingAdmission, RoutingFuture,
        },
    },
};

/// Move established owners into the composition. No mutable owner is cloned.
pub struct RouterResources {
    pub destinations: RouterRegistry,
    pub namespaces: AdmissionNamespaces,
    pub admission: OrderedAdmissionBudget,
    pub delivery: DeliveryDriver,
    pub outcomes: IngressOutcomes,
}

/// Validated ownership transfer; rejection returns every original resource.
/// No queues, preparations or outstanding obligations are dropped on failure.
pub struct ValidatedRouterResources(RouterResources);
impl RouterResources {
    #[allow(clippy::result_large_err)]
    pub fn validate(self) -> Result<ValidatedRouterResources, (ErrorCode, Self)> {
        let destinations = self.destinations.snapshot();
        let limit = self.delivery.snapshot().workers.limit;
        if destinations.destinations.len() + destinations.retired.len() > limit
            || destinations
                .destinations
                .iter()
                .chain(destinations.retired.iter())
                .any(|entry| entry.queue.is_none())
        {
            return Err((ErrorCode::InvalidInput, self));
        }
        Ok(ValidatedRouterResources(self))
    }
}

pub struct RouterServices {
    pub clock: Arc<dyn MonotonicClock>,
    pub timer: Arc<dyn DeadlineTimer>,
    pub wall: Arc<dyn WallClock>,
    pub metrics: Arc<dyn ScopedMetricsPort>,
}

struct Shared {
    destinations: RouterRegistry,
    namespaces: AdmissionNamespaces,
    admission: OrderedAdmissionBudget,
    delivery: Mutex<DeliveryDriver>,
    outcomes: IngressOutcomes,
    clock: Arc<dyn MonotonicClock>,
    timer: Arc<dyn DeadlineTimer>,
    cancellation: CancellationToken,
}

/// Owned producer endpoint suitable for the runtime's Arc<dyn EventRouterPort>.
/// Clones share admission, queues, tracking and shutdown fencing.
#[derive(Clone)]
pub struct RouterHandle {
    shared: Arc<Shared>,
    timeout: Option<Duration>,
}
impl EventRouterPort for RouterHandle {
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, RoutingAdmission> {
        Box::pin(async move {
            RouterPort {
                ingress: RouterIngress {
                    registry: &self.shared.destinations,
                    namespaces: &self.shared.namespaces,
                    budget: &self.shared.admission,
                    delivery: &self.shared.delivery,
                    clock: self.shared.clock.clone(),
                    timer: Some(self.shared.timer.as_ref()),
                },
                outcomes: &self.shared.outcomes,
                cancellation: self.shared.cancellation.clone(),
                timeout: self.timeout,
            }
            .admit(batch)
            .await
        })
    }
}

#[derive(Debug)]
#[must_use]
pub enum RuntimeDeliveryTurn {
    Driven(RegisteredDeliveryDrive),
    BestEffort {
        registry: super::driver::RegistryWorkerSync,
        recovery: super::tracking_recovery::TrackingRecoveryPass,
        membership: Poll<Result<u64, RouterRegistryError>>,
        descriptor: crate::live::ports::routing::DestinationDescriptor,
        admission: super::worker::WorkerAdmissionResult,
    },
    Finalized {
        registry: super::driver::RegistryWorkerSync,
        recovery: super::tracking_recovery::TrackingRecoveryPass,
        membership: Poll<Result<u64, RouterRegistryError>>,
        outcome: super::acknowledgement::ObligationFinalization,
        retained_for_recovery: bool,
    },
}

#[derive(Debug)]
#[must_use]
pub struct RouterTurn {
    pub delivery: RuntimeDeliveryTurn,
    pub delivery_scheduling: Result<(), ErrorCode>,
    pub terminal_reporting: Result<ReportingStep, ReportingCoordinatorError>,
    pub ingress_reporting: Result<Poll<ScheduledIngressStep>, ErrorCode>,
    pub metrics: Poll<Result<Option<RouterMetricResults>, ErrorCode>>,
}

#[derive(Debug)]
#[must_use]
pub enum RouterStop {
    Drained,
    DeadlineReached,
    Step(RouterTurn),
}

/// One mutable runtime owner for scheduling and reporting. Producer calls borrow
/// the same driver for commitment. Reporting adapters must be bounded/nonblocking
/// under their existing synchronous contract. Dropping this owner is abandonment.
pub struct RouterRuntime<R, I> {
    shared: Arc<Shared>,
    terminal: FinalizationReportingConsumer<R>,
    ingress: ScheduledIngressReporting,
    reporter: I,
    sampler: RouterMetricSampler,
    services: RouterServices,
    retry_interval: Duration,
    failed_payloads: std::collections::VecDeque<super::completion::FinalizedDelivery>,
    optional_outcomes: std::collections::VecDeque<super::worker::WorkerOutcome>,
}

impl<R: FinalizationReporter, I: IngressReporter> RouterRuntime<R, I> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        resources: ValidatedRouterResources,
        services: RouterServices,
        terminal: FinalizationReportingConsumer<R>,
        ingress: ScheduledIngressReporting,
        reporter: I,
        sampler: RouterMetricSampler,
        retry_interval_ns: std::num::NonZeroU64,
    ) -> Self {
        let resources = resources.0;
        Self {
            shared: Arc::new(Shared {
                destinations: resources.destinations,
                namespaces: resources.namespaces,
                admission: resources.admission,
                delivery: Mutex::new(resources.delivery),
                outcomes: resources.outcomes,
                clock: services.clock.clone(),
                timer: services.timer.clone(),
                cancellation: CancellationToken::new(),
            }),
            terminal,
            ingress,
            reporter,
            sampler,
            services,
            retry_interval: Duration::from_nanos(retry_interval_ns.get()),
            failed_payloads: std::collections::VecDeque::new(),
            optional_outcomes: std::collections::VecDeque::new(),
        }
    }

    pub fn producer(&self, timeout: Option<Duration>) -> RouterHandle {
        RouterHandle {
            shared: self.shared.clone(),
            timeout,
        }
    }

    pub fn register_namespace(&self, first: StreamCursor) -> Result<(), NamespaceError> {
        self.shared.namespaces.register(first)
    }

    /// Activate a real adapter with bounded queue accounting. Destination IDs
    /// identify instances; removal retains old workers/charges until drained.
    pub fn register_destination(
        &mut self,
        adapter: Arc<dyn EventDestinationPort>,
        max_waiters: usize,
    ) -> Result<(), RouterRegistryError> {
        // Every live/retired instance needs its own scheduler slot. A stalled
        // best-effort call must never occupy the last slot needed by another sink.
        let destinations = self.shared.destinations.snapshot();
        let workers = self
            .shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .snapshot()
            .workers
            .limit;
        if destinations.destinations.len() + destinations.retired.len() >= workers {
            return Err(RouterRegistryError::DestinationCapacity);
        }
        let capacity = super::destination::DestinationCapacity::new(
            adapter.descriptor().capacity,
            max_waiters,
        )
        .map_err(|_| RouterRegistryError::InvalidCapacity)?;
        self.shared
            .destinations
            .register_queue(adapter, super::queue::DestinationQueue::new(capacity))
    }

    pub fn unregister_destination(&mut self, id: DestinationId) -> Result<(), RouterRegistryError> {
        self.shared.destinations.unregister(id)
    }

    pub fn apply_completion(
        &self,
        completion: &DestinationCompletion,
    ) -> Result<super::acknowledgement::DeliveryObligationId, super::completion::PendingDeliveryError>
    {
        self.shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .apply_completion(completion)
    }

    /// Transfer the original charged payload and failed outcome to an explicit
    /// recovery consumer. Dequeue is not successful repair or historical coverage.
    pub fn take_failed_payload(&mut self) -> Option<super::completion::FinalizedDelivery> {
        self.failed_payloads.pop_front()
    }

    /// Optional attempts are observational, not required obligations. Transfer
    /// their charged payloads to reporting/recovery without claiming delivery.
    pub fn take_optional_outcome(&mut self) -> Option<super::worker::WorkerOutcome> {
        self.optional_outcomes.pop_front()
    }

    pub fn optional_outcomes_len(&self) -> usize {
        self.optional_outcomes.len()
    }

    pub fn failed_payloads_len(&self) -> usize {
        self.failed_payloads.len()
    }

    /// Explicit intervention retains the original pending ingress charge on
    /// transfer. Callers must accept recovery responsibility before releasing it.
    pub fn take_ingress_for_recovery(&mut self) -> Option<super::ingress_reporting::IngressRecord> {
        self.ingress.take_pending()
    }

    pub fn retry_ingress(&mut self, cx: &Context<'_>) -> bool {
        self.ingress.retry_escalated(cx)
    }

    pub fn escalated_terminal_reports(
        &self,
    ) -> Vec<super::acknowledgement::EscalatedFinalizationRecord> {
        self.shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .journal_mut()
            .escalated_records()
    }

    /// The management caller enforces authorization and local audit recording.
    pub fn retry_terminal_report(
        &mut self,
        id: super::acknowledgement::DeliveryObligationId,
        cx: &Context<'_>,
    ) -> Result<(), super::acknowledgement::FinalizationJournalError> {
        self.shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .journal_mut()
            .authorize_retry(id)?;
        cx.waker().wake_by_ref();
        Ok(())
    }

    /// Correlation failures retain their original payload until a recovery owner
    /// explicitly takes responsibility. No guessed obligation is constructed.
    pub fn take_blocked_for_recovery(
        &mut self,
    ) -> Option<(
        super::completion::PendingDeliveryError,
        super::worker::WorkerOutcome,
    )> {
        self.shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take_blocked_for_recovery()
    }

    pub fn take_delivery_for_recovery(
        &mut self,
        id: super::acknowledgement::DeliveryObligationId,
    ) -> Option<super::completion::RetainedDelivery> {
        self.shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take_for_recovery(id)
    }

    pub fn reconcile_unresolved_tracking(
        &mut self,
        id: super::acknowledgement::DeliveryObligationId,
    ) -> super::tracking_recovery::TrackingRecoveryOutcome {
        self.shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reconcile_unresolved_tracking(id)
    }

    pub fn snapshot(&self) -> CompleteRouterDrainSnapshot {
        let driver = self
            .shared
            .delivery
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        CompleteRouterDrainSnapshot {
            delivery: RouterDrainSnapshot::observe(
                &self.shared.namespaces,
                &self.shared.admission,
                &self.shared.destinations,
                &driver,
            ),
            ingress: self.ingress.snapshot(&self.shared.outcomes),
        }
    }

    /// At most one report of each kind and one bounded delivery turn. Release
    /// the delivery fence before ingress reporting reacquires it for tracking
    /// repair; holding it here would permanently block the real handoff path.
    pub fn drive(&mut self, cx: &mut Context<'_>) -> RouterTurn {
        let (delivery, delivery_scheduling, terminal_reporting) = {
            let mut driver = self
                .shared
                .delivery
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let turn = driver.drive_registered_reporting(
                &self.shared.destinations,
                cx,
                self.services.clock.as_ref(),
                self.services.timer.as_ref(),
                self.retry_interval,
                &self.terminal,
            );
            if self.shared.admission.snapshot().closed {
                super::shutdown::RouterShutdownOwners {
                    namespaces: &self.shared.namespaces,
                    admission: &self.shared.admission,
                    destinations: &self.shared.destinations,
                    delivery: &mut driver,
                }
                .close_reconciled_delivery(&turn.0, cx);
            }
            turn
        };
        let delivery = match delivery {
            RegisteredDeliveryDrive {
                registry,
                delivery:
                    super::driver::DeliveryDrive {
                        recovery,
                        delivery:
                            Poll::Ready(super::supervisor::SupervisionStep::BestEffort(outcome)),
                    },
                membership,
            } => {
                let descriptor = outcome.descriptor;
                let admission = outcome.admission.clone();
                self.optional_outcomes.push_back(outcome);
                RuntimeDeliveryTurn::BestEffort {
                    registry,
                    recovery,
                    membership,
                    descriptor,
                    admission,
                }
            }

            RegisteredDeliveryDrive {
                registry,
                delivery:
                    super::driver::DeliveryDrive {
                        recovery,
                        delivery:
                            Poll::Ready(super::supervisor::SupervisionStep::Finalized(finalized)),
                    },
                membership,
            } => {
                let outcome = finalized.outcome;
                let retained_for_recovery = matches!(
                    outcome,
                    super::acknowledgement::ObligationFinalization::Failed { .. }
                );
                if retained_for_recovery {
                    // Each payload keeps its original queue count/byte charge.
                    // Registry retirement retains that capacity owner. Therefore
                    // this collection is bounded by registered queue capacities.
                    self.failed_payloads.push_back(finalized);
                }
                RuntimeDeliveryTurn::Finalized {
                    registry,
                    recovery,
                    membership,
                    outcome,
                    retained_for_recovery,
                }
            }
            turn => RuntimeDeliveryTurn::Driven(turn),
        };
        let ingress_reporting = self.ingress.poll_step(
            &self.shared.outcomes,
            &mut TrackingIngressReporter {
                delivery: &self.shared.delivery,
                reporter: &mut self.reporter,
            },
            cx,
            self.services.clock.as_ref(),
            self.services.timer.as_ref(),
        );
        let metrics = self.sampler.poll_sample(
            cx,
            self.services.clock.as_ref(),
            self.services.timer.as_ref(),
            self.services.wall.as_ref(),
            self.services.metrics.as_ref(),
            || {
                let driver = self
                    .shared
                    .delivery
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                RouterDrainSnapshot::observe(
                    &self.shared.namespaces,
                    &self.shared.admission,
                    &self.shared.destinations,
                    &driver,
                )
            },
        );
        RouterTurn {
            delivery,
            delivery_scheduling,
            terminal_reporting,
            ingress_reporting,
            metrics,
        }
    }

    /// Close every producer owner before the deadline is checked. Pending
    /// completions remain serviceable through apply_completion after expiry.
    pub fn shutdown(&mut self, cx: &mut Context<'_>, deadline: &mut DeadlineFuture) -> RouterStop {
        let _ = self.shared.admission.poll_shutdown(cx);
        let _ = self.shared.namespaces.poll_shutdown(cx);
        let _ = self.shared.destinations.poll_shutdown(cx);
        let _ = self.shared.outcomes.poll_shutdown(cx);
        self.shared.cancellation.cancel();
        if self.snapshot().is_drained() {
            return RouterStop::Drained;
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return RouterStop::DeadlineReached;
        }
        RouterStop::Step(self.drive(cx))
    }
}

impl<I: IngressReporter> IngressReporter for &mut I {
    fn report(
        &mut self,
        batch: &EventBatch,
        outcome: &mut super::ingress::IngressOutcome,
    ) -> Result<(), super::reporting::ReportingError> {
        (**self).report(batch, outcome)
    }
}
