use crate::live::support::live_events::l2_event;
use marketforge_engine::live::{
    foundation::{clock::ManualClock, resources::ResourceUsage},
    metrics::registry::NativeMetrics,
    model::{
        delivery::{
            DeliveryCapacity, DeliveryPolicy, DeliveryStage, DeliveryStatus, DestinationKind,
        },
        observation::{MetricBinding, ResetBoundary},
        operation::{ResourceId, ResourceVersion},
    },
    ports::routing::{
        DestinationAdmission, DestinationCompletion, DestinationDescriptor, DestinationId,
        EventBatch, EventDestinationPort, EventRouterPort, RoutingAdmission, RoutingFuture,
    },
    routing::{
        acknowledgement::{
            AcknowledgementTracker, FinalizationJournal, FinalizationRecord,
            FinalizationRetryPolicy,
        },
        admission::{DeadlineFuture, DeadlineTimer},
        driver::DeliveryDriver,
        ingress::IngressOutcome,
        ingress_reporting::{IngressOutcomes, IngressReporter, ScheduledIngressReporting},
        metrics::{ROUTER_METRICS, RouterMetricSampler},
        namespaces::AdmissionNamespaces,
        ordered::OrderedAdmissionBudget,
        reporting::{FinalizationReporter, FinalizationReportingConsumer, ReportingError},
        router::RouterRegistry,
        runtime::{RouterResources, RouterRuntime, RouterServices, RouterStop},
        supervisor::DeliverySupervisor,
        tracking_recovery::TrackingRecovery,
        worker::DestinationWorkers,
    },
};
use std::{
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};
struct Timer;
impl DeadlineTimer for Timer {
    fn sleep_until(&self, _: Instant) -> DeadlineFuture {
        Box::pin(std::future::pending())
    }
}
#[derive(Clone, Default)]
struct Reports(
    Arc<Mutex<Vec<FinalizationRecord>>>,
    Arc<std::sync::atomic::AtomicBool>,
);
impl FinalizationReporter for Reports {
    fn report(&self, r: &FinalizationRecord) -> Result<(), ReportingError> {
        if self.1.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ReportingError::PermanentFailure);
        }
        self.0.lock().unwrap().push(r.clone());
        Ok(())
    }
}
#[derive(Clone, Default)]
struct Inputs(Arc<Mutex<Vec<EventBatch>>>);
impl IngressReporter for Inputs {
    fn report(&mut self, b: &EventBatch, _: &mut IngressOutcome) -> Result<(), ReportingError> {
        self.0.lock().unwrap().push(b.clone());
        Ok(())
    }
}
struct Adapter {
    descriptor: DestinationDescriptor,
    seen: Mutex<Vec<EventBatch>>,
    stall: bool,
}
impl EventDestinationPort for Adapter {
    fn descriptor(&self) -> DestinationDescriptor {
        self.descriptor
    }
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(batch.clone());
            if self.stall {
                std::future::pending::<()>().await;
            }
            DestinationAdmission::Accepted {
                destination: self.descriptor.id,
                through: batch.last_cursor().clone(),
            }
        })
    }
}
fn batch(offset: u64) -> EventBatch {
    EventBatch::new(vec![l2_event("owned-router", 1, offset)]).unwrap()
}
fn ready<F: Future>(f: F) -> F::Output {
    match std::pin::pin!(f)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected pending"),
    }
}
fn runtime(workers: usize) -> (RouterRuntime<Reports, Inputs>, Reports, Inputs) {
    let clock = Arc::new(ManualClock::new(Instant::now(), 0));
    let metrics = NativeMetrics::new(clock.clone(), 1, 6, true).unwrap();
    let metrics = metrics
        .register(MetricBinding {
            owner: ResourceVersion {
                resource: ResourceId::Runtime,
                generation: 1,
            },
            reset_epoch: 0,
            reset: ResetBoundary::OwnerGeneration,
            metrics: ROUTER_METRICS.to_vec(),
        })
        .unwrap();
    let limit = NonZeroUsize::new(workers).unwrap();
    let reports = Reports::default();
    let inputs = Inputs::default();
    let runtime = RouterRuntime::new(
        RouterResources {
            destinations: RouterRegistry::with_destination_limit(4).unwrap(),
            namespaces: AdmissionNamespaces::new(1).unwrap(),
            admission: OrderedAdmissionBudget::new(ResourceUsage {
                items: 4,
                bytes: 100_000,
            })
            .unwrap(),
            outcomes: IngressOutcomes::new(ResourceUsage {
                items: 4,
                bytes: 100_000,
            })
            .unwrap(),
            delivery: DeliveryDriver::new(
                DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
                AcknowledgementTracker::new(8).unwrap(),
                TrackingRecovery::new(8).unwrap(),
                FinalizationJournal::new(8).unwrap(),
            ),
        }
        .validate()
        .map_err(|(reason, _)| reason)
        .unwrap(),
        RouterServices {
            clock: clock.clone(),
            timer: Arc::new(Timer),
            wall: clock.clone(),
            metrics: Arc::new(metrics),
        },
        FinalizationReportingConsumer::new(
            reports.clone(),
            Duration::from_secs(1),
            FinalizationRetryPolicy {
                initial_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(2),
                max_attempts: 2,
            },
        )
        .unwrap(),
        ScheduledIngressReporting::new(Duration::from_secs(1), limit).unwrap(),
        inputs.clone(),
        RouterMetricSampler::new(
            Duration::from_secs(1),
            Some(marketforge_engine::live::foundation::MonotonicClock::now(
                clock.as_ref(),
            )),
        )
        .unwrap(),
        NonZeroU64::new(1_000_000_000).unwrap(),
    );
    runtime
        .register_namespace(batch(1).first_cursor().clone())
        .unwrap();
    (runtime, reports, inputs)
}
fn adapter(id: u64, policy: DeliveryPolicy, stall: bool) -> Arc<Adapter> {
    Arc::new(Adapter {
        descriptor: DestinationDescriptor {
            id: DestinationId(id),
            kind: DestinationKind::Publication,
            policy,
            capacity: DeliveryCapacity::new(4, 100_000).unwrap(),
        },
        seen: Mutex::default(),
        stall,
    })
}
fn complete(runtime: &RouterRuntime<Reports, Inputs>, id: u64) {
    runtime
        .apply_completion(&DestinationCompletion {
            destination: DestinationId(id),
            through: batch(1).last_cursor().clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
        })
        .unwrap();
}
#[test]
fn composed_router_owned_port_delivery_completion_reporting_and_metrics() {
    let (mut runtime, reports, inputs) = runtime(4);
    let adapter = adapter(1, DeliveryPolicy::Required, false);
    runtime.register_destination(adapter.clone(), 2).unwrap();
    let producer: Arc<dyn EventRouterPort> = Arc::new(runtime.producer(None));
    let accepted = batch(1);
    assert!(matches!(
        ready(producer.admit(accepted.clone())),
        RoutingAdmission::Accepted { .. }
    ));
    let mut cx = Context::from_waker(Waker::noop());
    let first = runtime.drive(&mut cx);
    assert!(
        matches!(first.metrics, Poll::Ready(Ok(Some(values))) if values.iter().all(|(_, v)| v.is_ok()))
    );
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(inputs.0.lock().unwrap().as_slice(), &[accepted.clone()]);
    let seen = adapter.seen.lock().unwrap();
    assert_eq!(seen.as_slice(), &[accepted.clone()]);
    assert!(Arc::ptr_eq(&seen[0].events()[0], &accepted.events()[0]));
    drop(seen);
    assert_eq!(
        reports.0.lock().unwrap().len(),
        0,
        "admission is not completion"
    );
    complete(&runtime, 1);
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(reports.0.lock().unwrap().len(), 1);
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    for _ in 0..8 {
        if matches!(
            runtime.shutdown(&mut cx, &mut deadline),
            RouterStop::Drained
        ) {
            assert!(runtime.snapshot().is_drained());
            assert!(matches!(
                ready(producer.admit(batch(2))),
                RoutingAdmission::Rejected { .. }
            ));
            return;
        }
    }
    panic!("failed drain: {:?}", runtime.snapshot());
}
#[test]
fn composed_router_deadline_retains_unclaimed_queues_and_resumes_shutdown() {
    let (mut runtime, reports, _) = runtime(2);
    let one = adapter(1, DeliveryPolicy::Required, false);
    let two = adapter(2, DeliveryPolicy::Required, false);
    runtime.register_destination(one.clone(), 2).unwrap();
    runtime.register_destination(two.clone(), 2).unwrap();
    let producer = runtime.producer(None);
    assert!(matches!(
        ready(producer.admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let mut cx = Context::from_waker(Waker::noop());
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        runtime.shutdown(&mut cx, &mut expired),
        RouterStop::DeadlineReached
    ));
    assert!(one.seen.lock().unwrap().is_empty() && two.seen.lock().unwrap().is_empty());
    assert!(!runtime.snapshot().is_drained());
    assert!(matches!(
        ready(producer.admit(batch(2))),
        RoutingAdmission::Rejected { .. }
    ));
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    let mut completed = [false; 2];
    for _ in 0..30 {
        let _ = runtime.shutdown(&mut cx, &mut deadline);
        for (i, adapter) in [&one, &two].into_iter().enumerate() {
            if !completed[i] && !adapter.seen.lock().unwrap().is_empty() {
                complete(&runtime, adapter.descriptor.id.0);
                completed[i] = true;
            }
        }
        if runtime.snapshot().is_drained() {
            assert_eq!(reports.0.lock().unwrap().len(), 2);
            return;
        }
    }
    panic!("failed capacity limited drain: {:?}", runtime.snapshot());
}
#[test]
fn composed_router_stalled_optional_adapter_preserves_required_progress() {
    let (mut runtime, reports, _) = runtime(4);
    let optional = adapter(1, DeliveryPolicy::BestEffort, true);
    let required = adapter(2, DeliveryPolicy::Required, false);
    runtime.register_destination(optional.clone(), 2).unwrap();
    runtime.register_destination(required.clone(), 2).unwrap();
    assert!(matches!(
        ready(runtime.producer(None).admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..8 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(required.seen.lock().unwrap().len(), 1);
    assert_eq!(optional.seen.lock().unwrap().len(), 1);
    complete(&runtime, 2);
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(reports.0.lock().unwrap().len(), 1);
    assert_eq!(runtime.snapshot().ingress.journal.usage.items, 0);
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        runtime.shutdown(&mut cx, &mut expired),
        RouterStop::DeadlineReached
    ));
}

#[test]
fn composed_router_failed_delivery_payload_survives_reporting_and_recovery_transfer() {
    use marketforge_engine::live::model::delivery::DeliveryFailure;
    let (mut runtime, reports, _) = runtime(4);
    runtime
        .register_destination(adapter(1, DeliveryPolicy::Required, false), 2)
        .unwrap();
    assert!(matches!(
        ready(runtime.producer(None).admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    runtime
        .apply_completion(&DestinationCompletion {
            destination: DestinationId(1),
            through: batch(1).last_cursor().clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::Disconnected),
        })
        .unwrap();
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(reports.0.lock().unwrap().len(), 1);
    assert_eq!(runtime.failed_payloads_len(), 1);
    let before = runtime.snapshot();
    assert_eq!(
        before.delivery.destinations.destinations[0]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .occupied_events,
        1
    );
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        runtime.shutdown(&mut cx, &mut expired),
        RouterStop::DeadlineReached
    ));
    let recovery = runtime.take_failed_payload().unwrap();
    assert_eq!(recovery.payload.batch(), &batch(1));
    assert!(
        !runtime.snapshot().is_drained(),
        "transferring responsibility retains the queue charge"
    );
    drop(recovery); // Explicit test recovery consumer releases its responsibility.
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    for _ in 0..8 {
        if matches!(
            runtime.shutdown(&mut cx, &mut deadline),
            RouterStop::Drained
        ) {
            return;
        }
    }
    panic!("recovery transfer did not drain: {:?}", runtime.snapshot());
}

#[test]
fn composed_router_registration_reserves_scheduler_slots_for_independent_sinks() {
    use marketforge_engine::live::routing::router::RouterRegistryError;
    let (mut runtime, _, _) = runtime(1);
    runtime
        .register_destination(adapter(1, DeliveryPolicy::BestEffort, true), 2)
        .unwrap();
    let before = runtime.snapshot().delivery.destinations.revision;
    assert_eq!(
        runtime.register_destination(adapter(2, DeliveryPolicy::Required, false), 2),
        Err(RouterRegistryError::DestinationCapacity)
    );
    assert_eq!(runtime.snapshot().delivery.destinations.revision, before);
    assert_eq!(
        runtime.snapshot().delivery.destinations.destinations.len(),
        1
    );
}

#[test]
fn composed_router_no_subscribers_preserves_order_and_expired_admission_has_no_effect() {
    let (mut runtime, _, inputs) = runtime(4);
    let expired = runtime.producer(Some(Duration::ZERO));
    assert!(matches!(
        ready(expired.admit(batch(1))),
        RoutingAdmission::Rejected { .. }
    ));
    let port = runtime.producer(None);
    assert!(matches!(
        ready(port.admit(batch(1))),
        RoutingAdmission::Rejected { .. }
    ));
    assert_eq!(
        runtime.snapshot().delivery.namespaces.namespaces[0]
            .order
            .next
            .as_ref()
            .unwrap()
            .offset,
        1
    );
    runtime
        .register_destination(adapter(1, DeliveryPolicy::BestEffort, false), 2)
        .unwrap();
    assert!(matches!(
        ready(port.admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    assert!(matches!(
        ready(port.admit(batch(2))),
        RoutingAdmission::Accepted { .. }
    ));
    assert!(matches!(
        ready(port.admit(batch(1))),
        RoutingAdmission::Rejected { .. }
    ));
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..8 {
        let _ = runtime.drive(&mut cx);
    }
    let inputs = inputs.0.lock().unwrap();
    assert_eq!(inputs[0], batch(1));
    assert_eq!(inputs[1], batch(2));
    assert_eq!(
        runtime.snapshot().delivery.admission_usage,
        ResourceUsage::default()
    );
}

#[test]
fn composed_router_optional_admission_outcomes_never_poison_required_correlation() {
    use marketforge_engine::live::model::delivery::DeliveryFailure;
    use marketforge_engine::live::routing::worker::WorkerAdmissionResult;
    struct OptionalResponse(u8);
    impl EventDestinationPort for OptionalResponse {
        fn descriptor(&self) -> DestinationDescriptor {
            adapter(1, DeliveryPolicy::BestEffort, false).descriptor
        }
        fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
            Box::pin(async move {
                match self.0 {
                    0 => DestinationAdmission::Accepted {
                        destination: DestinationId(1),
                        through: batch.last_cursor().clone(),
                    },
                    1 => DestinationAdmission::Rejected {
                        destination: DestinationId(1),
                        reason: DeliveryFailure::Disconnected,
                    },
                    _ => DestinationAdmission::Rejected {
                        destination: DestinationId(999),
                        reason: DeliveryFailure::Internal,
                    },
                }
            })
        }
    }
    for response in 0..3 {
        let (mut runtime, reports, _) = runtime(4);
        runtime
            .register_destination(Arc::new(OptionalResponse(response)), 2)
            .unwrap();
        let required = adapter(2, DeliveryPolicy::Required, false);
        runtime.register_destination(required.clone(), 2).unwrap();
        assert!(matches!(
            ready(runtime.producer(None).admit(batch(1))),
            RoutingAdmission::Accepted { .. }
        ));
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..8 {
            let _ = runtime.drive(&mut cx);
        }
        assert_eq!(required.seen.lock().unwrap().len(), 1);
        assert_eq!(runtime.snapshot().delivery.delivery.blocked_outcome, None);
        assert_eq!(runtime.optional_outcomes_len(), 1);
        complete(&runtime, 2);
        for _ in 0..4 {
            let _ = runtime.drive(&mut cx);
        }
        assert_eq!(reports.0.lock().unwrap().len(), 1);
        let optional = runtime.take_optional_outcome().unwrap();
        assert_eq!(optional.payload.batch(), &batch(1));
        assert!(
            matches!(
                optional.admission,
                WorkerAdmissionResult::InvalidResponse(_)
            ) == (response == 2)
        );
        let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
        assert!(matches!(
            runtime.shutdown(&mut cx, &mut expired),
            RouterStop::DeadlineReached
        ));
        drop(optional); // Explicit reporting consumer releases its original charge.
        let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
        let mut drained = false;
        for _ in 0..8 {
            if matches!(
                runtime.shutdown(&mut cx, &mut deadline),
                RouterStop::Drained
            ) {
                drained = true;
                break;
            }
        }
        assert!(
            drained,
            "optional outcome {response} prevented drain: {:?}",
            runtime.snapshot()
        );
    }
}

#[test]
fn composed_router_invalid_resource_transfer_returns_queued_responsibility() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue,
    };
    let registry = RouterRegistry::new();
    for id in 1..=2 {
        let adapter = adapter(id, DeliveryPolicy::Required, false);
        let queue = DestinationQueue::new(
            DestinationCapacity::new(adapter.descriptor.capacity, 2).unwrap(),
        );
        queue.try_enqueue(batch(1)).unwrap();
        registry.register_queue(adapter, queue).unwrap();
    }
    let limit = NonZeroUsize::new(1).unwrap();
    let resources = RouterResources {
        destinations: registry,
        namespaces: AdmissionNamespaces::new(1).unwrap(),
        admission: OrderedAdmissionBudget::new(ResourceUsage {
            items: 4,
            bytes: 100_000,
        })
        .unwrap(),
        outcomes: IngressOutcomes::new(ResourceUsage {
            items: 4,
            bytes: 100_000,
        })
        .unwrap(),
        delivery: DeliveryDriver::new(
            DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
            AcknowledgementTracker::new(8).unwrap(),
            TrackingRecovery::new(8).unwrap(),
            FinalizationJournal::new(8).unwrap(),
        ),
    };
    let Err((reason, resources)) = resources.validate() else {
        panic!("insufficient scheduler capacity accepted");
    };
    assert_eq!(
        reason,
        marketforge_engine::live::model::error::ErrorCode::InvalidInput
    );
    for id in 1..=2 {
        let queue = resources
            .destinations
            .peek_queue(DestinationId(id))
            .unwrap();
        assert_eq!(queue.snapshot().queued_events, 1);
        assert_eq!(queue.capacity().usage().occupied_events, 1);
    }
}

fn pending_id(
    runtime: &mut RouterRuntime<Reports, Inputs>,
) -> marketforge_engine::live::routing::acknowledgement::DeliveryObligationId {
    use marketforge_engine::live::routing::{
        runtime::RuntimeDeliveryTurn, supervisor::SupervisionStep,
    };
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..8 {
        if let RuntimeDeliveryTurn::Driven(turn) = runtime.drive(&mut cx).delivery {
            if let Poll::Ready(SupervisionStep::Retained(id)) = turn.delivery.delivery {
                return id;
            }
        }
    }
    panic!("no pending delivery: {:?}", runtime.snapshot());
}

#[test]
fn composed_router_recovery_return_after_shutdown_keeps_identity_and_completes_once() {
    let (mut runtime, reports, _) = runtime(4);
    let adapter = adapter(1, DeliveryPolicy::Required, false);
    runtime.register_destination(adapter.clone(), 2).unwrap();
    assert!(matches!(
        ready(runtime.producer(None).admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let id = pending_id(&mut runtime);
    let delivery = runtime.take_delivery_for_recovery(id).unwrap();
    assert_eq!(delivery.payload().batch(), &batch(1));
    assert_eq!(runtime.snapshot().delivery.delivery.tracked_obligations, 1);
    let mut cx = Context::from_waker(Waker::noop());
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        runtime.shutdown(&mut cx, &mut expired),
        RouterStop::DeadlineReached
    ));
    runtime.restore_delivery_from_recovery(delivery).unwrap();
    assert_eq!(runtime.snapshot().delivery.delivery.retained_deliveries, 1);
    complete(&runtime, 1);
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    for _ in 0..8 {
        if matches!(
            runtime.shutdown(&mut cx, &mut deadline),
            RouterStop::Drained
        ) {
            assert_eq!(reports.0.lock().unwrap().len(), 1);
            assert_eq!(reports.0.lock().unwrap()[0].obligation.id, id);
            assert_eq!(
                adapter.seen.lock().unwrap().len(),
                1,
                "restoration must not replay the adapter"
            );
            return;
        }
    }
    panic!("restored recovery did not drain: {:?}", runtime.snapshot());
}

#[test]
fn composed_router_recovery_return_rejects_full_and_foreign_owner_without_losing_charge() {
    use marketforge_engine::live::routing::completion::{CompletionError, PendingDeliveryError};
    let (mut original, _, _) = runtime(1);
    original
        .register_destination(adapter(1, DeliveryPolicy::Required, false), 2)
        .unwrap();
    for offset in 1..=2 {
        assert!(matches!(
            ready(original.producer(None).admit(batch(offset))),
            RoutingAdmission::Accepted { .. }
        ));
    }
    let first = pending_id(&mut original);
    let delivery = original.take_delivery_for_recovery(first).unwrap();
    let second = pending_id(&mut original);
    let (error, delivery) = original
        .restore_delivery_from_recovery(delivery)
        .unwrap_err();
    assert_eq!(error, PendingDeliveryError::Full);
    assert_eq!(delivery.obligation().id, first);
    let (mut foreign, _, _) = runtime(1);
    foreign
        .register_destination(adapter(1, DeliveryPolicy::Required, false), 2)
        .unwrap();
    assert!(matches!(
        ready(foreign.producer(None).admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let foreign_id = pending_id(&mut foreign);
    let foreign_delivery = foreign.take_delivery_for_recovery(foreign_id).unwrap();
    assert_eq!(
        delivery.obligation(),
        foreign_delivery.obligation(),
        "equal metadata must not substitute for owner identity"
    );
    let (error, delivery) = foreign
        .restore_delivery_from_recovery(delivery)
        .unwrap_err();
    assert_eq!(
        error,
        PendingDeliveryError::Completion(CompletionError::ForeignTracker)
    );
    assert_eq!(delivery.payload().batch(), &batch(1));
    assert_eq!(
        original.snapshot().delivery.destinations.destinations[0]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .occupied_events,
        2
    );
    let held_second = original.take_delivery_for_recovery(second).unwrap();
    original.restore_delivery_from_recovery(delivery).unwrap();
    complete(&original, 1);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..4 {
        let _ = original.drive(&mut cx);
    }
    original
        .restore_delivery_from_recovery(held_second)
        .unwrap();
    original
        .apply_completion(&DestinationCompletion {
            destination: DestinationId(1),
            through: batch(2).last_cursor().clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
        })
        .unwrap();
    foreign
        .restore_delivery_from_recovery(foreign_delivery)
        .unwrap();
    complete(&foreign, 1);
    for _ in 0..4 {
        let _ = original.drive(&mut cx);
        let _ = foreign.drive(&mut cx);
    }
    assert_eq!(original.snapshot().delivery.delivery.tracked_obligations, 0);
    assert_eq!(foreign.snapshot().delivery.delivery.tracked_obligations, 0);
}

#[test]
fn composed_router_recording_durability_requires_separate_exact_completion() {
    let (mut runtime, reports, _) = runtime(4);
    let mut recording = adapter(1, DeliveryPolicy::Durable, false);
    Arc::get_mut(&mut recording).unwrap().descriptor.kind = DestinationKind::Recording;
    runtime.register_destination(recording.clone(), 2).unwrap();
    assert!(matches!(
        ready(runtime.producer(None).admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let id = pending_id(&mut runtime);
    let completion = |stage, offset| DestinationCompletion {
        destination: DestinationId(1),
        through: batch(offset).last_cursor().clone(),
        status: DeliveryStatus::Progress(stage),
    };
    runtime
        .apply_completion(&completion(DeliveryStage::Committed, 1))
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    assert!(
        reports.0.lock().unwrap().is_empty(),
        "commit is not configured durability"
    );
    assert_eq!(runtime.snapshot().delivery.delivery.tracked_obligations, 1);
    assert!(
        runtime
            .apply_completion(&completion(DeliveryStage::Durable, 2))
            .is_err()
    );
    assert_eq!(runtime.snapshot().delivery.delivery.tracked_obligations, 1);
    runtime
        .apply_completion(&completion(DeliveryStage::Durable, 1))
        .unwrap();
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    let reports = reports.0.lock().unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].obligation.id, id);
    assert_eq!(reports[0].obligation.policy, DeliveryPolicy::Durable);
    assert_eq!(
        reports[0].obligation.range.last,
        batch(1).last_cursor().clone()
    );
    assert_eq!(recording.seen.lock().unwrap().len(), 1);
}

#[test]
fn composed_router_dropped_and_shutdown_cancelled_waits_release_every_precommit_charge() {
    let (mut runtime, _, _) = runtime(4);
    runtime
        .register_destination(adapter(1, DeliveryPolicy::Required, false), 2)
        .unwrap();
    let producer = runtime.producer(None);
    for offset in 1..=4 {
        assert!(matches!(
            ready(producer.admit(batch(offset))),
            RoutingAdmission::Accepted { .. }
        ));
    }
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..16 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(runtime.snapshot().ingress.journal.usage.items, 0);
    let mut waiting = producer.admit(batch(5));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    let before = runtime.snapshot();
    assert_eq!(before.delivery.admission_usage.items, 1);
    assert_eq!(before.ingress.journal.usage.items, 1);
    assert_eq!(
        before.delivery.destinations.destinations[0]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .waiting_operations,
        1
    );
    drop(waiting);
    let after = runtime.snapshot();
    assert_eq!(after.delivery.admission_usage, ResourceUsage::default());
    assert_eq!(after.ingress.journal.usage, ResourceUsage::default());
    assert_eq!(
        after.delivery.namespaces.namespaces[0]
            .order
            .next
            .as_ref()
            .unwrap()
            .offset,
        5
    );
    let mut waiting = producer.admit(batch(5));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        runtime.shutdown(&mut cx, &mut expired),
        RouterStop::DeadlineReached
    ));
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(RoutingAdmission::Rejected { .. })
    ));
    drop(waiting);
    let closed = runtime.snapshot();
    assert_eq!(closed.delivery.admission_usage, ResourceUsage::default());
    assert_eq!(closed.ingress.journal.usage, ResourceUsage::default());
    assert_eq!(
        closed.delivery.delivery.tracked_obligations, 4,
        "cancel cannot revoke accepted work"
    );
    assert_eq!(
        closed.delivery.destinations.destinations[0]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .occupied_events,
        4
    );
}

#[test]
fn composed_router_concurrent_producer_clones_share_one_frontier() {
    let (mut runtime, _, _) = runtime(4);
    runtime
        .register_destination(adapter(1, DeliveryPolicy::Required, false), 2)
        .unwrap();
    let producer: Arc<dyn EventRouterPort> = Arc::new(runtime.producer(None));
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let outcomes = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let producer = producer.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    ready(producer.admit(batch(1)))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        outcomes
            .iter()
            .filter(|v| matches!(v, RoutingAdmission::Accepted { .. }))
            .count(),
        1
    );
    let snapshot = runtime.snapshot();
    assert_eq!(
        snapshot.delivery.namespaces.namespaces[0]
            .order
            .next
            .as_ref()
            .unwrap()
            .offset,
        2
    );
    assert_eq!(
        snapshot.delivery.destinations.destinations[0]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .occupied_events,
        1
    );
    assert_eq!(snapshot.delivery.delivery.tracked_obligations, 1);
    assert_eq!(snapshot.ingress.journal.usage.items, 1);
}

#[test]
fn composed_router_owner_drop_fences_surviving_producer_handles() {
    let (runtime, _, _) = runtime(4);
    let producer = runtime.producer(None);
    drop(runtime);
    assert!(matches!(
        ready(producer.admit(batch(1))),
        RoutingAdmission::Rejected {
            reason: marketforge_engine::live::ports::routing::RoutingAdmissionFailure::ShuttingDown
        }
    ));
}

#[test]
fn composed_router_operational_resolution_requires_sink_acceptance_and_keeps_failed_payload() {
    use marketforge_engine::live::{
        model::delivery::DeliveryFailure,
        routing::{
            acknowledgement::{EscalationResolution, ObligationFinalization},
            resolution::{BoundedResolutionSink, ResolutionAcceptanceSink},
        },
    };
    let (mut runtime, reports, _) = runtime(4);
    reports.1.store(true, std::sync::atomic::Ordering::Relaxed);
    runtime
        .register_destination(adapter(1, DeliveryPolicy::Required, false), 2)
        .unwrap();
    assert!(matches!(
        ready(runtime.producer(None).admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let id = pending_id(&mut runtime);
    runtime
        .apply_completion(&DestinationCompletion {
            destination: DestinationId(1),
            through: batch(1).last_cursor().clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::Disconnected),
        })
        .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..4 {
        let _ = runtime.drive(&mut cx);
    }
    let escalated = runtime.escalated_terminal_reports();
    assert_eq!(escalated.len(), 1);
    let prepared = runtime
        .prepare_terminal_resolution(id, EscalationResolution::FailureAcknowledged)
        .unwrap();
    assert_eq!(prepared.record.record, escalated[0].record);
    assert_eq!(runtime.snapshot().delivery.delivery.journal_records, 1);
    let sink = BoundedResolutionSink::new(1).unwrap();
    let foreign = BoundedResolutionSink::new(1).unwrap();
    let receipt = sink.accept(prepared.token, &prepared.record).unwrap();
    assert!(
        runtime
            .commit_terminal_resolution(id, &receipt, &foreign)
            .is_err()
    );
    assert_eq!(runtime.snapshot().delivery.delivery.journal_records, 1);
    let resolved = runtime
        .commit_terminal_resolution(id, &receipt, &sink)
        .unwrap();
    assert!(matches!(
        resolved.record.outcome,
        ObligationFinalization::Failed {
            reason: DeliveryFailure::Disconnected,
            ..
        }
    ));
    assert_eq!(
        resolved.record.obligation.range.last,
        batch(1).last_cursor().clone()
    );
    assert_eq!(
        sink.len(),
        1,
        "operational consumer retains accepted responsibility"
    );
    assert_eq!(runtime.snapshot().delivery.delivery.journal_records, 0);
    assert_eq!(
        runtime.failed_payloads_len(),
        1,
        "acknowledging failure is not payload repair"
    );
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        runtime.shutdown(&mut cx, &mut expired),
        RouterStop::DeadlineReached
    ));
    let payload = runtime.take_failed_payload().unwrap();
    assert_eq!(payload.payload.batch(), &batch(1));
    assert!(!runtime.snapshot().is_drained());
    drop(payload); // Explicit recovery-consumer release, never evidence of repair.
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    for _ in 0..8 {
        if matches!(
            runtime.shutdown(&mut cx, &mut deadline),
            RouterStop::Drained
        ) {
            return;
        }
    }
    panic!(
        "local router obligations did not drain: {:?}",
        runtime.snapshot()
    );
}

#[test]
fn composed_router_namespace_retirement_and_measurement_update_preserve_mandatory_state() {
    use marketforge_engine::live::routing::namespaces::NamespaceError;
    let (mut runtime, _, _) = runtime(4);
    runtime
        .register_destination(adapter(1, DeliveryPolicy::Required, false), 2)
        .unwrap();
    let producer = runtime.producer(None);
    assert!(matches!(
        ready(producer.admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    let before = runtime.snapshot();
    runtime.schedule_metrics(None);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        runtime.drive(&mut cx).metrics,
        Poll::Ready(Ok(None))
    ));
    assert_eq!(
        runtime.snapshot().delivery.destinations.destinations[0]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .occupied_events,
        before.delivery.destinations.destinations[0]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .occupied_events
    );
    let cursor = batch(1).first_cursor().clone();
    runtime
        .retire_namespace(&cursor.stream_id, cursor.log_epoch)
        .unwrap();
    assert!(matches!(
        ready(producer.admit(batch(2))),
        RoutingAdmission::Rejected { .. }
    ));
    assert_eq!(
        runtime.register_namespace(cursor),
        Err(NamespaceError::AlreadyRegistered)
    );
    let snapshot = runtime.snapshot();
    assert!(snapshot.delivery.namespaces.namespaces[0].retired);
    assert_eq!(
        snapshot.delivery.namespaces.namespaces[0]
            .order
            .next
            .as_ref()
            .unwrap()
            .offset,
        2
    );
    assert_eq!(snapshot.delivery.delivery.tracked_obligations, 1);
}

#[test]
fn composed_router_destination_replacement_drains_original_instance_and_rejects_stale_completion() {
    let (mut runtime, reports, _) = runtime(4);
    let publication = adapter(1, DeliveryPolicy::Required, false);
    let old = adapter(2, DeliveryPolicy::Required, false);
    let new = adapter(2, DeliveryPolicy::Required, false);
    runtime
        .register_destination(publication.clone(), 2)
        .unwrap();
    runtime.register_destination(old.clone(), 2).unwrap();
    let producer = runtime.producer(None);
    assert!(matches!(
        ready(producer.admit(batch(1))),
        RoutingAdmission::Accepted { .. }
    ));
    runtime.unregister_destination(DestinationId(2)).unwrap();
    assert_eq!(runtime.snapshot().delivery.destinations.retired.len(), 1);
    runtime.register_destination(new.clone(), 2).unwrap();
    assert!(matches!(
        ready(producer.admit(batch(2))),
        RoutingAdmission::Accepted { .. }
    ));
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..16 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(old.seen.lock().unwrap().as_slice(), &[batch(1)]);
    assert_eq!(new.seen.lock().unwrap().as_slice(), &[batch(2)]);
    assert_eq!(
        publication.seen.lock().unwrap().as_slice(),
        &[batch(1), batch(2)]
    );
    let old_completion = DestinationCompletion {
        destination: DestinationId(2),
        through: batch(1).last_cursor().clone(),
        status: DeliveryStatus::Progress(DeliveryStage::Delivered),
    };
    runtime.apply_completion(&old_completion).unwrap();
    for _ in 0..8 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(runtime.snapshot().delivery.delivery.tracked_obligations, 3);
    assert!(runtime.apply_completion(&old_completion).is_err());
    assert_eq!(runtime.snapshot().delivery.delivery.tracked_obligations, 3);
    assert_eq!(runtime.snapshot().delivery.destinations.retired.len(), 0);
    for (id, offset) in [(1, 1), (1, 2), (2, 2)] {
        runtime
            .apply_completion(&DestinationCompletion {
                destination: DestinationId(id),
                through: batch(offset).last_cursor().clone(),
                status: DeliveryStatus::Progress(DeliveryStage::Delivered),
            })
            .unwrap();
    }
    for _ in 0..12 {
        let _ = runtime.drive(&mut cx);
    }
    assert_eq!(reports.0.lock().unwrap().len(), 4);
    assert_eq!(runtime.snapshot().delivery.delivery.tracked_obligations, 0);
    assert_eq!(
        runtime.snapshot().delivery.destinations.destinations[1]
            .queue
            .as_ref()
            .unwrap()
            .capacity
            .occupied_events,
        0
    );
}

#[test]
fn composed_router_resource_transfer_fences_equal_descriptor_foreign_worker_queues() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue, worker::DestinationWorker,
    };
    for same_owner in [false, true] {
        let sink = adapter(1, DeliveryPolicy::Required, false);
        let queue = || {
            DestinationQueue::new(DestinationCapacity::new(sink.descriptor.capacity, 2).unwrap())
        };
        let worker_queue = queue();
        worker_queue.try_enqueue(batch(1)).unwrap();
        let registered_queue = if same_owner {
            worker_queue.clone()
        } else {
            queue()
        };
        let registry = RouterRegistry::new();
        registry
            .register_queue(sink.clone(), registered_queue)
            .unwrap();
        let limit = NonZeroUsize::new(1).unwrap();
        let mut workers = DestinationWorkers::new(limit);
        workers
            .insert(DestinationWorker::new(worker_queue.clone(), sink).unwrap())
            .unwrap_or_else(|_| panic!("worker registration"));
        let resources = RouterResources {
            destinations: registry,
            namespaces: AdmissionNamespaces::new(1).unwrap(),
            admission: OrderedAdmissionBudget::new(ResourceUsage {
                items: 4,
                bytes: 100_000,
            })
            .unwrap(),
            outcomes: IngressOutcomes::new(ResourceUsage {
                items: 4,
                bytes: 100_000,
            })
            .unwrap(),
            delivery: DeliveryDriver::new(
                DeliverySupervisor::new(workers, limit),
                AcknowledgementTracker::new(8).unwrap(),
                TrackingRecovery::new(8).unwrap(),
                FinalizationJournal::new(8).unwrap(),
            ),
        };
        match resources.validate() {
            Ok(_validated) => assert!(same_owner),
            Err((reason, resources)) => {
                assert!(!same_owner);
                assert_eq!(
                    reason,
                    marketforge_engine::live::model::error::ErrorCode::InvalidInput
                );
                assert_eq!(resources.delivery.snapshot().workers.workers.len(), 1);
                assert_eq!(worker_queue.snapshot().queued_events, 1);
                assert_eq!(worker_queue.capacity().usage().occupied_events, 1);
            }
        }
    }
}
