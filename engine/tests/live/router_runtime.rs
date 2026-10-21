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
struct Reports(Arc<Mutex<Vec<FinalizationRecord>>>);
impl FinalizationReporter for Reports {
    fn report(&self, r: &FinalizationRecord) -> Result<(), ReportingError> {
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
