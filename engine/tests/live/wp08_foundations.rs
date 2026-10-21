//! Small composition root exercising real foundation components through ports.
use marketforge_engine::live::{
    foundation::{
        CancellationToken,
        clock::{ManualClock, MonotonicClock},
        deadline::ManualDeadlineTimer,
        resources::{ResourceBudget, ResourceUsage},
        shutdown::BoundedTaskSpawner,
    },
    metrics::registry::NativeMetrics,
    model::{
        error::{ControlError, ErrorCode},
        observation::*,
        operation::{ResourceId, ResourceVersion},
    },
    ports::{
        lifecycle::{TaskHandlePort, TaskSpawnerPort},
        observability::ScopedMetricsPort,
        routing::RoutingFuture,
    },
    routing::admission::{AdmissionContext, AdmissionWait, AdmissionWaitResult},
};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn poll<T>(future: std::pin::Pin<&mut (impl Future<Output = T> + ?Sized)>) -> Poll<T> {
    let waker = Waker::from(Arc::new(Noop));
    future.poll(&mut Context::from_waker(&waker))
}
fn binding() -> MetricBinding {
    MetricBinding {
        owner: ResourceVersion {
            resource: ResourceId::Runtime,
            generation: 1,
        },
        reset_epoch: 0,
        reset: ResetBoundary::RuntimeInstance,
        metrics: vec![MetricId::FailedTasksTotal],
    }
}
#[test]
fn deadline_cancellation_resource_and_observation_composition() {
    let now = Instant::now();
    let clock = Arc::new(ManualClock::new(now, 0));
    let timer = ManualDeadlineTimer::new(now, 2).unwrap();
    let metrics = NativeMetrics::new(clock.clone(), 2, 4, true).unwrap();
    let handle = metrics.register(binding()).unwrap();
    let token = CancellationToken::with_waiter_limit(2).unwrap();
    let budget = ResourceBudget::new(ResourceUsage {
        items: 1,
        bytes: 64,
    })
    .unwrap();
    let reservation = budget
        .reserve(ResourceUsage {
            items: 1,
            bytes: 64,
        })
        .unwrap();
    let operation = async move {
        let _reservation = reservation;
        std::future::pending::<()>().await
    };
    let mut waiting = Box::pin(
        AdmissionWait::new(
            operation,
            AdmissionContext::new(token.clone(), Some(now + Duration::from_secs(1))),
            Some(&timer),
        )
        .unwrap(),
    );
    assert!(poll(waiting.as_mut()).is_pending());
    clock.set_wall_ns(i64::MIN);
    assert!(poll(waiting.as_mut()).is_pending());
    clock.advance(Duration::from_secs(1)).unwrap();
    timer.advance_to(clock.now()).unwrap();
    assert!(matches!(
        poll(waiting.as_mut()),
        Poll::Ready(AdmissionWaitResult::Rejected(_))
    ));
    drop(waiting);
    assert_eq!(budget.usage(), ResourceUsage::default());
    handle
        .record(
            MetricId::FailedTasksTotal,
            Measurement::Available {
                value: MetricValue::Counter(1),
                observed_at_ns: 0,
            },
        )
        .unwrap();
    assert_eq!(metrics.snapshot(Duration::from_secs(1)).len(), 1);
    let mut waiting = Box::pin(
        AdmissionWait::new(
            std::future::pending::<()>(),
            AdmissionContext::without_deadline(token.clone()),
            None,
        )
        .unwrap(),
    );
    assert!(poll(waiting.as_mut()).is_pending());
    token.cancel();
    assert!(matches!(
        poll(waiting.as_mut()),
        Poll::Ready(AdmissionWaitResult::Rejected(_))
    ));
}
#[test]
fn bounded_task_port_with_reusable_fault_script() {
    use super::support::failure::{FaultPlan, FaultPoint};
    struct Failing {
        faults: FaultPlan,
    }
    impl TaskSpawnerPort for Failing {
        fn spawn(
            &self,
            _: ResourceVersion,
            _: RoutingFuture<'static, Result<(), ControlError>>,
        ) -> Result<Arc<dyn TaskHandlePort>, ControlError> {
            self.faults
                .check(FaultPoint::Spawn)
                .map_err(|e| ControlError::new(e, None))?;
            Err(ControlError::new(ErrorCode::UnsupportedCapability, None))
        }
    }
    let adapter = Arc::new(Failing {
        faults: FaultPlan::new(vec![(FaultPoint::Spawn, ErrorCode::Capacity)], 1).unwrap(),
    });
    let tasks = Arc::new(BoundedTaskSpawner::new(adapter.clone(), 1).unwrap());
    let clock = Arc::new(ManualClock::new(Instant::now(), 0));
    let registry = NativeMetrics::new(clock.clone(), 2, 4, true).unwrap();
    let metrics = Arc::new(registry.register(binding()).unwrap());
    let owner = marketforge_engine::live::runtime::dependencies::InstrumentedOwner {
        context: marketforge_engine::live::ports::context::OwnerContext {
            identity: binding().owner,
            clock,
            cancellation: CancellationToken::new(),
            limits: marketforge_engine::live::ports::context::OwnerLimits {
                queued_items: 1,
                queued_bytes: 64,
                in_flight: 1,
                shutdown_ns: 1_000_000,
            },
            tasks: tasks.clone(),
        },
        metrics: metrics.clone(),
        diagnostics: metrics,
    };
    owner.validate().unwrap();
    let result = tasks.spawn(binding().owner, Box::pin(async { Ok(()) }));
    assert!(matches!(
        result,
        Err(ControlError {
            code: ErrorCode::Capacity,
            ..
        })
    ));
    assert_eq!(tasks.admitted_tasks(), 0);
    assert_eq!(adapter.faults.remaining(), 0);
}
#[test]
#[ignore = "manual reproducible overhead baseline; run with --release --ignored --nocapture"]
fn native_measurement_overhead_baseline() {
    let clock = Arc::new(marketforge_engine::live::foundation::SystemMonotonicClock);
    let registry = NativeMetrics::new(clock, 2, 4, true).unwrap();
    let handle = registry.register(binding()).unwrap();
    let start = Instant::now();
    for _ in 0..100_000 {
        std::hint::black_box(handle.increment(MetricId::FailedTasksTotal, 1, 0).unwrap());
    }
    let enabled = start.elapsed();
    let mut timing_binding = binding();
    timing_binding.owner.resource = ResourceId::Configuration;
    timing_binding.metrics = vec![MetricId::RuntimeSchedulerDelay];
    let timing = registry.register(timing_binding).unwrap();
    registry
        .configure(
            &marketforge_engine::live::model::configuration::MetricsSettings {
                enabled: true,
                detailed_timing: true,
                snapshot_interval_ns: 1,
                timing_sample_one_in: 10,
            },
        )
        .unwrap();
    let start = Instant::now();
    for _ in 0..100_000 {
        std::hint::black_box(
            timing
                .observe_ns_lazy(MetricId::RuntimeSchedulerDelay, 0, || 7)
                .unwrap(),
        );
    }
    let detailed = start.elapsed();
    let start = Instant::now();
    for _ in 0..1000 {
        std::hint::black_box(registry.snapshot(Duration::from_secs(1)));
    }
    let snapshots = start.elapsed();
    registry.set_enabled(false);
    let start = Instant::now();
    for _ in 0..100_000 {
        std::hint::black_box(
            handle
                .record_lazy(MetricId::FailedTasksTotal, || panic!("disabled"))
                .unwrap(),
        );
    }
    eprintln!(
        "100000 updates: counters_ns={} timing_sample10_ns={} disabled_ns={}; 1000 two-series snapshots_ns={}",
        enabled.as_nanos(),
        detailed.as_nanos(),
        start.elapsed().as_nanos(),
        snapshots.as_nanos()
    );
}

#[test]
fn deterministic_transport_uses_real_port_context_limits_and_faults() {
    use super::support::{
        failure::{FaultPlan, FaultPoint},
        transport::{RestStep, ScriptedRestTransport},
    };
    use marketforge_engine::live::ports::{
        context::CallContext,
        transport::{RestRequest, RestResponse, RestTransportPort, TransportEndpoint},
    };
    let now = Instant::now();
    let clock = Arc::new(ManualClock::new(now, 0));
    let faults =
        Arc::new(FaultPlan::new(vec![(FaultPoint::Read, ErrorCode::Unavailable)], 1).unwrap());
    let transport = ScriptedRestTransport::new(
        clock,
        vec![RestStep {
            endpoint_name: "fixture".into(),
            profile_revision: 1,
            weight: 2,
            response: RestResponse {
                status: 200,
                received_at_ns: 0,
                retry_after_ns: None,
                body: vec![1, 2, 3],
            },
        }],
        faults,
        1,
        64,
    )
    .unwrap();
    let context = CallContext {
        cancellation: CancellationToken::new(),
        deadline: now + Duration::from_secs(1),
        expected: None,
    };
    let request = || RestRequest {
        endpoint: TransportEndpoint {
            profile_revision: 1,
            endpoint_name: "fixture".into(),
        },
        parameters: vec![],
        weight: 2,
        max_response_bytes: 3,
    };
    let mut first = transport.execute(&context, request());
    assert!(matches!(
        poll(first.as_mut()),
        Poll::Ready(Err(ControlError {
            code: ErrorCode::Unavailable,
            ..
        }))
    ));
    drop(first);
    assert_eq!(transport.remaining(), 1);
    let mut too_small = request();
    too_small.max_response_bytes = 2;
    let mut second = transport.execute(&context, too_small);
    assert!(matches!(
        poll(second.as_mut()),
        Poll::Ready(Err(ControlError {
            code: ErrorCode::Limit,
            ..
        }))
    ));
    drop(second);
    let mut third = transport.execute(&context, request());
    assert!(
        matches!(poll(third.as_mut()),Poll::Ready(Ok(RestResponse {body,..})) if body==vec![1,2,3])
    );
    drop(third);
    assert_eq!(transport.remaining(), 0);
    context.cancellation.cancel();
    let mut cancelled = transport.execute(&context, request());
    assert!(matches!(
        poll(cancelled.as_mut()),
        Poll::Ready(Err(ControlError {
            code: ErrorCode::Cancelled,
            ..
        }))
    ));
}
