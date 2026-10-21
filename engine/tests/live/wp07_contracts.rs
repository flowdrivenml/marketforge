//! Independent management consumers: wire client/server, configuration store,
//! snapshot presentation and failure witnesses. No daemon or database mutations.
use marketforge_engine::live::{
    control::{
        framing::{self, FrameLimits},
        operation::*,
        protocol::ControlCodec,
    },
    foundation::{CancellationToken, MonotonicClock},
    model::{
        configuration::*, derived::*, error::*, identity::*, observation::*, operation::*,
        protocol::*, snapshot::*,
    },
    ports::{configuration::*, context::*, control::*, lifecycle::*, routing::RoutingFuture},
};
use std::{
    future::Future,
    pin::pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
fn ready<T>(future: impl Future<Output = T>) -> T {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    let mut f = pin!(future);
    match f.as_mut().poll(&mut context) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fixture unexpectedly pending"),
    }
}
struct Clock(Instant);
impl MonotonicClock for Clock {
    fn now(&self) -> Instant {
        self.0
    }
}
fn runtime() -> RuntimeInstanceId {
    RuntimeInstanceId("test-runtime".into())
}
fn peer(grants: Vec<Permission>) -> AuthenticatedPeer {
    AuthenticatedPeer {
        session: SessionId {
            runtime: runtime(),
            sequence: 1,
        },
        uid: 1000,
        grants,
    }
}
fn request(command: ControlCommand) -> ControlRequest {
    ControlRequest {
        protocol_version: 1,
        request_id: RequestId {
            client_instance: "fixture".into(),
            sequence: 42,
        },
        command,
    }
}
fn query() -> ControlRequest {
    request(ControlCommand::Query {
        query: Query::GetRuntimeStatus,
        page: PageRequest {
            limit: 10,
            after: None,
        },
    })
}
fn shutdown() -> ControlRequest {
    request(ControlCommand::Shutdown {
        deadline_ns: 100,
        idempotency_key: "stop".into(),
    })
}
fn codec() -> ControlCodec {
    ControlCodec::new(FrameLimits {
        max_bytes: 65536,
        max_items: 128,
        max_string_bytes: 4096,
    })
    .unwrap()
}
fn context(now: Instant) -> CallContext {
    CallContext {
        cancellation: CancellationToken::new(),
        deadline: now + Duration::from_secs(1),
        expected: None,
    }
}
fn health(resource: ResourceId) -> HealthSnapshot {
    HealthSnapshot {
        resource,
        status: HealthStatus::Unknown,
        reasons: vec![HealthReason::NoObservation],
        capabilities: vec![],
        evaluated_at_ns: 100,
    }
}
fn stamp() -> SnapshotStamp {
    SnapshotStamp {
        runtime: runtime(),
        snapshot_epoch: 1,
        sequence: 2,
        captured_at_ns: 100,
        collection_duration_ns: 3,
    }
}
fn usage() -> ResourceUsage {
    ResourceUsage {
        events: 0,
        bytes: 0,
        limit_events: 10,
        limit_bytes: 1024,
        pending_operations: 0,
    }
}
fn dashboard() -> RuntimeSnapshot {
    RuntimeSnapshot {
        stamp: stamp(),
        lifecycle: RuntimeLifecycle::Running,
        revisions: Revisions {
            desired: ConfigRevisionId(2),
            applied: Some(ConfigRevisionId(1)),
            adopted: None,
        },
        active_connections: 0,
        active_sources: 0,
        active_derived: 0,
        active_subscribers: 0,
        active_recoveries: vec![],
        recording: RecordingState::Disabled,
        retention: usage(),
        health: health(ResourceId::Runtime),
        metrics: vec![],
    }
}
#[test]
fn bounded_frames_handle_every_split_and_trailing_messages() {
    let codec = codec();
    let r = query();
    let bytes = codec.encode_request(&r).unwrap();
    for split in 0..bytes.len() {
        assert!(codec.decode_request(&bytes[..split]).unwrap().is_none());
    }
    let mut two = bytes.clone();
    two.extend_from_slice(&bytes);
    let (decoded, n) = codec.decode_request(&two).unwrap().unwrap();
    assert_eq!(decoded, r);
    assert_eq!(n, bytes.len());
    assert_eq!(codec.decode_request(&two[n..]).unwrap().unwrap().0, r);
    assert_eq!(
        codec.decode_request(&u32::MAX.to_be_bytes()),
        Err(ErrorCode::Limit)
    );
    assert_eq!(
        codec.decode_request(&0u32.to_be_bytes()),
        Err(ErrorCode::Limit)
    );
}
#[test]
fn codec_rejects_versions_unknown_fields_duplicate_fields_and_limits() {
    let c = codec();
    let mut r = query();
    r.protocol_version = 2;
    assert_eq!(c.encode_request(&r), Err(ErrorCode::UnsupportedVersion));
    let limits = FrameLimits {
        max_bytes: 65536,
        max_items: 128,
        max_string_bytes: 4096,
    };
    for json in [
        r#"{"protocol_version":1,"protocol_version":1,"request_id":{"client_instance":"x","sequence":1},"command":{"command":"query","query":{"kind":"get_runtime_status"},"page":{"limit":1,"after":null}}}"#,
        r#"{"protocol_version":1,"request_id":{"client_instance":"x","sequence":1},"command":{"command":"query","query":{"kind":"get_runtime_status"},"page":{"limit":1,"after":null}},"role":"admin"}"#,
    ] {
        assert_eq!(
            c.decode_request(&framing::frame(json.as_bytes(), limits).unwrap()),
            Err(ErrorCode::Malformed)
        );
    }
    let mut r = query();
    r.request_id.client_instance = " ".into();
    assert_eq!(c.encode_request(&r), Err(ErrorCode::InvalidInput));
    assert!(
        ControlCodec::new(FrameLimits {
            max_bytes: 0,
            ..limits
        })
        .is_err()
    );
    let tiny = ControlCodec::new(FrameLimits {
        max_bytes: 32,
        ..limits
    })
    .unwrap();
    assert_eq!(tiny.encode_request(&query()), Err(ErrorCode::Limit));
}
#[test]
fn response_correlation_cannot_be_reused_for_another_request() {
    let c = codec();
    let r = query();
    let reply = ControlResponse {
        protocol_version: 1,
        request_id: r.request_id.clone(),
        result: ControlResult::Runtime(dashboard()),
    };
    let bytes = c.encode_response(&reply).unwrap();
    assert_eq!(
        c.decode_response(&bytes, &r.request_id).unwrap().unwrap().0,
        reply
    );
    let mut other = r.request_id;
    other.sequence += 1;
    assert_eq!(
        c.decode_response(&bytes, &other),
        Err(ErrorCode::IdempotencyConflict)
    );
}
struct Server {
    clock: Clock,
    calls: Mutex<u32>,
}
impl ManagementPort for Server {
    fn execute<'a>(
        &'a self,
        cx: &'a CallContext,
        peer: &'a AuthenticatedPeer,
        command: ControlCommand,
    ) -> RoutingFuture<'a, Result<ControlResult, ControlError>> {
        Box::pin(async move {
            cx.check(&self.clock, None)
                .map_err(|c| ControlError::new(c, None))?;
            command
                .authorize(peer)
                .map_err(|c| ControlError::new(c, None))?;
            command
                .validate(128)
                .map_err(|c| ControlError::new(c, None))?;
            match command {
                ControlCommand::Query {
                    query: Query::GetRuntimeStatus,
                    ..
                } => {
                    *self.calls.lock().unwrap() += 1;
                    Ok(ControlResult::Runtime(dashboard()))
                }
                _ => Err(ControlError::new(ErrorCode::UnsupportedCapability, None)),
            }
        })
    }
}
struct Client {
    server: Arc<dyn ManagementPort>,
    peer: AuthenticatedPeer,
}
impl ManagementClientPort for Client {
    fn request<'a>(
        &'a self,
        cx: &'a CallContext,
        r: ControlRequest,
    ) -> RoutingFuture<'a, Result<ControlResponse, ControlError>> {
        Box::pin(async move {
            let c = codec();
            let bytes = c
                .encode_request(&r)
                .map_err(|c| ControlError::new(c, None))?;
            let decoded = c.decode_request(&bytes).unwrap().unwrap().0;
            let result = self
                .server
                .execute(cx, &self.peer, decoded.command)
                .await
                .unwrap_or_else(ControlResult::Error);
            let response = ControlResponse {
                protocol_version: 1,
                request_id: decoded.request_id,
                result,
            };
            let encoded = c
                .encode_response(&response)
                .map_err(|c| ControlError::new(c, None))?;
            Ok(c.decode_response(&encoded, &r.request_id)
                .unwrap()
                .unwrap()
                .0)
        })
    }
}
#[test]
fn fake_client_server_use_the_same_contract_and_trusted_authorization() {
    let now = Instant::now();
    let server = Arc::new(Server {
        clock: Clock(now),
        calls: Mutex::new(0),
    });
    let client = Client {
        server: server.clone(),
        peer: peer(vec![Permission::Monitor]),
    };
    assert!(matches!(
        ready(client.request(&context(now), query()))
            .unwrap()
            .result,
        ControlResult::Runtime(_)
    ));
    assert!(matches!(
        ready(client.request(&context(now), shutdown()))
            .unwrap()
            .result,
        ControlResult::Error(ControlError {
            code: ErrorCode::PermissionDenied,
            ..
        })
    ));
    assert_eq!(*server.calls.lock().unwrap(), 1);
}
#[test]
fn configuration_mutation_requires_resource_permission_too() {
    let command = ControlCommand::ApplyConfiguration {
        update: ConfigUpdate {
            expected_revision: ConfigRevisionId(1),
            changes: vec![ConfigChange::SetMaxSubscribers { value: 12 }],
        },
        idempotency_key: "change".into(),
        objective: CompletionObjective::DesiredPersisted,
    };
    assert_eq!(
        command.authorize(&peer(vec![Permission::Configure])),
        Err(ErrorCode::PermissionDenied)
    );
    command
        .authorize(&peer(vec![
            Permission::Configure,
            Permission::ManageRuntime,
        ]))
        .unwrap();
}
#[test]
fn deadlines_cancellation_and_generation_fences_precede_effects() {
    let now = Instant::now();
    let clock = Clock(now);
    let mut cx = context(now);
    cx.check(&clock, None).unwrap();
    cx.deadline = now;
    assert_eq!(cx.check(&clock, None), Err(ErrorCode::Deadline));
    cx = context(now);
    cx.expected = Some(ResourceVersion {
        resource: ResourceId::Runtime,
        generation: 2,
    });
    assert_eq!(
        cx.check(
            &clock,
            Some(&ResourceVersion {
                resource: ResourceId::Runtime,
                generation: 1
            })
        ),
        Err(ErrorCode::StaleGeneration)
    );
    cx.cancellation.cancel();
    assert_eq!(
        cx.check(&clock, cx.expected.as_ref()),
        Err(ErrorCode::Cancelled)
    );
}
#[test]
fn idempotency_reserves_before_effects_and_does_not_evict_ambiguity() {
    let mut ledger = IdempotencyLedger::new(runtime(), 1, 65536).unwrap();
    let peer = peer(vec![Permission::ManageRuntime]);
    let r = shutdown();
    let ReservationOutcome::New(token) = ledger.reserve(&peer, &r).unwrap() else {
        panic!()
    };
    assert!(matches!(
        ledger.reserve(&peer, &r).unwrap(),
        ReservationOutcome::Pending
    ));
    let mut conflict = r.clone();
    conflict.command = ControlCommand::Shutdown {
        deadline_ns: 101,
        idempotency_key: "stop".into(),
    };
    assert!(matches!(
        ledger.reserve(&peer, &conflict),
        Err(ErrorCode::IdempotencyConflict)
    ));
    let mut different = r.clone();
    different.command = ControlCommand::Shutdown {
        deadline_ns: 100,
        idempotency_key: "other".into(),
    };
    assert!(matches!(
        ledger.reserve(&peer, &different),
        Err(ErrorCode::Capacity)
    ));
    let result = ControlResult::Error(ControlError::new(ErrorCode::CommitUnknown, None));
    ledger.complete(token, result.clone()).unwrap();
    let mut retry = r;
    retry.request_id.sequence += 1;
    assert!(
        matches!(ledger.reserve(&peer,&retry).unwrap(),ReservationOutcome::Completed(v) if v==result)
    );
    assert!(matches!(
        ledger.reserve(&self::peer(vec![]), &retry),
        Err(ErrorCode::PermissionDenied)
    ));
}
fn operation() -> OperationStatus {
    OperationStatus {
        id: OperationId {
            runtime: runtime(),
            sequence: 1,
        },
        request: query().request_id,
        target: ResourceVersion {
            resource: ResourceId::Source(StreamId("trades".into())),
            generation: 3,
        },
        objective: CompletionObjective::AcquisitionReady,
        requested_revision: Some(ConfigRevisionId(2)),
        state: OperationState::Accepted,
        created_at_ns: 100,
        started_at_ns: None,
        completed_at_ns: None,
        completion: None,
        error: None,
    }
}
#[test]
fn completion_cannot_confuse_persisted_intent_with_ready_or_new_generation() {
    let mut op = operation();
    op.start(101).unwrap();
    let mut evidence = CompletionEvidence {
        objective: CompletionObjective::DesiredPersisted,
        target: op.target.clone(),
        revision: op.requested_revision,
    };
    assert_eq!(
        op.complete(evidence.clone(), 102),
        Err(ErrorCode::InvalidCompletion)
    );
    evidence.objective = op.objective;
    evidence.target.generation += 1;
    assert_eq!(
        op.complete(evidence.clone(), 102),
        Err(ErrorCode::InvalidCompletion)
    );
    evidence.target = op.target.clone();
    evidence.revision = Some(ConfigRevisionId(1));
    assert_eq!(
        op.complete(evidence.clone(), 102),
        Err(ErrorCode::InvalidCompletion)
    );
    evidence.revision = op.requested_revision;
    op.complete(evidence, 102).unwrap();
    assert_eq!(op.start(103), Err(ErrorCode::InvalidTransition));
    assert_eq!(
        op.terminate(ControlError::new(ErrorCode::Cancelled, None), true, 103),
        Err(ErrorCode::InvalidTransition)
    );
}
#[test]
fn readiness_requires_more_than_a_confirmed_subscription() {
    let mut r = AcquisitionReadiness {
        subscription_confirmed: true,
        processor_initialized: true,
        metadata_available: true,
        integrity_satisfied: false,
        book_synchronized: Some(false),
    };
    assert!(!r.is_ready());
    r.integrity_satisfied = true;
    assert!(!r.is_ready());
    r.book_synchronized = Some(true);
    assert!(r.is_ready());
}
#[test]
fn zero_missing_disabled_and_stale_metrics_remain_distinct() {
    let zero = Measurement::Available {
        value: MetricValue::Counter(0),
        observed_at_ns: 100,
    };
    let missing = Measurement::<MetricValue>::Unavailable {
        reason: MissingMeasurement::NotCollected,
    };
    let disabled = Measurement::<MetricValue>::Unavailable {
        reason: MissingMeasurement::Disabled,
    };
    let stale = Measurement::Stale {
        last_value: Some(MetricValue::Counter(0)),
        observed_at_ns: 1,
    };
    for m in [&zero, &missing, &disabled, &stale] {
        assert_eq!(
            &serde_json::from_str::<Measurement<MetricValue>>(&serde_json::to_string(m).unwrap())
                .unwrap(),
            m
        );
    }
    assert_ne!(zero, missing);
    assert_ne!(missing, disabled);
    assert_ne!(zero, stale);
    let mut sample = MetricSample {
        resource: ResourceId::Runtime,
        generation: 2,
        reset_epoch: 3,
        metric: MetricId::BytesReceived,
        measurement: zero,
    };
    sample.validate().unwrap();
    sample.measurement = Measurement::Available {
        value: MetricValue::DurationNs(0),
        observed_at_ns: 100,
    };
    assert_eq!(sample.validate(), Err(ErrorCode::InvalidInput));
}
#[test]
fn cursor_positions_and_latency_boundaries_are_checked() {
    use marketforge_engine::live::model::cursor::*;
    let c = StreamCursor::new(StreamId("s".into()), LogEpoch(1), 5);
    CursorPositions {
        accepted: Some(c.clone()),
        committed: None,
        durable: None,
    }
    .validate()
    .unwrap();
    assert_eq!(
        CursorPositions {
            accepted: None,
            committed: Some(c),
            durable: None
        }
        .validate(),
        Err(ErrorCode::InvalidInput)
    );
    let mut summary = LatencySummary {
        count: 0,
        sum_ns: 0,
        min_ns: None,
        p50_ns: None,
        p95_ns: None,
        p99_ns: None,
        max_ns: None,
    };
    summary.validate().unwrap();
    summary.min_ns = Some(0);
    assert_eq!(summary.validate(), Err(ErrorCode::InvalidInput));
    summary = LatencySummary {
        count: 2,
        sum_ns: 30,
        min_ns: Some(10),
        p50_ns: Some(25),
        p95_ns: None,
        p99_ns: None,
        max_ns: Some(20),
    };
    assert_eq!(summary.validate(), Err(ErrorCode::InvalidInput));
}
pub(super) fn config() -> DesiredConfiguration {
    serde_json::from_str(include_str!("fixtures/default-config.json")).unwrap()
}

struct Store {
    state: Mutex<ConfigurationRevision>,
    fail: bool,
}
impl ConfigurationStorePort for Store {
    fn revision<'a>(
        &'a self,
        _: &'a CallContext,
        id: ConfigRevisionId,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>> {
        Box::pin(async move {
            let state = self.state.lock().unwrap();
            if state.id == id {
                Ok(state.clone())
            } else {
                Err(ControlError::new(ErrorCode::NotFound, None))
            }
        })
    }
    fn history<'a>(
        &'a self,
        _: &'a CallContext,
        page: PageRequest,
    ) -> RoutingFuture<'a, Result<Page<ConfigurationRevision>, ControlError>> {
        Box::pin(async move {
            if page.limit == 0 || page.after.is_some() {
                return Err(ControlError::new(ErrorCode::InvalidInput, None));
            }
            Ok(Page {
                items: vec![self.state.lock().unwrap().clone()],
                continuation: None,
            })
        })
    }
    fn load<'a>(
        &'a self,
        _: &'a CallContext,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>> {
        Box::pin(async { Ok(self.state.lock().unwrap().clone()) })
    }
    fn compare_and_swap<'a>(
        &'a self,
        _: &'a CallContext,
        expected: ConfigRevisionId,
        desired: DesiredConfiguration,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            if state.id != expected {
                return Err(ControlError::new(ErrorCode::RevisionConflict, None));
            }
            if state.desired == desired {
                return Ok(state.clone());
            }
            if self.fail {
                return Err(ControlError::new(ErrorCode::Persistence, None));
            }
            let next = state
                .id
                .0
                .checked_add(1)
                .ok_or_else(|| ControlError::new(ErrorCode::Capacity, None))?;
            *state = ConfigurationRevision {
                id: ConfigRevisionId(next),
                parent: Some(expected),
                created_at_ns: 101,
                desired,
            };
            Ok(state.clone())
        })
    }
}
#[test]
fn config_store_contract_preserves_noop_cas_and_failed_persistence() {
    let original = ConfigurationRevision {
        id: ConfigRevisionId(2),
        parent: Some(ConfigRevisionId(1)),
        created_at_ns: 100,
        desired: config(),
    };
    let store = Store {
        state: Mutex::new(original.clone()),
        fail: true,
    };
    let cx = context(Instant::now());
    assert_eq!(
        ready(store.compare_and_swap(&cx, original.id, config())).unwrap(),
        original
    );
    let mut changed = config();
    changed.global.logging = LogLevel::Debug;
    assert_eq!(
        ready(store.compare_and_swap(&cx, ConfigRevisionId(1), changed.clone()))
            .unwrap_err()
            .code,
        ErrorCode::RevisionConflict
    );
    assert_eq!(
        ready(store.compare_and_swap(&cx, original.id, changed))
            .unwrap_err()
            .code,
        ErrorCode::Persistence
    );
    assert_eq!(ready(store.load(&cx)).unwrap(), original);
}
#[test]
fn explicit_inheritance_and_hot_boundaries_survive_serialization() {
    let inherited = MetricsSettings {
        enabled: true,
        detailed_timing: false,
        snapshot_interval_ns: 100,
        timing_sample_one_in: 1,
    };
    assert_eq!(
        Override::<MetricsSettings>::Inherit.resolve(&inherited),
        inherited
    );
    let mut changed = inherited.clone();
    changed.snapshot_interval_ns = 200;
    assert_eq!(
        Override::<MetricsSettings>::Inherit.resolve(&changed),
        changed
    );
    assert_eq!(
        Override::Set(inherited.clone()).resolve(&changed),
        inherited
    );
    assert_eq!(
        ConfigChange::SetLogging {
            level: LogLevel::Debug
        }
        .boundary(),
        ReconfigurationBoundary::Immediate
    );
    assert_eq!(
        ConfigChange::SetMetrics {
            settings: inherited
        }
        .boundary(),
        ReconfigurationBoundary::NextMeasurement
    );
    assert_eq!(
        ConfigChange::SetRecordingBatch {
            batch_events: 10,
            flush_interval_ns: 100
        }
        .boundary(),
        ReconfigurationBoundary::NextBatch
    );
    assert_eq!(
        ConfigChange::SetMaxSubscribers { value: 10 }.boundary(),
        ReconfigurationBoundary::FutureAdmission
    );
    config().resolve(100).unwrap();
}
/// Presentation-only consumer: the fixture doesn't know runtime owner internals.
fn present(view: &ManagementView) -> String {
    match view {
        ManagementView::Dashboard { runtime } => {
            format!("{:?} {:?}", runtime.lifecycle, runtime.health.status)
        }
        ManagementView::Streams { sources } => format!("{} sources", sources.items.len()),
        ManagementView::Derived { streams } => format!("{} derived", streams.items.len()),
        ManagementView::Recording {
            recording,
            retention,
        } => format!(
            "{} recorders / {} retention",
            recording.items.len(),
            retention.items.len()
        ),
        ManagementView::Settings { desired, effective } => {
            format!("desired {} applied {:?}", desired.id.0, effective.applied)
        }
    }
}
#[test]
fn all_five_tui_views_consume_owned_versioned_snapshots() {
    let desired = ConfigurationRevision {
        id: ConfigRevisionId(2),
        parent: Some(ConfigRevisionId(1)),
        created_at_ns: 100,
        desired: config(),
    };
    let views = vec![
        ManagementView::Dashboard {
            runtime: dashboard(),
        },
        ManagementView::Streams {
            sources: Page {
                items: vec![],
                continuation: Some("page2".into()),
            },
        },
        ManagementView::Derived {
            streams: Page {
                items: vec![],
                continuation: None,
            },
        },
        ManagementView::Recording {
            recording: Page {
                items: vec![],
                continuation: None,
            },
            retention: Page {
                items: vec![],
                continuation: None,
            },
        },
        ManagementView::Settings {
            effective: EffectiveConfiguration {
                desired: desired.id,
                applied: Some(ConfigRevisionId(1)),
                adopted: vec![],
                resolved: config().resolve(100).unwrap(),
            },
            desired,
        },
    ];
    for view in views {
        let json = serde_json::to_string(&view).unwrap();
        let decoded: ManagementView = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, view);
        assert!(!present(&decoded).is_empty());
    }
}
#[test]
fn shutdown_cannot_hide_unjoined_tasks_or_unfinished_obligations() {
    let mut r = ShutdownReport {
        joined: true,
        unfinished: vec![],
        errors: vec![],
    };
    assert!(r.complete());
    r.joined = false;
    assert!(!r.complete());
    r.joined = true;
    r.unfinished.push(UnfinishedWork {
        owner: ResourceVersion {
            resource: ResourceId::Runtime,
            generation: 1,
        },
        first: None,
        last: None,
        events: 1,
        bytes: 10,
    });
    assert!(!r.complete());
    assert!(
        OwnerLimits {
            queued_items: 1,
            queued_bytes: 0,
            in_flight: 1,
            shutdown_ns: 1
        }
        .validate()
        .is_err()
    );
}

#[test]
fn metric_binding_bounds_series_and_preserves_generation_reset_and_units() {
    let binding = MetricBinding {
        owner: ResourceVersion {
            resource: ResourceId::Source(StreamId("s".into())),
            generation: 7,
        },
        reset_epoch: 9,
        reset: ResetBoundary::OwnerGeneration,
        metrics: vec![MetricId::BytesReceived],
    };
    binding.validate(1).unwrap();
    assert_eq!(binding.validate(0), Err(ErrorCode::Limit));
    let sample = binding
        .sample(
            MetricId::BytesReceived,
            Measurement::Available {
                value: MetricValue::Counter(0),
                observed_at_ns: 1,
            },
        )
        .unwrap();
    assert_eq!((sample.generation, sample.reset_epoch), (7, 9));
    let descriptor = sample.metric.metadata(binding.reset);
    assert_eq!(descriptor.unit, MetricUnit::Bytes);
    assert_eq!(descriptor.aggregation, Aggregation::Sum);
    assert_eq!(
        binding.sample(
            MetricId::MessagesReceived,
            Measurement::Unavailable {
                reason: MissingMeasurement::Disabled
            }
        ),
        Err(ErrorCode::UnsupportedCapability)
    );
    let mut duplicate = binding;
    duplicate.metrics.push(MetricId::BytesReceived);
    assert_eq!(duplicate.validate(2), Err(ErrorCode::InvalidInput));
}
#[test]
fn transient_idempotency_lifetime_is_session_scoped_and_fenced() {
    let mut ledger = IdempotencyLedger::new(runtime(), 3, 65536).unwrap();
    let mut peer = peer(vec![Permission::ManageStreams]);
    let r = request(ControlCommand::AcquireDemand {
        stream: StreamRef::Source(StreamId("s".into())),
        idempotency_key: "acquire".into(),
        objective: CompletionObjective::AcquisitionReady,
    });
    assert!(matches!(
        ledger.reserve(&peer, &r).unwrap(),
        ReservationOutcome::New(_)
    ));
    let first = peer.session.clone();
    peer.session.sequence += 1;
    assert!(matches!(
        ledger.reserve(&peer, &r).unwrap(),
        ReservationOutcome::New(_)
    ));
    assert!(matches!(
        ledger.reserve(&peer, &r).unwrap(),
        ReservationOutcome::Pending
    ));
    ledger.release_fenced_session(&first);
    peer.session = first;
    assert!(matches!(
        ledger.reserve(&peer, &r).unwrap(),
        ReservationOutcome::New(_)
    ));
    peer.session.runtime = RuntimeInstanceId("old-process".into());
    assert!(matches!(
        ledger.reserve(&peer, &r),
        Err(ErrorCode::StaleGeneration)
    ));
}
#[test]
fn declared_completion_objectives_match_command_semantics() {
    let mut r = request(ControlCommand::AcquireDemand {
        stream: StreamRef::Source(StreamId("s".into())),
        idempotency_key: "acquire".into(),
        objective: CompletionObjective::DesiredPersisted,
    });
    assert_eq!(
        codec().encode_request(&r),
        Err(ErrorCode::InvalidCompletion)
    );
    r.command = ControlCommand::RequestRecordingFlush {
        stream: StreamId("s".into()),
        generation: 1,
        idempotency_key: "flush".into(),
        objective: CompletionObjective::AcquisitionReady,
    };
    assert_eq!(
        codec().encode_request(&r),
        Err(ErrorCode::InvalidCompletion)
    );
}
#[test]
fn contrasting_depth_contracts_require_evidence_and_valid_strategy_inputs() {
    use marketforge_engine::live::model::acquisition::*;
    let evidence = CapabilityEvidence::Verified {
        profile_revision: 3,
        fixture: "synthetic-contract".into(),
    };
    let strategies = [
        DepthStrategyContract {
            synchronization: DepthSynchronization::RestSnapshotBridge {
                snapshot_endpoint: "snapshot".into(),
                max_buffer_events: 10,
                max_buffer_bytes: 1024,
            },
            sequence: SequenceStrategy::RangeBridge,
            checksum: ChecksumStrategy::NotProvided,
            evidence: evidence.clone(),
        },
        DepthStrategyContract {
            synchronization: DepthSynchronization::ServerSnapshot {
                reset_starts_new_book: true,
            },
            sequence: SequenceStrategy::Monotonic,
            checksum: ChecksumStrategy::NotProvided,
            evidence: evidence.clone(),
        },
        DepthStrategyContract {
            synchronization: DepthSynchronization::ServerSnapshot {
                reset_starts_new_book: true,
            },
            sequence: SequenceStrategy::PreviousAndCurrent,
            checksum: ChecksumStrategy::Compiled {
                implementation: "fixture-checksum".into(),
                depth: 25,
            },
            evidence,
        },
    ];
    for mut strategy in strategies {
        strategy.validate_for_activation(3).unwrap();
        assert_eq!(
            strategy.validate_for_activation(4),
            Err(ErrorCode::UnsupportedCapability)
        );
        let decoded: DepthStrategyContract =
            serde_json::from_str(&serde_json::to_string(&strategy).unwrap()).unwrap();
        assert_eq!(decoded, strategy);
        strategy.evidence = CapabilityEvidence::Unverified;
        assert_eq!(
            strategy.validate_for_activation(3),
            Err(ErrorCode::UnsupportedCapability)
        );
    }
    assert_eq!(
        HeartbeatStrategy::TransportPing {
            interval_ns: 0,
            timeout_ns: 1
        }
        .validate(128),
        Err(ErrorCode::InvalidInput)
    );
}

pub(super) fn source_key() -> SourceStreamKey {
    SourceStreamKey {
        exchange: marketforge_engine::canonical::Exchange::Bybit,
        market: MarketKind::Spot,
        instrument: marketforge_engine::instrument::InstrumentIdentity {
            exchange: "bybit".into(),
            symbol: "BTCUSDT".into(),
            instrument_kind: marketforge_engine::job::InstrumentKind::Spot,
            contract_kind: None,
        },
        channel: ChannelSpec {
            kind: ChannelKind::L2,
            depth: Some(50),
            interval_ms: None,
        },
    }
}
fn source_snapshot() -> SourceSnapshot {
    use marketforge_engine::live::model::lifecycle::*;
    SourceSnapshot {
        stamp: stamp(),
        stream: StreamId("book".into()),
        key: source_key(),
        persistent_enabled: false,
        effective_acquisition_required: true,
        demands: vec![
            marketforge_engine::live::model::demand::DemandOwner::Subscriber(
                marketforge_engine::live::model::demand::SubscriberId("client".into()),
            ),
        ],
        lifecycle: StreamLifecycle::Active,
        readiness: AcquisitionReadiness {
            subscription_confirmed: true,
            processor_initialized: true,
            metadata_available: true,
            integrity_satisfied: false,
            book_synchronized: Some(false),
        },
        connection: None,
        connection_state: Some(ConnectionState::Connected),
        connection_generation: None,
        subscription: None,
        subscription_state: SubscriptionState::Active,
        subscription_generation: None,
        integrity: IntegrityState::Synchronizing,
        recovery: RecoveryState::Recovering,
        coverage: CoverageState::Incomplete,
        revisions: dashboard().revisions,
        recording: RecordingState::Disabled,
        book_generation: None,
        latest_cursor: None,
        best_bid: Measurement::Unavailable {
            reason: MissingMeasurement::NotCollected,
        },
        best_ask: Measurement::Unavailable {
            reason: MissingMeasurement::NotCollected,
        },
        health: health(ResourceId::Source(StreamId("book".into()))),
        metrics: vec![],
    }
}
#[test]
fn populated_stream_fixture_preserves_independent_demand_readiness_and_coverage() {
    let source = source_snapshot();
    source.validate().unwrap();
    assert!(!source.persistent_enabled);
    assert!(source.effective_acquisition_required);
    assert!(!source.readiness.is_ready());
    let reply = ControlResponse {
        protocol_version: 1,
        request_id: query().request_id,
        result: ControlResult::Source(Box::new(source.clone())),
    };
    let c = codec();
    let bytes = c.encode_response(&reply).unwrap();
    assert_eq!(
        c.decode_response(&bytes, &reply.request_id)
            .unwrap()
            .unwrap()
            .0,
        reply
    );
    let mut invalid = source;
    invalid.readiness.book_synchronized = None;
    assert_eq!(invalid.validate(), Err(ErrorCode::InvalidInput));
}
#[test]
fn populated_settings_resolve_overrides_without_copying_global_budgets() {
    let mut desired = config();
    desired.sources.push(SourceDefinition {
        id: StreamId("book".into()),
        key: source_key(),
        enabled: false,
        recording: Override::Inherit,
        recovery: Override::Inherit,
        metrics: Override::Inherit,
    });
    desired.derived.push(DerivedDefinition {
        id: DerivedStreamId("merged".into()),
        enabled: true,
        transformation: DerivedTransformation::Merge {
            sources: vec![StreamRef::Source(StreamId("book".into()))],
            ordering: MergeOrdering::AdaptiveEventTime,
            initial_window_ns: 10,
            max_window_ns: 100,
            late_events: LateEventPolicy::EmitLate,
            source_failure: SourceFailurePolicy::ContinueDegraded,
            max_buffer_events: 10,
            max_buffer_bytes: 1024,
        },
        recording: Override::Inherit,
    });
    let first = desired.resolve(10).unwrap();
    assert_eq!(first.sources[0].metrics, desired.global.metrics);
    assert_eq!(first.global.budgets, desired.global.budgets);
    desired.global.metrics.snapshot_interval_ns = 1000;
    let second = desired.resolve(10).unwrap();
    assert_eq!(second.sources[0].metrics.snapshot_interval_ns, 1000);
    assert_ne!(first, second);
    desired.sources[0].metrics = Override::Set(MetricsSettings {
        snapshot_interval_ns: 0,
        ..desired.global.metrics.clone()
    });
    assert_eq!(desired.resolve(10), Err(ErrorCode::InvalidInput));
}
#[test]
fn invalid_operation_and_metric_responses_are_rejected_by_codec() {
    let mut op = operation();
    op.state = OperationState::Completed;
    let reply = ControlResponse {
        protocol_version: 1,
        request_id: query().request_id,
        result: ControlResult::Operation(op),
    };
    assert_eq!(
        codec().encode_response(&reply),
        Err(ErrorCode::InvalidCompletion)
    );
}

#[test]
fn populated_derived_recording_and_retention_views_preserve_boundary_metadata() {
    use marketforge_engine::live::model::{cursor::*, lifecycle::*};
    let cursor = StreamCursor::new(StreamId("book".into()), LogEpoch(4), 10);
    let interval = CursorInterval {
        first: StreamCursor::new(StreamId("book".into()), LogEpoch(4), 0),
        last: cursor.clone(),
    };
    let positions = CursorPositions {
        accepted: Some(cursor.clone()),
        committed: Some(cursor.clone()),
        durable: None,
    };
    let missing = Measurement::Unavailable {
        reason: MissingMeasurement::NotCollected,
    };
    let recording = RecordingSnapshot {
        stamp: stamp(),
        stream: StreamId("book".into()),
        generation: 2,
        state: RecordingState::Active,
        database_connected: Measurement::Available {
            value: true,
            observed_at_ns: 100,
        },
        queue: usage(),
        positions: positions.clone(),
        committed: vec![interval.clone()],
        gaps: vec![],
        integrity: IntegrityState::Valid,
        recovery: RecoveryState::Idle,
        commit_latency: missing,
        health: health(ResourceId::Recording(StreamId("book".into()))),
        metrics: vec![],
    };
    recording.validate().unwrap();
    let retention = RetentionSnapshot {
        stamp: stamp(),
        stream: StreamId("book".into()),
        positions,
        available: vec![interval.clone()],
        gaps: vec![],
        protected: vec![interval],
        protections: vec![ProtectionSnapshot {
            id: 1,
            generation: 2,
            owner: marketforge_engine::live::model::demand::RetentionSessionId("bootstrap".into()),
            first: cursor.clone(),
            last: None,
            safely_consumed: None,
            remaining_lifetime_ns: Measurement::Available {
                value: 10,
                observed_at_ns: 100,
            },
            active: true,
            failure: None,
        }],
        memory: usage(),
        disk: usage(),
        pending_spill_bytes: 0,
        coverage: CoverageState::Complete,
        health: health(ResourceId::Retention(StreamId("book".into()))),
        metrics: vec![],
    };
    retention.validate().unwrap();
    let derived = DerivedSnapshot {
        stamp: stamp(),
        definition: DerivedDefinition {
            id: DerivedStreamId("merged".into()),
            enabled: true,
            transformation: DerivedTransformation::Merge {
                sources: vec![StreamRef::Source(StreamId("book".into()))],
                ordering: MergeOrdering::AdaptiveEventTime,
                initial_window_ns: 10,
                max_window_ns: 100,
                late_events: LateEventPolicy::SideOutput,
                source_failure: SourceFailurePolicy::ContinueDegraded,
                max_buffer_events: 10,
                max_buffer_bytes: 1024,
            },
            recording: Override::Inherit,
        },
        generation: DerivedGeneration(7),
        lifecycle: StreamLifecycle::Active,
        revisions: dashboard().revisions,
        inputs: vec![SourceParticipation {
            source: StreamRef::Source(StreamId("book".into())),
            required: true,
            active: false,
            health: health(ResourceId::Source(StreamId("book".into()))),
            watermark_ns: Measurement::Unavailable {
                reason: MissingMeasurement::NotCollected,
            },
        }],
        references: vec![],
        watermark_ns: Measurement::Stale {
            last_value: Some(10),
            observed_at_ns: 10,
        },
        buffer: usage(),
        late_events: Measurement::Available {
            value: 0,
            observed_at_ns: 100,
        },
        latest_cursor: None,
        health: health(ResourceId::Derived(DerivedStreamId("merged".into()))),
        metrics: vec![],
    };
    for view in [
        ManagementView::Derived {
            streams: Page {
                items: vec![derived],
                continuation: None,
            },
        },
        ManagementView::Recording {
            recording: Page {
                items: vec![recording],
                continuation: None,
            },
            retention: Page {
                items: vec![retention],
                continuation: None,
            },
        },
    ] {
        let decoded: ManagementView =
            serde_json::from_str(&serde_json::to_string(&view).unwrap()).unwrap();
        assert_eq!(decoded, view);
        assert!(!present(&decoded).is_empty());
    }
}
#[test]
fn pool_compatibility_includes_handshake_authentication_and_profile_revisions() {
    use marketforge_engine::live::ports::transport::*;
    let reference = VersionedReference {
        name: "public".into(),
        revision: 1,
    };
    let base = ConnectionPoolKey {
        exchange: marketforge_engine::canonical::Exchange::Bybit,
        endpoint: reference.clone(),
        protocol_version: "v1".into(),
        authentication: None,
        handshake: reference.clone(),
        heartbeat_compatibility: reference,
        connection_class: None,
    };
    let mut other = base.clone();
    other.handshake.revision += 1;
    assert_ne!(base, other);
    other = base.clone();
    other.authentication = Some(VersionedReference {
        name: "account-reference".into(),
        revision: 1,
    });
    assert_ne!(base, other);
    other = base.clone();
    other.endpoint.revision += 1;
    assert_ne!(base, other);
}
#[test]
fn future_versions_are_rejected_before_interpreting_unknown_commands() {
    let limits = FrameLimits {
        max_bytes: 65536,
        max_items: 128,
        max_string_bytes: 4096,
    };
    let bytes=framing::frame(br#"{"protocol_version":2,"request_id":{"client_instance":"future","sequence":1},"command":{"command":"future_command"}}"#,limits).unwrap();
    assert_eq!(
        codec().decode_request(&bytes),
        Err(ErrorCode::UnsupportedVersion)
    );
}

#[test]
fn histogram_sum_must_agree_with_count_and_extrema_without_overflow() {
    let mut summary = LatencySummary {
        count: 2,
        sum_ns: 30,
        min_ns: Some(10),
        p50_ns: None,
        p95_ns: None,
        p99_ns: None,
        max_ns: Some(20),
    };
    summary.validate().unwrap();
    summary.sum_ns = 0;
    assert_eq!(summary.validate(), Err(ErrorCode::InvalidInput));
    summary.sum_ns = 41;
    assert_eq!(summary.validate(), Err(ErrorCode::InvalidInput));
    summary.count = u64::MAX;
    summary.min_ns = Some(u64::MAX);
    summary.max_ns = Some(u64::MAX);
    summary.sum_ns = u64::MAX;
    assert_eq!(summary.validate(), Err(ErrorCode::InvalidInput));
}

#[test]
fn owner_constructor_contract_checks_instrumentation_scope_and_task_admission_failure() {
    use marketforge_engine::live::{
        ports::observability::*, runtime::dependencies::InstrumentedOwner,
    };
    struct Metrics(MetricBinding);
    impl ScopedMetricsPort for Metrics {
        fn owner(&self) -> &ResourceVersion {
            &self.0.owner
        }
        fn record(
            &self,
            metric: MetricId,
            value: Measurement<MetricValue>,
        ) -> Result<ObservationAdmission, ErrorCode> {
            self.0.sample(metric, value)?;
            Ok(ObservationAdmission::Accepted)
        }
    }
    struct Diagnostics;
    impl ObservationSinkPort for Diagnostics {
        fn metric(&self, _: MetricSample) -> ObservationAdmission {
            ObservationAdmission::Disabled
        }
        fn health(&self, _: HealthSnapshot) -> ObservationAdmission {
            ObservationAdmission::Disabled
        }
        fn diagnostic(&self, _: DiagnosticEvent) -> ObservationAdmission {
            ObservationAdmission::Accepted
        }
    }
    /// Explicit fault injector, not a fallback task runtime.
    struct ExhaustedTasks;
    impl TaskSpawnerPort for ExhaustedTasks {
        fn spawn(
            &self,
            _: ResourceVersion,
            _: RoutingFuture<'static, Result<(), ControlError>>,
        ) -> Result<Arc<dyn TaskHandlePort>, ControlError> {
            Err(ControlError::new(ErrorCode::Capacity, None))
        }
    }
    let identity = ResourceVersion {
        resource: ResourceId::Source(StreamId("s".into())),
        generation: 2,
    };
    let metrics = Arc::new(Metrics(MetricBinding {
        owner: identity.clone(),
        reset_epoch: 1,
        reset: ResetBoundary::OwnerGeneration,
        metrics: vec![MetricId::MessagesReceived],
    }));
    let mut owner = InstrumentedOwner {
        context: OwnerContext {
            identity: identity.clone(),
            clock: Arc::new(Clock(Instant::now())),
            cancellation: CancellationToken::new(),
            limits: OwnerLimits {
                queued_items: 10,
                queued_bytes: 1024,
                in_flight: 1,
                shutdown_ns: 10,
            },
            tasks: Arc::new(ExhaustedTasks),
        },
        metrics,
        diagnostics: Arc::new(Diagnostics),
    };
    owner.validate().unwrap();
    assert_eq!(
        owner
            .context
            .tasks
            .spawn(identity, Box::pin(async { Ok(()) }))
            .err()
            .unwrap()
            .code,
        ErrorCode::Capacity
    );
    owner.context.identity.generation += 1;
    assert_eq!(owner.validate(), Err(ErrorCode::StaleGeneration));
    owner.context.identity.generation -= 1;
    owner.context.limits.queued_bytes = 0;
    assert_eq!(owner.validate(), Err(ErrorCode::InvalidInput));
}

#[test]
fn revision_history_and_restore_contract_never_rewind_revision_identity() {
    let mut current = config();
    current.global.logging = LogLevel::Debug;
    let store = Store {
        state: Mutex::new(ConfigurationRevision {
            id: ConfigRevisionId(2),
            parent: Some(ConfigRevisionId(1)),
            created_at_ns: 100,
            desired: current,
        }),
        fail: false,
    };
    let cx = context(Instant::now());
    assert_eq!(
        ready(store.revision(&cx, ConfigRevisionId(1)))
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    assert_eq!(
        ready(store.history(
            &cx,
            PageRequest {
                limit: 1,
                after: None
            }
        ))
        .unwrap()
        .items[0]
            .id,
        ConfigRevisionId(2)
    );
    let restore = request(ControlCommand::RestoreConfiguration {
        expected_revision: ConfigRevisionId(2),
        revision: ConfigRevisionId(1),
        idempotency_key: "restore".into(),
        objective: CompletionObjective::DesiredPersisted,
    });
    let bytes = codec().encode_request(&restore).unwrap();
    assert_eq!(codec().decode_request(&bytes).unwrap().unwrap().0, restore);
    assert_eq!(
        restore.command.authorize(&peer(vec![Permission::Monitor])),
        Err(ErrorCode::PermissionDenied)
    );
    let revision = ready(store.compare_and_swap(&cx, ConfigRevisionId(2), config())).unwrap();
    assert_eq!(revision.id, ConfigRevisionId(3));
    assert_eq!(revision.parent, Some(ConfigRevisionId(2)));
    let diff = request(ControlCommand::Query {
        query: Query::DiffConfiguration {
            before: ConfigRevisionId(1),
            after: ConfigRevisionId(3),
        },
        page: PageRequest {
            limit: 1,
            after: None,
        },
    });
    assert!(codec().encode_request(&diff).is_ok());
}
