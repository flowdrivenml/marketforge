//! Production compiler/store/manager composition with a real durable journal.
use marketforge_engine::{
    instrument::InstrumentCatalog,
    live::{
        config::{
            compiler::ConfigurationCompiler,
            loader::{self, LoadLimits},
            manager::{ConfigurationManager, ManagementError},
            store::{FileRevisionJournal, JournalLimits},
            worker::ConfigurationWorker,
        },
        foundation::{
            CancellationToken,
            clock::{ManualClock, MonotonicClock},
            deadline::ManualDeadlineTimer,
            resources::ResourceUsage,
        },
        model::{
            configuration::*,
            error::{ControlError, ErrorCode},
            operation::{ResourceId, ResourceVersion},
            snapshot::PageRequest,
        },
        ports::{
            configuration::{
                CompiledConfiguration, ConfigurationCompilerPort, ConfigurationStorePort,
            },
            context::CallContext,
            protocol::{AcquisitionPlan, ExchangeProtocolPort, ProtocolMessage},
            transport::ReceivedFrame,
        },
    },
};
use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
struct Signal(Mutex<bool>, Condvar);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        *self.0.lock().unwrap() = true;
        self.1.notify_one();
    }
}
fn run<F: Future>(future: F) -> F::Output {
    let signal = Arc::new(Signal(Mutex::new(false), Condvar::new()));
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        let ready = signal.0.lock().unwrap();
        let (mut ready, wait) = signal
            .1
            .wait_timeout_while(ready, Duration::from_secs(10), |v| !*v)
            .unwrap();
        assert!(!wait.timed_out(), "lost wakeup");
        *ready = false;
    }
}
struct Protocol;
impl ExchangeProtocolPort for Protocol {
    fn compile(
        &self,
        _: &marketforge_engine::live::model::identity::SourceStreamKey,
    ) -> Result<AcquisitionPlan, ControlError> {
        Err(ControlError::new(ErrorCode::UnsupportedCapability, None))
    }
    fn decode(&self, _: &ReceivedFrame) -> Result<Vec<ProtocolMessage>, ControlError> {
        panic!("configuration cannot decode runtime frames")
    }
}
fn desired() -> DesiredConfiguration {
    super::wp07_contracts::config()
}
fn compiler(dir: &std::path::Path) -> Arc<ConfigurationCompiler> {
    let path = dir.join("catalog.json");
    std::fs::write(&path, r#"{"version":1,"instruments":[]}"#).unwrap();
    Arc::new(ConfigurationCompiler {
        catalog: Arc::new(InstrumentCatalog::load(&path).unwrap()),
        protocol: Arc::new(Protocol),
        max_items: 100,
        max_edges: 100,
    })
}
fn owner() -> ResourceVersion {
    ResourceVersion {
        resource: ResourceId::Configuration,
        generation: 1,
    }
}
fn context(clock: &ManualClock) -> CallContext {
    CallContext {
        cancellation: CancellationToken::new(),
        deadline: clock.now() + Duration::from_secs(60),
        expected: Some(owner()),
    }
}
fn initial() -> CompiledConfiguration {
    let desired = desired();
    CompiledConfiguration {
        base_revision: ConfigRevisionId(0),
        resolved: desired.resolve(100).unwrap(),
        desired,
        impacts: vec![],
        boundaries: vec![],
    }
}
fn limits() -> JournalLimits {
    JournalLimits {
        bytes: 65536,
        history: 8,
        items: 100,
        edges: 100,
    }
}
fn worker(
    dir: &std::path::Path,
    compiler: Arc<dyn ConfigurationCompilerPort>,
    clock: Arc<ManualClock>,
    capacity: usize,
) -> Arc<ConfigurationWorker> {
    let journal = FileRevisionJournal::create(
        &dir.join("revisions.json"),
        &initial(),
        limits(),
        clock.clone(),
        clock.clone(),
        owner(),
    )
    .unwrap();
    Arc::new(
        ConfigurationWorker::start(
            journal,
            compiler,
            Arc::new(ManualDeadlineTimer::new(clock.now(), 100).unwrap()),
            ResourceUsage {
                items: capacity,
                bytes: capacity * 16 * 1024 * 1024,
            },
        )
        .unwrap(),
    )
}
fn update(id: u64, changes: Vec<ConfigChange>) -> ConfigurationProposal {
    ConfigurationProposal::Changes(ConfigUpdate {
        expected_revision: ConfigRevisionId(id),
        changes,
    })
}
fn drain(worker: &ConfigurationWorker) {
    run(std::future::poll_fn(|cx| worker.poll_shutdown(cx)));
    assert_eq!(worker.snapshot().usage, ResourceUsage::default());
}
#[test]
fn wp09_service_durable_acceptance_noop_conflict_history_restore_restart_and_status() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 123));
    let context = context(&clock);
    let compiler = compiler(dir.path());
    let worker = worker(dir.path(), compiler.clone(), clock.clone(), 4);
    let manager = ConfigurationManager::new(compiler.clone(), worker.clone());
    let change = update(
        1,
        vec![ConfigChange::SetLogging {
            level: LogLevel::Debug,
        }],
    );
    run(manager.validate(&context, &change)).unwrap();
    assert_eq!(
        run(manager.desired(&context)).unwrap().id,
        ConfigRevisionId(1)
    );
    let accepted = run(manager.accept(&context, &change)).unwrap();
    assert_eq!(accepted.revision.id, ConfigRevisionId(2));
    assert_eq!(
        accepted.plan.boundaries,
        vec![ReconfigurationBoundary::Immediate]
    );
    assert!(matches!(
        run(manager.accept(&context, &change)),
        Err(ManagementError::Validation(_))
    ));
    let noop = update(
        2,
        vec![ConfigChange::SetLogging {
            level: LogLevel::Debug,
        }],
    );
    assert_eq!(
        run(manager.accept(&context, &noop)).unwrap().revision.id,
        ConfigRevisionId(2)
    );
    let restored =
        run(manager.restore(&context, ConfigRevisionId(2), ConfigRevisionId(1))).unwrap();
    assert_eq!(restored.revision.id, ConfigRevisionId(3));
    assert_eq!(restored.revision.parent, Some(ConfigRevisionId(2)));
    assert_eq!(restored.revision.desired, desired());
    assert_eq!(
        run(manager.diff(&context, ConfigRevisionId(1), ConfigRevisionId(2)))
            .unwrap()
            .changes
            .len(),
        1
    );
    let history = run(manager.history(
        &context,
        PageRequest {
            limit: 2,
            after: None,
        },
    ))
    .unwrap();
    assert_eq!(history.items.len(), 2);
    assert_eq!(history.continuation.as_deref(), Some("2"));
    assert_eq!(
        run(manager.history(
            &context,
            PageRequest {
                limit: 2,
                after: history.continuation
            }
        ))
        .unwrap()
        .items[0]
            .id,
        ConfigRevisionId(3)
    );
    let status = run(manager.status(&context, None)).unwrap();
    assert_eq!(status.desired, ConfigRevisionId(3));
    assert_eq!(status.applied, None);
    assert!(status.adopted.is_empty());
    drain(&worker);
    let journal = FileRevisionJournal::open(
        &dir.path().join("revisions.json"),
        limits(),
        clock.clone(),
        clock.clone(),
        owner(),
    )
    .unwrap();
    assert_eq!(journal.load(&context).unwrap().id, ConfigRevisionId(3));
}
#[test]
fn wp09_service_rejects_restart_changes_missing_metadata_and_derived_economics() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let context = context(&clock);
    let compiler = compiler(dir.path());
    let worker = worker(dir.path(), compiler.clone(), clock, 2);
    let manager = ConfigurationManager::new(compiler, worker.clone());
    let mut budgets = desired().global.budgets;
    budgets.max_tasks += 1;
    assert!(
        matches!(run(manager.accept(&context,&update(1,vec![ConfigChange::SetBudgets{budgets}]))),Err(ManagementError::Control(e)) if e.code==ErrorCode::UnsupportedCapability)
    );
    let source = SourceDefinition {
        id: marketforge_engine::live::model::identity::StreamId("s".into()),
        key: super::wp07_contracts::source_key(),
        enabled: true,
        recording: Override::Inherit,
        recovery: Override::Inherit,
        metrics: Override::Inherit,
    };
    assert!(
        matches!(run(manager.accept(&context,&update(1,vec![ConfigChange::RegisterSource {definition:source}]))),Err(ManagementError::Validation(r)) if r.issues[0].code==ErrorCode::Dependency)
    );
    assert_eq!(
        run(manager.desired(&context)).unwrap().id,
        ConfigRevisionId(1)
    );
    drain(&worker);
}
#[test]
fn wp09_worker_cancel_deadline_generation_and_shutdown_are_effect_free() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let compiler = compiler(dir.path());
    let worker = worker(dir.path(), compiler, clock.clone(), 2);
    let cancelled = context(&clock);
    cancelled.cancellation.cancel();
    assert_eq!(
        run(worker.load(&cancelled)).unwrap_err().code,
        ErrorCode::Cancelled
    );
    let mut expired = context(&clock);
    expired.deadline = clock.now();
    assert_eq!(
        run(worker.load(&expired)).unwrap_err().code,
        ErrorCode::Deadline
    );
    let mut stale = context(&clock);
    stale.expected.as_mut().unwrap().generation = 2;
    assert_eq!(
        run(worker.load(&stale)).unwrap_err().code,
        ErrorCode::StaleGeneration
    );
    drain(&worker);
    assert_eq!(
        run(worker.load(&context(&clock))).unwrap_err().code,
        ErrorCode::Unavailable
    );
}
fn toml(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => format!(
            "{{{}}}",
            map.iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| format!("{} = {}", serde_json::to_string(k).unwrap(), toml(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        serde_json::Value::Array(array) => format!(
            "[{}]",
            array.iter().map(toml).collect::<Vec<_>>().join(", ")
        ),
        _ => value.to_string(),
    }
}
fn configuration_toml(desired: &DesiredConfiguration) -> String {
    serde_json::to_value(desired)
        .unwrap()
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| format!("{k} = {}\n", toml(v)))
        .collect()
}
fn load_limits() -> LoadLimits {
    LoadLimits {
        bytes: 65536,
        nodes: 4096,
        depth: 64,
        items: 100,
    }
}
#[test]
fn wp09_toml_real_parser_versions_limits_unknown_fields_and_inheritance() {
    let mut desired = desired();
    let source = SourceDefinition {
        id: marketforge_engine::live::model::identity::StreamId("s".into()),
        key: super::wp07_contracts::source_key(),
        enabled: false,
        recording: Override::Inherit,
        recovery: Override::Inherit,
        metrics: Override::Inherit,
    };
    desired.sources.push(source);
    let text = configuration_toml(&desired);
    assert_eq!(
        loader::parse_desired(&text, load_limits()).unwrap(),
        desired
    );
    let omitted = text
        .replace("\"recording\" = {\"mode\" = \"inherit\"}, ", "")
        .replace(", \"recovery\" = {\"mode\" = \"inherit\"}", "")
        .replace("\"metrics\" = {\"mode\" = \"inherit\"}, ", "");
    assert_ne!(omitted, text);
    assert_eq!(
        loader::parse_desired(&omitted, load_limits()).unwrap(),
        desired
    );
    assert_eq!(
        loader::parse_desired(
            &text.replace("schema_version = 1", "schema_version = 2"),
            load_limits()
        ),
        Err(ErrorCode::UnsupportedVersion)
    );
    assert_eq!(
        loader::parse_desired(&(text.clone() + "surprise = true\n"), load_limits()),
        Err(ErrorCode::InvalidInput)
    );
    assert_eq!(
        loader::parse_desired("schema_version = 1\nschema_version = 1", load_limits()),
        Err(ErrorCode::Malformed)
    );
    let mut limits = load_limits();
    limits.bytes = 3;
    assert_eq!(loader::parse_desired(&text, limits), Err(ErrorCode::Limit));
    let mut limits = load_limits();
    limits.depth = 2;
    assert_eq!(loader::parse_desired(&text, limits), Err(ErrorCode::Limit));
}
#[test]
fn wp09_diff_roundtrip_global_categories_and_unrepresentable_fields_fail() {
    let before = ConfigurationRevision {
        id: ConfigRevisionId(1),
        parent: None,
        created_at_ns: 0,
        desired: desired(),
    };
    let mut after = before.clone();
    after.id = ConfigRevisionId(2);
    after.desired.global.logging = LogLevel::Trace;
    after.desired.global.recording.batch_events += 1;
    after.desired.global.metrics.detailed_timing = true;
    after.desired.global.max_subscribers += 1;
    after.desired.global.budgets.max_tasks += 1;
    let diff = marketforge_engine::live::config::diff::between(&before, &after).unwrap();
    let candidate = marketforge_engine::live::config::compiler::build_candidate(
        &before,
        &update(1, diff.changes),
        100,
        100,
    )
    .unwrap();
    assert_eq!(candidate.desired, after.desired);
    after.desired.global.recovery.max_attempts += 1;
    assert_eq!(
        marketforge_engine::live::config::diff::between(&before, &after),
        Err(ErrorCode::UnsupportedCapability)
    );
}

struct GateCompiler {
    inner: Arc<dyn ConfigurationCompilerPort>,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl ConfigurationCompilerPort for GateCompiler {
    fn compile(
        &self,
        base: &ConfigurationRevision,
        proposal: &ConfigurationProposal,
    ) -> Result<CompiledConfiguration, ValidationReport> {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        self.inner.compile(base, proposal)
    }
}
#[test]
fn wp09_worker_bounded_queue_cancellation_retains_obligations_and_drain() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let context = context(&clock);
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(2);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(2);
    let compiler = Arc::new(GateCompiler {
        inner: compiler(dir.path()),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let worker = worker(dir.path(), compiler, clock.clone(), 2);
    let signal = Arc::new(Signal(Mutex::new(false), Condvar::new()));
    let waker = Waker::from(signal);
    let mut cx = Context::from_waker(&waker);
    let mut changed = desired();
    changed.global.logging = LogLevel::Trace;
    let mut first = worker.compare_and_swap(&context, ConfigRevisionId(1), changed.clone());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let queued_context = super_context(&clock);
    let mut queued = worker.compare_and_swap(&queued_context, ConfigRevisionId(1), changed);
    assert!(queued.as_mut().poll(&mut cx).is_pending());
    assert_eq!(
        run(worker.load(&context)).unwrap_err().code,
        ErrorCode::Capacity
    );
    queued_context.cancellation.cancel();
    assert_eq!(run(queued).unwrap_err().code, ErrorCode::Cancelled);
    // Cancelled caller did not release the still-queued worker obligation.
    assert_eq!(worker.snapshot().usage.items, 2);
    release_tx.send(()).unwrap();
    assert_eq!(run(first).unwrap().id, ConfigRevisionId(2));
    // A barrier read is queued behind the cancelled command. Its capacity can
    // remain charged until the worker consumes that command, so use shutdown
    // to prove the drain without timing-dependent retries.
    drain(&worker);
    let journal = FileRevisionJournal::open(
        &dir.path().join("revisions.json"),
        limits(),
        clock.clone(),
        clock.clone(),
        owner(),
    )
    .unwrap();
    assert_eq!(journal.load(&context).unwrap().id, ConfigRevisionId(2));
}
fn super_context(clock: &ManualClock) -> CallContext {
    context(clock)
}
#[test]
fn wp09_worker_persistence_failure_does_not_publish_candidate_and_fences_cas() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let context = context(&clock);
    let compiler = compiler(dir.path());
    let worker = worker(dir.path(), compiler.clone(), clock.clone(), 2);
    let manager = ConfigurationManager::new(compiler, worker.clone());
    std::fs::create_dir(dir.path().join("revisions.json.pending")).unwrap();
    let proposal = update(
        1,
        vec![ConfigChange::SetLogging {
            level: LogLevel::Trace,
        }],
    );
    assert!(
        matches!(run(manager.accept(&context,&proposal)),Err(ManagementError::Control(e)) if e.code==ErrorCode::Persistence)
    );
    assert_eq!(
        run(manager.desired(&context)).unwrap_err().code,
        ErrorCode::CommitUnknown
    );
    assert!(
        matches!(run(manager.accept(&context,&proposal)),Err(ManagementError::Control(e)) if e.code==ErrorCode::CommitUnknown)
    );
    drain(&worker);
    let persisted: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("revisions.json")).unwrap()).unwrap();
    assert_eq!(persisted["revisions"][0]["id"], 1);
    assert_eq!(persisted["revisions"].as_array().unwrap().len(), 1);
}
#[test]
fn wp09_compiler_boundaries_noop_and_immutable_effective_origins() {
    let dir = tempfile::tempdir().unwrap();
    let compiler = compiler(dir.path());
    let base = ConfigurationRevision {
        id: ConfigRevisionId(1),
        parent: None,
        created_at_ns: 0,
        desired: desired(),
    };
    let mut metrics = base.desired.global.metrics.clone();
    metrics.enabled = !metrics.enabled;
    let mut recording = base.desired.global.recording.clone();
    recording.batch_events += 1;
    for (change, boundary) in [
        (
            ConfigChange::SetLogging {
                level: LogLevel::Trace,
            },
            ReconfigurationBoundary::Immediate,
        ),
        (
            ConfigChange::SetMetrics { settings: metrics },
            ReconfigurationBoundary::NextMeasurement,
        ),
        (
            ConfigChange::SetRecordingBatch {
                batch_events: 20,
                flush_interval_ns: 20,
            },
            ReconfigurationBoundary::NextBatch,
        ),
        (
            ConfigChange::SetRecordingDefaults {
                settings: recording,
            },
            ReconfigurationBoundary::StreamTransition,
        ),
        (
            ConfigChange::SetMaxSubscribers { value: 20 },
            ReconfigurationBoundary::FutureAdmission,
        ),
    ] {
        let plan = compiler.compile(&base, &update(1, vec![change])).unwrap();
        assert_eq!(plan.boundaries, vec![boundary]);
    }
    let plan = compiler
        .compile(
            &base,
            &update(
                1,
                vec![ConfigChange::SetLogging {
                    level: base.desired.global.logging,
                }],
            ),
        )
        .unwrap();
    assert!(plan.boundaries.is_empty());
    assert!(plan.impacts.is_empty());
    let plan =
        marketforge_engine::live::config::effective::EffectivePlan::resolve(&base.desired, 100)
            .unwrap();
    assert_eq!(plan.resolved(), &base.desired.resolve(100).unwrap());
}

struct VerifiedProtocol {
    verified: bool,
}
impl ExchangeProtocolPort for VerifiedProtocol {
    fn compile(
        &self,
        source: &marketforge_engine::live::model::identity::SourceStreamKey,
    ) -> Result<AcquisitionPlan, ControlError> {
        use marketforge_engine::live::model::acquisition::{CapabilityEvidence, HeartbeatStrategy};
        Ok(AcquisitionPlan {
            profile: ProfileReference {
                exchange: source.exchange,
                api_family: "spot".into(),
                profile_revision: 1,
            },
            source: source.clone(),
            subscription_payload: vec![1],
            unsubscribe_payload: vec![2],
            max_message_bytes: 100,
            heartbeat: HeartbeatStrategy::NoProtocolHeartbeat {
                idle_timeout_ns: 100,
            },
            depth: None,
            evidence: if self.verified {
                CapabilityEvidence::Verified {
                    profile_revision: 1,
                    fixture: "deterministic-protocol-fixture".into(),
                }
            } else {
                CapabilityEvidence::Unverified
            },
        })
    }
    fn decode(&self, _: &ReceivedFrame) -> Result<Vec<ProtocolMessage>, ControlError> {
        panic!("compile cannot decode")
    }
}
fn source_compiler(
    dir: &std::path::Path,
    verified: bool,
) -> (ConfigurationCompiler, ConfigurationRevision) {
    use marketforge_engine::live::model::identity::{ChannelKind, StreamId};
    let mut key = super::wp07_contracts::source_key();
    key.channel.kind = ChannelKind::Trades;
    key.channel.depth = None;
    let mut metadata = serde_json::to_value(&key.instrument).unwrap();
    metadata["base_currency"] = "BTC".into();
    metadata["quote_currency"] = "USDT".into();
    let path = dir.join("known-catalog.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&serde_json::json!({"version":1,"instruments":[metadata]})).unwrap(),
    )
    .unwrap();
    let compiler = ConfigurationCompiler {
        catalog: Arc::new(InstrumentCatalog::load(&path).unwrap()),
        protocol: Arc::new(VerifiedProtocol { verified }),
        max_items: 100,
        max_edges: 100,
    };
    let mut desired = desired();
    desired.profiles.push(ProfileReference {
        exchange: key.exchange,
        api_family: "spot".into(),
        profile_revision: 1,
    });
    desired.sources.push(SourceDefinition {
        id: StreamId("inherited".into()),
        key: key.clone(),
        enabled: true,
        recording: Override::Inherit,
        recovery: Override::Inherit,
        metrics: Override::Inherit,
    });
    desired.sources.push(SourceDefinition {
        id: StreamId("explicit".into()),
        key,
        enabled: true,
        recording: Override::Set(desired.global.recording.clone()),
        recovery: Override::Set(desired.global.recovery.clone()),
        metrics: Override::Set(desired.global.metrics.clone()),
    });
    (
        compiler,
        ConfigurationRevision {
            id: ConfigRevisionId(1),
            parent: None,
            created_at_ns: 0,
            desired,
        },
    )
}
#[test]
fn wp09_compiler_verified_sources_affected_owners_and_exact_hot_boundaries() {
    use marketforge_engine::live::model::identity::StreamId;
    let dir = tempfile::tempdir().unwrap();
    let (compiler, base) = source_compiler(dir.path(), true);
    let plan = compiler
        .compile(
            &base,
            &update(
                1,
                vec![ConfigChange::SetRecordingBatch {
                    batch_events: 20,
                    flush_interval_ns: 20,
                }],
            ),
        )
        .unwrap();
    assert_eq!(plan.impacts.len(), 1);
    assert_eq!(
        plan.impacts[0].resource,
        ResourceId::Recording(StreamId("inherited".into()))
    );
    assert_eq!(plan.impacts[0].boundary, ReconfigurationBoundary::NextBatch);
    let mut metrics = base.desired.global.metrics.clone();
    metrics.detailed_timing = true;
    let plan = compiler
        .compile(
            &base,
            &update(1, vec![ConfigChange::SetMetrics { settings: metrics }]),
        )
        .unwrap();
    assert_eq!(plan.impacts.len(), 2);
    assert!(
        !plan
            .impacts
            .iter()
            .any(|i| i.resource == ResourceId::Source(StreamId("explicit".into())))
    );
    for change in [
        ConfigChange::SetSourceEnabled {
            stream: StreamId("explicit".into()),
            enabled: false,
        },
        ConfigChange::SetSourceRecording {
            stream: StreamId("explicit".into()),
            value: Override::Set(RecordingSettings {
                batch_events: 2,
                ..base.desired.global.recording.clone()
            }),
        },
        ConfigChange::SetSourceRecovery {
            stream: StreamId("explicit".into()),
            value: Override::Set(RecoveryPolicy {
                max_backoff_ns: 200,
                ..base.desired.global.recovery.clone()
            }),
        },
    ] {
        let plan = compiler.compile(&base, &update(1, vec![change])).unwrap();
        assert_eq!(
            plan.boundaries,
            vec![ReconfigurationBoundary::StreamTransition]
        );
        assert_eq!(plan.impacts.len(), 1);
    }
    let mut contents = base.clone();
    contents.desired.derived.push(
        marketforge_engine::live::model::derived::DerivedDefinition {
            id: marketforge_engine::live::model::identity::DerivedStreamId("merge".into()),
            enabled: true,
            recording: Override::Inherit,
            transformation:
                marketforge_engine::live::model::derived::DerivedTransformation::Merge {
                    sources: vec![marketforge_engine::live::model::derived::StreamRef::Source(
                        StreamId("inherited".into()),
                    )],
                    ordering: marketforge_engine::live::model::derived::MergeOrdering::Arrival,
                    initial_window_ns: 0,
                    max_window_ns: 0,
                    late_events: marketforge_engine::live::model::derived::LateEventPolicy::Fail,
                    source_failure:
                        marketforge_engine::live::model::derived::SourceFailurePolicy::Fail,
                    max_buffer_events: 10,
                    max_buffer_bytes: 100,
                },
        },
    );
    let error = compiler
        .compile(
            &base,
            &ConfigurationProposal::Restore {
                expected_revision: base.id,
                contents,
            },
        )
        .unwrap_err();
    assert_eq!(error.issues[0].path, "derived.economic_compiler");
    assert_eq!(error.issues[0].code, ErrorCode::Dependency);
    let plan =
        marketforge_engine::live::config::effective::EffectivePlan::resolve(&base.desired, 100)
            .unwrap();
    use marketforge_engine::live::config::effective::SettingOrigin;
    assert_eq!(plan.source_origins()[0].metrics, SettingOrigin::Global);
    assert_eq!(plan.source_origins()[1].metrics, SettingOrigin::Explicit);
    let (unverified, base) = source_compiler(dir.path(), false);
    assert_eq!(
        unverified
            .compile(&base, &update(1, vec![]))
            .unwrap_err()
            .issues[0]
            .code,
        ErrorCode::UnsupportedCapability
    );
}
#[test]
fn wp09_worker_concurrent_cas_has_exactly_one_durable_winner() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let context = context(&clock);
    let worker = worker(dir.path(), compiler(dir.path()), clock, 4);
    let gate = Arc::new(std::sync::Barrier::new(3));
    let mut threads = vec![];
    for level in [LogLevel::Trace, LogLevel::Debug] {
        let worker = worker.clone();
        let context = context.clone();
        let gate = gate.clone();
        threads.push(std::thread::spawn(move || {
            let mut desired = desired();
            desired.global.logging = level;
            gate.wait();
            run(worker.compare_and_swap(&context, ConfigRevisionId(1), desired))
        }));
    }
    gate.wait();
    let results = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r,Err(e) if e.code==ErrorCode::RevisionConflict))
            .count(),
        1
    );
    assert_eq!(run(worker.load(&context)).unwrap().id, ConfigRevisionId(2));
    drain(&worker);
}

#[test]
fn wp09_status_preserves_old_applied_and_adopted_while_desired_advances() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let context = context(&clock);
    let compiler = compiler(dir.path());
    let worker = worker(dir.path(), compiler.clone(), clock, 2);
    let manager = ConfigurationManager::new(compiler, worker.clone());
    let observed = EffectiveConfiguration {
        desired: ConfigRevisionId(1),
        applied: Some(ConfigRevisionId(1)),
        adopted: vec![(owner(), ConfigRevisionId(1))],
        resolved: desired().resolve(100).unwrap(),
    };
    run(manager.accept(
        &context,
        &update(
            1,
            vec![ConfigChange::SetLogging {
                level: LogLevel::Trace,
            }],
        ),
    ))
    .unwrap();
    let status = run(manager.status(&context, Some(&observed))).unwrap();
    assert_eq!(status.desired, ConfigRevisionId(2));
    assert_eq!(status.applied, Some(ConfigRevisionId(1)));
    assert_eq!(status.adopted, observed.adopted);
    assert_eq!(status.resolved, observed.resolved);
    let mut future = observed.clone();
    future.desired = ConfigRevisionId(3);
    assert_eq!(
        run(manager.status(&context, Some(&future)))
            .unwrap_err()
            .code,
        ErrorCode::StaleGeneration
    );
    drain(&worker);
}
#[test]
fn wp09_worker_deadline_during_compile_and_dropped_waiter_do_not_commit() {
    for drop_waiter in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(ManualClock::new(Instant::now(), 1));
        let context = context(&clock);
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let compiler = Arc::new(GateCompiler {
            inner: compiler(dir.path()),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let timer = Arc::new(ManualDeadlineTimer::new(clock.now(), 8).unwrap());
        let journal = FileRevisionJournal::create(
            &dir.path().join("revisions.json"),
            &initial(),
            limits(),
            clock.clone(),
            clock.clone(),
            owner(),
        )
        .unwrap();
        let worker = ConfigurationWorker::start(
            journal,
            compiler,
            timer.clone(),
            ResourceUsage {
                items: 1,
                bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap();
        let waker = Waker::from(Arc::new(Signal(Mutex::new(false), Condvar::new())));
        let mut cx = Context::from_waker(&waker);
        let mut changed = desired();
        changed.global.logging = LogLevel::Trace;
        let mut pending = worker.compare_and_swap(&context, ConfigRevisionId(1), changed);
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        if drop_waiter {
            drop(pending);
        } else {
            clock.advance(Duration::from_secs(60)).unwrap();
            timer.advance_to(context.deadline).unwrap();
            assert_eq!(run(pending).unwrap_err().code, ErrorCode::Deadline);
        }
        assert_eq!(worker.snapshot().usage.items, 1);
        release_tx.send(()).unwrap();
        drain(&worker);
        let journal = FileRevisionJournal::open(
            &dir.path().join("revisions.json"),
            limits(),
            clock.clone(),
            clock.clone(),
            owner(),
        )
        .unwrap();
        assert_eq!(
            journal.load(&super_context(&clock)).unwrap().id,
            ConfigRevisionId(1)
        );
    }
}

struct GateWall {
    enabled: std::sync::atomic::AtomicBool,
    entered: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl marketforge_engine::live::foundation::clock::WallClock for GateWall {
    fn unix_time_ns(&self) -> Result<i64, ErrorCode> {
        if self.enabled.load(std::sync::atomic::Ordering::SeqCst) {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
        Ok(123)
    }
}
#[test]
fn wp09_worker_cancel_after_commit_entry_reports_unknown_and_retains_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 1));
    let context = context(&clock);
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let wall = Arc::new(GateWall {
        enabled: std::sync::atomic::AtomicBool::new(false),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let journal = FileRevisionJournal::create(
        &dir.path().join("revisions.json"),
        &initial(),
        limits(),
        clock.clone(),
        wall.clone(),
        owner(),
    )
    .unwrap();
    wall.enabled
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let worker = ConfigurationWorker::start(
        journal,
        compiler(dir.path()),
        Arc::new(ManualDeadlineTimer::new(clock.now(), 8).unwrap()),
        ResourceUsage {
            items: 1,
            bytes: 16 * 1024 * 1024,
        },
    )
    .unwrap();
    let mut changed = desired();
    changed.global.logging = LogLevel::Trace;
    let mut pending = worker.compare_and_swap(&context, ConfigRevisionId(1), changed);
    let waker = Waker::from(Arc::new(Signal(Mutex::new(false), Condvar::new())));
    let mut cx = Context::from_waker(&waker);
    assert!(pending.as_mut().poll(&mut cx).is_pending());
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    context.cancellation.cancel();
    assert_eq!(run(pending).unwrap_err().code, ErrorCode::CommitUnknown);
    assert_eq!(worker.snapshot().usage.items, 1);
    release_tx.send(()).unwrap();
    drain(&worker);
    // The receipt was conservatively uncertain. Recovery establishes that the
    // journal's final pre-persistence fence prevented this particular write.
    let journal = FileRevisionJournal::open(
        &dir.path().join("revisions.json"),
        limits(),
        clock.clone(),
        clock.clone(),
        owner(),
    )
    .unwrap();
    assert_eq!(
        journal.load(&super_context(&clock)).unwrap().id,
        ConfigRevisionId(1)
    );
}
