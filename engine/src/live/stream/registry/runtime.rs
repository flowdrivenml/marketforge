use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use thiserror::Error;

use super::{
    demand::DemandRegistry,
    graph::StreamGraph,
    reconcile::{LifecyclePlanner, ReconcileError, ReconcilePlan},
};

// ============================================================
// LIFECYCLE STATE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamLifecycleState {
    Stopped,
    Starting,
    Running,
    Recovering,
    Degraded,
    Stopping,
    Failed,
}

impl StreamLifecycleState {
    /// Whether a runtime execution context still needs
    /// lifecycle management or cleanup.
    ///
    /// Failed is included deliberately: a failed instance
    /// must not be restarted automatically on every
    /// reconciliation pass.
    pub fn occupies_runtime(self) -> bool {
        !matches!(self, Self::Stopped)
    }
}

// ============================================================
// GENERATION TOKEN
// ============================================================

/// Registry-owned operation generation.
///
/// Not an exchange sequence, transport connection stamp,
/// or PostgreSQL recording epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeGeneration(u64);

impl RuntimeGeneration {
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Revocable publication permission issued only by the runtime owner.
/// Revocation serializes with Router admission and delivery.
#[derive(Debug, Clone)]
pub struct RuntimePublicationAuthority {
    stream_id: String,
    pub(crate) definition: Arc<crate::live::stream::persistence::PersistedStreamDefinition>,
    generation: RuntimeGeneration,
    current: Arc<parking_lot::Mutex<bool>>,
    claimed: Arc<AtomicBool>,
    revoked: Arc<tokio::sync::Notify>,
}
impl RuntimePublicationAuthority {
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }
    pub fn generation(&self) -> RuntimeGeneration {
        self.generation
    }
    /// One Router binding per runtime generation, including across Router instances.
    pub(crate) fn claim(&self) -> bool {
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    pub(crate) async fn revoked(&self) {
        let notified = self.revoked.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !*self.guard() {
            return;
        }
        notified.await;
    }
    fn revoke(&self) {
        *self.guard() = false;
        self.revoked.notify_waiters();
    }
    pub(crate) fn try_guard(&self) -> Option<parking_lot::MutexGuard<'_, bool>> {
        self.current.try_lock()
    }
    pub(crate) fn guard_until(
        &self,
        deadline: std::time::Instant,
    ) -> Option<parking_lot::MutexGuard<'_, bool>> {
        self.current.try_lock_until(deadline)
    }
    pub(crate) fn guard(&self) -> parking_lot::MutexGuard<'_, bool> {
        self.current.lock()
    }
}

// ============================================================
// RUNTIME STATUS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRuntimeStatus {
    pub stream_id: String,
    pub lifecycle: StreamLifecycleState,
    pub generation: RuntimeGeneration,
    pub last_error: Option<String>,
}

impl StreamRuntimeStatus {
    pub fn is_ready(&self) -> bool {
        self.lifecycle == StreamLifecycleState::Running
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum StreamRuntimeError {
    #[error("unknown stream: {0}")]
    UnknownStream(String),

    #[error(
        "invalid lifecycle transition for {stream_id}: \
         {current:?} -> {requested:?}"
    )]
    InvalidTransition {
        stream_id: String,
        current: StreamLifecycleState,
        requested: StreamLifecycleState,
    },

    #[error("runtime generation exhausted: {0}")]
    GenerationExhausted(String),

    #[error("invalid stream failure description")]
    InvalidFailure,

    #[error(transparent)]
    Reconcile(#[from] ReconcileError),
}

// ============================================================
// RUNTIME REGISTRY
// ============================================================

/// Authoritative lifecycle bookkeeping for managed streams.
///
/// It does not:
/// - Create WebSocket connections.
/// - Perform normalization.
/// - Execute recovery.
/// - Publish canonical events.
/// - Write to PostgreSQL.
///
/// All mutation must be serialized by the owning
/// stream-management execution context.
#[derive(Debug)]
pub struct StreamRuntimeRegistry {
    streams: BTreeMap<String, StreamRuntimeStatus>,
    authorities: BTreeMap<String, RuntimePublicationAuthority>,
}

impl StreamRuntimeRegistry {
    /// Initialize runtime state from validated definitions.
    ///
    /// Every restored definition begins Stopped, regardless
    /// of its state before the previous process terminated.
    pub fn new(graph: &StreamGraph) -> Self {
        let streams = graph
            .stream_ids()
            .map(|stream_id| {
                (
                    stream_id.to_owned(),
                    StreamRuntimeStatus {
                        stream_id: stream_id.to_owned(),
                        lifecycle: StreamLifecycleState::Stopped,
                        generation: RuntimeGeneration(0),
                        last_error: None,
                    },
                )
            })
            .collect();

        let authorities = graph
            .stream_ids()
            .map(|id| {
                (
                    id.to_owned(),
                    RuntimePublicationAuthority {
                        stream_id: id.to_owned(),
                        definition: Arc::new(graph.get(id).unwrap().clone()),
                        generation: RuntimeGeneration(0),
                        current: Arc::new(parking_lot::Mutex::new(false)),
                        claimed: Arc::new(AtomicBool::new(false)),
                        revoked: Arc::new(tokio::sync::Notify::new()),
                    },
                )
            })
            .collect();
        Self {
            streams,
            authorities,
        }
    }

    pub fn publication_authority(&self, stream_id: &str) -> Option<RuntimePublicationAuthority> {
        self.authorities
            .get(stream_id)
            .filter(|a| *a.guard())
            .cloned()
    }

    pub fn status(&self, stream_id: &str) -> Option<&StreamRuntimeStatus> {
        self.streams.get(stream_id)
    }

    pub fn statuses(&self) -> impl Iterator<Item = &StreamRuntimeStatus> {
        self.streams.values()
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// Streams that currently occupy a runtime lifecycle.
    ///
    /// This includes failed instances to prevent infinite
    /// automatic restart loops.
    pub fn occupied_streams(&self) -> BTreeSet<String> {
        self.streams
            .iter()
            .filter(|(_, status)| status.lifecycle.occupies_runtime())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Produce the next lifecycle plan.
    ///
    /// This is a plan, not an execution result.
    pub fn plan(
        &self,
        graph: &StreamGraph,
        demand: &DemandRegistry,
    ) -> Result<ReconcilePlan, StreamRuntimeError> {
        Ok(LifecyclePlanner::plan(
            graph,
            demand,
            &self.occupied_streams(),
        )?)
    }

    // ========================================================
    // START
    // ========================================================

    /// Begin a new execution generation.
    ///
    /// An explicit retry may restart a Failed stream.
    /// Automatic reconciliation must not repeatedly retry
    /// Failed instances without a separate retry policy.
    pub fn begin_start(
        &mut self,
        stream_id: &str,
    ) -> Result<RuntimeGeneration, StreamRuntimeError> {
        let status = self.require_mut(stream_id)?;

        if !matches!(
            status.lifecycle,
            StreamLifecycleState::Stopped | StreamLifecycleState::Failed
        ) {
            return Err(StreamRuntimeError::InvalidTransition {
                stream_id: stream_id.to_owned(),
                current: status.lifecycle,
                requested: StreamLifecycleState::Starting,
            });
        }

        let next = status
            .generation
            .0
            .checked_add(1)
            .ok_or_else(|| StreamRuntimeError::GenerationExhausted(stream_id.to_owned()))?;

        status.generation = RuntimeGeneration(next);
        status.lifecycle = StreamLifecycleState::Starting;
        status.last_error = None;

        let generation = status.generation;
        if let Some(old) = self.authorities.insert(
            stream_id.to_owned(),
            RuntimePublicationAuthority {
                stream_id: stream_id.to_owned(),
                definition: self.authorities.get(stream_id).unwrap().definition.clone(),
                generation,
                current: Arc::new(parking_lot::Mutex::new(true)),
                claimed: Arc::new(AtomicBool::new(false)),
                revoked: Arc::new(tokio::sync::Notify::new()),
            },
        ) {
            old.revoke();
        }
        Ok(generation)
    }

    /// Mark a generation ready only after the owning
    /// instance has verified all readiness conditions.
    pub fn mark_running(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<bool, StreamRuntimeError> {
        self.transition_if_current(
            stream_id,
            generation,
            &[
                StreamLifecycleState::Starting,
                StreamLifecycleState::Recovering,
                StreamLifecycleState::Degraded,
            ],
            StreamLifecycleState::Running,
        )
    }

    /// An instance is active but is rebuilding its
    /// integrity/continuity state.
    pub fn mark_recovering(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<bool, StreamRuntimeError> {
        self.transition_if_current(
            stream_id,
            generation,
            &[
                StreamLifecycleState::Starting,
                StreamLifecycleState::Running,
                StreamLifecycleState::Degraded,
            ],
            StreamLifecycleState::Recovering,
        )
    }

    /// Mark an active instance as degraded.
    pub fn mark_degraded(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<bool, StreamRuntimeError> {
        self.transition_if_current(
            stream_id,
            generation,
            &[
                StreamLifecycleState::Starting,
                StreamLifecycleState::Running,
                StreamLifecycleState::Recovering,
            ],
            StreamLifecycleState::Degraded,
        )
    }

    // ========================================================
    // STOP
    // ========================================================

    /// Begin coordinated instance shutdown.
    ///
    /// Invalidates prior completion tokens.
    pub fn begin_stop(&mut self, stream_id: &str) -> Result<RuntimeGeneration, StreamRuntimeError> {
        let status = self.require_mut(stream_id)?;

        if matches!(
            status.lifecycle,
            StreamLifecycleState::Stopped | StreamLifecycleState::Stopping
        ) {
            return Err(StreamRuntimeError::InvalidTransition {
                stream_id: stream_id.to_owned(),
                current: status.lifecycle,
                requested: StreamLifecycleState::Stopping,
            });
        }

        let next = status
            .generation
            .0
            .checked_add(1)
            .ok_or_else(|| StreamRuntimeError::GenerationExhausted(stream_id.to_owned()))?;

        status.generation = RuntimeGeneration(next);
        status.lifecycle = StreamLifecycleState::Stopping;

        let generation = status.generation;
        if let Some(authority) = self.authorities.get(stream_id) {
            authority.revoke();
        }
        Ok(generation)
    }

    /// Complete shutdown of the current operation generation.
    pub fn mark_stopped(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
    ) -> Result<bool, StreamRuntimeError> {
        self.transition_if_current(
            stream_id,
            generation,
            &[StreamLifecycleState::Stopping],
            StreamLifecycleState::Stopped,
        )
    }

    // ========================================================
    // FAILURE
    // ========================================================

    /// Report a failure from the currently authoritative
    /// runtime operation.
    ///
    /// Returns false for obsolete operation generations.
    pub fn mark_failed(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
        error: impl Into<String>,
    ) -> Result<bool, StreamRuntimeError> {
        let error = error.into();

        if error.trim().is_empty() {
            return Err(StreamRuntimeError::InvalidFailure);
        }

        let status = self.require_mut(stream_id)?;

        if status.generation != generation {
            return Ok(false);
        }

        if status.lifecycle == StreamLifecycleState::Stopped {
            return Err(StreamRuntimeError::InvalidTransition {
                stream_id: stream_id.to_owned(),
                current: status.lifecycle,
                requested: StreamLifecycleState::Failed,
            });
        }

        status.lifecycle = StreamLifecycleState::Failed;
        status.last_error = Some(error);
        if let Some(authority) = self.authorities.get(stream_id) {
            authority.revoke();
        }

        Ok(true)
    }

    // ========================================================
    // INTERNAL TRANSITION
    // ========================================================

    fn transition_if_current(
        &mut self,
        stream_id: &str,
        generation: RuntimeGeneration,
        allowed: &[StreamLifecycleState],
        next: StreamLifecycleState,
    ) -> Result<bool, StreamRuntimeError> {
        let status = self.require_mut(stream_id)?;

        // A stale callback must never modify newer state.
        if status.generation != generation {
            return Ok(false);
        }

        if !allowed.contains(&status.lifecycle) {
            return Err(StreamRuntimeError::InvalidTransition {
                stream_id: stream_id.to_owned(),
                current: status.lifecycle,
                requested: next,
            });
        }

        status.lifecycle = next;

        Ok(true)
    }

    fn require_mut(
        &mut self,
        stream_id: &str,
    ) -> Result<&mut StreamRuntimeStatus, StreamRuntimeError> {
        self.streams
            .get_mut(stream_id)
            .ok_or_else(|| StreamRuntimeError::UnknownStream(stream_id.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::stream::{
        persistence::{DesiredStreamState, PersistedStreamDefinition, StreamDefinition},
        registry::DemandOwner,
        request::CreateSourceStreamRequest,
    };
    use crate::live::{
        config::ProcessingConfig,
        infrastructure::processing::{
            DerivedProcessorController, FailureReconcileOutcome, MailboxLimits, ProcessingPool,
            Processor, ProcessorInstance, ProcessorMailbox, ProcessorToken,
            reconcile_processor_failure,
        },
    };
    use serde_json::json;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;
    use std::time::Instant;

    fn graph() -> StreamGraph {
        let request: CreateSourceStreamRequest = serde_json::from_value(json!({
            "instrument_id": 15,
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "market_category": "spot",
            "instrument_type": "spot",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "settlement_asset": null,
            "quantity_type": "base",
            "contract_value": null,
            "contract_value_asset": null,
            "channel": "trades"
        }))
        .unwrap();

        StreamGraph::restore(
            vec![PersistedStreamDefinition {
                stream_id: "btc".into(),
                recording_stream_key: Some(15),
                definition: StreamDefinition::Source(request),
                desired: DesiredStreamState::default(),
                revision: 1,
            }],
            vec![],
        )
        .unwrap()
    }

    #[test]
    fn restored_streams_start_stopped() {
        let registry = StreamRuntimeRegistry::new(&graph());

        let status = registry.status("btc").unwrap();

        assert_eq!(status.lifecycle, StreamLifecycleState::Stopped);

        assert_eq!(status.generation.get(), 0);
        assert!(!status.is_ready());
    }

    #[test]
    fn start_requires_explicit_readiness_confirmation() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        let generation = registry.begin_start("btc").unwrap();

        assert_eq!(generation.get(), 1);

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Starting
        );

        assert!(!registry.status("btc").unwrap().is_ready());

        assert!(registry.mark_running("btc", generation).unwrap());

        assert!(registry.status("btc").unwrap().is_ready());
    }

    #[test]
    fn recovering_stream_is_not_ready() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        let generation = registry.begin_start("btc").unwrap();

        assert!(registry.mark_recovering("btc", generation).unwrap());

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Recovering
        );

        assert!(!registry.status("btc").unwrap().is_ready());

        registry.mark_running("btc", generation).unwrap();

        assert!(registry.status("btc").unwrap().is_ready());
    }

    #[test]
    fn stopping_invalidates_old_generation() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        let old_generation = registry.begin_start("btc").unwrap();

        let stop_generation = registry.begin_stop("btc").unwrap();

        assert_ne!(old_generation, stop_generation);

        // An obsolete startup completion cannot revive
        // the instance during shutdown.
        assert!(!registry.mark_running("btc", old_generation).unwrap());

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Stopping
        );

        registry.mark_stopped("btc", stop_generation).unwrap();

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Stopped
        );
    }

    #[test]
    fn old_shutdown_cannot_stop_new_generation() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        let first = registry.begin_start("btc").unwrap();
        registry.mark_running("btc", first).unwrap();

        let stopping = registry.begin_stop("btc").unwrap();

        registry.mark_stopped("btc", stopping).unwrap();

        let second = registry.begin_start("btc").unwrap();

        assert!(second.get() > stopping.get());

        assert!(!registry.mark_stopped("btc", stopping).unwrap());

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Starting
        );
    }

    #[test]
    fn duplicate_start_is_rejected() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        registry.begin_start("btc").unwrap();

        assert!(matches!(
            registry.begin_start("btc"),
            Err(StreamRuntimeError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn failure_is_visible_and_preserves_reason() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        let generation = registry.begin_start("btc").unwrap();

        registry
            .mark_failed("btc", generation, "WebSocket subscription failed")
            .unwrap();

        let status = registry.status("btc").unwrap();

        assert_eq!(status.lifecycle, StreamLifecycleState::Failed);

        assert_eq!(
            status.last_error.as_deref(),
            Some("WebSocket subscription failed")
        );

        assert!(!status.is_ready());
    }

    #[test]
    fn stale_failure_does_not_poison_new_generation() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        let first = registry.begin_start("btc").unwrap();

        registry
            .mark_failed("btc", first, "first attempt failed")
            .unwrap();

        let second = registry.begin_start("btc").unwrap();

        registry.mark_running("btc", second).unwrap();

        assert!(
            !registry
                .mark_failed("btc", first, "obsolete failure")
                .unwrap()
        );

        let status = registry.status("btc").unwrap();

        assert!(status.is_ready());
        assert_eq!(status.last_error, None);
    }

    #[test]
    fn planner_starts_demanded_stopped_stream() {
        let graph = graph();

        let registry = StreamRuntimeRegistry::new(&graph);

        let mut demand = DemandRegistry::new();

        demand
            .acquire(
                &graph,
                DemandOwner::Manual {
                    session_id: "operator".into(),
                },
                "btc",
            )
            .unwrap();

        let plan = registry.plan(&graph, &demand).unwrap();

        assert_eq!(plan.start, vec!["btc"]);
        assert!(plan.stop.is_empty());
    }

    #[test]
    fn starting_stream_is_not_scheduled_twice() {
        let graph = graph();

        let mut registry = StreamRuntimeRegistry::new(&graph);

        let mut demand = DemandRegistry::new();

        demand
            .acquire(
                &graph,
                DemandOwner::Manual {
                    session_id: "operator".into(),
                },
                "btc",
            )
            .unwrap();

        registry.begin_start("btc").unwrap();

        let plan = registry.plan(&graph, &demand).unwrap();

        assert!(plan.is_noop());
    }

    #[test]
    fn failed_stream_does_not_restart_automatically() {
        let graph = graph();

        let mut registry = StreamRuntimeRegistry::new(&graph);

        let mut demand = DemandRegistry::new();

        demand
            .acquire(
                &graph,
                DemandOwner::Manual {
                    session_id: "operator".into(),
                },
                "btc",
            )
            .unwrap();

        let generation = registry.begin_start("btc").unwrap();

        registry
            .mark_failed("btc", generation, "startup failure")
            .unwrap();

        let plan = registry.plan(&graph, &demand).unwrap();

        assert!(plan.is_noop());
    }

    #[test]
    fn removing_demand_schedules_stop() {
        let graph = graph();

        let mut registry = StreamRuntimeRegistry::new(&graph);

        let demand = DemandRegistry::new();

        let generation = registry.begin_start("btc").unwrap();

        registry.mark_running("btc", generation).unwrap();

        let plan = registry.plan(&graph, &demand).unwrap();

        assert_eq!(plan.stop, vec!["btc"]);
        assert!(plan.start.is_empty());
    }

    #[test]
    fn unknown_stream_is_rejected() {
        let mut registry = StreamRuntimeRegistry::new(&graph());

        assert!(matches!(
            registry.begin_start("unknown"),
            Err(StreamRuntimeError::UnknownStream(_))
        ));
    }
    #[test]
    fn processor_failure_transitions_authoritative_stream() {
        struct PanickingProcessor;

        impl Processor for PanickingProcessor {
            fn run_turn(&mut self, _: usize) -> bool {
                panic!("simulated derived processor failure");
            }
        }

        let mut registry = StreamRuntimeRegistry::new(&graph());

        let stream_generation = registry.begin_start("btc").unwrap();

        assert!(registry.mark_running("btc", stream_generation).unwrap());

        let mut pool = ProcessingPool::new(ProcessingConfig {
            workers: 1,
            ready_queue_capacity: 8,
            max_events_per_turn: 16,
        })
        .unwrap();

        let processor = ProcessorToken {
            id: 42,
            generation: 1,
        };

        pool.register(processor, PanickingProcessor).unwrap();

        pool.notify(processor).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);

        while pool.failure(processor.id).is_none() {
            assert!(
                Instant::now() < deadline,
                "processor failure was not recorded"
            );

            std::thread::yield_now();
        }

        assert_eq!(
            reconcile_processor_failure(&mut registry, &pool, "btc", stream_generation, processor,)
                .unwrap(),
            FailureReconcileOutcome::Applied
        );

        let status = registry.status("btc").unwrap();

        assert_eq!(status.lifecycle, StreamLifecycleState::Failed);

        assert!(
            status
                .last_error
                .as_ref()
                .unwrap()
                .contains("derived processor")
        );

        // The failure is acknowledged only after the
        // authoritative registry records it.
        assert!(pool.failure(processor.id).is_none());

        // Failed streams cannot retain publication authority.
        assert!(registry.publication_authority("btc").is_none());

        pool.shutdown();
    }
    #[test]
    fn obsolete_processor_failure_cannot_fail_new_stream_generation() {
        struct PanickingProcessor;

        impl Processor for PanickingProcessor {
            fn run_turn(&mut self, _: usize) -> bool {
                panic!("obsolete processor failure");
            }
        }

        let mut registry = StreamRuntimeRegistry::new(&graph());

        let old_generation = registry.begin_start("btc").unwrap();

        let stopping = registry.begin_stop("btc").unwrap();

        registry.mark_stopped("btc", stopping).unwrap();

        let new_generation = registry.begin_start("btc").unwrap();

        registry.mark_running("btc", new_generation).unwrap();

        let mut pool = ProcessingPool::new(ProcessingConfig {
            workers: 1,
            ready_queue_capacity: 8,
            max_events_per_turn: 16,
        })
        .unwrap();

        let processor = ProcessorToken {
            id: 42,
            generation: 1,
        };

        pool.register(processor, PanickingProcessor).unwrap();

        pool.notify(processor).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);

        while pool.failure(processor.id).is_none() {
            assert!(
                Instant::now() < deadline,
                "obsolete processor failure was not recorded"
            );

            std::thread::yield_now();
        }

        assert_eq!(
            reconcile_processor_failure(&mut registry, &pool, "btc", old_generation, processor,)
                .unwrap(),
            FailureReconcileOutcome::Stale
        );

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Running
        );

        assert_eq!(registry.status("btc").unwrap().generation, new_generation);

        // The stale failure remains available for explicit cleanup.
        assert!(pool.failure(processor.id).is_some());

        assert!(pool.acknowledge_failure(processor));

        pool.shutdown();
    }
    #[test]
    fn derived_controller_fences_publication_before_retirement() {
        struct BlockingProcessor {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
        }

        impl Processor for BlockingProcessor {
            fn run_turn(&mut self, _: usize) -> bool {
                self.started.send(()).unwrap();
                self.release.recv().unwrap();
                false
            }
        }

        let mut registry = StreamRuntimeRegistry::new(&graph());

        let stream_generation = registry.begin_start("btc").unwrap();

        registry.mark_running("btc", stream_generation).unwrap();

        let pool = Arc::new(
            ProcessingPool::new(ProcessingConfig {
                workers: 1,
                ready_queue_capacity: 16,
                max_events_per_turn: 8,
            })
            .unwrap(),
        );

        let token = ProcessorToken {
            id: 42,
            generation: 1,
        };

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let instance = ProcessorInstance::<u64>::start(
            Arc::clone(&pool),
            token,
            MailboxLimits {
                max_events: 16,
                max_bytes: 1024,
            },
            move |_mailbox: Arc<ProcessorMailbox<u64>>| BlockingProcessor {
                started: started_tx,
                release: release_rx,
            },
        )
        .unwrap();

        let mut controller =
            DerivedProcessorController::new("btc".to_string(), stream_generation, instance);

        // Arrange an executing processor turn.
        pool.notify(token).unwrap();

        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        // Stop needs a separate thread because retirement blocks.
        let stop_thread = std::thread::spawn(move || {
            let result = controller.stop(&mut registry);
            (result, registry)
        });

        // Registry.begin_stop() executes before retirement.
        //
        // The test cannot inspect the moved registry until
        // stop() returns, but the production method's ordering
        // makes publication fencing precede the blocking wait.
        release_tx.send(()).unwrap();

        let (outcome, registry) = stop_thread.join().unwrap();

        let outcome = outcome.unwrap();

        assert!(outcome.failure.is_none());

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Stopped,
        );

        assert!(registry.publication_authority("btc").is_none());

        assert_eq!(pool.registered_count(), 0);
    }
    #[test]
    fn stale_derived_controller_cannot_stop_new_generation() {
        struct NoopProcessor;

        impl Processor for NoopProcessor {
            fn run_turn(&mut self, _: usize) -> bool {
                false
            }
        }

        let mut registry = StreamRuntimeRegistry::new(&graph());

        let old_generation = registry.begin_start("btc").unwrap();

        let stopping = registry.begin_stop("btc").unwrap();

        registry.mark_stopped("btc", stopping).unwrap();

        let new_generation = registry.begin_start("btc").unwrap();

        registry.mark_running("btc", new_generation).unwrap();

        let pool = Arc::new(
            ProcessingPool::new(ProcessingConfig {
                workers: 1,
                ready_queue_capacity: 16,
                max_events_per_turn: 8,
            })
            .unwrap(),
        );

        let instance = ProcessorInstance::<u64>::start(
            Arc::clone(&pool),
            ProcessorToken {
                id: 42,
                generation: 1,
            },
            MailboxLimits {
                max_events: 16,
                max_bytes: 1024,
            },
            |_mailbox| NoopProcessor,
        )
        .unwrap();

        let mut controller =
            DerivedProcessorController::new("btc".into(), old_generation, instance);

        assert!(matches!(
            controller.stop(&mut registry),
            Err(crate::live::infrastructure::processing::ControllerError::StaleGeneration)
        ));

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Running,
        );

        assert_eq!(registry.status("btc").unwrap().generation, new_generation,);

        // Explicit cleanup of the unattached test processor.
        // In production, the owner must perform this cleanup
        // before releasing the controller.
        pool.retire(controller.processor()).unwrap();
    }
    #[test]
    fn derived_controller_requires_correct_stop_phase_order() {
        struct NoopProcessor;

        impl Processor for NoopProcessor {
            fn run_turn(&mut self, _: usize) -> bool {
                false
            }
        }

        let mut registry = StreamRuntimeRegistry::new(&graph());

        let generation = registry.begin_start("btc").unwrap();

        registry.mark_running("btc", generation).unwrap();

        let pool = Arc::new(
            ProcessingPool::new(ProcessingConfig {
                workers: 1,
                ready_queue_capacity: 16,
                max_events_per_turn: 8,
            })
            .unwrap(),
        );

        let token = ProcessorToken {
            id: 50,
            generation: 1,
        };

        let instance = ProcessorInstance::<u64>::start(
            Arc::clone(&pool),
            token,
            MailboxLimits {
                max_events: 16,
                max_bytes: 1024,
            },
            |_mailbox| NoopProcessor,
        )
        .unwrap();

        let mut controller = DerivedProcessorController::new("btc".into(), generation, instance);

        // Cannot retire before publication fencing.
        assert!(matches!(
            controller.retire_processor(),
            Err(crate::live::infrastructure::processing::ControllerError::InvalidPhase(_))
        ));

        // Cannot finalize before retirement.
        assert!(matches!(
            controller.complete_stop(&mut registry),
            Err(crate::live::infrastructure::processing::ControllerError::InvalidPhase(_))
        ));

        let stop_generation = controller.begin_stop(&mut registry).unwrap();

        assert_ne!(stop_generation, generation);

        // Retrying phase A must not advance the generation.
        assert_eq!(
            controller.begin_stop(&mut registry).unwrap(),
            stop_generation
        );

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Stopping
        );

        assert!(registry.publication_authority("btc").is_none());

        controller.retire_processor().unwrap();

        // Retrying completed resource retirement is harmless.
        controller.retire_processor().unwrap();

        let outcome = controller.complete_stop(&mut registry).unwrap();

        assert!(outcome.failure.is_none());
        assert!(outcome.unprocessed.is_empty());

        assert_eq!(
            registry.status("btc").unwrap().lifecycle,
            StreamLifecycleState::Stopped
        );

        assert_eq!(pool.registered_count(), 0);
    }
    #[test]
    fn processor_retirement_does_not_hold_registry_lock() {
        use std::sync::{Arc, Mutex, mpsc};
        use std::time::Duration;

        struct BlockingProcessor {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
        }

        impl Processor for BlockingProcessor {
            fn run_turn(&mut self, _: usize) -> bool {
                self.started.send(()).unwrap();

                // Simulate an expensive CPU-processing turn.
                self.release.recv().unwrap();

                false
            }
        }

        let registry = Arc::new(Mutex::new(StreamRuntimeRegistry::new(&graph())));

        let generation = {
            let mut registry = registry.lock().unwrap();

            let generation = registry.begin_start("btc").unwrap();
            registry.mark_running("btc", generation).unwrap();

            generation
        };

        let pool = Arc::new(
            ProcessingPool::new(ProcessingConfig {
                workers: 1,
                ready_queue_capacity: 16,
                max_events_per_turn: 8,
            })
            .unwrap(),
        );

        let token = ProcessorToken {
            id: 51,
            generation: 1,
        };

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (fenced_tx, fenced_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();

        let instance = ProcessorInstance::<u64>::start(
            Arc::clone(&pool),
            token,
            MailboxLimits {
                max_events: 16,
                max_bytes: 1024,
            },
            move |_mailbox| BlockingProcessor {
                started: started_tx,
                release: release_rx,
            },
        )
        .unwrap();

        let mut controller = DerivedProcessorController::new("btc".into(), generation, instance);

        pool.notify(token).unwrap();

        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let controller_registry = Arc::clone(&registry);

        let shutdown_thread = std::thread::spawn(move || {
            // Phase A: brief exclusive access to the registry.
            {
                let mut registry = controller_registry.lock().unwrap();

                controller.begin_stop(&mut registry).unwrap();
            }

            fenced_tx.send(()).unwrap();

            // Phase B: potentially blocking CPU retirement.
            // No registry mutex guard exists in this scope.
            controller.retire_processor().unwrap();

            // Phase C: brief registry access for finalization.
            {
                let mut registry = controller_registry.lock().unwrap();

                controller.complete_stop(&mut registry).unwrap();
            }

            completed_tx.send(()).unwrap();
        });

        // Publication fencing must happen before retirement finishes.
        let fenced = fenced_rx.recv_timeout(Duration::from_secs(2)).is_ok();

        // While the CPU worker is still blocked, the main thread
        // should be able to inspect the authoritative registry.
        let registry_accessible = registry.try_lock().ok().map(|registry| {
            (
                registry.status("btc").unwrap().lifecycle,
                registry.publication_authority("btc").is_none(),
            )
        });

        // The blocked CPU turn should prevent shutdown completion.
        let finished_early = completed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();

        // Always release the worker before assertions.
        // Otherwise a failing assertion could strand shutdown.
        release_tx.send(()).unwrap();

        let completed = completed_rx.recv_timeout(Duration::from_secs(2)).is_ok();

        shutdown_thread.join().unwrap();

        assert!(fenced, "publication was not fenced");

        assert_eq!(
            registry_accessible,
            Some((StreamLifecycleState::Stopping, true)),
            "registry was inaccessible or publication remained enabled"
        );

        assert!(
            !finished_early,
            "shutdown completed before the processor finished"
        );

        assert!(completed, "shutdown did not finish");

        assert_eq!(
            registry.lock().unwrap().status("btc").unwrap().lifecycle,
            StreamLifecycleState::Stopped
        );
    }
}

impl Drop for StreamRuntimeRegistry {
    fn drop(&mut self) {
        for authority in self.authorities.values() {
            authority.revoke();
        }
    }
}
