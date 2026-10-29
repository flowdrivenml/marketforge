use std::collections::{BTreeMap, BTreeSet};

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

        Self { streams }
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

        Ok(status.generation)
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

        Ok(status.generation)
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

    use serde_json::json;

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
}
