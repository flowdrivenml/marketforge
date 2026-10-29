use std::collections::{BTreeSet, HashSet};

use thiserror::Error;

use super::{
    demand::{DemandError, DemandRegistry},
    graph::StreamGraph,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilePlan {
    /// Ordered from dependents to dependencies.
    pub stop: Vec<String>,

    /// Ordered from dependencies to dependents.
    pub start: Vec<String>,

    /// Complete desired running set.
    pub target: BTreeSet<String>,
}

impl ReconcilePlan {
    pub fn is_noop(&self) -> bool {
        self.stop.is_empty() && self.start.is_empty()
    }
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Demand(#[from] DemandError),

    #[error("unknown currently running stream: {0}")]
    UnknownRunningStream(String),

    #[error("stream dependency graph is inconsistent: {0}")]
    InvalidGraph(String),
}

/// Computes safe lifecycle transitions.
///
/// This component does not mutate the graph, demand registry,
/// or running instances.
///
/// Runtime execution must independently verify every operation.
pub struct LifecyclePlanner;

impl LifecyclePlanner {
    pub fn plan(
        graph: &StreamGraph,
        demand: &DemandRegistry,
        running: &BTreeSet<String>,
    ) -> Result<ReconcilePlan, ReconcileError> {
        for stream_id in running {
            if graph.get(stream_id).is_none() {
                return Err(ReconcileError::UnknownRunningStream(stream_id.clone()));
            }
        }

        let target = demand.required_streams(graph)?;

        let starts: BTreeSet<String> = target.difference(running).cloned().collect();

        let stops: BTreeSet<String> = running.difference(&target).cloned().collect();

        let start = order_dependencies_first(graph, &starts)?;

        let mut stop = order_dependencies_first(graph, &stops)?;
        stop.reverse();

        Ok(ReconcilePlan {
            stop,
            start,
            target,
        })
    }
}

fn order_dependencies_first(
    graph: &StreamGraph,
    selected: &BTreeSet<String>,
) -> Result<Vec<String>, ReconcileError> {
    let mut visited = HashSet::new();
    let mut result = Vec::with_capacity(selected.len());

    for stream_id in selected {
        visit(graph, stream_id, selected, &mut visited, &mut result)?;
    }

    Ok(result)
}

fn visit(
    graph: &StreamGraph,
    stream_id: &str,
    selected: &BTreeSet<String>,
    visited: &mut HashSet<String>,
    result: &mut Vec<String>,
) -> Result<(), ReconcileError> {
    if !visited.insert(stream_id.to_owned()) {
        return Ok(());
    }

    let inputs = graph.inputs(stream_id).ok_or_else(|| {
        ReconcileError::InvalidGraph(format!("missing dependency entry for {stream_id}"))
    })?;

    for input in inputs {
        if selected.contains(input) {
            visit(graph, input, selected, visited, result)?;
        }
    }

    result.push(stream_id.to_owned());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::stream::{
        persistence::{
            ActivationPolicy, DesiredStreamState, PersistedStreamDefinition, StreamDefinition,
            StreamDependency,
        },
        registry::DemandOwner,
        request::{
            CreateDerivedStreamRequest, CreateSourceStreamRequest, DerivedOperation, MergeOrdering,
        },
    };

    use serde_json::json;

    fn source(id: &str) -> PersistedStreamDefinition {
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

        PersistedStreamDefinition {
            stream_id: id.into(),
            recording_stream_key: Some(15),
            definition: StreamDefinition::Source(request),
            desired: DesiredStreamState::default(),
            revision: 1,
        }
    }

    fn derived(id: &str, inputs: &[&str]) -> PersistedStreamDefinition {
        PersistedStreamDefinition {
            stream_id: id.into(),
            recording_stream_key: None,

            definition: StreamDefinition::Derived(CreateDerivedStreamRequest {
                name: id.into(),
                operation: DerivedOperation::Merge,
                inputs: inputs.iter().map(|s| s.to_string()).collect(),
                ordering: Some(MergeOrdering::ArrivalOrder),
                max_lateness_ns: None,
                target_quote_asset: None,
            }),

            desired: DesiredStreamState {
                enabled: true,
                activation_policy: ActivationPolicy::OnDemand,
            },

            revision: 1,
        }
    }

    fn edge(derived: &str, input: &str, position: i32) -> StreamDependency {
        StreamDependency {
            derived_stream_id: derived.into(),
            input_stream_id: input.into(),
            input_position: position,
        }
    }

    fn graph() -> StreamGraph {
        StreamGraph::restore(
            vec![
                source("binance"),
                source("bybit"),
                derived("merged", &["binance", "bybit"]),
                derived("converted", &["merged", "binance"]),
            ],
            vec![
                edge("merged", "binance", 0),
                edge("merged", "bybit", 1),
                edge("converted", "merged", 0),
                edge("converted", "binance", 1),
            ],
        )
        .unwrap()
    }

    fn demand_for(graph: &StreamGraph, stream_id: &str) -> DemandRegistry {
        let mut demand = DemandRegistry::new();

        demand
            .acquire(
                graph,
                DemandOwner::Manual {
                    session_id: "test".into(),
                },
                stream_id,
            )
            .unwrap();

        demand
    }

    #[test]
    fn starts_dependencies_before_derived_streams() {
        let graph = graph();
        let demand = demand_for(&graph, "converted");

        let plan = LifecyclePlanner::plan(&graph, &demand, &BTreeSet::new()).unwrap();

        let position = |id: &str| plan.start.iter().position(|stream| stream == id).unwrap();

        assert!(position("binance") < position("merged"));
        assert!(position("bybit") < position("merged"));
        assert!(position("merged") < position("converted"));

        assert!(plan.stop.is_empty());
        assert_eq!(plan.start.len(), 4);
    }

    #[test]
    fn stops_derived_streams_before_sources() {
        let graph = graph();
        let demand = DemandRegistry::new();

        let running = BTreeSet::from([
            "binance".into(),
            "bybit".into(),
            "merged".into(),
            "converted".into(),
        ]);

        let plan = LifecyclePlanner::plan(&graph, &demand, &running).unwrap();

        let position = |id: &str| plan.stop.iter().position(|stream| stream == id).unwrap();

        assert!(position("converted") < position("merged"));
        assert!(position("merged") < position("binance"));
        assert!(position("merged") < position("bybit"));

        assert!(plan.start.is_empty());
    }

    #[test]
    fn existing_dependencies_are_not_restarted() {
        let graph = graph();
        let demand = demand_for(&graph, "merged");

        let running = BTreeSet::from(["binance".into(), "bybit".into()]);

        let plan = LifecyclePlanner::plan(&graph, &demand, &running).unwrap();

        assert_eq!(plan.start, vec!["merged"]);
        assert!(plan.stop.is_empty());
    }

    #[test]
    fn unchanged_demand_produces_noop() {
        let graph = graph();
        let demand = demand_for(&graph, "merged");

        let running = BTreeSet::from(["binance".into(), "bybit".into(), "merged".into()]);

        let plan = LifecyclePlanner::plan(&graph, &demand, &running).unwrap();

        assert!(plan.is_noop());
    }

    #[test]
    fn extra_source_is_stopped_without_disturbing_dependencies() {
        let graph = graph();
        let demand = demand_for(&graph, "binance");

        let running = BTreeSet::from(["binance".into(), "bybit".into()]);

        let plan = LifecyclePlanner::plan(&graph, &demand, &running).unwrap();

        assert_eq!(plan.stop, vec!["bybit"]);
        assert!(plan.start.is_empty());

        assert!(plan.target.contains("binance"));
        assert!(!plan.target.contains("bybit"));
    }

    #[test]
    fn rejects_unknown_running_stream() {
        let graph = graph();
        let demand = DemandRegistry::new();

        let running = BTreeSet::from(["missing".to_owned()]);

        assert!(matches!(
            LifecyclePlanner::plan(&graph, &demand, &running),
            Err(ReconcileError::UnknownRunningStream(id))
                if id == "missing"
        ));
    }

    #[test]
    fn empty_runtime_without_demand_needs_no_changes() {
        let graph = graph();
        let demand = DemandRegistry::new();

        let plan = LifecyclePlanner::plan(&graph, &demand, &BTreeSet::new()).unwrap();

        assert!(plan.is_noop());
        assert!(plan.target.is_empty());
    }
}
