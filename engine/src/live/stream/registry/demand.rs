use std::collections::{BTreeMap, BTreeSet, HashSet};

use thiserror::Error;

use super::graph::{StreamGraph, StreamGraphError};

/// Identity of a runtime consumer.
///
/// Derived-stream dependencies are resolved through StreamGraph,
/// not duplicated here as manually maintained reference counts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DemandOwner {
    Unix {
        connection_id: String,
        subscription_id: String,
    },

    Direct {
        consumer_id: String,
        subscription_id: String,
    },

    Manual {
        session_id: String,
    },

    Recording {
        recorder_id: String,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DemandError {
    #[error("demand owner is already registered")]
    AlreadyOwned,

    #[error("demand owner is unknown")]
    UnknownOwner,

    #[error("demand owner does not belong to stream {0}")]
    WrongStream(String),

    #[error(transparent)]
    Graph(#[from] StreamGraphError),
}

/// Owns ephemeral runtime demand.
///
/// Does not own WebSocket connections, stream instances,
/// PostgreSQL definitions, or persistent dependencies.
#[derive(Debug, Default)]
pub struct DemandRegistry {
    owners: BTreeMap<DemandOwner, String>,
}

impl DemandRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire demand for an existing, enabled stream.
    ///
    /// The change is committed only if the resulting
    /// dependency graph can be resolved successfully.
    pub fn acquire(
        &mut self,
        graph: &StreamGraph,
        owner: DemandOwner,
        stream_id: &str,
    ) -> Result<BTreeSet<String>, DemandError> {
        if self.owners.contains_key(&owner) {
            return Err(DemandError::AlreadyOwned);
        }

        let mut proposed = self.owners.clone();

        proposed.insert(owner, stream_id.to_owned());

        let required = resolve(graph, &proposed)?;

        self.owners = proposed;

        Ok(required)
    }

    /// Release only the specified owner's demand.
    ///
    /// `stream_id` prevents a caller from accidentally
    /// releasing an owner assigned to another stream.
    pub fn release(
        &mut self,
        graph: &StreamGraph,
        owner: &DemandOwner,
        stream_id: &str,
    ) -> Result<BTreeSet<String>, DemandError> {
        match self.owners.get(owner) {
            None => return Err(DemandError::UnknownOwner),

            Some(current) if current != stream_id => {
                return Err(DemandError::WrongStream(stream_id.to_owned()));
            }

            Some(_) => {}
        }

        let mut proposed = self.owners.clone();

        proposed.remove(owner);

        let required = resolve(graph, &proposed)?;

        self.owners = proposed;

        Ok(required)
    }

    /// Resolve the complete currently required stream set.
    pub fn required_streams(&self, graph: &StreamGraph) -> Result<BTreeSet<String>, DemandError> {
        resolve(graph, &self.owners)
    }

    /// Number of direct ephemeral demand owners.
    ///
    /// This excludes implicit derived dependencies
    /// and persistent AlwaysOn activation.
    pub fn owner_count(&self, stream_id: &str) -> usize {
        self.owners
            .values()
            .filter(|current| current.as_str() == stream_id)
            .count()
    }

    pub fn total_owners(&self) -> usize {
        self.owners.len()
    }

    pub fn owner_stream(&self, owner: &DemandOwner) -> Option<&str> {
        self.owners.get(owner).map(String::as_str)
    }
}

fn resolve(
    graph: &StreamGraph,
    owners: &BTreeMap<DemandOwner, String>,
) -> Result<BTreeSet<String>, DemandError> {
    let roots: HashSet<String> = owners.values().cloned().collect();

    Ok(graph.required_streams(&roots)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::stream::{
        persistence::{
            ActivationPolicy, DesiredStreamState, PersistedStreamDefinition, StreamDefinition,
            StreamDependency,
        },
        request::{
            CreateDerivedStreamRequest, CreateSourceStreamRequest, DerivedOperation, MergeOrdering,
        },
    };

    use serde_json::json;

    fn source(id: &str, enabled: bool) -> PersistedStreamDefinition {
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
            desired: DesiredStreamState {
                enabled,
                activation_policy: ActivationPolicy::OnDemand,
            },
            revision: 1,
        }
    }

    fn merged() -> PersistedStreamDefinition {
        PersistedStreamDefinition {
            stream_id: "merged".into(),
            recording_stream_key: None,
            definition: StreamDefinition::Derived(CreateDerivedStreamRequest {
                name: "merged".into(),
                operation: DerivedOperation::Merge,
                inputs: vec!["binance".into(), "bybit".into()],
                ordering: Some(MergeOrdering::ArrivalOrder),
                max_lateness_ns: None,
                target_quote_asset: None,
            }),
            desired: DesiredStreamState::default(),
            revision: 1,
        }
    }

    fn graph() -> StreamGraph {
        StreamGraph::restore(
            vec![source("binance", true), source("bybit", true), merged()],
            vec![
                StreamDependency {
                    derived_stream_id: "merged".into(),
                    input_stream_id: "binance".into(),
                    input_position: 0,
                },
                StreamDependency {
                    derived_stream_id: "merged".into(),
                    input_stream_id: "bybit".into(),
                    input_position: 1,
                },
            ],
        )
        .unwrap()
    }

    fn owner(id: &str) -> DemandOwner {
        DemandOwner::Manual {
            session_id: id.into(),
        }
    }

    #[test]
    fn acquiring_demand_activates_source() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        let required = demand.acquire(&graph, owner("one"), "binance").unwrap();

        assert!(required.contains("binance"));
        assert!(!required.contains("bybit"));

        assert_eq!(demand.owner_count("binance"), 1);
    }

    #[test]
    fn derived_demand_activates_all_inputs() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        let required = demand.acquire(&graph, owner("one"), "merged").unwrap();

        assert!(required.contains("merged"));
        assert!(required.contains("binance"));
        assert!(required.contains("bybit"));
    }

    #[test]
    fn releasing_one_owner_preserves_other_demand() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        demand.acquire(&graph, owner("one"), "binance").unwrap();

        demand.acquire(&graph, owner("two"), "binance").unwrap();

        assert_eq!(demand.owner_count("binance"), 2);

        let required = demand.release(&graph, &owner("one"), "binance").unwrap();

        assert_eq!(demand.owner_count("binance"), 1);
        assert!(required.contains("binance"));
    }

    #[test]
    fn releasing_last_owner_removes_on_demand_stream() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        demand.acquire(&graph, owner("one"), "binance").unwrap();

        let required = demand.release(&graph, &owner("one"), "binance").unwrap();

        assert!(required.is_empty());
        assert_eq!(demand.total_owners(), 0);
    }

    #[test]
    fn duplicate_owner_is_rejected_without_mutation() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        demand.acquire(&graph, owner("one"), "binance").unwrap();

        let result = demand.acquire(&graph, owner("one"), "bybit");

        assert!(matches!(result, Err(DemandError::AlreadyOwned)));

        assert_eq!(demand.total_owners(), 1);
        assert_eq!(demand.owner_stream(&owner("one")), Some("binance"));
    }

    #[test]
    fn unknown_owner_cannot_release_demand() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        let result = demand.release(&graph, &owner("unknown"), "binance");

        assert!(matches!(result, Err(DemandError::UnknownOwner)));

        assert_eq!(demand.total_owners(), 0);
    }

    #[test]
    fn owner_cannot_release_another_stream() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        demand.acquire(&graph, owner("one"), "binance").unwrap();

        let result = demand.release(&graph, &owner("one"), "bybit");

        assert!(matches!(result, Err(DemandError::WrongStream(_))));

        assert_eq!(demand.owner_stream(&owner("one")), Some("binance"));
    }

    #[test]
    fn multiple_consumer_types_are_independent() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        let unix = DemandOwner::Unix {
            connection_id: "connection-1".into(),
            subscription_id: "subscription-1".into(),
        };

        let direct = DemandOwner::Direct {
            consumer_id: "strategy-1".into(),
            subscription_id: "subscription-1".into(),
        };

        let recorder = DemandOwner::Recording {
            recorder_id: "postgres-1".into(),
        };

        demand.acquire(&graph, unix.clone(), "binance").unwrap();

        demand.acquire(&graph, direct, "binance").unwrap();

        demand.acquire(&graph, recorder, "binance").unwrap();

        assert_eq!(demand.owner_count("binance"), 3);

        let required = demand.release(&graph, &unix, "binance").unwrap();

        assert!(required.contains("binance"));
        assert_eq!(demand.owner_count("binance"), 2);
    }

    #[test]
    fn derived_dependency_survives_direct_owner_release() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        demand
            .acquire(&graph, owner("merged-owner"), "merged")
            .unwrap();

        demand
            .acquire(&graph, owner("direct-owner"), "binance")
            .unwrap();

        let required = demand
            .release(&graph, &owner("direct-owner"), "binance")
            .unwrap();

        // Binance remains required through the merged stream.
        assert!(required.contains("binance"));
        assert!(required.contains("bybit"));
        assert!(required.contains("merged"));

        assert_eq!(demand.owner_count("binance"), 0);
        assert_eq!(demand.owner_count("merged"), 1);
    }

    #[test]
    fn disabled_stream_rejects_acquisition_without_mutation() {
        let graph = StreamGraph::restore(vec![source("binance", false)], vec![]).unwrap();

        let mut demand = DemandRegistry::new();

        let result = demand.acquire(&graph, owner("one"), "binance");

        assert!(matches!(
            result,
            Err(DemandError::Graph(
                StreamGraphError::RequiredStreamDisabled(_)
            ))
        ));

        assert_eq!(demand.total_owners(), 0);
    }

    #[test]
    fn nonexistent_stream_rejects_acquisition_without_mutation() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        let result = demand.acquire(&graph, owner("one"), "missing");

        assert!(matches!(
            result,
            Err(DemandError::Graph(StreamGraphError::UnknownStream(_)))
        ));

        assert_eq!(demand.total_owners(), 0);
    }

    #[test]
    fn failed_acquisition_preserves_existing_owners() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        demand.acquire(&graph, owner("one"), "binance").unwrap();

        let result = demand.acquire(&graph, owner("two"), "missing");

        assert!(result.is_err());

        assert_eq!(demand.total_owners(), 1);
        assert_eq!(demand.owner_stream(&owner("one")), Some("binance"));
        assert_eq!(demand.owner_stream(&owner("two")), None);
    }

    #[test]
    fn distinct_subscriptions_on_one_connection_are_independent() {
        let graph = graph();
        let mut demand = DemandRegistry::new();

        let first = DemandOwner::Unix {
            connection_id: "client-1".into(),
            subscription_id: "sub-1".into(),
        };

        let second = DemandOwner::Unix {
            connection_id: "client-1".into(),
            subscription_id: "sub-2".into(),
        };

        demand.acquire(&graph, first.clone(), "binance").unwrap();

        demand.acquire(&graph, second.clone(), "bybit").unwrap();

        let required = demand.release(&graph, &first, "binance").unwrap();

        assert!(!required.contains("binance"));
        assert!(required.contains("bybit"));

        assert_eq!(demand.owner_stream(&first), None);
        assert_eq!(demand.owner_stream(&second), Some("bybit"));
    }
}
