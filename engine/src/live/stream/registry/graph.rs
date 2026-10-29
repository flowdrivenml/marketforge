use std::collections::{BTreeMap, BTreeSet, HashSet};

use thiserror::Error;

use crate::live::stream::persistence::{
    ActivationPolicy, PersistedStreamDefinition, StreamDefinition, StreamDependency,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StreamGraphError {
    #[error("duplicate stream definition: {0}")]
    DuplicateStream(String),

    #[error("unknown stream: {0}")]
    UnknownStream(String),

    #[error("invalid stream definition {0}: {1}")]
    InvalidDefinition(String, String),

    #[error("invalid dependency: {0}")]
    InvalidDependency(String),

    #[error("dependency cycle detected at stream: {0}")]
    DependencyCycle(String),

    #[error("required stream is disabled: {0}")]
    RequiredStreamDisabled(String),
}

/// Validated, read-only dependency graph.
///
/// Edges point from a derived stream to the streams
/// that it requires as inputs.
///
/// This structure does not own running instances.
#[derive(Debug)]
pub struct StreamGraph {
    definitions: BTreeMap<String, PersistedStreamDefinition>,
    dependencies: BTreeMap<String, Vec<String>>,
    dependents: BTreeMap<String, BTreeSet<String>>,
}

impl StreamGraph {
    /// Reconstruct and validate the persisted stream graph.
    ///
    /// No runtime instances are started here.
    pub fn restore(
        definitions: Vec<PersistedStreamDefinition>,
        dependencies: Vec<StreamDependency>,
    ) -> Result<Self, StreamGraphError> {
        let mut by_id = BTreeMap::new();

        for definition in definitions {
            let id = definition.stream_id.clone();

            definition
                .definition
                .validate()
                .map_err(|error| StreamGraphError::InvalidDefinition(id.clone(), error))?;

            if id.is_empty() || id.trim() != id || definition.revision <= 0 {
                return Err(StreamGraphError::InvalidDefinition(
                    id,
                    "invalid persistent identity or revision".into(),
                ));
            }

            match &definition.definition {
                StreamDefinition::Source(_) => {
                    if definition.recording_stream_key.is_none() {
                        return Err(StreamGraphError::InvalidDefinition(
                            id,
                            "source has no registered stream".into(),
                        ));
                    }
                }

                StreamDefinition::Derived(request) => {
                    if request.name != id || definition.recording_stream_key.is_some() {
                        return Err(StreamGraphError::InvalidDefinition(
                            id,
                            "derived stream identity is inconsistent".into(),
                        ));
                    }
                }
            }

            if by_id.insert(id.clone(), definition).is_some() {
                return Err(StreamGraphError::DuplicateStream(id));
            }
        }

        let mut positioned: BTreeMap<String, BTreeMap<i32, String>> = BTreeMap::new();

        let mut dependents: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        for edge in dependencies {
            let derived = by_id
                .get(&edge.derived_stream_id)
                .ok_or_else(|| StreamGraphError::UnknownStream(edge.derived_stream_id.clone()))?;

            if !matches!(derived.definition, StreamDefinition::Derived(_)) {
                return Err(StreamGraphError::InvalidDependency(format!(
                    "{} is not a derived stream",
                    edge.derived_stream_id
                )));
            }

            if !by_id.contains_key(&edge.input_stream_id) {
                return Err(StreamGraphError::UnknownStream(edge.input_stream_id));
            }

            if edge.input_position < 0 || edge.derived_stream_id == edge.input_stream_id {
                return Err(StreamGraphError::InvalidDependency(edge.derived_stream_id));
            }

            let positions = positioned
                .entry(edge.derived_stream_id.clone())
                .or_default();

            if positions
                .insert(edge.input_position, edge.input_stream_id.clone())
                .is_some()
            {
                return Err(StreamGraphError::InvalidDependency(format!(
                    "duplicate dependency position for {}",
                    edge.derived_stream_id
                )));
            }

            dependents
                .entry(edge.input_stream_id)
                .or_default()
                .insert(edge.derived_stream_id);
        }

        let mut ordered = BTreeMap::new();

        for (id, definition) in &by_id {
            match &definition.definition {
                StreamDefinition::Source(_) => {
                    if positioned.contains_key(id) {
                        return Err(StreamGraphError::InvalidDependency(format!(
                            "source stream {id} has input edges"
                        )));
                    }

                    ordered.insert(id.clone(), Vec::new());
                }

                StreamDefinition::Derived(request) => {
                    let positions = positioned.remove(id).unwrap_or_default();

                    let actual: Vec<String> = positions.values().cloned().collect();

                    let consecutive = positions
                        .keys()
                        .enumerate()
                        .all(|(index, position)| usize::try_from(*position).ok() == Some(index));

                    if !consecutive || actual != request.inputs {
                        return Err(StreamGraphError::InvalidDependency(format!(
                            "dependency rows disagree with \
                                     definition for {id}"
                        )));
                    }

                    ordered.insert(id.clone(), actual);
                }
            }
        }

        let graph = Self {
            definitions: by_id,
            dependencies: ordered,
            dependents,
        };

        graph.validate_acyclic()?;

        Ok(graph)
    }
    pub fn stream_ids(&self) -> impl Iterator<Item = &str> {
        self.definitions.keys().map(String::as_str)
    }
    pub fn get(&self, stream_id: &str) -> Option<&PersistedStreamDefinition> {
        self.definitions.get(stream_id)
    }

    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    pub fn inputs(&self, stream_id: &str) -> Option<&[String]> {
        self.dependencies.get(stream_id).map(Vec::as_slice)
    }

    pub fn dependents(&self, stream_id: &str) -> Option<&BTreeSet<String>> {
        self.definitions
            .contains_key(stream_id)
            .then(|| self.dependents.get(stream_id))
            .flatten()
    }

    /// Determine which streams must run.
    ///
    /// `ephemeral_demand` contains stream IDs required by
    /// current clients, manual sessions, or future in-process
    /// consumers.
    ///
    /// Always-on streams are additional demand roots.
    ///
    /// Every required derived stream recursively demands
    /// its inputs.
    ///
    /// Disabled requirements cause an explicit error.
    pub fn required_streams(
        &self,
        ephemeral_demand: &HashSet<String>,
    ) -> Result<BTreeSet<String>, StreamGraphError> {
        let mut roots: BTreeSet<String> = ephemeral_demand.iter().cloned().collect();

        for (id, definition) in &self.definitions {
            if definition.desired.enabled
                && definition.desired.activation_policy == ActivationPolicy::AlwaysOn
            {
                roots.insert(id.clone());
            }
        }

        let mut required = BTreeSet::new();

        for root in roots {
            self.add_required(&root, &mut required)?;
        }

        Ok(required)
    }

    fn add_required(
        &self,
        stream_id: &str,
        required: &mut BTreeSet<String>,
    ) -> Result<(), StreamGraphError> {
        let definition = self
            .definitions
            .get(stream_id)
            .ok_or_else(|| StreamGraphError::UnknownStream(stream_id.to_owned()))?;

        if !definition.desired.enabled {
            return Err(StreamGraphError::RequiredStreamDisabled(
                stream_id.to_owned(),
            ));
        }

        if !required.insert(stream_id.to_owned()) {
            return Ok(());
        }

        for input in &self.dependencies[stream_id] {
            self.add_required(input, required)?;
        }

        Ok(())
    }

    fn validate_acyclic(&self) -> Result<(), StreamGraphError> {
        let mut completed = HashSet::new();
        let mut visiting = HashSet::new();

        for stream_id in self.definitions.keys() {
            self.visit(stream_id, &mut visiting, &mut completed)?;
        }

        Ok(())
    }

    fn visit(
        &self,
        stream_id: &str,
        visiting: &mut HashSet<String>,
        completed: &mut HashSet<String>,
    ) -> Result<(), StreamGraphError> {
        if completed.contains(stream_id) {
            return Ok(());
        }

        if !visiting.insert(stream_id.to_owned()) {
            return Err(StreamGraphError::DependencyCycle(stream_id.to_owned()));
        }

        for input in &self.dependencies[stream_id] {
            self.visit(input, visiting, completed)?;
        }

        visiting.remove(stream_id);
        completed.insert(stream_id.to_owned());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::stream::{
        persistence::{
            ActivationPolicy, DesiredStreamState, PersistedStreamDefinition, StreamDefinition,
            StreamDependency,
        },
        request::{CreateDerivedStreamRequest, CreateSourceStreamRequest},
    };

    use serde_json::json;

    fn source(
        id: &str,
        enabled: bool,
        activation_policy: ActivationPolicy,
    ) -> PersistedStreamDefinition {
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
                activation_policy,
            },
            revision: 1,
        }
    }

    fn derived(
        id: &str,
        inputs: &[&str],
        activation_policy: ActivationPolicy,
    ) -> PersistedStreamDefinition {
        let request = CreateDerivedStreamRequest {
            name: id.into(),
            operation: crate::live::stream::request::DerivedOperation::Merge,
            inputs: inputs.iter().map(|id| id.to_string()).collect(),
            ordering: Some(crate::live::stream::request::MergeOrdering::ArrivalOrder),
            max_lateness_ns: None,
            target_quote_asset: None,
        };

        PersistedStreamDefinition {
            stream_id: id.into(),
            recording_stream_key: None,
            definition: StreamDefinition::Derived(request),
            desired: DesiredStreamState {
                enabled: true,
                activation_policy,
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
                source("binance", true, ActivationPolicy::OnDemand),
                source("bybit", true, ActivationPolicy::OnDemand),
                derived("merged", &["binance", "bybit"], ActivationPolicy::AlwaysOn),
            ],
            vec![edge("merged", "binance", 0), edge("merged", "bybit", 1)],
        )
        .unwrap()
    }

    #[test]
    fn restores_valid_dependency_graph() {
        let graph = graph();

        assert_eq!(graph.len(), 3);

        assert_eq!(graph.inputs("merged").unwrap(), ["binance", "bybit"]);
    }

    #[test]
    fn always_on_derived_demands_both_sources() {
        let required = graph().required_streams(&HashSet::new()).unwrap();

        assert_eq!(
            required,
            BTreeSet::from([
                "binance".to_owned(),
                "bybit".to_owned(),
                "merged".to_owned(),
            ])
        );
    }

    #[test]
    fn on_demand_source_without_consumers_remains_inactive() {
        let graph = StreamGraph::restore(
            vec![source("binance", true, ActivationPolicy::OnDemand)],
            vec![],
        )
        .unwrap();

        let required = graph.required_streams(&HashSet::new()).unwrap();

        assert!(required.is_empty());
    }

    #[test]
    fn ephemeral_demand_activates_source() {
        let graph = StreamGraph::restore(
            vec![source("binance", true, ActivationPolicy::OnDemand)],
            vec![],
        )
        .unwrap();

        let required = graph
            .required_streams(&HashSet::from(["binance".to_owned()]))
            .unwrap();

        assert!(required.contains("binance"));
    }

    #[test]
    fn disabled_required_source_is_rejected() {
        let graph = StreamGraph::restore(
            vec![
                source("binance", false, ActivationPolicy::OnDemand),
                source("bybit", true, ActivationPolicy::OnDemand),
                derived("merged", &["binance", "bybit"], ActivationPolicy::AlwaysOn),
            ],
            vec![edge("merged", "binance", 0), edge("merged", "bybit", 1)],
        )
        .unwrap();

        assert!(matches!(
            graph.required_streams(&HashSet::new()),
            Err(StreamGraphError::RequiredStreamDisabled(id))
                if id == "binance"
        ));
    }

    #[test]
    fn rejects_missing_dependency() {
        let result = StreamGraph::restore(
            vec![
                source("binance", true, ActivationPolicy::OnDemand),
                derived(
                    "merged",
                    &["binance", "missing"],
                    ActivationPolicy::OnDemand,
                ),
            ],
            vec![edge("merged", "binance", 0), edge("merged", "missing", 1)],
        );

        assert!(matches!(
            result,
            Err(StreamGraphError::UnknownStream(id))
                if id == "missing"
        ));
    }

    #[test]
    fn rejects_dependency_order_disagreement() {
        let result = StreamGraph::restore(
            vec![
                source("binance", true, ActivationPolicy::OnDemand),
                source("bybit", true, ActivationPolicy::OnDemand),
                derived("merged", &["binance", "bybit"], ActivationPolicy::OnDemand),
            ],
            vec![edge("merged", "bybit", 0), edge("merged", "binance", 1)],
        );

        assert!(matches!(
            result,
            Err(StreamGraphError::InvalidDependency(_))
        ));
    }

    #[test]
    fn rejects_dependency_cycle() {
        let result = StreamGraph::restore(
            vec![
                source("base", true, ActivationPolicy::OnDemand),
                derived("a", &["b", "base"], ActivationPolicy::OnDemand),
                derived("b", &["a", "base"], ActivationPolicy::OnDemand),
            ],
            vec![
                edge("a", "b", 0),
                edge("a", "base", 1),
                edge("b", "a", 0),
                edge("b", "base", 1),
            ],
        );

        assert!(matches!(result, Err(StreamGraphError::DependencyCycle(_))));
    }

    #[test]
    fn duplicate_stream_ids_are_rejected() {
        let result = StreamGraph::restore(
            vec![
                source("btc", true, ActivationPolicy::OnDemand),
                source("btc", true, ActivationPolicy::AlwaysOn),
            ],
            vec![],
        );

        assert!(matches!(
            result,
            Err(StreamGraphError::DuplicateStream(id))
                if id == "btc"
        ));
    }
}
