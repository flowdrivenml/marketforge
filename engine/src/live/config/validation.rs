//! Pure bounded dependency validation. This is not economic/capability approval.
use crate::live::model::{
    configuration::*,
    derived::{DerivedTransformation, StreamRef},
    error::ErrorCode,
};
use std::collections::{HashMap, VecDeque};
fn issue(path: String, code: ErrorCode) -> ConfigIssue {
    ConfigIssue {
        path,
        code,
        message: code.to_string(),
    }
}
/// Stable dependency-first order over source and derived identities. Lifecycle
/// enabled flags do not remove dependency demand or make missing nodes valid.
pub fn dependency_order(
    config: &DesiredConfiguration,
    max_items: usize,
    max_edges: usize,
) -> Result<Vec<StreamRef>, ConfigIssue> {
    config
        .validate_structure(max_items)
        .map_err(|e| issue("configuration".into(), e))?;
    let mut nodes: Vec<StreamRef> = config
        .sources
        .iter()
        .map(|s| StreamRef::Source(s.id.clone()))
        .collect();
    nodes.extend(
        config
            .derived
            .iter()
            .map(|d| StreamRef::Derived(d.id.clone())),
    );
    let index: HashMap<_, _> = nodes
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, n)| (n, i))
        .collect();
    let mut edges = vec![Vec::new(); nodes.len()];
    let mut incoming = vec![0usize; nodes.len()];
    let mut total = 0usize;
    for (i, derived) in config.derived.iter().enumerate() {
        let dependencies: Vec<_> = match &derived.transformation {
            DerivedTransformation::Merge { sources, .. } => sources.iter().collect(),
            DerivedTransformation::Conversion {
                source, references, ..
            } => std::iter::once(source).chain(references).collect(),
        };
        let target = config.sources.len() + i;
        let mut seen = std::collections::HashSet::new();
        for dependency in dependencies {
            if !seen.insert(dependency) {
                if matches!(derived.transformation, DerivedTransformation::Merge { .. }) {
                    return Err(issue(
                        format!("derived[{i}].sources"),
                        ErrorCode::InvalidInput,
                    ));
                }
                continue;
            }
            total = total
                .checked_add(1)
                .ok_or_else(|| issue("derived".into(), ErrorCode::Limit))?;
            if total > max_edges {
                return Err(issue("derived".into(), ErrorCode::Limit));
            }
            let from = *index
                .get(dependency)
                .ok_or_else(|| issue(format!("derived[{i}].dependencies"), ErrorCode::NotFound))?;
            edges[from].push(target);
            incoming[target] += 1;
        }
    }
    let mut ready: VecDeque<_> = incoming
        .iter()
        .enumerate()
        .filter_map(|(i, n)| (*n == 0).then_some(i))
        .collect();
    let mut order = Vec::with_capacity(nodes.len());
    while let Some(from) = ready.pop_front() {
        order.push(nodes[from].clone());
        for target in &edges[from] {
            incoming[*target] -= 1;
            if incoming[*target] == 0 {
                ready.push_back(*target);
            }
        }
    }
    if order.len() != nodes.len() {
        return Err(issue("derived".into(), ErrorCode::Dependency));
    }
    Ok(order)
}
