use std::collections::{HashMap, HashSet};

use crate::{
    error::{MarketForgeError, Result},
    instrument::Currency,
};

use super::catalog::ResolvedConversionInstrument;

// -----------------------------------------------------------------------------
// Graph types
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConversionDirection {
    Direct,
    Inverse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionEdge {
    pub from: Currency,
    pub to: Currency,

    pub reference_dataset_id: u64,
    pub reference_stream_rank: u32,
    pub reference_stream_id: String,

    pub direction: ConversionDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionPath {
    pub currencies: Vec<Currency>,
    pub edges: Vec<ConversionEdge>,
}

impl ConversionPath {
    pub fn hops(&self) -> usize {
        self.edges.len()
    }
}

#[derive(Debug, Clone)]
pub struct ConversionGraph {
    adjacency: HashMap<Currency, Vec<ConversionEdge>>,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl ConversionGraph {
    pub fn new(references: &[ResolvedConversionInstrument]) -> Result<Self> {
        let mut adjacency: HashMap<Currency, Vec<ConversionEdge>> = HashMap::new();

        let mut seen_streams = HashSet::new();

        for reference in references {
            if !seen_streams.insert(reference.stream_id.clone()) {
                return invalid(format!(
                    "duplicate conversion reference stream: {}",
                    reference.stream_id
                ));
            }

            let base = reference.base_currency.clone();
            let quote = reference.quote_currency.clone();

            if base == quote {
                return invalid("conversion reference has identical currencies");
            }

            adjacency
                .entry(base.clone())
                .or_default()
                .push(ConversionEdge {
                    from: base.clone(),
                    to: quote.clone(),
                    reference_dataset_id: reference.dataset_id,
                    reference_stream_rank: reference.stream_rank,
                    reference_stream_id: reference.stream_id.clone(),
                    direction: ConversionDirection::Direct,
                });

            adjacency
                .entry(quote.clone())
                .or_default()
                .push(ConversionEdge {
                    from: quote,
                    to: base,
                    reference_dataset_id: reference.dataset_id,
                    reference_stream_rank: reference.stream_rank,
                    reference_stream_id: reference.stream_id.clone(),
                    direction: ConversionDirection::Inverse,
                });
        }

        // Deterministic edge ordering.
        for edges in adjacency.values_mut() {
            edges.sort_by(|a, b| {
                (
                    a.to.as_str(),
                    &a.reference_stream_id,
                    a.reference_dataset_id,
                )
                    .cmp(&(
                        b.to.as_str(),
                        &b.reference_stream_id,
                        b.reference_dataset_id,
                    ))
            });
        }

        Ok(Self { adjacency })
    }

    // -------------------------------------------------------------------------
    // Path enumeration
    // -------------------------------------------------------------------------

    pub fn find_paths(
        &self,
        source: &Currency,
        target: &Currency,
        max_hops: usize,
    ) -> Result<Vec<ConversionPath>> {
        if max_hops == 0 {
            return invalid("conversion max_hops must be positive");
        }

        if source == target {
            return Ok(vec![ConversionPath {
                currencies: vec![source.clone()],
                edges: Vec::new(),
            }]);
        }

        let mut results = Vec::new();

        let mut currencies = vec![source.clone()];
        let mut edges = Vec::new();
        let mut visited = HashSet::new();

        visited.insert(source.clone());

        self.visit(
            source,
            target,
            max_hops,
            &mut visited,
            &mut currencies,
            &mut edges,
            &mut results,
        );

        Ok(results)
    }

    #[allow(clippy::too_many_arguments)]
    fn visit(
        &self,
        current: &Currency,
        target: &Currency,
        max_hops: usize,
        visited: &mut HashSet<Currency>,
        currencies: &mut Vec<Currency>,
        edges: &mut Vec<ConversionEdge>,
        results: &mut Vec<ConversionPath>,
    ) {
        if edges.len() >= max_hops {
            return;
        }

        let Some(neighbors) = self.adjacency.get(current) else {
            return;
        };

        for edge in neighbors {
            if visited.contains(&edge.to) {
                continue;
            }

            visited.insert(edge.to.clone());
            currencies.push(edge.to.clone());
            edges.push(edge.clone());

            if &edge.to == target {
                results.push(ConversionPath {
                    currencies: currencies.clone(),
                    edges: edges.clone(),
                });
            } else {
                self.visit(
                    &edge.to, target, max_hops, visited, currencies, edges, results,
                );
            }

            edges.pop();
            currencies.pop();
            visited.remove(&edge.to);
        }
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
