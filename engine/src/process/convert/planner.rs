use std::collections::HashSet;

use crate::{
    error::{MarketForgeError, Result},
    instrument::Currency,
    job::ConvertJob,
};

use super::{
    catalog::{ResolvedConversionCatalog, ResolvedConversionInstrument},
    graph::{ConversionGraph, ConversionPath},
};

pub const DEFAULT_MAX_PATHS: usize = 32;

#[derive(Debug, Clone)]
pub struct PlannedConversionStream {
    pub source: ResolvedConversionInstrument,
    pub paths: Vec<ConversionPath>,
    pub identity_conversion: bool,
}

#[derive(Debug, Clone)]
pub struct ConversionPlan {
    pub target_quote: Currency,
    pub streams: Vec<PlannedConversionStream>,

    pub source_streams: usize,
    pub reference_streams: usize,
    pub convertible_streams: usize,
    pub identity_streams: usize,
    pub unresolved_streams: usize,
}

pub fn plan_conversion(
    job: &ConvertJob,
    resolved: &ResolvedConversionCatalog,
) -> Result<ConversionPlan> {
    job.validate()?;

    let target = Currency::new(&job.conversion.target_quote)?;

    let graph = ConversionGraph::new(&resolved.references)?;

    let mut streams = Vec::with_capacity(resolved.sources.len());

    let mut convertible = 0;
    let mut identity = 0;
    let mut unresolved = 0;

    let mut seen_streams = HashSet::new();

    for source in &resolved.sources {
        if !seen_streams.insert(source.stream_rank) {
            return invalid(format!(
                "duplicate conversion source stream rank: {}",
                source.stream_rank
            ));
        }

        let is_identity = source.quote_currency == target;

        let paths = graph.find_paths(&source.quote_currency, &target, job.conversion.max_hops)?;

        if paths.len() > DEFAULT_MAX_PATHS {
            return invalid(format!(
                "conversion path limit exceeded for {}: {} paths (maximum {})",
                source.stream_id,
                paths.len(),
                DEFAULT_MAX_PATHS
            ));
        }

        if is_identity {
            identity += 1;
        } else if paths.is_empty() {
            unresolved += 1;
        } else {
            convertible += 1;
        }

        streams.push(PlannedConversionStream {
            source: source.clone(),
            paths,
            identity_conversion: is_identity,
        });
    }

    Ok(ConversionPlan {
        target_quote: target,
        source_streams: streams.len(),
        reference_streams: resolved.references.len(),
        convertible_streams: convertible,
        identity_streams: identity,
        unresolved_streams: unresolved,
        streams,
    })
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
