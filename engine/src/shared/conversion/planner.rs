use std::collections::HashSet;

use crate::{
    shared::domain::instrument::Currency,
    shared::error::{MarketForgeError, Result},
};

use super::graph::{ConversionGraph, ConversionPath};
use crate::shared::domain::instrument::InstrumentIdentity;

#[derive(Debug, Clone)]
pub struct ResolvedConversionInstrument {
    pub dataset_id: u64,
    pub stream_rank: u32,
    pub stream_id: String,

    pub identity: InstrumentIdentity,

    pub base_currency: Currency,
    pub quote_currency: Currency,
}

pub const DEFAULT_MAX_PATHS: usize = 32;

#[derive(Debug, Clone)]
pub struct PlannedConversionStream {
    pub source: ResolvedConversionInstrument,
    pub paths: Vec<ConversionPath>,
    pub identity_conversion: bool,
    pub selected_for_conversion: bool,
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
    pub skipped_streams: usize,
}

pub fn plan_conversion(
    target: Currency,
    sources: &[ResolvedConversionInstrument],
    references: &[ResolvedConversionInstrument],
    selected: impl Fn(&ResolvedConversionInstrument) -> bool,
    max_hops: usize,
) -> Result<ConversionPlan> {
    let graph = ConversionGraph::new(references)?;

    let mut streams = Vec::with_capacity(sources.len());

    let mut convertible = 0;
    let mut identity = 0;
    let mut unresolved = 0;
    let mut skipped = 0;

    let mut seen_streams = HashSet::new();

    for source in sources {
        if !seen_streams.insert(source.stream_rank) {
            return invalid(format!(
                "duplicate conversion source stream rank: {}",
                source.stream_rank
            ));
        }

        let selected_for_conversion = selected(source);

        let is_identity = selected_for_conversion && source.quote_currency == target;

        let paths = if selected_for_conversion {
            graph.find_paths(&source.quote_currency, &target, max_hops)?
        } else {
            Vec::new()
        };

        if paths.len() > DEFAULT_MAX_PATHS {
            return invalid(format!(
                "conversion path limit exceeded for {}: {} paths (maximum {})",
                source.stream_id,
                paths.len(),
                DEFAULT_MAX_PATHS
            ));
        }

        if !selected_for_conversion {
            skipped += 1;
        } else if is_identity {
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
            selected_for_conversion,
        });
    }

    if convertible + identity + unresolved + skipped != streams.len() {
        return invalid("conversion planner stream accounting mismatch");
    }

    Ok(ConversionPlan {
        target_quote: target,
        source_streams: streams.len(),
        reference_streams: references.len(),
        convertible_streams: convertible,
        identity_streams: identity,
        unresolved_streams: unresolved,
        streams,
        skipped_streams: skipped,
    })
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
