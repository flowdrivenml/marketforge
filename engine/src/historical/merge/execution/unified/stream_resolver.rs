use std::{collections::HashSet, path::Path};

use crate::{
    historical::job::{ContentType, load_committed_processing_job},
    shared::error::{MarketForgeError, Result},
};

use {
    crate::historical::merge::execution::shared::resolver::ResolvedMergeInput,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeStream,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeStreamKind,
};

// -----------------------------------------------------------------------------
// Resolve unified stream metadata
// -----------------------------------------------------------------------------

pub fn resolve_unified_merge_streams(
    inputs: &[ResolvedMergeInput],
    _project_root: &Path,
) -> Result<Vec<UnifiedMergeStream>> {
    let mut streams = Vec::with_capacity(inputs.len());
    let mut seen_datasets = HashSet::new();

    for (rank, input) in inputs.iter().enumerate() {
        if !seen_datasets.insert(input.dataset_id) {
            return invalid(format!(
                "duplicate unified input dataset {}",
                input.dataset_id
            ));
        }

        if input.manifest.content_type == ContentType::Depth {
            if input.canonical_files.len() != input.manifest.files.len() {
                return invalid(format!(
                    "depth publication {}: selected parts require validated reconstruction-state dependencies",
                    input.dataset_id
                ));
            }
            if input.manifest.protocol_version >= 4 {
                return invalid(format!(
                    "depth publication {}: merge cannot yet replay v{} segmented/native-order depth with causal ordering",
                    input.dataset_id, input.manifest.protocol_version
                ));
            }
        }
        if input.manifest.content_type == ContentType::Trades
            && input.manifest.protocol_version >= 4
            && (input
                .manifest
                .processing_quality
                .as_ref()
                .is_none_or(|quality| quality.timestamp_ordering != "event_timestamp_ns")
                || input.manifest.streams.iter().any(|stream| {
                    let mut tasks = HashSet::new();
                    stream
                        .sources
                        .iter()
                        .any(|source| !source.ranges.is_empty() || !tasks.insert(source.task_id))
                }))
        {
            return invalid(format!(
                "trade publication {}: merge cannot yet consume disjoint source ranges or native-order output",
                input.dataset_id
            ));
        }

        let job = load_committed_processing_job(&input.dataset_path)?;

        if job.dataset_id.0 != input.dataset_id {
            return invalid(format!(
                "dataset {}: processing job identity mismatch",
                input.dataset_id
            ));
        }

        if job.tasks.is_empty() {
            return invalid(format!(
                "dataset {}: processing job contains no tasks",
                input.dataset_id
            ));
        }

        // ---------------------------------------------------------------------
        // Validate source task identities
        // ---------------------------------------------------------------------

        let expected_tasks = input
            .manifest
            .source_tasks
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        let actual_tasks = job
            .tasks
            .iter()
            .map(|task| task.task_id.0)
            .collect::<HashSet<_>>();

        if expected_tasks != actual_tasks {
            return invalid(format!(
                "dataset {}: processing job task mismatch",
                input.dataset_id
            ));
        }

        // ---------------------------------------------------------------------
        // Validate consistent instrument metadata
        // ---------------------------------------------------------------------

        let selected_stream = if input.manifest.content_type == ContentType::Trades
            && !input.manifest.streams.is_empty()
            && !input.canonical_files.is_empty()
        {
            let matching = input.selected_streams()?;
            if matching.len() != 1 {
                return invalid(format!(
                    "publication {}: select files from a single concrete trade stream; multiple stream ranks per publication are unsupported",
                    input.dataset_id
                ));
            }
            Some(matching[0])
        } else {
            None
        };

        let mut tasks = job.tasks.iter().filter(|task| {
            selected_stream.is_none_or(|stream| {
                stream
                    .sources
                    .iter()
                    .any(|source| source.task_id == task.task_id.0)
            })
        });
        let first = tasks
            .next()
            .ok_or_else(|| invalid_error("selected stream has no source tasks"))?;
        if tasks.any(|task| {
            task.stream_id != first.stream_id
                || task.exchange != first.exchange
                || task.instrument_id != first.instrument_id
                || task.symbol != first.symbol
                || task.instrument != first.instrument
        }) {
            return invalid(format!(
                "publication {}: incompatible selected task metadata",
                input.dataset_id
            ));
        }

        if let Some(stream) = selected_stream {
            if stream.stream_id != first.stream_id.0 {
                return invalid("selected manifest stream differs from processing metadata");
            }
        } else if job.streams.len() != 1 || job.streams[0].stream_id != first.stream_id {
            return invalid(format!(
                "publication {}: expected exactly one compatible processing stream",
                input.dataset_id
            ));
        }

        // ---------------------------------------------------------------------
        // Determine stream kind
        // ---------------------------------------------------------------------

        let kind = match input.manifest.content_type {
            ContentType::Trades => UnifiedMergeStreamKind::Trade,
            ContentType::Depth => UnifiedMergeStreamKind::Depth,

            ContentType::Combined => {
                return invalid(format!(
                    "dataset {}: combined canonical datasets are not yet supported as unified merge inputs",
                    input.dataset_id
                ));
            }
        };

        streams.push(UnifiedMergeStream {
            stream_rank: u32::try_from(rank)
                .map_err(|_| invalid_error("unified stream rank overflow"))?,

            dataset_id: input.dataset_id,
            kind,

            exchange: format!("{:?}", first.exchange).to_lowercase(),
            instrument_id: first.instrument_id,
            symbol: first.symbol.clone(),
            stream_id: first.stream_id.0.clone(),

            // Currency metadata is not available in InstrumentSpec.
            base_currency: None,
            quote_currency: None,
        });
    }

    Ok(streams)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
