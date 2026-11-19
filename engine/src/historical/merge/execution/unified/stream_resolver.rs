use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

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
    project_root: &Path,
) -> Result<Vec<UnifiedMergeStream>> {
    resolve_ranked_merge_streams(inputs, project_root, None)
}

pub fn resolve_ranked_merge_streams(
    inputs: &[ResolvedMergeInput],
    _project_root: &Path,
    submission: Option<&crate::historical::job::MergeJobDocument>,
) -> Result<Vec<UnifiedMergeStream>> {
    let mut streams = Vec::with_capacity(inputs.len());
    let mut seen_datasets = HashSet::new();

    let mut identities = HashMap::new();
    let mut ranks = HashMap::new();
    for (rank, input) in inputs.iter().enumerate() {
        if !seen_datasets.insert(input.dataset_id) {
            return invalid(format!(
                "duplicate unified input dataset {}",
                input.dataset_id
            ));
        }

        let declarations = submission
            .into_iter()
            .flat_map(|job| &job.inputs)
            .filter(|file| file.publication.engine_dataset_id == input.dataset_id)
            .map(|file| (file.path.as_path(), file))
            .collect::<HashMap<_, _>>();
        let job = load_committed_processing_job(&input.dataset_path)?;

        if job.output.content_type != input.manifest.content_type {
            return invalid(format!(
                "publication {}: processing content type differs from manifest",
                input.dataset_id
            ));
        }

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

        if expected_tasks.len() != input.manifest.source_tasks.len()
            || expected_tasks != actual_tasks
        {
            return invalid(format!(
                "dataset {}: processing job task mismatch",
                input.dataset_id
            ));
        }

        // ---------------------------------------------------------------------
        // Validate consistent instrument metadata
        // ---------------------------------------------------------------------

        let matching = input.selected_streams()?;
        let selected = if matching.is_empty() {
            vec![None]
        } else {
            matching.into_iter().map(Some).collect()
        };
        if input.manifest.content_type == ContentType::Depth
            && input.manifest.protocol_version < 4
            && selected.len() != 1
        {
            return invalid(format!(
                "depth publication {}: replay requires one concrete stream per publication",
                input.dataset_id
            ));
        }
        for selected_stream in selected {
            if input.manifest.content_type == ContentType::Trades
                && input.manifest.protocol_version >= 4
                && (input
                    .manifest
                    .processing_quality
                    .as_ref()
                    .is_none_or(|quality| quality.timestamp_ordering != "event_timestamp_ns")
                    || selected_stream.is_some_and(|stream| {
                        let mut tasks = HashSet::new();
                        stream.sources.iter().any(|source| {
                            !source.ranges.is_empty() || !tasks.insert(source.task_id)
                        })
                    }))
            {
                return invalid(format!(
                    "trade publication {}: merge cannot yet consume disjoint source ranges or native-order output",
                    input.dataset_id
                ));
            }

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

            let mut rank =
                u32::try_from(rank).map_err(|_| invalid_error("unified stream rank overflow"))?;
            if submission.is_some() {
                let mut assigned = None;
                for file in input.canonical_files.iter().filter(|path| {
                    selected_stream.is_none_or(|stream| {
                        path.parent() == Some(input.dataset_path.join(&stream.directory).as_path())
                    })
                }) {
                    let declared = declarations.get(file.as_path()).ok_or_else(|| {
                        invalid_error(format!(
                            "selected file has no stream declaration: {}",
                            file.display()
                        ))
                    })?;
                    if declared.stream_id != first.stream_id.0 {
                        return invalid(format!(
                            "registered file {}: declared stream_id does not match committed stream {}",
                            declared.dataset_id, first.stream_id.0
                        ));
                    }
                    if declared.content_type != input.manifest.content_type {
                        return invalid(format!(
                            "registered file {}: declared content_type does not match committed content type",
                            declared.dataset_id
                        ));
                    }
                    if assigned
                        .replace(declared.stream_rank)
                        .is_some_and(|previous| previous != declared.stream_rank)
                    {
                        return invalid(format!(
                            "conflicting ranks for stream {}",
                            first.stream_id.0
                        ));
                    }
                }
                rank = assigned
                    .ok_or_else(|| invalid_error("selected stream has no declared files"))?;
                let namespace = input.manifest.instrument_namespace.as_deref();
                if let Some(namespace) = namespace
                    && job.tasks.iter().any(|task| {
                        task.raw_schema
                            .pointer("/marketforge_source/instrument_namespace")
                            .and_then(serde_json::Value::as_str)
                            != Some(namespace)
                    })
                {
                    return invalid(format!(
                        "publication {}: committed processing metadata has a different instrument namespace",
                        input.dataset_id
                    ));
                }
                let identity = (
                    namespace,
                    namespace.is_none().then_some(input.dataset_id),
                    first.stream_id.0.clone(),
                );
                let metadata = (
                    rank,
                    kind,
                    first.exchange,
                    first.instrument_id,
                    first.symbol.clone(),
                    first.instrument.clone(),
                );
                if identities
                    .insert(identity.clone(), metadata.clone())
                    .is_some_and(|previous| previous != metadata)
                {
                    return invalid(format!(
                        "conflicting declarations or metadata for stream {}",
                        first.stream_id.0
                    ));
                }
                if ranks
                    .insert(rank, identity.clone())
                    .is_some_and(|previous| previous != identity)
                {
                    return invalid(format!(
                        "stream rank {rank} is assigned to distinct streams"
                    ));
                }
            } else if streams
                .iter()
                .any(|previous: &UnifiedMergeStream| previous.stream_rank == rank)
            {
                return invalid(
                    "multiple streams in a publication require an explicit file-based merge job",
                );
            }
            if kind == UnifiedMergeStreamKind::Depth
                && streams.iter().any(|previous| previous.stream_rank == rank)
            {
                return invalid(
                    "depth stream spans publications without validated cross-publication reconstruction",
                );
            }
            if kind == UnifiedMergeStreamKind::Depth {
                if input.canonical_files.len() != input.manifest.files.len() {
                    return invalid(format!(
                        "depth publication {}: selected parts require validated reconstruction-state dependencies",
                        input.dataset_id
                    ));
                }
                if input.manifest.protocol_version >= 4 {
                    return invalid(format!(
                        "depth publication {}: merge cannot yet map v{} segmented depth to a timestamp-ordered timeline while preserving causal replay",
                        input.dataset_id, input.manifest.protocol_version
                    ));
                }
            }
            streams.push(UnifiedMergeStream {
                stream_rank: rank,

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
    }

    streams.sort_by_key(|stream| (stream.stream_rank, stream.dataset_id));
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
