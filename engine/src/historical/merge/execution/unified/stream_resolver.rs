use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use crate::{
    historical::job::{ContentType, load_processing_job},
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
    let jobs_directory = project_root.join("data/.jobs/process");

    let job_paths = if inputs
        .iter()
        .any(|i| !i.dataset_path.join("processing-job.json").is_file())
    {
        discover_processing_jobs(&jobs_directory)?
    } else {
        HashMap::new()
    };

    let mut streams = Vec::with_capacity(inputs.len());
    let mut seen_datasets = HashSet::new();

    for (rank, input) in inputs.iter().enumerate() {
        if !seen_datasets.insert(input.dataset_id) {
            return invalid(format!(
                "duplicate unified input dataset {}",
                input.dataset_id
            ));
        }

        let snapshot = input.dataset_path.join("processing-job.json");
        let job = if snapshot.is_file() {
            crate::historical::job::load_committed_processing_job(&input.dataset_path)?
        } else {
            let job_path = job_paths.get(&input.dataset_id).ok_or_else(|| {
                invalid_error(format!(
                    "dataset {}: processing job not found",
                    input.dataset_id
                ))
            })?;
            load_processing_job(job_path)?
        };

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
            let matching = input
                .manifest
                .streams
                .iter()
                .filter(|stream| {
                    let directory = input.dataset_path.join(&stream.directory);
                    input
                        .canonical_files
                        .iter()
                        .any(|file| file.parent() == Some(directory.as_path()))
                })
                .collect::<Vec<_>>();
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
// Discover processing jobs
// -----------------------------------------------------------------------------

fn discover_processing_jobs(directory: &Path) -> Result<HashMap<u64, PathBuf>> {
    let mut jobs = HashMap::new();

    for entry in fs::read_dir(directory).map_err(|error| {
        invalid_error(format!(
            "failed to read processing jobs {}: {error}",
            directory.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            invalid_error(format!("failed to read processing job entry: {error}"))
        })?;

        let path = entry.path();

        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }

        // Processing job filenames use the suffix -d<ID>.json.
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid_error("invalid processing job filename"))?;

        let Some(stem) = filename.strip_suffix(".json") else {
            continue;
        };

        let Some((_, suffix)) = stem.rsplit_once("-d") else {
            continue;
        };

        let Ok(dataset_id) = suffix.parse::<u64>() else {
            continue;
        };

        if jobs.insert(dataset_id, path.clone()).is_some() {
            return invalid(format!("duplicate processing job for dataset {dataset_id}"));
        }
    }

    Ok(jobs)
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
