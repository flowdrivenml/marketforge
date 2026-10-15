use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use crate::{
    error::{MarketForgeError, Result},
    job::{ContentType, load_processing_job},
};

use super::{
    resolver::ResolvedMergeInput,
    unified_manifest::{UnifiedMergeStream, UnifiedMergeStreamKind},
};

// -----------------------------------------------------------------------------
// Resolve unified stream metadata
// -----------------------------------------------------------------------------

pub fn resolve_unified_merge_streams(
    inputs: &[ResolvedMergeInput],
    project_root: &Path,
) -> Result<Vec<UnifiedMergeStream>> {
    let jobs_directory = project_root.join("data/.jobs/process");

    let job_paths = discover_processing_jobs(&jobs_directory)?;

    let mut streams = Vec::with_capacity(inputs.len());
    let mut seen_datasets = HashSet::new();

    for (rank, input) in inputs.iter().enumerate() {
        if !seen_datasets.insert(input.dataset_id) {
            return invalid(format!(
                "duplicate unified input dataset {}",
                input.dataset_id
            ));
        }

        let job_path = job_paths.get(&input.dataset_id).ok_or_else(|| {
            invalid_error(format!(
                "dataset {}: processing job not found",
                input.dataset_id
            ))
        })?;

        let job = load_processing_job(job_path)?;

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

        let first = &job.tasks[0];

        for task in &job.tasks[1..] {
            if task.stream_id != first.stream_id
                || task.exchange != first.exchange
                || task.instrument_id != first.instrument_id
                || task.symbol != first.symbol
                || task.instrument != first.instrument
            {
                return invalid(format!(
                    "dataset {}: inconsistent instrument metadata across tasks",
                    input.dataset_id
                ));
            }
        }

        // ---------------------------------------------------------------------
        // Validate stream configuration
        // ---------------------------------------------------------------------

        if job.streams.len() != 1 {
            return invalid(format!(
                "dataset {}: expected exactly one processing stream",
                input.dataset_id
            ));
        }

        if job.streams[0].stream_id != first.stream_id {
            return invalid(format!(
                "dataset {}: processing stream identity mismatch",
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
