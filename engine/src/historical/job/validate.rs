use std::collections::HashSet;

use super::{ContentType, PROCESSING_PROTOCOL_VERSION, ProcessingJob, TargetSchema};
use crate::shared::error::{MarketForgeError, Result};

// -----------------------------------------------------------------------------
// Public validation
// -----------------------------------------------------------------------------

pub fn validate_processing_job(job: &ProcessingJob) -> Result<()> {
    validate_processing_job_inner(job, true)
}

pub fn validate_processing_job_structure(job: &ProcessingJob) -> Result<()> {
    validate_processing_job_inner(job, false)
}

// -----------------------------------------------------------------------------
// Processing job validation
// -----------------------------------------------------------------------------

fn validate_processing_job_inner(
    job: &ProcessingJob,
    validate_legacy_resources: bool,
) -> Result<()> {
    // -------------------------------------------------------------------------
    // Protocol version
    // -------------------------------------------------------------------------

    if job.protocol_version != PROCESSING_PROTOCOL_VERSION {
        return invalid(format!(
            "unsupported processing protocol version: expected {}, received {}",
            PROCESSING_PROTOCOL_VERSION, job.protocol_version,
        ));
    }

    // -------------------------------------------------------------------------
    // Tasks
    // -------------------------------------------------------------------------

    if job.tasks.is_empty() {
        return invalid("processing job contains no tasks");
    }

    // -------------------------------------------------------------------------
    // Streams
    // -------------------------------------------------------------------------

    if job.streams.is_empty() {
        return invalid("processing job contains no streams");
    }

    // -------------------------------------------------------------------------
    // Legacy resource validation
    // -------------------------------------------------------------------------
    //
    // Processing jobs retain their original resource fields.
    //
    // The production executor uses the authoritative global configuration:
    //
    // data/.jobs/processing.json
    //
    // This validation remains for compatibility with existing callers.
    // -------------------------------------------------------------------------

    if validate_legacy_resources {
        let resources = &job.resources;

        if resources.workers == 0 {
            return invalid("worker count must be greater than zero");
        }

        if resources.memory_budget_bytes == 0 {
            return invalid("memory budget must be greater than zero");
        }

        if resources.scratch_budget_bytes == 0 {
            return invalid("scratch budget must be greater than zero");
        }

        if resources.parquet.row_group_target_bytes == 0 {
            return invalid("Parquet row-group target must be greater than zero");
        }

        if resources.parquet.file_target_bytes == 0 {
            return invalid("Parquet file target must be greater than zero");
        }

        if resources.parquet.row_group_target_bytes > resources.parquet.file_target_bytes {
            return invalid("Parquet row-group target cannot exceed file target");
        }
    }

    // -------------------------------------------------------------------------
    // Output validation
    // -------------------------------------------------------------------------

    if job.output.staging_path == job.output.dataset_path {
        return invalid("staging path and final dataset path must be different");
    }

    // -------------------------------------------------------------------------
    // Task validation
    // -------------------------------------------------------------------------

    let mut task_ids = HashSet::new();

    let expected_target = match job.output.content_type {
        ContentType::Trades => TargetSchema::Trade,
        ContentType::Depth => TargetSchema::Depth,

        ContentType::Combined => {
            return invalid("combined output is not supported for raw processing jobs");
        }
    };

    for task in &job.tasks {
        // Reject duplicate task IDs.

        if !task_ids.insert(task.task_id) {
            return invalid(format!("duplicate task ID: {}", task.task_id.0,));
        }

        // Every task requires at least one normalization.

        if task.normalizations.is_empty() {
            return invalid(format!(
                "task {} requires at least one normalization",
                task.task_id.0,
            ));
        }

        // All normalizations must match the declared output type.

        if task
            .normalizations
            .iter()
            .any(|normalization| normalization.target_schema != expected_target)
        {
            return invalid(format!(
                "task {} has normalization targets incompatible with {:?}",
                task.task_id.0, job.output.content_type,
            ));
        }

        // Trades require exactly one normalization.
        // Depth may contain separate snapshot/update normalizations.

        if expected_target == TargetSchema::Trade && task.normalizations.len() != 1 {
            return invalid(format!(
                "trade task {} requires exactly one normalization",
                task.task_id.0,
            ));
        }
    }

    // -------------------------------------------------------------------------
    // Stream validation
    // -------------------------------------------------------------------------

    let mut stream_ids = HashSet::new();
    let mut stream_ranks = HashSet::new();

    for stream in &job.streams {
        if !stream_ids.insert(&stream.stream_id) {
            return invalid(format!("duplicate stream ID: {}", stream.stream_id.0,));
        }

        if !stream_ranks.insert(stream.stream_rank) {
            return invalid(format!("duplicate stream rank: {}", stream.stream_rank,));
        }
    }

    // -------------------------------------------------------------------------
    // Task references and instrument validation
    // -------------------------------------------------------------------------

    for task in &job.tasks {
        if !stream_ids.contains(&task.stream_id) {
            return invalid(format!(
                "task {} references unknown stream: {}",
                task.task_id.0, task.stream_id.0,
            ));
        }

        if task.instrument.tick_size <= rust_decimal::Decimal::ZERO {
            return invalid(format!(
                "task {} has invalid tick size: {}",
                task.task_id.0, task.instrument.tick_size,
            ));
        }

        if task.instrument.contract_kind.is_none() && task.instrument.contract_value.is_some() {
            return invalid(format!(
                "task {} defines contract value without contract kind",
                task.task_id.0,
            ));
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
