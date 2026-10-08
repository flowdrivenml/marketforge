use std::collections::HashSet;

use crate::error::{MarketForgeError, Result};

use super::{PROCESSING_PROTOCOL_VERSION, ProcessingJob, ProcessingOperation};

pub fn validate_processing_job(job: &ProcessingJob) -> Result<()> {
    validate_processing_job_inner(job, true)
}

pub fn validate_processing_job_structure(job: &ProcessingJob) -> Result<()> {
    validate_processing_job_inner(job, false)
}

fn validate_processing_job_inner(
    job: &ProcessingJob,
    validate_legacy_resources: bool,
) -> Result<()> {
    if job.protocol_version != PROCESSING_PROTOCOL_VERSION {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "unsupported processing protocol version: expected {}, received {}",
            PROCESSING_PROTOCOL_VERSION, job.protocol_version,
        )));
    }

    match job.operation {
        ProcessingOperation::Process => {
            if job.tasks.is_empty() {
                return Err(MarketForgeError::InvalidConfiguration(
                    "process job contains no tasks".to_owned(),
                ));
            }

            if !job.merge_inputs.is_empty() {
                return Err(MarketForgeError::InvalidConfiguration(
                    "process job contains merge inputs".to_owned(),
                ));
            }

            if job.time_range.is_some() {
                return Err(MarketForgeError::InvalidConfiguration(
                    "process job contains merge time range".to_owned(),
                ));
            }
        }

        ProcessingOperation::Merge => {
            if !job.tasks.is_empty() {
                return Err(MarketForgeError::InvalidConfiguration(
                    "merge job contains raw processing tasks".to_owned(),
                ));
            }

            if job.merge_inputs.is_empty() {
                return Err(MarketForgeError::InvalidConfiguration(
                    "merge job contains no inputs".to_owned(),
                ));
            }

            if job.time_range.is_none() {
                return Err(MarketForgeError::InvalidConfiguration(
                    "merge job contains no time range".to_owned(),
                ));
            }
        }
    }

    if job.streams.is_empty() {
        return Err(MarketForgeError::InvalidConfiguration(
            "processing job contains no streams".to_owned(),
        ));
    }

    // -----------------------------------------------------------------
    // Legacy manifest resource validation
    // -----------------------------------------------------------------
    //
    // Processing jobs retain their original resource fields.
    //
    // The production executor ignores these values and instead uses
    // the authoritative global configuration:
    //
    // data/.jobs/processing.json
    //
    // This validation block is retained for compatibility with
    // existing callers of validate_processing_job().
    // -----------------------------------------------------------------

    if validate_legacy_resources {
        if job.resources.workers == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "worker count must be greater than zero".to_owned(),
            ));
        }

        if job.resources.memory_budget_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "memory budget must be greater than zero".to_owned(),
            ));
        }

        if job.resources.scratch_budget_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "scratch budget must be greater than zero".to_owned(),
            ));
        }

        if job.resources.parquet.row_group_target_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet row-group target must be greater than zero".to_owned(),
            ));
        }

        if job.resources.parquet.file_target_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet file target must be greater than zero".to_owned(),
            ));
        }

        if job.resources.parquet.row_group_target_bytes > job.resources.parquet.file_target_bytes {
            return Err(MarketForgeError::InvalidConfiguration(
                "Parquet row-group target cannot exceed file target".to_owned(),
            ));
        }
    }

    // -----------------------------------------------------------------
    // Output validation
    // -----------------------------------------------------------------

    if job.output.staging_path == job.output.dataset_path {
        return Err(MarketForgeError::InvalidConfiguration(
            "staging path and final dataset path must be different".to_owned(),
        ));
    }

    // -----------------------------------------------------------------
    // Task validation
    // -----------------------------------------------------------------

    let mut task_ids = HashSet::new();

    for task in &job.tasks {
        if !task_ids.insert(task.task_id) {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "duplicate task ID: {}",
                task.task_id.0
            )));
        }
    }

    // -----------------------------------------------------------------
    // Stream validation
    // -----------------------------------------------------------------

    let mut stream_ids = HashSet::new();
    let mut stream_ranks = HashSet::new();

    for stream in &job.streams {
        if !stream_ids.insert(&stream.stream_id) {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "duplicate stream ID: {}",
                stream.stream_id.0,
            )));
        }

        if !stream_ranks.insert(stream.stream_rank) {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "duplicate stream rank: {}",
                stream.stream_rank,
            )));
        }
    }

    // -----------------------------------------------------------------
    // Task references and instrument validation
    // -----------------------------------------------------------------

    for task in &job.tasks {
        if !stream_ids.contains(&task.stream_id) {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "task {} references unknown stream: {}",
                task.task_id.0, task.stream_id.0,
            )));
        }

        if task.instrument.tick_size <= rust_decimal::Decimal::ZERO {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "task {} has invalid tick size: {}",
                task.task_id.0, task.instrument.tick_size,
            )));
        }

        if task.instrument.contract_kind.is_none() && task.instrument.contract_value.is_some() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "task {} defines contract value without contract kind",
                task.task_id.0,
            )));
        }
    }

    // -----------------------------------------------------------------
    // Merge input validation
    // -----------------------------------------------------------------

    for input in &job.merge_inputs {
        if !stream_ids.contains(&input.stream_id) {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "merge input dataset {} references unknown stream: {}",
                input.dataset_id.0, input.stream_id.0,
            )));
        }
    }

    for input in &job.merge_inputs {
        if input.end_timestamp_ns <= input.start_timestamp_ns {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "merge input dataset {} has invalid time range",
                input.dataset_id.0,
            )));
        }
    }

    // -----------------------------------------------------------------
    // Merge time-range validation
    // -----------------------------------------------------------------

    if let Some(range) = job.time_range {
        if range.end_timestamp_ns <= range.start_timestamp_ns {
            return Err(MarketForgeError::InvalidConfiguration(
                "merge job has invalid time range".to_owned(),
            ));
        }
    }

    Ok(())
}
