use std::collections::HashSet;

use crate::error::{MarketForgeError, Result};

use super::{PROCESSING_PROTOCOL_VERSION, ProcessingJob};

pub fn validate_processing_job(job: &ProcessingJob) -> Result<()> {
    if job.protocol_version != PROCESSING_PROTOCOL_VERSION {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "unsupported processing protocol version: expected {}, received {}",
            PROCESSING_PROTOCOL_VERSION, job.protocol_version,
        )));
    }

    if job.tasks.is_empty() {
        return Err(MarketForgeError::InvalidConfiguration(
            "processing job contains no tasks".to_owned(),
        ));
    }

    if job.streams.is_empty() {
        return Err(MarketForgeError::InvalidConfiguration(
            "processing job contains no streams".to_owned(),
        ));
    }

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

    if job.output.staging_path == job.output.dataset_path {
        return Err(MarketForgeError::InvalidConfiguration(
            "staging path and final dataset path must be different".to_owned(),
        ));
    }

    let mut task_ids = HashSet::new();

    for task in &job.tasks {
        if !task_ids.insert(task.task_id) {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "duplicate task ID: {}",
                task.task_id.0
            )));
        }
    }

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

    Ok(())
}
