use std::fs;

use crate::{
    error::{MarketForgeError, Result},
    job::{ParquetResourceConfig, TargetSchema, WorkTask},
};

use super::budget::ResourceRequirements;

// -----------------------------------------------------------------------------
// Estimation constants
// -----------------------------------------------------------------------------

const MIB: u64 = 1024 * 1024;

// Baseline memory for processor state, decoding, and bookkeeping.
const BASE_MEMORY_BYTES: u64 = 128 * MIB;

// Conservative Parquet buffering multiplier.
const PARQUET_BUFFER_MULTIPLIER: u64 = 2;

// Source-format multipliers.
// These are initial scheduling heuristics, not measured memory guarantees.
const STREAMING_SOURCE_MULTIPLIER: u64 = 1;
const XLSX_SOURCE_MULTIPLIER: u64 = 20;

// Minimum estimated memory for XLSX processing.
const XLSX_MINIMUM_MEMORY_BYTES: u64 = 512 * MIB;

// -----------------------------------------------------------------------------
// Task resource estimation
// -----------------------------------------------------------------------------

pub fn estimate_task_resources(
    task: &WorkTask,
    parquet: &ParquetResourceConfig,
) -> Result<ResourceRequirements> {
    let metadata = fs::metadata(&task.input_path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "cannot inspect source archive {}: {error}",
            task.input_path.display(),
        ))
    })?;

    let source_bytes = metadata.len();

    let is_depth = task
        .normalizations
        .iter()
        .any(|normalization| normalization.target_schema == TargetSchema::Depth);

    let is_xlsx = is_depth
        && task
            .raw_schema
            .get("workbook_sheets")
            .is_some_and(|value| value.as_u64().is_some_and(|count| count > 0));

    let source_memory = if is_xlsx {
        source_bytes
            .checked_mul(XLSX_SOURCE_MULTIPLIER)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("XLSX memory estimate overflow".to_owned())
            })?
            .max(XLSX_MINIMUM_MEMORY_BYTES)
    } else {
        source_bytes
            .checked_mul(STREAMING_SOURCE_MULTIPLIER)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("source memory estimate overflow".to_owned())
            })?
    };

    let parquet_memory = parquet
        .row_group_target_bytes
        .checked_mul(PARQUET_BUFFER_MULTIPLIER)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("Parquet memory estimate overflow".to_owned())
        })?;

    let memory_bytes = BASE_MEMORY_BYTES
        .checked_add(source_memory)
        .and_then(|value| value.checked_add(parquet_memory))
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("total memory estimate overflow".to_owned())
        })?;

    Ok(ResourceRequirements {
        memory_bytes,
        scratch_bytes: 0,
        open_files: 0,
    })
}
