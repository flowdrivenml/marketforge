use {
    crate::historical::job::MergeParquetConfig, crate::historical::job::MergeResources,
    crate::historical::normalize::resources::ResourceRequirements,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

use crate::historical::merge::materialize::planning::window::MaterializationBatchLimits;

const MIB: u64 = 1024 * 1024;

const BASE_MEMORY_BYTES: u64 = 128 * MIB;
const SOURCE_CURSOR_MEMORY_BYTES: u64 = 32 * MIB;

// Conservative forward-reader reservations.
const FORWARD_CURSOR_MEMORY_BYTES: u64 = 16 * MIB;
const DECODE_MEMORY_BYTES: u64 = 256 * MIB;
const MATERIALIZATION_MEMORY_BYTES: u64 = 256 * MIB;

const WRITER_MULTIPLIER: u64 = 2;

const MAX_WINDOW_EVENTS: usize = 16_384;
const MAX_WINDOW_CANONICAL_ROWS: u64 = 131_072;

const PREFERRED_OPEN_CURSORS: usize = 64;

// Timeline reader + output writer + temporary replacement + safety allowance.
const NON_CURSOR_FILE_ALLOWANCE: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationWorkerLimits {
    pub batch_limits: MaterializationBatchLimits,
    pub reader_batch_size: usize,
    pub max_open_cursors: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationResourcePlan {
    pub requirements: ResourceRequirements,
    pub limits: MaterializationWorkerLimits,
}

pub fn estimate_materialization_resources(
    input_tasks: usize,
    parquet: &MergeParquetConfig,
    resources: &MergeResources,
) -> Result<MaterializationResourcePlan> {
    if input_tasks == 0 {
        return invalid("materialization requires at least one input task");
    }

    if resources.workers == 0 {
        return invalid("materialization requires at least one worker");
    }

    if resources.memory_limit_mb == 0 {
        return invalid("materialization memory budget must be positive");
    }

    if parquet.row_group_target_mb == 0 {
        return invalid("materialization row-group target must be positive");
    }

    if parquet.file_target_mb < parquet.row_group_target_mb {
        return invalid("materialization file target cannot be smaller than row-group target");
    }

    let total_memory = resources
        .memory_limit_mb
        .checked_mul(MIB)
        .ok_or_else(|| invalid_error("materialization memory budget overflow"))?;

    let task_count =
        u64::try_from(input_tasks).map_err(|_| invalid_error("input task count overflow"))?;

    let source_memory = task_count
        .checked_mul(SOURCE_CURSOR_MEMORY_BYTES)
        .ok_or_else(|| invalid_error("source memory estimate overflow"))?;

    let row_group_bytes = parquet
        .row_group_target_mb
        .checked_mul(MIB)
        .ok_or_else(|| invalid_error("row-group byte conversion overflow"))?;

    let writer_memory = row_group_bytes
        .checked_mul(WRITER_MULTIPLIER)
        .ok_or_else(|| invalid_error("writer memory estimate overflow"))?;

    let fixed_memory = BASE_MEMORY_BYTES
        .checked_add(source_memory)
        .and_then(|value| value.checked_add(writer_memory))
        .and_then(|value| value.checked_add(DECODE_MEMORY_BYTES))
        .and_then(|value| value.checked_add(MATERIALIZATION_MEMORY_BYTES))
        .ok_or_else(|| invalid_error("fixed memory estimate overflow"))?;

    // At least one forward cursor is required.
    let minimum_memory = fixed_memory
        .checked_add(FORWARD_CURSOR_MEMORY_BYTES)
        .ok_or_else(|| invalid_error("minimum memory estimate overflow"))?;

    if minimum_memory > total_memory {
        return invalid(format!(
            "insufficient materialization memory: required={} MiB, available={} MiB",
            minimum_memory / MIB,
            total_memory / MIB,
        ));
    }

    let file_budget = u64::try_from(resources.max_open_files)
        .map_err(|_| invalid_error("file budget conversion overflow"))?;

    if file_budget <= NON_CURSOR_FILE_ALLOWANCE {
        return invalid("insufficient file budget for materialization");
    }

    let available_cursor_files = file_budget - NON_CURSOR_FILE_ALLOWANCE;

    let available_cursor_memory = total_memory - fixed_memory;

    let memory_cursor_capacity = available_cursor_memory / FORWARD_CURSOR_MEMORY_BYTES;

    let cursor_capacity = available_cursor_files
        .min(memory_cursor_capacity)
        .min(PREFERRED_OPEN_CURSORS as u64);

    if cursor_capacity == 0 {
        return invalid("insufficient resources for canonical forward cursor");
    }

    let max_open_cursors =
        usize::try_from(cursor_capacity).map_err(|_| invalid_error("cursor capacity overflow"))?;

    let cursor_memory = cursor_capacity
        .checked_mul(FORWARD_CURSOR_MEMORY_BYTES)
        .ok_or_else(|| invalid_error("cursor memory estimate overflow"))?;

    let memory_bytes = fixed_memory
        .checked_add(cursor_memory)
        .ok_or_else(|| invalid_error("materialization memory estimate overflow"))?;

    let open_files = cursor_capacity
        .checked_add(NON_CURSOR_FILE_ALLOWANCE)
        .ok_or_else(|| invalid_error("open-file estimate overflow"))?;

    let batch_limits =
        MaterializationBatchLimits::new(MAX_WINDOW_EVENTS, MAX_WINDOW_CANONICAL_ROWS)?;

    Ok(MaterializationResourcePlan {
        requirements: ResourceRequirements {
            memory_bytes,
            scratch_bytes: 0,
            open_files,
        },
        limits: MaterializationWorkerLimits {
            batch_limits,
            reader_batch_size: 8192,
            max_open_cursors,
        },
    })
}
fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
