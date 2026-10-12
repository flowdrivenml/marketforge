use crate::{
    error::{MarketForgeError, Result},
    job::{MergeParquetConfig, MergeResources},
    process::resources::ResourceRequirements,
};

use super::materialize_window::MaterializationBatchLimits;

const MIB: u64 = 1024 * 1024;

// Worker bookkeeping, Arrow metadata, and temporary allocations.
const BASE_MEMORY_BYTES: u64 = 128 * MIB;

// Per-source event metadata cursor and associated buffers.
const SOURCE_CURSOR_MEMORY_BYTES: u64 = 32 * MIB;

// Canonical row-group cache reservation.
const MIN_CACHE_BYTES: u64 = 64 * MIB;
const MAX_CACHE_BYTES: u64 = 512 * MIB;

// Canonical decoding and nested Arrow construction.
const DECODE_MULTIPLIER: u64 = 2;
const MATERIALIZATION_MULTIPLIER: u64 = 2;

// Parquet writer buffering.
const WRITER_MULTIPLIER: u64 = 2;

// Internal reference window bounds.
const MAX_WINDOW_EVENTS: usize = 16_384;
const MAX_WINDOW_CANONICAL_ROWS: u64 = 131_072;

// One output writer and one descriptor allowance.
const OUTPUT_FILE_ALLOWANCE: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationWorkerLimits {
    pub cache_bytes: u64,
    pub batch_limits: MaterializationBatchLimits,
    pub reader_batch_size: usize,
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

    // ResourceBudget determines effective concurrency.
    //
    // The estimator describes one partition's requirements,
    // independently of the configured worker count.
    let worker_budget = total_memory;

    let task_count =
        u64::try_from(input_tasks).map_err(|_| invalid_error("input task count overflow"))?;

    let cursor_memory = task_count
        .checked_mul(SOURCE_CURSOR_MEMORY_BYTES)
        .ok_or_else(|| invalid_error("cursor memory estimate overflow"))?;

    let row_group_bytes = parquet
        .row_group_target_mb
        .checked_mul(MIB)
        .ok_or_else(|| invalid_error("row-group byte conversion overflow"))?;

    let writer_memory = row_group_bytes
        .checked_mul(WRITER_MULTIPLIER)
        .ok_or_else(|| invalid_error("writer memory estimate overflow"))?;

    let fixed_memory = BASE_MEMORY_BYTES
        .checked_add(cursor_memory)
        .and_then(|value| value.checked_add(writer_memory))
        .ok_or_else(|| invalid_error("fixed memory estimate overflow"))?;

    if worker_budget <= fixed_memory {
        return invalid(format!(
            "insufficient materialization worker memory: \
             available={} MiB, fixed={} MiB",
            worker_budget / MIB,
            fixed_memory / MIB
        ));
    }

    let available = worker_budget - fixed_memory;

    // Prefer a 128 MiB worker-local cache.
    // Reduce it only when the available budget requires it.
    const PREFERRED_CACHE_BYTES: u64 = 128 * MIB;

    if available / 5 < MIN_CACHE_BYTES {
        return invalid(format!(
            "insufficient memory for minimum materialization cache: \
         available={} MiB, required={} MiB",
            available / MIB,
            (MIN_CACHE_BYTES * 5) / MIB
        ));
    }

    let cache_bytes = PREFERRED_CACHE_BYTES
        .min(available / 5)
        .clamp(MIN_CACHE_BYTES, MAX_CACHE_BYTES);

    let decode_memory = cache_bytes
        .checked_mul(DECODE_MULTIPLIER)
        .ok_or_else(|| invalid_error("decode memory estimate overflow"))?;

    let materialization_memory = cache_bytes
        .checked_mul(MATERIALIZATION_MULTIPLIER)
        .ok_or_else(|| invalid_error("materialization memory estimate overflow"))?;

    let memory_bytes = fixed_memory
        .checked_add(cache_bytes)
        .and_then(|value| value.checked_add(decode_memory))
        .and_then(|value| value.checked_add(materialization_memory))
        .ok_or_else(|| invalid_error("materialization memory estimate overflow"))?;

    if memory_bytes > worker_budget {
        return invalid(format!(
            "materialization resource estimate exceeds worker budget: \
             required={} MiB, available={} MiB",
            memory_bytes / MIB,
            worker_budget / MIB
        ));
    }

    let open_files = task_count
        .checked_mul(2)
        .and_then(|value| value.checked_add(OUTPUT_FILE_ALLOWANCE))
        .ok_or_else(|| invalid_error("open-file estimate overflow"))?;

    let max_events = MAX_WINDOW_EVENTS;

    let max_canonical_rows = MAX_WINDOW_CANONICAL_ROWS;

    let batch_limits = MaterializationBatchLimits::new(max_events, max_canonical_rows)?;

    let limits = MaterializationWorkerLimits {
        cache_bytes,
        batch_limits,
        reader_batch_size: 8192,
    };

    Ok(MaterializationResourcePlan {
        requirements: ResourceRequirements {
            memory_bytes,
            scratch_bytes: 0,
            open_files,
        },
        limits,
    })
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
