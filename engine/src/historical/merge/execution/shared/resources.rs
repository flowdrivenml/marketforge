use crate::historical::merge::partition::shared::partition_input::PartitionInputPlan;
use {
    crate::historical::job::MergeParquetConfig,
    crate::historical::normalize::resources::ResourceRequirements,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};
// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

const MIB: u64 = 1024 * 1024;

// Baseline memory for partition execution, metadata, heap,
// stream bookkeeping, and temporary allocations.
const BASE_MEMORY_BYTES: u64 = 128 * MIB;

// Estimated memory per active Parquet event cursor.
//
// Includes Arrow batches, page-index metadata, decoder buffers,
// and additional temporary allocations.
const CURSOR_MEMORY_BYTES: u64 = 32 * MIB;

// Parquet writer buffering multiplier.
//
// Accounts for the timeline record buffer, Arrow conversion,
// and Parquet encoding buffers.
const WRITER_BUFFER_MULTIPLIER: u64 = 2;

// One output writer and one additional descriptor allowance.
const OUTPUT_FILE_ALLOWANCE: u64 = 2;

// -----------------------------------------------------------------------------
// Merge partition resource estimation
// -----------------------------------------------------------------------------

/// Estimate resource requirements for one merge partition.
///
/// `input_streams` is the number of active source streams.
///
/// These estimates are cooperative scheduling reservations,
/// not strict operating-system resource limits.
pub fn estimate_merge_stream_resources(
    input_streams: usize,
    parquet: &MergeParquetConfig,
) -> Result<ResourceRequirements> {
    if input_streams == 0 {
        return Ok(ResourceRequirements {
            memory_bytes: 384 * MIB,
            scratch_bytes: 0,
            open_files: 0,
        });
    }

    if parquet.row_group_target_mb == 0 {
        return invalid("merge row-group target must be greater than zero");
    }

    if parquet.file_target_mb < parquet.row_group_target_mb {
        return invalid("merge file target must be at least the row-group target");
    }

    // -------------------------------------------------------------------------
    // Cursor memory
    // -------------------------------------------------------------------------

    let stream_count =
        u64::try_from(input_streams).map_err(|_| invalid_error("merge stream count overflow"))?;

    let cursor_memory = stream_count
        .checked_mul(CURSOR_MEMORY_BYTES)
        .ok_or_else(|| invalid_error("merge cursor memory estimate overflow"))?;

    // -------------------------------------------------------------------------
    // Writer memory
    // -------------------------------------------------------------------------

    let row_group_bytes = parquet
        .row_group_target_mb
        .checked_mul(MIB)
        .ok_or_else(|| invalid_error("merge row-group byte conversion overflow"))?;

    let writer_memory = row_group_bytes
        .checked_mul(WRITER_BUFFER_MULTIPLIER)
        .ok_or_else(|| invalid_error("merge writer memory estimate overflow"))?;

    // -------------------------------------------------------------------------
    // Total memory
    // -------------------------------------------------------------------------

    let memory_bytes = BASE_MEMORY_BYTES
        .checked_add(cursor_memory)
        .and_then(|value| value.checked_add(writer_memory))
        .ok_or_else(|| invalid_error("merge memory estimate overflow"))?;

    // -------------------------------------------------------------------------
    // Open-file reservations
    // -------------------------------------------------------------------------

    let open_files = stream_count
        .checked_add(OUTPUT_FILE_ALLOWANCE)
        .ok_or_else(|| invalid_error("merge open-file estimate overflow"))?;

    Ok(ResourceRequirements {
        memory_bytes,
        scratch_bytes: 0,
        open_files,
    })
}
pub fn estimate_merge_partition_resources(
    plan: &PartitionInputPlan,
    parquet: &MergeParquetConfig,
) -> Result<ResourceRequirements> {
    use std::collections::HashSet;

    let input_streams = plan
        .inputs
        .iter()
        .map(|input| input.dataset_id)
        .collect::<HashSet<_>>()
        .len();

    if input_streams == 0 {
        return Ok(ResourceRequirements {
            memory_bytes: 384 * MIB,
            scratch_bytes: 0,
            open_files: OUTPUT_FILE_ALLOWANCE,
        });
    }

    estimate_merge_stream_resources(input_streams, parquet)
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

// -----------------------------------------------------------------------------
// Unit tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MergeParquetConfig {
        MergeParquetConfig {
            row_group_target_mb: 128,
            file_target_mb: 1024,
            compression_level: 3,
        }
    }

    #[test]
    fn estimates_four_stream_partition() {
        let requirements = estimate_merge_stream_resources(4, &config()).unwrap();

        assert_eq!(requirements.memory_bytes, 512 * MIB);
        assert_eq!(requirements.scratch_bytes, 0);
        assert_eq!(requirements.open_files, 6);
    }

    #[test]
    fn estimates_single_stream_partition() {
        let requirements = estimate_merge_stream_resources(1, &config()).unwrap();

        assert_eq!(requirements.memory_bytes, 416 * MIB);
        assert_eq!(requirements.open_files, 3);
    }

    #[test]
    fn rejects_empty_streams() {
        assert!(estimate_merge_stream_resources(0, &config()).is_err());
    }

    #[test]
    fn rejects_invalid_row_group_target() {
        let mut parquet = config();

        parquet.row_group_target_mb = 0;

        assert!(estimate_merge_stream_resources(4, &parquet).is_err());
    }

    #[test]
    fn rejects_invalid_file_target() {
        let mut parquet = config();

        parquet.file_target_mb = 64;

        assert!(estimate_merge_stream_resources(4, &parquet).is_err());
    }

    #[test]
    fn rejects_memory_overflow() {
        let mut parquet = config();

        parquet.row_group_target_mb = u64::MAX;

        assert!(estimate_merge_stream_resources(4, &parquet).is_err());
    }

    #[test]
    fn resource_estimates_scale_with_stream_count() {
        let one = estimate_merge_stream_resources(1, &config()).unwrap();

        let four = estimate_merge_stream_resources(4, &config()).unwrap();

        assert!(four.memory_bytes > one.memory_bytes);
        assert!(four.open_files > one.open_files);
    }
}
