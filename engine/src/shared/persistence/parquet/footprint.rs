//! Admission allowances, not measured peaks or allocator guarantees.
use crate::shared::{
    error::{MarketForgeError, Result},
    persistence::config::ParquetResourceConfig,
};
/// Builders retain their Vec capacity while Arrow arrays are constructed. Encoding
/// and compression add another working set. Depth also retains segment state and
/// clones it when accepting an outcome and when constructing its manifest.
pub fn normalization_writer_memory(
    config: &ParquetResourceConfig,
    depth: bool,
    maximum_levels: u64,
    identity_bytes: u64,
    metadata_entries: u64,
) -> Result<u64> {
    let overflow = || MarketForgeError::InvalidConfiguration("writer footprint overflow".into());
    // Row-group column metadata, truncated statistics and allocator overhead;
    // 16 KiB per group is a heuristic allowance for the active canonical and
    // auxiliary writer footers, whose group counts are explicitly bounded.
    let batch = config
        .row_group_target_bytes
        .checked_mul(4)
        .and_then(|v| v.checked_add(metadata_entries.checked_mul(16 * 1024)?))
        .ok_or_else(overflow)?;
    if !depth {
        return batch.checked_add(16 * 1024 * 1024).ok_or_else(overflow);
    }
    // Complete outcomes can overshoot a batch boundary; snapshots can emit both
    // old-level removals and new-level additions. Includes retained Vec capacity.
    let outcome = maximum_levels
        .checked_mul(2)
        .and_then(|v| v.checked_mul(identity_bytes.checked_add(512)?))
        .ok_or_else(overflow)?;
    batch
        .checked_add(outcome)
        .and_then(|v| {
            v.checked_add(
                normalization_segment_memory(config, metadata_entries)
                    .ok()?
                    .checked_mul(3)?,
            )
        })
        // Fixed 8192-entry event batch, Arrow index batch and encoder allowance.
        .and_then(|v| v.checked_add(16 * 1024 * 1024))
        .ok_or_else(overflow)
}

/// All owning writers use default Parquet properties and explicitly flush each
/// batch. Account for automatic row splitting before ArrowWriter::write grows
/// its retained footer inventory. No configured output behavior is changed.
pub(super) fn check_row_group_growth(current: usize, batch_rows: usize) -> Result<()> {
    if let Some(limits) = crate::shared::historical::limits::current() {
        let additional =
            batch_rows.div_ceil(parquet::file::properties::DEFAULT_MAX_ROW_GROUP_ROW_COUNT);
        let requested = current.checked_add(additional).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("row group accounting overflow".into())
        })?;
        crate::shared::historical::limits::check(
            "Parquet row groups",
            limits.metric_entries,
            requested as u64,
            "writer footer growth",
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod resource_tests {
    use super::*;
    #[test]
    fn footer_growth_checks_predicted_groups_before_writing() {
        let mut limits = crate::shared::historical::limits::NormalizationLimits::default();
        limits.metric_entries = 2;
        let _scope =
            crate::shared::historical::limits::LimitScope::enter(limits, "mock writer".into());
        assert!(check_row_group_growth(1, 10).is_ok());
        assert!(matches!(
            check_row_group_growth(2, 1),
            Err(MarketForgeError::ResourceLimit(_))
        ));
        // Inject row counts rather than building a million-row Arrow batch.
        assert!(matches!(
            check_row_group_growth(
                1,
                parquet::file::properties::DEFAULT_MAX_ROW_GROUP_ROW_COUNT + 1
            ),
            Err(MarketForgeError::ResourceLimit(_))
        ));
    }
}

/// Maximum logical segment inventory under both the shared byte allowance and
/// normalization's metadata-entry quota. Vec/clone peaks are accounted separately.
pub fn normalization_segment_memory(config: &ParquetResourceConfig, entries: u64) -> Result<u64> {
    let bytes = entries
        .checked_mul(std::mem::size_of::<crate::shared::domain::book::BookSegment>() as u64)
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("segment footprint overflow".into())
        })?;
    Ok(config.row_group_target_bytes.max(1024 * 1024).min(bytes))
}
