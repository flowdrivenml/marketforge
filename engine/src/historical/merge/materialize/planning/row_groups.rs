use std::{
    fs::File,
    path::{Path, PathBuf},
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::shared::error::{MarketForgeError, Result};

// -----------------------------------------------------------------------------
// Row-group metadata
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalRowGroup {
    pub index: usize,
    pub row_start: u64,
    pub row_end: u64,
}

impl CanonicalRowGroup {
    pub fn row_count(&self) -> u64 {
        self.row_end - self.row_start
    }
}

// -----------------------------------------------------------------------------
// Row-group slice
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalRowGroupSlice {
    pub group_index: usize,

    /// Row offset relative to the beginning of this row group.
    pub local_offset: u64,

    pub row_count: u64,
}

// -----------------------------------------------------------------------------
// Immutable row-group index
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CanonicalRowGroupIndex {
    path: PathBuf,
    groups: Vec<CanonicalRowGroup>,
    total_rows: u64,
}

impl CanonicalRowGroupIndex {
    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /// Build a row-group index from Parquet file metadata.
    ///
    /// No canonical values are decoded.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "cannot open canonical Parquet {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!(
                "cannot inspect canonical Parquet {}: {error}",
                path.display()
            ))
        })?;

        let metadata = builder.metadata();

        let mut groups = Vec::with_capacity(metadata.num_row_groups());
        let mut offset = 0_u64;

        for (index, group) in metadata.row_groups().iter().enumerate() {
            let rows = u64::try_from(group.num_rows())
                .map_err(|_| invalid_error("invalid Parquet row-group size"))?;

            if rows == 0 {
                return invalid(format!(
                    "empty Parquet row group: file={}, group={}",
                    path.display(),
                    index
                ));
            }

            let end = offset
                .checked_add(rows)
                .ok_or_else(|| invalid_error("row-group offset overflow"))?;

            groups.push(CanonicalRowGroup {
                index,
                row_start: offset,
                row_end: end,
            });

            offset = end;
        }

        let expected = u64::try_from(metadata.file_metadata().num_rows())
            .map_err(|_| invalid_error("invalid Parquet file row count"))?;

        if offset != expected {
            return invalid(format!(
                "Parquet row-group count mismatch: file={}, groups={}, metadata={}",
                path.display(),
                offset,
                expected
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            groups,
            total_rows: offset,
        })
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn groups(&self) -> &[CanonicalRowGroup] {
        &self.groups
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub fn row_group_count(&self) -> usize {
        self.groups.len()
    }

    // -------------------------------------------------------------------------
    // Row-range resolution
    // -------------------------------------------------------------------------

    /// Resolve a file-relative row range into row-group-local slices.
    ///
    /// The returned slices preserve physical row ordering.
    pub fn resolve(&self, row_offset: u64, row_count: u64) -> Result<Vec<CanonicalRowGroupSlice>> {
        let end = row_offset
            .checked_add(row_count)
            .ok_or_else(|| invalid_error("canonical row range overflow"))?;

        if end > self.total_rows {
            return invalid(format!(
                "canonical row range out of bounds: offset={}, count={}, total={}",
                row_offset, row_count, self.total_rows
            ));
        }

        if row_count == 0 {
            return Ok(Vec::new());
        }

        let mut slices = Vec::new();
        let mut resolved = 0_u64;

        for group in &self.groups {
            if group.row_end <= row_offset {
                continue;
            }

            if group.row_start >= end {
                break;
            }

            let overlap_start = row_offset.max(group.row_start);
            let overlap_end = end.min(group.row_end);

            if overlap_start >= overlap_end {
                continue;
            }

            let count = overlap_end - overlap_start;

            slices.push(CanonicalRowGroupSlice {
                group_index: group.index,
                local_offset: overlap_start - group.row_start,
                row_count: count,
            });

            resolved = resolved
                .checked_add(count)
                .ok_or_else(|| invalid_error("resolved row count overflow"))?;
        }

        if resolved != row_count {
            return invalid(format!(
                "incomplete row-group resolution: expected={}, resolved={}",
                row_count, resolved
            ));
        }

        Ok(slices)
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
