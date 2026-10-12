use std::{fs::File, path::Path};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{MarketForgeError, Result};

use super::{materialize_cache::CanonicalRowGroupCache, materialize_inventory::CanonicalFileSlice};

const READ_BATCH_SIZE: usize = 8192;

/// Reads exact canonical row ranges from Parquet files.
///
/// - Preserves all Arrow types, including Decimal256.
/// - Supports ranges spanning multiple row groups.
/// - Decodes only overlapping row groups.
/// - Returns bounded Arrow batches.
/// - Does not modify canonical files.
#[derive(Debug, Clone)]
pub struct CanonicalParquetReader {
    batch_size: usize,
}

impl Default for CanonicalParquetReader {
    fn default() -> Self {
        Self {
            batch_size: READ_BATCH_SIZE,
        }
    }
}

impl CanonicalParquetReader {
    pub fn new(batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            return invalid("canonical reader batch size must be positive");
        }

        Ok(Self { batch_size })
    }

    /// Read one file-local canonical row range.
    pub fn read_range(
        &self,
        path: &Path,
        row_offset: u64,
        row_count: u64,
    ) -> Result<Vec<RecordBatch>> {
        if row_count == 0 {
            return Ok(Vec::new());
        }

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

        let end = row_offset
            .checked_add(row_count)
            .ok_or_else(|| invalid_error("canonical row range overflow"))?;

        let total_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
            .map_err(|_| invalid_error("invalid canonical Parquet row count"))?;

        if end > total_rows {
            return invalid(format!(
                "canonical range out of bounds: file={}, offset={}, count={}, total={}",
                path.display(),
                row_offset,
                row_count,
                total_rows
            ));
        }

        // -------------------------------------------------------------------------
        // Locate overlapping Parquet row groups
        // -------------------------------------------------------------------------

        let mut selected_groups = Vec::new();
        let mut group_start = 0_u64;
        let mut selected_start = None;

        for (index, group) in builder.metadata().row_groups().iter().enumerate() {
            let group_rows = u64::try_from(group.num_rows())
                .map_err(|_| invalid_error("invalid Parquet row-group size"))?;

            let group_end = group_start
                .checked_add(group_rows)
                .ok_or_else(|| invalid_error("row-group offset overflow"))?;

            if group_end > row_offset && group_start < end {
                selected_start.get_or_insert(group_start);
                selected_groups.push(index);
            }

            group_start = group_end;

            if group_start >= end {
                break;
            }
        }

        let selected_start = selected_start
            .ok_or_else(|| invalid_error("canonical range has no overlapping row groups"))?;

        // -------------------------------------------------------------------------
        // Initialize reader for selected row groups
        // -------------------------------------------------------------------------

        let reader = builder
            .with_row_groups(selected_groups)
            .with_batch_size(self.batch_size)
            .build()
            .map_err(|error| {
                invalid_error(format!("cannot initialize canonical reader: {error}"))
            })?;

        // -------------------------------------------------------------------------
        // Read and slice exact canonical row range
        // -------------------------------------------------------------------------

        let mut cursor = selected_start;
        let mut result = Vec::new();
        let mut resolved = 0_u64;

        for batch in reader {
            let batch = batch.map_err(|error| {
                invalid_error(format!("canonical Parquet decoding failed: {error}"))
            })?;

            let batch_rows = u64::try_from(batch.num_rows())
                .map_err(|_| invalid_error("batch row count overflow"))?;

            let batch_end = cursor
                .checked_add(batch_rows)
                .ok_or_else(|| invalid_error("batch offset overflow"))?;

            let overlap_start = cursor.max(row_offset);
            let overlap_end = batch_end.min(end);

            if overlap_start < overlap_end {
                let local_start = usize::try_from(overlap_start - cursor)
                    .map_err(|_| invalid_error("slice offset overflow"))?;

                let length = usize::try_from(overlap_end - overlap_start)
                    .map_err(|_| invalid_error("slice length overflow"))?;

                result.push(batch.slice(local_start, length));

                resolved = resolved
                    .checked_add(length as u64)
                    .ok_or_else(|| invalid_error("resolved row count overflow"))?;
            }

            cursor = batch_end;

            if cursor >= end {
                break;
            }
        }

        // -------------------------------------------------------------------------
        // Validate exact row coverage
        // -------------------------------------------------------------------------

        if resolved != row_count {
            return invalid(format!(
                "incomplete canonical read: expected={}, resolved={}",
                row_count, resolved
            ));
        }

        Ok(result)
    }

    /// Read an exact file-local canonical row range using a decoded
    /// row-group cache.
    ///
    /// Cached batches retain their original row-group-relative ordering.
    /// Returned RecordBatch slices share Arrow buffers without copying
    /// canonical values.
    pub fn read_range_cached(
        &self,
        path: &Path,
        row_offset: u64,
        row_count: u64,
        cache: &mut CanonicalRowGroupCache,
    ) -> Result<Vec<RecordBatch>> {
        if row_count == 0 {
            return Ok(Vec::new());
        }

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

        let end = row_offset
            .checked_add(row_count)
            .ok_or_else(|| invalid_error("canonical row range overflow"))?;

        let total_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
            .map_err(|_| invalid_error("invalid canonical Parquet row count"))?;

        if end > total_rows {
            return invalid(format!(
                "canonical range out of bounds: file={}, offset={}, count={}, total={}",
                path.display(),
                row_offset,
                row_count,
                total_rows
            ));
        }

        // -------------------------------------------------------------------------
        // Identify overlapping row groups
        // -------------------------------------------------------------------------

        let mut groups = Vec::new();
        let mut group_start = 0_u64;

        for (index, group) in builder.metadata().row_groups().iter().enumerate() {
            let rows = u64::try_from(group.num_rows())
                .map_err(|_| invalid_error("invalid Parquet row-group size"))?;

            let group_end = group_start
                .checked_add(rows)
                .ok_or_else(|| invalid_error("row-group offset overflow"))?;

            if group_end > row_offset && group_start < end {
                groups.push((index, group_start, group_end));
            }

            group_start = group_end;

            if group_start >= end {
                break;
            }
        }

        if groups.is_empty() {
            return invalid("canonical range has no overlapping row groups");
        }

        // -------------------------------------------------------------------------
        // Resolve each row group
        // -------------------------------------------------------------------------

        let mut result = Vec::new();
        let mut resolved_rows = 0_u64;

        for (group_index, group_start, group_end) in groups {
            let batches = match cache.get(path, group_index) {
                Some(cached) => cached,

                None => {
                    let decoded = self.decode_row_group(path, group_index)?;

                    cache.insert(path, group_index, decoded.clone())?;

                    decoded
                }
            };

            let overlap_start = row_offset.max(group_start);
            let overlap_end = end.min(group_end);

            let local_start = overlap_start - group_start;
            let local_end = overlap_end - group_start;

            // ---------------------------------------------------------------------
            // Extract requested rows from decoded Arrow batches
            // ---------------------------------------------------------------------

            let mut batch_start = 0_u64;
            let mut group_resolved = 0_u64;

            for batch in batches {
                let batch_rows = u64::try_from(batch.num_rows())
                    .map_err(|_| invalid_error("batch row count overflow"))?;

                let batch_end = batch_start
                    .checked_add(batch_rows)
                    .ok_or_else(|| invalid_error("batch offset overflow"))?;

                let start = local_start.max(batch_start);
                let end = local_end.min(batch_end);

                if start < end {
                    let offset = usize::try_from(start - batch_start)
                        .map_err(|_| invalid_error("slice offset overflow"))?;

                    let length = usize::try_from(end - start)
                        .map_err(|_| invalid_error("slice length overflow"))?;

                    result.push(batch.slice(offset, length));

                    group_resolved = group_resolved
                        .checked_add(length as u64)
                        .ok_or_else(|| invalid_error("resolved row count overflow"))?;
                }

                batch_start = batch_end;

                if batch_start >= local_end {
                    break;
                }
            }

            let expected = overlap_end - overlap_start;

            if group_resolved != expected {
                return invalid(format!(
                    "incomplete row-group resolution: group={}, expected={}, resolved={}",
                    group_index, expected, group_resolved
                ));
            }

            resolved_rows = resolved_rows
                .checked_add(group_resolved)
                .ok_or_else(|| invalid_error("total resolved row count overflow"))?;
        }

        if resolved_rows != row_count {
            return invalid(format!(
                "incomplete cached canonical read: expected={}, resolved={}",
                row_count, resolved_rows
            ));
        }

        Ok(result)
    }

    /// Decode one complete Parquet row group.
    ///
    /// The returned batches are suitable for insertion into the
    /// canonical row-group cache.
    fn decode_row_group(&self, path: &Path, row_group: usize) -> Result<Vec<RecordBatch>> {
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

        if row_group >= builder.metadata().num_row_groups() {
            return invalid(format!(
                "Parquet row group out of bounds: file={}, group={}",
                path.display(),
                row_group
            ));
        }

        let reader = builder
            .with_row_groups(vec![row_group])
            .with_batch_size(self.batch_size)
            .build()
            .map_err(|error| {
                invalid_error(format!("cannot initialize row-group reader: {error}"))
            })?;

        let mut batches = Vec::new();

        for batch in reader {
            batches.push(batch.map_err(|error| {
                invalid_error(format!("canonical row-group decoding failed: {error}"))
            })?);
        }

        Ok(batches)
    }

    /// Resolve file slices using the row-group cache.
    pub fn read_slices_cached(
        &self,
        slices: &[CanonicalFileSlice],
        cache: &mut CanonicalRowGroupCache,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();

        for slice in slices {
            result.extend(self.read_range_cached(
                &slice.path,
                slice.local_offset,
                slice.row_count,
                cache,
            )?);
        }

        Ok(result)
    }

    /// Read file slices produced by CanonicalRowIndex.
    ///
    /// The returned batches preserve the ordering of the input slices.
    pub fn read_slices(&self, slices: &[CanonicalFileSlice]) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();

        for slice in slices {
            let batches = self.read_range(&slice.path, slice.local_offset, slice.row_count)?;

            result.extend(batches);
        }

        Ok(result)
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_batch_size() {
        assert!(CanonicalParquetReader::new(0).is_err());
    }

    #[test]
    fn default_batch_size_is_positive() {
        assert!(CanonicalParquetReader::default().batch_size > 0);
    }
}
