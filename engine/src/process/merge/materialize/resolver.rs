use std::{collections::HashMap, fs::File, path::PathBuf};

use arrow_array::{Array, Int64Array, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    error::{MarketForgeError, Result},
    process::parquet::{depth_schema, trade_schema},
};

use super::{
    materialize_cache::CanonicalRowGroupCache, materialize_inventory::CanonicalRowIndex,
    materialize_reader::CanonicalParquetReader, unified_timeline::UnifiedTimelineRecord,
};

// -----------------------------------------------------------------------------
// Canonical source identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CanonicalTaskKey {
    pub dataset_id: u64,
    pub task_id: u64,
    pub event_kind: u8,
}

// -----------------------------------------------------------------------------
// Resolved event
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ResolvedMarketEvent {
    pub reference: UnifiedTimelineRecord,
    pub canonical_batches: Vec<RecordBatch>,
}

impl ResolvedMarketEvent {
    pub fn canonical_rows(&self) -> usize {
        self.canonical_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }
}

// -----------------------------------------------------------------------------
// Resolver
// -----------------------------------------------------------------------------

pub struct CanonicalReferenceResolver {
    tasks: HashMap<CanonicalTaskKey, CanonicalRowIndex>,
    reader: CanonicalParquetReader,
    cache: CanonicalRowGroupCache,
}

impl CanonicalReferenceResolver {
    pub fn new(reader: CanonicalParquetReader, cache: CanonicalRowGroupCache) -> Self {
        Self {
            tasks: HashMap::new(),
            reader,
            cache,
        }
    }

    // -------------------------------------------------------------------------
    // Task registration
    // -------------------------------------------------------------------------

    /// Register one canonical task.
    ///
    /// Files must be ordered by canonical part index.
    /// Each entry contains (path, canonical row count).
    pub fn register_task(
        &mut self,
        key: CanonicalTaskKey,
        files: Vec<(PathBuf, u64)>,
    ) -> Result<()> {
        if key.event_kind > 1 {
            return invalid(format!(
                "unsupported canonical event kind: {}",
                key.event_kind
            ));
        }

        if self.tasks.contains_key(&key) {
            return invalid(format!("duplicate canonical task registration: {:?}", key));
        }

        let index = CanonicalRowIndex::new(files)?;

        // Validate the complete canonical schema and physical row counts
        // before accepting the task.
        let expected_schema = match key.event_kind {
            0 => trade_schema(),
            1 => depth_schema(),
            _ => unreachable!(),
        };

        for file in index.files() {
            let handle = File::open(&file.path).map_err(|error| {
                invalid_error(format!(
                    "cannot open canonical file {}: {error}",
                    file.path.display()
                ))
            })?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(handle).map_err(|error| {
                invalid_error(format!(
                    "cannot inspect canonical file {}: {error}",
                    file.path.display()
                ))
            })?;

            if builder.schema().as_ref() != expected_schema.as_ref() {
                return invalid(format!(
                    "canonical schema mismatch: {}",
                    file.path.display()
                ));
            }

            let physical_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
                .map_err(|_| invalid_error("invalid canonical row count"))?;

            let indexed_rows = file.row_end - file.row_start;

            if physical_rows != indexed_rows {
                return invalid(format!(
                    "canonical row-count mismatch: file={}, indexed={}, physical={}",
                    file.path.display(),
                    indexed_rows,
                    physical_rows
                ));
            }
        }

        self.tasks.insert(key, index);

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Reference resolution
    // -------------------------------------------------------------------------

    pub fn resolve(&mut self, reference: UnifiedTimelineRecord) -> Result<ResolvedMarketEvent> {
        let key = CanonicalTaskKey {
            dataset_id: reference.dataset_id,
            task_id: reference.task_id,
            event_kind: reference.event_kind,
        };

        let index = self
            .tasks
            .get(&key)
            .ok_or_else(|| invalid_error(format!("unregistered canonical task: {:?}", key)))?;

        match reference.event_kind {
            0 => {
                if reference.canonical_row_count != 1
                    || reference.event_boundary.is_some()
                    || reference.source_operation.is_some()
                {
                    return invalid("invalid canonical trade reference");
                }
            }

            1 => {
                if reference.event_boundary.is_none() || reference.source_operation.is_none() {
                    return invalid("incomplete canonical depth metadata");
                }
            }

            _ => return invalid("unsupported event kind"),
        }

        let slices = index.resolve(
            reference.canonical_row_offset,
            reference.canonical_row_count,
        )?;

        let batches = self.reader.read_slices_cached(&slices, &mut self.cache)?;

        let resolved = batches.iter().try_fold(0_u64, |total, batch| {
            let rows = u64::try_from(batch.num_rows())
                .map_err(|_| invalid_error("canonical batch row-count overflow"))?;

            total
                .checked_add(rows)
                .ok_or_else(|| invalid_error("canonical row-count overflow"))
        })?;

        if resolved != reference.canonical_row_count {
            return invalid(format!(
                "canonical reference mismatch: expected={}, resolved={}",
                reference.canonical_row_count, resolved
            ));
        }

        // Every canonical row belonging to the event must carry
        // the timestamp established by the logical merge.
        for batch in &batches {
            let column = batch
                .column_by_name("event_timestamp_ns")
                .ok_or_else(|| invalid_error("missing canonical timestamp"))?;

            let timestamps = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| invalid_error("invalid canonical timestamp type"))?;

            for row in 0..timestamps.len() {
                if timestamps.is_null(row) || timestamps.value(row) != reference.event_timestamp_ns
                {
                    return invalid(format!(
                        "canonical timestamp mismatch: dataset={}, task={}, ordinal={}",
                        reference.dataset_id, reference.task_id, reference.event_ordinal
                    ));
                }
            }
        }

        Ok(ResolvedMarketEvent {
            reference,
            canonical_batches: batches,
        })
    }

    // -------------------------------------------------------------------------
    // Batch resolution
    // -------------------------------------------------------------------------

    /// Resolve references without changing chronological order.
    ///
    /// This is the correctness-first implementation.
    /// Source-grouped resolution will be added in M2.5b.
    pub fn resolve_batch(
        &mut self,
        references: &[UnifiedTimelineRecord],
    ) -> Result<Vec<ResolvedMarketEvent>> {
        let mut output = Vec::with_capacity(references.len());

        for reference in references {
            output.push(self.resolve(*reference)?);
        }

        Ok(output)
    }

    pub fn cache_metrics(&self) -> &super::materialize_cache::CanonicalCacheMetrics {
        self.cache.metrics()
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
