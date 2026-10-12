use std::{collections::HashMap, path::PathBuf};

use arrow_array::RecordBatch;

use super::{
    materialize_cache::CanonicalRowGroupCache, materialize_planner::GroupedReadPlan,
    materialize_reader::CanonicalParquetReader, materialize_resolver::ResolvedMarketEvent,
    materialize_row_groups::CanonicalRowGroupIndex,
};
use crate::error::{MarketForgeError, Result};
use arrow_array::{Array, Int64Array};

pub struct GroupedCanonicalDecoder {
    reader: CanonicalParquetReader,
    cache: CanonicalRowGroupCache,
    row_groups: HashMap<PathBuf, CanonicalRowGroupIndex>,
}

impl GroupedCanonicalDecoder {
    pub fn new(reader: CanonicalParquetReader, cache: CanonicalRowGroupCache) -> Self {
        Self {
            reader,
            cache,
            row_groups: HashMap::new(),
        }
    }

    pub fn register_row_groups(&mut self, index: CanonicalRowGroupIndex) -> Result<()> {
        let path = index.path().to_path_buf();

        if self.row_groups.contains_key(&path) {
            return invalid(format!("duplicate row-group index: {}", path.display()));
        }

        self.row_groups.insert(path, index);

        Ok(())
    }

    pub fn decode(&mut self, plan: &GroupedReadPlan) -> Result<Vec<ResolvedMarketEvent>> {
        // Each event may contain multiple canonical slices.
        // Preserve their original within-event ordering.
        let mut slots: Vec<Vec<Option<Vec<RecordBatch>>>> = plan
            .event_slice_counts
            .iter()
            .map(|&count| vec![None; count])
            .collect();

        let mut resolved_rows = 0_u64;

        // BTreeMap iteration is deterministic.
        // Physical row-group order does not determine output event order.
        for (key, reads) in &plan.groups {
            let index = self.row_groups.get(&key.path).ok_or_else(|| {
                invalid_error(format!(
                    "unregistered row-group index: {}",
                    key.path.display()
                ))
            })?;

            let group = index.groups().get(key.row_group).ok_or_else(|| {
                invalid_error(format!(
                    "row group {} does not exist in {}",
                    key.row_group,
                    key.path.display()
                ))
            })?;

            if group.index != key.row_group {
                return invalid("row-group index mismatch");
            }

            let group_rows = group.row_count();

            // Decode the complete row group once for this window.
            // The existing reader/cache handles cache hits and misses.
            let batches = self.reader.read_range_cached(
                &key.path,
                group.row_start,
                group_rows,
                &mut self.cache,
            )?;

            let decoded_rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();

            if decoded_rows != group_rows {
                return invalid(format!(
                    "incomplete row-group decode: expected={}, actual={}",
                    group_rows, decoded_rows
                ));
            }

            for read in reads {
                let event_slots = slots
                    .get_mut(read.event_index)
                    .ok_or_else(|| invalid_error("invalid event index"))?;

                let slot = event_slots
                    .get_mut(read.slice_index)
                    .ok_or_else(|| invalid_error("invalid slice index"))?;

                if slot.is_some() {
                    return invalid("duplicate planned canonical slice");
                }

                let slices = slice_batches(&batches, read.local_offset, read.row_count)?;

                resolved_rows = resolved_rows
                    .checked_add(read.row_count)
                    .ok_or_else(|| invalid_error("resolved row-count overflow"))?;

                *slot = Some(slices);
            }
        }

        if resolved_rows != plan.canonical_rows {
            return invalid(format!(
                "grouped decode row mismatch: planned={}, resolved={}",
                plan.canonical_rows, resolved_rows
            ));
        }

        // Restore original chronological order.
        let mut output = Vec::with_capacity(plan.references.len());

        for (reference, event_slots) in plan.references.iter().zip(slots.into_iter()) {
            let mut canonical_batches = Vec::new();

            for slot in event_slots {
                let batches =
                    slot.ok_or_else(|| invalid_error("missing planned canonical slice"))?;

                canonical_batches.extend(batches);
            }

            let rows: u64 = canonical_batches
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum();

            if rows != reference.canonical_row_count {
                return invalid(format!(
                    "event row mismatch: ordinal={}, expected={}, actual={}",
                    reference.event_ordinal, reference.canonical_row_count, rows
                ));
            }

            // Validate canonical timestamps against the logical reference.
            for batch in &canonical_batches {
                let column = batch
                    .column_by_name("event_timestamp_ns")
                    .ok_or_else(|| invalid_error("missing canonical timestamp"))?;

                let timestamps = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid_error("invalid canonical timestamp type"))?;

                for row in 0..timestamps.len() {
                    if timestamps.is_null(row)
                        || timestamps.value(row) != reference.event_timestamp_ns
                    {
                        return invalid(format!(
                            "canonical timestamp mismatch: dataset={}, task={}, ordinal={}",
                            reference.dataset_id, reference.task_id, reference.event_ordinal
                        ));
                    }
                }
            }

            output.push(ResolvedMarketEvent {
                reference: *reference,
                canonical_batches,
            });
        }

        Ok(output)
    }

    pub fn cache_metrics(&self) -> &super::materialize_cache::CanonicalCacheMetrics {
        self.cache.metrics()
    }
}

// -----------------------------------------------------------------------------
// Extract exact rows from a sequence of Arrow batches
// -----------------------------------------------------------------------------

fn slice_batches(batches: &[RecordBatch], offset: u64, count: u64) -> Result<Vec<RecordBatch>> {
    let end = offset
        .checked_add(count)
        .ok_or_else(|| invalid_error("slice range overflow"))?;

    let mut result = Vec::new();
    let mut cursor = 0_u64;
    let mut resolved = 0_u64;

    for batch in batches {
        let rows = batch.num_rows() as u64;

        let batch_end = cursor
            .checked_add(rows)
            .ok_or_else(|| invalid_error("batch offset overflow"))?;

        let start = offset.max(cursor);
        let stop = end.min(batch_end);

        if start < stop {
            let local_offset = usize::try_from(start - cursor)
                .map_err(|_| invalid_error("slice offset overflow"))?;

            let length = usize::try_from(stop - start)
                .map_err(|_| invalid_error("slice length overflow"))?;

            result.push(batch.slice(local_offset, length));

            resolved = resolved
                .checked_add(length as u64)
                .ok_or_else(|| invalid_error("resolved slice overflow"))?;
        }

        cursor = batch_end;

        if cursor >= end {
            break;
        }
    }

    if resolved != count {
        return invalid(format!(
            "incomplete Arrow slice: expected={}, resolved={}",
            count, resolved
        ));
    }

    Ok(result)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
