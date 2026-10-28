use std::{collections::HashMap, path::PathBuf};

use crate::historical::merge::materialize::readers::reader::CanonicalForwardCursor;
use crate::shared::error::{MarketForgeError, Result};
use arrow_array::RecordBatch;
use arrow_array::{Array, Int64Array};
use std::collections::HashSet;
use std::collections::VecDeque;
use {
    crate::historical::merge::materialize::planning::planner::GroupedReadPlan,
    crate::historical::merge::materialize::planning::row_groups::CanonicalRowGroupIndex,
    crate::historical::merge::materialize::readers::reader::CanonicalParquetReader,
    crate::historical::merge::materialize::shared::resolver::ResolvedMarketEvent,
};

pub struct GroupedCanonicalDecoder {
    reader: CanonicalParquetReader,
    row_groups: HashMap<PathBuf, CanonicalRowGroupIndex>,
    profile_seen_groups: HashSet<(PathBuf, usize)>,
    profile_repeated_groups: u64,
    profile_group_requests: u64,
    forward_cursors: HashMap<PathBuf, CanonicalForwardCursor>,
    max_open_cursors: usize,
    cursor_lru: VecDeque<PathBuf>,
    cursor_high_water: HashMap<PathBuf, u64>,
    cursor_openings: u64,
    cursor_evictions: u64,
    peak_open_cursors: usize,
}

impl GroupedCanonicalDecoder {
    pub fn new(reader: CanonicalParquetReader, max_open_cursors: usize) -> Self {
        assert!(max_open_cursors > 0);

        Self {
            reader,
            row_groups: HashMap::new(),
            forward_cursors: HashMap::new(),
            max_open_cursors,
            cursor_lru: VecDeque::new(),

            profile_seen_groups: HashSet::new(),
            profile_repeated_groups: 0,
            profile_group_requests: 0,
            cursor_high_water: HashMap::new(),
            cursor_openings: 0,
            cursor_evictions: 0,
            peak_open_cursors: 0,
        }
    }
    // -------------------------------------------------------------------------
    // Cursor LRU management
    // -------------------------------------------------------------------------

    fn touch_cursor(&mut self, path: &PathBuf) {
        if let Some(position) = self.cursor_lru.iter().position(|item| item == path) {
            self.cursor_lru.remove(position);
        }

        self.cursor_lru.push_back(path.clone());
    }

    fn ensure_cursor(
        &mut self,
        path: &PathBuf,
        start_offset: u64,
        batch_size: usize,
    ) -> Result<()> {
        // Preserve forward-only semantics even after cursor eviction.
        if let Some(&high_water) = self.cursor_high_water.get(path) {
            if start_offset < high_water {
                return invalid(format!(
                    "backward canonical cursor reopening: \
                 file={}, requested={}, high_water={}",
                    path.display(),
                    start_offset,
                    high_water
                ));
            }
        }

        if self.forward_cursors.contains_key(path) {
            self.touch_cursor(path);
            return Ok(());
        }

        // Create the replacement before evicting an existing cursor.
        // This avoids losing a working cursor if opening fails.
        let cursor = CanonicalForwardCursor::new(path, start_offset, batch_size)?;

        if self.forward_cursors.len() >= self.max_open_cursors {
            let oldest = self
                .cursor_lru
                .pop_front()
                .ok_or_else(|| invalid_error("cursor LRU is empty"))?;

            self.forward_cursors.remove(&oldest);
            self.cursor_evictions += 1;
        }

        self.forward_cursors.insert(path.clone(), cursor);

        self.cursor_openings += 1;

        self.peak_open_cursors = self.peak_open_cursors.max(self.forward_cursors.len());

        self.touch_cursor(path);

        Ok(())
    }
    pub fn register_row_groups(&mut self, index: CanonicalRowGroupIndex) -> Result<()> {
        let path = index.path().to_path_buf();

        if self.row_groups.contains_key(&path) {
            return invalid(format!("duplicate row-group index: {}", path.display()));
        }

        self.row_groups.insert(path, index);

        Ok(())
    }
    pub fn cursor_openings(&self) -> u64 {
        self.cursor_openings
    }

    pub fn cursor_evictions(&self) -> u64 {
        self.cursor_evictions
    }

    pub fn peak_open_cursors(&self) -> usize {
        self.peak_open_cursors
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
        let batch_size = self.reader.batch_size();

        // BTreeMap iteration is deterministic.
        // Physical row-group order does not determine output event order.
        for (key, reads) in &plan.groups {
            if std::env::var_os("MARKETFORGE_PROFILE").is_some() {
                self.profile_group_requests += 1;

                if !self
                    .profile_seen_groups
                    .insert((key.path.clone(), key.row_group))
                {
                    self.profile_repeated_groups += 1;
                }
            }
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
            let group_start = group.row_start;

            // -------------------------------------------------------------------------
            // Resolve canonical reads using persistent forward-only cursors
            // -------------------------------------------------------------------------

            let mut ordered_reads = reads.iter().collect::<Vec<_>>();

            ordered_reads.sort_unstable_by_key(|read| read.local_offset);

            let first_read = ordered_reads
                .first()
                .ok_or_else(|| invalid_error("empty planned row-group reads"))?;

            let first_offset = group_start
                .checked_add(first_read.local_offset)
                .ok_or_else(|| invalid_error("canonical offset overflow"))?;

            self.ensure_cursor(&key.path, first_offset, batch_size)?;

            let cursor = self
                .forward_cursors
                .get_mut(&key.path)
                .ok_or_else(|| invalid_error("missing forward cursor"))?;

            for read in ordered_reads {
                let event_slots = slots
                    .get_mut(read.event_index)
                    .ok_or_else(|| invalid_error("invalid event index"))?;

                let slot = event_slots
                    .get_mut(read.slice_index)
                    .ok_or_else(|| invalid_error("invalid slice index"))?;

                if slot.is_some() {
                    return invalid("duplicate planned canonical slice");
                }

                let absolute_offset = group_start
                    .checked_add(read.local_offset)
                    .ok_or_else(|| invalid_error("canonical offset overflow"))?;

                let slices = cursor.read_range(absolute_offset, read.row_count)?;
                let position = cursor.position();

                self.cursor_high_water.insert(key.path.clone(), position);
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
    pub fn profile_row_group_requests(&self) -> u64 {
        self.profile_group_requests
    }

    pub fn profile_repeated_row_groups(&self) -> u64 {
        self.profile_repeated_groups
    }

    pub fn profile_unique_row_groups(&self) -> usize {
        self.profile_seen_groups.len()
    }
}

// -----------------------------------------------------------------------------
// Extract exact rows from a sequence of Arrow batches
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
