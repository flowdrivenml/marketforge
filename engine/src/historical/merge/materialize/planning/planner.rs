use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::materialize::planning::row_groups::CanonicalRowGroupIndex,
    crate::historical::merge::materialize::readers::inventory::CanonicalRowIndex,
    crate::historical::merge::materialize::shared::resolver::CanonicalTaskKey,
    crate::historical::merge::output::writers::unified_timeline::UnifiedTimelineRecord,
};

// -----------------------------------------------------------------------------
// Planned canonical read
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedCanonicalRead {
    /// Original position in the chronological reference batch.
    pub event_index: usize,

    /// Position of this slice within the original canonical event.
    pub slice_index: usize,

    /// Row offset relative to the beginning of the Parquet row group.
    pub local_offset: u64,

    pub row_count: u64,
}

// -----------------------------------------------------------------------------
// Row-group identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PlannedRowGroupKey {
    pub path: PathBuf,
    pub row_group: usize,
}

// -----------------------------------------------------------------------------
// Grouped read plan
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GroupedReadPlan {
    /// References remain in their original chronological order.
    pub references: Vec<UnifiedTimelineRecord>,

    /// Canonical reads grouped by physical Parquet row group.
    pub groups: BTreeMap<PlannedRowGroupKey, Vec<PlannedCanonicalRead>>,

    /// Number of canonical slices required by each event.
    pub event_slice_counts: Vec<usize>,

    pub canonical_rows: u64,
}

impl GroupedReadPlan {
    pub fn event_count(&self) -> usize {
        self.references.len()
    }

    pub fn row_group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn read_count(&self) -> usize {
        self.groups.values().map(Vec::len).sum()
    }
}

// -----------------------------------------------------------------------------
// Planner
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct CanonicalReadPlanner {
    tasks: HashMap<CanonicalTaskKey, CanonicalRowIndex>,
    row_groups: HashMap<PathBuf, CanonicalRowGroupIndex>,
}

impl CanonicalReadPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    // -------------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------------

    pub fn register_task(&mut self, key: CanonicalTaskKey, index: CanonicalRowIndex) -> Result<()> {
        if key.event_kind > 1 {
            return invalid("unsupported canonical event kind");
        }

        if self.tasks.contains_key(&key) {
            return invalid(format!("duplicate canonical task registration: {key:?}"));
        }

        self.tasks.insert(key, index);

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

    // -------------------------------------------------------------------------
    // Planning
    // -------------------------------------------------------------------------

    pub fn plan(&self, references: &[UnifiedTimelineRecord]) -> Result<GroupedReadPlan> {
        let mut groups = BTreeMap::<PlannedRowGroupKey, Vec<PlannedCanonicalRead>>::new();

        let mut event_slice_counts = Vec::with_capacity(references.len());
        let mut canonical_rows = 0_u64;

        for (event_index, reference) in references.iter().enumerate() {
            let key = CanonicalTaskKey {
                dataset_id: reference.dataset_id,
                task_id: reference.task_id,
                event_kind: reference.event_kind,
            };

            let task = self
                .tasks
                .get(&key)
                .ok_or_else(|| invalid_error(format!("unregistered canonical task: {key:?}")))?;

            // Validate basic event cardinality.
            match reference.event_kind {
                0 if reference.canonical_row_count != 1 => {
                    return invalid("trade reference must contain exactly one row");
                }
                0 | 1 => {}
                _ => return invalid("unsupported event kind"),
            }

            let file_slices = task.resolve(
                reference.canonical_row_offset,
                reference.canonical_row_count,
            )?;

            let mut slice_index = 0_usize;
            let mut event_rows = 0_u64;

            for file_slice in file_slices {
                let index = self.row_groups.get(&file_slice.path).ok_or_else(|| {
                    invalid_error(format!(
                        "missing row-group index: {}",
                        file_slice.path.display()
                    ))
                })?;

                let row_group_slices =
                    index.resolve(file_slice.local_offset, file_slice.row_count)?;

                for slice in row_group_slices {
                    let group_key = PlannedRowGroupKey {
                        path: file_slice.path.clone(),
                        row_group: slice.group_index,
                    };

                    groups
                        .entry(group_key)
                        .or_default()
                        .push(PlannedCanonicalRead {
                            event_index,
                            slice_index,
                            local_offset: slice.local_offset,
                            row_count: slice.row_count,
                        });

                    event_rows = event_rows
                        .checked_add(slice.row_count)
                        .ok_or_else(|| invalid_error("event row-count overflow"))?;

                    slice_index = slice_index
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("event slice-count overflow"))?;
                }
            }

            if event_rows != reference.canonical_row_count {
                return invalid(format!(
                    "incomplete event planning: event={}, expected={}, planned={}",
                    event_index, reference.canonical_row_count, event_rows
                ));
            }

            canonical_rows = canonical_rows
                .checked_add(event_rows)
                .ok_or_else(|| invalid_error("planned row-count overflow"))?;

            event_slice_counts.push(slice_index);
        }

        Ok(GroupedReadPlan {
            references: references.to_vec(),
            groups,
            event_slice_counts,
            canonical_rows,
        })
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
