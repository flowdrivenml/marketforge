use std::collections::{HashMap, HashSet};

use super::{
    boundary::{DepthBoundaryLink, MergeBoundaryRelation},
    depth_cursor::DepthEventCursor,
    depth_stream::DepthEventRef,
    inventory::DepthTaskInventory,
    partition::MergePartition,
};
use super::{manifest::MergeSkippedInitialization, source::DepthEventSource};
use crate::{
    error::{MarketForgeError, Result},
    formats::depth::DepthEventBoundary,
};

// -----------------------------------------------------------------------------
// Partition depth stream
// -----------------------------------------------------------------------------

pub struct PartitionDepthStream {
    dataset_id: u64,
    partition: MergePartition,

    tasks: Vec<DepthTaskInventory>,

    boundary_relations: HashMap<(u64, u64), MergeBoundaryRelation>,
    preceding_tasks: HashMap<u64, u64>,

    current_task: usize,
    current_cursor: Option<DepthEventCursor>,

    previous_timestamp_ns: Option<i64>,

    events_emitted: u64,
    initializations_skipped: u64,
    skipped_initializations: Vec<MergeSkippedInitialization>,
    timestamp_regressions_dropped: u64,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl PartitionDepthStream {
    pub fn new(
        dataset_id: u64,
        partition: MergePartition,
        mut tasks: Vec<DepthTaskInventory>,
        boundaries: &[DepthBoundaryLink],
    ) -> Result<Self> {
        if tasks.iter().any(|task| task.dataset_id != dataset_id) {
            return invalid(format!(
                "dataset {dataset_id}: partition contains tasks from another dataset"
            ));
        }

        // Preserve deterministic source-task ordering.
        tasks.sort_by_key(|task| {
            (
                task.boundary.first_timestamp_ns.unwrap_or(i64::MAX),
                task.task_id,
            )
        });

        let mut seen = HashSet::new();

        for task in &tasks {
            if !seen.insert(task.task_id) {
                return invalid(format!(
                    "dataset {dataset_id}: duplicate task ID {}",
                    task.task_id
                ));
            }
        }

        // Boundary metadata must describe the complete dataset,
        // not only the tasks selected for this partition.
        let mut boundary_relations = HashMap::new();
        let mut preceding_tasks = HashMap::new();

        for link in boundaries {
            if link.dataset_id != dataset_id {
                continue;
            }

            let key = (link.previous_task_id, link.next_task_id);

            if boundary_relations.insert(key, link.relation).is_some() {
                return invalid(format!(
                    "dataset {dataset_id}: duplicate boundary {} -> {}",
                    key.0, key.1
                ));
            }

            if preceding_tasks
                .insert(link.next_task_id, link.previous_task_id)
                .is_some()
            {
                return invalid(format!(
                    "dataset {dataset_id}: multiple predecessors for task {}",
                    link.next_task_id
                ));
            }
        }

        Ok(Self {
            dataset_id,
            partition,
            tasks,
            boundary_relations,
            preceding_tasks,

            current_task: 0,
            current_cursor: None,

            previous_timestamp_ns: None,

            events_emitted: 0,
            initializations_skipped: 0,
            skipped_initializations: Vec::new(),
            timestamp_regressions_dropped: 0,
        })
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn dataset_id(&self) -> u64 {
        self.dataset_id
    }

    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }

    pub fn initializations_skipped(&self) -> u64 {
        self.initializations_skipped
    }
    pub fn timestamp_regressions_dropped(&self) -> u64 {
        self.timestamp_regressions_dropped
    }

    // -------------------------------------------------------------------------
    // Event streaming
    // -------------------------------------------------------------------------

    pub fn next_event(&mut self) -> Result<Option<DepthEventRef>> {
        loop {
            if self.current_task >= self.tasks.len() {
                return Ok(None);
            }

            // -------------------------------------------------------------
            // Open current task using indexed range selection
            // -------------------------------------------------------------

            if self.current_cursor.is_none() {
                let task = &self.tasks[self.current_task];

                let cursor = DepthEventCursor::open_indexed_range(
                    &task.events_path,
                    self.partition.start_ns,
                    self.partition.end_ns,
                )?;

                self.current_cursor = Some(cursor);
            }

            // -------------------------------------------------------------
            // Read next event
            // -------------------------------------------------------------

            let event = self
                .current_cursor
                .as_mut()
                .expect("cursor initialized")
                .next_event()?;

            let Some(event) = event else {
                self.current_cursor = None;
                self.current_task += 1;
                continue;
            };

            let task_id = self.tasks[self.current_task].task_id;

            // -----------------------------------------------------------------
            // Initialization handling
            // -----------------------------------------------------------------

            // Only the first initialization of a source task can represent
            // a redundant archive checkpoint.
            //
            // Later initialization events may represent genuine book resets
            // and must always be preserved.

            if event.event_boundary == DepthEventBoundary::Initialization
                && event.canonical_row_offset == 0
                && self.should_skip_initialization(task_id)?
            {
                self.initializations_skipped = self
                    .initializations_skipped
                    .checked_add(1)
                    .ok_or_else(|| invalid_error("skipped initialization counter overflow"))?;

                self.skipped_initializations
                    .push(MergeSkippedInitialization {
                        dataset_id: self.dataset_id,
                        task_id,

                        event_ordinal: event.event_ordinal,
                        canonical_row_offset: event.canonical_row_offset,
                        canonical_row_count: event.canonical_row_count,

                        timestamp_ns: event.event_timestamp_ns,
                    });

                continue;
            }

            // -------------------------------------------------------------
            // Chronological ordering
            // -------------------------------------------------------------

            if let Some(previous) = self.previous_timestamp_ns {
                if event.event_timestamp_ns < previous {
                    self.timestamp_regressions_dropped = self
                        .timestamp_regressions_dropped
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("timestamp regression counter overflow"))?;

                    continue;
                }
            }

            self.previous_timestamp_ns = Some(event.event_timestamp_ns);

            self.events_emitted = self
                .events_emitted
                .checked_add(1)
                .ok_or_else(|| invalid_error("partition event counter overflow"))?;

            return Ok(Some(DepthEventRef {
                dataset_id: self.dataset_id,
                task_id,
                event,
            }));
        }
    }

    // -------------------------------------------------------------------------
    // Boundary-aware initialization handling
    // -------------------------------------------------------------------------

    fn should_skip_initialization(&self, task_id: u64) -> Result<bool> {
        let Some(&previous_task_id) = self.preceding_tasks.get(&task_id) else {
            // First task in the complete dataset.
            return Ok(false);
        };

        let relation = self
            .boundary_relations
            .get(&(previous_task_id, task_id))
            .copied()
            .ok_or_else(|| {
                invalid_error(format!(
                    "dataset {}: missing boundary relation {} -> {}",
                    self.dataset_id, previous_task_id, task_id,
                ))
            })?;

        match relation {
            MergeBoundaryRelation::VerifiedContinuation => Ok(true),

            MergeBoundaryRelation::IndependentInitialization
            | MergeBoundaryRelation::Unverifiable
            | MergeBoundaryRelation::SequenceGap => Ok(false),

            MergeBoundaryRelation::StateMismatch => invalid(format!(
                "dataset {}: state mismatch between tasks {} and {}",
                self.dataset_id, previous_task_id, task_id,
            )),
        }
    }
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
// Depth event source implementation
// -----------------------------------------------------------------------------

impl DepthEventSource for PartitionDepthStream {
    fn next_event(&mut self) -> Result<Option<DepthEventRef>> {
        PartitionDepthStream::next_event(self)
    }

    fn skipped_initializations(&self) -> &[MergeSkippedInitialization] {
        &self.skipped_initializations
    }

    fn timestamp_regressions_dropped(&self) -> u64 {
        self.timestamp_regressions_dropped
    }
}
