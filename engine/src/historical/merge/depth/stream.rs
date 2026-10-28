use std::collections::HashMap;

use crate::historical::merge::core::source::DepthEventSource;
use crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization;
use {
    crate::historical::merge::depth::boundary::DepthBoundaryLink,
    crate::historical::merge::depth::boundary::MergeBoundaryRelation,
    crate::historical::merge::depth::cursor::DepthEventCursor,
    crate::historical::merge::depth::inventory::DepthTaskInventory,
};
use {
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
    crate::shared::formats::depth::DepthEventBoundary,
    crate::shared::persistence::parquet::IndexedDepthEvent,
};
// -----------------------------------------------------------------------------
// Logical depth event
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DepthEventRef {
    pub dataset_id: u64,
    pub task_id: u64,
    pub event: IndexedDepthEvent,
}

// -----------------------------------------------------------------------------
// Depth stream
// -----------------------------------------------------------------------------

pub struct DepthStream {
    dataset_id: u64,

    tasks: Vec<DepthTaskInventory>,
    boundary_relations: HashMap<(u64, u64), MergeBoundaryRelation>,

    current_task: usize,
    current_cursor: Option<DepthEventCursor>,

    previous_timestamp_ns: Option<i64>,
    previous_sequence_end: Option<u64>,

    events_emitted: u64,
    initializations_skipped: u64,
    skipped_initializations: Vec<MergeSkippedInitialization>,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl DepthStream {
    pub fn new(
        dataset_id: u64,
        mut tasks: Vec<DepthTaskInventory>,
        boundaries: &[DepthBoundaryLink],
    ) -> Result<Self> {
        if tasks.is_empty() {
            return invalid(format!(
                "dataset {dataset_id}: cannot create an empty depth stream"
            ));
        }

        if tasks.iter().any(|task| task.dataset_id != dataset_id) {
            return invalid(format!(
                "dataset {dataset_id}: depth stream contains tasks from another dataset"
            ));
        }

        // Sort tasks chronologically using their recorded first timestamps.
        // Task IDs provide deterministic tie-breaking.
        tasks.sort_by_key(|task| {
            (
                task.boundary.first_timestamp_ns.unwrap_or(i64::MAX),
                task.task_id,
            )
        });

        // Reject duplicate task IDs.
        for pair in tasks.windows(2) {
            if pair[0].task_id == pair[1].task_id {
                return invalid(format!(
                    "dataset {dataset_id}: duplicate task ID {}",
                    pair[0].task_id
                ));
            }
        }

        // Index the previously validated boundary relationships.
        let mut boundary_relations = HashMap::new();

        for link in boundaries {
            if link.dataset_id != dataset_id {
                continue;
            }

            let key = (link.previous_task_id, link.next_task_id);

            if boundary_relations.insert(key, link.relation).is_some() {
                return invalid(format!(
                    "dataset {dataset_id}: duplicate boundary relationship {} -> {}",
                    key.0, key.1
                ));
            }
        }

        // Every adjacent task pair must have a reconciliation result.
        for pair in tasks.windows(2) {
            let key = (pair[0].task_id, pair[1].task_id);

            if !boundary_relations.contains_key(&key) {
                return invalid(format!(
                    "dataset {dataset_id}: missing boundary relationship {} -> {}",
                    key.0, key.1
                ));
            }
        }

        Ok(Self {
            dataset_id,
            tasks,
            boundary_relations,

            current_task: 0,
            current_cursor: None,

            previous_timestamp_ns: None,
            previous_sequence_end: None,

            events_emitted: 0,
            initializations_skipped: 0,
            skipped_initializations: Vec::new(),
        })
    }

    // -------------------------------------------------------------------------
    // Public metrics
    // -------------------------------------------------------------------------

    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }

    pub fn initializations_skipped(&self) -> u64 {
        self.initializations_skipped
    }

    pub fn dataset_id(&self) -> u64 {
        self.dataset_id
    }
    pub fn skipped_initializations(&self) -> &[MergeSkippedInitialization] {
        &self.skipped_initializations
    }

    // -------------------------------------------------------------------------
    // Streaming
    // -------------------------------------------------------------------------

    pub fn next_event(&mut self) -> Result<Option<DepthEventRef>> {
        loop {
            // -------------------------------------------------------------
            // End of stream
            // -------------------------------------------------------------

            if self.current_task >= self.tasks.len() {
                return Ok(None);
            }

            // -------------------------------------------------------------
            // Open current task
            // -------------------------------------------------------------

            if self.current_cursor.is_none() {
                let task_id = self.tasks[self.current_task].task_id;
                let events_path = self.tasks[self.current_task].events_path.clone();

                let mut cursor = DepthEventCursor::open(&events_path)?;

                // The first event of a task must be an initialization.
                let first = cursor.next_event()?.ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "dataset {} task {}: empty depth event index",
                        self.dataset_id, task_id
                    ))
                })?;

                if first.event_boundary != DepthEventBoundary::Initialization {
                    return invalid(format!(
                        "dataset {} task {}: first event is not an initialization",
                        self.dataset_id, task_id
                    ));
                }

                // ---------------------------------------------------------
                // Handle archive boundary
                // ---------------------------------------------------------

                let skip_initialization = if self.current_task == 0 {
                    false
                } else {
                    let previous = &self.tasks[self.current_task - 1];

                    let relation = self
                        .boundary_relations
                        .get(&(previous.task_id, task_id))
                        .copied()
                        .ok_or_else(|| {
                            MarketForgeError::InvalidConfiguration(format!(
                                "dataset {}: missing boundary relation {} -> {}",
                                self.dataset_id, previous.task_id, task_id
                            ))
                        })?;

                    match relation {
                        MergeBoundaryRelation::VerifiedContinuation => {
                            // The next task's initial book matches the
                            // preceding task's final reconstructed state.
                            true
                        }

                        MergeBoundaryRelation::IndependentInitialization
                        | MergeBoundaryRelation::Unverifiable
                        | MergeBoundaryRelation::SequenceGap => false,

                        MergeBoundaryRelation::StateMismatch => {
                            return invalid(format!(
                                "dataset {}: state mismatch between tasks {} and {}",
                                self.dataset_id, previous.task_id, task_id
                            ));
                        }
                    }
                };

                self.current_cursor = Some(cursor);

                if skip_initialization {
                    self.initializations_skipped += 1;

                    self.skipped_initializations
                        .push(MergeSkippedInitialization {
                            dataset_id: self.dataset_id,
                            task_id,

                            event_ordinal: first.event_ordinal,
                            canonical_row_offset: first.canonical_row_offset,
                            canonical_row_count: first.canonical_row_count,

                            timestamp_ns: first.event_timestamp_ns,
                        });
                } else {
                    self.validate_ordering(&first)?;

                    self.events_emitted += 1;

                    return Ok(Some(DepthEventRef {
                        dataset_id: self.dataset_id,
                        task_id: task_id,
                        event: first,
                    }));
                }

                continue;
            }

            // -------------------------------------------------------------
            // Read next event
            // -------------------------------------------------------------

            let event = self
                .current_cursor
                .as_mut()
                .expect("cursor must be initialized")
                .next_event()?;

            match event {
                Some(event) => {
                    self.validate_ordering(&event)?;

                    self.events_emitted += 1;

                    return Ok(Some(DepthEventRef {
                        dataset_id: self.dataset_id,
                        task_id: self.tasks[self.current_task].task_id,
                        event,
                    }));
                }

                None => {
                    self.current_cursor = None;
                    self.current_task += 1;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Ordering validation
    // -------------------------------------------------------------------------

    fn validate_ordering(&mut self, event: &IndexedDepthEvent) -> Result<()> {
        if let Some(previous) = self.previous_timestamp_ns {
            if event.event_timestamp_ns < previous {
                return invalid(format!(
                    "dataset {}: timestamp regression during depth stream replay: previous={}, current={}",
                    self.dataset_id, previous, event.event_timestamp_ns,
                ));
            }
        }

        // Sequence ordering is meaningful only when both events have
        // native sequence metadata.
        if let (Some(previous), Some(current)) = (self.previous_sequence_end, event.sequence_start)
        {
            if current < previous {
                return invalid(format!(
                    "dataset {}: sequence regression during depth stream replay: previous={}, current={}",
                    self.dataset_id, previous, current,
                ));
            }
        }

        self.previous_timestamp_ns = Some(event.event_timestamp_ns);
        self.previous_sequence_end = event.sequence_end;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}

// -----------------------------------------------------------------------------
// Depth event source implementation
// -----------------------------------------------------------------------------

impl DepthEventSource for DepthStream {
    fn next_event(&mut self) -> Result<Option<DepthEventRef>> {
        DepthStream::next_event(self)
    }

    fn skipped_initializations(&self) -> &[MergeSkippedInitialization] {
        DepthStream::skipped_initializations(self)
    }
}
