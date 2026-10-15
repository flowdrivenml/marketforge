use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::Sender,
    },
    time::{Duration, Instant},
};

// -----------------------------------------------------------------------------
// Merge phases
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergePhase {
    InputResolution,
    Inventory,
    BoundaryReconciliation,
    PartitionPlanning,
    LogicalMerge,
    LogicalValidation,
    MaterializationPlanning,
    PhysicalMaterialization,
    PhysicalValidation,
    Checkpoints,
    Manifest,
    Commit,
    PublicationPreparation,
}

impl MergePhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::InputResolution => "INPUT RESOLUTION",
            Self::Inventory => "SOURCE INVENTORY",
            Self::BoundaryReconciliation => "BOUNDARY RECONCILIATION",
            Self::PartitionPlanning => "PARTITION PLANNING",
            Self::LogicalMerge => "LOGICAL MERGE",
            Self::LogicalValidation => "LOGICAL VALIDATION",
            Self::MaterializationPlanning => "MATERIALIZATION PLANNING",
            Self::PhysicalMaterialization => "PHYSICAL MATERIALIZATION",
            Self::PhysicalValidation => "PHYSICAL VALIDATION",
            Self::Checkpoints => "CHECKPOINTS",
            Self::Manifest => "MANIFEST",
            Self::Commit => "TRANSACTIONAL COMMIT",
            Self::PublicationPreparation => "PUBLICATION PREPARATION",
        }
    }
}

// -----------------------------------------------------------------------------
// Partition identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct PartitionKey {
    pub phase: MergePhase,
    pub partition_id: u32,
}

// -----------------------------------------------------------------------------
// Progress events
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum MergeProgressEvent {
    PhaseStarted {
        phase: MergePhase,
    },

    PhaseCompleted {
        phase: MergePhase,
    },

    PartitionsPlanned {
        phase: MergePhase,
        count: usize,
    },

    PartitionStarted {
        key: PartitionKey,
    },

    PartitionCompleted {
        key: PartitionKey,
        events: u64,
    },

    PartitionFailed {
        key: PartitionKey,
        message: String,
    },

    Finished {
        success: bool,
    },
    ValidationStarted {
        total_partitions: usize,
        total_files: u64,
        expected_events: u64,
    },

    ValidationProgress {
        partitions_completed: usize,
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    },

    ValidationCompleted {
        partitions_completed: usize,
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    },
}

// -----------------------------------------------------------------------------
// Atomic partition counters
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct PartitionProgressCounters {
    events: AtomicU64,
}

impl PartitionProgressCounters {
    pub fn update(&self, events: u64) {
        self.events.store(events, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> PartitionProgress {
        PartitionProgress {
            events: self.events.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartitionProgress {
    pub events: u64,
}

// -----------------------------------------------------------------------------
// Shared progress state
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct MergeProgressState {
    partitions: Mutex<HashMap<PartitionKey, Arc<PartitionProgressCounters>>>,
}

impl MergeProgressState {
    pub fn register(&self, key: PartitionKey) -> Arc<PartitionProgressCounters> {
        let mut partitions = self.partitions.lock().unwrap();

        partitions
            .entry(key)
            .or_insert_with(|| Arc::new(PartitionProgressCounters::default()))
            .clone()
    }

    pub fn snapshots(&self) -> HashMap<PartitionKey, PartitionProgress> {
        let partitions = self.partitions.lock().unwrap();

        partitions
            .iter()
            .map(|(&key, counters)| (key, counters.snapshot()))
            .collect()
    }
}

// -----------------------------------------------------------------------------
// Progress reporter
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct MergeProgressReporter {
    sender: Option<Sender<MergeProgressEvent>>,
    state: Arc<MergeProgressState>,
}

impl MergeProgressReporter {
    pub fn new(sender: Sender<MergeProgressEvent>) -> Self {
        Self {
            sender: Some(sender),
            state: Arc::new(MergeProgressState::default()),
        }
    }
    // -----------------------------------------------------------------------------
    // Physical validation progress
    // -----------------------------------------------------------------------------

    pub fn validation_started(
        &self,
        total_partitions: usize,
        total_files: u64,
        expected_events: u64,
    ) {
        self.emit(MergeProgressEvent::ValidationStarted {
            total_partitions,
            total_files,
            expected_events,
        });
    }

    pub fn validation_progress(
        &self,
        partitions_completed: usize,
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    ) {
        self.emit(MergeProgressEvent::ValidationProgress {
            partitions_completed,
            files_completed,
            events_validated,
            row_groups_validated,
        });
    }

    pub fn validation_completed(
        &self,
        partitions_completed: usize,
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    ) {
        self.emit(MergeProgressEvent::ValidationCompleted {
            partitions_completed,
            files_completed,
            events_validated,
            row_groups_validated,
        });
    }
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_enabled(&self) -> bool {
        self.sender.is_some()
    }

    pub fn state(&self) -> Arc<MergeProgressState> {
        Arc::clone(&self.state)
    }

    pub fn emit(&self, event: MergeProgressEvent) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(event);
        }
    }

    pub fn phase_started(&self, phase: MergePhase) {
        self.emit(MergeProgressEvent::PhaseStarted { phase });
    }

    pub fn phase_completed(&self, phase: MergePhase) {
        self.emit(MergeProgressEvent::PhaseCompleted { phase });
    }

    pub fn partitions_planned(&self, phase: MergePhase, count: usize) {
        self.emit(MergeProgressEvent::PartitionsPlanned { phase, count });
    }

    pub fn partition_started(&self, key: PartitionKey) {
        if self.is_enabled() {
            self.state.register(key);
        }

        self.emit(MergeProgressEvent::PartitionStarted { key });
    }

    pub fn partition_completed(&self, key: PartitionKey, events: u64) {
        if self.is_enabled() {
            self.state.register(key).update(events);
        }

        self.emit(MergeProgressEvent::PartitionCompleted { key, events });
    }

    pub fn partition_failed(&self, key: PartitionKey, message: String) {
        self.emit(MergeProgressEvent::PartitionFailed { key, message });
    }

    pub fn finished(&self, success: bool) {
        self.emit(MergeProgressEvent::Finished { success });
    }
}

// -----------------------------------------------------------------------------
// Per-partition progress handle
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct MergeWorkerProgress {
    counters: Option<Arc<PartitionProgressCounters>>,
    last_update: Instant,
    last_checked_events: u64,
}

impl MergeWorkerProgress {
    pub fn new(reporter: &MergeProgressReporter, key: PartitionKey) -> Self {
        let counters = if reporter.is_enabled() {
            Some(reporter.state.register(key))
        } else {
            None
        };

        Self {
            counters,
            last_update: Instant::now(),
            last_checked_events: 0,
        }
    }

    pub fn maybe_update(&mut self, events: u64) {
        let Some(counters) = &self.counters else {
            return;
        };

        if events.saturating_sub(self.last_checked_events) < 8192 {
            return;
        }

        self.last_checked_events = events;

        if self.last_update.elapsed() < Duration::from_millis(500) {
            return;
        }

        counters.update(events);
        self.last_update = Instant::now();
    }

    pub fn finish(&self, events: u64) {
        if let Some(counters) = &self.counters {
            counters.update(events);
        }
    }
}
