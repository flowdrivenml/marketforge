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
// Conversion phases
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConversionPhase {
    SourceValidation,
    PartitionPlanning,
    ReferencePrepass,
    PhysicalConversion,
    ExecutionReceipt,
    PhysicalValidation,
    ReceiptReconciliation,
    SourcePreservation,
    ManifestConstruction,
    PublicationPreparation,
    Commit,
}

impl ConversionPhase {
    pub fn label(self) -> &'static str {
        match self {
            Self::SourceValidation => "SOURCE VALIDATION",
            Self::PartitionPlanning => "PARTITION PLANNING",
            Self::ReferencePrepass => "REFERENCE PREPASS",
            Self::PhysicalConversion => "PHYSICAL CONVERSION",
            Self::ExecutionReceipt => "EXECUTION RECEIPT",
            Self::PhysicalValidation => "PHYSICAL VALIDATION",
            Self::ReceiptReconciliation => "RECEIPT RECONCILIATION",
            Self::SourcePreservation => "SOURCE PRESERVATION",
            Self::ManifestConstruction => "MANIFEST CONSTRUCTION",
            Self::PublicationPreparation => "PUBLICATION PREPARATION",
            Self::Commit => "TRANSACTIONAL COMMIT",
        }
    }
}

// -----------------------------------------------------------------------------
// Conversion type
// -----------------------------------------------------------------------------
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionKind {
    Trades,
    Depth,
    Combined,
}

#[allow(dead_code)]
impl ConversionKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Trades => "TRADES",
            Self::Depth => "DEPTH",
            Self::Combined => "COMBINED",
        }
    }
}

// -----------------------------------------------------------------------------
// Partition identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConversionPartitionKey {
    pub partition_id: u32,
}

// -----------------------------------------------------------------------------
// Progress counters
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct ConversionProgress {
    pub events_read: u64,
    pub events_written: u64,

    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,
    pub failures: [u64; 6],
    pub skipped_events: u64,

    pub trade_events: u64,
    pub depth_events: u64,

    pub depth_levels_read: u64,
    pub depth_levels_converted: u64,
    pub depth_levels_unavailable: u64,
    pub depth_levels_skipped: u64,

    pub internal_reference_updates: u64,
    pub external_reference_updates: u64,

    pub files_written: u64,
    pub row_groups_written: u64,
}

// -----------------------------------------------------------------------------
// Atomic partition counters
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ConversionProgressCounters {
    events_read: AtomicU64,
    events_written: AtomicU64,

    identity_events: AtomicU64,
    converted_events: AtomicU64,
    unavailable_events: AtomicU64,
    failures: [AtomicU64; 6],
    skipped_events: AtomicU64,

    trade_events: AtomicU64,
    depth_events: AtomicU64,

    depth_levels_read: AtomicU64,
    depth_levels_converted: AtomicU64,
    depth_levels_unavailable: AtomicU64,
    depth_levels_skipped: AtomicU64,

    internal_reference_updates: AtomicU64,
    external_reference_updates: AtomicU64,

    files_written: AtomicU64,
    row_groups_written: AtomicU64,
}

impl ConversionProgressCounters {
    pub fn update(&self, progress: &ConversionProgress) {
        macro_rules! store {
            ($field:ident) => {
                self.$field.store(progress.$field, Ordering::Relaxed);
            };
        }

        store!(events_read);
        store!(events_written);

        store!(identity_events);
        store!(converted_events);
        store!(unavailable_events);
        for (counter, value) in self.failures.iter().zip(progress.failures) {
            counter.store(value, Ordering::Relaxed);
        }
        store!(skipped_events);

        store!(trade_events);
        store!(depth_events);

        store!(depth_levels_read);
        store!(depth_levels_converted);
        store!(depth_levels_unavailable);
        store!(depth_levels_skipped);

        store!(internal_reference_updates);
        store!(external_reference_updates);

        store!(files_written);
        store!(row_groups_written);
    }

    pub fn snapshot(&self) -> ConversionProgress {
        macro_rules! load {
            ($field:ident) => {
                self.$field.load(Ordering::Relaxed)
            };
        }

        ConversionProgress {
            events_read: load!(events_read),
            events_written: load!(events_written),

            identity_events: load!(identity_events),
            converted_events: load!(converted_events),
            unavailable_events: load!(unavailable_events),
            failures: std::array::from_fn(|index| self.failures[index].load(Ordering::Relaxed)),
            skipped_events: load!(skipped_events),

            trade_events: load!(trade_events),
            depth_events: load!(depth_events),

            depth_levels_read: load!(depth_levels_read),
            depth_levels_converted: load!(depth_levels_converted),
            depth_levels_unavailable: load!(depth_levels_unavailable),
            depth_levels_skipped: load!(depth_levels_skipped),

            internal_reference_updates: load!(internal_reference_updates),
            external_reference_updates: load!(external_reference_updates),

            files_written: load!(files_written),
            row_groups_written: load!(row_groups_written),
        }
    }
}

// -----------------------------------------------------------------------------
// Shared progress state
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ConversionProgressState {
    interrupted: std::sync::atomic::AtomicBool,
    partitions: Mutex<HashMap<ConversionPartitionKey, Arc<ConversionProgressCounters>>>,
}

impl ConversionProgressState {
    pub fn interrupt(&self) {
        self.interrupted
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn register(&self, key: ConversionPartitionKey) -> Arc<ConversionProgressCounters> {
        let mut partitions = self.partitions.lock().unwrap();

        partitions
            .entry(key)
            .or_insert_with(|| Arc::new(ConversionProgressCounters::default()))
            .clone()
    }

    pub fn total(&self) -> ConversionProgress {
        let snapshots = self.snapshots();

        let mut total = ConversionProgress::default();

        for progress in snapshots.values() {
            total.events_read += progress.events_read;
            total.events_written += progress.events_written;

            total.identity_events += progress.identity_events;
            total.converted_events += progress.converted_events;
            total.unavailable_events += progress.unavailable_events;
            for (total, value) in total.failures.iter_mut().zip(progress.failures) {
                *total += value;
            }
            total.skipped_events += progress.skipped_events;

            total.trade_events += progress.trade_events;
            total.depth_events += progress.depth_events;

            total.depth_levels_read += progress.depth_levels_read;
            total.depth_levels_converted += progress.depth_levels_converted;
            total.depth_levels_unavailable += progress.depth_levels_unavailable;
            total.depth_levels_skipped += progress.depth_levels_skipped;

            total.internal_reference_updates += progress.internal_reference_updates;
            total.external_reference_updates += progress.external_reference_updates;

            total.files_written += progress.files_written;
            total.row_groups_written += progress.row_groups_written;
        }

        total
    }
    pub fn snapshots(&self) -> HashMap<ConversionPartitionKey, ConversionProgress> {
        let partitions = self.partitions.lock().unwrap();

        partitions
            .iter()
            .map(|(&key, counters)| (key, counters.snapshot()))
            .collect()
    }
}

// -----------------------------------------------------------------------------
// Progress events
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ConversionProgressEvent {
    PhaseStarted {
        phase: ConversionPhase,
    },

    PhaseCompleted {
        phase: ConversionPhase,
    },

    PartitionsPlanned {
        count: usize,
    },
    ReferencePrepassProgress {
        events_scanned: u64,
        total_events: u64,
        checkpoints_created: usize,
        total_checkpoints: usize,
    },

    PartitionStarted {
        key: ConversionPartitionKey,
    },

    PartitionCompleted {
        key: ConversionPartitionKey,
        metrics: ConversionProgress,
    },

    PartitionFailed {
        key: ConversionPartitionKey,
        message: String,
    },

    ValidationStarted {
        total_files: u64,
        expected_events: u64,
    },

    ValidationProgress {
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    },

    ValidationCompleted {
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    },
    SourcePreservationProgress {
        events_compared: u64,
        total_events: u64,
    },
    Finished(ConversionOutcome),
}

// -----------------------------------------------------------------------------
// Progress reporter
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ConversionProgressReporter {
    sender: Option<Sender<ConversionProgressEvent>>,
    state: Arc<ConversionProgressState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversionOutcome {
    Validated,
    Materialized,
    Published,
    Failed(String),
    Interrupted,
}

impl ConversionOutcome {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Validated => "VALIDATED",
            Self::Materialized => "MATERIALIZED (UNPUBLISHED)",
            Self::Published => "PUBLISHED",
            Self::Failed(_) => "FAILED",
            Self::Interrupted => "INTERRUPTED",
        }
    }

    pub fn succeeded(&self) -> bool {
        matches!(self, Self::Validated | Self::Materialized | Self::Published)
    }
}

impl ConversionProgressReporter {
    pub fn check_interrupted(&self) -> crate::shared::error::Result<()> {
        if self.state.is_interrupted() {
            return Err(crate::shared::error::MarketForgeError::ConversionInterrupted);
        }
        Ok(())
    }

    pub fn new(sender: Sender<ConversionProgressEvent>) -> Self {
        Self {
            sender: Some(sender),
            ..Self::default()
        }
    }

    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_enabled(&self) -> bool {
        self.sender.is_some()
    }

    pub fn state(&self) -> Arc<ConversionProgressState> {
        Arc::clone(&self.state)
    }

    pub fn emit(&self, event: ConversionProgressEvent) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(event);
        }
    }

    pub fn phase_started(&self, phase: ConversionPhase) {
        self.emit(ConversionProgressEvent::PhaseStarted { phase });
    }

    pub fn phase_completed(&self, phase: ConversionPhase) {
        self.emit(ConversionProgressEvent::PhaseCompleted { phase });
    }

    pub fn partitions_planned(&self, count: usize) {
        self.emit(ConversionProgressEvent::PartitionsPlanned { count });
    }
    pub fn reference_prepass_progress(
        &self,
        events_scanned: u64,
        total_events: u64,
        checkpoints_created: usize,
        total_checkpoints: usize,
    ) {
        self.emit(ConversionProgressEvent::ReferencePrepassProgress {
            events_scanned,
            total_events,
            checkpoints_created,
            total_checkpoints,
        });
    }

    pub fn partition_started(&self, key: ConversionPartitionKey) {
        if self.is_enabled() {
            self.state.register(key);
        }

        self.emit(ConversionProgressEvent::PartitionStarted { key });
    }

    pub fn partition_completed(&self, key: ConversionPartitionKey, metrics: ConversionProgress) {
        if self.is_enabled() {
            self.state.register(key).update(&metrics);
        }

        self.emit(ConversionProgressEvent::PartitionCompleted { key, metrics });
    }

    pub fn partition_failed(&self, key: ConversionPartitionKey, message: String) {
        self.emit(ConversionProgressEvent::PartitionFailed { key, message });
    }

    pub fn validation_started(&self, total_files: u64, expected_events: u64) {
        self.emit(ConversionProgressEvent::ValidationStarted {
            total_files,
            expected_events,
        });
    }

    pub fn validation_progress(
        &self,
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    ) {
        self.emit(ConversionProgressEvent::ValidationProgress {
            files_completed,
            events_validated,
            row_groups_validated,
        });
    }

    pub fn validation_completed(
        &self,
        files_completed: u64,
        events_validated: u64,
        row_groups_validated: u64,
    ) {
        self.emit(ConversionProgressEvent::ValidationCompleted {
            files_completed,
            events_validated,
            row_groups_validated,
        });
    }
    pub fn source_preservation_progress(&self, events_compared: u64, total_events: u64) {
        self.emit(ConversionProgressEvent::SourcePreservationProgress {
            events_compared,
            total_events,
        });
    }
    pub fn finished(&self, outcome: ConversionOutcome) {
        self.emit(ConversionProgressEvent::Finished(outcome));
    }
}

// -----------------------------------------------------------------------------
// Per-partition progress handle
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ConversionWorkerProgress {
    reporter: ConversionProgressReporter,
    counters: Option<Arc<ConversionProgressCounters>>,
    last_update: Instant,
    last_checked_events: u64,
}

impl ConversionWorkerProgress {
    pub fn check_interrupted(&self) -> crate::shared::error::Result<()> {
        self.reporter.check_interrupted()
    }

    pub fn new(reporter: &ConversionProgressReporter, key: ConversionPartitionKey) -> Self {
        let counters = if reporter.is_enabled() {
            Some(reporter.state.register(key))
        } else {
            None
        };

        Self {
            reporter: reporter.clone(),
            counters,
            last_update: Instant::now(),
            last_checked_events: 0,
        }
    }

    pub fn maybe_update(&mut self, progress: &ConversionProgress) {
        let Some(counters) = &self.counters else {
            return;
        };

        if progress
            .events_read
            .saturating_sub(self.last_checked_events)
            < 8192
        {
            return;
        }

        self.last_checked_events = progress.events_read;

        if self.last_update.elapsed() < Duration::from_millis(500) {
            return;
        }

        counters.update(progress);
        self.last_update = Instant::now();
    }

    pub fn finish(&self, progress: &ConversionProgress) {
        if let Some(counters) = &self.counters {
            counters.update(progress);
        }
    }
}
