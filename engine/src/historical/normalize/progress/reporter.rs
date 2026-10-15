use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::Sender,
    },
    time::{Duration, Instant},
};

use crate::historical::normalize::metrics::ProcessingCounters;

// -----------------------------------------------------------------------------
// Progress events
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProgressTaskId {
    pub job_id: u64,
    pub task_id: u64,
}

impl ProgressTaskId {
    pub const fn new(job_id: u64, task_id: u64) -> Self {
        Self { job_id, task_id }
    }
}
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    TaskStarted {
        id: ProgressTaskId,
        exchange: String,
        instrument: String,
        content_type: String,
        source: String,
    },

    TaskCompleted {
        id: ProgressTaskId,
        records_read: u64,
        records_written: u64,
        records_rejected: u64,
    },

    TaskFailed {
        id: ProgressTaskId,
        message: String,
    },

    Finalizing,

    Finished {
        success: bool,
    },
    TaskSkipped {
        id: ProgressTaskId,
        reason: String,
    },
}

// -----------------------------------------------------------------------------
// Atomic task counters
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct TaskProgressCounters {
    pub records_read: AtomicU64,
    pub records_written: AtomicU64,
    pub records_rejected: AtomicU64,
}

impl TaskProgressCounters {
    pub fn update(&self, counters: &ProcessingCounters) {
        self.records_read
            .store(counters.records_read, Ordering::Relaxed);

        self.records_written
            .store(counters.events_written, Ordering::Relaxed);

        self.records_rejected
            .store(counters.records_rejected, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ProcessingProgress {
        ProcessingProgress {
            records_read: self.records_read.load(Ordering::Relaxed),
            records_written: self.records_written.load(Ordering::Relaxed),
            records_rejected: self.records_rejected.load(Ordering::Relaxed),
        }
    }
}

// -----------------------------------------------------------------------------
// Processing progress snapshot
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ProcessingProgress {
    pub records_read: u64,
    pub records_written: u64,
    pub records_rejected: u64,
}

impl From<&ProcessingCounters> for ProcessingProgress {
    fn from(counters: &ProcessingCounters) -> Self {
        Self {
            records_read: counters.records_read,
            records_written: counters.events_written,
            records_rejected: counters.records_rejected,
        }
    }
}

// -----------------------------------------------------------------------------
// Shared progress state
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ProgressState {
    tasks: Mutex<HashMap<ProgressTaskId, Arc<TaskProgressCounters>>>,
}

impl ProgressState {
    pub fn register_task(&self, id: ProgressTaskId) -> Arc<TaskProgressCounters> {
        let mut tasks = self.tasks.lock().unwrap();

        tasks
            .entry(id)
            .or_insert_with(|| Arc::new(TaskProgressCounters::default()))
            .clone()
    }

    pub fn task_snapshot(&self, id: ProgressTaskId) -> Option<ProcessingProgress> {
        let tasks = self.tasks.lock().unwrap();

        tasks.get(&id).map(|counters| counters.snapshot())
    }

    pub fn snapshots(&self) -> HashMap<ProgressTaskId, ProcessingProgress> {
        let tasks = self.tasks.lock().unwrap();

        tasks
            .iter()
            .map(|(&id, counters)| (id, counters.snapshot()))
            .collect()
    }
}
// -----------------------------------------------------------------------------
// Progress reporter
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ProgressReporter {
    sender: Option<Sender<ProgressEvent>>,
    state: Arc<ProgressState>,
}

impl ProgressReporter {
    pub fn new(sender: Sender<ProgressEvent>) -> Self {
        Self {
            sender: Some(sender),
            state: Arc::new(ProgressState::default()),
        }
    }

    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_enabled(&self) -> bool {
        self.sender.is_some()
    }

    pub fn state(&self) -> Arc<ProgressState> {
        Arc::clone(&self.state)
    }

    pub fn emit(&self, event: ProgressEvent) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(event);
        }
    }
    pub fn task_skipped(&self, id: ProgressTaskId, reason: String) {
        self.emit(ProgressEvent::TaskSkipped { id, reason });
    }
    pub fn task_started(
        &self,
        id: ProgressTaskId,
        exchange: String,
        instrument: String,
        content_type: String,
        source: String,
    ) {
        if self.is_enabled() {
            self.state.register_task(id);
        }

        self.emit(ProgressEvent::TaskStarted {
            id,
            exchange,
            instrument,
            content_type,
            source,
        });
    }

    pub fn task_completed(&self, id: ProgressTaskId, counters: &ProcessingCounters) {
        if self.is_enabled() {
            let task = self.state.register_task(id);
            task.update(counters);
        }

        self.emit(ProgressEvent::TaskCompleted {
            id,
            records_read: counters.records_read,
            records_written: counters.events_written,
            records_rejected: counters.records_rejected,
        });
    }

    pub fn task_failed(&self, id: ProgressTaskId, message: String) {
        self.emit(ProgressEvent::TaskFailed { id, message });
    }

    pub fn finalizing(&self) {
        self.emit(ProgressEvent::Finalizing);
    }

    pub fn finished(&self, success: bool) {
        self.emit(ProgressEvent::Finished { success });
    }
}

// -----------------------------------------------------------------------------
// Per-worker progress handle
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct WorkerProgress {
    counters: Option<Arc<TaskProgressCounters>>,
    last_update: Instant,
    interval: Duration,
    last_checked_records: u64,
}

impl WorkerProgress {
    pub fn new(reporter: &ProgressReporter, id: ProgressTaskId) -> Self {
        let counters = if reporter.is_enabled() {
            Some(reporter.state.register_task(id))
        } else {
            None
        };
        Self {
            counters,
            last_update: Instant::now(),
            interval: Duration::from_millis(500),
            last_checked_records: 0,
        }
    }

    /// Called periodically by processing workers.
    ///
    /// Time is checked only after sufficient record progress.
    pub fn maybe_update(&mut self, metrics: &ProcessingCounters) {
        let Some(counters) = &self.counters else {
            return;
        };

        let records = metrics.records_read;

        if records.saturating_sub(self.last_checked_records) < 8192 {
            return;
        }

        self.last_checked_records = records;

        if self.last_update.elapsed() < self.interval {
            return;
        }

        counters.update(metrics);
        self.last_update = Instant::now();
    }

    pub fn finish(&self, metrics: &ProcessingCounters) {
        if let Some(counters) = &self.counters {
            counters.update(metrics);
        }
    }
}
