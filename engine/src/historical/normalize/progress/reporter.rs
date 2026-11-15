use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
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

    JobFinished {
        job_id: u64,
        success: bool,
    },

    StreamFailed {
        job_id: u64,
        stream: String,
        task_ids: Vec<u64>,
        message: String,
    },
    JobOutcome {
        job_id: u64,
        success: bool,
        message: Option<String>,
        manifest_path: Option<String>,
        failure_report_path: Option<String>,
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
    pub routing_records: AtomicU64,
    pub records_written: AtomicU64,
    records_processed: AtomicU64,
    events_normalized: AtomicU64,
    spooled_records: AtomicU64,
    has_spool: AtomicBool,
    pub records_rejected: AtomicU64,
    pub last_progress_unix_ms: AtomicU64,
    meaningful: Mutex<Option<Instant>>,
    publication_requested: AtomicBool,
}

impl TaskProgressCounters {
    pub fn update(&self, counters: &ProcessingCounters) {
        if self.records_read.load(Ordering::Relaxed) != counters.records_read
            || self.records_written.load(Ordering::Relaxed) != counters.events_written
            || self.records_processed.load(Ordering::Relaxed) != counters.records_processed
        {
            *self.meaningful.lock().unwrap() = Some(Instant::now());
        }
        self.last_progress_unix_ms.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            Ordering::Relaxed,
        );
        self.records_processed
            .store(counters.records_processed, Ordering::Relaxed);
        self.events_normalized
            .store(counters.events_normalized, Ordering::Relaxed);
        self.records_read
            .store(counters.records_read, Ordering::Relaxed);

        self.records_written
            .store(counters.events_written, Ordering::Relaxed);

        self.records_rejected
            .store(counters.records_rejected, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ProcessingProgress {
        // Consumers already have a regular cadence. Request one publication on
        // the worker's next callback without a clock read on every input row.
        self.publication_requested.store(true, Ordering::Relaxed);
        ProcessingProgress {
            last_meaningful_age: self.meaningful.lock().unwrap().map(|t| t.elapsed()),
            routing_records: self.routing_records.load(Ordering::Relaxed),
            records_processed: self.records_processed.load(Ordering::Relaxed),
            events_normalized: self.events_normalized.load(Ordering::Relaxed),
            spooled_records: self
                .has_spool
                .load(Ordering::Relaxed)
                .then(|| self.spooled_records.load(Ordering::Relaxed)),
            last_progress_unix_ms: self.last_progress_unix_ms.load(Ordering::Relaxed),
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
    pub last_meaningful_age: Option<Duration>,
    /// Physical rows examined during family routing; separate from worker counters.
    pub routing_records: u64,
    pub records_processed: u64,
    pub events_normalized: u64,
    pub spooled_records: Option<u64>,
    pub last_progress_unix_ms: u64,
    pub records_read: u64,
    pub records_written: u64,
    pub records_rejected: u64,
}

impl From<&ProcessingCounters> for ProcessingProgress {
    fn from(counters: &ProcessingCounters) -> Self {
        Self {
            last_meaningful_age: None,
            routing_records: 0,
            records_processed: counters.records_processed,
            events_normalized: counters.events_normalized,
            spooled_records: None,
            last_progress_unix_ms: 0,
            records_read: counters.records_read,
            records_written: counters.events_written,
            records_rejected: counters.records_rejected,
        }
    }
}

// -----------------------------------------------------------------------------
// Shared progress state
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum EventKey {
    Task(ProgressTaskId),
    Job(u64),
    Stream(u64, String),
    Finalizing,
    Finished,
}

#[cfg(feature = "normalization-test-hooks")]
#[derive(Clone)]
struct PhaseHook(Arc<PhaseCallback>);
#[cfg(feature = "normalization-test-hooks")]
impl std::fmt::Debug for PhaseHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PhaseHook")
    }
}

type PhaseKey = (u64, Option<u64>, String);
type StageActivity = HashMap<(u64, String), (Instant, u64, &'static str)>;
#[cfg(feature = "normalization-test-hooks")]
type PhaseCallback = dyn Fn(u64, Option<u64>, &str, &'static str) + Send + Sync;

type PhaseTiming = (&'static str, Instant, u64);

#[derive(Debug, Default)]
pub struct ProgressState {
    #[cfg(feature = "normalization-test-hooks")]
    phase_hook: Mutex<Option<PhaseHook>>,
    coalesced: AtomicBool,
    pending: Mutex<BTreeMap<EventKey, ProgressEvent>>,
    tasks: Mutex<HashMap<ProgressTaskId, Arc<TaskProgressCounters>>>,
    phases: Mutex<HashMap<PhaseKey, PhaseTiming>>,
    activity: Mutex<StageActivity>,
    snapshots_enabled: AtomicBool,
    pub(crate) phase_totals: Mutex<HashMap<String, u64>>,
    pub(crate) wake: Mutex<Option<std::sync::mpsc::SyncSender<super::diagnostics::Command>>>,
    pub(crate) resources: Mutex<HashMap<String, serde_json::Value>>,
}

impl ProgressState {
    pub(crate) fn set_phase(&self, job: u64, task: Option<u64>, stream: &str, phase: &'static str) {
        let previous = self.phases.lock().unwrap().insert(
            (job, task, stream.to_owned()),
            (
                phase,
                Instant::now(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            ),
        );
        if let Some((previous, start, _)) = previous {
            *self
                .phase_totals
                .lock()
                .unwrap()
                .entry(format!(
                    "job={job}:task={task:?}:stream={stream}:phase={previous}"
                ))
                .or_default() += start.elapsed().as_millis() as u64;
        }
        if let Some(wake) = self.wake.lock().unwrap().as_ref() {
            let _ = wake.try_send(super::diagnostics::Command::Changed);
        }
        #[cfg(feature = "normalization-test-hooks")]
        {
            let hook = self.phase_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                (hook.0)(job, task, stream, phase);
            }
        }
    }
    pub(crate) fn end_snapshots(&self) {
        self.snapshots_enabled.store(false, Ordering::Relaxed);
        self.wake.lock().unwrap().take();
    }
    pub(crate) fn phase_snapshots(&self) -> Vec<serde_json::Value> {
        self.phases.lock().unwrap().iter().map(|((job,task,stream),(phase,start,unix))| serde_json::json!({"job":job,"source_task":task,"stream":stream,"phase":phase,"phase_start_unix_ms":unix,"phase_elapsed_ms":start.elapsed().as_millis() as u64})).collect()
    }

    /// Latest phase for this source or its execution unit, including job finalization.
    /// Unit keys are internal identities, distinct from the public stream name.
    pub fn dashboard_phase(&self, id: ProgressTaskId) -> Option<(&'static str, Duration)> {
        let phases = self.phases.lock().unwrap();
        let unit = phases
            .iter()
            .filter(|((job, task, _), _)| *job == id.job_id && *task == Some(id.task_id))
            .max_by_key(|(_, (_, start, _))| *start)
            .map(|((_, _, key), _)| key);
        phases
            .iter()
            .filter(|((job, task, key), _)| {
                *job == id.job_id
                    && match task {
                        Some(task) => *task == id.task_id,
                        None => key.is_empty() || unit == Some(key),
                    }
            })
            .max_by_key(|(_, (_, start, _))| *start)
            .map(|(_, (phase, start, _))| (*phase, start.elapsed()))
    }

    /// Counters describe a stage and unit; they are never treated as a percent.
    pub fn dashboard_activity(&self, id: ProgressTaskId) -> Option<(Duration, u64, &'static str)> {
        let unit = self
            .phases
            .lock()
            .unwrap()
            .iter()
            .filter(|((job, task, _), _)| *job == id.job_id && *task == Some(id.task_id))
            .max_by_key(|(_, (_, t, _))| *t)
            .map(|((_, _, unit), _)| unit.clone())?;
        self.activity
            .lock()
            .unwrap()
            .get(&(id.job_id, unit))
            .map(|(t, n, u)| (t.elapsed(), *n, *u))
    }

    /// At most one pending lifecycle outcome per known identity. Counters and
    /// phases never enter this queue. Bound each frame's work independently.
    pub fn has_pending_events(&self) -> bool {
        !self.pending.lock().unwrap().is_empty()
    }
    pub fn take_events(&self, maximum: usize) -> Vec<ProgressEvent> {
        let mut pending = self.pending.lock().unwrap();
        (0..maximum)
            .filter_map(|_| pending.pop_first().map(|(_, e)| e))
            .collect()
    }
    pub fn dashboard_overview_phase(
        &self,
        active: &std::collections::BTreeSet<ProgressTaskId>,
        jobs: &std::collections::BTreeSet<u64>,
    ) -> Option<(&'static str, Duration)> {
        let phases = self.phases.lock().unwrap();
        let units: std::collections::BTreeSet<_> = phases
            .keys()
            .filter_map(|(job, task, unit)| {
                task.filter(|task| active.contains(&ProgressTaskId::new(*job, *task)))
                    .map(|_| (*job, unit.as_str()))
            })
            .collect();
        phases
            .iter()
            .filter(|((job, task, unit), _)| match task {
                Some(task) => active.contains(&ProgressTaskId::new(*job, *task)),
                None if unit.is_empty() => jobs.contains(job),
                None => units.contains(&(*job, unit.as_str())),
            })
            .max_by_key(|(_, (_, start, _))| *start)
            .map(|(_, (phase, start, _))| (*phase, start.elapsed()))
    }

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

    pub fn coalesced() -> Self {
        let reporter = Self::default();
        reporter.state.coalesced.store(true, Ordering::Relaxed);
        reporter
    }
    /// Test-only phase barrier; invoked on the worker after releasing state locks.
    #[cfg(feature = "normalization-test-hooks")]
    pub fn set_phase_hook(
        &self,
        hook: impl Fn(u64, Option<u64>, &str, &'static str) + Send + Sync + 'static,
    ) {
        *self.state.phase_hook.lock().unwrap() = Some(PhaseHook(Arc::new(hook)));
    }
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_enabled(&self) -> bool {
        self.sender.is_some()
            || self.state.coalesced.load(Ordering::Relaxed)
            || self.state.snapshots_enabled.load(Ordering::Relaxed)
    }

    pub(crate) fn snapshots_enabled(&self) -> bool {
        self.state.snapshots_enabled.load(Ordering::Relaxed)
    }
    pub(crate) fn enable_snapshots(&self) {
        self.state.phases.lock().unwrap().clear();
        self.state.phase_totals.lock().unwrap().clear();
        self.state.resources.lock().unwrap().clear();
        if self.sender.is_none() {
            self.state.tasks.lock().unwrap().clear();
        }
        self.state.snapshots_enabled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn routing_records(&self, id: ProgressTaskId, records: u64) {
        if self.is_enabled() {
            let task = self.state.register_task(id);
            if task.routing_records.swap(records, Ordering::Relaxed) != records {
                *task.meaningful.lock().unwrap() = Some(Instant::now());
            }
        }
    }
    pub(crate) fn source_spooled(&self, id: ProgressTaskId, records: u64) {
        if self.is_enabled() {
            let task = self.state.register_task(id);
            if task.spooled_records.swap(records, Ordering::Relaxed) != records {
                *task.meaningful.lock().unwrap() = Some(Instant::now());
            }
            task.has_spool.store(true, Ordering::Relaxed);
        }
    }
    pub(crate) fn record_workload(&self, job: &crate::historical::job::ProcessingJob) {
        if !self.snapshots_enabled() {
            return;
        }
        use sha2::{Digest, Sha256};
        let bytes = match serde_json::to_vec(job) {
            Ok(bytes) => bytes,
            Err(_) => return,
        };
        let fingerprint: String = super::super::hex_digest(Sha256::digest(bytes));
        let sources: Vec<_> = job.tasks.iter().map(|task|serde_json::json!({"task":task.task_id.0,"source":task.input_path,"archive_member":task.archive_member,"symbol":task.symbol,"stream":task.stream_id.0})).collect();
        self.resource_snapshot(&format!("workload:{}",job.job_id.0),serde_json::json!({"job":job.job_id.0,"dataset":job.dataset_id.0,"job_sha256":fingerprint,"sources":sources,"debug_assertions":cfg!(debug_assertions)}));
    }
    /// Record a caller-owned wall-clock interval without combining it with
    /// overlapping phase or process CPU measurements.
    pub fn measured_wall_time(&self, id: ProgressTaskId, name: &str, elapsed: Duration) {
        self.resource_snapshot(
            &format!("wall:{}:{name}", id.job_id),
            serde_json::json!({"job":id.job_id,"name":name,"wall_seconds":elapsed.as_secs_f64()}),
        );
    }
    pub(crate) fn resource_snapshot(&self, key: &str, mut value: serde_json::Value) {
        if self.is_enabled() {
            if let Some(fields) = value.as_object_mut() {
                fields.insert(
                    "last_progress_unix_ms".into(),
                    serde_json::json!(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64
                    ),
                );
            }
            self.state
                .resources
                .lock()
                .unwrap()
                .insert(key.to_owned(), value);
        }
    }
    pub(crate) fn resource_wait(&self, id: ProgressTaskId, stream: &str, elapsed: Duration) {
        self.resource_snapshot(&format!("wait:{}:{stream}", id.job_id), serde_json::json!({"job":id.job_id,"stream":stream,"resource_wait_ms":elapsed.as_millis() as u64}));
    }
    pub(crate) fn stage_count(&self, job: u64, stream: &str, count: u64, unit: &'static str) {
        if self.is_enabled() {
            let mut activity = self.state.activity.lock().unwrap();
            let key = (job, stream.to_owned());
            if activity
                .get(&key)
                .is_none_or(|(_, old, u)| *old != count || *u != unit)
            {
                activity.insert(key, (Instant::now(), count, unit));
            }
        }
    }
    pub(crate) fn job_outcome(&self, result: &super::super::ProcessingResult) {
        self.emit(ProgressEvent::JobOutcome {
            job_id: result.job_id.0,
            success: result.status == super::super::ProcessingStatus::Complete,
            message: result.failure.as_ref().map(|f| f.message.clone()),
            manifest_path: result.manifest_path.clone(),
            failure_report_path: result.failure_report_path.clone(),
        });
    }
    pub fn state(&self) -> Arc<ProgressState> {
        Arc::clone(&self.state)
    }

    pub fn emit(&self, event: ProgressEvent) {
        if self.state.coalesced.load(Ordering::Relaxed) {
            let key = match &event {
                ProgressEvent::TaskStarted { id, .. }
                | ProgressEvent::TaskCompleted { id, .. }
                | ProgressEvent::TaskFailed { id, .. }
                | ProgressEvent::TaskSkipped { id, .. } => EventKey::Task(*id),
                ProgressEvent::JobFinished { job_id, .. }
                | ProgressEvent::JobOutcome { job_id, .. } => EventKey::Job(*job_id),
                ProgressEvent::StreamFailed { job_id, stream, .. } => {
                    EventKey::Stream(*job_id, stream.clone())
                }
                ProgressEvent::Finalizing => EventKey::Finalizing,
                ProgressEvent::Finished { .. } => EventKey::Finished,
            };
            let mut pending = self.state.pending.lock().unwrap();
            if matches!(event, ProgressEvent::TaskStarted { .. }) && pending.contains_key(&key) {
                return;
            }
            if matches!(event, ProgressEvent::JobFinished { .. })
                && matches!(pending.get(&key), Some(ProgressEvent::JobOutcome { .. }))
            {
                return;
            }
            pending.insert(key, event);
            return;
        }
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
}

impl WorkerProgress {
    pub fn new(reporter: &ProgressReporter, id: ProgressTaskId) -> Self {
        let counters = reporter
            .is_enabled()
            .then(|| reporter.state.register_task(id));
        if let Some(counters) = &counters {
            counters
                .publication_requested
                .store(true, Ordering::Relaxed);
        }
        Self { counters }
    }

    /// Snapshot consumers coalesce publication requests at their own cadence.
    /// The per-record path reads an atomic flag, not the clock or an input-row
    /// threshold. Stage instrumentation remains independent of this callback.
    pub fn maybe_update(&mut self, metrics: &ProcessingCounters) {
        let Some(counters) = &self.counters else {
            return;
        };
        if counters.publication_requested.load(Ordering::Relaxed)
            && counters
                .publication_requested
                .swap(false, Ordering::Relaxed)
        {
            counters.update(metrics);
        }
    }

    pub fn finish(&self, metrics: &ProcessingCounters) {
        if let Some(counters) = &self.counters {
            counters.update(metrics);
        }
    }
}
