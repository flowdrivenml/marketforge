//! Presentation state. Lifecycle events own status; atomic snapshots own live counters.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
    Stopped,
    NotRun,
}
impl TaskStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Pending => "Queued",
            Self::Running => "Running",
            Self::Completed => "Done",
            Self::Failed => "Failed",
            Self::Skipped => "Skipped",
            Self::Stopped => "Stopped",
            Self::NotRun => "Not run",
        }
    }
    fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Skipped | Self::Stopped | Self::NotRun
        )
    }
}
pub(super) struct TaskState {
    pub exchange: String,
    pub instrument: String,
    pub content_type: String,
    pub source: String,
    pub stream: String,
    pub status: TaskStatus,
    pub routing_records: u64,
    pub records_read: u64,
    pub records_processed: u64,
    pub records_written: u64,
    pub normalized_rows: u64,
    pub records_rejected: u64,
    pub speed: f64,
    pub last_meaningful_age: Option<Duration>,
    pub rate_ready: bool,
    has_metrics: bool,
    pub message: String,
    sample: (Instant, u64),
}
impl TaskState {
    fn update(&mut self, snapshot: &ProcessingProgress, now: Instant) {
        self.has_metrics |= snapshot.last_progress_unix_ms != 0;
        self.last_meaningful_age = snapshot.last_meaningful_age;
        self.routing_records = snapshot.routing_records;
        self.records_processed = self.records_processed.max(snapshot.records_processed);
        self.normalized_rows = self.normalized_rows.max(snapshot.events_normalized);
        // Completed event counters are final. Delayed snapshots cannot replace them.
        if self.status == TaskStatus::Completed {
            return;
        }
        self.records_read = self.records_read.max(snapshot.records_read);
        self.records_written = self
            .records_written
            .max(snapshot.staged_rows.unwrap_or(snapshot.records_written));
        self.records_rejected = self.records_rejected.max(snapshot.records_rejected);
        let seconds = now.duration_since(self.sample.0).as_secs_f64();
        if seconds >= 1.0 && self.has_metrics {
            self.rate_ready = true;
            self.speed = if self.status == TaskStatus::Running {
                self.records_processed.saturating_sub(self.sample.1) as f64 / seconds
            } else {
                0.0
            };
            self.sample = (now, self.records_processed);
        }
    }
}
pub(super) struct ProcessingDashboard {
    pub tasks: BTreeMap<ProgressTaskId, TaskState>,
    pub total_jobs: usize,
    pub total_tasks: usize,
    pub started: Instant,
    pub ended: Option<Instant>,
    pub finalizing: bool,
    pub finished: bool,
    pub success: bool,
    pub jobs: BTreeMap<u64, bool>,
    pub outputs: BTreeMap<u64, String>,
    pub published: BTreeMap<u64, String>,
    pub committed_rows: BTreeMap<u64, u64>,
    pub stream_failures: BTreeMap<(u64, String), String>,
    pub throughput_history: VecDeque<u64>,
    pub activity: VecDeque<String>,
    pub table: TableState,
    pub selected: usize,
    pub detail: bool,
    pub help: bool,
    pub selected_phase: String,
    pub overview_phase: String,
    pub log_path: Option<String>,
    pub detail_scroll: u16,
    pub current_speed: f64,
    pub rate_ready: bool,
    pub rolling_speed: Option<f64>,
    pub(super) rate_window: VecDeque<(Instant, u64)>,
    pub(super) sample: (Instant, u64),
}
impl ProcessingDashboard {
    pub fn new(job: &ProcessingJob, config: &ProcessingConfig) -> Self {
        Self::new_session(std::slice::from_ref(job), config)
    }
    pub fn new_session(jobs: &[ProcessingJob], _config: &ProcessingConfig) -> Self {
        let now = Instant::now();
        let tasks: BTreeMap<_, _> = jobs
            .iter()
            .flat_map(|job| {
                job.tasks.iter().map(move |task| {
                    (
                        ProgressTaskId::new(job.job_id.0, task.task_id.0),
                        TaskState {
                            exchange: format!("{:?}", task.exchange),
                            instrument: task.symbol.clone(),
                            content_type: task
                                .normalizations
                                .first()
                                .map(|n| format!("{:?}", n.target_schema))
                                .unwrap_or_else(|| "—".into()),
                            source: task.input_path.display().to_string(),
                            stream: task.stream_id.0.clone(),
                            status: TaskStatus::Pending,
                            routing_records: 0,
                            records_read: 0,
                            records_processed: 0,
                            records_written: 0,
                            normalized_rows: 0,
                            records_rejected: 0,
                            speed: 0.0,
                            last_meaningful_age: None,
                            rate_ready: false,
                            has_metrics: false,
                            message: String::new(),
                            sample: (now, 0),
                        },
                    )
                })
            })
            .collect();
        Self {
            total_tasks: tasks.len(),
            tasks,
            total_jobs: jobs.len(),
            started: now,
            ended: None,
            finalizing: false,
            finished: false,
            success: false,
            jobs: BTreeMap::new(),
            published: BTreeMap::new(),
            committed_rows: BTreeMap::new(),
            stream_failures: BTreeMap::new(),
            outputs: jobs
                .iter()
                .map(|j| (j.job_id.0, j.output.dataset_path.display().to_string()))
                .collect(),
            throughput_history: VecDeque::with_capacity(60),
            activity: VecDeque::with_capacity(100),
            table: TableState::default().with_selected(Some(0)),
            selected: 0,
            detail: false,
            help: false,
            selected_phase: String::new(),
            overview_phase: String::new(),
            log_path: None,
            detail_scroll: 0,
            current_speed: 0.0,
            rate_ready: false,
            rolling_speed: None,
            rate_window: VecDeque::from([(now, 0)]),
            sample: (now, 0),
        }
    }
    pub fn new_prepared(
        batch: &crate::historical::config::PreparedBatch,
        config: &ProcessingConfig,
    ) -> Self {
        let mut dashboard = Self::new_session(&[], config);
        dashboard.total_jobs = batch.jobs.iter().filter(|j| !j.tasks.is_empty()).count();
        for job in &batch.jobs {
            dashboard
                .outputs
                .insert(job.job_id.0, job.output.dataset_path.display().to_string());
            for task in &job.tasks {
                dashboard.handle_event(ProgressEvent::TaskRegistered {
                    id: ProgressTaskId::new(job.job_id.0, task.task_id.0),
                    exchange: format!("{:?}", task.exchange),
                    instrument: task.symbol.clone().unwrap_or_else(|| {
                        format!(
                            "family:{}",
                            task.scope.as_ref().map_or("", |s| s.identifier.as_str())
                        )
                    }),
                    content_type: format!("{:?}", job.output.content_type),
                    source: task.input_path.display().to_string(),
                    stream: task.stream_id.0.clone(),
                });
            }
        }
        dashboard
    }
    pub fn reconcile_results(&mut self, results: &[ProcessingResult]) {
        for result in results {
            self.handle_event(ProgressEvent::JobOutcome {
                job_id: result.job_id.0,
                events_written: result.events_written,
                success: result.status == ProcessingStatus::Complete,
                message: result.failure.as_ref().map(|f| f.message.clone()),
                manifest_path: result.manifest_path.clone(),
                failure_report_path: result.failure_report_path.clone(),
            });
        }
        self.success = results
            .iter()
            .all(|r| r.status == ProcessingStatus::Complete);
    }
    pub fn elapsed(&self) -> Duration {
        self.ended
            .unwrap_or_else(Instant::now)
            .duration_since(self.started)
    }
    pub fn handle_event(&mut self, event: ProgressEvent) {
        let message = match event {
            ProgressEvent::TaskRegistered {
                id,
                exchange,
                instrument,
                content_type,
                source,
                stream,
            } => {
                let now = Instant::now();
                let task = self.tasks.entry(id).or_insert_with(|| TaskState {
                    exchange: String::new(),
                    instrument: String::new(),
                    content_type: String::new(),
                    source: String::new(),
                    stream: String::new(),
                    status: TaskStatus::Pending,
                    routing_records: 0,
                    records_read: 0,
                    records_processed: 0,
                    records_written: 0,
                    normalized_rows: 0,
                    records_rejected: 0,
                    speed: 0.0,
                    last_meaningful_age: None,
                    rate_ready: false,
                    has_metrics: false,
                    message: String::new(),
                    sample: (now, 0),
                });
                task.exchange = exchange;
                task.instrument = instrument;
                task.content_type = content_type;
                task.source = source;
                task.stream = stream;
                self.total_tasks = self.tasks.len();
                return;
            }
            ProgressEvent::TaskStarted {
                id,
                exchange,
                instrument,
                content_type,
                source,
            } => {
                let Some(t) = self.tasks.get_mut(&id) else {
                    return;
                };
                if t.status != TaskStatus::Pending {
                    return;
                }
                t.exchange = exchange;
                t.instrument = instrument;
                t.content_type = content_type;
                t.source = source;
                t.status = TaskStatus::Running;
                t.sample = (Instant::now(), t.records_processed);
                format!("{} {} started", t.exchange, t.instrument)
            }
            ProgressEvent::TaskCompleted {
                id,
                records_read,
                records_written,
                records_rejected,
            } => {
                let Some(t) = self.tasks.get_mut(&id) else {
                    return;
                };
                if t.status.terminal() {
                    return;
                }
                t.status = TaskStatus::Completed;
                t.records_read = records_read;
                t.records_written = records_written;
                t.records_rejected = records_rejected;
                t.speed = 0.0;
                format!("{} {} source complete", t.exchange, t.instrument)
            }
            ProgressEvent::TaskFailed { id, message } => {
                let Some(t) = self.tasks.get_mut(&id) else {
                    return;
                };
                if t.status.terminal() {
                    return;
                }
                t.status = TaskStatus::Failed;
                t.speed = 0.0;
                t.message = message.clone();
                format!("Job {} / task {} failed: {message}", id.job_id, id.task_id)
            }
            ProgressEvent::TaskSkipped { id, reason } => {
                let Some(t) = self.tasks.get_mut(&id) else {
                    return;
                };
                if t.status.terminal() {
                    return;
                }
                t.status = TaskStatus::Skipped;
                t.speed = 0.0;
                t.message = reason.clone();
                format!("Task {} skipped: {reason}", id.task_id)
            }
            ProgressEvent::JobFinished { job_id, success } => {
                if !self.outputs.contains_key(&job_id) || self.jobs.contains_key(&job_id) {
                    return;
                }
                self.jobs.insert(job_id, success);
                for (id, task) in &mut self.tasks {
                    if id.job_id == job_id && !success {
                        task.status = match task.status {
                            TaskStatus::Running => TaskStatus::Stopped,
                            TaskStatus::Pending => TaskStatus::NotRun,
                            s => s,
                        };
                        task.speed = 0.0;
                    }
                }
                format!(
                    "Job {job_id} {}",
                    if success { "committed" } else { "failed" }
                )
            }
            ProgressEvent::StreamFailed {
                job_id,
                stream,
                task_ids,
                message,
            } => {
                for task_id in task_ids {
                    if let Some(task) = self.tasks.get_mut(&ProgressTaskId::new(job_id, task_id)) {
                        if task.status == TaskStatus::Running {
                            task.status = TaskStatus::Stopped;
                        }
                        task.message = format!("Stream failure: {message}");
                        task.speed = 0.0;
                    }
                }
                self.stream_failures
                    .insert((job_id, stream.clone()), message.clone());
                format!("Job {job_id} stream {stream} failed: {message}")
            }
            ProgressEvent::JobOutcome {
                job_id,
                events_written,
                success,
                message,
                manifest_path,
                failure_report_path,
            } => {
                if !self.outputs.contains_key(&job_id) {
                    return;
                }
                // Definitive results supersede provisional lifecycle notifications.
                self.jobs.insert(job_id, success);
                if success && let Some(path) = manifest_path {
                    self.published.insert(job_id, path);
                    self.committed_rows.insert(job_id, events_written);
                }
                if !success {
                    self.published.remove(&job_id);
                    self.committed_rows.remove(&job_id);
                }
                for (id, task) in &mut self.tasks {
                    if id.job_id == job_id {
                        task.status = match task.status {
                            TaskStatus::Running => TaskStatus::Stopped,
                            TaskStatus::Pending => TaskStatus::NotRun,
                            s => s,
                        };
                        task.speed = 0.0;
                    }
                }
                format!(
                    "Job {job_id} {}{}{}",
                    if success { "committed" } else { "failed" },
                    message.map(|s| format!(": {s}")).unwrap_or_default(),
                    failure_report_path
                        .map(|s| format!("; failure context: {s}"))
                        .unwrap_or_default()
                )
            }
            ProgressEvent::Finalizing => {
                self.finalizing = true;
                "Finalizing outputs and validating integrity".into()
            }
            ProgressEvent::Finished { success } => {
                if self.finished {
                    return;
                }
                for task in self.tasks.values_mut() {
                    task.status = match task.status {
                        TaskStatus::Running => TaskStatus::Stopped,
                        TaskStatus::Pending => TaskStatus::NotRun,
                        status => status,
                    };
                    task.speed = 0.0;
                }
                self.finished = true;
                self.success = success;
                self.ended = Some(Instant::now());
                if success {
                    "Run complete"
                } else {
                    "Run failed; see diagnostics and failure manifest"
                }
                .into()
            }
        };
        if self.activity.len() == 100 {
            self.activity.pop_front();
        }
        self.activity
            .push_back(format!("{}  {message}", format_duration(self.elapsed())));
    }
    pub fn update_snapshots(&mut self, state: &ProgressState) {
        let now = Instant::now();
        for (id, snapshot) in state.snapshots() {
            if let Some(task) = self.tasks.get_mut(&id) {
                task.update(&snapshot, now);
            }
        }
        if let Some((id, _)) = self.tasks.iter().nth(self.selected) {
            self.selected_phase = state
                .dashboard_phase(*id)
                .map(|(phase, age)| format!("{phase} ({} in phase)", format_duration(age)))
                .unwrap_or_else(|| "Awaiting phase information".into());
            if let Some((age, count, unit)) = state.dashboard_activity(*id) {
                self.selected_phase.push_str(&format!(
                    " · {count} {unit} · last update {} ago",
                    format_duration(age)
                ));
            } else if let Some(age) = self.tasks[id].last_meaningful_age {
                self.selected_phase.push_str(&format!(
                    " · last counter change {} ago",
                    format_duration(age)
                ));
            } else {
                self.selected_phase
                    .push_str(" · progress information unavailable");
            }
        }
        let active = self
            .tasks
            .iter()
            .filter(|(_, t)| !t.status.terminal())
            .map(|(id, _)| *id)
            .collect();
        let jobs = self
            .outputs
            .keys()
            .filter(|job| !self.jobs.contains_key(job))
            .copied()
            .collect();
        self.overview_phase = state
            .dashboard_overview_phase(&active, &jobs)
            .map(|(phase, age)| format!("{phase} ({} in phase)", format_duration(age)))
            .unwrap_or_else(|| "No active stage telemetry".into());
        let seconds = now.duration_since(self.sample.0).as_secs_f64();
        if seconds >= 1.0 && self.tasks.values().any(|t| t.has_metrics) {
            self.rate_ready = true;
            let total = self.total_records_processed();
            self.current_speed = total.saturating_sub(self.sample.1) as f64 / seconds;
            self.sample = (now, total);
            self.rate_window.push_back((now, total));
            while self.rate_window.len() > 2
                && now.duration_since(self.rate_window.front().unwrap().0) > Duration::from_secs(5)
            {
                self.rate_window.pop_front();
            }
            if let Some((start, count)) = self.rate_window.front() {
                let seconds = now.duration_since(*start).as_secs_f64();
                self.rolling_speed =
                    (seconds >= 1.0).then(|| total.saturating_sub(*count) as f64 / seconds);
            }
            if self.throughput_history.len() == 60 {
                self.throughput_history.pop_front();
            }
            self.throughput_history.push_back(self.current_speed as u64);
        }
    }
    pub fn settled_tasks(&self) -> usize {
        self.tasks.values().filter(|t| t.status.terminal()).count()
    }
    pub fn stopped_tasks(&self) -> usize {
        self.count(TaskStatus::Stopped)
    }
    pub fn not_run_tasks(&self) -> usize {
        self.count(TaskStatus::NotRun)
    }
    pub fn completed_tasks(&self) -> usize {
        self.count(TaskStatus::Completed)
    }
    pub fn failed_tasks(&self) -> usize {
        self.count(TaskStatus::Failed)
    }
    pub fn skipped_tasks(&self) -> usize {
        self.count(TaskStatus::Skipped)
    }
    pub fn active_tasks(&self) -> usize {
        self.count(TaskStatus::Running)
    }
    fn count(&self, status: TaskStatus) -> usize {
        self.tasks.values().filter(|t| t.status == status).count()
    }
    pub fn completed_jobs(&self) -> usize {
        self.jobs.values().filter(|s| **s).count()
    }
    pub fn total_records_processed(&self) -> u64 {
        self.tasks
            .values()
            .fold(0u64, |n, t| n.saturating_add(t.records_processed))
    }
    pub fn total_routing_records(&self) -> u64 {
        self.tasks
            .values()
            .fold(0u64, |n, t| n.saturating_add(t.routing_records))
    }
    pub fn total_records_read(&self) -> u64 {
        self.tasks
            .values()
            .fold(0u64, |n, t| n.saturating_add(t.records_read))
    }
    pub fn total_records_written(&self) -> u64 {
        self.tasks
            .values()
            .fold(0u64, |n, t| n.saturating_add(t.records_written))
    }
    pub fn total_records_rejected(&self) -> u64 {
        self.tasks
            .values()
            .fold(0u64, |n, t| n.saturating_add(t.records_rejected))
    }
    pub fn next_active(&mut self) {
        if let Some(index) = self
            .tasks
            .values()
            .enumerate()
            .skip(self.selected + 1)
            .chain(self.tasks.values().enumerate().take(self.selected + 1))
            .find_map(|(i, t)| (t.status == TaskStatus::Running).then_some(i))
        {
            self.selected = index;
            self.table.select(Some(index));
        }
    }
    pub fn navigate(&mut self, key: KeyCode, page: usize) {
        let last = self.tasks.len().saturating_sub(1);
        self.selected = match key {
            KeyCode::Down => self.selected.saturating_add(1).min(last),
            KeyCode::Up => self.selected.saturating_sub(1),
            KeyCode::PageDown => self.selected.saturating_add(page).min(last),
            KeyCode::PageUp => self.selected.saturating_sub(page),
            KeyCode::Home => 0,
            KeyCode::End => last,
            _ => self.selected,
        };
        self.table
            .select((!self.tasks.is_empty()).then_some(self.selected));
    }
}
