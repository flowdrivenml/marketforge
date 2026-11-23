use crate::shared::terminal::format::{compact, format_duration};
use crate::shared::terminal::guard::TerminalGuard;
use crate::shared::terminal::interrupt::InterruptSignals;
use crate::shared::terminal::palette::{BG, CYAN, FG, GREEN, MUTED, RED, YELLOW};
use crate::shared::terminal::widgets::panel;
use std::{
    collections::{HashMap, VecDeque},
    io::{self, IsTerminal, Stdout, Write},
    sync::{
        Arc,
        mpsc::{self, Receiver},
    },
    thread,
    time::{Duration, Instant},
};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table, Wrap},
};

use {
    crate::historical::job::MergeJob, crate::historical::merge::MergePhase,
    crate::historical::merge::MergeProgressEvent, crate::historical::merge::MergeProgressReporter,
    crate::historical::merge::MergeProgressState, crate::historical::merge::PartitionKey,
    crate::historical::merge::PartitionProgress,
    crate::historical::merge::UnifiedMergeExecutionResult,
    crate::historical::merge::execute_unified_merge_job_with_progress,
};

// -----------------------------------------------------------------------------
// Theme — Tokyo Night
// -----------------------------------------------------------------------------

const REFRESH_INTERVAL: Duration = Duration::from_millis(250);
const HISTORY_LENGTH: usize = 60;
const ACTIVITY_LENGTH: usize = 12;

// -----------------------------------------------------------------------------
// Terminal guard
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Partition status
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Interrupted,
}

// -----------------------------------------------------------------------------
// Partition state
// -----------------------------------------------------------------------------

#[derive(Debug)]
struct PartitionState {
    status: PartitionStatus,
    events: u64,
    speed: Option<f64>,
    started: Instant,
    ended: Option<Instant>,
    last_report: Option<Instant>,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            status: PartitionStatus::Pending,
            events: 0,
            speed: None,
            started: Instant::now(),
            ended: None,
            last_report: None,
        }
    }

    fn update(&mut self, snapshot: &PartitionProgress) {
        let Some(reported) = snapshot
            .reported
            .filter(|time| Some(*time) != self.last_report)
        else {
            return;
        };
        let duration = reported.saturating_duration_since(self.last_report.unwrap_or(self.started));
        if !duration.is_zero() {
            let rate = snapshot.events.saturating_sub(self.events) as f64 / duration.as_secs_f64();
            self.speed = Some(self.speed.map_or(rate, |old| 0.35 * rate + 0.65 * old));
        }
        self.last_report = Some(reported);
        self.events = snapshot.events;
    }

    fn rate(&self) -> Option<f64> {
        self.last_report
            .filter(|time| time.elapsed() <= Duration::from_secs(3))
            .filter(|_| self.status == PartitionStatus::Running)
            .and(self.speed)
    }
}

pub struct MergeTuiPaths {
    pub origin: std::path::PathBuf,
    pub loaded: std::path::PathBuf,
    pub checksum: String,
    pub report: std::path::PathBuf,
    pub managed: bool,
}

#[derive(Debug, Default)]
struct ValidationState {
    started: bool,
    completed: bool,

    total_partitions: usize,
    partitions_completed: usize,

    total_files: u64,
    files_completed: u64,

    expected_events: u64,
    events_validated: u64,

    row_groups_validated: u64,
}

struct OutputMetrics {
    trades: u64,
    depth: u64,
    reconstruction: u64,
    canonical_rows: u64,
}

struct MergeDashboard {
    job_id: u64,

    paths: MergeTuiPaths,
    workers: usize,
    memory: u64,
    open_files: usize,
    row_group: u64,
    file_target: u64,
    compression: i32,
    partitions_per_worker: usize,
    content: &'static str,
    input_datasets: usize,
    selected_files: usize,
    verified_files: usize,
    concrete_streams: Option<usize>,
    requested: crate::historical::job::MergeTimeRange,
    ordering: crate::historical::job::MergeTimelineOrdering,
    output: std::path::PathBuf,

    phase: Option<MergePhase>,
    completed_phases: Vec<MergePhase>,

    partitions: HashMap<PartitionKey, PartitionState>,
    planned: HashMap<MergePhase, usize>,

    started: Instant,
    phase_started: Instant,
    phase_ended: Option<Instant>,
    ended: Option<Instant>,
    rate: Option<f64>,
    last_sample: u64,
    details: bool,
    scroll: u16,
    interrupted: bool,

    finished: bool,
    success: bool,

    throughput_history: VecDeque<u64>,
    last_throughput_sample: Instant,

    activity: VecDeque<(String, Color)>,
    validation: ValidationState,
    output_metrics: Option<OutputMetrics>,
}

impl MergeDashboard {
    fn new(job: &MergeJob, paths: MergeTuiPaths) -> Self {
        use crate::historical::job::ContentType;
        let inputs = job
            .submission
            .iter()
            .flat_map(|submission| &submission.inputs);
        let trades = inputs
            .clone()
            .any(|input| input.content_type == ContentType::Trades);
        let depth = inputs
            .clone()
            .any(|input| input.content_type == ContentType::Depth);
        let content = match (trades, depth) {
            (true, true) => "Trades + depth",
            (true, false) => "Trades",
            (false, true) => "Depth",
            _ => "Content resolving",
        };
        let now = Instant::now();
        Self {
            paths,
            job_id: job.job_id,
            workers: job.resources.workers,
            memory: job.resources.memory_limit_mb,
            open_files: job.resources.max_open_files,
            row_group: job.parquet.row_group_target_mb,
            file_target: job.parquet.file_target_mb,
            compression: job.parquet.compression_level,
            partitions_per_worker: job.partitioning.partitions_per_worker,
            content,
            input_datasets: job.inputs.len(),
            selected_files: inputs.clone().count(),
            concrete_streams: job.submission.as_ref().map(|_| {
                inputs
                    .map(|input| input.stream_rank)
                    .collect::<std::collections::HashSet<_>>()
                    .len()
            }),
            verified_files: 0,
            requested: job.time_range,
            ordering: job.timeline_ordering(),
            output: job.output.dataset_path.clone(),
            phase: None,
            completed_phases: Vec::new(),
            partitions: HashMap::new(),
            planned: HashMap::new(),
            started: now,
            phase_started: now,
            phase_ended: None,
            ended: None,
            rate: None,
            last_sample: 0,
            details: false,
            scroll: 0,
            interrupted: false,
            finished: false,
            success: false,
            throughput_history: VecDeque::with_capacity(HISTORY_LENGTH),
            last_throughput_sample: now,
            activity: VecDeque::with_capacity(ACTIVITY_LENGTH),
            validation: ValidationState::default(),
            output_metrics: None,
        }
    }

    fn elapsed(&self) -> Duration {
        self.ended
            .unwrap_or_else(Instant::now)
            .duration_since(self.started)
    }

    fn phase_time(&self) -> Duration {
        self.phase_ended
            .or(self.ended)
            .unwrap_or_else(Instant::now)
            .duration_since(self.phase_started)
    }

    fn phase_status(&self, phase: MergePhase) -> &'static str {
        if self.completed_phases.contains(&phase) {
            "Done"
        } else if self.phase == Some(phase) {
            if self.interrupted {
                "Interrupted"
            } else if self.finished {
                "Failed"
            } else {
                "Active"
            }
        } else {
            "Pending"
        }
    }

    fn is_partition_phase(&self) -> bool {
        matches!(
            self.phase,
            Some(MergePhase::LogicalMerge | MergePhase::PhysicalMaterialization)
        )
    }

    fn handle_event(&mut self, event: MergeProgressEvent) {
        match event {
            MergeProgressEvent::InputsVerified { files } => self.verified_files = files,
            MergeProgressEvent::InputsResolved { streams } => self.concrete_streams = Some(streams),
            MergeProgressEvent::PartitionsPlanned { phase, count } => {
                self.planned.insert(phase, count);
            }
            MergeProgressEvent::ValidationStarted {
                total_partitions,
                total_files,
                expected_events,
            } => {
                self.validation = ValidationState {
                    started: true,
                    total_partitions,
                    total_files,
                    expected_events,
                    ..ValidationState::default()
                };
            }

            MergeProgressEvent::ValidationProgress {
                partitions_completed,
                files_completed,
                events_validated,
                row_groups_validated,
            } => {
                self.validation.partitions_completed = partitions_completed;
                self.validation.files_completed = files_completed;
                self.validation.events_validated = events_validated;
                self.validation.row_groups_validated = row_groups_validated;
            }

            MergeProgressEvent::ValidationCompleted {
                partitions_completed,
                files_completed,
                events_validated,
                row_groups_validated,
            } => {
                self.validation.completed = true;
                self.validation.partitions_completed = partitions_completed;
                self.validation.files_completed = files_completed;
                self.validation.events_validated = events_validated;
                self.validation.row_groups_validated = row_groups_validated;
            }
            MergeProgressEvent::PhaseCompleted { phase } => {
                if !self.completed_phases.contains(&phase) {
                    self.completed_phases.push(phase);
                }
                if self.phase == Some(phase) {
                    self.phase_ended = Some(Instant::now());
                }
                self.log_activity(format!("Done · {}", phase_name(phase)), GREEN);
            }
            MergeProgressEvent::PhaseStarted { phase } => {
                self.phase = Some(phase);
                self.phase_started = Instant::now();
                self.phase_ended = None;
                self.throughput_history.clear();
                self.last_throughput_sample = self.phase_started;
                self.last_sample = 0;
                self.rate = None;
                self.log_activity(format!("Started · {}", phase_name(phase)), CYAN);
            }
            MergeProgressEvent::PartitionStarted { key } => {
                let partition = self
                    .partitions
                    .entry(key)
                    .or_insert_with(PartitionState::new);

                if partition.status == PartitionStatus::Pending {
                    partition.status = PartitionStatus::Running;
                    partition.started = Instant::now();
                }
            }

            MergeProgressEvent::PartitionCompleted { key, events } => {
                let partition = self
                    .partitions
                    .entry(key)
                    .or_insert_with(PartitionState::new);

                partition.status = PartitionStatus::Completed;
                partition.events = events;
                partition.ended = Some(Instant::now());
                partition.last_report = partition.ended;
            }
            MergeProgressEvent::PartitionFailed { key, message } => {
                let partition = self
                    .partitions
                    .entry(key)
                    .or_insert_with(PartitionState::new);
                partition.status = PartitionStatus::Failed;
                partition.ended = Some(Instant::now());
                self.log_activity(
                    format!("Partition {} failed · {message}", key.partition_id),
                    RED,
                );
            }
            MergeProgressEvent::OutputMetrics {
                trades,
                depth,
                reconstruction,
                canonical_rows,
            } => {
                self.output_metrics = Some(OutputMetrics {
                    trades,
                    depth,
                    reconstruction,
                    canonical_rows,
                });
            }
            MergeProgressEvent::Finished { error } => {
                self.finish(error.as_deref(), false);
            }
        }
    }

    fn finish(&mut self, error: Option<&str>, interrupted: bool) {
        self.finished = true;
        self.interrupted = interrupted;
        self.success = error.is_none();
        let now = Instant::now();
        self.ended = Some(now);
        for partition in self
            .partitions
            .values_mut()
            .filter(|p| p.status == PartitionStatus::Running)
        {
            partition.status = if interrupted {
                PartitionStatus::Interrupted
            } else {
                PartitionStatus::Failed
            };
            partition.ended = Some(now);
        }
        match error {
            None => self.log_activity("Publication committed".into(), GREEN),
            Some(message) => {
                self.log_activity(message.into(), if interrupted { YELLOW } else { RED })
            }
        }
    }

    fn update_snapshots(&mut self, state: &MergeProgressState) {
        if let Some(sample) = state.take_validation_sample()
            && self.validation.started
            && !self.validation.completed
        {
            self.handle_event(sample);
        }
        for (key, snapshot) in state.snapshots() {
            if let Some(partition) = self.partitions.get_mut(&key)
                && partition.status == PartitionStatus::Running
            {
                partition.update(&snapshot);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Partition statistics
    // -------------------------------------------------------------------------

    fn planned_partitions(&self) -> usize {
        self.phase
            .and_then(|phase| self.planned.get(&phase).copied())
            .unwrap_or(0)
    }

    fn completed_partitions(&self) -> usize {
        self.partitions
            .iter()
            .filter(|(key, partition)| {
                Some(key.phase) == self.phase && partition.status == PartitionStatus::Completed
            })
            .count()
    }

    fn failed_partitions(&self) -> usize {
        self.partitions
            .iter()
            .filter(|(key, partition)| {
                Some(key.phase) == self.phase && partition.status == PartitionStatus::Failed
            })
            .count()
    }

    fn active_partitions(&self) -> usize {
        self.partitions
            .iter()
            .filter(|(key, partition)| {
                Some(key.phase) == self.phase && partition.status == PartitionStatus::Running
            })
            .count()
    }

    fn total_events(&self) -> u64 {
        self.partitions
            .iter()
            .filter(|(key, _)| Some(key.phase) == self.phase)
            .map(|(_, partition)| partition.events)
            .sum()
    }

    fn events_for(&self, phase: MergePhase) -> u64 {
        self.partitions
            .iter()
            .filter(|(key, _)| key.phase == phase)
            .map(|(_, partition)| partition.events)
            .sum()
    }

    fn sample_throughput(&mut self) {
        let duration = self.last_throughput_sample.elapsed();
        if self.finished || duration < Duration::from_secs(1) {
            return;
        }
        let count = match self.phase {
            Some(MergePhase::LogicalMerge | MergePhase::PhysicalMaterialization) => {
                self.total_events()
            }
            Some(MergePhase::PhysicalValidation) if self.validation.started => {
                self.validation.events_validated
            }
            _ => return,
        };
        let delta = count.saturating_sub(self.last_sample);
        self.last_sample = count;
        self.last_throughput_sample = Instant::now();
        self.rate = (delta > 0).then_some(delta as f64 / duration.as_secs_f64());
        if let Some(rate) = self.rate {
            if self.throughput_history.len() >= HISTORY_LENGTH {
                self.throughput_history.pop_front();
            }
            self.throughput_history.push_back(rate as u64);
        }
    }

    fn log_activity(&mut self, message: String, color: Color) {
        let timestamp = format_duration(self.started.elapsed());

        if self.activity.len() >= ACTIVITY_LENGTH {
            self.activity.pop_front();
        }

        self.activity
            .push_back((format!("{timestamp}  {message}"), color));
    }
}

fn phase_name(phase: MergePhase) -> &'static str {
    match phase {
        MergePhase::InputResolution => "Checking inputs",
        MergePhase::Inventory => "Reading source inventory",
        MergePhase::BoundaryReconciliation => "Preparing depth boundaries",
        MergePhase::PartitionPlanning => "Planning timeline",
        MergePhase::LogicalMerge => "Building timeline",
        MergePhase::LogicalValidation => "Checking timeline",
        MergePhase::MaterializationPlanning => "Planning Parquet output",
        MergePhase::PhysicalMaterialization => "Writing Parquet",
        MergePhase::PhysicalValidation => "Verifying output",
        MergePhase::Checkpoints => "Preparing checkpoints",
        MergePhase::Manifest => "Writing manifest",
        MergePhase::Commit => "Committing publication",
        MergePhase::PublicationPreparation => "Preparing publication",
    }
}

fn utc(ns: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ns)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

fn render(frame: &mut ratatui::Frame, d: &MergeDashboard) {
    use ratatui::{
        layout::Alignment,
        text::{Line, Span},
    };
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(BG).fg(FG)), area);
    let [header, phase, main, footer] = Layout::vertical([
        Constraint::Length(if area.width >= 105 { 5 } else { 6 }),
        Constraint::Length(if d.details { 3 } else { 5 }),
        Constraint::Min(0),
        Constraint::Length(3),
    ])
    .areas(area);
    let (status, color) = if d.interrupted {
        ("INTERRUPTED", YELLOW)
    } else if d.finished && d.success {
        (
            if d.paths.managed {
                "ENGINE COMMITTED"
            } else {
                "COMMITTED"
            },
            GREEN,
        )
    } else if d.finished {
        ("FAILED", RED)
    } else {
        ("RUNNING", CYAN)
    };
    let current_phase_name = if d.finished && d.success {
        "Publication ready"
    } else {
        d.phase.map_or("Starting", phase_name)
    };
    let id = d.job_id.to_string();
    let id = if id.len() > 10 {
        format!("…{}", &id[id.len() - 8..])
    } else {
        id
    };
    let colors = |color| Style::default().fg(color);
    let mut lines = vec![
        Line::from(vec![
            Span::styled(status, colors(color).add_modifier(Modifier::BOLD)),
            Span::raw(format!("   {current_phase_name}   ·   Job {id}")),
        ]),
        Line::from(format!(
            "{} primary files   ·   {} streams   ·   {} publications   ·   {}",
            d.selected_files,
            d.concrete_streams.map_or("—".into(), |n| n.to_string()),
            d.input_datasets,
            d.content
        )),
    ];
    let start = utc(d.requested.start_ns);
    let end = utc(d.requested.end_ns);
    if area.width >= 105 {
        lines.push(Line::styled(
            format!("{start} → {end} UTC (end exclusive)   ·   {:?}", d.ordering),
            colors(MUTED),
        ));
    } else {
        lines.push(Line::styled(
            format!("{start} → {end} UTC (end exclusive)"),
            colors(MUTED),
        ));
        lines.push(Line::styled(
            format!("Timeline  {:?}", d.ordering),
            colors(MUTED),
        ));
    }
    frame.render_widget(
        Paragraph::new(lines).block(panel(" MarketForge / Merge ")),
        header,
    );

    let unit = match d.phase {
        Some(MergePhase::LogicalMerge) => "references",
        Some(MergePhase::PhysicalMaterialization | MergePhase::PhysicalValidation) => {
            "output events"
        }
        _ => "",
    };
    let rate = d.rate.filter(|_| !d.finished).map_or("—".into(), |rate| {
        format!("{} {unit}/s", compact(rate as u64))
    });
    let metrics = match d.phase {
        Some(MergePhase::InputResolution) => format!(
            "Primary files checked  {} / {}",
            d.verified_files, d.selected_files
        ),
        Some(MergePhase::LogicalMerge) => format!(
            "{} timeline references built   ·   Total unknown",
            compact(d.total_events())
        ),
        Some(MergePhase::PhysicalMaterialization) => format!(
            "Output events written  {} / {}",
            compact(d.total_events()),
            compact(d.events_for(MergePhase::LogicalMerge))
        ),
        Some(MergePhase::PhysicalValidation) if d.validation.started => format!(
            "Output events verified  {} / {}   ·   Files  {} / {}",
            compact(d.validation.events_validated),
            compact(d.validation.expected_events),
            d.validation.files_completed,
            d.validation.total_files
        ),
        _ => format!(
            "Timeline check  {}   ·   Output verification  {}",
            d.phase_status(MergePhase::LogicalValidation),
            d.phase_status(MergePhase::PhysicalValidation)
        ),
    };
    let metrics = if let Some(output) = &d.output_metrics {
        format!(
            "{} trades   ·   {} depth   ·   {} context events",
            compact(output.trades),
            compact(output.depth),
            compact(output.reconstruction)
        )
    } else {
        metrics
    };
    let queue = if d.is_partition_phase() && d.planned_partitions() > 0 {
        format!(
            "{} queued   ·   {} / {} partitions done",
            d.planned_partitions().saturating_sub(
                d.active_partitions() + d.completed_partitions() + d.failed_partitions()
            ),
            d.completed_partitions(),
            d.planned_partitions()
        )
    } else {
        "No partition queue for this phase".into()
    };
    let mut progress = vec![
        Line::styled(metrics, colors(color)),
        Line::from(format!(
            "Phase {}   ·   Total {}   ·   Rate {rate}",
            format_duration(d.phase_time()),
            format_duration(d.elapsed())
        )),
        Line::styled(
            format!(
                "{} active / {} worker cap   ·   {queue}",
                d.active_partitions(),
                d.workers
            ),
            colors(MUTED),
        ),
    ];
    if d.details {
        progress.truncate(1);
    }
    frame.render_widget(Paragraph::new(progress).block(panel(" Progress ")), phase);

    if d.details {
        let mut detail = vec![
            Line::from(format!(
                "Job {}   ·   {} ",
                d.job_id,
                d.phase.map_or("STARTING", MergePhase::label)
            )),
            Line::from(format!(
                "Requested ns  [{} , {})",
                d.requested.start_ns, d.requested.end_ns
            )),
            Line::from(format!("Source TOML  {}", d.paths.origin.display())),
            Line::from(format!("Loaded TOML  {}", d.paths.loaded.display())),
            Line::from(format!("TOML SHA-256  {}", d.paths.checksum)),
            Line::from(format!(
                "Worker cap {}   ·   Memory {} MiB   ·   Open files {}",
                d.workers, d.memory, d.open_files
            )),
            Line::from(format!(
                "Parquet  row group {} MiB   ·   file {} MiB   ·   ZSTD {}",
                d.row_group, d.file_target, d.compression
            )),
            Line::from(format!(
                "Chronological target  {} partitions/worker",
                d.partitions_per_worker
            )),
            Line::from(format!(
                "Timeline check  {}   ·   Output verification  {}",
                d.phase_status(MergePhase::LogicalValidation),
                d.phase_status(MergePhase::PhysicalValidation)
            )),
            Line::from(format!(
                "Planned verification  {} partitions   ·   {} row groups checked",
                d.validation.total_partitions, d.validation.row_groups_validated
            )),
            Line::from(format!("Output  {}", d.output.display())),
            Line::from(format!("Report  {}", d.paths.report.display())),
        ];
        if let Some(output) = &d.output_metrics {
            detail.push(Line::from(format!(
                "Canonical payload rows  {}",
                output.canonical_rows
            )));
        }
        detail.push(Line::styled(
            "Caps are configured limits. Live memory and open-file usage are unavailable.",
            colors(MUTED),
        ));
        if d.paths.managed && d.success && d.finished {
            detail.push(Line::styled(
                "Forge verification and registration follow the engine commit.",
                colors(MUTED),
            ));
        }
        detail.extend(
            d.activity
                .iter()
                .rev()
                .filter(|(_, color)| *color == RED || *color == YELLOW)
                .map(|(message, color)| Line::styled(message, colors(*color))),
        );
        frame.render_widget(
            Paragraph::new(detail)
                .wrap(Wrap { trim: false })
                .scroll((d.scroll, 0))
                .block(panel(" Details / ↑ ↓ to scroll ")),
            main,
        );
    } else {
        let (table, sidebar) = if area.width >= 105 {
            let [table, sidebar] =
                Layout::horizontal([Constraint::Percentage(62), Constraint::Percentage(38)])
                    .areas(main);
            (table, sidebar)
        } else {
            let [table, sidebar] =
                Layout::vertical([Constraint::Min(4), Constraint::Length(4)]).areas(main);
            (table, sidebar)
        };
        if d.is_partition_phase() && d.planned_partitions() > 0 {
            let mut rows = d
                .partitions
                .iter()
                .filter(|(key, _)| Some(key.phase) == d.phase)
                .collect::<Vec<_>>();
            rows.sort_unstable_by_key(|(key, value)| {
                (value.status != PartitionStatus::Running, key.partition_id)
            });
            let numeric = |text: String| Cell::from(Line::from(text).alignment(Alignment::Right));
            let mut rows = rows
                .into_iter()
                .map(|(key, p)| {
                    let (label, color) = match p.status {
                        PartitionStatus::Pending => ("Queued", MUTED),
                        PartitionStatus::Running => ("Active", CYAN),
                        PartitionStatus::Completed => ("Done", GREEN),
                        PartitionStatus::Failed => ("Failed", RED),
                        PartitionStatus::Interrupted => ("Interrupted", YELLOW),
                    };
                    Row::new([
                        Cell::from(format!("#{}", key.partition_id)),
                        Cell::from(label).style(colors(color)),
                        numeric(compact(p.events)),
                        numeric(p.rate().map_or("—".into(), |r| compact(r as u64))),
                        numeric(format_duration(
                            p.ended
                                .unwrap_or_else(Instant::now)
                                .duration_since(p.started),
                        )),
                        numeric(
                            p.last_report
                                .map_or("—".into(), |t| format_duration(t.elapsed())),
                        ),
                    ])
                })
                .collect::<Vec<_>>();
            let queued = d.planned_partitions().saturating_sub(
                d.active_partitions() + d.completed_partitions() + d.failed_partitions(),
            );
            if queued > 0 && !d.finished {
                rows.insert(
                    d.active_partitions(),
                    Row::new([format!("{queued} queued"), "Awaiting start".into()]),
                );
            }
            frame.render_widget(
                Table::new(
                    rows,
                    [
                        Constraint::Length(12),
                        Constraint::Length(15),
                        Constraint::Min(8),
                        Constraint::Length(9),
                        Constraint::Length(8),
                        Constraint::Length(8),
                    ],
                )
                .header(
                    Row::new([
                        Cell::from("Partition"),
                        Cell::from("State"),
                        numeric("Work".into()),
                        numeric("Work/s".into()),
                        numeric("Elapsed".into()),
                        numeric("Age".into()),
                    ])
                    .style(colors(MUTED)),
                )
                .block(panel(" Partition work ")),
                table,
            );
        } else {
            let pipeline = [
                MergePhase::InputResolution,
                MergePhase::LogicalMerge,
                MergePhase::LogicalValidation,
                MergePhase::PhysicalMaterialization,
                MergePhase::PhysicalValidation,
                MergePhase::Commit,
            ]
            .into_iter()
            .map(|phase| {
                let status = d.phase_status(phase);
                let color = match status {
                    "Done" => GREEN,
                    "Active" => CYAN,
                    "Failed" => RED,
                    "Interrupted" => YELLOW,
                    _ => MUTED,
                };
                Line::from(vec![
                    Span::styled(format!("{status:12}"), colors(color)),
                    Span::raw(phase_name(phase)),
                ])
            })
            .collect::<Vec<_>>();
            frame.render_widget(
                Paragraph::new(pipeline).block(panel(" Merge stages ")),
                table,
            );
        }
        let history_height = if sidebar.height >= 10 && area.width >= 105 {
            4
        } else {
            0
        };
        let [activity, history] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(history_height)])
                .areas(sidebar);
        let activities = d
            .activity
            .iter()
            .rev()
            .take(activity.height.saturating_sub(2) as usize)
            .map(|(text, color)| Line::styled(text.as_str(), colors(*color)))
            .collect::<Vec<_>>();
        frame.render_widget(
            Paragraph::new(activities).block(panel(" Activity ")),
            activity,
        );
        if history_height > 0 {
            let samples = d.throughput_history.iter().copied().collect::<Vec<_>>();
            let title = if samples.is_empty() {
                " Throughput history · waiting for samples "
            } else {
                " Throughput history · work/s "
            };
            frame.render_widget(
                Sparkline::default()
                    .data(&samples)
                    .style(colors(CYAN))
                    .block(panel(title)),
                history,
            );
        }
    }
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                format!(
                    "Config {}   ·   Output {}",
                    d.paths
                        .origin
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy(),
                    d.output.display()
                ),
                colors(MUTED),
            ),
            Line::styled(
                format!(
                    "D details   ·   Ctrl+C interrupt   ·   Report {}",
                    d.paths.report.display()
                ),
                colors(MUTED),
            ),
        ])
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(colors(MUTED)),
        ),
        footer,
    );
}

pub fn execute_merge_with_tui(
    job: MergeJob,
    project_root: std::path::PathBuf,
    paths: MergeTuiPaths,
) -> Result<UnifiedMergeExecutionResult, Box<dyn std::error::Error>> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("merge TUI requires an interactive terminal".into());
    }

    let _diagnostics = paths
        .managed
        .then(|| {
            crate::historical::normalize::progress::log::DashboardLog::enter_managed(
                &paths.report.with_file_name("engine-tui.log"),
            )
            .ok()
        })
        .flatten();
    let _signals = InterruptSignals::install()?;
    // Initialize terminal before launching the merge.
    let guard = TerminalGuard::enter()?;

    let backend = CrosstermBackend::new(io::stdout());

    let mut terminal = match Terminal::new(backend) {
        Ok(terminal) => terminal,
        Err(error) => {
            drop(guard);
            return Err(Box::new(error));
        }
    };

    // Create progress communication infrastructure.
    let (sender, receiver) = mpsc::channel();

    let reporter = MergeProgressReporter::new(sender);
    let progress_state = reporter.state();

    // Initialize dashboard.
    let mut dashboard = MergeDashboard::new(&job, paths);

    // Launch the existing unified merge engine.
    let worker = thread::spawn(move || {
        execute_unified_merge_job_with_progress(&job, &project_root, &reporter)
    });

    // Render live progress until the worker finishes.
    let dashboard_result = run_dashboard(
        &mut terminal,
        &mut dashboard,
        &receiver,
        &progress_state,
        &worker,
    );

    // Restore terminal before displaying results.
    drop(terminal);
    drop(guard);

    if dashboard_result
        .as_ref()
        .is_err_and(|e| e.kind() == io::ErrorKind::Interrupted)
    {
        // The foreground CLI exits immediately, leaving recovery artifacts.
        return Err(Box::new(dashboard_result.unwrap_err()));
    }
    let merge_result = worker.join();
    dashboard_result?;
    let result = merge_result.map_err(|_| "merge worker thread panicked")??;

    if !dashboard.paths.managed {
        print_merge_summary(&dashboard, &result);
    }

    Ok(result)
}
// -----------------------------------------------------------------------------
// Dashboard refresh loop
// -----------------------------------------------------------------------------

fn run_dashboard<T>(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    dashboard: &mut MergeDashboard,
    receiver: &Receiver<MergeProgressEvent>,
    progress_state: &Arc<MergeProgressState>,
    worker: &thread::JoinHandle<T>,
) -> io::Result<()>
where
    T: Send,
{
    let mut last_draw = None;
    loop {
        if TerminalGuard::interrupted_by_panic() {
            return Err(io::Error::other("merge worker panicked"));
        }
        drain_events(dashboard, receiver);
        if worker.is_finished() {
            // Finished is a lifecycle event, sent before the worker returns.
            drain_events(dashboard, receiver);
            if !dashboard.finished {
                dashboard.finish(Some("Worker exited without reporting a result"), false);
            }
            dashboard.update_snapshots(progress_state);
            terminal.draw(|frame| render(frame, dashboard))?;
            break;
        }

        if InterruptSignals::requested() {
            return interrupt_dashboard(terminal, dashboard);
        }

        if last_draw.is_none_or(|draw: Instant| draw.elapsed() >= REFRESH_INTERVAL) {
            dashboard.update_snapshots(progress_state);
            dashboard.sample_throughput();
            terminal.draw(|frame| render(frame, dashboard))?;
            last_draw = Some(Instant::now());
        }

        let wait = REFRESH_INTERVAL.saturating_sub(last_draw.unwrap().elapsed());
        if event::poll(wait)?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return interrupt_dashboard(terminal, dashboard);
                }
                KeyCode::Char('d' | 'D') => {
                    dashboard.details = !dashboard.details;
                    dashboard.scroll = 0;
                }
                KeyCode::Up if dashboard.details => {
                    dashboard.scroll = dashboard.scroll.saturating_sub(1)
                }
                KeyCode::Down if dashboard.details => {
                    dashboard.scroll = dashboard.scroll.saturating_add(1)
                }
                _ => {}
            }
        }
    }

    Ok(())
}
fn drain_events(dashboard: &mut MergeDashboard, receiver: &Receiver<MergeProgressEvent>) {
    while let Ok(event) = receiver.try_recv() {
        dashboard.handle_event(event);
    }
}

fn interrupt_dashboard(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    dashboard: &mut MergeDashboard,
) -> io::Result<()> {
    dashboard.finish(Some("Merge interrupted"), true);
    let _ = terminal.draw(|frame| render(frame, dashboard));
    Err(io::Error::new(
        io::ErrorKind::Interrupted,
        "interrupted before engine completion",
    ))
}

// -----------------------------------------------------------------------------
// Final merge summary
// -----------------------------------------------------------------------------

fn print_merge_summary(dashboard: &MergeDashboard, result: &UnifiedMergeExecutionResult) {
    let mut stderr = io::stderr().lock();

    let _ = writeln!(
        stderr,
        "Merge committed · job {} · {} · {} output events · {} Parquet files",
        result.job_id,
        format_duration(dashboard.elapsed()),
        result.events_written,
        result.files_written
    );
    let _ = writeln!(stderr, "Publication {}", result.dataset_path.display());
    let _ = writeln!(stderr, "Manifest {}", result.manifest_path.display());
}

#[cfg(test)]
mod merge_dashboard_tests {
    use super::*;
    use crate::historical::job::{
        MergeOperation, MergeOutput, MergeParquetConfig, MergePartitioning, MergeResources,
        MergeTimeRange,
    };
    use ratatui::backend::TestBackend;

    fn dashboard() -> MergeDashboard {
        let job = MergeJob {
            job_id: 10,
            operation: MergeOperation::Merge,
            inputs: vec![1],
            input_paths: HashMap::new(),
            primary_files: HashMap::new(),
            submission: None,
            time_range: MergeTimeRange {
                start_ns: 50,
                end_ns: 90,
            },
            resources: MergeResources {
                workers: 2,
                memory_limit_mb: 256,
                max_open_files: 32,
            },
            partitioning: MergePartitioning {
                partitions_per_worker: 1,
            },
            parquet: MergeParquetConfig {
                row_group_target_mb: 1,
                file_target_mb: 4,
                compression_level: 3,
            },
            output: MergeOutput {
                dataset_path: "/temporary/output".into(),
            },
        };
        MergeDashboard::new(
            &job,
            MergeTuiPaths {
                origin: "/test/general.toml".into(),
                loaded: "/test/pinned.toml".into(),
                checksum: "checksum-for-test".into(),
                report: "/test/report.json".into(),
                managed: true,
            },
        )
    }

    fn frame_text(terminal: &Terminal<TestBackend>) -> String {
        let buffer = terminal.backend().buffer();
        buffer
            .content
            .chunks(buffer.area.width as usize)
            .map(|row| {
                row.iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn merge_dashboard_keeps_progress_and_outcomes_distinct_at_three_sizes() {
        let mut dashboard = dashboard();
        let (sender, events) = mpsc::channel();
        let reporter = MergeProgressReporter::new(sender);
        let state = reporter.state();
        let phase = MergePhase::LogicalMerge;
        reporter.phase_started(phase);
        reporter.partitions_planned(phase, 37);
        for partition_id in 0..2 {
            let key = PartitionKey {
                phase,
                partition_id,
            };
            reporter.partition_started(key);
            crate::historical::merge::MergeWorkerProgress::new(&reporter, key).finish(1_000_000);
        }
        drain_events(&mut dashboard, &events);
        dashboard.update_snapshots(&state);
        dashboard.selected_files = 93;
        dashboard.concrete_streams = Some(40);
        dashboard.input_datasets = 2;
        dashboard.content = "Trades + depth";
        dashboard.requested = MergeTimeRange {
            start_ns: 1793059200000000000,
            end_ns: 1793318400000000000,
        };
        dashboard.output =
            "/a/very/long/workspace/datasets/with/many/components/for/merge/2026/temporary/output"
                .into();
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        for (width, height) in [(80, 24), (120, 36), (160, 48)] {
            terminal.backend_mut().resize(width, height);
            terminal.draw(|frame| render(frame, &dashboard)).unwrap();
            let screen = frame_text(&terminal);
            assert!(screen.contains("93 primary files"), "{screen}");
            assert!(screen.contains("40 streams"));
            assert!(screen.contains("2 publications"));
            assert!(screen.contains("end exclusive"));
            assert!(screen.contains("2.00M timeline references built"));
            assert!(screen.contains("2 active / 2 worker cap"));
            assert!(screen.contains("35 queued"));
            assert!(screen.contains("0 / 37 partitions done"));
            assert!(screen.contains("D details"));
            if width == 80 {
                println!("{screen}");
            }
            dashboard.details = true;
            terminal.draw(|frame| render(frame, &dashboard)).unwrap();
            let screen = frame_text(&terminal);
            assert!(screen.contains("Source TOML  /test/general.toml"));
            assert!(screen.contains("Loaded TOML  /test/pinned.toml"));
            assert!(screen.contains("Memory 256 MiB"));
            assert!(screen.contains("Output verification  Pending"));
            if width >= 120 {
                assert!(screen.contains("TOML SHA-256  checksum-for-test"));
                assert!(screen.contains("row group 1 MiB"));
                assert!(!screen.contains("Canonical payload rows"));
            }
            dashboard.details = false;
        }
        for partition_id in 0..2 {
            reporter.partition_completed(
                PartitionKey {
                    phase,
                    partition_id,
                },
                1_000_000,
            );
        }
        reporter.phase_completed(phase);
        reporter.phase_started(MergePhase::PhysicalValidation);
        reporter.validation_started(1, 1, 2_000_000);
        reporter.validation_progress(0, 0, 350, 0);
        reporter.validation_progress(0, 0, 1000, 0);
        reporter.validation_completed(1, 1, 2_000_000, 1);
        reporter.phase_completed(MergePhase::PhysicalValidation);
        reporter.phase_started(MergePhase::Commit);
        reporter.phase_completed(MergePhase::Commit);
        reporter.finished(None);
        drain_events(&mut dashboard, &events);
        dashboard.update_snapshots(&state);
        assert_eq!(dashboard.validation.events_validated, 2_000_000);
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal.draw(|frame| render(frame, &dashboard)).unwrap();
        let screen = frame_text(&terminal);
        assert!(screen.contains("ENGINE COMMITTED") && !screen.contains("RUNNING"));
        dashboard.finish(Some("Merge interrupted"), true);
        terminal.draw(|frame| render(frame, &dashboard)).unwrap();
        let screen = frame_text(&terminal);
        assert!(screen.contains("INTERRUPTED") && !screen.contains("RUNNING"));
    }

    #[test]
    fn merge_dashboard_tracks_phase_qualified_partitions_and_renders_narrow() {
        let mut dashboard = dashboard();
        let logical = PartitionKey {
            phase: MergePhase::LogicalMerge,
            partition_id: 0,
        };
        let physical = PartitionKey {
            phase: MergePhase::PhysicalMaterialization,
            partition_id: 0,
        };
        dashboard.handle_event(MergeProgressEvent::PhaseStarted {
            phase: logical.phase,
        });
        dashboard.handle_event(MergeProgressEvent::PartitionStarted { key: logical });
        dashboard.handle_event(MergeProgressEvent::PartitionCompleted {
            key: logical,
            events: 10,
        });
        dashboard.handle_event(MergeProgressEvent::PartitionStarted { key: logical });
        dashboard.handle_event(MergeProgressEvent::PhaseStarted {
            phase: physical.phase,
        });
        dashboard.handle_event(MergeProgressEvent::PartitionStarted { key: physical });
        let (sender, _receiver) = mpsc::channel();
        let reporter = MergeProgressReporter::new(sender);
        let progress_state = reporter.state();
        let worker = crate::historical::merge::MergeWorkerProgress::new(&reporter, physical);
        worker.finish(4);
        dashboard.update_snapshots(&progress_state);
        assert_eq!(dashboard.partitions[&physical].events, 4);
        dashboard.handle_event(MergeProgressEvent::Finished {
            error: Some("commit failed".to_owned()),
        });
        assert_eq!(
            dashboard.partitions[&logical].status,
            PartitionStatus::Completed
        );
        assert_eq!(
            dashboard.partitions[&physical].status,
            PartitionStatus::Failed
        );
        assert!(!dashboard.success);
        let mut terminal = Terminal::new(TestBackend::new(64, 16)).unwrap();
        terminal.draw(|frame| render(frame, &dashboard)).unwrap();
        let cells = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(cells.contains("FAILED"));
        assert!(cells.contains("/temporary/output"));
    }
}
