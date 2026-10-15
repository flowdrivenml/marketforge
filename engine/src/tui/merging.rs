use std::{
    collections::{HashMap, VecDeque},
    io::{self, IsTerminal, Stdout, Write},
    sync::{
        Arc,
        mpsc::{self, Receiver, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};

use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Gauge, Paragraph, Row, Sparkline, Table},
};

use crate::{
    job::MergeJob,
    process::merge::{
        MergePhase, MergeProgressEvent, MergeProgressReporter, MergeProgressState, PartitionKey,
        PartitionProgress, UnifiedMergeExecutionResult, execute_unified_merge_job_with_progress,
    },
};

// -----------------------------------------------------------------------------
// Theme — Tokyo Night
// -----------------------------------------------------------------------------

const BG: Color = Color::Rgb(26, 27, 38);
const FG: Color = Color::Rgb(192, 202, 245);
const BLUE: Color = Color::Rgb(122, 162, 247);
const CYAN: Color = Color::Rgb(125, 207, 255);
const GREEN: Color = Color::Rgb(158, 206, 106);
const RED: Color = Color::Rgb(247, 118, 142);
const MUTED: Color = Color::Rgb(86, 95, 137);
const YELLOW: Color = Color::Rgb(224, 175, 104);
const REFRESH_INTERVAL: Duration = Duration::from_millis(250);
const HISTORY_LENGTH: usize = 60;
const ACTIVITY_LENGTH: usize = 12;

// -----------------------------------------------------------------------------
// Terminal guard
// -----------------------------------------------------------------------------

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;

        if let Err(error) = execute!(io::stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error);
        }

        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

// -----------------------------------------------------------------------------
// Partition status
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

// -----------------------------------------------------------------------------
// Partition state
// -----------------------------------------------------------------------------

#[derive(Debug)]
struct PartitionState {
    status: PartitionStatus,

    events: u64,
    speed: f64,

    last_events: u64,
    last_sample: Instant,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            status: PartitionStatus::Pending,
            events: 0,
            speed: 0.0,
            last_events: 0,
            last_sample: Instant::now(),
        }
    }

    fn update(&mut self, snapshot: &PartitionProgress) {
        let now = Instant::now();

        let delta = snapshot.events.saturating_sub(self.last_events);

        if delta > 0 {
            let elapsed = now.duration_since(self.last_sample).as_secs_f64();

            if elapsed > 0.0 {
                let instantaneous = delta as f64 / elapsed;

                const ALPHA: f64 = 0.35;

                self.speed = if self.speed == 0.0 {
                    instantaneous
                } else {
                    ALPHA * instantaneous + (1.0 - ALPHA) * self.speed
                };
            }

            self.last_events = snapshot.events;
            self.last_sample = now;
        } else if now.duration_since(self.last_sample) >= Duration::from_secs(2) {
            self.speed = 0.0;
        }

        self.events = snapshot.events;
    }
}

// -----------------------------------------------------------------------------
// Dashboard state
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// Physical validation state
// -----------------------------------------------------------------------------

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

impl ValidationState {
    fn progress_ratio(&self) -> f64 {
        if self.total_files > 0 {
            self.files_completed as f64 / self.total_files as f64
        } else if self.total_partitions > 0 {
            self.partitions_completed as f64 / self.total_partitions as f64
        } else if self.completed {
            1.0
        } else {
            0.0
        }
        .clamp(0.0, 1.0)
    }
}
struct MergeDashboard {
    job_id: u64,

    workers: usize,
    input_datasets: usize,

    phase: Option<MergePhase>,
    completed_phases: Vec<MergePhase>,

    partitions: HashMap<PartitionKey, PartitionState>,
    planned: HashMap<MergePhase, usize>,

    started: Instant,

    finished: bool,
    success: bool,

    throughput_history: VecDeque<u64>,
    last_throughput_sample: Instant,

    activity: VecDeque<String>,
    validation: ValidationState,
}

impl MergeDashboard {
    fn new(job: &MergeJob) -> Self {
        Self {
            job_id: job.job_id,

            workers: job.resources.workers,
            input_datasets: job.inputs.len(),

            phase: None,
            completed_phases: Vec::new(),

            partitions: HashMap::new(),
            planned: HashMap::new(),

            started: Instant::now(),

            finished: false,
            success: false,

            throughput_history: VecDeque::with_capacity(HISTORY_LENGTH),

            last_throughput_sample: Instant::now(),

            activity: VecDeque::with_capacity(ACTIVITY_LENGTH),
            validation: ValidationState::default(),
        }
    }
    fn current_progress(&self) -> (f64, String) {
        match self.phase {
            Some(MergePhase::LogicalMerge) | Some(MergePhase::PhysicalMaterialization) => {
                let planned = self.planned_partitions();

                let completed = self.completed_partitions() + self.failed_partitions();

                let ratio = if planned == 0 {
                    0.0
                } else {
                    completed as f64 / planned as f64
                };

                (
                    ratio.clamp(0.0, 1.0),
                    format!("PARTITIONS  {} / {}", completed, planned),
                )
            }

            Some(MergePhase::PhysicalValidation) => {
                let ratio = self.validation.progress_ratio();

                (
                    ratio,
                    format!(
                        "VALIDATION  {} / {} FILES",
                        self.validation.files_completed, self.validation.total_files,
                    ),
                )
            }

            Some(phase) => {
                let ratio = self.pipeline_progress();

                (
                    ratio,
                    format!("PIPELINE  {:.1}%  —  {}", ratio * 100.0, phase.label(),),
                )
            }

            None => (0.0, "INITIALIZING".to_owned()),
        }
    }
    fn pipeline_progress(&self) -> f64 {
        const PHASES: &[MergePhase] = &[
            MergePhase::InputResolution,
            MergePhase::Inventory,
            MergePhase::LogicalMerge,
            MergePhase::LogicalValidation,
            MergePhase::PhysicalMaterialization,
            MergePhase::PhysicalValidation,
            MergePhase::Checkpoints,
            MergePhase::Manifest,
            MergePhase::PublicationPreparation,
            MergePhase::Commit,
        ];

        if self.finished && self.success {
            return 1.0;
        }

        let completed = PHASES
            .iter()
            .filter(|&&phase| self.phase_completed(phase))
            .count();

        completed as f64 / PHASES.len() as f64
    }
    // -------------------------------------------------------------------------
    // Progress events
    // -------------------------------------------------------------------------
    fn is_partition_phase(&self) -> bool {
        matches!(
            self.phase,
            Some(MergePhase::LogicalMerge | MergePhase::PhysicalMaterialization)
        )
    }

    fn phase_running(&self, phase: MergePhase) -> bool {
        self.phase == Some(phase) && !self.phase_completed(phase) && !self.finished
    }
    fn handle_event(&mut self, event: MergeProgressEvent) {
        match event {
            MergeProgressEvent::PartitionsPlanned { phase, count } => {
                self.planned.insert(phase, count);

                self.log_activity(format!(
                    "● {} — {} partitions planned",
                    phase.label(),
                    count,
                ));
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

                self.log_activity(format!(
                    "● Validating {} partitions, {} Parquet files",
                    total_partitions, total_files,
                ));
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

                self.log_activity(format!(
                    "✓ Validated {} events across {} Parquet files",
                    compact(events_validated),
                    files_completed,
                ));
            }
            MergeProgressEvent::PhaseCompleted { phase } => {
                if !self.completed_phases.contains(&phase) {
                    self.completed_phases.push(phase);
                }

                self.log_activity(format!("✓ {} completed", phase.label(),));
            }

            MergeProgressEvent::PhaseStarted { phase } => {
                self.phase = Some(phase);

                // Start a fresh throughput graph for each processing phase.
                if matches!(
                    phase,
                    MergePhase::LogicalMerge | MergePhase::PhysicalMaterialization
                ) {
                    self.throughput_history.clear();
                    self.last_throughput_sample = Instant::now();
                }

                self.log_activity(format!("● {} started", phase.label(),));
            }

            MergeProgressEvent::PartitionStarted { key } => {
                let partition = self
                    .partitions
                    .entry(key)
                    .or_insert_with(PartitionState::new);

                partition.status = PartitionStatus::Running;
                partition.last_sample = Instant::now();
            }

            MergeProgressEvent::PartitionCompleted { key, events } => {
                let partition = self
                    .partitions
                    .entry(key)
                    .or_insert_with(PartitionState::new);

                partition.status = PartitionStatus::Completed;
                partition.events = events;
                partition.speed = 0.0;

                self.log_activity(format!(
                    "✓ {} partition {} completed — {} events",
                    key.phase.label(),
                    key.partition_id,
                    compact(events),
                ));
            }

            MergeProgressEvent::PartitionFailed { key, message } => {
                let partition = self
                    .partitions
                    .entry(key)
                    .or_insert_with(PartitionState::new);

                partition.status = PartitionStatus::Failed;
                partition.speed = 0.0;

                self.log_activity(format!(
                    "✗ {} partition {} failed — {}",
                    key.phase.label(),
                    key.partition_id,
                    message,
                ));
            }

            MergeProgressEvent::Finished { success } => {
                self.finished = true;
                self.success = success;

                self.log_activity(if success {
                    "✓ Merge completed successfully".to_owned()
                } else {
                    "✗ Merge failed".to_owned()
                });
            }
        }
    }
    fn phase_completed(&self, phase: MergePhase) -> bool {
        self.completed_phases.contains(&phase)
    }

    // fn phase_running(&self, phase: MergePhase) -> bool {
    //     self.phase == Some(phase) && !self.phase_completed(phase) && !self.finished
    // }
    //
    // fn is_partition_phase(&self) -> bool {
    //     matches!(
    //         self.phase,
    //         Some(MergePhase::LogicalMerge | MergePhase::PhysicalMaterialization)
    //     )
    // }
    // -------------------------------------------------------------------------
    // Atomic counter snapshots
    // -------------------------------------------------------------------------

    fn update_snapshots(&mut self, state: &MergeProgressState) {
        for (key, snapshot) in state.snapshots() {
            if let Some(partition) = self.partitions.get_mut(&key) {
                if partition.status == PartitionStatus::Running {
                    partition.update(&snapshot);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Partition statistics
    // -------------------------------------------------------------------------

    fn current_phase(&self) -> Option<MergePhase> {
        self.phase
    }

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

    fn total_speed(&self) -> f64 {
        self.partitions
            .iter()
            .filter(|(key, _)| Some(key.phase) == self.phase)
            .map(|(_, partition)| partition.speed)
            .sum()
    }

    // -------------------------------------------------------------------------
    // Throughput history
    // -------------------------------------------------------------------------

    fn sample_throughput(&mut self) {
        if self.last_throughput_sample.elapsed() < Duration::from_secs(1) {
            return;
        }

        self.last_throughput_sample = Instant::now();

        let throughput = self.total_speed().max(0.0) as u64;

        if self.throughput_history.len() >= HISTORY_LENGTH {
            self.throughput_history.pop_front();
        }

        self.throughput_history.push_back(throughput);
    }

    // -------------------------------------------------------------------------
    // Activity log
    // -------------------------------------------------------------------------

    fn log_activity(&mut self, message: String) {
        let timestamp = format_duration(self.started.elapsed());

        if self.activity.len() >= ACTIVITY_LENGTH {
            self.activity.pop_front();
        }

        self.activity.push_back(format!("{timestamp}  {message}"));
    }
}

// -----------------------------------------------------------------------------
// Formatting helpers
// -----------------------------------------------------------------------------

fn compact(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.2}B", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.2}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn format_speed(value: f64) -> String {
    format!("{}/s", compact(value.max(0.0) as u64))
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();

    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60,
    )
}
// -----------------------------------------------------------------------------
// Header
// -----------------------------------------------------------------------------

fn render_header(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let status = if dashboard.finished {
        if dashboard.success {
            "COMPLETE"
        } else {
            "FAILED"
        }
    } else if let Some(phase) = dashboard.current_phase() {
        if dashboard.phase_completed(phase) {
            "COMPLETED"
        } else {
            "RUNNING"
        }
    } else {
        "INITIALIZING"
    };

    let status_color = if dashboard.finished {
        if dashboard.success { GREEN } else { RED }
    } else {
        CYAN
    };

    let phase = dashboard
        .current_phase()
        .map(MergePhase::label)
        .unwrap_or("INITIALIZING");

    let title = format!(" ◆ MARKETFORGE  │  MERGING  │  JOB #{} ", dashboard.job_id,);

    let status_text = format!(" {phase}  ● {status} ");

    let sections = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    frame.render_widget(
        Paragraph::new(title)
            .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD))
            .block(
                Block::default()
                    .borders(Borders::TOP | Borders::LEFT | Borders::BOTTOM)
                    .border_style(Style::default().fg(BLUE)),
            ),
        sections[0],
    );

    frame.render_widget(
        Paragraph::new(status_text)
            .alignment(ratatui::layout::Alignment::Right)
            .style(
                Style::default()
                    .fg(status_color)
                    .add_modifier(Modifier::BOLD),
            )
            .block(
                Block::default()
                    .borders(Borders::TOP | Borders::RIGHT | Borders::BOTTOM)
                    .border_style(Style::default().fg(BLUE)),
            ),
        sections[1],
    );
}
// -----------------------------------------------------------------------------
// Session overview
// -----------------------------------------------------------------------------

fn render_overview(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let planned = dashboard.planned_partitions();
    let completed = dashboard.completed_partitions();
    let failed = dashboard.failed_partitions();
    let active = dashboard.active_partitions();

    let pending = planned.saturating_sub(completed + failed + active);

    let progress_text = if dashboard.phase == Some(MergePhase::PhysicalValidation) {
        format!(
            "Validation  {} / {}\n\
             Files       {} / {}\n\
             Elapsed     {}",
            dashboard.validation.partitions_completed,
            dashboard.validation.total_partitions,
            dashboard.validation.files_completed,
            dashboard.validation.total_files,
            format_duration(dashboard.started.elapsed()),
        )
    } else if dashboard.is_partition_phase() {
        format!(
            "Partitions  {} / {}\n\
             Active      {}\n\
             Elapsed     {}",
            dashboard.completed_partitions() + dashboard.failed_partitions(),
            dashboard.planned_partitions(),
            dashboard.active_partitions(),
            format_duration(dashboard.started.elapsed()),
        )
    } else {
        format!(
            "Phase       {}\n\
             Status      {}\n\
             Elapsed     {}",
            dashboard
                .current_phase()
                .map(MergePhase::label)
                .unwrap_or("INITIALIZING"),
            if dashboard
                .current_phase()
                .is_some_and(|phase| dashboard.phase_completed(phase))
            {
                "COMPLETE"
            } else {
                "RUNNING"
            },
            format_duration(dashboard.started.elapsed()),
        )
    };
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Length(2)])
        .split(area);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(34),
            Constraint::Percentage(33),
        ])
        .split(sections[0]);

    let session = Paragraph::new(progress_text)
        .style(Style::default().fg(FG))
        .block(
            Block::default()
                .title(" SESSION ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BLUE)),
        );

    let performance = Paragraph::new(format!(
        "Throughput  {}\n\
         Events      {}\n\
         Inputs      {}",
        format_speed(dashboard.total_speed()),
        compact(dashboard.total_events()),
        dashboard.input_datasets,
    ))
    .style(Style::default().fg(FG))
    .block(
        Block::default()
            .title(" PERFORMANCE ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    let resources = Paragraph::new(format!(
        "Workers     {} / {}\n\
         Pending     {}\n\
         Failed      {}",
        active, dashboard.workers, pending, failed,
    ))
    .style(Style::default().fg(FG))
    .block(
        Block::default()
            .title(" RESOURCES ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(session, columns[0]);
    frame.render_widget(performance, columns[1]);
    frame.render_widget(resources, columns[2]);

    let (progress_ratio, progress_label) = dashboard.current_progress();

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(CYAN).bg(MUTED))
        .ratio(progress_ratio)
        .label(progress_label);

    frame.render_widget(gauge, sections[1]);
}
// -----------------------------------------------------------------------------
// Active partitions
// -----------------------------------------------------------------------------

fn render_partitions(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let mut active: Vec<_> = dashboard
        .partitions
        .iter()
        .filter(|(key, partition)| {
            Some(key.phase) == dashboard.phase && partition.status == PartitionStatus::Running
        })
        .collect();

    active.sort_by_key(|(key, _)| key.partition_id);

    let rows = active.into_iter().map(|(key, partition)| {
        Row::new(vec![
            format!("{:06}", key.partition_id),
            compact(partition.events),
            format_speed(partition.speed),
            format_duration(partition.last_sample.elapsed()),
            "RUNNING".to_owned(),
        ])
        .style(Style::default().fg(FG))
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(18),
            Constraint::Length(18),
            Constraint::Length(14),
            Constraint::Min(12),
        ],
    )
    .header(
        Row::new(["PARTITION", "EVENTS", "THROUGHPUT", "LAST UPDATE", "STATUS"])
            .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(" ACTIVE PARTITIONS ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(table, area);
}
// -----------------------------------------------------------------------------
// Physical validation panel
// -----------------------------------------------------------------------------

fn render_validation(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let validation = &dashboard.validation;

    let block = Block::default()
        .title(" PHYSICAL DATASET VALIDATION ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BLUE));

    let inner = block.inner(area);

    frame.render_widget(block, area);

    if inner.height < 4 || inner.width < 30 {
        return;
    }

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Min(1),
        ])
        .split(inner);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(sections[0]);

    let partitions = Paragraph::new(format!(
        "Partitions      {} / {}\n\
         Parquet files    {} / {}",
        validation.partitions_completed,
        validation.total_partitions,
        validation.files_completed,
        validation.total_files,
    ))
    .style(Style::default().fg(FG));

    let events = Paragraph::new(format!(
        "Events validated  {} / {}\n\
         Row groups        {}",
        compact(validation.events_validated),
        compact(validation.expected_events),
        validation.row_groups_validated,
    ))
    .style(Style::default().fg(FG));

    frame.render_widget(partitions, columns[0]);
    frame.render_widget(events, columns[1]);

    let ratio = validation.progress_ratio();

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(CYAN).bg(MUTED))
        .ratio(ratio)
        .label(format!("VALIDATION  {:.1}%", ratio * 100.0,));

    frame.render_widget(gauge, sections[1]);

    let status = if validation.completed {
        "VALIDATION COMPLETE"
    } else if validation.started {
        "VALIDATING PHYSICAL PARQUET"
    } else {
        "WAITING FOR VALIDATION"
    };

    let status_color = if validation.completed { GREEN } else { YELLOW };

    frame.render_widget(
        Paragraph::new(status).style(
            Style::default()
                .fg(status_color)
                .add_modifier(Modifier::BOLD),
        ),
        sections[2],
    );
}
// -----------------------------------------------------------------------------
// Throughput history
// -----------------------------------------------------------------------------

fn render_throughput(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let block = Block::default()
        .title(
            if dashboard.phase == Some(MergePhase::PhysicalMaterialization)
                || dashboard.phase_completed(MergePhase::PhysicalMaterialization)
            {
                " PHYSICAL THROUGHPUT HISTORY "
            } else {
                " LOGICAL THROUGHPUT HISTORY "
            },
        )
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BLUE));

    let inner = block.inner(area);

    frame.render_widget(block, area);

    if inner.width < 10 || inner.height < 3 {
        return;
    }

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(2)])
        .split(inner);

    let data: Vec<u64> = dashboard.throughput_history.iter().copied().collect();

    let sparkline = Sparkline::default()
        .data(&data)
        .style(Style::default().fg(CYAN));

    frame.render_widget(sparkline, sections[0]);

    let current = dashboard.total_speed();

    let peak = dashboard
        .throughput_history
        .iter()
        .copied()
        .max()
        .unwrap_or(0);

    let average = if dashboard.throughput_history.is_empty() {
        0
    } else {
        dashboard.throughput_history.iter().sum::<u64>() / dashboard.throughput_history.len() as u64
    };

    let statistics = Paragraph::new(format!(
        "Current {}   Avg {}   Peak {}",
        format_speed(current),
        format_speed(average as f64),
        format_speed(peak as f64),
    ))
    .style(Style::default().fg(FG));

    frame.render_widget(statistics, sections[1]);
}
// -----------------------------------------------------------------------------
// Recent activity
// -----------------------------------------------------------------------------

fn render_activity(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let visible_rows = area.height.saturating_sub(2) as usize;

    let activity = dashboard
        .activity
        .iter()
        .rev()
        .take(visible_rows)
        .cloned()
        .collect::<Vec<_>>();

    let text = activity.into_iter().rev().collect::<Vec<_>>().join("\n");

    let widget = Paragraph::new(text).style(Style::default().fg(FG)).block(
        Block::default()
            .title(" RECENT ACTIVITY ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(widget, area);
}
// -----------------------------------------------------------------------------
// Output statistics
// -----------------------------------------------------------------------------

fn render_totals(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let logical_events = dashboard
        .partitions
        .iter()
        .filter(|(key, _)| key.phase == MergePhase::LogicalMerge)
        .map(|(_, partition)| partition.events)
        .sum::<u64>();

    let physical_events = dashboard
        .partitions
        .iter()
        .filter(|(key, _)| key.phase == MergePhase::PhysicalMaterialization)
        .map(|(_, partition)| partition.events)
        .sum::<u64>();

    let widget = Paragraph::new(format!(
        "Logical events      {}\n\
         Physical events     {}\n\
         Input datasets     {}",
        compact(logical_events),
        compact(physical_events),
        dashboard.input_datasets,
    ))
    .style(Style::default().fg(FG))
    .block(
        Block::default()
            .title(" MERGE OUTPUT ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(widget, area);
}
// -----------------------------------------------------------------------------
// Main dashboard rendering
// -----------------------------------------------------------------------------

fn render(frame: &mut ratatui::Frame, dashboard: &MergeDashboard) {
    let area = frame.area();

    frame.render_widget(Block::default().style(Style::default().bg(BG).fg(FG)), area);

    let active = dashboard.active_partitions();

    let table_height = (active as u16).saturating_add(3).clamp(5, 20);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Length(table_height),
            Constraint::Length(10),
            Constraint::Min(7),
            Constraint::Length(6),
        ])
        .split(area);

    render_header(frame, sections[0], dashboard);
    render_overview(frame, sections[1], dashboard);
    if dashboard.phase == Some(MergePhase::PhysicalValidation) {
        render_validation(frame, sections[2], dashboard);
    } else if dashboard.is_partition_phase() {
        render_partitions(frame, sections[2], dashboard);
    } else {
        render_pipeline_status(frame, sections[2], dashboard);
    }

    let monitoring = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(sections[3]);

    render_throughput(frame, monitoring[0], dashboard);

    render_activity(frame, monitoring[1], dashboard);

    render_totals(frame, sections[5], dashboard);
}
// -----------------------------------------------------------------------------
// Execute unified merge with TUI
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// Pipeline status panel
// -----------------------------------------------------------------------------

fn render_pipeline_status(frame: &mut ratatui::Frame, area: Rect, dashboard: &MergeDashboard) {
    let phases = [
        MergePhase::InputResolution,
        MergePhase::Inventory,
        MergePhase::LogicalMerge,
        MergePhase::LogicalValidation,
        MergePhase::PhysicalMaterialization,
        MergePhase::PhysicalValidation,
        MergePhase::Checkpoints,
        MergePhase::Manifest,
        MergePhase::PublicationPreparation,
        MergePhase::Commit,
    ];

    let rows = phases.into_iter().map(|phase| {
        let (status, color): (&str, Color) = if dashboard.phase_completed(phase) {
            ("COMPLETE", GREEN)
        } else if dashboard.phase_running(phase) {
            ("RUNNING", CYAN)
        } else {
            ("PENDING", MUTED)
        };

        Row::new(vec![phase.label().to_owned(), status.to_owned()])
            .style(Style::default().fg(color))
    });

    let table = Table::new(
        rows,
        [Constraint::Percentage(70), Constraint::Percentage(30)],
    )
    .header(
        Row::new(["PIPELINE STAGE", "STATUS"])
            .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(" MERGE PIPELINE ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(table, area);
}
pub fn execute_merge_with_tui(
    job: MergeJob,
    project_root: std::path::PathBuf,
) -> Result<UnifiedMergeExecutionResult, Box<dyn std::error::Error>> {
    if !io::stdout().is_terminal() {
        return Err("merge TUI requires an interactive terminal".into());
    }

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
    let mut dashboard = MergeDashboard::new(&job);

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

    // Always wait for the merge thread.
    let merge_result = worker.join();

    // Restore terminal before displaying results.
    drop(terminal);
    drop(guard);

    dashboard_result?;

    let result = merge_result.map_err(|_| "merge worker thread panicked")??;

    print_merge_summary(&dashboard, &result);

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
    loop {
        // Consume all available lifecycle events.
        loop {
            match receiver.try_recv() {
                Ok(event) => dashboard.handle_event(event),

                Err(TryRecvError::Empty) => break,

                Err(TryRecvError::Disconnected) => break,
            }
        }

        // Read live atomic partition counters.
        dashboard.update_snapshots(progress_state);

        // Update throughput history.
        dashboard.sample_throughput();

        // Render the dashboard.
        terminal.draw(|frame| {
            render(frame, dashboard);
        })?;

        if worker.is_finished() {
            // Consume remaining lifecycle events.
            while let Ok(event) = receiver.try_recv() {
                dashboard.handle_event(event);
            }

            // Capture final counter values.
            dashboard.update_snapshots(progress_state);

            // Render final state.
            terminal.draw(|frame| {
                render(frame, dashboard);
            })?;

            break;
        }

        thread::sleep(REFRESH_INTERVAL);
    }

    Ok(())
}
// -----------------------------------------------------------------------------
// Final merge summary
// -----------------------------------------------------------------------------

fn print_merge_summary(dashboard: &MergeDashboard, result: &UnifiedMergeExecutionResult) {
    let mut stderr = io::stderr().lock();

    let elapsed = dashboard.started.elapsed();

    let _ = writeln!(stderr);
    let _ = writeln!(stderr, "MARKETFORGE — MERGE COMPLETE");

    let _ = writeln!(stderr, "{}", "─".repeat(64));

    let _ = writeln!(stderr, "Job ID               {}", result.job_id,);

    let _ = writeln!(stderr, "Input datasets       {}", dashboard.input_datasets,);

    let _ = writeln!(
        stderr,
        "Partitions completed {}",
        result.partitions_completed,
    );

    let _ = writeln!(stderr, "Events written       {}", result.events_written,);

    let _ = writeln!(stderr, "Trade events         {}", result.trade_events,);

    let _ = writeln!(stderr, "Depth events         {}", result.depth_events,);

    let _ = writeln!(stderr, "Parquet files        {}", result.files_written,);

    let _ = writeln!(
        stderr,
        "Skipped snapshots    {}",
        result.skipped_initializations,
    );

    let _ = writeln!(stderr, "Elapsed              {}", format_duration(elapsed),);

    let throughput = if elapsed.as_secs_f64() > 0.0 {
        result.events_written as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let _ = writeln!(stderr, "Average throughput   {}", format_speed(throughput),);

    let _ = writeln!(
        stderr,
        "Dataset              {}",
        result.dataset_path.display(),
    );

    let _ = writeln!(
        stderr,
        "Manifest             {}",
        result.manifest_path.display(),
    );

    let _ = writeln!(stderr, "{}", "─".repeat(64));
    let _ = writeln!(stderr);
}
