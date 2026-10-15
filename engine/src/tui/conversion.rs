use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::{Arc, mpsc::Receiver},
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};

use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Axis, Block, Borders, Chart, Dataset, Gauge, GraphType, Paragraph, Row, Table},
};

use crate::process::convert::progress::{
    ConversionPartitionKey, ConversionPhase, ConversionProgress, ConversionProgressEvent,
    ConversionProgressState,
};

// -----------------------------------------------------------------------------
// Tokyo Night palette
// -----------------------------------------------------------------------------

const BG: Color = Color::Rgb(26, 27, 38);
const FG: Color = Color::Rgb(192, 202, 245);
const BLUE: Color = Color::Rgb(122, 162, 247);
const CYAN: Color = Color::Rgb(125, 207, 255);
const GREEN: Color = Color::Rgb(158, 206, 106);
const YELLOW: Color = Color::Rgb(224, 175, 104);
const MUTED: Color = Color::Rgb(86, 95, 137);

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
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

// -----------------------------------------------------------------------------
// Validation state
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ValidationState {
    started: bool,
    completed: bool,

    total_files: u64,
    files_completed: u64,

    expected_events: u64,
    events_validated: u64,
    row_groups_validated: u64,
}

impl ValidationState {
    fn ratio(&self) -> f64 {
        if self.total_files > 0 {
            (self.files_completed as f64 / self.total_files as f64).clamp(0.0, 1.0)
        } else if self.completed {
            1.0
        } else {
            0.0
        }
    }
}

// -----------------------------------------------------------------------------
// Dashboard state
// -----------------------------------------------------------------------------

struct ConversionDashboard {
    started: Instant,
    prepass_started: Option<Instant>,
    phase: Option<ConversionPhase>,
    completed_phases: HashSet<ConversionPhase>,
    preservation_started: Option<Instant>,
    preservation_events_compared: u64,
    preservation_total_events: u64,
    planned_partitions: usize,

    active: HashMap<ConversionPartitionKey, Instant>,
    completed: HashSet<ConversionPartitionKey>,
    failed: HashSet<ConversionPartitionKey>,

    progress_state: Arc<ConversionProgressState>,
    throughput_history: VecDeque<u64>,
    last_throughput_sample: Instant,
    last_events_sampled: u64,
    current_throughput: f64,
    validation: ValidationState,

    activity: VecDeque<String>,
    prepass_events_scanned: u64,
    prepass_total_events: u64,
    prepass_checkpoints_created: usize,
    prepass_total_checkpoints: usize,
    finished: bool,
    success: bool,
}

impl ConversionDashboard {
    fn new(progress_state: Arc<ConversionProgressState>) -> Self {
        Self {
            started: Instant::now(),
            prepass_started: None,
            phase: None,
            completed_phases: HashSet::new(),

            planned_partitions: 0,
            preservation_started: None,
            preservation_events_compared: 0,
            preservation_total_events: 0,
            active: HashMap::new(),
            completed: HashSet::new(),
            failed: HashSet::new(),
            prepass_events_scanned: 0,
            prepass_total_events: 0,
            prepass_checkpoints_created: 0,
            prepass_total_checkpoints: 0,
            progress_state,
            throughput_history: VecDeque::with_capacity(120),
            last_throughput_sample: Instant::now(),
            last_events_sampled: 0,
            current_throughput: 0.0,
            validation: ValidationState::default(),

            activity: VecDeque::with_capacity(128),

            finished: false,
            success: false,
        }
    }
    fn sample_throughput(&mut self) {
        let now = Instant::now();

        let elapsed = now
            .duration_since(self.last_throughput_sample)
            .as_secs_f64();

        // Sample once per second.
        if elapsed < 1.0 {
            return;
        }

        let total = self.total_progress();

        let current_events = total.events_read;

        let delta = current_events.saturating_sub(self.last_events_sampled);

        let instantaneous = delta as f64 / elapsed;

        // Exponential moving average.
        const ALPHA: f64 = 0.35;

        if self.throughput_history.is_empty() {
            self.current_throughput = instantaneous;
        } else {
            self.current_throughput =
                ALPHA * instantaneous + (1.0 - ALPHA) * self.current_throughput;
        }

        self.last_events_sampled = current_events;
        self.last_throughput_sample = now;

        self.throughput_history
            .push_back(self.current_throughput.max(0.0) as u64);

        while self.throughput_history.len() > 120 {
            self.throughput_history.pop_front();
        }
    }
    fn total_progress(&self) -> ConversionProgress {
        let snapshots = self.snapshots();

        let mut total = ConversionProgress::default();

        for progress in snapshots.values() {
            total.events_read += progress.events_read;
            total.events_written += progress.events_written;

            total.identity_events += progress.identity_events;
            total.converted_events += progress.converted_events;
            total.unavailable_events += progress.unavailable_events;
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
    fn log(&mut self, message: impl Into<String>) {
        let elapsed = self.started.elapsed().as_secs();
        let minutes = elapsed / 60;
        let seconds = elapsed % 60;

        self.activity
            .push_back(format!("{minutes:02}:{seconds:02}  {}", message.into(),));

        while self.activity.len() > 128 {
            self.activity.pop_front();
        }
    }

    fn handle_event(&mut self, event: ConversionProgressEvent) {
        match event {
            ConversionProgressEvent::PhaseStarted { phase } => {
                if phase == ConversionPhase::ReferencePrepass {
                    self.prepass_started = Some(Instant::now());

                    self.prepass_events_scanned = 0;
                    self.prepass_total_events = 0;
                    self.prepass_checkpoints_created = 0;
                    self.prepass_total_checkpoints = 0;
                }
                self.phase = Some(phase);
                if phase == ConversionPhase::SourcePreservation {
                    self.preservation_started = Some(Instant::now());
                    self.preservation_events_compared = 0;
                    self.preservation_total_events = 0;
                }
                if phase == ConversionPhase::PhysicalConversion {
                    self.throughput_history.clear();
                    self.last_throughput_sample = Instant::now();
                    self.last_events_sampled = 0;
                    self.current_throughput = 0.0;
                }

                self.log(format!("● {} started", phase.label()));
            }
            ConversionProgressEvent::PhaseCompleted { phase } => {
                self.completed_phases.insert(phase);

                if self.phase == Some(phase) {
                    self.phase = None;
                }

                self.log(format!("✓ {} completed", phase.label()));
            }
            ConversionProgressEvent::ReferencePrepassProgress {
                events_scanned,
                total_events,
                checkpoints_created,
                total_checkpoints,
            } => {
                self.prepass_events_scanned = events_scanned;
                self.prepass_total_events = total_events;
                self.prepass_checkpoints_created = checkpoints_created;
                self.prepass_total_checkpoints = total_checkpoints;
            }
            ConversionProgressEvent::PartitionsPlanned { count } => {
                self.planned_partitions = count;
                self.log(format!("● {count} conversion partitions planned"));
            }

            ConversionProgressEvent::PartitionStarted { key } => {
                self.active.insert(key, Instant::now());
                self.log(format!("● Partition {:06} started", key.partition_id));
            }

            ConversionProgressEvent::PartitionCompleted { key, metrics } => {
                self.active.remove(&key);
                self.completed.insert(key);

                self.log(format!(
                    "✓ Partition {:06} completed — {} events",
                    key.partition_id, metrics.events_written,
                ));
            }

            ConversionProgressEvent::PartitionFailed { key, message } => {
                self.active.remove(&key);
                self.failed.insert(key);

                self.log(format!(
                    "✗ Partition {:06} failed: {}",
                    key.partition_id, message,
                ));
            }

            ConversionProgressEvent::ValidationStarted {
                total_files,
                expected_events,
            } => {
                self.validation = ValidationState {
                    started: true,
                    total_files,
                    expected_events,
                    ..ValidationState::default()
                };

                self.log(format!("● Validating {total_files} Parquet files",));
            }

            ConversionProgressEvent::ValidationProgress {
                files_completed,
                events_validated,
                row_groups_validated,
            } => {
                self.validation.files_completed =
                    self.validation.files_completed.max(files_completed);

                self.validation.events_validated =
                    self.validation.events_validated.max(events_validated);

                self.validation.row_groups_validated = self
                    .validation
                    .row_groups_validated
                    .max(row_groups_validated);
            }
            ConversionProgressEvent::SourcePreservationProgress {
                events_compared,
                total_events,
            } => {
                self.preservation_events_compared =
                    self.preservation_events_compared.max(events_compared);

                self.preservation_total_events = total_events;
            }
            ConversionProgressEvent::ValidationCompleted {
                files_completed,
                events_validated,
                row_groups_validated,
            } => {
                self.validation.completed = true;
                self.validation.files_completed = files_completed;
                self.validation.events_validated = events_validated;
                self.validation.row_groups_validated = row_groups_validated;

                self.log(format!("✓ Validated {events_validated} events",));
            }

            ConversionProgressEvent::Finished { success } => {
                self.finished = true;
                self.success = success;

                self.log(if success {
                    "✓ Conversion completed successfully"
                } else {
                    "✗ Conversion failed"
                });
            }
        }
    }

    fn snapshots(&self) -> HashMap<ConversionPartitionKey, ConversionProgress> {
        self.progress_state.snapshots()
    }

    fn current_progress(&self) -> (f64, String) {
        if self.finished {
            return if self.success {
                (1.0, "CONVERSION COMPLETE".to_owned())
            } else {
                (
                    self.completed_phases.len() as f64 / 13.0,
                    "CONVERSION FAILED".to_owned(),
                )
            };
        }
        match self.phase {
            Some(ConversionPhase::ReferencePrepass) => {
                let ratio = if self.prepass_total_events > 0 {
                    self.prepass_events_scanned as f64 / self.prepass_total_events as f64
                } else {
                    0.0
                };

                (
                    ratio.clamp(0.0, 1.0),
                    format!(
                        "REFERENCE PREPASS  {} / {}",
                        compact(self.prepass_events_scanned),
                        compact(self.prepass_total_events),
                    ),
                )
            }
            Some(ConversionPhase::SourcePreservation) => {
                let ratio = if self.preservation_total_events > 0 {
                    self.preservation_events_compared as f64 / self.preservation_total_events as f64
                } else {
                    0.0
                };

                (
                    ratio.clamp(0.0, 1.0),
                    format!(
                        "SOURCE PRESERVATION  {} / {}",
                        compact(self.preservation_events_compared),
                        compact(self.preservation_total_events),
                    ),
                )
            }
            Some(ConversionPhase::PhysicalConversion) => {
                let done = self.completed.len() + self.failed.len();

                let ratio = if self.planned_partitions > 0 {
                    done as f64 / self.planned_partitions as f64
                } else {
                    0.0
                };

                (
                    ratio.clamp(0.0, 1.0),
                    format!("PARTITIONS  {done} / {}", self.planned_partitions,),
                )
            }

            Some(ConversionPhase::PhysicalValidation) => (
                self.validation.ratio(),
                format!(
                    "VALIDATION  {} / {} FILES",
                    self.validation.files_completed, self.validation.total_files,
                ),
            ),

            Some(phase) => {
                let completed = self.completed_phases.len();

                let ratio = completed as f64 / 13.0;

                (
                    ratio.clamp(0.0, 1.0),
                    format!("PIPELINE  {:.1}% — {}", ratio * 100.0, phase.label(),),
                )
            }

            None => (0.0, "INITIALIZING".to_owned()),
        }
    }
}

// -----------------------------------------------------------------------------
// Initial dashboard rendering
// -----------------------------------------------------------------------------

fn render(frame: &mut ratatui::Frame, dashboard: &ConversionDashboard) {
    let area = frame.area();

    frame.render_widget(Block::default().style(Style::default().bg(BG)), area);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(7),
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(8),
        ])
        .split(area);

    let status = if dashboard.finished {
        if dashboard.success {
            "COMPLETE"
        } else {
            "FAILED"
        }
    } else {
        "RUNNING"
    };

    let header = Paragraph::new(format!(" ◆ MARKETFORGE  │  CONVERSION  │  {}", status,))
        .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BLUE)),
        );

    frame.render_widget(header, sections[0]);

    render_performance(frame, sections[1], dashboard);
    let (ratio, label) = dashboard.current_progress();

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(CYAN).bg(MUTED))
        .ratio(ratio)
        .label(label);

    frame.render_widget(gauge, sections[2]);

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(sections[4]);

    render_throughput(frame, bottom[0], dashboard);
    let activity = dashboard
        .activity
        .iter()
        .rev()
        .take(6)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");

    let activity_panel = Paragraph::new(activity)
        .style(Style::default().fg(FG))
        .block(
            Block::default()
                .title(" RECENT ACTIVITY ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BLUE)),
        );
    frame.render_widget(activity_panel, bottom[1]);

    match dashboard.phase {
        Some(ConversionPhase::ReferencePrepass) => {
            render_reference_prepass(frame, sections[3], dashboard);
        }

        Some(ConversionPhase::PhysicalConversion) => {
            render_active_partitions(frame, sections[3], dashboard);
        }

        Some(ConversionPhase::PhysicalValidation) => {
            render_validation(frame, sections[3], dashboard);
        }

        Some(ConversionPhase::SourcePreservation) => {
            render_source_preservation(frame, sections[3], dashboard);
        }

        _ => {
            render_pipeline(frame, sections[3], dashboard);
        }
    }
}

// -----------------------------------------------------------------------------
// TUI event loop
// -----------------------------------------------------------------------------

pub fn execute_conversion_with_tui(
    receiver: Receiver<ConversionProgressEvent>,
    progress_state: Arc<ConversionProgressState>,
) -> io::Result<()> {
    let _guard = TerminalGuard::enter()?;

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut dashboard = ConversionDashboard::new(progress_state);

    loop {
        while let Ok(event) = receiver.try_recv() {
            dashboard.handle_event(event);
        }

        if dashboard.phase == Some(ConversionPhase::PhysicalConversion) && !dashboard.finished {
            dashboard.sample_throughput();
        }

        terminal.draw(|frame| render(frame, &dashboard))?;

        if dashboard.finished && std::env::var_os("MARKETFORGE_TUI_HOLD").is_none() {
            break;
        }

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press
                    && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                {
                    if dashboard.finished {
                        break;
                    }

                    dashboard.log("Conversion is running; wait for completion.");
                }
            }
        }
    }

    Ok(())
}
fn compact(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.2}B", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.2}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.2}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}
fn render_performance(frame: &mut ratatui::Frame, area: Rect, dashboard: &ConversionDashboard) {
    let total = dashboard.total_progress();

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(area);

    let elapsed = dashboard.started.elapsed().as_secs();

    let session_text = if dashboard.phase == Some(ConversionPhase::PhysicalValidation) {
        format!(
            "Phase       VALIDATION\n\
         Files       {} / {}\n\
         Events      {}\n\
         Elapsed     {:02}:{:02}:{:02}",
            dashboard.validation.files_completed,
            dashboard.validation.total_files,
            compact(dashboard.validation.events_validated),
            elapsed / 3600,
            (elapsed / 60) % 60,
            elapsed % 60,
        )
    } else if dashboard.phase == Some(ConversionPhase::PhysicalConversion) {
        format!(
            "Phase       CONVERSION\n\
         Partitions  {} / {}\n\
         Active      {}\n\
         Elapsed     {:02}:{:02}:{:02}",
            dashboard.completed.len(),
            dashboard.planned_partitions,
            dashboard.active.len(),
            elapsed / 3600,
            (elapsed / 60) % 60,
            elapsed % 60,
        )
    } else {
        format!(
            "Phase       {}\n\
         Status      {}\n\
         Elapsed     {:02}:{:02}:{:02}",
            dashboard
                .phase
                .map(ConversionPhase::label)
                .unwrap_or("INITIALIZING"),
            if dashboard.finished {
                if dashboard.success {
                    "COMPLETE"
                } else {
                    "FAILED"
                }
            } else {
                "RUNNING"
            },
            elapsed / 3600,
            (elapsed / 60) % 60,
            elapsed % 60,
        )
    };

    let session = Paragraph::new(session_text)
        .style(Style::default().fg(FG))
        .block(
            Block::default()
                .title(" SESSION ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BLUE)),
        );

    let performance = Paragraph::new(format!(
        "Read rate   {}/s\n\
         Events read  {}\n\
         Written      {}\n\
         Converted    {}",
        compact(dashboard.current_throughput as u64),
        compact(total.events_read),
        compact(total.events_written),
        compact(total.converted_events),
    ))
    .style(Style::default().fg(FG))
    .block(
        Block::default()
            .title(" PERFORMANCE ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    let active = dashboard.active.len();

    let completed = dashboard.completed.len();
    let failed = dashboard.failed.len();

    let pending = dashboard
        .planned_partitions
        .saturating_sub(active + completed + failed);

    let resources = Paragraph::new(format!(
        "Active       {}\n\
     Pending      {}\n\
     Completed    {}\n\
     Failed       {}",
        active, pending, completed, failed,
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
}
fn render_active_partitions(
    frame: &mut ratatui::Frame,
    area: Rect,
    dashboard: &ConversionDashboard,
) {
    let snapshots = dashboard.snapshots();

    let mut active = dashboard.active.iter().collect::<Vec<_>>();

    active.sort_by_key(|(key, _)| key.partition_id);

    let visible_rows = area.height.saturating_sub(3) as usize;

    let rows = active.into_iter().take(visible_rows).map(|(key, started)| {
        let progress = snapshots.get(key).copied().unwrap_or_default();

        let elapsed = started.elapsed().as_secs_f64().max(0.001);

        let speed = progress.events_written as f64 / elapsed;

        Row::new(vec![
            format!("{:06}", key.partition_id),
            compact(progress.events_read),
            compact(progress.events_written),
            compact(progress.converted_events),
            compact(progress.identity_events),
            compact(progress.unavailable_events),
            format!("{}/s", compact(speed as u64)),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(11),
            Constraint::Percentage(14),
            Constraint::Percentage(14),
            Constraint::Percentage(14),
            Constraint::Percentage(14),
            Constraint::Percentage(14),
            Constraint::Percentage(16),
        ],
    )
    .header(
        Row::new([
            "PARTITION",
            "READ",
            "WRITTEN",
            "CONVERTED",
            "IDENTITY",
            "UNAVAILABLE",
            "SPEED",
        ])
        .style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(" ACTIVE CONVERSION ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(table, area);
}
fn render_throughput(frame: &mut ratatui::Frame, area: Rect, dashboard: &ConversionDashboard) {
    let history = dashboard
        .throughput_history
        .iter()
        .enumerate()
        .map(|(index, &value)| (index as f64, value as f64 / 1_000_000.0))
        .collect::<Vec<_>>();

    let max_y = history
        .iter()
        .map(|(_, value)| *value)
        .fold(0.0_f64, f64::max)
        .max(1.0);

    let max_x = history.len().saturating_sub(1).max(1) as f64;

    let datasets = vec![
        Dataset::default()
            .name("Events/s")
            .marker(ratatui::symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(CYAN))
            .data(&history),
    ];

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(format!(
                    " THROUGHPUT HISTORY — {}/s ",
                    compact(dashboard.current_throughput as u64),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(BLUE)),
        )
        .x_axis(
            Axis::default()
                .bounds([0.0, max_x])
                .style(Style::default().fg(MUTED)),
        )
        .y_axis(
            Axis::default()
                .bounds([0.0, max_y * 1.1])
                .labels([
                    ratatui::text::Line::from("0"),
                    ratatui::text::Line::from(format!("{:.1}M/s", max_y)),
                ])
                .style(Style::default().fg(MUTED)),
        );

    frame.render_widget(chart, area);
}

fn render_validation(frame: &mut ratatui::Frame, area: Rect, dashboard: &ConversionDashboard) {
    let validation = &dashboard.validation;

    let block = Block::default()
        .title(" PHYSICAL DATASET VALIDATION ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BLUE));

    let inner = block.inner(area);

    frame.render_widget(block, area);

    if inner.width < 30 || inner.height < 5 {
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

    let files = Paragraph::new(format!(
        "Parquet files     {} / {}\n\
         Events validated {}",
        validation.files_completed,
        validation.total_files,
        compact(validation.events_validated),
    ))
    .style(Style::default().fg(FG));

    let expected = if validation.expected_events > 0 {
        compact(validation.expected_events)
    } else {
        "N/A".to_owned()
    };

    let statistics = Paragraph::new(format!(
        "Expected events  {}\n\
         Row groups       {}",
        expected, validation.row_groups_validated,
    ))
    .style(Style::default().fg(FG));

    frame.render_widget(files, columns[0]);
    frame.render_widget(statistics, columns[1]);

    let ratio = validation.ratio();

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(CYAN).bg(MUTED))
        .ratio(ratio)
        .label(format!("VALIDATION {:.1}%", ratio * 100.0));

    frame.render_widget(gauge, sections[1]);

    let (status, color) = if validation.completed {
        ("VALIDATION COMPLETE", GREEN)
    } else if validation.started {
        ("VALIDATING PHYSICAL PARQUET", YELLOW)
    } else {
        ("WAITING FOR VALIDATION", MUTED)
    };

    frame.render_widget(
        Paragraph::new(status).style(Style::default().fg(color).add_modifier(Modifier::BOLD)),
        sections[2],
    );
}

fn render_pipeline(frame: &mut ratatui::Frame, area: Rect, dashboard: &ConversionDashboard) {
    const PHASES: &[ConversionPhase] = &[
        ConversionPhase::SourceValidation,
        ConversionPhase::CatalogResolution,
        ConversionPhase::ConversionPlanning,
        ConversionPhase::PartitionPlanning,
        ConversionPhase::ReferencePrepass,
        ConversionPhase::PhysicalConversion,
        ConversionPhase::ExecutionReceipt,
        ConversionPhase::PhysicalValidation,
        ConversionPhase::ReceiptReconciliation,
        ConversionPhase::SourcePreservation,
        ConversionPhase::ManifestConstruction,
        ConversionPhase::PublicationPreparation,
        ConversionPhase::Commit,
    ];

    let rows = PHASES.iter().map(|&phase| {
        let (status, color): (&str, Color) = if dashboard.completed_phases.contains(&phase) {
            ("COMPLETE", GREEN)
        } else if dashboard.phase == Some(phase) {
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
            .title(" CONVERSION PIPELINE ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(table, area);
}

fn render_reference_prepass(
    frame: &mut ratatui::Frame,
    area: Rect,
    dashboard: &ConversionDashboard,
) {
    let scanned = dashboard.prepass_events_scanned;
    let total = dashboard.prepass_total_events;

    let checkpoints = dashboard.prepass_checkpoints_created;
    let total_checkpoints = dashboard.prepass_total_checkpoints;

    let ratio = if total > 0 {
        (scanned as f64 / total as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };

    let block = Block::default()
        .title(" REFERENCE CHECKPOINT PREPASS ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BLUE));

    let inner = block.inner(area);

    frame.render_widget(block, area);

    if inner.width < 30 || inner.height < 5 {
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

    let elapsed = dashboard
        .prepass_started
        .map(|started| started.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    let throughput = if elapsed > 0.0 {
        scanned as f64 / elapsed
    } else {
        0.0
    };

    let statistics = Paragraph::new(format!(
        "Events scanned       {} / {}\n\
         Checkpoints created  {} / {}\n\
         Scan throughput      {}/s",
        compact(scanned),
        compact(total),
        checkpoints,
        total_checkpoints,
        compact(throughput as u64),
    ))
    .style(Style::default().fg(FG));

    frame.render_widget(statistics, sections[0]);

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(CYAN).bg(MUTED))
        .ratio(ratio)
        .label(format!("REFERENCE PREPASS {:.1}%", ratio * 100.0));

    frame.render_widget(gauge, sections[1]);

    frame.render_widget(
        Paragraph::new("SCANNING HISTORICAL REFERENCE EVENTS").style(Style::default().fg(YELLOW)),
        sections[2],
    );
}

fn render_source_preservation(
    frame: &mut ratatui::Frame,
    area: Rect,
    dashboard: &ConversionDashboard,
) {
    let compared = dashboard.preservation_events_compared;
    let total = dashboard.preservation_total_events;

    let ratio = if total > 0 {
        (compared as f64 / total as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };

    let elapsed = dashboard
        .preservation_started
        .map(|started| started.elapsed().as_secs_f64())
        .unwrap_or(0.0);

    let throughput = if elapsed > 0.0 {
        compared as f64 / elapsed
    } else {
        0.0
    };

    let block = Block::default()
        .title(" SOURCE PRESERVATION ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BLUE));

    let inner = block.inner(area);

    frame.render_widget(block, area);

    if inner.width < 30 || inner.height < 5 {
        return;
    }

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(2),
            Constraint::Min(1),
        ])
        .split(inner);

    let statistics = Paragraph::new(format!(
        "Events compared       {} / {}\n\
         Comparison throughput {}/s\n\
         Elapsed               {:.1} s",
        compact(compared),
        compact(total),
        compact(throughput as u64),
        elapsed,
    ))
    .style(Style::default().fg(FG));

    frame.render_widget(statistics, sections[0]);

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(CYAN).bg(MUTED))
        .ratio(ratio)
        .label(format!("PRESERVATION {:.1}%", ratio * 100.0));

    frame.render_widget(gauge, sections[1]);

    frame.render_widget(
        Paragraph::new("VERIFYING ORIGINAL MARKET DATA").style(Style::default().fg(YELLOW)),
        sections[2],
    );
}
