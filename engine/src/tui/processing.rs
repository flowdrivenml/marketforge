use crate::historical::normalize::ProcessingStatus;
use crate::historical::normalize::progress::ProgressTaskId;
use crate::historical::normalize::session::{
    ProcessingSessionResult, execute_processing_session_with_progress,
};
use crate::shared::terminal::format::{compact, format_duration, format_speed};
use crate::shared::terminal::guard::TerminalGuard;
use crate::shared::terminal::palette::{BG, BLUE, CYAN, FG, GREEN, MUTED, RED, YELLOW};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    widgets::{Block, Borders, Gauge, Paragraph, Row, Sparkline, Table},
};
use std::io::Write;
use std::{
    collections::{HashMap, VecDeque},
    io::{self, IsTerminal, Stdout},
    sync::{
        Arc,
        mpsc::{self, Receiver, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};
use {
    crate::historical::job::ProcessingJob, crate::historical::normalize::ProcessingConfig,
    crate::historical::normalize::ProcessingResult,
    crate::historical::normalize::execute_processing_job_with_progress,
    crate::historical::normalize::progress::ProcessingProgress,
    crate::historical::normalize::progress::ProgressEvent,
    crate::historical::normalize::progress::ProgressReporter,
    crate::historical::normalize::progress::ProgressState,
};

// -----------------------------------------------------------------------------
// Tokyo Night theme
// -----------------------------------------------------------------------------

const REFRESH_INTERVAL: Duration = Duration::from_millis(250);

// -----------------------------------------------------------------------------
// Terminal guard
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Task state
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
}

#[derive(Debug)]
struct TaskState {
    exchange: String,
    instrument: String,
    content_type: String,
    source: String,

    status: TaskStatus,

    records_read: u64,
    records_written: u64,
    records_rejected: u64,

    speed: f64,

    last_records: u64,
    last_sample: Instant,
}

impl TaskState {
    fn new() -> Self {
        Self {
            exchange: String::new(),
            instrument: String::new(),
            content_type: String::new(),

            status: TaskStatus::Pending,

            records_read: 0,
            records_written: 0,
            records_rejected: 0,

            speed: 0.0,
            source: String::new(),
            last_records: 0,
            last_sample: Instant::now(),
        }
    }

    fn update(&mut self, snapshot: &ProcessingProgress) {
        let now = Instant::now();

        let delta = snapshot.records_read.saturating_sub(self.last_records);

        if delta > 0 {
            let elapsed = now.duration_since(self.last_sample).as_secs_f64();

            if elapsed > 0.0 {
                let instantaneous_speed = delta as f64 / elapsed;

                // Exponential moving average for smoother throughput.
                const ALPHA: f64 = 0.35;

                self.speed = if self.speed == 0.0 {
                    instantaneous_speed
                } else {
                    ALPHA * instantaneous_speed + (1.0 - ALPHA) * self.speed
                };
            }

            self.last_records = snapshot.records_read;
            self.last_sample = now;
        } else if now.duration_since(self.last_sample) >= Duration::from_secs(2) {
            // No new records for two seconds.
            self.speed = 0.0;
        }

        self.records_read = snapshot.records_read;
        self.records_written = snapshot.records_written;
        self.records_rejected = snapshot.records_rejected;
    }
}

// -----------------------------------------------------------------------------
// Dashboard state
// -----------------------------------------------------------------------------

struct ProcessingDashboard {
    tasks: HashMap<ProgressTaskId, TaskState>,

    total_jobs: usize,
    total_tasks: usize,
    worker_limit: usize,

    started: Instant,

    finalizing: bool,
    finished: bool,
    success: bool,
    // Rolling throughput history.
    throughput_history: VecDeque<u64>,
    last_throughput_sample: Instant,

    // Recent processing events.
    activity: VecDeque<String>,
}

impl ProcessingDashboard {
    fn new(job: &ProcessingJob, config: &ProcessingConfig) -> Self {
        Self::new_session(std::slice::from_ref(job), config)
    }
    fn skipped_tasks(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| task.status == TaskStatus::Skipped)
            .count()
    }
    fn new_session(jobs: &[ProcessingJob], config: &ProcessingConfig) -> Self {
        let tasks: HashMap<ProgressTaskId, TaskState> = jobs
            .iter()
            .flat_map(|job| {
                job.tasks.iter().map(move |task| {
                    (
                        ProgressTaskId::new(job.job_id.0, task.task_id.0),
                        TaskState::new(),
                    )
                })
            })
            .collect();

        let total_tasks = tasks.len();

        Self {
            tasks,
            total_tasks,
            worker_limit: config.resources.workers,
            started: Instant::now(),
            finalizing: false,
            finished: false,
            success: false,
            throughput_history: VecDeque::with_capacity(60),
            last_throughput_sample: Instant::now(),
            activity: VecDeque::with_capacity(12),
            total_jobs: jobs.len(),
        }
    }

    fn handle_event(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::TaskStarted {
                id,
                exchange,
                instrument,
                content_type,
                source,
            } => {
                if let Some(task) = self.tasks.get_mut(&id) {
                    task.exchange = exchange.clone();
                    task.instrument = instrument.clone();
                    task.content_type = content_type;
                    task.source = source.clone();
                    task.status = TaskStatus::Running;
                    task.last_sample = Instant::now();
                }

                self.log_activity(format!(
                    "● Job #{} Task #{} started — {exchange} {instrument} — {source}",
                    id.job_id, id.task_id,
                ));
            }

            ProgressEvent::TaskCompleted {
                id,
                records_read,
                records_written,
                records_rejected,
            } => {
                if let Some(task) = self.tasks.get_mut(&id) {
                    task.status = TaskStatus::Completed;
                    task.records_read = records_read;
                    task.records_written = records_written;
                    task.records_rejected = records_rejected;
                    task.speed = 0.0;
                }

                self.log_activity(format!(
                    "✓ Job #{} Task #{} completed — {} records",
                    id.job_id,
                    id.task_id,
                    compact(records_read),
                ));
            }

            ProgressEvent::TaskFailed { id, message } => {
                if let Some(task) = self.tasks.get_mut(&id) {
                    task.status = TaskStatus::Failed;
                    task.speed = 0.0;
                }

                self.log_activity(format!(
                    "✗ Job #{} Task #{} failed — {message}",
                    id.job_id, id.task_id,
                ));
            }

            ProgressEvent::Finalizing => {
                self.finalizing = true;

                self.log_activity("● Finalizing dataset and validating integrity".to_owned());
            }
            ProgressEvent::TaskSkipped { id, reason } => {
                if let Some(task) = self.tasks.get_mut(&id) {
                    task.status = TaskStatus::Skipped;
                    task.speed = 0.0;
                }

                self.log_activity(format!(
                    "○ Job #{} Task #{} skipped — {reason}",
                    id.job_id, id.task_id,
                ));
            }
            ProgressEvent::Finished { success } => {
                self.finished = true;
                self.success = success;

                self.log_activity(if success {
                    "✓ Processing completed successfully".to_owned()
                } else {
                    "✗ Processing failed".to_owned()
                });
            }
        }
    }
    fn completed_jobs(&self) -> usize {
        let mut jobs: HashMap<u64, bool> = HashMap::new();

        for (id, task) in &self.tasks {
            let finished = matches!(
                task.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Skipped
            );

            jobs.entry(id.job_id)
                .and_modify(|complete| *complete &= finished)
                .or_insert(finished);
        }

        jobs.values().filter(|&&complete| complete).count()
    }
    fn update_snapshots(&mut self, state: &ProgressState) {
        let snapshots = state.snapshots();

        for (id, snapshot) in snapshots {
            if let Some(task) = self.tasks.get_mut(&id) {
                if task.status == TaskStatus::Running {
                    task.update(&snapshot);
                }
            }
        }
    }

    fn completed_tasks(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| task.status == TaskStatus::Completed)
            .count()
    }

    fn failed_tasks(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| task.status == TaskStatus::Failed)
            .count()
    }

    fn active_tasks(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| task.status == TaskStatus::Running)
            .count()
    }

    fn total_records_read(&self) -> u64 {
        self.tasks.values().map(|task| task.records_read).sum()
    }

    fn total_records_written(&self) -> u64 {
        self.tasks.values().map(|task| task.records_written).sum()
    }

    fn total_records_rejected(&self) -> u64 {
        self.tasks.values().map(|task| task.records_rejected).sum()
    }

    fn total_speed(&self) -> f64 {
        self.tasks
            .values()
            .filter(|task| task.status == TaskStatus::Running)
            .map(|task| task.speed)
            .sum()
    }
    fn sample_throughput(&mut self) {
        if self.last_throughput_sample.elapsed() < Duration::from_secs(1) {
            return;
        }

        self.last_throughput_sample = Instant::now();

        let throughput = self.total_speed().max(0.0) as u64;

        if self.throughput_history.len() >= 60 {
            self.throughput_history.pop_front();
        }

        self.throughput_history.push_back(throughput);
    }

    fn log_activity(&mut self, message: String) {
        let timestamp = format_duration(self.started.elapsed());

        if self.activity.len() >= 12 {
            self.activity.pop_front();
        }

        self.activity.push_back(format!("{timestamp}  {message}"));
    }
}

// -----------------------------------------------------------------------------
// Formatting
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Header
// -----------------------------------------------------------------------------

fn render_header(frame: &mut ratatui::Frame, area: Rect, dashboard: &ProcessingDashboard) {
    let (status, color) = if dashboard.finished {
        if dashboard.success {
            ("COMPLETE", GREEN)
        } else {
            ("FAILED", RED)
        }
    } else if dashboard.finalizing {
        ("FINALIZING", YELLOW)
    } else {
        ("RUNNING", CYAN)
    };

    let header = Paragraph::new(format!(
        "◆ MARKETFORGE                              PROCESSING ● {status}"
    ))
    .style(Style::default().fg(color).add_modifier(Modifier::BOLD))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(header, area);
}

// -----------------------------------------------------------------------------
// Overview
// -----------------------------------------------------------------------------

fn render_overview(frame: &mut ratatui::Frame, area: Rect, dashboard: &ProcessingDashboard) {
    let completed = dashboard.completed_tasks();
    let failed = dashboard.failed_tasks();
    let active = dashboard.active_tasks();
    let skipped = dashboard.skipped_tasks();
    let pending = dashboard
        .total_tasks
        .saturating_sub(completed + failed + skipped + active);

    let ratio = if dashboard.total_tasks == 0 {
        1.0
    } else {
        (completed + failed + skipped) as f64 / dashboard.total_tasks as f64
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

    let session = Paragraph::new(format!(
        "Jobs        {} / {}\n\
     Tasks       {} / {}\n\
     Elapsed     {}",
        dashboard.completed_jobs(),
        dashboard.total_jobs,
        completed + failed + skipped,
        dashboard.total_tasks,
        format_duration(dashboard.started.elapsed()),
    ))
    .style(Style::default().fg(FG))
    .block(
        Block::default()
            .title(" SESSION ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    let performance = Paragraph::new(format!(
        "Throughput  {}\n\
         Read        {}\n\
         Written     {}",
        format_speed(dashboard.total_speed()),
        compact(dashboard.total_records_read()),
        compact(dashboard.total_records_written()),
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
        active, dashboard.worker_limit, pending, failed,
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

    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(CYAN).bg(MUTED))
        .ratio(ratio.clamp(0.0, 1.0))
        .label(format!("TASK COMPLETION  {:.1}%", ratio * 100.0,));

    frame.render_widget(gauge, sections[1]);
}
// -----------------------------------------------------------------------------
// Active tasks
// -----------------------------------------------------------------------------

fn render_tasks(frame: &mut ratatui::Frame, area: Rect, dashboard: &ProcessingDashboard) {
    let mut active: Vec<_> = dashboard
        .tasks
        .iter()
        .filter(|(_, task)| task.status == TaskStatus::Running)
        .collect();

    active.sort_by_key(|(task_id, _)| **task_id);

    let rows = active.into_iter().map(|(id, task)| {
        Row::new(vec![
            id.job_id.to_string(),
            id.task_id.to_string(),
            task.exchange.clone(),
            task.instrument.clone(),
            task.source.clone(),
            task.content_type.clone(),
            compact(task.records_read),
            compact(task.records_written),
            format_speed(task.speed),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(11),
            Constraint::Length(18),
            Constraint::Min(24),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Length(11),
            Constraint::Length(12),
        ],
    )
    .header(
        Row::new([
            "JOB",
            "TASK",
            "EXCHANGE",
            "INSTRUMENT",
            "SOURCE",
            "TYPE",
            "READ",
            "WRITTEN",
            "SPEED",
        ])
        .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(" ACTIVE PROCESSING ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(table, area);
}
// -----------------------------------------------------------------------------
// Totals
// -----------------------------------------------------------------------------

fn render_totals(frame: &mut ratatui::Frame, area: Rect, dashboard: &ProcessingDashboard) {
    let totals = Paragraph::new(format!(
        "Records read       {:>18}\n\
         Events written     {:>18}\n\
         Records rejected   {:>18}",
        dashboard.total_records_read(),
        dashboard.total_records_written(),
        dashboard.total_records_rejected(),
    ))
    .style(Style::default().fg(GREEN))
    .block(
        Block::default()
            .title("TOTALS")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(BLUE)),
    );

    frame.render_widget(totals, area);
}

// -----------------------------------------------------------------------------
// Main renderer
// -----------------------------------------------------------------------------

fn render(frame: &mut ratatui::Frame, dashboard: &ProcessingDashboard) {
    let area = frame.area();

    frame.render_widget(Block::default().style(Style::default().bg(BG).fg(FG)), area);

    let active = dashboard.active_tasks();

    let desired_table_height = (active as u16).saturating_add(3).clamp(5, 20);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Length(desired_table_height),
            Constraint::Length(10),
            Constraint::Min(7),
            Constraint::Length(6),
        ])
        .split(area);

    render_header(frame, sections[0], dashboard);
    render_overview(frame, sections[1], dashboard);
    render_tasks(frame, sections[2], dashboard);

    // Throughput and activity side by side.
    let monitoring = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(sections[3]);

    let throughput_block = Block::default()
        .title(" THROUGHPUT — LAST 60 SECONDS ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BLUE));

    frame.render_widget(throughput_block, monitoring[0]);

    render_throughput(
        frame,
        monitoring[0].inner(ratatui::layout::Margin {
            horizontal: 1,
            vertical: 1,
        }),
        dashboard,
    );

    render_activity(frame, monitoring[1], dashboard);

    render_totals(frame, sections[5], dashboard);
}
// -----------------------------------------------------------------------------
// Execute processing with TUI
// -----------------------------------------------------------------------------

pub fn execute_processing_with_tui(
    job: ProcessingJob,
    config: ProcessingConfig,
) -> Result<ProcessingResult, Box<dyn std::error::Error>> {
    if !io::stdout().is_terminal() {
        return Err("processing TUI requires an interactive terminal".into());
    }

    // Initialize terminal before launching processing.
    let guard = TerminalGuard::enter()?;

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = match Terminal::new(backend) {
        Ok(terminal) => terminal,
        Err(error) => {
            drop(guard);
            return Err(Box::new(error));
        }
    };

    let (sender, receiver) = mpsc::channel();

    let reporter = ProgressReporter::new(sender);
    let progress_state = reporter.state();

    let mut dashboard = ProcessingDashboard::new(&job, &config);

    let worker =
        thread::spawn(move || execute_processing_job_with_progress(&job, &config, &reporter));

    let dashboard_result = run_dashboard(
        &mut terminal,
        &mut dashboard,
        &receiver,
        &progress_state,
        &worker,
    );

    // Always wait for processing to finish.
    let processing_result = worker.join();

    // Restore terminal before printing errors or results.
    // Restore terminal before printing anything.
    drop(terminal);
    drop(guard);

    dashboard_result?;

    let result = processing_result.map_err(|_| "processing worker thread panicked")??;

    // Print permanent processing summary.
    print_final_summary(&dashboard, &result);

    Ok(result)
}
// -----------------------------------------------------------------------------
// Multi-job processing session with TUI
// -----------------------------------------------------------------------------

pub fn execute_processing_session_with_tui(
    jobs: Vec<ProcessingJob>,
    config: ProcessingConfig,
) -> Result<ProcessingSessionResult, Box<dyn std::error::Error>> {
    if !io::stdout().is_terminal() {
        return Err("processing TUI requires an interactive terminal".into());
    }

    // Initialize terminal before launching processing.
    let guard = TerminalGuard::enter()?;

    let backend = CrosstermBackend::new(io::stdout());

    let mut terminal = match Terminal::new(backend) {
        Ok(terminal) => terminal,
        Err(error) => {
            drop(guard);
            return Err(Box::new(error));
        }
    };

    // Create shared progress infrastructure.
    let (sender, receiver) = mpsc::channel();

    let reporter = ProgressReporter::new(sender);
    let progress_state = reporter.state();

    // Initialize dashboard with every job and task.
    let mut dashboard = ProcessingDashboard::new_session(&jobs, &config);

    // Execute the existing global processing session.
    let worker =
        thread::spawn(move || execute_processing_session_with_progress(&jobs, &config, &reporter));

    // Display live processing statistics.
    let dashboard_result = run_dashboard(
        &mut terminal,
        &mut dashboard,
        &receiver,
        &progress_state,
        &worker,
    );

    // Always wait for the processing session.
    let processing_result = worker.join();

    // Restore terminal before printing results.
    drop(terminal);
    drop(guard);

    dashboard_result?;

    let result = processing_result.map_err(|_| "processing session thread panicked")??;

    print_session_summary(&dashboard, &result);

    Ok(result)
}

// -----------------------------------------------------------------------------
// Multi-job session summary
// -----------------------------------------------------------------------------

fn print_session_summary(dashboard: &ProcessingDashboard, result: &ProcessingSessionResult) {
    let completed_jobs = result
        .jobs
        .iter()
        .filter(|job| job.status == ProcessingStatus::Complete)
        .count();

    let failed_jobs = result.jobs.len() - completed_jobs;

    let mut stderr = io::stderr().lock();

    let _ = writeln!(stderr);
    let _ = writeln!(stderr, "MARKETFORGE — PROCESSING SESSION COMPLETE");

    let _ = writeln!(stderr, "{}", "─".repeat(60));

    let _ = writeln!(stderr, "Jobs              {}", result.jobs.len(),);

    let _ = writeln!(stderr, "Jobs completed    {}", completed_jobs,);

    let _ = writeln!(stderr, "Jobs failed       {}", failed_jobs,);

    let _ = writeln!(stderr, "Total tasks       {}", result.total_tasks,);

    let _ = writeln!(stderr, "Tasks completed   {}", dashboard.completed_tasks(),);

    let _ = writeln!(stderr, "Tasks failed      {}", dashboard.failed_tasks(),);

    let _ = writeln!(
        stderr,
        "Records read      {}",
        dashboard.total_records_read(),
    );

    let _ = writeln!(
        stderr,
        "Events written    {}",
        dashboard.total_records_written(),
    );

    let _ = writeln!(
        stderr,
        "Records rejected  {}",
        dashboard.total_records_rejected(),
    );

    let _ = writeln!(
        stderr,
        "Elapsed           {}",
        format_duration(dashboard.started.elapsed()),
    );

    let _ = writeln!(stderr, "{}", "─".repeat(60));
    let _ = writeln!(stderr);
}
// -----------------------------------------------------------------------------
// Dashboard event loop
// -----------------------------------------------------------------------------

fn run_dashboard<T>(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    dashboard: &mut ProcessingDashboard,
    receiver: &Receiver<ProgressEvent>,
    progress_state: &Arc<ProgressState>,
    worker: &thread::JoinHandle<T>,
) -> io::Result<()>
where
    T: Send,
{
    loop {
        // Process all available lifecycle events.
        loop {
            match receiver.try_recv() {
                Ok(event) => dashboard.handle_event(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }

        // Refresh atomic processing counters.
        dashboard.update_snapshots(progress_state);
        dashboard.sample_throughput();

        // Render current dashboard.
        terminal.draw(|frame| {
            render(frame, dashboard);
        })?;

        if worker.is_finished() {
            // Consume remaining events.
            while let Ok(event) = receiver.try_recv() {
                dashboard.handle_event(event);
            }

            dashboard.update_snapshots(progress_state);

            terminal.draw(|frame| {
                render(frame, dashboard);
            })?;

            break;
        }

        thread::sleep(REFRESH_INTERVAL);
    }

    Ok(())
}

fn render_throughput(frame: &mut ratatui::Frame, area: Rect, dashboard: &ProcessingDashboard) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(2)])
        .margin(1)
        .split(area);

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
        "Current: {}   Average: {}   Peak: {}",
        format_speed(current),
        format_speed(average as f64),
        format_speed(peak as f64),
    ))
    .style(Style::default().fg(FG));

    frame.render_widget(statistics, sections[1]);
}

fn render_activity(frame: &mut ratatui::Frame, area: Rect, dashboard: &ProcessingDashboard) {
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

fn print_final_summary(dashboard: &ProcessingDashboard, result: &ProcessingResult) {
    let status = if result.status == ProcessingStatus::Complete {
        "COMPLETE"
    } else {
        "FAILED"
    };

    let mut stderr = std::io::stderr().lock();

    let _ = writeln!(stderr);
    let _ = writeln!(stderr, "MARKETFORGE — PROCESSING {status}");
    let _ = writeln!(stderr, "{}", "─".repeat(60));

    let _ = writeln!(
        stderr,
        "Tasks             {} / {}",
        dashboard.completed_tasks() + dashboard.failed_tasks(),
        dashboard.total_tasks,
    );

    let _ = writeln!(stderr, "Completed         {}", dashboard.completed_tasks(),);

    let _ = writeln!(stderr, "Failed            {}", dashboard.failed_tasks(),);

    let _ = writeln!(
        stderr,
        "Records read      {}",
        dashboard.total_records_read(),
    );

    let _ = writeln!(
        stderr,
        "Events written    {}",
        dashboard.total_records_written(),
    );

    let _ = writeln!(
        stderr,
        "Records rejected  {}",
        dashboard.total_records_rejected(),
    );

    let _ = writeln!(
        stderr,
        "Elapsed           {}",
        format_duration(dashboard.started.elapsed()),
    );

    let _ = writeln!(stderr, "{}", "─".repeat(60));
    let _ = writeln!(stderr);
}
