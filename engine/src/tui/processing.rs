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
    widgets::{Block, Clear, Gauge, Paragraph, Row, Sparkline, Table, TableState},
};
use std::io::Write;
use std::{
    collections::{BTreeMap, VecDeque},
    io::{self, IsTerminal, Stdout},
    sync::Arc,
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

use crate::shared::terminal::widgets::panel;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
#[path = "processing/state.rs"]
mod state;
use state::*;
#[cfg(test)]
use std::sync::mpsc;
#[path = "processing/view.rs"]
mod view;
use view::render;
pub fn execute_processing_with_tui(
    job: ProcessingJob,
    config: ProcessingConfig,
) -> Result<ProcessingResult, Box<dyn std::error::Error>> {
    if !io::stdout().is_terminal() {
        return Err("processing TUI requires an interactive terminal".into());
    }

    // Initialize terminal before launching processing.
    let log = crate::historical::normalize::progress::log::DashboardLog::enter()?;
    let guard = TerminalGuard::enter()?;

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = match Terminal::new(backend) {
        Ok(terminal) => terminal,
        Err(error) => {
            drop(guard);
            return Err(Box::new(error));
        }
    };

    let reporter = ProgressReporter::coalesced();
    let progress_state = reporter.state();

    let mut dashboard = ProcessingDashboard::new(&job, &config);
    dashboard.log_path = Some(log.path.display().to_string());
    dashboard
        .activity
        .push_back(format!("Diagnostics: {}", log.path.display()));

    let worker =
        thread::spawn(move || execute_processing_job_with_progress(&job, &config, &reporter));

    let dashboard_result = run_dashboard(&mut terminal, &mut dashboard, &progress_state, &worker);

    // Release terminal ownership immediately, including on rendering failures.
    drop(terminal);
    drop(guard);
    if let Err(error) = dashboard_result {
        eprintln!("Dashboard stopped: {error}; waiting for processing to finish");
    }
    let processing_result = worker.join();
    loop {
        let events = progress_state.take_events(256);
        if events.is_empty() {
            break;
        }
        for event in events {
            dashboard.handle_event(event);
        }
    }
    dashboard.update_snapshots(&progress_state);
    dashboard.ended.get_or_insert_with(Instant::now);

    eprintln!("Normalization diagnostics: {}", log.path.display());

    let result = processing_result.map_err(|_| "processing worker thread panicked")??;

    dashboard.reconcile_results(std::slice::from_ref(&result));

    // Print permanent processing summary.
    print_final_summary(&dashboard, &result);
    print_output_summary(&dashboard, std::slice::from_ref(&result));

    Ok(result)
}
// -----------------------------------------------------------------------------
// Multi-job processing session with TUI
// -----------------------------------------------------------------------------

pub fn execute_processing_session_with_tui(
    jobs: Vec<ProcessingJob>,
    config: ProcessingConfig,
) -> Result<ProcessingSessionResult, Box<dyn std::error::Error>> {
    let dashboard = ProcessingDashboard::new_session(&jobs, &config);
    execute_session_dashboard(dashboard, config, move |_| Ok(jobs))
}

/// Resolve physical sources on the worker after the dashboard owns the terminal.
pub fn execute_prepared_session_with_tui<F>(
    batch: &crate::historical::config::PreparedBatch,
    config: ProcessingConfig,
    prepare: F,
) -> Result<ProcessingSessionResult, Box<dyn std::error::Error>>
where
    F: FnOnce(&ProgressReporter) -> crate::shared::error::Result<Vec<ProcessingJob>>
        + Send
        + 'static,
{
    let dashboard = ProcessingDashboard::new_prepared(batch, &config);
    execute_session_dashboard(dashboard, config, prepare)
}

fn execute_session_dashboard<F>(
    mut dashboard: ProcessingDashboard,
    config: ProcessingConfig,
    prepare: F,
) -> Result<ProcessingSessionResult, Box<dyn std::error::Error>>
where
    F: FnOnce(&ProgressReporter) -> crate::shared::error::Result<Vec<ProcessingJob>>
        + Send
        + 'static,
{
    if !io::stdout().is_terminal() {
        return Err("processing TUI requires an interactive terminal".into());
    }

    // Initialize terminal before launching processing.
    let log = crate::historical::normalize::progress::log::DashboardLog::enter()?;
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
    let reporter = ProgressReporter::coalesced();
    let progress_state = reporter.state();

    // Initialize dashboard with every job and task.
    dashboard.log_path = Some(log.path.display().to_string());
    dashboard
        .activity
        .push_back(format!("Diagnostics: {}", log.path.display()));

    // Execute the existing global processing session.
    let worker = thread::spawn(move || {
        let result = prepare(&reporter)
            .and_then(|jobs| execute_processing_session_with_progress(&jobs, &config, &reporter));
        if result.is_err() {
            reporter.finished(false);
        }
        result
    });

    // Display live processing statistics.
    let dashboard_result = run_dashboard(&mut terminal, &mut dashboard, &progress_state, &worker);

    drop(terminal);
    drop(guard);
    if let Err(error) = dashboard_result {
        eprintln!("Dashboard stopped: {error}; waiting for processing to finish");
    }
    let processing_result = worker.join();
    loop {
        let events = progress_state.take_events(256);
        if events.is_empty() {
            break;
        }
        for event in events {
            dashboard.handle_event(event);
        }
    }
    dashboard.update_snapshots(&progress_state);
    dashboard.ended.get_or_insert_with(Instant::now);

    eprintln!("Normalization diagnostics: {}", log.path.display());

    let result = processing_result.map_err(|_| "processing session thread panicked")??;

    dashboard.reconcile_results(&result.jobs);
    print_session_summary(&dashboard, &result);
    print_output_summary(&dashboard, &result.jobs);

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
    let _ = writeln!(stderr, "Tasks skipped     {}", dashboard.skipped_tasks());
    let _ = writeln!(stderr, "Tasks stopped     {}", dashboard.stopped_tasks());
    let _ = writeln!(stderr, "Tasks not run     {}", dashboard.not_run_tasks());
    let _ = writeln!(
        stderr,
        "Streams failed    {}",
        dashboard.stream_failures.len()
    );

    let _ = writeln!(
        stderr,
        "Records read      {}",
        dashboard.total_records_read(),
    );

    let _ = writeln!(
        stderr,
        "Events written    {}",
        result
            .jobs
            .iter()
            .fold(0u64, |total, job| total.saturating_add(job.events_written)),
    );

    let _ = writeln!(
        stderr,
        "Records rejected  {}",
        dashboard.total_records_rejected(),
    );

    let _ = writeln!(
        stderr,
        "Elapsed           {}",
        format_duration(dashboard.elapsed()),
    );

    let _ = writeln!(stderr, "{}", "─".repeat(60));
    let _ = writeln!(stderr);
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
        dashboard.settled_tasks(),
        dashboard.total_tasks,
    );

    let _ = writeln!(stderr, "Completed         {}", dashboard.completed_tasks(),);

    let _ = writeln!(stderr, "Failed            {}", dashboard.failed_tasks(),);
    let _ = writeln!(stderr, "Skipped           {}", dashboard.skipped_tasks());

    let _ = writeln!(
        stderr,
        "Records read      {}",
        dashboard.total_records_read(),
    );

    let _ = writeln!(stderr, "Events written    {}", result.events_written,);

    let _ = writeln!(
        stderr,
        "Records rejected  {}",
        dashboard.total_records_rejected(),
    );

    let _ = writeln!(
        stderr,
        "Elapsed           {}",
        format_duration(dashboard.elapsed()),
    );

    let _ = writeln!(stderr, "{}", "─".repeat(60));
    let _ = writeln!(stderr);
}

fn run_dashboard<T: Send>(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    d: &mut ProcessingDashboard,
    state: &Arc<ProgressState>,
    worker: &thread::JoinHandle<T>,
) -> io::Result<()> {
    loop {
        if TerminalGuard::interrupted_by_panic() {
            return Err(io::Error::other("processing panicked; terminal restored"));
        }
        for event in state.take_events(256) {
            d.handle_event(event);
        }
        d.update_snapshots(state);
        terminal.draw(|f| render(f, d))?;
        if worker.is_finished() && !state.has_pending_events() {
            d.update_snapshots(state);
            if !d.finished {
                d.handle_event(ProgressEvent::Finished { success: false });
            }
            terminal.draw(|f| render(f, d))?;
            break;
        }
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('?') => d.help = !d.help,
                KeyCode::Char('a') => d.next_active(),
                KeyCode::Esc => {
                    d.help = false;
                    d.detail = false;
                }
                KeyCode::Tab | KeyCode::Enter => {
                    d.detail = !d.detail;
                    d.detail_scroll = 0;
                }
                code if d.detail => match code {
                    KeyCode::Down => d.detail_scroll = d.detail_scroll.saturating_add(1),
                    KeyCode::Up => d.detail_scroll = d.detail_scroll.saturating_sub(1),
                    KeyCode::PageDown => d.detail_scroll = d.detail_scroll.saturating_add(10),
                    KeyCode::PageUp => d.detail_scroll = d.detail_scroll.saturating_sub(10),
                    KeyCode::Home => d.detail_scroll = 0,
                    _ => {}
                },
                code => d.navigate(
                    code,
                    terminal.size()?.height.saturating_sub(14).max(1) as usize,
                ),
            }
        }
    }
    Ok(())
}

fn print_output_summary(d: &ProcessingDashboard, results: &[ProcessingResult]) {
    for path in d.published.values() {
        eprintln!("Published manifest: {path}");
    }
    for result in results {
        if let Some(failure) = &result.failure {
            eprintln!("Job {} failed: {}", result.job_id.0, failure.message);
        }
        if let Some(path) = &result.failure_report_path {
            eprintln!("Failure context / partial output status: {path}");
        }
        if let Some(path) = &result.manifest_path {
            eprintln!("Manifest: {path}");
        }
    }
}

#[cfg(test)]
#[path = "processing/tests.rs"]
mod tests;
