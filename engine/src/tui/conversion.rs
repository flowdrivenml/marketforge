use std::{
    collections::HashSet,
    io,
    sync::{
        Arc,
        mpsc::{Receiver, TryRecvError},
    },
    time::{Duration, Instant},
};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::Style,
    widgets::{Gauge, Paragraph, Wrap},
};

use crate::{
    historical::convert::progress::{
        ConversionOutcome, ConversionPartitionKey, ConversionPhase, ConversionProgressEvent,
        ConversionProgressState,
    },
    shared::terminal::{
        format::{compact, format_duration, format_speed},
        guard::TerminalGuard,
        interrupt::InterruptSignals,
        palette::{BG, CYAN, FG, GREEN, MUTED, RED, YELLOW},
        widgets::panel,
    },
};

pub struct ConversionDashboardContext {
    pub title: String,
    pub details: Vec<String>,
    pub expected_rows: u64,
}

struct Dashboard {
    context: ConversionDashboardContext,
    progress: Arc<ConversionProgressState>,
    started: Instant,
    phase_started: Instant,
    phase: Option<ConversionPhase>,
    phase_complete: bool,
    active: HashSet<ConversionPartitionKey>,
    complete: usize,
    failed: usize,
    planned: usize,
    scanned: u64,
    expected: u64,
    work: String,
    outcome: Option<ConversionOutcome>,
    last_error: Option<String>,
    details: bool,
    scroll: u16,
}

impl Dashboard {
    fn new(context: ConversionDashboardContext, progress: Arc<ConversionProgressState>) -> Self {
        Self {
            context,
            progress,
            started: Instant::now(),
            phase_started: Instant::now(),
            phase: None,
            phase_complete: false,
            active: HashSet::new(),
            complete: 0,
            failed: 0,
            planned: 0,
            scanned: 0,
            expected: 0,
            work: String::new(),
            outcome: None,
            last_error: None,
            details: false,
            scroll: 0,
        }
    }

    fn receive(&mut self, event: ConversionProgressEvent) {
        match event {
            ConversionProgressEvent::PhaseStarted { phase } => {
                self.phase = Some(phase);
                self.phase_complete = false;
                self.phase_started = Instant::now();
                self.scanned = 0;
                self.expected = 0;
                self.work.clear();
            }
            ConversionProgressEvent::PhaseCompleted { phase } => {
                self.phase_complete = self.phase == Some(phase);
            }
            ConversionProgressEvent::PartitionsPlanned { count } => self.planned = count,
            ConversionProgressEvent::PartitionStarted { key } => {
                self.active.insert(key);
            }
            ConversionProgressEvent::PartitionCompleted { key, .. } => {
                self.active.remove(&key);
                self.complete += 1;
            }
            ConversionProgressEvent::PartitionFailed { key, message } => {
                self.active.remove(&key);
                self.failed += 1;
                self.last_error = Some(message);
            }
            ConversionProgressEvent::ReferencePrepassProgress {
                events_scanned,
                total_events,
                checkpoints_created,
                total_checkpoints,
            } => {
                self.scanned = events_scanned;
                self.expected = total_events;
                self.work = format!("Checkpoints {checkpoints_created}/{total_checkpoints}");
            }
            ConversionProgressEvent::ValidationStarted {
                total_files,
                expected_events,
            } => {
                self.expected = if expected_events == 0 {
                    self.context.expected_rows
                } else {
                    expected_events
                };
                self.work = format!("Files 0/{total_files}");
            }
            ConversionProgressEvent::ValidationProgress {
                files_completed,
                events_validated,
                row_groups_validated,
            }
            | ConversionProgressEvent::ValidationCompleted {
                files_completed,
                events_validated,
                row_groups_validated,
            } => {
                self.scanned = self.scanned.max(events_validated);
                self.work = format!(
                    "{files_completed} files · {row_groups_validated} row groups validated"
                );
            }
            ConversionProgressEvent::SourcePreservationProgress {
                events_compared,
                total_events,
            } => {
                self.scanned = events_compared;
                self.expected = total_events;
            }
            ConversionProgressEvent::Finished(outcome) => {
                if let ConversionOutcome::Failed(message) = &outcome {
                    self.last_error = Some(message.clone());
                }
                self.outcome = Some(outcome);
            }
        }
    }

    fn render(&self, frame: &mut ratatui::Frame) {
        let areas = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(frame.area());
        let (status, color) = match &self.outcome {
            Some(outcome) if outcome.succeeded() => (outcome.label(), GREEN),
            Some(ConversionOutcome::Interrupted) => ("INTERRUPTED", YELLOW),
            Some(outcome) => (outcome.label(), RED),
            None if self.progress.is_interrupted() => ("INTERRUPTING", YELLOW),
            None => ("RUNNING", CYAN),
        };
        frame.render_widget(
            Paragraph::new(format!("{status} · {}", self.context.title))
                .style(Style::default().fg(color).bg(BG))
                .block(panel(" Conversion ")),
            areas[0],
        );
        let total = self.progress.total();
        let (done, expected) = if self.phase == Some(ConversionPhase::PhysicalConversion) {
            (total.events_written, self.context.expected_rows)
        } else {
            (self.scanned, self.expected)
        };
        let phase = self.phase.map_or("Preparing", ConversionPhase::label);
        let elapsed = self.phase_started.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 {
            done as f64 / elapsed
        } else {
            0.0
        };
        if expected > 0 {
            let ratio = (done as f64 / expected as f64).min(1.0);
            let mut label = format!(
                "{phase} · {} / {} · {}",
                compact(done),
                compact(expected),
                format_speed(rate)
            );
            if self.outcome.is_none() && done > 0 && done < expected {
                let seconds = ((expected - done) as f64 / rate) as u64;
                label.push_str(&format!(
                    " · ETA {}",
                    format_duration(Duration::from_secs(seconds))
                ));
            }
            frame.render_widget(
                Gauge::default()
                    .gauge_style(Style::default().fg(CYAN).bg(MUTED))
                    .ratio(ratio)
                    .label(label),
                areas[1],
            );
        } else {
            frame.render_widget(
                Paragraph::new(if self.phase_complete {
                    format!("{phase} · complete")
                } else {
                    phase.to_owned()
                })
                .style(Style::default().fg(CYAN)),
                areas[1],
            );
        }
        let failures = [
            "missing",
            "stale",
            "future",
            "mismatch",
            "arithmetic",
            "no path",
        ]
        .into_iter()
        .zip(total.failures)
        .filter(|(_, count)| *count > 0)
        .map(|(reason, count)| format!("{reason} {}", compact(count)))
        .collect::<Vec<_>>()
        .join(" · ");
        let body = if self.details {
            self.context.details.join("\n")
        } else {
            format!(
                "Elapsed {} · {} conversion workers active\nPartitions {} active · {}/{} complete · {} failed\nRead {} · written {} · {} files · {} row groups\nIdentity {} · converted {} · unavailable {} · skipped {}\nTrades {} · depth {} · depth levels {} ({} valued · {} unavailable · {} skipped)\nReference updates {} internal · {} external\n{}\n{}\n{}",
                format_duration(self.started.elapsed()),
                self.active.len(),
                self.active.len(),
                self.complete,
                self.planned,
                self.failed,
                compact(total.events_read),
                compact(total.events_written),
                compact(total.files_written),
                compact(total.row_groups_written),
                compact(total.identity_events),
                compact(total.converted_events),
                compact(total.unavailable_events),
                compact(total.skipped_events),
                compact(total.trade_events),
                compact(total.depth_events),
                compact(total.depth_levels_read),
                compact(total.depth_levels_converted),
                compact(total.depth_levels_unavailable),
                compact(total.depth_levels_skipped),
                compact(total.internal_reference_updates),
                compact(total.external_reference_updates),
                failures,
                self.work,
                self.last_error.as_deref().unwrap_or_default(),
            )
        };
        let pane = panel(if self.details {
            " Job details "
        } else {
            " Execution "
        });
        let paragraph = Paragraph::new(body)
            .wrap(Wrap { trim: false })
            .style(Style::default().fg(FG));
        frame.render_widget(paragraph.scroll((self.scroll, 0)).block(pane), areas[2]);
        frame.render_widget(
            Paragraph::new("D details · ↑↓ scroll · Ctrl+C interrupt")
                .style(Style::default().fg(MUTED)),
            areas[3],
        );
    }
}

pub fn execute_conversion_with_tui(
    receiver: Receiver<ConversionProgressEvent>,
    progress: Arc<ConversionProgressState>,
    context: ConversionDashboardContext,
    start: std::sync::mpsc::Sender<()>,
) -> io::Result<()> {
    let _signals = InterruptSignals::install()?;
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut dashboard = Dashboard::new(context, progress);
    terminal.draw(|frame| dashboard.render(frame))?;
    start
        .send(())
        .map_err(|_| io::Error::other("conversion worker disconnected before startup"))?;
    let hold = std::env::var_os("MARKETFORGE_TUI_HOLD").is_some();

    loop {
        if TerminalGuard::interrupted_by_panic() {
            return Err(io::Error::other(
                "conversion dashboard interrupted by panic",
            ));
        }
        if InterruptSignals::requested() {
            dashboard.progress.interrupt();
        }
        loop {
            match receiver.try_recv() {
                Ok(event) => dashboard.receive(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) if dashboard.outcome.is_some() => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(io::Error::other(
                        "conversion progress channel disconnected before completion",
                    ));
                }
            }
        }
        terminal.draw(|frame| dashboard.render(frame))?;
        if dashboard.outcome.is_some() && (!hold || dashboard.progress.is_interrupted()) {
            return Ok(());
        }
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    dashboard.progress.interrupt()
                }
                KeyCode::Char('q') | KeyCode::Esc if dashboard.outcome.is_some() => return Ok(()),
                KeyCode::Char('d' | 'D') => {
                    dashboard.details = !dashboard.details;
                    dashboard.scroll = 0;
                }
                KeyCode::Down => dashboard.scroll = dashboard.scroll.saturating_add(1),
                KeyCode::Up => dashboard.scroll = dashboard.scroll.saturating_sub(1),
                _ => (),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    #[test]
    fn status_and_details_render_at_small_sizes() {
        let mut dashboard = Dashboard::new(
            ConversionDashboardContext {
                title: "Job 42 · Materialize · Combined".into(),
                details: vec![format!("Input /{}", "nested/".repeat(80))],
                expected_rows: 8,
            },
            Arc::default(),
        );
        dashboard.receive(ConversionProgressEvent::Finished(
            ConversionOutcome::Materialized,
        ));
        for (width, height) in [(100, 20), (60, 12), (10, 3)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
            terminal.draw(|frame| dashboard.render(frame)).unwrap();
            if width >= 60 {
                assert!(
                    terminal
                        .backend()
                        .to_string()
                        .contains("MATERIALIZED (UNPUBLISHED)")
                );
            }
            dashboard.details = true;
            terminal.draw(|frame| dashboard.render(frame)).unwrap();
            dashboard.details = false;
        }
    }
}
