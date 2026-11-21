//! Terminal lifecycle for the conversion dashboard.
mod rate;
mod state;
mod view;

use std::{
    io,
    sync::{
        Arc,
        mpsc::{Receiver, Sender, TryRecvError},
    },
    time::{Duration, Instant},
};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::{Terminal, backend::CrosstermBackend};

use crate::{
    historical::{
        convert::progress::{ConversionProgressEvent, ConversionProgressState},
        merge::UnifiedMergeMode,
    },
    shared::terminal::{guard::TerminalGuard, interrupt::InterruptSignals},
};

use state::{Dashboard, View};

pub struct ConversionDashboardContext {
    pub operation: &'static str,
    pub mode: UnifiedMergeMode,
    pub target_quote: String,
    pub instruments: usize,
    pub streams: usize,
    pub expected_rows: u64,
    pub reconstruction_rows: u64,
    pub trade_events: u64,
    pub depth_events: u64,
    pub requested_workers: usize,
    pub details: Vec<String>,
}

pub(crate) const UNAVAILABLE_REASONS: [&str; 6] = [
    "Missing reference",
    "Stale reference",
    "Future reference",
    "Identity mismatch",
    "Arithmetic",
    "No path",
];

pub fn execute_conversion_with_tui(
    receiver: Receiver<ConversionProgressEvent>,
    progress: Arc<ConversionProgressState>,
    context: ConversionDashboardContext,
    start: Sender<()>,
) -> io::Result<()> {
    let _signals = InterruptSignals::install()?;
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut dashboard = Dashboard::new(context, Instant::now());
    terminal.draw(|frame| view::render(frame, &mut dashboard, Instant::now()))?;
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
            progress.interrupt();
        }
        let now = Instant::now();
        if progress.is_interrupted() {
            dashboard.interrupt();
        }
        loop {
            match receiver.try_recv() {
                Ok(event) => dashboard.receive(event, now),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) if dashboard.outcome.is_some() => break,
                Err(TryRecvError::Disconnected) => {
                    return Err(io::Error::other(
                        "conversion progress channel disconnected before completion",
                    ));
                }
            }
        }
        dashboard.sample(&progress, now);
        terminal.draw(|frame| view::render(frame, &mut dashboard, now))?;
        if dashboard.outcome.is_some() && (!hold || progress.is_interrupted()) {
            return Ok(());
        }
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind != KeyEventKind::Release
        {
            match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    progress.interrupt();
                }
                KeyCode::Char('q') if dashboard.outcome.is_some() => return Ok(()),
                KeyCode::Esc if dashboard.view != View::Overview => dashboard.view = View::Overview,
                KeyCode::Esc if dashboard.outcome.is_some() => return Ok(()),
                KeyCode::Char('d' | 'D') => {
                    dashboard.view = if dashboard.view == View::Details {
                        View::Overview
                    } else {
                        View::Details
                    }
                }
                KeyCode::Char('?') => {
                    dashboard.view = if dashboard.view == View::Help {
                        View::Overview
                    } else {
                        View::Help
                    }
                }
                KeyCode::Down => dashboard.navigate(true, false),
                KeyCode::Up => dashboard.navigate(false, false),
                KeyCode::PageDown => dashboard.navigate(true, true),
                KeyCode::PageUp => dashboard.navigate(false, true),
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests;
