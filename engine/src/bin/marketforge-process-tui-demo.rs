use std::{
    io::{self, stdout},
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
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Gauge, Paragraph, Row, Table},
};

const BG: Color = Color::Rgb(26, 27, 38);
const FG: Color = Color::Rgb(192, 202, 245);
const BLUE: Color = Color::Rgb(122, 162, 247);
const CYAN: Color = Color::Rgb(125, 207, 255);
const GREEN: Color = Color::Rgb(158, 206, 106);

struct Task {
    exchange: &'static str,
    instrument: &'static str,
    kind: &'static str,
    records: u64,
    written: u64,
    speed: u64,
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        if let Err(error) = execute!(stdout(), EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error);
        }
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

fn compact(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn main() -> io::Result<()> {
    let _guard = TerminalGuard::enter()?;

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut tasks = vec![
        Task {
            exchange: "Bybit",
            instrument: "BTCUSDT",
            kind: "Trades",
            records: 0,
            written: 0,
            speed: 1_240_000,
        },
        Task {
            exchange: "Binance",
            instrument: "ETHUSDT",
            kind: "Trades",
            records: 0,
            written: 0,
            speed: 1_820_000,
        },
        Task {
            exchange: "OKX",
            instrument: "BTC-USDT-SWAP",
            kind: "Depth",
            records: 0,
            written: 0,
            speed: 640_000,
        },
    ];

    let started = Instant::now();
    let duration = Duration::from_secs(30);
    let refresh = Duration::from_millis(250);

    loop {
        let elapsed = started.elapsed();
        let progress = (elapsed.as_secs_f64() / duration.as_secs_f64()).clamp(0.0, 1.0);

        for task in &mut tasks {
            task.records = (task.speed as f64 * elapsed.as_secs_f64()) as u64;

            task.written = task.records.saturating_sub(task.records / 1000);
        }

        terminal.draw(|frame| {
            let area = frame.area();

            let background = Block::default().style(Style::default().bg(BG).fg(FG));

            frame.render_widget(background, area);

            let sections = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(7),
                    Constraint::Min(8),
                    Constraint::Length(6),
                ])
                .split(area);

            let header = Paragraph::new(
                "◆ MARKETFORGE                                      PROCESSING ● RUNNING",
            )
            .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD))
            .block(Block::default().borders(Borders::ALL));

            frame.render_widget(header, sections[0]);

            let overview = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Length(3)])
                .margin(1)
                .split(sections[1]);

            let elapsed_text = format!(
                "Tasks: 3 active    Workers: 3    Elapsed: {:.1}s",
                elapsed.as_secs_f64()
            );

            frame.render_widget(
                Paragraph::new(elapsed_text).style(Style::default().fg(FG)),
                overview[0],
            );

            let gauge = Gauge::default()
                .block(Block::default().title("Demo progress"))
                .gauge_style(Style::default().fg(CYAN).bg(BG))
                .ratio(progress)
                .label(format!("{:.1}%", progress * 100.0));

            frame.render_widget(gauge, overview[1]);

            let rows = tasks.iter().map(|task| {
                Row::new(vec![
                    task.exchange.to_string(),
                    task.instrument.to_string(),
                    task.kind.to_string(),
                    compact(task.records),
                    compact(task.written),
                    format!("{}/s", compact(task.speed)),
                ])
            });

            let table = Table::new(
                rows,
                [
                    Constraint::Length(12),
                    Constraint::Length(19),
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(12),
                    Constraint::Length(12),
                ],
            )
            .header(
                Row::new(["EXCHANGE", "INSTRUMENT", "TYPE", "READ", "WRITTEN", "SPEED"])
                    .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD)),
            )
            .block(
                Block::default()
                    .title("ACTIVE PROCESSING")
                    .borders(Borders::ALL),
            );

            frame.render_widget(table, sections[2]);

            let total_read: u64 = tasks.iter().map(|t| t.records).sum();
            let total_written: u64 = tasks.iter().map(|t| t.written).sum();
            let total_speed: u64 = tasks.iter().map(|t| t.speed).sum();

            let statistics = Paragraph::new(format!(
                "Records read: {:>15}\nEvents written: {:>14}\nThroughput: {:>17}/s",
                total_read,
                total_written,
                compact(total_speed),
            ))
            .style(Style::default().fg(GREEN))
            .block(Block::default().title("STATISTICS").borders(Borders::ALL));

            frame.render_widget(statistics, sections[3]);
        })?;

        if elapsed >= duration {
            break;
        }

        thread::sleep(refresh);
    }

    Ok(())
}
