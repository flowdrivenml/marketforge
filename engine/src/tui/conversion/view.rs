use std::time::Instant;

use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Cell, Gauge, Padding, Paragraph, Row, Sparkline, Table},
};

use crate::{
    historical::convert::progress::{ConversionOutcome, ConversionPhase},
    shared::terminal::{
        format::{compact, format_duration},
        palette::{BG, BLUE, CYAN, FG, GREEN, RED, YELLOW},
        widgets::panel,
    },
};

use super::{
    UNAVAILABLE_REASONS,
    state::{Dashboard, PartitionState, Stage, View},
};

const SELECTED: Color = Color::Rgb(41, 47, 68);

fn color(color: Color) -> Style {
    Style::default().fg(color)
}
fn emphasis(value: Color) -> Style {
    color(value).add_modifier(Modifier::BOLD)
}

fn section(title: &str) -> Block<'_> {
    panel(title).padding(Padding::horizontal(1))
}

fn line_at(area: Rect, row: u16) -> Rect {
    Rect::new(area.x, area.y.saturating_add(row), area.width, 1)
}

pub(super) fn render(frame: &mut Frame, d: &mut Dashboard, now: Instant) {
    let area = frame.area();
    frame.render_widget(Block::default().style(color(FG).bg(BG)), area);
    let status_color = match &d.outcome {
        Some(ConversionOutcome::Failed(_)) => RED,
        Some(ConversionOutcome::Interrupted) => YELLOW,
        Some(_) => GREEN,
        None if d.interrupting => YELLOW,
        None => CYAN,
    };
    if area.width < 62 || area.height < 12 {
        let phase = d
            .active
            .as_ref()
            .map_or("Conversion", |stage| stage.phase.label());
        let mut lines = vec![
            Line::styled(d.status(), emphasis(status_color)),
            Line::raw(phase),
        ];
        if let Some(stage) = &d.active {
            lines.push(Line::raw(counts(stage)));
        }
        if let Some(error) = &d.error {
            lines.push(Line::styled(error.as_str(), color(RED)));
        }
        frame.render_widget(Paragraph::new(lines), area);
        if area.height >= 3 {
            footer(frame, line_at(area, area.height - 1), d, true);
        }
        return;
    }
    let inner = area.inner(ratatui::layout::Margin {
        horizontal: 1,
        vertical: 0,
    });
    let error = d.error.as_deref().or_else(|| d.counter_warning());
    let [header, phase, outcomes, alert, body, keys] = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(4),
        Constraint::Length(if area.width >= 111 || area.width < 76 {
            2
        } else {
            1
        }),
        Constraint::Length(u16::from(error.is_some())),
        Constraint::Min(0),
        Constraint::Length(1),
    ])
    .areas(inner);
    let elapsed = d.ended.unwrap_or(now).saturating_duration_since(d.started);
    let title = Line::from(vec![
        Span::styled("MarketForge / Conversion", emphasis(BLUE)),
        Span::raw("   "),
        Span::styled(d.status(), emphasis(status_color)),
    ]);
    frame.render_widget(Paragraph::new(title), line_at(header, 0));
    frame.render_widget(
        Paragraph::new(format!(
            "{} · {:?} → {} · {} instruments / {} streams",
            d.context.operation,
            d.context.mode,
            d.context.target_quote,
            d.context.instruments,
            d.context.streams
        )),
        line_at(header, 1),
    );
    if area.width >= 85 {
        frame.render_widget(
            Paragraph::new(format!("Elapsed {}", format_duration(elapsed)))
                .alignment(Alignment::Right),
            line_at(header, 0),
        );
    } else {
        frame.render_widget(
            Paragraph::new(format!("Elapsed {}", format_duration(elapsed))),
            line_at(header, 2),
        );
    }
    render_phase(frame, d, phase);
    summary(frame, d, outcomes);
    if let Some(message) = error {
        frame.render_widget(
            Paragraph::new(format!(
                "{}  {message}",
                if d.error.is_some() {
                    "Execution"
                } else {
                    "Telemetry"
                }
            ))
            .style(emphasis(RED)),
            alert,
        );
    }
    match d.view {
        View::Overview => overview(frame, d, body, now),
        View::Details => details(frame, d, body),
        View::Help => frame.render_widget(
            Paragraph::new(vec![
                Line::raw("D       Toggle job details"),
                Line::raw("↑ / ↓   Select a partition or scroll details"),
                Line::raw("Pg↑/Pg↓ Move one page"),
                Line::raw("Ctrl+C  Request interruption"),
                Line::raw("Esc     Return to overview"),
                Line::raw("Q       Dismiss a held result"),
            ])
            .block(section(" Keyboard help ")),
            body,
        ),
    }
    footer(frame, keys, d, false);
}

fn footer(frame: &mut Frame, area: Rect, d: &Dashboard, tiny: bool) {
    let message = if d.outcome.is_some() {
        if tiny {
            "Q close"
        } else {
            "D details   ↑↓ navigate   ? help   Q close"
        }
    } else if tiny {
        "Ctrl+C stop"
    } else if d.view == View::Details {
        "D overview   ↑↓ / Pg↑ Pg↓ scroll   ? help   Ctrl+C interrupt"
    } else {
        "D details   ↑↓ / Pg↑ Pg↓ navigate   ? help   Ctrl+C interrupt"
    };
    frame.render_widget(Paragraph::new(message).style(color(FG)), area);
}

fn counts(stage: &Stage) -> String {
    if let Some(expected) = stage.expected.filter(|&n| n > 0) {
        format!(
            "{} / {} {}",
            compact(stage.done),
            compact(expected),
            stage.unit()
        )
    } else if !stage.work.is_empty() {
        stage.work.clone()
    } else {
        "In progress".into()
    }
}

fn render_phase(frame: &mut Frame, d: &Dashboard, area: Rect) {
    if let Some(stage) = &d.active {
        let scope = match stage.phase {
            ConversionPhase::PhysicalConversion => "Conversion",
            ConversionPhase::ReferencePrepass => "Prepass",
            ConversionPhase::TimestampScan => "Scan",
            ConversionPhase::PhysicalValidation => "Validation",
            ConversionPhase::SourcePreservation => "Comparison",
            _ => "Phase",
        };
        let label = if d.outcome.is_some() {
            format!("Stopped during {}", stage.phase.label())
        } else {
            stage.phase.label().into()
        };
        frame.render_widget(
            Paragraph::new(label).style(emphasis(CYAN)),
            line_at(area, 0),
        );
        if let Some(expected) = stage.expected.filter(|&n| n > 0) {
            let ratio = stage.done as f64 / expected as f64;
            let count = counts(stage);
            let bar_width = if area.width >= 100 { 32 } else { 12 };
            let row = line_at(area, 1);
            let [bar, text] =
                Layout::horizontal([Constraint::Length(bar_width), Constraint::Min(0)]).areas(row);
            frame.render_widget(
                Gauge::default()
                    .ratio(ratio.min(1.0))
                    .label(" ")
                    .use_unicode(true)
                    .gauge_style(color(CYAN).bg(SELECTED)),
                bar,
            );
            frame.render_widget(
                Paragraph::new(format!("  {:.0}%  {count}", ratio * 100.0)),
                text,
            );
            if d.outcome.is_none() {
                let rate = stage.rate.current.map_or("estimating".into(), |rate| {
                    format!("{} rows/s", compact(rate as u64))
                });
                let eta = stage
                    .rate
                    .eta(expected.saturating_sub(stage.done))
                    .map_or("—".into(), format_duration);
                frame.render_widget(
                    Paragraph::new(format!(
                        "{rate} · {scope} ETA {eta}{}",
                        if area.width >= 100 && !stage.work.is_empty() {
                            format!("  ·  {}", stage.work)
                        } else {
                            String::new()
                        }
                    )),
                    line_at(area, 2),
                );
            }
        } else {
            frame.render_widget(
                Paragraph::new(if d.outcome.is_some() {
                    "Stopped".into()
                } else {
                    counts(stage)
                }),
                line_at(area, 1),
            );
        }
    } else {
        let label = if d.outcome.is_some() {
            "Result"
        } else if d.completed.is_empty() {
            "Preparing conversion"
        } else {
            "Preparing next stage"
        };
        frame.render_widget(
            Paragraph::new(label).style(emphasis(CYAN)),
            line_at(area, 0),
        );
        frame.render_widget(
            Paragraph::new(format!(
                "{} physical rows · {} reconstruction",
                compact(d.context.expected_rows),
                compact(d.context.reconstruction_rows)
            )),
            line_at(area, 1),
        );
    }
    if let Some((phase, elapsed)) = d.completed.last() {
        frame.render_widget(
            Paragraph::new(format!(
                "Completed  {}  {}",
                phase.label(),
                format_duration(*elapsed)
            ))
            .style(color(FG)),
            line_at(area, 3),
        );
    }
}

fn summary(frame: &mut Frame, d: &Dashboard, area: Rect) {
    let t = &d.total;
    let values = [
        ("Identity", t.identity_events, FG),
        ("Converted", t.converted_events, GREEN),
        (
            "Unavailable",
            t.unavailable_events,
            if t.unavailable_events > 0 { YELLOW } else { FG },
        ),
        ("Skipped", t.skipped_events, FG),
    ];
    let compact_grid = area.width < 74;
    let columns = if compact_grid { 2 } else { 4 };
    for (index, (label, count, shade)) in values.into_iter().enumerate() {
        let row = line_at(area, (index / columns) as u16);
        let cells =
            Layout::horizontal(vec![Constraint::Ratio(1, columns as u32); columns]).split(row);
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::raw(format!("{label} ")),
                Span::styled(compact(count), emphasis(shade)),
            ])),
            cells[index % columns],
        );
    }
    if area.height > 1 && !compact_grid {
        frame.render_widget(Paragraph::new(format!(
            "{} read · {} written · {} trades · {} depth events · {} finalized files · {} row groups",
            compact(t.events_read), compact(t.events_written), compact(t.trade_events), compact(t.depth_events),
            compact(t.files_written), compact(t.row_groups_written)
        )), line_at(area, 1));
    }
}

fn overview(frame: &mut Frame, d: &mut Dashboard, area: Rect, now: Instant) {
    let wide = area.width >= 132;
    let [main, _gap, side] = Layout::horizontal([
        Constraint::Min(0),
        Constraint::Length(if wide { 2 } else { 0 }),
        Constraint::Length(if wide { 44 } else { 0 }),
    ])
    .areas(area);
    let t = d.total;
    let active = d
        .partitions
        .values()
        .filter(|p| p.state == PartitionState::Running)
        .count();
    let done = d
        .partitions
        .values()
        .filter(|p| p.state == PartitionState::Complete)
        .count();
    let failed = d
        .partitions
        .values()
        .filter(|p| matches!(p.state, PartitionState::Failed(_)))
        .count();
    let partition_summary = format!(
        "{active} active · {done} complete · {failed} failed · worker capacity {}",
        d.workers.map_or_else(
            || format!("{} requested", d.context.requested_workers),
            |n| n.to_string()
        )
    );
    let activity = if area.height >= 12 {
        u16::min(6, d.activity.len() as u16 + 2)
    } else {
        0
    };
    let compact_diagnostics = if wide || main.height < 3 {
        0
    } else if main.height >= 14 {
        2
    } else {
        1
    };
    let table_height = (d.partitions.len().max(1).min(u16::MAX as usize - 3) as u16 + 3).min(
        main.height
            .saturating_sub(compact_diagnostics + activity + 1),
    );
    let [status, table, diagnostic_row, history, _spare] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(table_height),
        Constraint::Length(compact_diagnostics),
        Constraint::Length(activity),
        Constraint::Min(0),
    ])
    .areas(main);
    frame.render_widget(Paragraph::new(partition_summary), status);
    partitions(frame, d, table, now);
    if compact_diagnostics > 0 {
        let summary = if t.unavailable_events > 0 && d.restored_updates.is_none() {
            let leading = UNAVAILABLE_REASONS
                .iter()
                .zip(t.failures)
                .max_by_key(|&(_, count)| count);
            leading.filter(|(_, count)| *count > 0).map_or(
                "Unavailable valuation details · D".into(),
                |(reason, count)| format!("{reason} {} rows · D diagnostics", compact(count)),
            )
        } else {
            format!("{} checkpoints · D details", d.checkpoints.0)
        };
        frame.render_widget(Paragraph::new(summary), line_at(diagnostic_row, 0));
        if compact_diagnostics == 2 {
            let references = d.restored_updates.map_or_else(
                || {
                    format!(
                        "Updates {} internal / {} external · {} checkpoints · {} valued levels",
                        compact(t.internal_reference_updates),
                        compact(t.external_reference_updates),
                        d.checkpoints.0,
                        compact(t.depth_levels_converted)
                    )
                },
                |count| format!("{} reference updates from receipt", compact(count)),
            );
            frame.render_widget(Paragraph::new(references), line_at(diagnostic_row, 1));
        }
    }
    if activity > 0 {
        frame.render_widget(
            Paragraph::new(
                d.activity
                    .iter()
                    .rev()
                    .take(history.height.saturating_sub(2).into())
                    .map(|text| Line::raw(text.as_str()))
                    .collect::<Vec<_>>(),
            )
            .block(section(" Recent activity ")),
            history,
        );
    }
    if wide {
        let spark = d
            .active
            .as_ref()
            .filter(|s| s.rate.history.len() >= 2 && side.height >= 21);
        let [_top, diagnostics, throughput, _empty] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(side.height.saturating_sub(1).min(15)),
            Constraint::Length(if spark.is_some() { 4 } else { 0 }),
            Constraint::Min(0),
        ])
        .areas(side);
        frame.render_widget(
            Paragraph::new(diagnostic_lines(d, true)).block(section(" Diagnostics ")),
            diagnostics,
        );
        if let Some(stage) = spark {
            let samples = stage.rate.history.iter().copied().collect::<Vec<_>>();
            frame.render_widget(
                Sparkline::default()
                    .data(&samples)
                    .style(color(CYAN))
                    .block(section(" Phase throughput · rows/s ")),
                throughput,
            );
        }
    }
}

fn partitions(frame: &mut Frame, d: &mut Dashboard, area: Rect, now: Instant) {
    let pane = section(" Partitions ");
    if d.partitions.is_empty() || area.height < 4 {
        let message = d
            .selected
            .and_then(|id| {
                d.partitions.get(&id).map(|partition| {
                    format!(
                        "Partition {id} · {} read · {} written",
                        compact(partition.metrics.events_read),
                        compact(partition.metrics.events_written)
                    )
                })
            })
            .unwrap_or_else(|| "Partition activity appears during conversion".to_owned());
        frame.render_widget(Paragraph::new(message).block(pane), area);
        return;
    }
    let capacity = usize::from(area.height - 3);
    d.page_height = capacity;
    let index = d
        .partitions
        .keys()
        .position(|key| Some(*key) == d.selected)
        .unwrap_or(0);
    d.table_offset = d.table_offset.min(index);
    if index >= d.table_offset + capacity {
        d.table_offset = index + 1 - capacity;
    }
    d.table_offset = d
        .table_offset
        .min(d.partitions.len().saturating_sub(capacity));
    let numeric = |value: String| Cell::from(Line::raw(value).alignment(Alignment::Right));
    let rows = d
        .partitions
        .iter()
        .skip(d.table_offset)
        .take(capacity)
        .map(|(&id, partition)| {
            let (state, tone) = match partition.state {
                PartitionState::Queued => ("Queued", FG),
                PartitionState::Running => ("Running", CYAN),
                PartitionState::Complete => ("Done", GREEN),
                PartitionState::Stopped => ("Stopped", YELLOW),
                PartitionState::Failed(_) => ("Failed", RED),
            };
            let rate = if partition.state == PartitionState::Running && d.outcome.is_none() {
                partition
                    .rate
                    .current
                    .map_or("—".into(), |rate| compact(rate as u64))
            } else {
                "—".into()
            };
            let selected = Some(id) == d.selected;
            let width = if area.width >= 78 { 9 } else { 7 };
            let written = if area.width >= 78 {
                vec![
                    numeric(compact(partition.metrics.events_written)),
                    numeric(partition.expected.map_or("—".into(), compact)),
                ]
            } else {
                vec![numeric(format!(
                    "{}/{}",
                    compact(partition.metrics.events_written),
                    partition.expected.map_or("—".into(), compact)
                ))]
            };
            let cells = [
                Cell::from(format!("{} {id}", if selected { "›" } else { " " })),
                Cell::from(Span::styled(state, color(tone))),
                numeric(compact(partition.metrics.events_read)),
            ]
            .into_iter()
            .chain(written)
            .chain([
                numeric(rate),
                numeric(partition.elapsed(now).map_or("—".into(), |elapsed| {
                    if width < 9 && elapsed.as_secs() < 3600 {
                        format!(
                            "{:02}:{:02}",
                            elapsed.as_secs() / 60,
                            elapsed.as_secs() % 60
                        )
                    } else {
                        format_duration(elapsed)
                    }
                })),
            ]);
            Row::new(cells).style(if selected {
                color(FG).bg(SELECTED)
            } else {
                color(FG)
            })
        });
    let mut widths = vec![
        Constraint::Length(5),
        Constraint::Length(8),
        Constraint::Min(7),
    ];
    let mut header = vec![
        Cell::from("ID"),
        Cell::from("State"),
        numeric("Read".into()),
    ];
    if area.width >= 78 {
        widths.extend([Constraint::Min(10), Constraint::Min(10)]);
        header.extend([numeric("Written".into()), numeric("Total".into())]);
    } else {
        widths.push(Constraint::Length(14));
        header.push(numeric("Written/total".into()));
    }
    widths.extend([Constraint::Min(7), Constraint::Min(9)]);
    header.extend([numeric("Rows/s".into()), numeric("Elapsed".into())]);
    frame.render_widget(
        Table::new(rows, widths)
            .column_spacing(1)
            .header(Row::new(header).style(emphasis(BLUE)))
            .block(pane),
        area,
    );
}

fn diagnostic_lines(d: &Dashboard, compact_counts: bool) -> Vec<Line<'static>> {
    let t = &d.total;
    let display = |n| {
        if compact_counts {
            compact(n)
        } else {
            n.to_string()
        }
    };
    let updates = d.restored_updates.map_or_else(
        || {
            format!(
                "Updates  {} internal / {} external",
                display(t.internal_reference_updates),
                display(t.external_reference_updates)
            )
        },
        |updates| format!("Reference updates  {}", display(updates)),
    );
    let mut lines = vec![
        Line::styled("References", emphasis(BLUE)),
        Line::raw(updates),
        Line::raw(format!(
            "Checkpoints  {} / {}",
            d.checkpoints.0, d.checkpoints.1
        )),
        Line::raw(format!(
            "Valued levels  {} / {}",
            display(t.depth_levels_converted),
            display(t.depth_levels_read)
        )),
        Line::raw(""),
        Line::styled("Unavailable reasons · event rows", emphasis(BLUE)),
    ];
    if d.restored_updates.is_some() {
        lines.push(Line::raw("Reason breakdown unavailable in receipt"));
    } else {
        for (reason, count) in UNAVAILABLE_REASONS.into_iter().zip(t.failures) {
            lines.push(Line::styled(
                format!("{reason:<20} {:>12}", display(count)),
                if count > 0 { color(YELLOW) } else { color(FG) },
            ));
        }
    }
    lines
}

fn details(frame: &mut Frame, d: &mut Dashboard, area: Rect) {
    let pane = section(" Job details ");
    let inner = pane.inner(area);
    let mut info = Vec::new();
    if let Some(message) = &d.error {
        info.push(format!("Execution error  {message}"));
    }
    if let Some(id) = d.selected
        && let Some(partition) = d.partitions.get(&id)
    {
        info.push(format!(
            "Partition {id} · {} read · {} written",
            partition.metrics.events_read, partition.metrics.events_written
        ));
        if let PartitionState::Failed(message) = &partition.state {
            info.push(format!("Partition error  {message}"));
        }
    }
    let t = &d.total;
    info.extend([
        format!(
            "Identity {} · converted {} · unavailable {} · skipped {}",
            t.identity_events, t.converted_events, t.unavailable_events, t.skipped_events
        ),
        format!(
            "{} total physical rows · {} market rows · {} reconstruction rows",
            d.context.expected_rows,
            d.context
                .expected_rows
                .saturating_sub(d.context.reconstruction_rows),
            d.context.reconstruction_rows
        ),
        format!(
            "{} trades · {} depth events · {} finalized files · {} row groups",
            t.trade_events, t.depth_events, t.files_written, t.row_groups_written
        ),
        format!(
            "{} unavailable levels · {} skipped levels",
            t.depth_levels_unavailable, t.depth_levels_skipped
        ),
    ]);
    info.extend(d.context.details.iter().cloned());
    info.extend(diagnostic_lines(d, false).iter().map(ToString::to_string));
    for (phase, elapsed) in &d.completed {
        info.push(format!(
            "{} completed in {}",
            phase.label(),
            format_duration(*elapsed)
        ));
    }
    let mut lines = Vec::new();
    for text in &info {
        for line in text.split('\n') {
            wrap(line, usize::from(inner.width.max(1)), &mut lines);
        }
    }
    d.details_max_scroll = lines.len().saturating_sub(usize::from(inner.height));
    d.details_scroll = d.details_scroll.min(d.details_max_scroll);
    d.page_height = usize::from(inner.height);
    let visible = lines
        .into_iter()
        .skip(d.details_scroll)
        .take(inner.height.into())
        .map(Line::raw)
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(visible).block(pane), area);
}

fn wrap<'a>(text: &'a str, width: usize, lines: &mut Vec<&'a str>) {
    let mut start = 0;
    let mut columns = 0;
    for (index, ch) in text.char_indices() {
        let len = Span::raw(&text[index..index + ch.len_utf8()]).width();
        if columns + len > width && index > start {
            lines.push(&text[start..index]);
            start = index;
            columns = 0;
        }
        columns += len;
    }
    lines.push(&text[start..]);
}
