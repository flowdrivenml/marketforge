//! Responsive Ratatui presentation. No filesystem or processing work occurs here.
use super::*;
pub(super) fn render(frame: &mut ratatui::Frame, d: &mut ProcessingDashboard) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(BG).fg(FG)), area);
    if area.width < 40 || area.height < 10 {
        frame.render_widget(
            Paragraph::new(format!(
                "MARKETFORGE\n{} active · {} failed\nRead {}\nResize to at least 40 × 10",
                d.active_tasks(),
                d.failed_tasks(),
                compact(d.total_records_read())
            )),
            area,
        );
        return;
    }
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(if area.height >= 28 {
                7
            } else if area.height >= 22 {
                4
            } else {
                0
            }),
            Constraint::Length(1),
        ])
        .split(area);
    let (status, color) = if d.finished {
        if d.success {
            ("COMPLETE", GREEN)
        } else {
            ("FAILED", RED)
        }
    } else if d.finalizing {
        ("FINALIZING", YELLOW)
    } else {
        ("RUNNING", CYAN)
    };
    let head = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(0), Constraint::Length(26)])
        .split(sections[0]);
    frame.render_widget(
        Paragraph::new(format!(" MARKETFORGE\n {}", d.overview_phase))
            .style(Style::default().fg(BLUE).add_modifier(Modifier::BOLD)),
        head[0],
    );
    frame.render_widget(
        Paragraph::new(format!("NORMALIZATION  {status}")).style(Style::default().fg(color)),
        head[1],
    );
    let cards = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(25); 4])
        .split(sections[1]);
    for (i, (label, value)) in [
        ("Elapsed", format_duration(d.elapsed())),
        (
            "Jobs committed / failed",
            format!(
                "{} of {} · {} failed",
                d.completed_jobs(),
                d.total_jobs,
                d.jobs.values().filter(|s| !**s).count()
            ),
        ),
        (
            "Sources settled",
            format!("{} / {}", d.settled_tasks(), d.total_tasks),
        ),
        (
            "Normalize rec/s",
            if !d.rate_ready {
                "—".into()
            } else {
                compact(d.current_speed as u64)
            },
        ),
    ]
    .into_iter()
    .enumerate()
    {
        frame.render_widget(Paragraph::new(value).block(panel(label)), cards[i]);
    }
    let settled = d.settled_tasks();
    let ratio = if d.total_tasks == 0 {
        0.0
    } else {
        settled as f64 / d.total_tasks as f64
    };
    frame.render_widget(
        Gauge::default()
            .ratio(ratio.clamp(0.0, 1.0))
            .gauge_style(
                Style::default()
                    .fg(if d.failed_tasks() > 0 { YELLOW } else { CYAN })
                    .bg(BG),
            )
            .label(format!(
                "Source lifecycle {:.1}% · {} active · {} failed · {} skipped",
                ratio * 100.0,
                d.active_tasks(),
                d.failed_tasks(),
                d.skipped_tasks()
            )),
        sections[2],
    );
    if d.detail {
        render_detail(frame, sections[3], d);
    } else {
        render_tasks(frame, sections[3], d);
    }
    if sections[4].height > 0 {
        let lower = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(46), Constraint::Percentage(54)])
            .split(sections[4]);
        let block = panel(" Execution summary ");
        let inner = block.inner(lower[0]);
        frame.render_widget(block, lower[0]);
        frame.render_widget(
            Paragraph::new(format!(
                "Read {}  Staged events {}\nRejected {}  Failed {}\nAvg proc {}/s · Routed {}",
                compact(d.total_records_read()),
                compact(d.total_records_written()),
                compact(d.total_records_rejected()),
                d.failed_tasks(),
                if !d.rate_ready {
                    "—".into()
                } else {
                    compact((d.total_records_processed() as f64 / d.elapsed().as_secs_f64()) as u64)
                },
                compact(d.total_routing_records())
            )),
            inner,
        );
        if inner.height >= 5 {
            let data: Vec<_> = d.throughput_history.iter().copied().collect();
            frame.render_widget(
                Paragraph::new(format!(
                    "Recent processed/s {}",
                    d.rolling_speed
                        .map(|s| compact(s as u64))
                        .unwrap_or_else(|| "—".into())
                ))
                .style(Style::default().fg(MUTED)),
                Rect::new(inner.x, inner.y + 3, inner.width, 1),
            );
            frame.render_widget(
                Sparkline::default()
                    .data(&data)
                    .style(Style::default().fg(CYAN)),
                Rect::new(inner.x, inner.y + 4, inner.width, inner.height - 4),
            );
        }
        let block = panel(" Recent activity ");
        let inner = block.inner(lower[1]);
        frame.render_widget(block, lower[1]);
        frame.render_widget(
            Paragraph::new(
                d.activity
                    .iter()
                    .rev()
                    .take(inner.height as usize)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            inner,
        );
    }
    frame.render_widget(
        Paragraph::new(if area.width < 75 {
            " Tab Details · ↑↓ Select · ? Help "
        } else {
            " Tab/Enter Details  ↑↓ Select  PgUp/PgDn Scroll  Home/End  a Active  ? Help "
        })
        .style(Style::default().fg(MUTED)),
        sections[5],
    );
    if d.help {
        let overlay = Rect::new(
            area.x + area.width / 10,
            area.y + area.height / 4,
            area.width * 8 / 10,
            area.height / 2,
        );
        frame.render_widget(Clear, overlay);
        frame.render_widget(Paragraph::new("Tab / Enter  Switch table and selected source details\n↑ / ↓        Select a source (stable job/task ordering)\nPageUp/Down  Move one page · Home/End First/last\n?            Toggle help · Esc Close help/details\n\nSources are not committed jobs. Finalization validates\nand publishes outputs after sources finish.\na            Jump to next active source\nNo input-record totals: per-source percent is unknown.\nRates use processed-record deltas, ≥1s windows.\nExecution exits after processing completes.\nRouting rows are separate from worker read counters.\nDiagnostics are saved to the run's log file.").block(panel(" Dashboard help ")),overlay);
    }
}
fn render_tasks(frame: &mut ratatui::Frame, area: Rect, d: &mut ProcessingDashboard) {
    let wide = area.width >= 100;
    let block = panel(" Sources · ordered by job / task · Enter for details ");
    let inner = block.inner(area);
    let visible = inner.height.saturating_sub(1) as usize;
    // Format only the visible window; stable BTreeMap ordering preserves selection.
    let offset = if d.selected < d.table.offset() {
        d.selected
    } else if d.selected >= d.table.offset() + visible.max(1) {
        d.selected.saturating_sub(visible.saturating_sub(1))
    } else {
        d.table.offset()
    };
    *d.table.offset_mut() = offset;
    let rows = d.tasks.iter().skip(offset).take(visible).map(|(id, t)| {
        let mut cells = vec![
            id.task_id.to_string(),
            t.exchange.clone(),
            t.instrument.clone(),
            t.status.label().into(),
            compact(t.records_read),
        ];
        if wide {
            cells.insert(3, t.content_type.clone());
            cells.push(if t.status == TaskStatus::Running && t.rate_ready {
                format_speed(t.speed)
            } else {
                "—".into()
            });
        }
        Row::new(cells).style(Style::default().fg(match t.status {
            TaskStatus::Failed => RED,
            TaskStatus::Completed => GREEN,
            TaskStatus::Running => CYAN,
            _ => MUTED,
        }))
    });
    let (headers, widths) = if wide {
        (
            vec![
                "Task",
                "Exchange",
                "Instrument",
                "Type",
                "State",
                "Read",
                "Proc rec/s",
            ],
            vec![
                Constraint::Length(13),
                Constraint::Length(10),
                Constraint::Min(12),
                Constraint::Length(14),
                Constraint::Length(8),
                Constraint::Length(10),
                Constraint::Length(11),
            ],
        )
    } else {
        (
            vec!["Task", "Exchange", "Instrument", "State", "Read"],
            vec![
                Constraint::Length(10),
                Constraint::Length(9),
                Constraint::Min(6),
                Constraint::Length(7),
                Constraint::Length(8),
            ],
        )
    };
    let table = Table::new(rows, widths)
        .column_spacing(1)
        .header(Row::new(headers).style(Style::default().fg(BLUE)))
        .row_highlight_style(Style::default().bg(MUTED).fg(FG))
        .highlight_symbol("› ");
    // Window-local selection keeps allocation and formatting bounded by the viewport.
    let mut state = TableState::default()
        .with_selected((!d.tasks.is_empty()).then_some(d.selected.saturating_sub(offset)));
    frame.render_widget(block, area);
    frame.render_stateful_widget(table, inner, &mut state);
}
fn render_detail(frame: &mut ratatui::Frame, area: Rect, d: &ProcessingDashboard) {
    let Some((id, t)) = d.tasks.iter().nth(d.selected) else {
        frame.render_widget(
            Paragraph::new("No sources").block(panel(" Source details ")),
            area,
        );
        return;
    };
    let details = format!(
        "Job {} · Task {} · {}\n\
         {} / {} / {}\n\
         Stream: {}\n\
         Input: {}\n\
         Read: {}   Processed: {}\n\
         Events written (staged): {}   Rejected: {}\n\
         Routing rows: {}\n\
         Stage: {}\n\
         Source progress: unknown (no input total)\n\
         {}\n\
         Planned destination (publication requires manifest): {}\n\
         Log: {}",
        id.job_id,
        id.task_id,
        t.status.label(),
        t.exchange,
        t.instrument,
        t.content_type,
        t.stream,
        t.source,
        t.records_read,
        t.records_processed,
        t.records_written,
        t.records_rejected,
        t.routing_records,
        d.selected_phase,
        t.message,
        d.outputs.get(&id.job_id).map(String::as_str).unwrap_or("—"),
        d.log_path.as_deref().unwrap_or("—"),
    );
    frame.render_widget(
        Paragraph::new(details)
            .wrap(ratatui::widgets::Wrap { trim: false })
            .scroll((d.detail_scroll, 0))
            .block(panel(" Source details · Esc to return ")),
        area,
    );
}
