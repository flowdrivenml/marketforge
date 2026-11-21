use super::{
    ConversionDashboardContext,
    rate::Rate,
    state::{Dashboard, PartitionState, View},
    view,
};
use crate::historical::{
    convert::progress::{
        ConversionOutcome, ConversionPartitionKey, ConversionPhase, ConversionProgress,
        ConversionProgressEvent as Event, ConversionProgressState,
    },
    merge::UnifiedMergeMode,
};
use ratatui::{Terminal, backend::TestBackend};
use std::time::{Duration, Instant};

fn key(partition_id: u32) -> ConversionPartitionKey {
    ConversionPartitionKey { partition_id }
}

fn context() -> ConversionDashboardContext {
    ConversionDashboardContext {
        operation: "Run",
        mode: UnifiedMergeMode::Combined,
        target_quote: "USDT".into(),
        instruments: 4,
        streams: 8,
        expected_rows: 12_480_000,
        reconstruction_rows: 1200,
        trade_events: 5_240_000,
        depth_events: 7_240_000,
        requested_workers: 2,
        details: vec![
            "Job 265083950276773788 · Source publication 2079541492669036539".into(),
            format!("Input /{}market-data.parquet", "nested/".repeat(32)),
        ],
    }
}

fn start(d: &mut Dashboard, phase: ConversionPhase, now: Instant) {
    d.receive(Event::PhaseStarted { phase }, now);
}
fn complete(d: &mut Dashboard, phase: ConversionPhase, now: Instant) {
    d.receive(Event::PhaseCompleted { phase }, now);
}
fn at(now: Instant, seconds: u64) -> Instant {
    now + Duration::from_secs(seconds)
}

#[test]
fn rate_is_windowed_and_resets_on_counter_discontinuities() {
    let now = Instant::now();
    let mut rate = Rate::default();
    rate.sample(now, 0);
    rate.sample(now, 100);
    assert_eq!(rate.eta(125), None);
    rate.sample(at(now, 1), 100);
    rate.sample(at(now, 2), 200);
    assert_eq!(rate.current, Some(100.0));
    assert_eq!(rate.eta(125), Some(Duration::from_secs(2)));
    assert_eq!(rate.eta(0), None);
    for time in 3..=8 {
        rate.sample(at(now, time), 200);
    }
    assert_eq!(rate.current, Some(0.0));
    assert_eq!(rate.eta(1), None);
    rate.sample(at(now, 9), 0);
    assert!(rate.current.is_none() && rate.history.is_empty());
}

#[test]
fn stage_and_partition_lifecycles_are_independent_and_idempotent() {
    use ConversionPhase::*;
    let now = Instant::now();
    let mut d = Dashboard::new(context(), now);
    start(&mut d, ReferencePrepass, now);
    d.receive(
        Event::ReferencePrepassProgress {
            events_scanned: 12_480_000,
            total_events: 12_480_000,
            checkpoints_created: 4,
            total_checkpoints: 4,
        },
        at(now, 5),
    );
    d.sample(&ConversionProgressState::default(), at(now, 5));
    complete(&mut d, ReferencePrepass, at(now, 5));
    assert!(d.active.is_none());
    start(&mut d, PhysicalConversion, at(now, 6));
    d.receive(
        Event::PartitionsPlanned {
            partitions: vec![(key(4), 10), (key(2), 10)],
            workers: 2,
        },
        at(now, 6),
    );
    assert_eq!(d.selected, Some(2));
    d.navigate(true, false);
    assert_eq!(d.selected, Some(4));
    d.receive(Event::PartitionStarted { key: key(4) }, at(now, 7));
    d.receive(Event::PartitionStarted { key: key(2) }, at(now, 8));
    start(&mut d, ReferencePrepass, at(now, 8));
    start(&mut d, PhysicalConversion, at(now, 8));
    assert_eq!(d.active.as_ref().unwrap().started, at(now, 6));
    assert_eq!(d.active.as_ref().unwrap().rate.current, None);
    let metrics = ConversionProgress {
        events_written: 10,
        identity_events: 10,
        ..Default::default()
    };
    for _ in 0..2 {
        d.receive(
            Event::PartitionCompleted {
                key: key(4),
                metrics,
            },
            at(now, 9),
        );
        d.receive(
            Event::PartitionFailed {
                key: key(4),
                message: "stale".into(),
            },
            at(now, 9),
        );
    }
    d.sample(&ConversionProgressState::default(), at(now, 10));
    assert_eq!(d.partitions[&4].state, PartitionState::Complete);
    assert_eq!(d.partitions[&4].metrics.events_written, 10);
    assert_eq!(
        d.partitions[&4].elapsed(at(now, 30)),
        Some(Duration::from_secs(2))
    );
    assert_eq!(d.selected, Some(4));
    d.receive(
        Event::MaterializationTotals(ConversionProgress {
            events_written: 12_480_000,
            identity_events: 12_480_000,
            ..Default::default()
        }),
        at(now, 10),
    );
    complete(&mut d, PhysicalConversion, at(now, 10));
    start(&mut d, PhysicalValidation, at(now, 11));
    assert!(d.outcome.is_none());
    assert_eq!(d.active.as_ref().unwrap().phase, PhysicalValidation);
    assert_eq!(d.completed[0], (ReferencePrepass, Duration::from_secs(5)));
    d.receive(
        Event::Finished(ConversionOutcome::Failed("bad footer".into())),
        at(now, 12),
    );
    d.receive(Event::Finished(ConversionOutcome::Published), at(now, 13));
    assert_eq!(d.status(), "FAILED");
    assert_eq!(d.error.as_deref(), Some("bad footer"));
}

fn running(now: Instant) -> Dashboard {
    let mut d = Dashboard::new(context(), now);
    let progress = ConversionProgressState::default();
    start(&mut d, ConversionPhase::ReferencePrepass, now);
    d.receive(
        Event::ReferencePrepassProgress {
            events_scanned: 12_480_000,
            total_events: 12_480_000,
            checkpoints_created: 4,
            total_checkpoints: 4,
        },
        at(now, 8),
    );
    complete(&mut d, ConversionPhase::ReferencePrepass, at(now, 8));
    start(&mut d, ConversionPhase::PhysicalConversion, at(now, 9));
    d.receive(
        Event::PartitionsPlanned {
            partitions: (0..4).map(|id| (key(id), 3_120_000)).collect(),
            workers: 2,
        },
        at(now, 9),
    );
    let active = progress.register(key(0));
    d.receive(Event::PartitionStarted { key: key(0) }, at(now, 9));
    for (time, written) in [
        (9, 0),
        (10, 420_000),
        (11, 885_000),
        (12, 1_330_000),
        (13, 1_840_000),
    ] {
        active.update(&ConversionProgress {
            events_read: written + 1400,
            events_written: written,
            identity_events: written / 2,
            converted_events: written / 3,
            unavailable_events: written / 6,
            failures: [written / 13, written / 12, 13, 7, 3, 0],
            trade_events: written / 3,
            depth_events: written - written / 3,
            depth_levels_read: written * 3,
            depth_levels_converted: written * 2,
            row_groups_written: written / 100_000,
            internal_reference_updates: written / 5,
            ..Default::default()
        });
        d.sample(&progress, at(now, time));
    }
    d
}

fn render(d: &mut Dashboard, size: (u16, u16), now: Instant, capture: &str) -> Vec<String> {
    let mut terminal = Terminal::new(TestBackend::new(size.0, size.1)).unwrap();
    terminal.draw(|frame| view::render(frame, d, now)).unwrap();
    let buffer = terminal.backend().buffer();
    let lines = buffer
        .content
        .chunks(usize::from(size.0))
        .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
        .collect::<Vec<_>>();
    if let Some(dir) =
        std::env::var_os("MARKETFORGE_CONVERSION_CAPTURE_DIR").filter(|_| !capture.is_empty())
    {
        let cells = buffer
            .content
            .chunks(usize::from(size.0))
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        (
                            cell.symbol(),
                            format!("{:?}", cell.fg),
                            format!("{:?}", cell.bg),
                            cell.modifier.contains(ratatui::style::Modifier::BOLD),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        std::fs::write(
            std::path::Path::new(&dir).join(format!("{capture}.json")),
            serde_json::to_vec(&cells).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(lines.len(), usize::from(size.1));
    lines
}

#[test]
fn overview_layouts_keep_progress_diagnostics_and_controls_visible() {
    let now = Instant::now();
    let mut d = running(now);
    for (size, capture) in [
        ((160, 45), "wide-running"),
        ((120, 30), ""),
        ((80, 24), "compact-running"),
        ((65, 16), "reduced-running"),
    ] {
        let lines = render(&mut d, size, at(now, 13), capture);
        assert!(lines[0].contains("RUNNING"));
        assert!(lines[1].contains("Combined → USDT"));
        assert!(lines[3].contains("PHYSICAL CONVERSION"));
        assert!(lines[4].contains("15%") && lines[4].contains("12.48M physical rows written"));
        assert!(lines[5].contains("Conversion ETA"));
        assert!(lines[6].contains("Completed  REFERENCE PREPASS"));
        assert!(lines.iter().any(|line| line.contains("Running")
            && line.contains("1.84M")
            && line.contains("3.12M")));
        assert!(lines.last().unwrap().contains("Ctrl+C interrupt"));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("Unavailable") && line.contains("306.7K"))
        );
        if size.0 == 160 {
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains("Partitions") && line.contains("Diagnostics"))
            );
            assert!(lines.iter().any(|line| line.contains("Phase throughput")));
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains("Stale reference") && line.contains("153.3K"))
            );
        }
    }
    for size in [(24, 8), (9, 3), (1, 1)] {
        let lines = render(&mut d, size, now, "");
        assert!(!lines[0].trim().is_empty());
    }
}

#[test]
fn layout_handles_prepass_validation_publication_failure_and_details() {
    use ConversionPhase::*;
    let now = Instant::now();
    let mut d = Dashboard::new(context(), now);
    start(&mut d, ReferencePrepass, now);
    d.receive(
        Event::ReferencePrepassProgress {
            events_scanned: 1_000_000,
            total_events: 12_480_000,
            checkpoints_created: 1,
            total_checkpoints: 4,
        },
        at(now, 1),
    );
    let lines = render(&mut d, (120, 30), at(now, 1), "");
    assert!(lines[4].contains("source rows scanned"));
    assert!(lines.iter().any(|line| line.contains("Checkpoints 1/4")));
    complete(&mut d, ReferencePrepass, at(now, 2));
    start(&mut d, PhysicalValidation, at(now, 2));
    d.receive(
        Event::ValidationStarted {
            total_files: 2,
            expected_events: 12_480_000,
        },
        at(now, 2),
    );
    d.receive(
        Event::ValidationProgress {
            files_completed: 1,
            events_validated: 6_240_000,
            row_groups_validated: 3,
        },
        at(now, 3),
    );
    assert!(render(&mut d, (80, 24), at(now, 3), "")[4].contains("50%"));
    complete(&mut d, PhysicalValidation, at(now, 4));
    start(&mut d, Commit, at(now, 4));
    let lines = render(&mut d, (65, 16), at(now, 4), "");
    assert!(lines[3].contains("TRANSACTIONAL COMMIT") && !lines[4].contains('%'));
    d.interrupt();
    assert!(render(&mut d, (65, 16), at(now, 4), "")[0].contains("INTERRUPTING"));
    d.receive(Event::Finished(ConversionOutcome::Interrupted), at(now, 5));
    assert!(render(&mut d, (65, 16), at(now, 5), "")[0].contains("INTERRUPTED"));

    let mut failure = running(now);
    failure.receive(
        Event::PartitionsPlanned {
            partitions: (4..100).map(|id| (key(id), 40)).collect(),
            workers: 2,
        },
        at(now, 13),
    );
    failure.navigate(true, true);
    let selected = failure.selected;
    render(&mut failure, (65, 16), at(now, 13), "");
    assert_eq!(failure.selected, selected);
    assert_ne!(failure.table_offset, 0);
    failure.receive(
        Event::PartitionFailed {
            key: key(0),
            message: "Output writer: invalid footer in the temporary partition".into(),
        },
        at(now, 14),
    );
    failure.receive(
        Event::Finished(ConversionOutcome::Failed(
            "Output writer: invalid footer in the temporary partition".into(),
        )),
        at(now, 14),
    );
    failure.selected = Some(0);
    failure.view = View::Details;
    let lines = render(&mut failure, (100, 29), at(now, 14), "failure-details");
    assert!(lines[0].contains("FAILED"));
    assert!(lines.iter().any(|line| line.contains("Job details")));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("Execution error") && line.contains("invalid footer"))
    );
    failure.navigate(true, true);
    let scrolled = failure.details_scroll;
    render(&mut failure, (65, 16), at(now, 14), "");
    assert_eq!(failure.details_scroll, scrolled);
}

#[test]
fn successful_terminal_states_keep_distinct_labels_and_final_accounting() {
    let now = Instant::now();
    for outcome in [
        ConversionOutcome::Validated,
        ConversionOutcome::Materialized,
        ConversionOutcome::Published,
    ] {
        let mut d = running(now);
        for id in 0..4 {
            d.receive(Event::PartitionStarted { key: key(id) }, at(now, 14));
            d.receive(
                Event::PartitionCompleted {
                    key: key(id),
                    metrics: ConversionProgress {
                        events_read: 3_120_000,
                        events_written: 3_120_000,
                        ..Default::default()
                    },
                },
                at(now, 19),
            );
        }
        d.receive(
            Event::MaterializationTotals(ConversionProgress {
                events_read: 12_480_000,
                events_written: 12_480_000,
                identity_events: 7_200_000,
                converted_events: 4_940_000,
                unavailable_events: 320_000,
                skipped_events: 20_000,
                failures: [200_000, 100_000, 20_000, 0, 0, 0],
                files_written: 4,
                row_groups_written: 44,
                trade_events: 5_240_000,
                depth_events: 7_240_000,
                depth_levels_read: 840_000_000,
                depth_levels_converted: 831_000_000,
                internal_reference_updates: 120_000,
                ..Default::default()
            }),
            at(now, 20),
        );
        complete(&mut d, ConversionPhase::PhysicalConversion, at(now, 20));
        if outcome == ConversionOutcome::Published {
            start(&mut d, ConversionPhase::Commit, at(now, 22));
            complete(&mut d, ConversionPhase::Commit, at(now, 24));
        }
        let label = outcome.label();
        let capture = if outcome == ConversionOutcome::Published {
            "published"
        } else {
            ""
        };
        d.receive(Event::Finished(outcome), at(now, 24));
        assert!(d.counter_warning().is_none());
        let lines = render(&mut d, (120, 30), at(now, 40), capture);
        assert!(lines[0].contains(label) && lines.last().unwrap().contains("Q close"));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("4 finalized files") && line.contains("44 row groups"))
        );
    }
}
