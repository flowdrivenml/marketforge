use super::*;
static DIAGNOSTIC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
use ratatui::backend::TestBackend;
fn dashboard(count: usize) -> ProcessingDashboard {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let config = crate::historical::normalize::load_processing_config(
        root.join("data/.config/historical/general.toml"),
        root,
    )
    .unwrap();
    let catalog =
        crate::historical::config::FormatCatalog::load(root.join("data/.config/historical"))
            .unwrap();
    let mut job = crate::historical::config::load_normalization_job(
        root.join("docs/historical/normalize-job-v3.example.json"),
        &catalog,
        &config,
        root,
    )
    .unwrap();
    let template = job.tasks[0].clone();
    job.tasks = (0..count)
        .map(|i| {
            let mut t = template.clone();
            t.task_id.0 = i as u64;
            t.symbol = "VERY-LONG-INSTRUMENT-測試-PERPETUAL".into();
            t.input_path = std::path::PathBuf::from(format!(
                "/{}archive.csv.gz",
                "long-directory/".repeat(30)
            ));
            t
        })
        .collect();
    ProcessingDashboard::new(&job, &config)
}
fn id(n: u64) -> ProgressTaskId {
    ProgressTaskId::new(1001, n)
}
fn complete(n: u64, read: u64) -> ProgressEvent {
    ProgressEvent::TaskCompleted {
        id: id(n),
        records_read: read,
        records_written: read.saturating_sub(1),
        records_rejected: 1,
    }
}
fn start(n: u64) -> ProgressEvent {
    ProgressEvent::TaskStarted {
        id: id(n),
        exchange: "Bybit".into(),
        instrument: "BTCUSDT".into(),
        content_type: "Trades".into(),
        source: "/very/long/path.csv.gz".into(),
    }
}
#[test]
fn dashboard_resizes_and_clips_every_lifecycle() {
    for count in [0, 1, 1000] {
        let mut d = dashboard(count);
        if count > 0 {
            d.handle_event(start(0));
            d.tasks.get_mut(&id(0)).unwrap().records_read = u64::MAX;
        }
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        for (w, h) in [
            (1, 1),
            (39, 9),
            (40, 10),
            (60, 16),
            (80, 24),
            (100, 30),
            (120, 40),
            (240, 80),
            (80, 24),
        ] {
            terminal.backend_mut().resize(w, h);
            terminal.resize(Rect::new(0, 0, w, h)).unwrap();
            for detail in [false, true] {
                d.detail = detail;
                d.help = false;
                terminal.draw(|f| render(f, &mut d)).unwrap();
                d.help = true;
                terminal.draw(|f| render(f, &mut d)).unwrap();
            }
        }
        d.help = false;
        d.detail = false;
        if count > 0 {
            d.handle_event(ProgressEvent::TaskFailed {
                id: id(0),
                message: "controlled failure".into(),
            });
        }
        d.handle_event(ProgressEvent::Finished { success: false });
        terminal.draw(|f| render(f, &mut d)).unwrap();
        let buffer = terminal.backend().buffer();
        let text = buffer
            .content
            .iter()
            .map(|c| c.symbol())
            .collect::<String>();
        assert!(text.contains("FAILED"));
        assert!(!text.contains("/very/long/path"));
    }
}
#[test]
fn dashboard_lifecycle_is_idempotent_and_jobs_require_commit() {
    let mut d = dashboard(2);
    d.handle_event(start(0));
    d.handle_event(complete(0, 100));
    d.handle_event(start(0));
    d.handle_event(complete(0, 100));
    d.handle_event(ProgressEvent::TaskSkipped {
        id: id(1),
        reason: "duplicate".into(),
    });
    assert_eq!(d.completed_tasks(), 1);
    assert_eq!(d.skipped_tasks(), 1);
    assert_eq!(d.total_records_read(), 100);
    assert_eq!(d.completed_jobs(), 0);
    d.handle_event(ProgressEvent::Finalizing);
    assert_eq!(d.completed_jobs(), 0);
    d.handle_event(ProgressEvent::JobFinished {
        job_id: 1001,
        success: true,
    });
    assert_eq!(d.completed_jobs(), 1);
    d.handle_event(ProgressEvent::JobFinished {
        job_id: 1001,
        success: false,
    });
    d.handle_event(ProgressEvent::JobFinished {
        job_id: 9999,
        success: true,
    });
    assert_eq!(d.completed_jobs(), 1);
    let reporter = ProgressReporter::disabled();
    let counters = reporter.state().register_task(id(0));
    counters
        .records_read
        .store(10, std::sync::atomic::Ordering::Relaxed);
    d.update_snapshots(&reporter.state());
    assert_eq!(d.total_records_read(), 100);
    d.handle_event(ProgressEvent::Finished { success: true });
    let elapsed = d.elapsed();
    assert_eq!(d.elapsed(), elapsed);
}
#[test]
fn dashboard_rates_use_windowed_deltas_including_completed_sources() {
    let mut d = dashboard(2);
    d.sample = (Instant::now() - Duration::from_secs(2), 0);
    d.rate_window = VecDeque::from([(Instant::now() - Duration::from_secs(4), 0)]);
    let state = ProgressState::default();
    state
        .register_task(id(0))
        .update(&crate::historical::normalize::metrics::ProcessingCounters {
            records_read: 1000,
            records_processed: 900,
            ..Default::default()
        });
    d.handle_event(complete(0, 1000));
    d.update_snapshots(&state);
    assert!((400.0..=450.0).contains(&d.current_speed));
    assert!((200.0..=225.0).contains(&d.rolling_speed.unwrap()));
    d.sample = (Instant::now() - Duration::from_secs(2), 900);
    d.update_snapshots(&state);
    assert_eq!(d.current_speed, 0.0);
    d.tasks.get_mut(&id(1)).unwrap().records_read = u64::MAX;
    assert_eq!(d.total_records_read(), u64::MAX);
}
#[test]
fn dashboard_selection_survives_rapid_events() {
    let mut d = dashboard(1000);
    d.navigate(KeyCode::End, 10);
    for n in 0..1000 {
        d.handle_event(start(n));
        d.handle_event(complete(n, n));
    }
    assert_eq!(d.selected, 999);
    assert_eq!(d.activity.len(), 100);
    assert_eq!(d.total_records_read(), 499500);
    let mut t = Terminal::new(TestBackend::new(80, 24)).unwrap();
    t.draw(|f| render(f, &mut d)).unwrap();
    assert!(d.table.offset() > 0);
    d.navigate(KeyCode::Home, 10);
    t.draw(|f| render(f, &mut d)).unwrap();
    assert_eq!(d.table.offset(), 0);
}

#[test]
fn dashboard_keeps_failure_counters_and_marks_unattempted_work() {
    let mut d = dashboard(3);
    d.handle_event(start(0));
    d.handle_event(start(1));
    let state = ProgressState::default();
    let counter = state.register_task(id(0));
    counter
        .records_read
        .store(500, std::sync::atomic::Ordering::Relaxed);
    d.handle_event(ProgressEvent::TaskFailed {
        id: id(0),
        message: "sink failed".into(),
    });
    d.update_snapshots(&state);
    assert_eq!(d.total_records_read(), 500);
    d.handle_event(ProgressEvent::JobFinished {
        job_id: 1001,
        success: false,
    });
    d.handle_event(ProgressEvent::Finished { success: false });
    assert_eq!(d.tasks[&id(1)].status, TaskStatus::Stopped);
    assert_eq!(d.tasks[&id(2)].status, TaskStatus::NotRun);
    assert_eq!(d.active_tasks(), 0);
    assert_eq!(d.failed_tasks(), 1);
    assert_eq!(d.completed_jobs(), 0);
}
#[test]
fn dashboard_uses_internal_unit_phases_and_separate_routing_counts() {
    let mut d = dashboard(1);
    let reporter = ProgressReporter::disabled();
    // Public stream names and internal execution-unit identities are different.
    // A channel enables instrumentation without writing any actual output.
    let (tx, _rx) = mpsc::channel();
    let live = ProgressReporter::new(tx);
    live.phase(id(0), "internal-unit-hash", "routing");
    live.state()
        .register_task(id(0))
        .routing_records
        .store(900, std::sync::atomic::Ordering::Relaxed);
    d.update_snapshots(&live.state());
    assert!(d.selected_phase.contains("routing"));
    assert_eq!(d.tasks[&id(0)].routing_records, 900);
    assert_eq!(d.total_records_read(), 0);
    assert!(!d.rate_ready);
    live.stream_phase(1001, "other-unit", "unrelated");
    d.update_snapshots(&live.state());
    assert!(d.selected_phase.contains("routing"));
    live.stream_phase(1001, "internal-unit-hash", "reconciliation");
    d.update_snapshots(&live.state());
    assert!(d.selected_phase.contains("reconciliation"));
    live.stream_phase(1001, "", "publication");
    d.update_snapshots(&live.state());
    assert!(d.selected_phase.contains("publication"));
    assert!(reporter.state().dashboard_phase(id(0)).is_none());
}
#[test]
fn dashboard_header_table_footer_and_details_have_distinct_rows() {
    let mut d = dashboard(1);
    d.handle_event(start(0));
    let mut t = Terminal::new(TestBackend::new(80, 24)).unwrap();
    t.draw(|f| render(f, &mut d)).unwrap();
    let buffer = t.backend().buffer();
    let row = |y: u16| (0..80).map(|x| buffer[(x, y)].symbol()).collect::<String>();
    assert!(row(0).contains("MARKETFORGE"));
    assert!(row(0).contains("NORMALIZATION"));
    assert!(row(5).contains("Source lifecycle"));
    assert!(row(7).contains("Instrument"));
    assert!(row(8).contains("BTCUSDT"));
    assert!(row(8).contains("Running"));
    assert!(row(23).contains("Details"));
    d.detail = true;
    t.draw(|f| render(f, &mut d)).unwrap();
    let text = t
        .backend()
        .buffer()
        .content
        .iter()
        .map(|c| c.symbol())
        .collect::<String>();
    assert!(text.contains("/very/long/path.csv.gz"));
    assert!(text.contains("no input total"));
    assert_eq!(compact(u64::MAX), "18.45E");
}
#[test]
fn dashboard_diagnostics_go_to_a_dedicated_log() {
    let _diagnostics = DIAGNOSTIC_TEST_LOCK.lock().unwrap();
    use crate::historical::normalize::progress::log::{DashboardLog, diagnostic};
    let log = DashboardLog::enter().unwrap();
    assert!(DashboardLog::enter().is_err());
    diagnostic(format_args!("controlled diagnostic {}", 42));
    let path = log.path.clone();
    drop(log);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "controlled diagnostic 42\n"
    );
    std::fs::remove_file(path).unwrap();
}

#[test]
fn coalesced_telemetry_is_bounded_and_terminal_outcomes_survive_a_burst() {
    let reporter = ProgressReporter::coalesced();
    let mut d = dashboard(2);
    for _ in 0..100_000 {
        reporter.emit(start(0));
    }
    reporter.emit(complete(0, 42));
    reporter.emit(start(0)); // Delayed start cannot replace the terminal outcome.
    reporter.emit(ProgressEvent::JobOutcome {
        job_id: 1001,
        success: false,
        message: Some("stream reconciliation rejected ambiguous identity".into()),
        manifest_path: None,
        failure_report_path: Some("staging/failure.json".into()),
    });
    reporter.finished(false);
    let state = reporter.state();
    let mut frames = 0;
    while state.has_pending_events() {
        let events = state.take_events(1);
        assert_eq!(events.len(), 1);
        for event in events {
            d.handle_event(event);
        }
        d.navigate(KeyCode::Down, 1); // Input gets a turn between bounded drains.
        frames += 1;
    }
    assert_eq!(frames, 3);
    assert_eq!(d.tasks[&id(0)].status, TaskStatus::Completed);
    assert_eq!(d.failed_tasks(), 0);
    assert!(!d.jobs[&1001]);
    assert!(d.published.is_empty());
    assert!(d.activity.iter().any(|s| s.contains("ambiguous identity")));
    assert_eq!(d.tasks[&id(1)].status, TaskStatus::NotRun);
}

#[test]
fn quiet_phase_redraw_and_navigation_do_not_need_record_events() {
    let mut d = dashboard(3);
    let live = ProgressReporter::coalesced();
    live.phase(id(0), "unit", "source normalization complete");
    live.stream_phase(1001, "unit", "Parquet writing/finalization");
    d.started = Instant::now() - Duration::from_secs(65);
    d.update_snapshots(&live.state());
    assert!(d.selected_phase.contains("Parquet writing/finalization"));
    assert!(
        d.selected_phase
            .contains("progress information unavailable")
    );
    let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
    terminal.draw(|f| render(f, &mut d)).unwrap();
    let text = terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|c| c.symbol())
        .collect::<String>();
    assert!(text.contains("00:01:05"));
    assert!(text.contains("Parquet writing/finalization"));
    d.navigate(KeyCode::Down, 1);
    d.help = true;
    terminal.draw(|f| render(f, &mut d)).unwrap();
    assert_eq!(d.selected, 1);
    assert_eq!(d.total_records_read(), 0);
}

#[cfg(feature = "normalization-test-hooks")]
#[test]
fn production_replay_can_pause_without_record_updates_and_matches_non_tui_output() {
    let _diagnostics = DIAGNOSTIC_TEST_LOCK.lock().unwrap();
    use crate::historical::{
        config::{FormatCatalog, load_normalization_job},
        normalize::{execute_processing_job, manifest::DatasetManifest},
    };
    use crate::shared::io::container::SourceContainer;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let mut config = crate::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let catalog = FormatCatalog::load(root.join("data/.config/historical")).unwrap();
    let mut job = load_normalization_job(
        root.join("docs/historical/normalize-job-v3.example.json"),
        &catalog,
        &config,
        root,
    )
    .unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = root.join(format!(
        "data/tests/historical/normalize/stabilization/{stamp}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    config.resources.scratch_path = path.join("scratch");
    let template = job.tasks[0].clone();
    job.tasks = (0..48)
        .map(|i| {
            let mut t = template.clone();
            t.task_id.0 = i + 1;
            t.input_path = path.join(format!("input-{i}.csv"));
            t.source_compression = SourceContainer::Plain;
            std::fs::write(
                &t.input_path,
                format!(
                    "id,timestamp,price,volume,side,rpi\n{}, {},200,3,buy,0\n",
                    i + 1,
                    1000 + i * 1000
                )
                .replace(", ", ","),
            )
            .unwrap();
            t
        })
        .collect();
    job.output.staging_path = path.join("live-staging");
    job.output.dataset_path = path.join("live");
    let reporter = ProgressReporter::coalesced();
    let (reached_tx, reached_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_rx = Mutex::new(release_rx);
    let once = AtomicBool::new(false);
    reporter.set_phase_hook(move |_, _, _, phase| {
        if phase == "chronological append into Parquet writer" && !once.swap(true, Ordering::SeqCst)
        {
            reached_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }
    });
    let state = reporter.state();
    let mut d = ProcessingDashboard::new(&job, &config);
    let live_job = job.clone();
    let live_config = config.clone();
    let worker = thread::spawn(move || {
        execute_processing_job_with_progress(&live_job, &live_config, &reporter).unwrap()
    });
    reached_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("real worker reached replay barrier");
    for event in state.take_events(256) {
        d.handle_event(event);
    }
    d.update_snapshots(&state);
    assert_eq!(d.total_records_read(), 48);
    assert!(d.selected_phase.contains("append into Parquet"));
    let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
    d.started = Instant::now() - Duration::from_secs(65);
    terminal.draw(|f| render(f, &mut d)).unwrap();
    d.navigate(KeyCode::Down, 1);
    d.help = true;
    terminal.backend_mut().resize(80, 24);
    terminal.resize(Rect::new(0, 0, 80, 24)).unwrap();
    terminal.draw(|f| render(f, &mut d)).unwrap();
    assert_eq!(d.selected, 1);
    assert_eq!(d.total_records_read(), 48);
    release_tx.send(()).unwrap();
    let live = worker.join().unwrap();
    assert_eq!(
        live.status,
        ProcessingStatus::Complete,
        "{:?}",
        live.failure
    );
    d.reconcile_results(std::slice::from_ref(&live));
    job.output.staging_path = path.join("plain-staging");
    job.output.dataset_path = path.join("plain");
    let plain = execute_processing_job(&job, &config).unwrap();
    assert_eq!(plain.status, live.status);
    assert_eq!(plain.events_written, 48);
    assert_eq!(plain.events_written, live.events_written);
    let a: DatasetManifest =
        serde_json::from_reader(std::fs::File::open(live.manifest_path.unwrap()).unwrap()).unwrap();
    let b: DatasetManifest =
        serde_json::from_reader(std::fs::File::open(plain.manifest_path.unwrap()).unwrap())
            .unwrap();
    assert_eq!(a.processing_metrics, b.processing_metrics);
    for file in a.files {
        assert_eq!(
            std::fs::read(path.join("live").join(&file.path)).unwrap(),
            std::fs::read(path.join("plain").join(&file.path)).unwrap()
        );
    }
}

#[test]
fn stream_failure_settles_sources_without_fabricating_failed_inputs() {
    let mut d = dashboard(3);
    d.handle_event(start(0));
    d.handle_event(start(1));
    d.handle_event(ProgressEvent::StreamFailed {
        job_id: 1001,
        stream: "unit".into(),
        task_ids: vec![0, 1],
        message: "ambiguous trade overlap".into(),
    });
    assert_eq!(d.failed_tasks(), 0);
    assert_eq!(d.stopped_tasks(), 2);
    assert_eq!(d.active_tasks(), 0);
    assert_eq!(d.stream_failures.len(), 1);
    d.handle_event(ProgressEvent::JobOutcome {
        job_id: 1001,
        success: false,
        message: Some("ambiguous trade overlap".into()),
        manifest_path: None,
        failure_report_path: Some("retained-staging/failure.json".into()),
    });
    assert_eq!(d.settled_tasks(), 3);
    assert_eq!(d.not_run_tasks(), 1);
    assert!(d.published.is_empty());
}

/// Driven by .agents/reports/normalization-tui-pty.py, with an actual terminal,
/// a resize, help input and a synchronization FIFO at the production replay phase.
#[cfg(feature = "normalization-test-hooks")]
#[test]
#[ignore = "requires the PTY/FIFO driver; run through normalization-tui-pty.py"]
fn pty_production_loop_harness() {
    let _diagnostics = DIAGNOSTIC_TEST_LOCK.lock().unwrap();
    use crate::historical::config::{FormatCatalog, load_normalization_job};
    use crate::shared::io::container::SourceContainer;
    use std::{
        io::Read,
        sync::atomic::{AtomicBool, Ordering},
    };
    let path = std::path::PathBuf::from(
        std::env::var_os("MARKETFORGE_TUI_PTY_DIR").expect("PTY driver directory"),
    );
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let mut config = crate::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let catalog = FormatCatalog::load(root.join("data/.config/historical")).unwrap();
    let mut job = load_normalization_job(
        root.join("docs/historical/normalize-job-v3.example.json"),
        &catalog,
        &config,
        root,
    )
    .unwrap();
    config.resources.scratch_path = path.join("scratch");
    let template = job.tasks[0].clone();
    job.tasks = (0..4)
        .map(|i| {
            let mut t = template.clone();
            t.task_id.0 = i + 1;
            t.source_compression = SourceContainer::Plain;
            t.input_path = path.join(format!("source-{i}.csv"));
            std::fs::write(
                &t.input_path,
                format!(
                    "id,timestamp,price,volume,side,rpi\n{}, {},200,3,buy,0\n",
                    i + 1,
                    1000 + i * 1000
                )
                .replace(", ", ","),
            )
            .unwrap();
            t
        })
        .collect();
    job.output.staging_path = path.join("staging");
    job.output.dataset_path = path.join("dataset");
    // Use the same event loop and terminal guard as production, with a test-only
    // phase gate supplied when constructing its reporter.
    let once = AtomicBool::new(false);
    let gate_path = path.clone();
    let reporter = ProgressReporter::coalesced();
    reporter.set_phase_hook(move |_, _, _, phase| {
        if phase == "chronological append into Parquet writer" && !once.swap(true, Ordering::SeqCst)
        {
            std::fs::write(gate_path.join("reached"), b"replay").unwrap();
            let mut byte = [0];
            std::fs::File::open(gate_path.join("release.fifo"))
                .unwrap()
                .read_exact(&mut byte)
                .unwrap();
        }
    });
    let state = reporter.state();
    let mut d = ProcessingDashboard::new(&job, &config);
    let _log = crate::historical::normalize::progress::log::DashboardLog::enter().unwrap();
    let guard = TerminalGuard::enter().unwrap();
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout())).unwrap();
    let worker = thread::spawn(move || {
        execute_processing_job_with_progress(&job, &config, &reporter).unwrap()
    });
    run_dashboard(&mut terminal, &mut d, &state, &worker).unwrap();
    drop(terminal);
    drop(guard);
    let result = worker.join().unwrap();
    assert_eq!(result.status, ProcessingStatus::Complete);
    assert_eq!(result.events_written, 4);
    assert!(d.finished);
    assert_eq!(d.completed_tasks(), 4);
}

#[test]
fn concurrent_report_quota_failure_drains_without_deadlock_and_keeps_metrics() {
    let _diagnostics = DIAGNOSTIC_TEST_LOCK.lock().unwrap();
    use crate::historical::config::{FormatCatalog, load_normalization_job};
    use crate::historical::normalize::{
        failure::ProcessingFailureReport, session::execute_processing_session_with_progress,
    };
    use crate::shared::io::container::SourceContainer;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let mut config = crate::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let catalog = FormatCatalog::load(root.join("data/.config/historical")).unwrap();
    let mut job = load_normalization_job(
        root.join("docs/historical/normalize-job-v3.example.json"),
        &catalog,
        &config,
        root,
    )
    .unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = root.join(format!(
        "data/tests/historical/normalize/stabilization/quota-{stamp}"
    ));
    std::fs::create_dir_all(&path).unwrap();
    config.resources.scratch_path = path.join("scratch");
    config.resources.scratch_budget_bytes = 1;
    config.resources.workers = 2;
    let template = job.tasks[0].clone();
    job.tasks = (0..16)
        .map(|i| {
            let mut t = template.clone();
            t.task_id.0 = i + 1;
            t.instrument_id += i as i64;
            t.source_compression = SourceContainer::Plain;
            t.input_path = path.join(format!("input-{i}.csv"));
            std::fs::write(
                &t.input_path,
                "id,timestamp,price,volume,side,rpi\n1,1000,200,3,buy,0\n",
            )
            .unwrap();
            t
        })
        .collect();
    job.output.staging_path = path.join("staging");
    job.output.dataset_path = path.join("dataset");
    let reporter = ProgressReporter::coalesced();
    let state = reporter.state();
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        done_tx
            .send(execute_processing_session_with_progress(
                &[job],
                &config,
                &reporter,
            ))
            .unwrap()
    });
    let result = done_rx
        .recv_timeout(Duration::from_secs(15))
        .expect("quota failure must drain without blocked workers")
        .unwrap();
    worker.join().unwrap();
    assert_eq!(result.jobs[0].status, ProcessingStatus::Failed);
    assert!(result.jobs[0].manifest_path.is_none());
    assert!(
        result.jobs[0]
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("scratch bytes")
    );
    let report: ProcessingFailureReport = serde_json::from_reader(
        std::fs::File::open(result.jobs[0].failure_report_path.as_ref().unwrap()).unwrap(),
    )
    .unwrap();
    assert!((1..=2).contains(&report.processing_metrics.counters.records_read));
    assert!(
        report
            .processing_metrics
            .scoped_integrity
            .global
            .invalid_record
            .eligible
            > 0
    );
    assert_eq!(report.processing_metrics.tasks.len(), 16);
    assert!(
        state
            .take_events(100)
            .iter()
            .any(|e| matches!(e, ProgressEvent::JobOutcome { success: false, .. }))
    );
}

#[test]
fn real_stream_rejection_preserves_spilled_failure_reports_without_failed_inputs() {
    let _diagnostics = DIAGNOSTIC_TEST_LOCK.lock().unwrap();
    use crate::historical::config::{FormatCatalog, load_normalization_job};
    use crate::historical::normalize::failure::ProcessingFailureReport;
    use crate::shared::io::container::SourceContainer;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let mut config = crate::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let catalog = FormatCatalog::load(root.join("data/.config/historical")).unwrap();
    let mut job = load_normalization_job(
        root.join("docs/historical/normalize-job-v3.example.json"),
        &catalog,
        &config,
        root,
    )
    .unwrap();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = root.join(format!(
        "data/tests/historical/normalize/stabilization/overlap-{stamp}"
    ));
    std::fs::create_dir_all(&path).unwrap();
    config.resources.scratch_path = path.join("scratch");
    let template = job.tasks[0].clone();
    job.tasks = (0..2)
        .map(|i| {
            let mut t = template.clone();
            t.task_id.0 = i + 1;
            t.source_compression = SourceContainer::Plain;
            t.input_path = path.join(format!("source-{i}.csv"));
            // The profile already establishes archive-local IDs. Equal intervals
            // cannot be automatically reconciled into a global identity domain.
            std::fs::write(
                &t.input_path,
                "id,timestamp,price,volume,side,rpi\n1,1000,200,3,buy,0\n",
            )
            .unwrap();
            t
        })
        .collect();
    job.output.staging_path = path.join("staging");
    job.output.dataset_path = path.join("dataset");
    let reporter = ProgressReporter::coalesced();
    let mut d = ProcessingDashboard::new(&job, &config);
    let result = execute_processing_job_with_progress(&job, &config, &reporter).unwrap();
    assert_eq!(result.status, ProcessingStatus::Failed);
    assert!(result.manifest_path.is_none());
    let events = reporter.state().take_events(100);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, ProgressEvent::TaskFailed { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ProgressEvent::StreamFailed { .. }))
    );
    for event in events {
        d.handle_event(event);
    }
    d.update_snapshots(&reporter.state());
    d.reconcile_results(std::slice::from_ref(&result));
    assert_eq!(d.failed_tasks(), 0);
    assert_eq!(d.stream_failures.len(), 1);
    assert_eq!(d.total_records_read(), 2);
    assert!(d.published.is_empty());
    assert_eq!(d.active_tasks(), 0);
    let report: ProcessingFailureReport =
        serde_json::from_reader(std::fs::File::open(result.failure_report_path.unwrap()).unwrap())
            .unwrap();
    assert_eq!(report.failed_task_id, None);
    assert!(report.error_message.contains("ambiguous trade overlap"));
    assert_eq!(report.processing_metrics.tasks.len(), 2);
    assert_eq!(report.processing_metrics.scoped_integrity.files.len(), 2);
    assert_eq!(report.processing_metrics.counters.rows_unpublished, 2);
    assert_eq!(
        std::fs::read_dir(config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn overview_tracks_running_units_while_selection_stays_on_a_completed_source() {
    let mut d = dashboard(2);
    let reporter = ProgressReporter::coalesced();
    reporter.phase(id(0), "done", "source ready");
    reporter.phase(id(1), "active", "source collected");
    d.handle_event(start(0));
    d.handle_event(complete(0, 1));
    d.handle_event(start(1));
    reporter.stream_phase(1001, "active", "identity resolution");
    reporter.stream_phase(1001, "done", "reconciliation complete");
    d.update_snapshots(&reporter.state());
    assert_eq!(d.selected, 0);
    assert!(d.selected_phase.contains("complete"));
    assert!(d.overview_phase.contains("identity resolution"));
}

#[test]
fn production_worker_sampler_publishes_without_an_input_record_delta() {
    use crate::historical::normalize::{metrics::ProcessingCounters, progress::WorkerProgress};
    let reporter = ProgressReporter::coalesced();
    let state = reporter.state();
    let mut worker = WorkerProgress::new(&reporter, id(0));
    let mut counters = ProcessingCounters {
        records_read: 10,
        records_processed: 10,
        ..Default::default()
    };
    worker.maybe_update(&counters);
    assert_eq!(state.task_snapshot(id(0)).unwrap().records_read, 10);
    counters.events_written = 7; // Same input count; later output progress.
    worker.maybe_update(&counters);
    assert_eq!(state.task_snapshot(id(0)).unwrap().records_written, 7);
}
