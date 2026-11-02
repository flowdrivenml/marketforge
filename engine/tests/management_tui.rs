use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use marketforge_engine::{
    management_protocol::{definitions::*, *},
    tui::management::{App, Intent, Modal, Screen, Update, safe_text},
};
use std::time::{Duration, Instant};
fn view() -> Overview {
    Overview {
        daemon_instance: "test-daemon".into(),
        observation_unix_ms: "18446744073709551615".into(),
        graph_revision: "1".into(),
        config_revision: "1".into(),
        event_cursor: "1".into(),
        serving: true,
        shutting_down: false,
        streams: ["a", "b"]
            .into_iter()
            .map(|id| StreamView {
                stream_id: id.into(),
                definition: PersistedStreamDefinition {
                    stream_id: id.into(),
                    recording_stream_key: None,
                    definition: StreamDefinition::Derived(CreateDerivedStreamRequest {
                        name: id.into(),
                        operation: DerivedOperation::Merge,
                        inputs: vec!["x".into(), "y".into()],
                        reference_inputs: vec![],
                        conversion: None,
                        ordering: Some(MergeOrdering::ArrivalOrder),
                        max_lateness_ns: None,
                        target_quote_asset: None,
                    }),
                    desired: DesiredStreamState::default(),
                    revision: 1,
                },
                runtime: "Running".into(),
                generation: "18446744073709551615".into(),
                readiness: "Ready".into(),
                reason: None,
                inputs: vec!["x".into(), "y".into()],
                reference_inputs: vec![],
                dependents: vec![],
                direct_owners: vec![],
                why_running: vec!["Unix subscription".into()],
                last_data_age_ms: None,
                integrity: serde_json::json!({"coverage":"unverified"}),
            })
            .collect(),
        pool: PoolView {
            revision: "2".into(),
            requested_target: 8,
            live_workers: 4,
            starting_workers: 4,
            retiring_workers: 0,
            executing_turns: 2,
            resizing: true,
            resize_error: None,
            saved_startup_workers: Some(4),
            registered_processors: 2,
            ready_processors: None,
            retained_payload_events: None,
            retained_payload_bytes: None,
        },
        metrics: None,
        operations: vec![],
        consistency: "independent samples".into(),
    }
}
fn text(app: &App, width: u16, height: u16) -> String {
    let mut terminal =
        ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, height)).unwrap();
    terminal.draw(|f| app.draw(f)).unwrap();
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|c| c.symbol())
        .collect::<String>()
}
fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}
#[test]
fn layouts_narrow_fallback_worker_progress_and_stale_state() {
    let mut app = App::default();
    app.update(Update::Overview(view()));
    assert!(text(&app, 110, 24).contains("Streams"));
    app.screen = Screen::Runtime;
    let runtime = text(&app, 110, 24);
    assert!(runtime.contains("Live: 4"));
    assert!(runtime.contains("In progress"));
    assert!(text(&app, 30, 8).contains("too narrow"));
    app.update(Update::Disconnected);
    assert!(text(&app, 110, 24).contains("STALE"));
    assert!(app.input(key(KeyCode::Char(':'))).is_none());
    assert!(app.modal.is_none());
}
#[test]
fn selection_is_stable_and_quit_is_not_shutdown() {
    let mut app = App::default();
    app.update(Update::Overview(view()));
    app.input(key(KeyCode::Char('j')));
    assert_eq!(app.selected.as_deref(), Some("b"));
    let mut changed = view();
    changed.streams.reverse();
    app.update(Update::Overview(changed));
    assert_eq!(app.selected.as_deref(), Some("b"));
    assert!(app.input(key(KeyCode::Char('q'))).is_none());
    assert!(app.quit);
}
#[test]
fn disruptive_actions_require_review_and_explicit_confirmation() {
    let mut app = App::default();
    app.update(Update::Overview(view()));
    app.modal = Some(Modal::Review(ChangePlan {
        id: "plan-1".into(),
        daemon_instance: "d".into(),
        action: Action::Shutdown,
        graph_revision: "1".into(),
        pool_revision: "1".into(),
        config_revision: "1".into(),
        demand_revision: None,
        affected_streams: vec!["a".into()],
        transitions: vec!["retire".into()],
        continuity_impact: "ends subscriptions".into(),
        persistence: "none".into(),
        warnings: vec![],
        expires_in_seconds: 60,
    }));
    let mut ui = text(&app, 110, 24);
    assert!(ui.contains("REVIEW"));
    assert!(ui.contains("ends subscriptions"));
    assert!(matches!(
        app.input(key(KeyCode::Enter)),
        Some(Intent::Apply(_, _))
    ));
    app.modal = None;
    app.updated = Some(Instant::now() - Duration::from_secs(10));
    app.input(key(KeyCode::Char(':')));
    assert!(app.modal.is_none());
    ui = text(&app, 110, 24);
    assert!(ui.contains("STALE"));
}
#[test]
fn form_parsing_checks_duration_overflow_and_missing_metadata() {
    let app = App::default();
    let fields = vec![
        ("name".into(), "m".into()),
        ("inputs".into(), "a,b".into()),
        ("ordering".into(), "event_time".into()),
        ("lateness".into(), u64::MAX.to_string()),
    ];
    assert!(
        app.form_action("merge", &fields)
            .unwrap_err()
            .contains("overflow")
    );
    let fields = vec![
        ("name".into(), "a".into()),
        ("template".into(), "0".into()),
        ("channel".into(), "trades".into()),
    ];
    assert!(
        app.form_action("source", &fields)
            .unwrap_err()
            .contains("No authoritative")
    );
}
#[test]
fn terminal_controls_cannot_reach_rendered_output() {
    assert_eq!(safe_text("a\x1b[2J\r\x07\u{202e}b"), "a[2Jb");
    let mut app = App::default();
    let mut v = view();
    v.streams[0].stream_id = "evil\x1b[31m".into();
    app.update(Update::Overview(v));
    let output = text(&app, 110, 24);
    assert!(!output.contains('\x1b'));
}
