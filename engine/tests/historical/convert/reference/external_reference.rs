use marketforge_engine::historical::convert::reference::ExternalReferencePosition;

use {
    marketforge_engine::historical::convert::reference::ExternalReferenceCursor,
    marketforge_engine::historical::convert::reference::ReferenceKey,
    marketforge_engine::historical::convert::reference::ReferenceState,
    marketforge_engine::historical::convert::reference::ReferenceUnavailable,
    marketforge_engine::historical::merge::{PhysicalMergeManifest, UnifiedMergeMode},
};

const MAX_STALENESS_NS: u64 = 5_000_000_000;

struct Fixture {
    dataset: tempfile::TempDir,
    manifest: PhysicalMergeManifest,
    stream_ids: Vec<String>,
}

fn state_snapshot(
    state: &ReferenceState,
) -> Vec<(
    String,
    u64,
    ReferenceKey,
    marketforge_engine::historical::convert::ConversionDecimal,
)> {
    let mut observations = state
        .observations()
        .map(|observation| {
            (
                observation.stream_id.clone(),
                observation.dataset_id,
                observation.key,
                observation.price,
            )
        })
        .collect::<Vec<_>>();

    observations.sort_by(|a, b| a.0.cmp(&b.0));

    observations
}
fn fixture() -> Fixture {
    let dataset = tempfile::tempdir().unwrap();
    let manifest = crate::market::create(dataset.path(), UnifiedMergeMode::Trades, false);
    let stream_ids = manifest
        .streams
        .iter()
        .map(|stream| stream.stream_id.clone())
        .collect();
    Fixture {
        dataset,
        manifest,
        stream_ids,
    }
}

impl Fixture {
    fn cursor(&self, streams: Vec<String>) -> ExternalReferenceCursor {
        ExternalReferenceCursor::new(self.dataset.path(), &self.manifest, streams).unwrap()
    }
}

// -----------------------------------------------------------------------------
// Cursor construction
// -----------------------------------------------------------------------------

#[test]
fn constructs_external_reference_cursor() {
    let fixture = fixture();

    let cursor = fixture.cursor(fixture.stream_ids.clone());

    assert_eq!(cursor.observations_read(), 0);
    assert_eq!(cursor.reference_updates(), 0);
    assert_eq!(cursor.pending_timestamp_ns(), None);
}

// -----------------------------------------------------------------------------
// Missing streams
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_reference_stream() {
    let fixture = fixture();

    let result = ExternalReferenceCursor::new(
        fixture.dataset.path(),
        &fixture.manifest,
        vec!["nonexistent:trade:stream".to_owned()],
    );

    assert!(result.is_err());
}

#[test]
fn rejects_empty_reference_selection() {
    let fixture = fixture();

    let result = ExternalReferenceCursor::new(
        fixture.dataset.path(),
        &fixture.manifest,
        Vec::<String>::new(),
    );

    assert!(result.is_err());
}

// -----------------------------------------------------------------------------
// Strict timestamp ordering
// -----------------------------------------------------------------------------

#[test]
fn excludes_equal_timestamp_references() {
    let fixture = fixture();

    let mut cursor = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    // Three trades share the first physical timestamp.
    let first_timestamp = 100;

    let updates = cursor.advance_before(first_timestamp, &mut state).unwrap();

    assert_eq!(updates, 0);
    assert!(state.is_empty());

    assert_eq!(cursor.pending_timestamp_ns(), Some(first_timestamp),);

    // The first observation becomes eligible only when the
    // requested timestamp advances beyond it.
    let updates = cursor
        .advance_before(first_timestamp + 1, &mut state)
        .unwrap();

    assert!(updates >= 1);
    assert!(!state.is_empty());

    for observation in state.observations() {
        assert!(observation.key.timestamp_ns < first_timestamp + 1);
    }
}

// -----------------------------------------------------------------------------
// Pending observation preservation
// -----------------------------------------------------------------------------

#[test]
fn preserves_pending_future_observation() {
    let fixture = fixture();

    let mut cursor = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let first_timestamp = 100;

    cursor.advance_before(first_timestamp, &mut state).unwrap();

    let pending = cursor.pending_timestamp_ns();

    assert_eq!(pending, Some(first_timestamp));

    cursor.advance_before(first_timestamp, &mut state).unwrap();

    assert_eq!(cursor.pending_timestamp_ns(), pending,);

    assert_eq!(cursor.reference_updates(), 0);
}

// -----------------------------------------------------------------------------
// Monotonic advancement
// -----------------------------------------------------------------------------

#[test]
fn rejects_timestamp_regression() {
    let fixture = fixture();

    let mut cursor = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let timestamp = 100;

    cursor.advance_before(timestamp + 1000, &mut state).unwrap();

    assert!(cursor.advance_before(timestamp, &mut state).is_err());
}

// -----------------------------------------------------------------------------
// Multiple reference streams
// -----------------------------------------------------------------------------

#[test]
fn advances_multiple_reference_streams() {
    let fixture = fixture();

    let mut cursor = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 100;

    let updates = cursor.advance_before(start + 30, &mut state).unwrap();

    assert!(updates > 0);

    assert_eq!(state.len(), 4);

    assert_eq!(cursor.reference_updates(), updates,);

    for observation in state.observations() {
        assert!(observation.key.timestamp_ns < start + 30);
    }
}

// -----------------------------------------------------------------------------
// Reference staleness
// -----------------------------------------------------------------------------

#[test]
fn evaluates_reference_staleness() {
    let fixture = fixture();

    let mut cursor = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 100;

    cursor.advance_before(start + 30, &mut state).unwrap();

    let observation = state.observations().next().unwrap();

    let stream_id = observation.stream_id.clone();
    let reference_timestamp = observation.key.timestamp_ns;

    let source_key = ReferenceKey::new(
        reference_timestamp + MAX_STALENESS_NS as i64 + 1,
        u32::MAX,
        u64::MAX,
        u64::MAX,
    );

    let result = state.evaluate(&stream_id, source_key, MAX_STALENESS_NS);

    assert_eq!(result.unwrap_err(), ReferenceUnavailable::Stale,);
}

// -----------------------------------------------------------------------------
// Reference provenance
// -----------------------------------------------------------------------------

#[test]
fn preserves_reference_provenance() {
    let fixture = fixture();

    let mut cursor = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 100;

    cursor.advance_before(start + 30, &mut state).unwrap();

    for observation in state.observations() {
        let stream = fixture
            .manifest
            .streams
            .iter()
            .find(|stream| stream.stream_id == observation.stream_id)
            .unwrap();

        assert_eq!(observation.dataset_id, stream.dataset_id,);

        assert_eq!(observation.key.stream_rank, stream.stream_rank,);

        assert!(observation.price.is_positive());
    }
}

// -----------------------------------------------------------------------------
// Selected reference filtering
// -----------------------------------------------------------------------------

#[test]
fn filters_unselected_reference_streams() {
    let fixture = fixture();

    let selected = fixture.stream_ids[0].clone();

    let mut cursor = fixture.cursor(vec![selected.clone()]);
    let mut state = ReferenceState::new();

    let start = 100;

    cursor.advance_before(start + 30, &mut state).unwrap();

    assert_eq!(state.len(), 1);

    let observation = state.observations().next().unwrap();

    assert_eq!(observation.stream_id, selected);
}

// -----------------------------------------------------------------------------
// Diagnostics
// -----------------------------------------------------------------------------

#[test]
fn prints_external_reference_summary() {
    let fixture = fixture();

    let mut cursor = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 100;
    let end = start + 30;

    let updates = cursor.advance_before(end, &mut state).unwrap();

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — EXTERNAL TRADE REFERENCES");
    println!("{}", "=".repeat(85));

    println!("Reference streams : {}", state.len());
    println!("Observations read : {}", cursor.observations_read());
    println!("Reference updates : {}", updates);
    println!("Pending timestamp : {:?}", cursor.pending_timestamp_ns());

    println!("STATUS            : PASS");

    assert_eq!(state.len(), 4);
}

#[test]
fn resumed_cursor_matches_uninterrupted_execution() {
    let fixture = fixture();

    let start = 100;

    let checkpoint_timestamp = start + 10;
    let final_timestamp = start + 30;

    // -------------------------------------------------------------
    // Uninterrupted reference execution
    // -------------------------------------------------------------

    let mut sequential = fixture.cursor(fixture.stream_ids.clone());
    let mut sequential_state = ReferenceState::new();

    sequential
        .advance_before(checkpoint_timestamp, &mut sequential_state)
        .unwrap();

    let checkpoint_state = sequential_state.snapshot();
    let checkpoint_position = sequential.position();

    sequential
        .advance_before(final_timestamp, &mut sequential_state)
        .unwrap();

    // -------------------------------------------------------------
    // Resume from checkpoint
    // -------------------------------------------------------------

    let mut resumed = ExternalReferenceCursor::resume(
        fixture.dataset.path(),
        &fixture.manifest,
        fixture.stream_ids.clone(),
        checkpoint_position,
    )
    .unwrap();

    let mut resumed_state = checkpoint_state;

    resumed
        .advance_before(final_timestamp, &mut resumed_state)
        .unwrap();

    // -------------------------------------------------------------
    // Exact state equivalence
    // -------------------------------------------------------------

    assert_eq!(
        state_snapshot(&sequential_state),
        state_snapshot(&resumed_state),
    );

    assert_eq!(sequential.position(), resumed.position(),);
}

#[test]
fn resumed_cursor_excludes_equal_timestamp_references() {
    let fixture = fixture();

    let first_timestamp = 100;

    let mut original = fixture.cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    original
        .advance_before(first_timestamp, &mut state)
        .unwrap();

    let checkpoint = original.position();

    let mut resumed = ExternalReferenceCursor::resume(
        fixture.dataset.path(),
        &fixture.manifest,
        fixture.stream_ids.clone(),
        checkpoint,
    )
    .unwrap();

    let mut resumed_state = state.snapshot();

    let updates = resumed
        .advance_before(first_timestamp, &mut resumed_state)
        .unwrap();

    assert_eq!(updates, 0);
    assert!(resumed_state.is_empty());

    assert_eq!(resumed.pending_timestamp_ns(), Some(first_timestamp),);

    resumed
        .advance_before(first_timestamp + 1, &mut resumed_state)
        .unwrap();

    assert!(!resumed_state.is_empty());
}

#[test]
fn rejects_invalid_reference_resume_position() {
    let fixture = fixture();

    let invalid = ExternalReferencePosition {
        observations_consumed: 100,
        next_observation_index: 99,
    };

    assert!(
        ExternalReferenceCursor::resume(
            fixture.dataset.path(),
            &fixture.manifest,
            fixture.stream_ids.clone(),
            invalid,
        )
        .is_err()
    );

    let beyond_end = ExternalReferencePosition {
        observations_consumed: fixture.manifest.events_written + 1,
        next_observation_index: fixture.manifest.events_written + 1,
    };

    assert!(
        ExternalReferenceCursor::resume(
            fixture.dataset.path(),
            &fixture.manifest,
            fixture.stream_ids.clone(),
            beyond_end,
        )
        .is_err()
    );
}
