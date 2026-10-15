use marketforge_engine::historical::convert::reference::ExternalReferencePosition;
use marketforge_engine::historical::normalize::{
    convert::reference::{
        ExternalReferenceCursor, ReferenceKey, ReferenceState, ReferenceUnavailable,
    },
    merge::PhysicalMergeManifest,
};
use std::{fs, path::PathBuf, sync::OnceLock};

const MAX_STALENESS_NS: u64 = 5_000_000_000;

struct Fixture {
    dataset: PathBuf,
    manifest: PhysicalMergeManifest,
    stream_ids: Vec<String>,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
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
                observation.price.clone(),
            )
        })
        .collect::<Vec<_>>();

    observations.sort_by(|a, b| a.0.cmp(&b.0));

    observations
}
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();

    FIXTURE.get_or_init(|| {
        let dataset = root().join("data/merged/conversion-multihop");

        let manifest: PhysicalMergeManifest =
            serde_json::from_slice(&fs::read(dataset.join("manifest.json")).unwrap()).unwrap();

        manifest.validate().unwrap();

        let stream_ids = manifest
            .streams
            .iter()
            .map(|stream| stream.stream_id.clone())
            .collect::<Vec<_>>();

        assert_eq!(stream_ids.len(), 3);

        Fixture {
            dataset,
            manifest,
            stream_ids,
        }
    })
}

fn cursor(streams: Vec<String>) -> ExternalReferenceCursor {
    let fixture = fixture();

    ExternalReferenceCursor::new(&fixture.dataset, &fixture.manifest, streams).unwrap()
}

// -----------------------------------------------------------------------------
// Cursor construction
// -----------------------------------------------------------------------------

#[test]
fn constructs_external_reference_cursor() {
    let fixture = fixture();

    let cursor = cursor(fixture.stream_ids.clone());

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
        &fixture.dataset,
        &fixture.manifest,
        vec!["nonexistent:trade:stream".to_owned()],
    );

    assert!(result.is_err());
}

#[test]
fn rejects_empty_reference_selection() {
    let fixture = fixture();

    let result =
        ExternalReferenceCursor::new(&fixture.dataset, &fixture.manifest, Vec::<String>::new());

    assert!(result.is_err());
}

// -----------------------------------------------------------------------------
// Strict timestamp ordering
// -----------------------------------------------------------------------------

#[test]
fn excludes_equal_timestamp_references() {
    let fixture = fixture();

    let mut cursor = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    // First timestamp from our real conversion fixture.
    let first_timestamp = 1_788_220_800_322_740_000_i64;

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

    let mut cursor = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let first_timestamp = 1_788_220_800_322_740_000_i64;

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

    let mut cursor = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let timestamp = 1_788_220_800_322_740_000_i64;

    cursor.advance_before(timestamp + 1000, &mut state).unwrap();

    assert!(cursor.advance_before(timestamp, &mut state).is_err());
}

// -----------------------------------------------------------------------------
// Multiple reference streams
// -----------------------------------------------------------------------------

#[test]
fn advances_multiple_reference_streams() {
    let fixture = fixture();

    let mut cursor = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 1_788_220_800_322_740_000_i64;

    let updates = cursor
        .advance_before(start + 10_000_000_000, &mut state)
        .unwrap();

    assert!(updates > 0);

    assert_eq!(state.len(), 3);

    assert_eq!(cursor.reference_updates(), updates,);

    for observation in state.observations() {
        assert!(observation.key.timestamp_ns < start + 10_000_000_000);
    }
}

// -----------------------------------------------------------------------------
// Reference staleness
// -----------------------------------------------------------------------------

#[test]
fn evaluates_reference_staleness() {
    let fixture = fixture();

    let mut cursor = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 1_788_220_800_322_740_000_i64;

    cursor
        .advance_before(start + 10_000_000_000, &mut state)
        .unwrap();

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

    let mut cursor = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 1_788_220_800_322_740_000_i64;

    cursor
        .advance_before(start + 10_000_000_000, &mut state)
        .unwrap();

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

    let mut cursor = cursor(vec![selected.clone()]);
    let mut state = ReferenceState::new();

    let start = 1_788_220_800_322_740_000_i64;

    cursor
        .advance_before(start + 10_000_000_000, &mut state)
        .unwrap();

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

    let mut cursor = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    let start = 1_788_220_800_322_740_000_i64;
    let end = start + 10_000_000_000;

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

    assert_eq!(state.len(), 3);
}

#[test]
fn resumed_cursor_matches_uninterrupted_execution() {
    let fixture = fixture();

    let start = 1_788_220_800_322_740_000_i64;

    let checkpoint_timestamp = start + 3_000_000_000;
    let final_timestamp = start + 10_000_000_000;

    // -------------------------------------------------------------
    // Uninterrupted reference execution
    // -------------------------------------------------------------

    let mut sequential = cursor(fixture.stream_ids.clone());
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
        &fixture.dataset,
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

    let first_timestamp = 1_788_220_800_322_740_000_i64;

    let mut original = cursor(fixture.stream_ids.clone());
    let mut state = ReferenceState::new();

    original
        .advance_before(first_timestamp, &mut state)
        .unwrap();

    let checkpoint = original.position();

    let mut resumed = ExternalReferenceCursor::resume(
        &fixture.dataset,
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
            &fixture.dataset,
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
            &fixture.dataset,
            &fixture.manifest,
            fixture.stream_ids.clone(),
            beyond_end,
        )
        .is_err()
    );
}
