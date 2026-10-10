#![cfg(feature = "process")]

use marketforge_engine::formats::depth::{DepthProcessingOutcome, DepthSourceEventMetadata};

// -----------------------------------------------------------------------------
// Source metadata attachment
// -----------------------------------------------------------------------------

#[test]
fn attaches_source_metadata() {
    let source = DepthSourceEventMetadata {
        source_event_ordinal: Some(42),

        event_timestamp_ns: 1_788_220_802_715_000_000,
        system_timestamp_ns: Some(1_788_220_802_716_000_000),

        sequence_start: Some(100),
        sequence_end: Some(100),
    };

    let outcome = DepthProcessingOutcome::changes(vec![]).with_source(source.clone());

    assert_eq!(outcome.source, source);

    assert_eq!(outcome.source.source_event_ordinal, Some(42));

    assert_eq!(outcome.source.sequence_start, Some(100));

    assert_eq!(outcome.source.sequence_end, Some(100));
}

// -----------------------------------------------------------------------------
// Unsequenced source
// -----------------------------------------------------------------------------

#[test]
fn supports_unsequenced_sources() {
    let source = DepthSourceEventMetadata {
        source_event_ordinal: Some(1),

        event_timestamp_ns: 1000,
        system_timestamp_ns: None,

        sequence_start: None,
        sequence_end: None,
    };

    let outcome = DepthProcessingOutcome::changes(vec![]).with_source(source);

    assert!(outcome.source.sequence_start.is_none());
    assert!(outcome.source.sequence_end.is_none());
}

// -----------------------------------------------------------------------------
// Default constructors remain compatible
// -----------------------------------------------------------------------------

#[test]
fn existing_constructors_remain_compatible() {
    let snapshot = DepthProcessingOutcome::initialization(vec![]);
    let changes = DepthProcessingOutcome::changes(vec![]);

    assert_eq!(snapshot.source.source_event_ordinal, None);
    assert_eq!(changes.source.source_event_ordinal, None);

    assert_eq!(snapshot.source.sequence_start, None);
    assert_eq!(changes.source.sequence_end, None);
}
