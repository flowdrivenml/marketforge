use marketforge_engine::process::merge::{
    MaterializationBatchLimits, MaterializationWindowBuilder, UnifiedTimelineRecord,
};

fn reference(ordinal: u64, rows: u64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: ordinal as i64,
        stream_rank: 0,
        event_kind: 1,
        dataset_id: 10,
        task_id: 0,
        event_ordinal: ordinal,
        canonical_row_offset: ordinal,
        canonical_row_count: rows,
        event_boundary: Some(0),
        source_operation: Some(1),
    }
}

#[test]
fn splits_on_event_limit() {
    let limits = MaterializationBatchLimits::new(2, 100).unwrap();
    let mut builder = MaterializationWindowBuilder::new(limits);

    assert!(builder.push(reference(0, 1)).unwrap().is_none());
    assert!(builder.push(reference(1, 1)).unwrap().is_none());

    let window = builder.push(reference(2, 1)).unwrap().unwrap();

    assert_eq!(window.event_count(), 2);
    assert_eq!(window.canonical_rows, 2);

    assert_eq!(builder.finish().unwrap().event_count(), 1);
}

#[test]
fn splits_on_canonical_row_limit() {
    let limits = MaterializationBatchLimits::new(100, 10).unwrap();
    let mut builder = MaterializationWindowBuilder::new(limits);

    builder.push(reference(0, 6)).unwrap();

    let window = builder.push(reference(1, 5)).unwrap().unwrap();

    assert_eq!(window.event_count(), 1);
    assert_eq!(window.canonical_rows, 6);

    assert_eq!(builder.finish().unwrap().canonical_rows, 5);
}

#[test]
fn oversized_depth_event_is_atomic() {
    let limits = MaterializationBatchLimits::new(100, 10).unwrap();
    let mut builder = MaterializationWindowBuilder::new(limits);

    builder.push(reference(0, 50_000)).unwrap();

    let window = builder.push(reference(1, 1)).unwrap().unwrap();

    assert_eq!(window.event_count(), 1);
    assert_eq!(window.canonical_rows, 50_000);

    assert_eq!(builder.finish().unwrap().event_count(), 1);
}

#[test]
fn zero_row_events_are_preserved() {
    let limits = MaterializationBatchLimits::new(3, 10).unwrap();
    let mut builder = MaterializationWindowBuilder::new(limits);

    builder.push(reference(0, 0)).unwrap();
    builder.push(reference(1, 0)).unwrap();
    builder.push(reference(2, 0)).unwrap();

    let window = builder.finish().unwrap();

    assert_eq!(window.event_count(), 3);
    assert_eq!(window.canonical_rows, 0);
}

#[test]
fn preserves_chronological_order() {
    let limits = MaterializationBatchLimits::new(2, 10).unwrap();
    let mut builder = MaterializationWindowBuilder::new(limits);

    let mut output = Vec::new();

    for ordinal in 0..5 {
        if let Some(window) = builder.push(reference(ordinal, 1)).unwrap() {
            output.extend(window.references);
        }
    }

    if let Some(window) = builder.finish() {
        output.extend(window.references);
    }

    assert_eq!(
        output
            .iter()
            .map(|event| event.event_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
}

#[test]
fn rejects_invalid_limits() {
    assert!(MaterializationBatchLimits::new(0, 10).is_err());
    assert!(MaterializationBatchLimits::new(10, 0).is_err());
}
