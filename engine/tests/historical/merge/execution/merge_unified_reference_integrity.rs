use crate::publication::{depth_publication, trade_publication};
use marketforge_engine::historical::merge::{
    DepthStream, DepthTaskInventory, MergeEventRef, MergedEvent, TradeStream, TradeTaskInventory,
    UnifiedReferenceValidator, UnifiedTimelineRecord, build_depth_inventory, build_trade_inventory,
    reconcile_depth_boundaries,
};

fn trade(offset: u64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        causal: None,
        event_timestamp_ns: 100,
        stream_rank: 0,
        event_kind: 0,
        dataset_id: 11,
        task_id: 7,
        event_ordinal: offset,
        canonical_row_offset: offset,
        canonical_row_count: 1,
        event_boundary: None,
        source_operation: None,
    }
}

fn depth(ordinal: u64, offset: u64, count: u64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_kind: 1,
        dataset_id: 12,
        event_ordinal: ordinal,
        canonical_row_offset: offset,
        canonical_row_count: count,
        event_boundary: Some(0),
        source_operation: Some(1),
        ..trade(0)
    }
}

fn trade_validator() -> UnifiedReferenceValidator {
    UnifiedReferenceValidator::new(
        &[TradeTaskInventory {
            dataset_id: 11,
            task_id: 7,
            files: vec![],
            canonical_rows: 3,
            start_timestamp_ns: 100,
            end_timestamp_ns: 100,
        }],
        &[],
    )
    .unwrap()
}

fn depth_validator() -> UnifiedReferenceValidator {
    let boundary = marketforge_engine::historical::normalize::boundary::DepthBoundaryManifest::new(
        marketforge_engine::shared::domain::canonical::Exchange::Bybit,
        12,
        "BTCUSDT".into(),
        "depth".into(),
    );
    UnifiedReferenceValidator::new(
        &[],
        &[DepthTaskInventory {
            dataset_id: 12,
            task_id: 7,
            canonical_files: vec![],
            canonical_slices: None,
            source_range: Some((0, 3, 0)),
            events_path: Default::default(),
            segments_path: Default::default(),
            boundary_path: Default::default(),
            canonical_rows: 4,
            segments: marketforge_engine::shared::persistence::parquet::DepthSegmentManifest::new(
                4,
                vec![marketforge_engine::shared::domain::book::BookSegment {
                    segment_id: 0,
                    start_timestamp_ns: 100,
                    initial_event_offset: 0,
                    initial_level_count: 2,
                }],
            )
            .unwrap(),
            boundary: (&boundary).into(),
        }],
    )
    .unwrap()
}

fn reject(
    validator: &mut UnifiedReferenceValidator,
    record: UnifiedTimelineRecord,
    expected: &str,
) {
    let error = validator.validate(&record).unwrap_err();
    assert!(error.to_string().contains(expected), "{error}");
}

#[test]
fn accepts_valid_trade_references() {
    let mut validator = trade_validator();
    for offset in 0..3 {
        validator.validate(&trade(offset)).unwrap();
    }
}

#[test]
fn rejects_duplicate_trade_identity() {
    let mut validator = trade_validator();
    validator.validate(&trade(0)).unwrap();
    reject(
        &mut validator,
        trade(0),
        "duplicate or regressing canonical event ordinal",
    );
}

#[test]
fn rejects_unknown_task_identity() {
    let mut validator = trade_validator();
    validator.validate(&trade(0)).unwrap();
    reject(
        &mut validator,
        UnifiedTimelineRecord {
            task_id: 8,
            ..trade(1)
        },
        "unknown canonical task",
    );
}

#[test]
fn rejects_out_of_bounds_trade_offset() {
    let mut validator = trade_validator();
    validator.validate(&trade(2)).unwrap();
    reject(
        &mut validator,
        trade(3),
        "canonical reference exceeds task bounds",
    );
    reject(
        &mut validator,
        trade(u64::MAX),
        "canonical row range overflow",
    );
}

#[test]
fn rejects_overlapping_depth_ranges() {
    let mut validator = depth_validator();
    validator.validate(&depth(0, 0, 2)).unwrap();
    reject(
        &mut validator,
        depth(1, 1, 2),
        "overlapping canonical row references",
    );
    validator.validate(&depth(1, 2, 2)).unwrap();
}

#[test]
fn rejects_task_event_kind_mismatch() {
    let mut validator = trade_validator();
    validator.validate(&trade(0)).unwrap();
    reject(
        &mut validator,
        UnifiedTimelineRecord {
            event_kind: 1,
            ..trade(1)
        },
        "canonical task/event kind mismatch",
    );
}

#[test]
fn accepts_depth_events_without_canonical_changes() {
    let mut validator = depth_validator();
    validator.validate(&depth(0, 0, 2)).unwrap();
    validator.validate(&depth(1, 2, 0)).unwrap();
    validator.validate(&depth(2, 2, 2)).unwrap();
}

#[test]
fn validates_generated_trade_stream() {
    let root = tempfile::tempdir().unwrap();
    let inventory = build_trade_inventory(&[trade_publication(root.path(), 11)]).unwrap();
    let mut validator = UnifiedReferenceValidator::new(&inventory, &[]).unwrap();
    let mut stream = TradeStream::new(11, inventory).unwrap();
    for task in 0..2 {
        for ordinal in 0..2 {
            let event = stream.next_event().unwrap().unwrap();
            assert_eq!(
                (
                    event.dataset_id,
                    event.task_id,
                    event.event_ordinal,
                    event.canonical_row_offset
                ),
                (11, task, ordinal, ordinal)
            );
            validator
                .validate(&UnifiedTimelineRecord::from(&MergedEvent {
                    stream_rank: 0,
                    event: MergeEventRef::Trade(event),
                }))
                .unwrap();
        }
    }
    assert!(stream.next_event().unwrap().is_none());
}

#[test]
fn validates_generated_depth_stream() {
    let root = tempfile::tempdir().unwrap();
    let inventory = build_depth_inventory(&[depth_publication(root.path(), 12)]).unwrap();
    let boundaries = reconcile_depth_boundaries(&inventory).unwrap();
    let mut validator = UnifiedReferenceValidator::new(&[], &inventory).unwrap();
    let mut stream = DepthStream::new(12, inventory, &boundaries).unwrap();
    for task in 0..2 {
        for (ordinal, (offset, count)) in [(0, 1), (1, 1)].into_iter().enumerate() {
            let event = stream.next_event().unwrap().unwrap();
            assert_eq!(
                (
                    event.dataset_id,
                    event.task_id,
                    event.event.event_ordinal,
                    event.event.canonical_row_offset,
                    event.event.canonical_row_count
                ),
                (12, task, ordinal as u64, offset, count)
            );
            validator
                .validate(&UnifiedTimelineRecord::from(&MergedEvent {
                    stream_rank: 0,
                    event: MergeEventRef::Depth(event),
                }))
                .unwrap();
        }
    }
    assert!(stream.next_event().unwrap().is_none());
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn validates_real_trade_stream() {
    crate::recorded_references::validate_trade_references();
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn validates_real_depth_stream() {
    crate::recorded_references::validate_depth_references();
}
