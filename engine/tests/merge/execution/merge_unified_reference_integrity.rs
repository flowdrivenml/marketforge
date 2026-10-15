#![cfg(feature = "historical")]

use marketforge_engine::historical::merge::{
    DepthStream, MergeEventRef, MergedEvent, TradeStream, UnifiedReferenceValidator,
    UnifiedTimelineRecord, build_depth_inventory, build_trade_inventory,
    build_trade_inventory as build_trades, reconcile_depth_boundaries,
};

#[path = "../../common/mod.rs"]
mod common;

use common::datasets::{project_root, resolve_depth_dataset, resolve_trade_dataset};

// -----------------------------------------------------------------------------
// Synthetic record helpers
// -----------------------------------------------------------------------------

fn trade(dataset_id: u64, task_id: u64, ordinal: u64, offset: u64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: 100,
        stream_rank: 0,
        event_kind: 0,

        dataset_id,
        task_id,
        event_ordinal: ordinal,

        canonical_row_offset: offset,
        canonical_row_count: 1,

        event_boundary: None,
        source_operation: None,
    }
}

fn depth(
    dataset_id: u64,
    task_id: u64,
    ordinal: u64,
    offset: u64,
    count: u64,
) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: 100,
        stream_rank: 0,
        event_kind: 1,

        dataset_id,
        task_id,
        event_ordinal: ordinal,

        canonical_row_offset: offset,
        canonical_row_count: count,

        event_boundary: Some(0),
        source_operation: Some(1),
    }
}

// -----------------------------------------------------------------------------
// Real inventory helpers
// -----------------------------------------------------------------------------

fn trade_validator(dataset_id: u64) -> UnifiedReferenceValidator {
    let root = project_root();

    let input = resolve_trade_dataset(&root, dataset_id);

    let inventory = build_trade_inventory(&[input]).unwrap();

    UnifiedReferenceValidator::new(&inventory, &[]).unwrap()
}

fn depth_validator(dataset_id: u64) -> UnifiedReferenceValidator {
    let root = project_root();

    let input = resolve_depth_dataset(&root, dataset_id);

    let inventory = build_depth_inventory(&[input]).unwrap();

    UnifiedReferenceValidator::new(&[], &inventory).unwrap()
}

// -----------------------------------------------------------------------------
// Valid trade references
// -----------------------------------------------------------------------------

#[test]
fn accepts_valid_trade_references() {
    let mut validator = trade_validator(111);

    for ordinal in 0..100 {
        validator
            .validate(&trade(111, 0, ordinal, ordinal))
            .unwrap();
    }
}

// -----------------------------------------------------------------------------
// Duplicate trade identity
// -----------------------------------------------------------------------------

#[test]
fn rejects_duplicate_trade_identity() {
    let mut validator = trade_validator(111);

    let record = trade(111, 0, 10, 10);

    validator.validate(&record).unwrap();

    assert!(validator.validate(&record).is_err());
}

// -----------------------------------------------------------------------------
// Invalid task identity
// -----------------------------------------------------------------------------

#[test]
fn rejects_unknown_task_identity() {
    let mut validator = trade_validator(111);

    assert!(validator.validate(&trade(111, 999_999, 0, 0)).is_err());
}

// -----------------------------------------------------------------------------
// Out-of-bounds trade reference
// -----------------------------------------------------------------------------

#[test]
fn rejects_out_of_bounds_trade_offset() {
    let mut validator = trade_validator(111);

    assert!(validator.validate(&trade(111, 0, 0, u64::MAX)).is_err());
}

// -----------------------------------------------------------------------------
// Overlapping depth references
// -----------------------------------------------------------------------------

#[test]
fn rejects_overlapping_depth_ranges() {
    let mut validator = depth_validator(105);

    validator.validate(&depth(105, 0, 0, 0, 10)).unwrap();

    assert!(validator.validate(&depth(105, 0, 1, 5, 10)).is_err());
}

// -----------------------------------------------------------------------------
// Incorrect event kind
// -----------------------------------------------------------------------------

#[test]
fn rejects_task_event_kind_mismatch() {
    let mut validator = trade_validator(111);

    assert!(validator.validate(&depth(111, 0, 0, 0, 10)).is_err());
}

// -----------------------------------------------------------------------------
// Real trade stream validation
// -----------------------------------------------------------------------------

#[test]
fn validates_real_trade_stream() {
    let root = project_root();

    let dataset_id = 111;

    let input = resolve_trade_dataset(&root, dataset_id);

    let inventory = build_trades(&[input]).unwrap();

    let mut validator = UnifiedReferenceValidator::new(&inventory, &[]).unwrap();

    let mut stream = TradeStream::new(dataset_id, inventory).unwrap();

    let mut count = 0_u64;

    while let Some(event) = stream.next_event().unwrap() {
        let merged = MergedEvent {
            stream_rank: 0,
            event: MergeEventRef::Trade(event),
        };

        let record = UnifiedTimelineRecord::from(&merged);

        validator.validate(&record).unwrap();

        count += 1;
    }

    assert_eq!(count, 3_554_747);

    println!("Real trade references : {count} | PASS");
}

// -----------------------------------------------------------------------------
// Real depth stream validation
// -----------------------------------------------------------------------------

#[test]
fn validates_real_depth_stream() {
    let root = project_root();

    let dataset_id = 105;

    let input = resolve_depth_dataset(&root, dataset_id);

    let inventory = build_depth_inventory(&[input]).unwrap();

    let boundaries = reconcile_depth_boundaries(&inventory).unwrap();

    let mut validator = UnifiedReferenceValidator::new(&[], &inventory).unwrap();

    let mut stream = DepthStream::new(dataset_id, inventory, &boundaries).unwrap();

    let mut count = 0_u64;

    while let Some(event) = stream.next_event().unwrap() {
        let merged = MergedEvent {
            stream_rank: 0,
            event: MergeEventRef::Depth(event),
        };

        let record = UnifiedTimelineRecord::from(&merged);

        validator.validate(&record).unwrap();

        count += 1;
    }

    assert!(count > 0);

    println!("Real depth references : {count} | PASS");
}

#[test]
fn accepts_depth_events_without_canonical_changes() {
    let mut validator = depth_validator(105);

    validator.validate(&depth(105, 0, 0, 0, 10)).unwrap();

    // Event exists but emits no canonical rows.
    validator.validate(&depth(105, 0, 1, 10, 0)).unwrap();

    // Subsequent event resumes canonical row emission.
    validator.validate(&depth(105, 0, 2, 10, 5)).unwrap();
}
