#![cfg(feature = "historical")]
use crate::publication::depth_publication;

use arrow_array::Array;

use marketforge_engine::historical::merge::{
    MergeEventRef, MergedEvent, UnifiedTimelineBatchBuilder, UnifiedTimelineRecord,
};
use {
    marketforge_engine::historical::merge::DepthStream,
    marketforge_engine::historical::merge::MergeEventKind,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
};

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Schema validation
// -----------------------------------------------------------------------------

#[test]
fn converts_depth_events_to_unified_records() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let dataset_id = 112_u64;

    let input = depth_publication(&root, dataset_id);

    let inventory = build_depth_inventory(&[input]).expect("build depth inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile depth boundaries");

    let mut stream =
        DepthStream::new(dataset_id, inventory, &boundaries).expect("construct depth stream");

    let mut builder = UnifiedTimelineBatchBuilder::new();

    let mut records = 0_usize;

    while let Some(depth) = stream.next_event().expect("read depth event") {
        let merged = MergedEvent {
            stream_rank: 0,
            event: MergeEventRef::Depth(depth),
        };

        let record = UnifiedTimelineRecord::from(&merged);

        assert_eq!(record.event_kind, MergeEventKind::Depth as u8);
        assert_eq!(record.dataset_id, dataset_id);

        assert!(record.event_boundary.is_some());
        assert!(record.source_operation.is_some());

        assert!([0, 1, 2].contains(&record.canonical_row_count));

        builder.push(record);

        records += 1;

        if records == 8 {
            break;
        }
    }

    assert_eq!(records, 4);

    let batch = builder.finish().unwrap().unwrap();

    assert_eq!(batch.num_rows(), records);

    let boundaries = batch.column_by_name("event_boundary").unwrap();

    let operations = batch.column_by_name("source_operation").unwrap();

    assert_eq!(boundaries.null_count(), 0);
    assert_eq!(operations.null_count(), 0);

    println!("Depth events : {} | metadata=PASS | Arrow=PASS", records);
}
