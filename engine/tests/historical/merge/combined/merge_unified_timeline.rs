#![cfg(feature = "historical")]

use arrow_array::Array;

use marketforge_engine::historical::merge::{
    MergeEventRef, MergedEvent, UnifiedTimelineBatchBuilder, UnifiedTimelineRecord,
};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use {
    marketforge_engine::historical::merge::DepthStream,
    marketforge_engine::historical::merge::MergeEventKind,
    marketforge_engine::historical::merge::ResolvedMergeInput,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::normalize::manifest::DatasetManifest,
};

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn resolve_depth_dataset(root: &Path, dataset_id: u64) -> ResolvedMergeInput {
    let dataset_path = root.join(format!("data/processed/{dataset_id}"));

    let manifest: DatasetManifest = serde_json::from_reader(
        File::open(dataset_path.join("manifest.json")).expect("open depth manifest"),
    )
    .expect("parse depth manifest");

    assert_eq!(manifest.dataset_id, dataset_id);

    let canonical_files = manifest
        .files
        .iter()
        .map(|file| dataset_path.join(&file.path))
        .collect();

    let auxiliary_files = manifest
        .auxiliary_files
        .iter()
        .map(|file| dataset_path.join(&file.path))
        .collect();

    ResolvedMergeInput {
        dataset_id,
        dataset_path,
        manifest,
        canonical_files,
        auxiliary_files,
    }
}

// -----------------------------------------------------------------------------
// Schema validation
// -----------------------------------------------------------------------------

#[test]
fn converts_real_depth_events_to_unified_records() {
    let root = project_root();

    let dataset_id = 112_u64;

    let input = resolve_depth_dataset(&root, dataset_id);

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

        assert!(record.canonical_row_count > 0);

        builder.push(record);

        records += 1;

        if records == 10_000 {
            break;
        }
    }

    assert_eq!(records, 10_000);

    let batch = builder.finish().unwrap().unwrap();

    assert_eq!(batch.num_rows(), records);

    let boundaries = batch.column_by_name("event_boundary").unwrap();

    let operations = batch.column_by_name("source_operation").unwrap();

    assert_eq!(boundaries.null_count(), 0);
    assert_eq!(operations.null_count(), 0);

    println!("Depth events : {} | metadata=PASS | Arrow=PASS", records);
}
