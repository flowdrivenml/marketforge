#![cfg(feature = "process")]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    book::BookSegment,
    process::parquet::{DepthSegmentManifest, read_depth_segments, write_depth_segments},
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn temp_directory() -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    let path = std::env::temp_dir().join(format!(
        "marketforge-depth-segments-{}-{id}",
        std::process::id()
    ));

    fs::create_dir_all(&path).unwrap();

    path
}

#[test]
fn persists_and_loads_segment_manifest() {
    let directory = temp_directory();

    let manifest = DepthSegmentManifest::new(
        950,
        vec![
            BookSegment {
                segment_id: 0,
                start_timestamp_ns: 1000,
                initial_event_offset: 0,
                initial_level_count: 400,
            },
            BookSegment {
                segment_id: 1,
                start_timestamp_ns: 2000,
                initial_event_offset: 500,
                initial_level_count: 400,
            },
        ],
    )
    .unwrap();

    write_depth_segments(&directory, &manifest).unwrap();

    let loaded = read_depth_segments(&directory).unwrap();

    assert_eq!(loaded, manifest);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn rejects_overlapping_segments() {
    let result = DepthSegmentManifest::new(
        1000,
        vec![
            BookSegment {
                segment_id: 0,
                start_timestamp_ns: 1000,
                initial_event_offset: 0,
                initial_level_count: 400,
            },
            BookSegment {
                segment_id: 1,
                start_timestamp_ns: 2000,
                initial_event_offset: 300,
                initial_level_count: 400,
            },
        ],
    );

    assert!(result.is_err());
}

#[test]
fn rejects_out_of_bounds_initialization() {
    let result = DepthSegmentManifest::new(
        100,
        vec![BookSegment {
            segment_id: 0,
            start_timestamp_ns: 1000,
            initial_event_offset: 0,
            initial_level_count: 400,
        }],
    );

    assert!(result.is_err());
}

#[test]
fn rejects_missing_initialization() {
    let result = DepthSegmentManifest::new(100, vec![]);

    assert!(result.is_err());
}
