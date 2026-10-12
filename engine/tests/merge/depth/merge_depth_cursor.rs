#![cfg(feature = "process")]

use std::path::PathBuf;

use marketforge_engine::process::merge::DepthEventCursor;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn reads_real_depth_event_index() {
    let path = project_root().join("data/processed/105/tasks/task-000000/depth/events.parquet");

    let mut cursor = DepthEventCursor::open(&path).expect("open depth event index");

    let first = cursor
        .next_event()
        .expect("read first event")
        .expect("missing first event");

    assert_eq!(first.canonical_row_offset, 0);
    assert_eq!(first.canonical_row_count, 400);

    println!("\nMARKETFORGE — DEPTH EVENT CURSOR");
    println!("First ordinal : {}", first.event_ordinal);
    println!("Timestamp     : {}", first.event_timestamp_ns);
    println!("Canonical rows: {}", first.canonical_row_count);
    println!("RESULT        : PASS");
}
