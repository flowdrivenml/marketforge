#![cfg(feature = "historical")]

use std::path::PathBuf;

use marketforge_engine::historical::merge::DepthEventCursor;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn reads_depth_events_within_requested_range() {
    let root = project_root();

    let path = root.join("data/processed/112/tasks/task-000000/depth/events.parquet");

    let start_ns = 1788220800000000000_i64;
    let end_ns = start_ns + 60_000_000_000;

    let mut cursor =
        DepthEventCursor::open_range(&path, start_ns, end_ns).expect("open ranged depth cursor");

    let mut count = 0_u64;
    let mut previous_timestamp = None;

    while let Some(event) = cursor.next_event().expect("read event") {
        assert!(event.event_timestamp_ns >= start_ns);
        assert!(event.event_timestamp_ns < end_ns);

        if let Some(previous) = previous_timestamp {
            assert!(event.event_timestamp_ns >= previous);
        }

        previous_timestamp = Some(event.event_timestamp_ns);

        count += 1;
    }

    assert!(count > 0);

    println!("\nMARKETFORGE — DEPTH CURSOR RANGE");
    println!("Events : {}", count);
    println!("Range  : [{start_ns}, {end_ns})");
    println!("Result : PASS");
}

#[test]
fn rejects_invalid_range() {
    let root = project_root();

    let path = root.join("data/processed/112/tasks/task-000000/depth/events.parquet");

    assert!(DepthEventCursor::open_range(&path, 100, 100).is_err());

    assert!(DepthEventCursor::open_range(&path, 200, 100).is_err());
}
