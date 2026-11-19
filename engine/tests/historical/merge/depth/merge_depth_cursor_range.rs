use crate::depth_index::write_index;
use marketforge_engine::historical::merge::DepthEventCursor;

#[test]
fn reads_depth_events_within_requested_range() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.parquet");
    let expected = write_index(&path, &[10, 12, 19, 14, 18]);
    let mut cursor = DepthEventCursor::open_source(&path, 0, 5, 0, Some((12, 18))).unwrap();
    assert_eq!(cursor.next_event().unwrap(), Some(expected[1].clone()));
    assert_eq!(cursor.next_event().unwrap(), Some(expected[3].clone()));
    assert_eq!(cursor.next_event().unwrap(), None);
}

#[test]
fn rejects_invalid_range() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unused");
    assert!(
        DepthEventCursor::open_range(&path, 100, 100)
            .err()
            .expect("reject invalid interval")
            .to_string()
            .contains("invalid depth event range")
    );
    assert!(
        DepthEventCursor::open_range(&path, 200, 100)
            .err()
            .expect("reject invalid interval")
            .to_string()
            .contains("invalid depth event range")
    );
}
