use crate::depth_index::write_index;
use marketforge_engine::historical::merge::DepthEventCursor;

#[test]
fn indexed_cursor_matches_sequential_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("events.parquet");
    let events = write_index(&path, &[10, 12, 12, 14, 18, 19]);
    for (start, end, indices) in [
        (10, 12, 0..1),
        (12, 18, 1..4),
        (18, 20, 4..6),
        (20, 21, 6..6),
    ] {
        let mut sequential = DepthEventCursor::open_range(&path, start, end).unwrap();
        let mut indexed = DepthEventCursor::open_indexed_range(&path, start, end).unwrap();
        for expected in &events[indices] {
            assert_eq!(indexed.next_event().unwrap().as_ref(), Some(expected));
            assert_eq!(sequential.next_event().unwrap().as_ref(), Some(expected));
        }
        assert!(indexed.next_event().unwrap().is_none());
        assert!(sequential.next_event().unwrap().is_none());
    }
}
