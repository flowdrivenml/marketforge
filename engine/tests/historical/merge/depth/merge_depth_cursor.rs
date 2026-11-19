use crate::publication::depth_publication;
use marketforge_engine::historical::merge::{DepthEventCursor, build_depth_inventory};
use marketforge_engine::shared::domain::canonical::DepthEventBoundary;

#[test]
fn reads_generated_depth_event_index() {
    let root = tempfile::tempdir().unwrap();
    let inventory = build_depth_inventory(&[depth_publication(root.path(), 12)]).unwrap();
    let mut cursor =
        DepthEventCursor::open_source(&inventory[1].events_path, 0, 2, 0, None).unwrap();
    for (ordinal, (timestamp, boundary)) in [
        (300, DepthEventBoundary::Initialization),
        (400, DepthEventBoundary::Changes),
    ]
    .into_iter()
    .enumerate()
    {
        let event = cursor.next_event().unwrap().unwrap();
        assert_eq!(
            (
                event.event_ordinal,
                event.event_timestamp_ns,
                event.canonical_row_offset,
                event.canonical_row_count,
                event.event_boundary
            ),
            (ordinal as u64, timestamp, ordinal as u64, 1, boundary)
        );
    }
    assert!(cursor.next_event().unwrap().is_none());
}
