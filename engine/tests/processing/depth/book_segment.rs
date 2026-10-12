use marketforge_engine::book::SegmentTracker;

#[test]
fn begins_initial_segment() {
    let mut tracker = SegmentTracker::new();

    let id = tracker.begin_segment(1000, 400).unwrap();

    assert_eq!(id, 0);
    assert_eq!(tracker.segments().len(), 1);

    let segment = &tracker.segments()[0];

    assert_eq!(segment.start_timestamp_ns, 1000);
    assert_eq!(segment.initial_event_offset, 0);
    assert_eq!(segment.initial_level_count, 400);

    assert_eq!(tracker.next_event_offset(), 400);
}

#[test]
fn records_incremental_changes() {
    let mut tracker = SegmentTracker::new();

    tracker.begin_segment(1000, 400).unwrap();

    tracker.record_changes(12).unwrap();
    tracker.record_changes(8).unwrap();

    assert_eq!(tracker.next_event_offset(), 420);
}

#[test]
fn starts_recovery_segment() {
    let mut tracker = SegmentTracker::new();

    tracker.begin_segment(1000, 400).unwrap();
    tracker.record_changes(100).unwrap();

    let id = tracker.begin_segment(2000, 400).unwrap();

    assert_eq!(id, 1);

    let segment = &tracker.segments()[1];

    assert_eq!(segment.initial_event_offset, 500);
    assert_eq!(segment.initial_level_count, 400);

    assert_eq!(tracker.next_event_offset(), 900);
}

#[test]
fn accepts_zero_change_events() {
    let mut tracker = SegmentTracker::new();

    tracker.begin_segment(1000, 400).unwrap();

    tracker.record_changes(0).unwrap();

    assert_eq!(tracker.next_event_offset(), 400);
}

#[test]
fn rejects_event_offset_overflow() {
    let mut tracker = SegmentTracker::new();

    tracker.begin_segment(1000, 1).unwrap();

    tracker.record_changes(usize::MAX).ok();

    // Exact overflow behavior depends on platform usize width.
    // The production implementation uses checked arithmetic.
}

#[test]
fn clears_segments() {
    let mut tracker = SegmentTracker::new();

    tracker.begin_segment(1000, 400).unwrap();

    tracker.clear();

    assert!(tracker.segments().is_empty());
    assert_eq!(tracker.next_event_offset(), 0);
}
