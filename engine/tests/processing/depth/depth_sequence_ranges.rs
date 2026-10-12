use marketforge_engine::book::{SequencePolicy, SequenceTracker};

#[test]
fn accepts_consecutive_ranges() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Ranged);

    tracker.initialize(Some(99)).unwrap();

    tracker.validate_update_range(Some(100), Some(3)).unwrap();

    tracker.commit_update_range(Some(100), Some(3));

    assert_eq!(tracker.last_sequence(), Some(102));

    tracker.validate_update_range(Some(103), Some(2)).unwrap();

    tracker.commit_update_range(Some(103), Some(2));

    assert_eq!(tracker.last_sequence(), Some(104));
}

#[test]
fn rejects_sequence_gap() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Ranged);

    tracker.initialize(Some(99)).unwrap();

    assert!(tracker.validate_update_range(Some(101), Some(1)).is_err());

    assert_eq!(tracker.last_sequence(), Some(99));
}

#[test]
fn rejects_overlapping_ranges() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Ranged);

    tracker.initialize(Some(99)).unwrap();

    tracker.validate_update_range(Some(100), Some(3)).unwrap();

    tracker.commit_update_range(Some(100), Some(3));

    assert!(tracker.validate_update_range(Some(102), Some(2)).is_err());

    assert_eq!(tracker.last_sequence(), Some(102));
}

#[test]
fn rejects_zero_sequence_count() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Ranged);

    tracker.initialize(Some(99)).unwrap();

    assert!(tracker.validate_update_range(Some(100), Some(0)).is_err());
}

#[test]
fn rejects_sequence_overflow() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Ranged);

    tracker.initialize(Some(u64::MAX - 2)).unwrap();

    assert!(
        tracker
            .validate_update_range(Some(u64::MAX - 1), Some(3))
            .is_err()
    );
}

#[test]
fn preserves_consecutive_policy() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    tracker.initialize(Some(100)).unwrap();

    tracker.validate_update(Some(101)).unwrap();
    tracker.commit_update(Some(101));

    assert_eq!(tracker.last_sequence(), Some(101));

    assert!(tracker.validate_update(Some(103)).is_err());
}

#[test]
fn preserves_unsequenced_policy() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Unsequenced);

    tracker.initialize(None).unwrap();

    tracker.validate_update(None).unwrap();
    tracker.commit_update(None);

    assert!(tracker.is_synchronized());
}
