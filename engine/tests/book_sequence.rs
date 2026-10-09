use marketforge_engine::book::{SequencePolicy, SequenceTracker};

use marketforge_engine::{error::MarketForgeError, job::IntegrityCategory};

#[test]
fn initializes_from_snapshot() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    tracker.initialize(Some(100)).unwrap();

    assert!(tracker.is_synchronized());
    assert_eq!(tracker.last_sequence(), Some(100));
}

#[test]
fn accepts_consecutive_updates() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    tracker.initialize(Some(100)).unwrap();

    tracker.validate_update(Some(101)).unwrap();
    tracker.commit_update(Some(101));

    tracker.validate_update(Some(102)).unwrap();
    tracker.commit_update(Some(102));

    assert_eq!(tracker.last_sequence(), Some(102));
}

#[test]
fn detects_sequence_gap() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    tracker.initialize(Some(100)).unwrap();

    let error = tracker.validate_update(Some(103)).unwrap_err();

    assert!(matches!(
        error,
        MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::SequenceGap,
            ..
        }
    ));

    // Validation does not commit a failed sequence.
    assert_eq!(tracker.last_sequence(), Some(100));
}

#[test]
fn rejects_duplicate_sequence() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    tracker.initialize(Some(100)).unwrap();

    assert!(tracker.validate_update(Some(100)).is_err());
}

#[test]
fn rejects_update_before_snapshot() {
    let tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    let error = tracker.validate_update(Some(101)).unwrap_err();

    assert!(matches!(
        error,
        MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::MissingSnapshot,
            ..
        }
    ));
}

#[test]
fn rejects_missing_sequence() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    assert!(tracker.initialize(None).is_err());

    tracker.initialize(Some(100)).unwrap();

    assert!(tracker.validate_update(None).is_err());
}

#[test]
fn accepts_snapshot_reset() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    tracker.initialize(Some(100)).unwrap();

    tracker.validate_update(Some(101)).unwrap();
    tracker.commit_update(Some(101));

    // A new authoritative snapshot establishes a new baseline.
    tracker.initialize(Some(500)).unwrap();

    assert_eq!(tracker.last_sequence(), Some(500));

    tracker.validate_update(Some(501)).unwrap();
}

#[test]
fn accepts_unsequenced_updates() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Unsequenced);

    tracker.initialize(None).unwrap();

    tracker.validate_update(None).unwrap();
    tracker.commit_update(None);

    assert!(tracker.is_synchronized());
}

#[test]
fn invalidation_requires_new_snapshot() {
    let mut tracker = SequenceTracker::new(SequencePolicy::Consecutive);

    tracker.initialize(Some(100)).unwrap();

    tracker.invalidate();

    assert!(!tracker.is_synchronized());
    assert!(tracker.validate_update(Some(101)).is_err());

    tracker.initialize(Some(200)).unwrap();

    assert!(tracker.validate_update(Some(201)).is_ok());
}
