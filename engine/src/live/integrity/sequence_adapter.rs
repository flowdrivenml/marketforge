use thiserror::Error;

use crate::{
    shared::domain::book::{SequencePolicy, SequenceTracker},
    shared::domain::canonical::SequenceMetadata,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SequenceAdapterError {
    #[error("unsupported sequence metadata: {0}")]
    Unsupported(String),

    #[error("missing required sequence field: {0}")]
    Missing(&'static str),

    #[error("invalid sequence range: first={first}, last={last}")]
    InvalidRange { first: u64, last: u64 },

    #[error("sequence range length overflow")]
    RangeOverflow,

    #[error("sequence validation failed: {0}")]
    Validation(String),

    #[error("prepared sequence update is no longer current")]
    StalePreparation,
}

// ============================================================
// SUPPORTED COMPATIBILITY MODES
// ============================================================

/// The subset of native continuity semantics supported
/// by the existing shared SequenceTracker.
///
/// Additional exchange-specific policies must be handled
/// by separate adapters or explicitly extended primitives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacySequenceMode {
    /// Each update has exactly one sequence identifier.
    /// The next identifier must equal the previous + 1.
    Consecutive,

    /// Each update covers a consecutive [first, last] range.
    /// The next range must start at the previous end + 1.
    Ranged,
}

impl LegacySequenceMode {
    fn policy(self) -> SequencePolicy {
        match self {
            Self::Consecutive => SequencePolicy::Consecutive,
            Self::Ranged => SequencePolicy::Ranged,
        }
    }
}

// ============================================================
// PREPARED UPDATE
// ============================================================

/// A sequence transition that has passed validation
/// but has not yet advanced the authoritative tracker.
///
/// Its fields are private: callers cannot fabricate a
/// prepared transition using a struct literal.
#[derive(Debug)]
pub struct PreparedSequenceUpdate {
    previous: Option<u64>,
    first: u64,
    count: Option<u64>,
}

// ============================================================
// ADAPTER
// ============================================================

/// Live adapter around the existing shared SequenceTracker.
///
/// This adapter does not:
///
/// - Own an order book.
/// - Apply canonical L2 levels.
/// - Perform snapshot bridging.
/// - Handle reconnections.
/// - Detect transport interruptions.
/// - Record integrity gaps.
/// - Publish events.
///
/// The stream-level controller remains responsible for
/// those operations.
#[derive(Debug)]
pub struct BookSequenceAdapter {
    mode: LegacySequenceMode,
    tracker: SequenceTracker,
}

impl BookSequenceAdapter {
    pub fn new(mode: LegacySequenceMode) -> Self {
        Self {
            mode,
            tracker: SequenceTracker::new(mode.policy()),
        }
    }

    pub fn mode(&self) -> LegacySequenceMode {
        self.mode
    }

    pub fn is_synchronized(&self) -> bool {
        self.tracker.is_synchronized()
    }

    pub fn last_sequence(&self) -> Option<u64> {
        self.tracker.last_sequence()
    }

    // ========================================================
    // AUTHORITATIVE SNAPSHOT
    // ========================================================

    /// Establish the sequence baseline from an already
    /// validated authoritative snapshot.
    ///
    /// This does not establish snapshot authenticity,
    /// stream-generation validity, or book-state validity.
    ///
    /// The caller must complete those checks first.
    pub fn initialize_from_snapshot(
        &mut self,
        sequence: &SequenceMetadata,
    ) -> Result<(), SequenceAdapterError> {
        if sequence.sequence_previous.is_some() || sequence.cross_sequence.is_some() {
            return Err(SequenceAdapterError::Unsupported(
                "snapshot contains sequence metadata not \
                 interpreted by the legacy tracker"
                    .into(),
            ));
        }

        let last = sequence
            .sequence_last
            .ok_or(SequenceAdapterError::Missing("sequence_last"))?;

        if let Some(first) = sequence.sequence_first {
            if first > last {
                return Err(SequenceAdapterError::InvalidRange { first, last });
            }
        }

        self.tracker
            .initialize(Some(last))
            .map_err(|error| SequenceAdapterError::Validation(error.to_string()))
    }

    // ========================================================
    // PREPARE UPDATE
    // ========================================================

    /// Validate an incremental update without changing
    /// accepted sequence state.
    ///
    /// A successful result does not authorize publication.
    pub fn prepare_update(
        &self,
        sequence: &SequenceMetadata,
    ) -> Result<PreparedSequenceUpdate, SequenceAdapterError> {
        if sequence.sequence_previous.is_some() || sequence.cross_sequence.is_some() {
            return Err(SequenceAdapterError::Unsupported(
                "previous/cross sequence fields require \
                 a different continuity strategy"
                    .into(),
            ));
        }

        let (first, count) = match self.mode {
            LegacySequenceMode::Consecutive => {
                if sequence.sequence_first.is_some() {
                    return Err(SequenceAdapterError::Unsupported(
                        "consecutive mode does not accept \
                         sequence_first"
                            .into(),
                    ));
                }

                let current = sequence
                    .sequence_last
                    .ok_or(SequenceAdapterError::Missing("sequence_last"))?;

                (current, None)
            }

            LegacySequenceMode::Ranged => {
                let first = sequence
                    .sequence_first
                    .ok_or(SequenceAdapterError::Missing("sequence_first"))?;

                let last = sequence
                    .sequence_last
                    .ok_or(SequenceAdapterError::Missing("sequence_last"))?;

                if first > last {
                    return Err(SequenceAdapterError::InvalidRange { first, last });
                }

                let count = last
                    .checked_sub(first)
                    .and_then(|difference| difference.checked_add(1))
                    .ok_or(SequenceAdapterError::RangeOverflow)?;

                (first, Some(count))
            }
        };

        self.tracker
            .validate_update_range(Some(first), count)
            .map_err(|error| SequenceAdapterError::Validation(error.to_string()))?;

        Ok(PreparedSequenceUpdate {
            previous: self.tracker.last_sequence(),
            first,
            count,
        })
    }

    // ========================================================
    // COMMIT UPDATE
    // ========================================================

    /// Advance the accepted sequence after the owning
    /// stream controller has successfully accepted the
    /// corresponding complete L2 operation.
    ///
    /// This method does not perform publication or
    /// persistence.
    pub fn commit_update(
        &mut self,
        prepared: PreparedSequenceUpdate,
    ) -> Result<(), SequenceAdapterError> {
        if !self.tracker.is_synchronized() || self.tracker.last_sequence() != prepared.previous {
            return Err(SequenceAdapterError::StalePreparation);
        }

        // Validate again to guard against changes to the
        // tracker between preparation and commit.
        self.tracker
            .validate_update_range(Some(prepared.first), prepared.count)
            .map_err(|_| SequenceAdapterError::StalePreparation)?;

        self.tracker
            .commit_update_range(Some(prepared.first), prepared.count);

        Ok(())
    }

    // ========================================================
    // INVALIDATION
    // ========================================================

    pub fn invalidate(&mut self) {
        self.tracker.invalidate();
    }
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(last: u64) -> SequenceMetadata {
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(last),
            sequence_previous: None,
            cross_sequence: None,
        }
    }

    fn consecutive(current: u64) -> SequenceMetadata {
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(current),
            sequence_previous: None,
            cross_sequence: None,
        }
    }

    fn ranged(first: u64, last: u64) -> SequenceMetadata {
        SequenceMetadata {
            sequence_first: Some(first),
            sequence_last: Some(last),
            sequence_previous: None,
            cross_sequence: None,
        }
    }

    #[test]
    fn consecutive_update_advances_sequence() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Consecutive);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        let prepared = adapter.prepare_update(&consecutive(101)).unwrap();

        // Preparation must never mutate accepted state.
        assert_eq!(adapter.last_sequence(), Some(100));

        adapter.commit_update(prepared).unwrap();

        assert_eq!(adapter.last_sequence(), Some(101));
    }

    #[test]
    fn rejects_consecutive_gap_without_advancing() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Consecutive);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        assert!(adapter.prepare_update(&consecutive(103)).is_err());

        assert_eq!(adapter.last_sequence(), Some(100));
    }

    #[test]
    fn ranged_update_commits_last_sequence() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Ranged);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        let prepared = adapter.prepare_update(&ranged(101, 105)).unwrap();

        assert_eq!(adapter.last_sequence(), Some(100));

        adapter.commit_update(prepared).unwrap();

        assert_eq!(adapter.last_sequence(), Some(105));
    }

    #[test]
    fn rejects_noncontiguous_range() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Ranged);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        assert!(adapter.prepare_update(&ranged(103, 105)).is_err());

        assert_eq!(adapter.last_sequence(), Some(100));
    }

    #[test]
    fn rejects_invalid_range() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Ranged);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        assert!(matches!(
            adapter.prepare_update(&ranged(105, 101)),
            Err(SequenceAdapterError::InvalidRange { .. })
        ));
    }

    #[test]
    fn rejects_unsupported_previous_sequence() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Consecutive);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        let mut update = consecutive(101);
        update.sequence_previous = Some(100);

        assert!(matches!(
            adapter.prepare_update(&update),
            Err(SequenceAdapterError::Unsupported(_))
        ));
    }

    #[test]
    fn rejects_update_before_initialization() {
        let adapter = BookSequenceAdapter::new(LegacySequenceMode::Consecutive);

        assert!(adapter.prepare_update(&consecutive(1)).is_err());
    }

    #[test]
    fn invalidation_requires_new_snapshot() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Consecutive);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        adapter.invalidate();

        assert!(!adapter.is_synchronized());

        assert!(adapter.prepare_update(&consecutive(101)).is_err());
    }

    #[test]
    fn stale_prepared_update_cannot_commit() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Consecutive);

        adapter.initialize_from_snapshot(&snapshot(100)).unwrap();

        let stale = adapter.prepare_update(&consecutive(101)).unwrap();

        adapter.invalidate();

        assert!(matches!(
            adapter.commit_update(stale),
            Err(SequenceAdapterError::StalePreparation)
        ));
    }

    #[test]
    fn supports_maximum_sequence_without_overflow() {
        let mut adapter = BookSequenceAdapter::new(LegacySequenceMode::Consecutive);

        adapter
            .initialize_from_snapshot(&snapshot(u64::MAX))
            .unwrap();

        assert!(adapter.prepare_update(&consecutive(0)).is_err());
    }
}
