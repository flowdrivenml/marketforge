use crate::{
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};

// -----------------------------------------------------------------------------
// Sequence policy
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequencePolicy {
    Consecutive,
    Ranged,
    Unsequenced,
}

// -----------------------------------------------------------------------------
// Sequence tracker
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SequenceTracker {
    policy: SequencePolicy,
    last_sequence: Option<u64>,
    synchronized: bool,
}

impl SequenceTracker {
    pub fn new(policy: SequencePolicy) -> Self {
        Self {
            policy,
            last_sequence: None,
            synchronized: false,
        }
    }

    pub fn is_synchronized(&self) -> bool {
        self.synchronized
    }

    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    /// Establish synchronization from an authoritative snapshot.
    ///
    /// A snapshot may reset the source sequence.
    /// Establish synchronization from an authoritative snapshot.
    pub fn initialize(&mut self, sequence: Option<u64>) -> Result<()> {
        if matches!(
            self.policy,
            SequencePolicy::Consecutive | SequencePolicy::Ranged
        ) && sequence.is_none()
        {
            return invalid("sequenced snapshot requires an update ID");
        }

        self.last_sequence = sequence;
        self.synchronized = true;

        Ok(())
    }

    /// Validate an incremental update without modifying tracker state.
    pub fn validate_update(&self, sequence: Option<u64>) -> Result<()> {
        self.validate_update_range(sequence, None)
    }

    /// Validate a sequence range without modifying tracker state.
    ///
    /// `count` is the number of consecutive source sequence IDs covered.
    pub fn validate_update_range(&self, sequence: Option<u64>, count: Option<u64>) -> Result<()> {
        if !self.synchronized {
            return invalid_category(
                IntegrityCategory::MissingSnapshot,
                "cannot apply depth update before synchronization",
            );
        }

        match self.policy {
            SequencePolicy::Unsequenced => Ok(()),

            SequencePolicy::Consecutive | SequencePolicy::Ranged => {
                let previous = self.last_sequence.ok_or_else(|| {
                    integrity_error(
                        IntegrityCategory::SequenceGap,
                        "previous depth sequence is missing",
                    )
                })?;

                let current = sequence.ok_or_else(|| {
                    integrity_error(
                        IntegrityCategory::SequenceGap,
                        "current depth sequence is missing",
                    )
                })?;

                if self.policy == SequencePolicy::Ranged {
                    match count {
                        Some(0) | None => {
                            return invalid_category(
                                IntegrityCategory::InvalidRecord,
                                "ranged depth update requires positive sequence count",
                            );
                        }
                        Some(_) => {}
                    }
                }

                let expected = previous.checked_add(1).ok_or_else(|| {
                    integrity_error(IntegrityCategory::SequenceGap, "depth sequence overflow")
                })?;

                if current != expected {
                    return invalid_category(
                        IntegrityCategory::SequenceGap,
                        format!(
                            "depth sequence discontinuity: expected {expected}, received {current}"
                        ),
                    );
                }

                if self.policy == SequencePolicy::Ranged {
                    let count = count.unwrap();

                    current.checked_add(count - 1).ok_or_else(|| {
                        integrity_error(
                            IntegrityCategory::SequenceGap,
                            "depth sequence range overflow",
                        )
                    })?;
                }

                Ok(())
            }
        }
    }

    /// Commit a previously validated update.
    pub fn commit_update(&mut self, sequence: Option<u64>) {
        self.commit_update_range(sequence, None);
    }

    /// Commit a previously validated sequence range.
    pub fn commit_update_range(&mut self, sequence: Option<u64>, count: Option<u64>) {
        self.last_sequence = match (self.policy, sequence, count) {
            (SequencePolicy::Ranged, Some(start), Some(count)) if count > 0 => {
                start.checked_add(count - 1)
            }

            _ => sequence,
        };
    }

    /// Invalidate synchronization after an unrecoverable sequence gap.
    pub fn invalidate(&mut self) {
        self.synchronized = false;
        self.last_sequence = None;
    }

    pub fn clear(&mut self) {
        self.invalidate();
    }
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn integrity_error(category: IntegrityCategory, message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category,
        message: message.into(),
    }
}

fn invalid_category<T>(category: IntegrityCategory, message: impl Into<String>) -> Result<T> {
    Err(integrity_error(category, message))
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    invalid_category(IntegrityCategory::InvalidRecord, message)
}
