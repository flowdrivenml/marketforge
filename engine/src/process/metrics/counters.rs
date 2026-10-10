use serde::{Deserialize, Serialize};

use crate::error::{MarketForgeError, Result};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingCounters {
    pub records_read: u64,
    pub records_matched: u64,
    pub records_skipped_instrument: u64,
    pub records_rejected: u64,

    pub events_normalized: u64,
    pub events_written: u64,

    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub records_rejected_unmatched: u64,
    pub records_processed: u64,
}

impl ProcessingCounters {
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        let mut merged = self.clone();

        macro_rules! add {
            ($field:ident) => {
                merged.$field = merged.$field.checked_add(other.$field).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "processing counter overflow: {}",
                        stringify!($field)
                    ))
                })?;
            };
        }

        add!(records_read);
        add!(records_matched);
        add!(records_skipped_instrument);
        add!(records_rejected);

        add!(events_normalized);
        add!(events_written);

        add!(tasks_completed);
        add!(tasks_failed);
        add!(records_rejected_unmatched);
        add!(records_processed);

        *self = merged;

        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.records_rejected_unmatched > self.records_rejected {
            return Err(MarketForgeError::InvalidConfiguration(
                "unmatched rejected records exceed total rejected records".to_owned(),
            ));
        }

        let matched_rejected = self
            .records_rejected
            .checked_sub(self.records_rejected_unmatched)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "invalid rejected-record accounting".to_owned(),
                )
            })?;

        let classified = self
            .records_skipped_instrument
            .checked_add(self.records_matched)
            .and_then(|value| value.checked_add(self.records_rejected_unmatched))
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "source-record accounting overflow".to_owned(),
                )
            })?;

        if self.records_read != classified {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "source-record accounting mismatch: read={}, classified={}",
                self.records_read, classified
            )));
        }

        let matched_accounted = matched_rejected
            .checked_add(self.records_processed)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "matched-record accounting overflow".to_owned(),
                )
            })?;

        if self.records_matched != matched_accounted {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "matched-record accounting mismatch: matched={}, accounted={}",
                self.records_matched, matched_accounted
            )));
        }

        if self.events_normalized != self.events_written {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "normalized/written event mismatch: normalized={}, written={}",
                self.events_normalized, self.events_written
            )));
        }

        Ok(())
    }
}

#[test]
fn validates_unmatched_parse_failures() {
    let counters = ProcessingCounters {
        records_read: 10,
        records_matched: 5,
        records_skipped_instrument: 4,
        records_rejected: 2,
        records_rejected_unmatched: 1,

        records_processed: 4,

        events_normalized: 4,
        events_written: 4,

        tasks_completed: 1,
        tasks_failed: 0,
    };

    counters.validate().unwrap();
}

#[test]
fn validates_one_to_many_depth_processing() {
    let counters = ProcessingCounters {
        records_read: 100,
        records_matched: 100,
        records_skipped_instrument: 0,
        records_rejected: 0,
        records_rejected_unmatched: 0,

        records_processed: 100,

        events_normalized: 1500,
        events_written: 1500,

        tasks_completed: 1,
        tasks_failed: 0,
    };

    counters.validate().unwrap();
}

#[test]
fn validates_depth_records_without_emitted_changes() {
    let counters = ProcessingCounters {
        records_read: 100,
        records_matched: 100,
        records_skipped_instrument: 0,
        records_rejected: 0,
        records_rejected_unmatched: 0,

        records_processed: 100,

        events_normalized: 0,
        events_written: 0,

        tasks_completed: 1,
        tasks_failed: 0,
    };

    counters.validate().unwrap();
}
