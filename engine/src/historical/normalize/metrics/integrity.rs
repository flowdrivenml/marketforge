use serde::{Deserialize, Serialize};

use crate::{
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};

pub const DEFAULT_MAX_DIAGNOSTICS: usize = 20;
pub const MAX_DIAGNOSTIC_MESSAGE_BYTES: usize = 1024;

// -----------------------------------------------------------------------------
// Integrity counters
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityCounters {
    pub parse_failure: u64,
    pub invalid_record: u64,
    pub sequence_gap: u64,
    pub timestamp_regression: u64,
    pub missing_snapshot: u64,
    pub invalid_book: u64,
    pub transformation_failure: u64,
}

impl IntegrityCounters {
    pub fn increment(&mut self, category: IntegrityCategory) -> Result<u64> {
        let counter = match category {
            IntegrityCategory::ParseFailure => &mut self.parse_failure,
            IntegrityCategory::InvalidRecord => &mut self.invalid_record,
            IntegrityCategory::SequenceGap => &mut self.sequence_gap,
            IntegrityCategory::TimestampRegression => &mut self.timestamp_regression,
            IntegrityCategory::MissingSnapshot => &mut self.missing_snapshot,
            IntegrityCategory::InvalidBook => &mut self.invalid_book,
            IntegrityCategory::TransformationFailure => &mut self.transformation_failure,
        };

        *counter = counter.checked_add(1).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("integrity counter overflow".to_owned())
        })?;

        Ok(*counter)
    }

    pub fn total(&self) -> Result<u64> {
        [
            self.parse_failure,
            self.invalid_record,
            self.sequence_gap,
            self.timestamp_regression,
            self.missing_snapshot,
            self.invalid_book,
            self.transformation_failure,
        ]
        .into_iter()
        .try_fold(0u64, |total, count| {
            total.checked_add(count).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("integrity total overflow".to_owned())
            })
        })
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        let mut merged = self.clone();

        macro_rules! add {
            ($field:ident) => {
                merged.$field = merged.$field.checked_add(other.$field).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "integrity counter overflow: {}",
                        stringify!($field)
                    ))
                })?;
            };
        }

        add!(parse_failure);
        add!(invalid_record);
        add!(sequence_gap);
        add!(timestamp_regression);
        add!(missing_snapshot);
        add!(invalid_book);
        add!(transformation_failure);

        *self = merged;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Diagnostic records
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityDiagnostic {
    pub category: IntegrityCategory,

    pub task_id: u64,
    pub source_record: Option<u64>,
    pub event_timestamp_ns: Option<i64>,

    pub message: String,
}

// -----------------------------------------------------------------------------
// Integrity metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityMetrics {
    pub counters: IntegrityCounters,
    pub diagnostics: Vec<IntegrityDiagnostic>,

    pub max_diagnostics: usize,
}

impl Default for IntegrityMetrics {
    fn default() -> Self {
        Self {
            counters: IntegrityCounters::default(),
            diagnostics: Vec::new(),
            max_diagnostics: DEFAULT_MAX_DIAGNOSTICS,
        }
    }
}

impl IntegrityMetrics {
    pub fn new(max_diagnostics: usize) -> Self {
        Self {
            max_diagnostics,
            ..Self::default()
        }
    }

    pub fn record(&mut self, mut diagnostic: IntegrityDiagnostic) -> Result<()> {
        self.counters.increment(diagnostic.category)?;

        if self.diagnostics.len() >= self.max_diagnostics {
            return Ok(());
        }

        // Bound message size while preserving UTF-8 boundaries.
        if diagnostic.message.len() > MAX_DIAGNOSTIC_MESSAGE_BYTES {
            let mut end = MAX_DIAGNOSTIC_MESSAGE_BYTES;

            while !diagnostic.message.is_char_boundary(end) {
                end -= 1;
            }

            diagnostic.message.truncate(end);
        }

        self.diagnostics.push(diagnostic);

        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        // Merge counters first. Overflow must leave the original
        // metrics unchanged.

        let mut merged_counters = self.counters.clone();
        merged_counters.merge(&other.counters)?;

        let remaining = self.max_diagnostics.saturating_sub(self.diagnostics.len());

        let mut diagnostics = self.diagnostics.clone();

        diagnostics.extend(other.diagnostics.iter().take(remaining).cloned());

        self.counters = merged_counters;
        self.diagnostics = diagnostics;

        Ok(())
    }

    pub fn has_violations(&self) -> bool {
        let counters = &self.counters;

        counters.parse_failure > 0
            || counters.invalid_record > 0
            || counters.sequence_gap > 0
            || counters.timestamp_regression > 0
            || counters.missing_snapshot > 0
            || counters.invalid_book > 0
            || counters.transformation_failure > 0
    }
}

// -----------------------------------------------------------------------------
// Unit tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn diagnostic(category: IntegrityCategory, index: u64) -> IntegrityDiagnostic {
        IntegrityDiagnostic {
            category,
            task_id: 120,
            source_record: Some(index),
            event_timestamp_ns: None,
            message: format!("integrity violation {index}"),
        }
    }

    #[test]
    fn counts_all_violations() {
        let mut metrics = IntegrityMetrics::new(2);

        for index in 0..10 {
            metrics
                .record(diagnostic(IntegrityCategory::InvalidRecord, index))
                .unwrap();
        }

        assert_eq!(metrics.counters.invalid_record, 10);
        assert_eq!(metrics.diagnostics.len(), 2);
        assert!(metrics.has_violations());
    }

    #[test]
    fn preserves_category_counts() {
        let mut metrics = IntegrityMetrics::default();

        metrics
            .record(diagnostic(IntegrityCategory::ParseFailure, 1))
            .unwrap();

        metrics
            .record(diagnostic(IntegrityCategory::SequenceGap, 2))
            .unwrap();

        assert_eq!(metrics.counters.parse_failure, 1);
        assert_eq!(metrics.counters.sequence_gap, 1);
        assert_eq!(metrics.counters.total().unwrap(), 2);
    }

    #[test]
    fn truncates_long_messages() {
        let mut metrics = IntegrityMetrics::default();

        let mut entry = diagnostic(IntegrityCategory::InvalidRecord, 1);
        entry.message = "x".repeat(5000);

        metrics.record(entry).unwrap();

        assert_eq!(
            metrics.diagnostics[0].message.len(),
            MAX_DIAGNOSTIC_MESSAGE_BYTES
        );
    }

    #[test]
    fn truncates_utf8_safely() {
        let mut metrics = IntegrityMetrics::default();

        let mut entry = diagnostic(IntegrityCategory::InvalidRecord, 1);
        entry.message = "€".repeat(1000);

        metrics.record(entry).unwrap();

        assert!(metrics.diagnostics[0].message.len() <= MAX_DIAGNOSTIC_MESSAGE_BYTES);

        assert!(std::str::from_utf8(metrics.diagnostics[0].message.as_bytes()).is_ok());
    }

    #[test]
    fn merges_integrity_metrics() {
        let mut first = IntegrityMetrics::new(2);
        let mut second = IntegrityMetrics::new(2);

        first
            .record(diagnostic(IntegrityCategory::InvalidRecord, 1))
            .unwrap();

        second
            .record(diagnostic(IntegrityCategory::ParseFailure, 2))
            .unwrap();

        second
            .record(diagnostic(IntegrityCategory::ParseFailure, 3))
            .unwrap();

        first.merge(&second).unwrap();

        assert_eq!(first.counters.invalid_record, 1);
        assert_eq!(first.counters.parse_failure, 2);

        assert_eq!(first.diagnostics.len(), 2);
    }

    #[test]
    fn counter_overflow_is_atomic() {
        let mut first = IntegrityCounters {
            invalid_record: u64::MAX,
            ..Default::default()
        };

        let second = IntegrityCounters {
            invalid_record: 1,
            ..Default::default()
        };

        let original = first.clone();

        assert!(first.merge(&second).is_err());
        assert_eq!(first, original);
    }
}
