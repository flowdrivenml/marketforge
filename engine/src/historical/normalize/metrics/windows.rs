use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::shared::error::{MarketForgeError, Result};

use crate::shared::validation::IntegrityCategory;

// -----------------------------------------------------------------------------
// UTC window constants
// -----------------------------------------------------------------------------

pub const HOUR_NS: i64 = 3_600_000_000_000;
pub const DAY_NS: i64 = 86_400_000_000_000;

// -----------------------------------------------------------------------------
// Category-specific observations
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategoryObservation {
    pub eligible: u64,
    pub violations: u64,
}

impl CategoryObservation {
    pub fn rate(&self) -> Option<f64> {
        if self.eligible == 0 {
            return None;
        }

        Some(self.violations as f64 / self.eligible as f64)
    }

    pub fn record(&mut self, violated: bool) -> Result<()> {
        let eligible = self.eligible.checked_add(1).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "integrity eligible-observation counter overflow".to_owned(),
            )
        })?;

        let violations = if violated {
            self.violations.checked_add(1).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "integrity violation counter overflow".to_owned(),
                )
            })?
        } else {
            self.violations
        };

        self.eligible = eligible;
        self.violations = violations;

        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        let eligible = self.eligible.checked_add(other.eligible).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "integrity eligible-observation counter overflow".to_owned(),
            )
        })?;

        let violations = self
            .violations
            .checked_add(other.violations)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "integrity violation counter overflow".to_owned(),
                )
            })?;

        self.eligible = eligible;
        self.violations = violations;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Per-category statistics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategoryStatistics {
    pub parse_failure: CategoryObservation,
    pub invalid_record: CategoryObservation,
    pub sequence_gap: CategoryObservation,
    pub timestamp_regression: CategoryObservation,
    pub missing_snapshot: CategoryObservation,
    pub invalid_book: CategoryObservation,
    pub transformation_failure: CategoryObservation,
}

impl CategoryStatistics {
    pub fn get(&self, category: IntegrityCategory) -> &CategoryObservation {
        match category {
            IntegrityCategory::ParseFailure => &self.parse_failure,
            IntegrityCategory::InvalidRecord => &self.invalid_record,
            IntegrityCategory::SequenceGap => &self.sequence_gap,
            IntegrityCategory::TimestampRegression => &self.timestamp_regression,
            IntegrityCategory::MissingSnapshot => &self.missing_snapshot,
            IntegrityCategory::InvalidBook => &self.invalid_book,
            IntegrityCategory::TransformationFailure => &self.transformation_failure,
        }
    }

    pub fn get_mut(&mut self, category: IntegrityCategory) -> &mut CategoryObservation {
        match category {
            IntegrityCategory::ParseFailure => &mut self.parse_failure,
            IntegrityCategory::InvalidRecord => &mut self.invalid_record,
            IntegrityCategory::SequenceGap => &mut self.sequence_gap,
            IntegrityCategory::TimestampRegression => &mut self.timestamp_regression,
            IntegrityCategory::MissingSnapshot => &mut self.missing_snapshot,
            IntegrityCategory::InvalidBook => &mut self.invalid_book,
            IntegrityCategory::TransformationFailure => &mut self.transformation_failure,
        }
    }

    pub fn record(&mut self, category: IntegrityCategory, violated: bool) -> Result<()> {
        self.get_mut(category).record(violated)
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        // Make aggregation atomic on overflow.
        let mut merged = self.clone();

        for category in [
            IntegrityCategory::ParseFailure,
            IntegrityCategory::InvalidRecord,
            IntegrityCategory::SequenceGap,
            IntegrityCategory::TimestampRegression,
            IntegrityCategory::MissingSnapshot,
            IntegrityCategory::InvalidBook,
            IntegrityCategory::TransformationFailure,
        ] {
            merged.get_mut(category).merge(other.get(category))?;
        }

        *self = merged;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Time-window identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WindowGranularity {
    Hourly,
    Daily,
}

impl WindowGranularity {
    pub fn duration_ns(self) -> i64 {
        match self {
            Self::Hourly => HOUR_NS,
            Self::Daily => DAY_NS,
        }
    }

    pub fn start_ns(self, timestamp_ns: i64) -> i64 {
        let duration = self.duration_ns();

        timestamp_ns.div_euclid(duration) * duration
    }
}

// -----------------------------------------------------------------------------
// Integrity scope
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IntegrityScope {
    pub task_id: u64,
    pub stream_id: String,
    pub source_file: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityWindowEntry {
    pub scope: IntegrityScope,
    pub window_start_ns: i64,
    pub statistics: CategoryStatistics,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScopedIntegrityMetrics {
    #[serde(skip)]
    pub(crate) retained: std::collections::BTreeMap<(u64, u64), super::retained::Details>,
    pub global: CategoryStatistics,
    /// Eligible observations and violations lacking trustworthy event time.
    #[serde(default)]
    pub unattributed: CategoryStatistics,

    #[serde(deserialize_with = "file_map_serde::deserialize")]
    pub files: BTreeMap<IntegrityScope, CategoryStatistics>,

    #[serde(deserialize_with = "window_map_serde::deserialize")]
    pub hourly: BTreeMap<(IntegrityScope, i64), CategoryStatistics>,

    #[serde(deserialize_with = "window_map_serde::deserialize")]
    pub daily: BTreeMap<(IntegrityScope, i64), CategoryStatistics>,
}

impl ScopedIntegrityMetrics {
    pub fn record(
        &mut self,
        scope: &IntegrityScope,
        category: IntegrityCategory,
        violated: bool,
        event_timestamp_ns: Option<i64>,
    ) -> Result<()> {
        if !self.retained.is_empty() {
            return Err(MarketForgeError::InvalidConfiguration(
                "cannot record observations after source report sealing".into(),
            ));
        }
        // The usual ordered, task-local observation updates existing final map
        // entries. Borrow their keys instead of cloning identical path/scope strings.
        let windows = event_timestamp_ns.map(|t| {
            (
                WindowGranularity::Hourly.start_ns(t),
                WindowGranularity::Daily.start_ns(t),
            )
        });
        let file_match = self
            .files
            .last_key_value()
            .is_some_and(|(key, _)| key == scope);
        let window_match = windows.is_none_or(|(hour, day)| {
            self.hourly
                .last_key_value()
                .is_some_and(|((key, t), _)| key == scope && *t == hour)
                && self
                    .daily
                    .last_key_value()
                    .is_some_and(|((key, t), _)| key == scope && *t == day)
        });
        if file_match && window_match {
            if let Some(limits) = crate::shared::historical::limits::current() {
                crate::shared::historical::limits::check(
                    "task metric entries",
                    limits.metric_entries,
                    (self.files.len() + self.hourly.len() + self.daily.len()) as u64,
                    "integrity observation",
                )?;
            }
            let mut global = self.global.clone();
            let mut file = self.files.last_key_value().unwrap().1.clone();
            let mut unattributed = self.unattributed.clone();
            global.record(category, violated)?;
            file.record(category, violated)?;
            let updates = if windows.is_some() {
                let mut hour = self.hourly.last_key_value().unwrap().1.clone();
                let mut day = self.daily.last_key_value().unwrap().1.clone();
                hour.record(category, violated)?;
                day.record(category, violated)?;
                Some((hour, day))
            } else {
                unattributed.record(category, violated)?;
                None
            };
            self.global = global;
            self.unattributed = unattributed;
            *self.files.last_entry().unwrap().get_mut() = file;
            if let Some((hour, day)) = updates {
                *self.hourly.last_entry().unwrap().get_mut() = hour;
                *self.daily.last_entry().unwrap().get_mut() = day;
            }
            return Ok(());
        }
        if let Some(limits) = crate::shared::historical::limits::current() {
            let mut requested = self
                .files
                .len()
                .checked_add(self.hourly.len())
                .and_then(|n| n.checked_add(self.daily.len()))
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "metric entry accounting overflow".into(),
                    )
                })?;
            requested = requested
                .checked_add(usize::from(!self.files.contains_key(scope)))
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "metric entry accounting overflow".into(),
                    )
                })?;
            if let Some(timestamp) = event_timestamp_ns {
                requested = requested
                    .checked_add(usize::from(!self.hourly.contains_key(&(
                        scope.clone(),
                        WindowGranularity::Hourly.start_ns(timestamp),
                    ))))
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "metric entry accounting overflow".into(),
                        )
                    })?;
                requested = requested
                    .checked_add(usize::from(!self.daily.contains_key(&(
                        scope.clone(),
                        WindowGranularity::Daily.start_ns(timestamp),
                    ))))
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "metric entry accounting overflow".into(),
                        )
                    })?;
            }
            crate::shared::historical::limits::check(
                "task metric entries",
                limits.metric_entries,
                requested as u64,
                "integrity observation",
            )?;
        }
        // Precompute every update before mutating the collector.
        // This avoids partial accounting if a counter overflows.

        let mut global = self.global.clone();
        global.record(category, violated)?;

        let mut file = self.files.get(scope).cloned().unwrap_or_default();
        file.record(category, violated)?;

        let mut unattributed = self.unattributed.clone();
        if event_timestamp_ns.is_none() {
            unattributed.record(category, violated)?;
        }
        let mut hourly_update = None;
        let mut daily_update = None;

        if let Some(timestamp) = event_timestamp_ns {
            let hour = WindowGranularity::Hourly.start_ns(timestamp);
            let day = WindowGranularity::Daily.start_ns(timestamp);

            let hourly_key = (scope.clone(), hour);
            let daily_key = (scope.clone(), day);

            let mut hourly = self.hourly.get(&hourly_key).cloned().unwrap_or_default();

            let mut daily = self.daily.get(&daily_key).cloned().unwrap_or_default();

            hourly.record(category, violated)?;
            daily.record(category, violated)?;

            hourly_update = Some((hourly_key, hourly));
            daily_update = Some((daily_key, daily));
        }

        self.global = global;
        self.unattributed = unattributed;
        self.files.insert(scope.clone(), file);

        if let Some((key, value)) = hourly_update {
            self.hourly.insert(key, value);
        }

        if let Some((key, value)) = daily_update {
            self.daily.insert(key, value);
        }

        Ok(())
    }
    /// Validate all arithmetic before mutating any scope. Temporary state is one
    /// CategoryStatistics value, independent of the retained map cardinality.
    pub(super) fn validate_merge(&self, other: &Self) -> Result<()> {
        let mut global = self.global.clone();
        global.merge(&other.global)?;
        let mut unattributed = self.unattributed.clone();
        unattributed.merge(&other.unattributed)?;
        fn check<K: Ord>(
            left: &BTreeMap<K, CategoryStatistics>,
            right: &BTreeMap<K, CategoryStatistics>,
        ) -> Result<()> {
            for (key, value) in right {
                let mut updated = left.get(key).cloned().unwrap_or_default();
                updated.merge(value)?;
            }
            Ok(())
        }
        check(&self.files, &other.files)?;
        check(&self.hourly, &other.hourly)?;
        check(&self.daily, &other.daily)
    }
    pub(super) fn merge_validated(&mut self, other: &Self) {
        self.global
            .merge(&other.global)
            .expect("prevalidated global merge");
        self.unattributed
            .merge(&other.unattributed)
            .expect("prevalidated unattributed merge");
        fn apply<K: Ord + Clone>(
            left: &mut BTreeMap<K, CategoryStatistics>,
            right: &BTreeMap<K, CategoryStatistics>,
        ) {
            for (key, value) in right {
                left.entry(key.clone())
                    .or_default()
                    .merge(value)
                    .expect("prevalidated scope merge");
            }
        }
        apply(&mut self.files, &other.files);
        apply(&mut self.hourly, &other.hourly);
        apply(&mut self.daily, &other.daily);
    }
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        if !self.retained.is_empty() || !other.retained.is_empty() {
            let mut left = self.clone();
            left.materialize()?;
            let mut right = other.clone();
            right.materialize()?;
            left.merge(&right)?;
            *self = left;
            return Ok(());
        }
        self.validate_merge(other)?;
        self.merge_validated(other);
        Ok(())
    }
}

mod window_map_serde {
    use std::collections::BTreeMap;

    use serde::Deserializer;

    use super::{CategoryStatistics, IntegrityScope, IntegrityWindowEntry};

    type WindowMap = BTreeMap<(IntegrityScope, i64), CategoryStatistics>;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<WindowMap, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Entries;
        impl<'de> serde::de::Visitor<'de> for Entries {
            type Value = WindowMap;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("integrity window entries")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut entries: A,
            ) -> Result<WindowMap, A::Error> {
                let mut map = BTreeMap::new();
                while let Some(entry) = entries.next_element::<IntegrityWindowEntry>()? {
                    if map
                        .insert((entry.scope, entry.window_start_ns), entry.statistics)
                        .is_some()
                    {
                        return Err(serde::de::Error::custom("duplicate integrity window entry"));
                    }
                }
                Ok(map)
            }
        }
        deserializer.deserialize_seq(Entries)
    }
}

mod file_map_serde {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize};

    use super::{CategoryStatistics, IntegrityScope};

    #[derive(Serialize, Deserialize)]
    struct FileEntry {
        scope: IntegrityScope,
        statistics: CategoryStatistics,
    }

    type FileMap = BTreeMap<IntegrityScope, CategoryStatistics>;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<FileMap, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Entries;
        impl<'de> serde::de::Visitor<'de> for Entries {
            type Value = FileMap;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("integrity file entries")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut entries: A,
            ) -> Result<FileMap, A::Error> {
                let mut map = BTreeMap::new();
                while let Some(entry) = entries.next_element::<FileEntry>()? {
                    if map.insert(entry.scope, entry.statistics).is_some() {
                        return Err(serde::de::Error::custom("duplicate integrity file entry"));
                    }
                }
                Ok(map)
            }
        }
        deserializer.deserialize_seq(Entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> IntegrityScope {
        IntegrityScope {
            task_id: 120,
            stream_id: "okx:BTC-USDT:trades".to_owned(),
            source_file: "BTC-USDT-trades-2026-09-01.zip".to_owned(),
        }
    }

    #[test]
    fn calculates_error_rates() {
        let mut observations = CategoryObservation::default();

        for _ in 0..100 {
            observations.record(false).unwrap();
        }

        for _ in 0..5 {
            observations.record(true).unwrap();
        }

        assert_eq!(observations.eligible, 105);
        assert_eq!(observations.violations, 5);
        assert_eq!(observations.rate(), Some(5.0 / 105.0));
    }

    #[test]
    fn separates_hourly_windows() {
        let mut metrics = ScopedIntegrityMetrics::default();
        let scope = scope();

        let first_hour = 1_788_220_800_000_000_000;
        let second_hour = first_hour + HOUR_NS;

        metrics
            .record(
                &scope,
                IntegrityCategory::InvalidRecord,
                true,
                Some(first_hour),
            )
            .unwrap();

        metrics
            .record(
                &scope,
                IntegrityCategory::InvalidRecord,
                false,
                Some(second_hour),
            )
            .unwrap();

        assert_eq!(metrics.hourly.len(), 2);
        assert_eq!(metrics.daily.len(), 1);

        assert_eq!(
            metrics
                .global
                .get(IntegrityCategory::InvalidRecord)
                .eligible,
            2
        );
    }

    #[test]
    fn missing_timestamp_remains_in_file_metrics() {
        let mut metrics = ScopedIntegrityMetrics::default();
        let scope = scope();

        metrics
            .record(&scope, IntegrityCategory::ParseFailure, true, None)
            .unwrap();

        assert_eq!(metrics.files.len(), 1);
        assert!(metrics.hourly.is_empty());
        assert!(metrics.daily.is_empty());

        assert_eq!(
            metrics
                .global
                .get(IntegrityCategory::ParseFailure)
                .violations,
            1
        );
    }

    #[test]
    fn supports_negative_timestamps() {
        assert_eq!(WindowGranularity::Hourly.start_ns(-1), -HOUR_NS);

        assert_eq!(WindowGranularity::Daily.start_ns(-1), -DAY_NS);
    }

    #[test]
    fn merges_category_statistics() {
        let mut first = CategoryStatistics::default();
        let mut second = CategoryStatistics::default();

        first
            .record(IntegrityCategory::InvalidRecord, true)
            .unwrap();

        second
            .record(IntegrityCategory::InvalidRecord, false)
            .unwrap();

        first.merge(&second).unwrap();

        let stats = first.get(IntegrityCategory::InvalidRecord);

        assert_eq!(stats.eligible, 2);
        assert_eq!(stats.violations, 1);
    }

    #[test]
    fn rejects_counter_overflow_without_partial_mutation() {
        let mut metrics = ScopedIntegrityMetrics::default();
        let scope = scope();

        metrics.global.invalid_record.eligible = u64::MAX;

        let original = metrics.clone();

        assert!(
            metrics
                .record(&scope, IntegrityCategory::InvalidRecord, false, None,)
                .is_err()
        );

        assert_eq!(metrics, original);
    }

    #[test]
    fn merges_scoped_metrics() {
        let scope = scope();

        let mut first = ScopedIntegrityMetrics::default();
        let mut second = ScopedIntegrityMetrics::default();

        let timestamp = 1_788_220_800_000_000_000;

        first
            .record(
                &scope,
                IntegrityCategory::InvalidRecord,
                false,
                Some(timestamp),
            )
            .unwrap();

        second
            .record(
                &scope,
                IntegrityCategory::InvalidRecord,
                true,
                Some(timestamp),
            )
            .unwrap();

        first.merge(&second).unwrap();

        let global = first.global.get(IntegrityCategory::InvalidRecord);

        assert_eq!(global.eligible, 2);
        assert_eq!(global.violations, 1);

        assert_eq!(first.files.len(), 1);
        assert_eq!(first.hourly.len(), 1);
        assert_eq!(first.daily.len(), 1);
    }

    #[test]
    fn scoped_merge_overflow_is_atomic() {
        let mut first = ScopedIntegrityMetrics::default();
        let mut second = ScopedIntegrityMetrics::default();

        first.global.invalid_record.eligible = u64::MAX;
        second.global.invalid_record.eligible = 1;

        let original = first.clone();

        assert!(first.merge(&second).is_err());
        assert_eq!(first, original);
    }
    #[test]
    fn scoped_integrity_json_roundtrip() {
        let mut metrics = ScopedIntegrityMetrics::default();

        let scope = scope();
        let timestamp = 1_788_220_800_000_000_000;

        metrics
            .record(
                &scope,
                IntegrityCategory::InvalidRecord,
                true,
                Some(timestamp),
            )
            .unwrap();

        let json = serde_json::to_string_pretty(&metrics).expect("serialize scoped integrity");

        let restored: ScopedIntegrityMetrics =
            serde_json::from_str(&json).expect("deserialize scoped integrity");

        assert_eq!(restored, metrics);

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(value["files"].is_array());
        assert!(value["hourly"].is_array());
        assert!(value["daily"].is_array());
    }

    #[test]
    fn rejects_duplicate_window_entries() {
        let mut metrics = ScopedIntegrityMetrics::default();

        metrics
            .record(
                &scope(),
                IntegrityCategory::InvalidRecord,
                false,
                Some(1_788_220_800_000_000_000),
            )
            .unwrap();

        let mut value = serde_json::to_value(&metrics).unwrap();

        let duplicate = value["hourly"][0].clone();

        value["hourly"].as_array_mut().unwrap().push(duplicate);

        assert!(serde_json::from_value::<ScopedIntegrityMetrics>(value).is_err());
    }
}
