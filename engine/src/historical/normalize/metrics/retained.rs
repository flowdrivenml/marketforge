//! Immutable observation storage. Mutable output accounting stays in TaskMetrics.
use super::{CategoryStatistics, IntegrityScope, ScopedIntegrityMetrics};
use crate::{
    historical::normalize::{
        ProcessingConfig,
        reconcile::{
            Quota,
            spool::{RetainedSpool, Workspace},
        },
    },
    shared::error::{MarketForgeError, Result},
};
use serde::{
    Deserialize, Serialize,
    ser::{SerializeSeq, SerializeStruct},
};

#[derive(Debug, Clone)]
pub(crate) struct Details {
    spool: RetainedSpool,
    counts: [usize; 3],
}
#[derive(Serialize, Deserialize)]
enum Entry {
    File(IntegrityScope, CategoryStatistics),
    Hour((IntegrityScope, i64), CategoryStatistics),
    Day((IntegrityScope, i64), CategoryStatistics),
}
impl ScopedIntegrityMetrics {
    /// Called only after source attribution is stable, before retaining another
    /// source. On any write error the authoritative resident maps remain intact.
    pub fn spill(&mut self, job: u64, task: u64, config: &ProcessingConfig) -> Result<()> {
        if self.files.is_empty() && self.hourly.is_empty() && self.daily.is_empty() {
            return Ok(());
        }
        crate::shared::historical::limits::check(
            "task metric entries",
            config.resources.normalization_limits.metric_entries,
            self.entry_count() as u64,
            "completed source report persistence",
        )?;
        if !self.retained.is_empty() {
            self.materialize()?;
        }
        if self.files.keys().any(|s| s.task_id != task)
            || self
                .hourly
                .keys()
                .chain(self.daily.keys())
                .any(|(s, _)| s.task_id != task)
        {
            return Err(MarketForgeError::InvalidConfiguration(
                "report scope/task mismatch".into(),
            ));
        }
        let identity = self
            .files
            .keys()
            .chain(self.hourly.keys().map(|(s, _)| s))
            .chain(self.daily.keys().map(|(s, _)| s))
            .try_fold(0u64, |maximum, scope| {
                (scope.stream_id.len() as u64)
                    .checked_add(scope.source_file.len() as u64)
                    .map(|n| maximum.max(n))
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration("report identity overflow".into())
                    })
            })?;
        let workspace = Workspace::new(
            &config.resources.scratch_path,
            config.resources.scratch_budget_bytes,
            Quota::new(super::report_buffer_bytes(identity)?),
        )?;
        let spool = workspace.create()?;
        let mut writer = spool.writer()?;
        for (k, v) in &self.files {
            writer.push(&Entry::File(k.clone(), v.clone()))?;
        }
        for (k, v) in &self.hourly {
            writer.push(&Entry::Hour(k.clone(), v.clone()))?;
        }
        for (k, v) in &self.daily {
            writer.push(&Entry::Day(k.clone(), v.clone()))?;
        }
        writer.finish()?;
        let details = Details {
            spool: spool.retain()?,
            counts: [self.files.len(), self.hourly.len(), self.daily.len()],
        };
        self.retained.insert((job, task), details);
        self.files.clear();
        self.hourly.clear();
        self.daily.clear();
        Ok(())
    }
    pub(crate) fn entry_count(&self) -> usize {
        self.files.len()
            + self.hourly.len()
            + self.daily.len()
            + self
                .retained
                .values()
                .map(|d| d.counts.iter().sum::<usize>())
                .sum::<usize>()
    }
    pub(crate) fn visit_files(
        &self,
        mut f: impl FnMut(&IntegrityScope, &CategoryStatistics) -> Result<()>,
    ) -> Result<()> {
        let mut tasks: std::collections::BTreeSet<_> =
            self.files.keys().map(|k| k.task_id).collect();
        tasks.extend(self.retained.keys().map(|(_, t)| *t));
        for task in tasks {
            for (k, v) in self.files.iter().filter(|(k, _)| k.task_id == task) {
                f(k, v)?;
            }
            for details in self
                .retained
                .iter()
                .filter(|((_, t), _)| *t == task)
                .map(|(_, d)| d)
            {
                details.spool.visit::<Entry>(|entry| {
                    if let Entry::File(k, v) = entry {
                        f(k, v)?;
                    }
                    Ok(())
                })?;
            }
        }
        Ok(())
    }
    pub(crate) fn visit_windows(
        &self,
        daily: bool,
        mut f: impl FnMut(&(IntegrityScope, i64), &CategoryStatistics) -> Result<()>,
    ) -> Result<()> {
        let map = if daily { &self.daily } else { &self.hourly };
        let mut tasks: std::collections::BTreeSet<_> = map.keys().map(|(k, _)| k.task_id).collect();
        tasks.extend(self.retained.keys().map(|(_, t)| *t));
        for task in tasks {
            for (k, v) in map.iter().filter(|((k, _), _)| k.task_id == task) {
                f(k, v)?;
            }
            for details in self
                .retained
                .iter()
                .filter(|((_, t), _)| *t == task)
                .map(|(_, d)| d)
            {
                details.spool.visit::<Entry>(|entry| {
                    match entry {
                        Entry::Day(k, v) if daily => f(k, v)?,
                        Entry::Hour(k, v) if !daily => f(k, v)?,
                        _ => (),
                    }
                    Ok(())
                })?;
            }
        }
        Ok(())
    }
    /// Only used to combine routing and normalization observations for one task.
    pub(crate) fn materialize(&mut self) -> Result<()> {
        let mut files = self.files.clone();
        let mut hourly = self.hourly.clone();
        let mut daily = self.daily.clone();
        for details in self.retained.values() {
            details.spool.visit::<Entry>(|entry| {
                match entry {
                    Entry::File(k, v) => files.entry(k.clone()).or_default().merge(v)?,
                    Entry::Hour(k, v) => hourly.entry(k.clone()).or_default().merge(v)?,
                    Entry::Day(k, v) => daily.entry(k.clone()).or_default().merge(v)?,
                }
                Ok(())
            })?;
        }
        self.files = files;
        self.hourly = hourly;
        self.daily = daily;
        self.retained.clear();
        Ok(())
    }
    /// Task identities are checked by ProcessingMetricsReport before this call.
    /// No detailed-map decoding or job-wide clone is needed for aggregation.
    pub(crate) fn validate_append(&self, other: &Self) -> Result<()> {
        if other.retained.keys().any(|k| self.retained.contains_key(k)) {
            return Err(MarketForgeError::InvalidConfiguration(
                "duplicate retained observations".into(),
            ));
        }
        self.validate_merge(other)
    }
    pub(crate) fn append_validated(&mut self, other: &Self) {
        self.merge_validated(other);
        self.retained
            .extend(other.retained.iter().map(|(k, v)| (*k, v.clone())));
    }
}
impl Serialize for ScopedIntegrityMetrics {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        struct Entries<'a>(&'a ScopedIntegrityMetrics, usize);
        impl Serialize for Entries<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let resident = match self.1 {
                    0 => self.0.files.len(),
                    1 => self.0.hourly.len(),
                    _ => self.0.daily.len(),
                };
                let count = self
                    .0
                    .retained
                    .values()
                    .try_fold(resident, |n, d| n.checked_add(d.counts[self.1]))
                    .ok_or_else(|| serde::ser::Error::custom("report length overflow"))?;
                let mut seq = serializer.serialize_seq(Some(count))?;
                // Persisted field names and ordering match the original maps.
                #[derive(Serialize)]
                struct File<'a> {
                    scope: &'a IntegrityScope,
                    statistics: &'a CategoryStatistics,
                }
                #[derive(Serialize)]
                struct Window<'a> {
                    scope: &'a IntegrityScope,
                    window_start_ns: i64,
                    statistics: &'a CategoryStatistics,
                }
                // Serialization failures retain their serializer context.
                let mut failure = None;
                let result =
                    if self.1 == 0 {
                        self.0.visit_files(|scope, statistics| {
                            match seq.serialize_element(&File { scope, statistics }) {
                                Ok(()) => Ok(()),
                                Err(e) => {
                                    failure = Some(e);
                                    Err(MarketForgeError::InvalidConfiguration(
                                        "report serialization failed".into(),
                                    ))
                                }
                            }
                        })
                    } else {
                        self.0
                            .visit_windows(self.1 == 2, |(scope, window_start_ns), statistics| {
                                match seq.serialize_element(&Window {
                                    scope,
                                    window_start_ns: *window_start_ns,
                                    statistics,
                                }) {
                                    Ok(()) => Ok(()),
                                    Err(e) => {
                                        failure = Some(e);
                                        Err(MarketForgeError::InvalidConfiguration(
                                            "report serialization failed".into(),
                                        ))
                                    }
                                }
                            })
                    };
                if let Some(e) = failure {
                    return Err(e);
                }
                result.map_err(serde::ser::Error::custom)?;
                seq.end()
            }
        }
        let mut s = serializer.serialize_struct("ScopedIntegrityMetrics", 5)?;
        s.serialize_field("global", &self.global)?;
        s.serialize_field("unattributed", &self.unattributed)?;
        s.serialize_field("files", &Entries(self, 0))?;
        s.serialize_field("hourly", &Entries(self, 1))?;
        s.serialize_field("daily", &Entries(self, 2))?;
        s.end()
    }
}
impl PartialEq for ScopedIntegrityMetrics {
    fn eq(&self, other: &Self) -> bool {
        if std::ptr::eq(self, other) {
            return true;
        }
        if self.global != other.global || self.unattributed != other.unattributed {
            return false;
        }
        if self.retained.len() == other.retained.len()
            && self
                .retained
                .iter()
                .zip(&other.retained)
                .all(|((a, x), (b, y))| {
                    a == b && x.counts == y.counts && x.spool.same_storage(&y.spool)
                })
        {
            return self.files == other.files
                && self.hourly == other.hourly
                && self.daily == other.daily;
        }
        // Comparison is also bounded when one side came from persisted JSON.
        use sha2::{Digest, Sha256};
        struct Hash(Sha256);
        impl std::io::Write for Hash {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.update(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let digest = |s: &Self| {
            let mut h = Hash(Sha256::new());
            serde_json::to_writer(&mut h, s).ok()?;
            Some(h.0.finalize())
        };
        match (digest(self), digest(other)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}
impl Eq for ScopedIntegrityMetrics {}
