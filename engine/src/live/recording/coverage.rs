//! Verified committed interval bookkeeping, not a MAX(id) completeness claim.
use crate::live::model::{
    cursor::{LogEpoch, StreamCursor},
    identity::StreamId,
    replay::{ReplayError, ReplayRange},
};
use std::collections::HashMap;
#[derive(Debug, Clone)]
pub struct CommittedCoverage {
    intervals: HashMap<(StreamId, LogEpoch), Vec<(u64, u64)>>,
    max_intervals: usize,
}
impl CommittedCoverage {
    pub fn new(max_intervals: usize) -> Result<Self, ReplayError> {
        if max_intervals == 0 {
            return Err(ReplayError::Capacity);
        }
        Ok(Self {
            intervals: HashMap::new(),
            max_intervals,
        })
    }
    /// Called only after an acknowledged committed transaction or verified retry
    /// reconciliation. Unknown outcomes must not advance this state.
    pub fn observe(&mut self, range: &ReplayRange) -> Result<(), ReplayError> {
        let end = range.last().ok_or(ReplayError::Range)?.offset;
        let key = (range.first().stream_id.clone(), range.first().log_epoch);
        let mut ranges = self.intervals.get(&key).cloned().unwrap_or_default();
        ranges.push((range.first().offset, end));
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = vec![];
        for (first, last) in ranges {
            if let Some(prior) = merged.last_mut() {
                if first <= prior.1 || prior.1.checked_add(1) == Some(first) {
                    prior.1 = prior.1.max(last);
                    continue;
                }
            }
            merged.push((first, last));
        }
        let total: usize = self.intervals.values().map(Vec::len).sum();
        let old = self.intervals.get(&key).map_or(0, Vec::len);
        if total
            .checked_sub(old)
            .and_then(|n| n.checked_add(merged.len()))
            .ok_or(ReplayError::Capacity)?
            > self.max_intervals
        {
            return Err(ReplayError::Capacity);
        }
        self.intervals.insert(key, merged);
        Ok(())
    }
    pub fn observe_batch(&mut self, ranges: &[ReplayRange]) -> Result<(), ReplayError> {
        let mut staged = self.clone();
        for range in ranges {
            staged.observe(range)?;
        }
        *self = staged;
        Ok(())
    }
    pub fn contiguous_through(&self, from: &StreamCursor) -> Option<StreamCursor> {
        let (_, last) = self
            .intervals
            .get(&(from.stream_id.clone(), from.log_epoch))?
            .iter()
            .find(|(a, b)| *a <= from.offset && from.offset <= *b)?;
        Some(StreamCursor::new(
            from.stream_id.clone(),
            from.log_epoch,
            *last,
        ))
    }
}
