use std::collections::{HashSet, VecDeque};

// ============================================================
// CONFIGURATION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeIdentityLimits {
    /// Maximum retained native trade IDs.
    pub max_ids: usize,

    /// Maximum UTF-8 byte length of one native ID.
    pub max_id_bytes: usize,

    /// Maximum records processed in one atomic batch.
    pub max_batch_records: usize,
}

impl TradeIdentityLimits {
    pub fn validate(self) -> Result<Self, TradeIdentityError> {
        if self.max_ids == 0 || self.max_id_bytes == 0 || self.max_batch_records == 0 {
            return Err(TradeIdentityError::InvalidLimits);
        }

        Ok(self)
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeIdentityError {
    InvalidLimits,
    EmptyStreamId,
    EmptyTradeId,
    TradeIdTooLong,
    BatchTooLarge,
}

// ============================================================
// OBSERVATIONS
// ============================================================

/// The result of checking one trade identity.
///
/// `NewWithinWindow` does NOT establish that the trade
/// has never appeared before.
///
/// Previously evicted identities cannot be recognized
/// without additional authoritative history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeIdentityDecision {
    NewWithinWindow,
    DuplicateWithinWindow,
}

// ============================================================
// TRACKER
// ============================================================

/// Bounded native trade-identity tracking for one source
/// stream and one runtime generation.
///
/// The caller must construct a new tracker for a new
/// stream generation.
///
/// This tracker does not:
/// - Infer missing trades.
/// - Compare numeric trade-ID distances.
/// - Perform REST reconciliation.
/// - Own canonical Trade objects.
/// - Record GapTracker incidents.
/// - Guarantee global deduplication.
#[derive(Debug)]
pub struct TradeIdentityTracker {
    stream_id: String,
    generation: u64,
    limits: TradeIdentityLimits,

    retained: HashSet<String>,
    order: VecDeque<String>,

    evictions_total: u64,
    observations_total: u64,
    duplicates_total: u64,
}

impl TradeIdentityTracker {
    pub fn new(
        stream_id: impl Into<String>,
        generation: u64,
        limits: TradeIdentityLimits,
    ) -> Result<Self, TradeIdentityError> {
        let stream_id = stream_id.into();
        let limits = limits.validate()?;

        if stream_id.trim().is_empty() {
            return Err(TradeIdentityError::EmptyStreamId);
        }

        Ok(Self {
            stream_id,
            generation,
            limits,
            retained: HashSet::new(),
            order: VecDeque::new(),
            evictions_total: 0,
            observations_total: 0,
            duplicates_total: 0,
        })
    }

    // ========================================================
    // BATCH PROCESSING
    // ========================================================

    /// Observe a complete batch of native trade IDs.
    ///
    /// Validation is performed before any tracker state
    /// is mutated.
    ///
    /// An invalid ID rejects the entire batch.
    ///
    /// Duplicate classification is based on the retained
    /// history at batch admission plus identities already
    /// encountered in the same batch.
    ///
    /// The caller must provide IDs belonging to this
    /// tracker's authoritative stream and generation.
    pub(super) fn observe_batch(
        &mut self,
        ids: &[&str],
    ) -> Result<Vec<TradeIdentityDecision>, TradeIdentityError> {
        if ids.len() > self.limits.max_batch_records || ids.len() > self.limits.max_ids {
            return Err(TradeIdentityError::BatchTooLarge);
        }

        // Validate everything before changing state.
        for id in ids {
            if id.is_empty() {
                return Err(TradeIdentityError::EmptyTradeId);
            }

            if id.len() > self.limits.max_id_bytes {
                return Err(TradeIdentityError::TradeIdTooLong);
            }
        }

        // Classify against the original retained history
        // and previously encountered IDs in this batch.
        //
        // This prevents capacity evictions during commit
        // from altering duplicate classification within
        // the same incoming batch.
        let mut seen_in_batch = HashSet::new();
        let mut decisions = Vec::with_capacity(ids.len());

        for id in ids {
            let duplicate = self.retained.contains(*id) || !seen_in_batch.insert(*id);

            decisions.push(if duplicate {
                TradeIdentityDecision::DuplicateWithinWindow
            } else {
                TradeIdentityDecision::NewWithinWindow
            });
        }

        // All fallible validation has completed.
        //
        // Commit first observations in source batch order.
        for (id, decision) in ids.iter().zip(decisions.iter()) {
            self.observations_total = self.observations_total.saturating_add(1);

            match decision {
                TradeIdentityDecision::DuplicateWithinWindow => {
                    self.duplicates_total = self.duplicates_total.saturating_add(1);
                }

                TradeIdentityDecision::NewWithinWindow => {
                    // This ID may already exist if it was
                    // classified against the original history
                    // and capacity changed during this commit.
                    //
                    // Do not insert duplicate queue entries.
                    if self.retained.insert((*id).to_owned()) {
                        self.order.push_back((*id).to_owned());
                    }

                    while self.order.len() > self.limits.max_ids {
                        if let Some(evicted) = self.order.pop_front() {
                            self.retained.remove(&evicted);

                            self.evictions_total = self.evictions_total.saturating_add(1);
                        }
                    }
                }
            }
        }

        Ok(decisions)
    }

    /// Convenience operation for one trade identity.
    pub(super) fn observe(
        &mut self,
        id: &str,
    ) -> Result<TradeIdentityDecision, TradeIdentityError> {
        let decisions = self.observe_batch(&[id])?;

        Ok(decisions[0])
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn limits(&self) -> TradeIdentityLimits {
        self.limits
    }

    pub fn retained_count(&self) -> usize {
        self.retained.len()
    }

    pub fn contains(&self, id: &str) -> bool {
        self.retained.contains(id)
    }

    pub fn observations_total(&self) -> u64 {
        self.observations_total
    }

    pub fn duplicates_total(&self) -> u64 {
        self.duplicates_total
    }

    pub fn evictions_total(&self) -> u64 {
        self.evictions_total
    }

    /// Whether the bounded history has already discarded
    /// at least one identity.
    ///
    /// After an eviction, absence from this tracker must
    /// not be interpreted as proof of a globally new trade.
    pub fn history_truncated(&self) -> bool {
        self.evictions_total > 0
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker() -> TradeIdentityTracker {
        TradeIdentityTracker::new(
            "binance:spot:BTCUSDT:trades",
            10,
            TradeIdentityLimits {
                max_ids: 3,
                max_id_bytes: 32,
                max_batch_records: 3,
            },
        )
        .unwrap()
    }

    #[test]
    fn first_identity_is_new() {
        let mut tracker = tracker();

        assert_eq!(
            tracker.observe("100").unwrap(),
            TradeIdentityDecision::NewWithinWindow,
        );

        assert!(tracker.contains("100"));
        assert_eq!(tracker.retained_count(), 1);
    }

    #[test]
    fn repeated_identity_is_duplicate() {
        let mut tracker = tracker();

        tracker.observe("100").unwrap();

        assert_eq!(
            tracker.observe("100").unwrap(),
            TradeIdentityDecision::DuplicateWithinWindow,
        );

        assert_eq!(tracker.duplicates_total(), 1);
        assert_eq!(tracker.retained_count(), 1);
    }

    #[test]
    fn duplicates_inside_one_batch_are_detected() {
        let mut tracker = tracker();

        let result = tracker.observe_batch(&["100", "101", "100"]).unwrap();

        assert_eq!(
            result,
            vec![
                TradeIdentityDecision::NewWithinWindow,
                TradeIdentityDecision::NewWithinWindow,
                TradeIdentityDecision::DuplicateWithinWindow,
            ],
        );

        assert_eq!(tracker.retained_count(), 2);
        assert_eq!(tracker.duplicates_total(), 1);
    }

    #[test]
    fn oldest_identity_is_evicted_at_capacity() {
        let mut tracker = tracker();

        tracker.observe_batch(&["100", "101", "102"]).unwrap();
        tracker.observe("103").unwrap();

        assert!(!tracker.contains("100"));
        assert!(tracker.contains("101"));
        assert!(tracker.contains("102"));
        assert!(tracker.contains("103"));

        assert_eq!(tracker.retained_count(), 3);
        assert_eq!(tracker.evictions_total(), 1);

        assert!(tracker.history_truncated());
    }

    #[test]
    fn evicted_identity_is_not_claimed_globally_new() {
        let mut tracker = tracker();

        tracker.observe_batch(&["100", "101", "102"]).unwrap();
        tracker.observe("103").unwrap();

        // The tracker has forgotten ID 100.
        //
        // This means "not found in retained history",
        // not "never observed by MarketForge".
        assert_eq!(
            tracker.observe("100").unwrap(),
            TradeIdentityDecision::NewWithinWindow,
        );

        assert!(tracker.history_truncated());
    }

    #[test]
    fn invalid_batch_does_not_modify_state() {
        let mut tracker = tracker();

        tracker.observe("100").unwrap();

        let result = tracker.observe_batch(&["101", "", "102"]);

        assert_eq!(result, Err(TradeIdentityError::EmptyTradeId),);

        assert!(tracker.contains("100"));
        assert!(!tracker.contains("101"));

        assert_eq!(tracker.retained_count(), 1);
        assert_eq!(tracker.observations_total(), 1);
    }

    #[test]
    fn oversized_batch_is_rejected() {
        let mut tracker = tracker();

        let result = tracker.observe_batch(&["1", "2", "3", "4"]);

        assert_eq!(result, Err(TradeIdentityError::BatchTooLarge),);

        assert_eq!(tracker.retained_count(), 0);
    }

    #[test]
    fn oversized_identity_is_rejected() {
        let mut tracker = tracker();

        let oversized = "a".repeat(33);

        assert_eq!(
            tracker.observe(&oversized),
            Err(TradeIdentityError::TradeIdTooLong),
        );

        assert_eq!(tracker.observations_total(), 0);
    }

    #[test]
    fn tracker_is_generation_scoped() {
        let mut first = tracker();

        first.observe("100").unwrap();

        let second = TradeIdentityTracker::new(first.stream_id(), 11, first.limits()).unwrap();

        assert_eq!(first.generation(), 10);
        assert_eq!(second.generation(), 11);

        assert!(!second.contains("100"));
    }

    #[test]
    fn nonnumeric_trade_ids_are_supported() {
        let mut tracker = tracker();

        assert_eq!(
            tracker.observe("execution-abc-123").unwrap(),
            TradeIdentityDecision::NewWithinWindow,
        );
    }

    #[test]
    fn exact_capacity_does_not_evict() {
        let mut tracker = tracker();

        tracker.observe_batch(&["100", "101", "102"]).unwrap();

        assert_eq!(tracker.retained_count(), 3);
        assert_eq!(tracker.evictions_total(), 0);
        assert!(!tracker.history_truncated());
    }

    #[test]
    fn rejects_zero_limits() {
        let result = TradeIdentityTracker::new(
            "stream",
            1,
            TradeIdentityLimits {
                max_ids: 0,
                max_id_bytes: 32,
                max_batch_records: 3,
            },
        );

        assert!(matches!(result, Err(TradeIdentityError::InvalidLimits)));
    }
}
