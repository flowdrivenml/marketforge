use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use super::policy::WindowQuota;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BudgetError {
    #[error("request cost must be greater than zero")]
    InvalidCost,

    #[error("request cost {cost} exceeds rate-limit capacity {capacity}")]
    CostExceedsCapacity { cost: u64, capacity: u64 },

    #[error("insufficient rate-limit capacity")]
    InsufficientCapacity,
}

// ============================================================
// USAGE RECORD
// ============================================================

/// One admitted operation and its weighted cost.
#[derive(Debug, Clone, Copy)]
struct UsageRecord {
    timestamp: Instant,
    cost: u64,
}

// ============================================================
// RATE-LIMIT BUDGET
// ============================================================

/// Weighted sliding-window rate-limit budget.
///
/// Each admitted operation consumes capacity until its
/// admission timestamp falls outside the configured window.
///
/// This type manages exactly one budget.
///
/// Synchronization and asynchronous waiting belong to
/// RateLimitManager, not this component.
#[derive(Debug)]
pub struct RateLimitBudget {
    quota: WindowQuota,

    usage: VecDeque<UsageRecord>,

    used_capacity: u64,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl RateLimitBudget {
    pub fn new(quota: WindowQuota) -> Self {
        Self {
            quota,
            usage: VecDeque::new(),
            used_capacity: 0,
        }
    }

    pub fn quota(&self) -> WindowQuota {
        self.quota
    }

    pub fn used_capacity(&self) -> u64 {
        self.used_capacity
    }

    pub fn available_capacity(&self) -> u64 {
        self.quota.capacity - self.used_capacity
    }

    pub fn is_empty(&self) -> bool {
        self.usage.is_empty()
    }

    // ========================================================
    // EXPIRATION
    // ========================================================

    /// Remove usage records whose sliding-window lifetime
    /// has expired.
    ///
    /// `now` must be monotonic relative to previously
    /// supplied timestamps.
    pub fn expire(&mut self, now: Instant) {
        while let Some(record) = self.usage.front() {
            let expired = now
                .checked_duration_since(record.timestamp)
                .is_some_and(|elapsed| elapsed >= self.quota.window);

            if !expired {
                break;
            }

            self.used_capacity -= record.cost;
            self.usage.pop_front();
        }
    }

    // ========================================================
    // COST VALIDATION
    // ========================================================

    fn validate_cost(&self, cost: u64) -> Result<(), BudgetError> {
        if cost == 0 {
            return Err(BudgetError::InvalidCost);
        }

        if cost > self.quota.capacity {
            return Err(BudgetError::CostExceedsCapacity {
                cost,
                capacity: self.quota.capacity,
            });
        }

        Ok(())
    }

    // ========================================================
    // CAPACITY CHECK
    // ========================================================

    /// Check whether an operation can be admitted now.
    ///
    /// Expired usage is removed before checking capacity.
    ///
    /// No capacity is consumed.
    pub fn can_acquire(&mut self, cost: u64, now: Instant) -> Result<bool, BudgetError> {
        self.validate_cost(cost)?;

        self.expire(now);

        Ok(cost <= self.available_capacity())
    }

    // ========================================================
    // ACQUISITION
    // ========================================================

    /// Attempt to admit an operation immediately.
    ///
    /// On success, its cost is recorded against the
    /// current sliding window.
    ///
    /// No waiting or sleeping occurs.
    pub fn acquire(&mut self, cost: u64, now: Instant) -> Result<(), BudgetError> {
        if !self.can_acquire(cost, now)? {
            return Err(BudgetError::InsufficientCapacity);
        }

        self.usage.push_back(UsageRecord {
            timestamp: now,
            cost,
        });

        self.used_capacity += cost;

        Ok(())
    }

    // ========================================================
    // NEXT AVAILABLE CAPACITY
    // ========================================================

    /// Calculate the minimum wait before enough capacity
    /// becomes available for an operation.
    ///
    /// Returns Duration::ZERO when admission is possible now.
    ///
    /// Does not consume capacity.
    pub fn next_available(&mut self, cost: u64, now: Instant) -> Result<Duration, BudgetError> {
        self.validate_cost(cost)?;

        self.expire(now);

        if cost <= self.available_capacity() {
            return Ok(Duration::ZERO);
        }

        let required_release = cost - self.available_capacity();

        let mut released = 0_u64;

        for record in &self.usage {
            released += record.cost;

            if released >= required_release {
                let expiration = record.timestamp + self.quota.window;

                return Ok(expiration.saturating_duration_since(now));
            }
        }

        // This should be unreachable when the budget's
        // internal accounting is consistent.
        Err(BudgetError::InsufficientCapacity)
    }
    // ========================================================
    // OBSERVABILITY
    // ========================================================

    /// Return current local usage and available capacity.
    ///
    /// Expired usage records are removed before calculating
    /// the snapshot.
    ///
    /// This operation does not consume rate-limit capacity.
    ///
    /// The caller must provide monotonic time consistent
    /// with previous budget operations.
    ///
    /// Returns:
    /// - used capacity
    /// - available capacity
    pub fn snapshot_usage(&mut self, now: Instant) -> (u64, u64) {
        self.expire(now);

        (self.used_capacity(), self.available_capacity())
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(capacity: u64, seconds: u64) -> RateLimitBudget {
        let quota = WindowQuota::new(capacity, Duration::from_secs(seconds)).unwrap();

        RateLimitBudget::new(quota)
    }

    #[test]
    fn admits_requests_within_capacity() {
        let mut budget = budget(10, 60);
        let now = Instant::now();

        assert!(budget.acquire(4, now).is_ok());
        assert!(budget.acquire(6, now).is_ok());

        assert_eq!(budget.used_capacity(), 10);
        assert_eq!(budget.available_capacity(), 0);
    }

    #[test]
    fn rejects_excessive_usage() {
        let mut budget = budget(10, 60);
        let now = Instant::now();

        budget.acquire(8, now).unwrap();

        assert!(matches!(
            budget.acquire(3, now),
            Err(BudgetError::InsufficientCapacity)
        ));

        assert_eq!(budget.used_capacity(), 8);
    }

    #[test]
    fn expires_usage_after_window() {
        let mut budget = budget(10, 5);
        let start = Instant::now();

        budget.acquire(10, start).unwrap();

        let later = start + Duration::from_secs(5);

        assert!(budget.can_acquire(10, later).unwrap());
        assert_eq!(budget.used_capacity(), 0);
    }

    #[test]
    fn calculates_wait_for_partial_capacity() {
        let mut budget = budget(10, 10);
        let start = Instant::now();

        budget.acquire(6, start).unwrap();

        budget.acquire(4, start + Duration::from_secs(2)).unwrap();

        let now = start + Duration::from_secs(3);

        // A new request of cost 5 needs the first
        // record to expire at t=10.
        assert_eq!(
            budget.next_available(5, now).unwrap(),
            Duration::from_secs(7)
        );

        // A request of cost 7 needs both records
        // to expire by t=12.
        assert_eq!(
            budget.next_available(7, now).unwrap(),
            Duration::from_secs(9)
        );
    }

    #[test]
    fn supports_weighted_operations() {
        let mut budget = budget(100, 60);
        let now = Instant::now();

        budget.acquire(20, now).unwrap();
        budget.acquire(30, now).unwrap();

        assert_eq!(budget.used_capacity(), 50);
        assert_eq!(budget.available_capacity(), 50);

        assert!(budget.can_acquire(50, now).unwrap());
        assert!(!budget.can_acquire(51, now).unwrap());
    }

    #[test]
    fn rejects_cost_exceeding_capacity() {
        let mut budget = budget(10, 60);
        let now = Instant::now();

        assert!(matches!(
            budget.acquire(11, now),
            Err(BudgetError::CostExceedsCapacity {
                cost: 11,
                capacity: 10,
            })
        ));
    }

    #[test]
    fn independent_budgets_do_not_share_usage() {
        let now = Instant::now();

        let mut first = budget(10, 60);
        let mut second = budget(10, 60);

        first.acquire(10, now).unwrap();

        assert_eq!(first.available_capacity(), 0);
        assert_eq!(second.available_capacity(), 10);

        assert!(second.acquire(10, now).is_ok());
    }
    #[test]
    fn snapshot_excludes_expired_usage() {
        let mut budget = budget(20, 5);
        let start = Instant::now();

        budget.acquire(15, start).unwrap();

        // Usage is still inside the five-second window.
        assert_eq!(
            budget.snapshot_usage(start + Duration::from_secs(3)),
            (15, 5)
        );

        // After the window expires, all capacity returns.
        assert_eq!(
            budget.snapshot_usage(start + Duration::from_secs(5)),
            (0, 20)
        );
    }
}
