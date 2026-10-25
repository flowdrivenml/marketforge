use std::time::Duration;

use super::policy::BudgetId;

// ============================================================
// INDIVIDUAL BUDGET SNAPSHOT
// ============================================================

/// Point-in-time operational state of one rate-limit budget.
///
/// Values represent the manager's local accounting unless
/// explicitly identified as exchange-reported observations.
///
/// A snapshot contains no mutable runtime state, locks,
/// timers, or references to the manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitBudgetSnapshot {
    /// Unique identity of this exchange-enforced quota.
    pub id: BudgetId,

    // --------------------------------------------------------
    // CONFIGURED QUOTA
    // --------------------------------------------------------
    /// Maximum weighted usage permitted within the window.
    pub capacity: u64,

    /// Duration of the configured accounting window.
    pub window: Duration,

    // --------------------------------------------------------
    // LOCAL ACCOUNTING
    // --------------------------------------------------------
    /// Locally admitted usage still inside the window.
    ///
    /// Expired records must be removed before this
    /// snapshot is constructed.
    pub locally_used: u64,

    /// Remaining locally available capacity.
    ///
    /// An active cooldown may prevent admission even
    /// when this value is greater than zero.
    pub locally_available: u64,

    // --------------------------------------------------------
    // WAITING OPERATIONS
    // --------------------------------------------------------
    /// Number of pending operations referencing this budget.
    ///
    /// One operation can reference multiple budgets.
    /// Therefore, summing per-budget pending counts may
    /// exceed the global pending-operation count.
    ///
    /// A request referencing this budget may currently
    /// be blocked by another budget instead.
    pub pending_requests: usize,

    // --------------------------------------------------------
    // EXCHANGE THROTTLING
    // --------------------------------------------------------
    /// Remaining exchange-imposed cooldown.
    ///
    /// Zero means no active cooldown.
    pub cooldown_remaining: Duration,

    // --------------------------------------------------------
    // EXCHANGE OBSERVATION
    // --------------------------------------------------------
    /// Latest externally reported usage, if available.
    ///
    /// This is observational only and is not currently
    /// reconciled into the manager's local accounting.
    pub observed_exchange_usage: Option<RateLimitUsageObservation>,
}

// ============================================================
// EXCHANGE USAGE OBSERVATION
// ============================================================

/// Latest usage information reported by an exchange.
///
/// `age` is measured when the enclosing snapshot is taken.
///
/// Observations may be stale and may include activity
/// from other applications sharing the same exchange quota.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitUsageObservation {
    /// Exchange-reported usage.
    pub used: u64,

    /// Accounting window represented by the observation.
    pub window: Duration,

    /// Elapsed time since this observation was received.
    pub age: Duration,
}

// ============================================================
// GLOBAL SNAPSHOT
// ============================================================

/// Complete point-in-time view of the rate-limit manager.
///
/// All budgets and global counters must be captured under
/// one synchronization boundary using the same monotonic
/// timestamp.
///
/// Snapshot construction must not consume rate-limit
/// capacity or wait for quota availability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitSnapshot {
    /// All currently registered budgets.
    pub budgets: Vec<RateLimitBudgetSnapshot>,

    /// Number of unique pending acquisition operations.
    pub total_pending_requests: usize,

    /// Number of budgets with active exchange cooldowns.
    pub active_cooldowns: usize,
}

// ============================================================
// SNAPSHOT HELPERS
// ============================================================

impl RateLimitBudgetSnapshot {
    /// Whether this budget currently has an active
    /// exchange-imposed cooldown.
    pub fn is_cooling_down(&self) -> bool {
        !self.cooldown_remaining.is_zero()
    }

    /// Whether locally accounted usage has exhausted
    /// the configured capacity.
    ///
    /// This does not consider exchange-reported usage
    /// or additional budgets required by an operation.
    pub fn is_locally_exhausted(&self) -> bool {
        self.locally_available == 0
    }

    /// Local budget utilization, from 0.0 to 1.0.
    ///
    /// Intended for TUI gauges and operational displays.
    ///
    /// The quota configuration guarantees positive capacity.
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            return 0.0;
        }

        (self.locally_used as f64 / self.capacity as f64).clamp(0.0, 1.0)
    }
}

impl RateLimitSnapshot {
    /// Number of currently registered budgets.
    pub fn budget_count(&self) -> usize {
        self.budgets.len()
    }

    /// Retrieve a budget by its exact identity.
    pub fn budget(&self, id: &BudgetId) -> Option<&RateLimitBudgetSnapshot> {
        self.budgets.iter().find(|budget| &budget.id == id)
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::config::exchange::ExchangeId;

    use super::super::policy::BudgetScope;

    fn sample_budget() -> RateLimitBudgetSnapshot {
        RateLimitBudgetSnapshot {
            id: BudgetId::new(
                ExchangeId::Bitget,
                "uta_v3_rest",
                "rest_endpoint:GET:/api/v3/market/orderbook",
                BudgetScope::Egress("primary".into()),
            )
            .unwrap(),

            capacity: 20,
            window: Duration::from_secs(1),

            locally_used: 15,
            locally_available: 5,

            pending_requests: 3,
            cooldown_remaining: Duration::ZERO,

            observed_exchange_usage: None,
        }
    }

    #[test]
    fn calculates_utilization() {
        let budget = sample_budget();

        assert_eq!(budget.utilization(), 0.75);
        assert!(!budget.is_locally_exhausted());
        assert!(!budget.is_cooling_down());
    }

    #[test]
    fn identifies_exhaustion_and_cooldown() {
        let mut budget = sample_budget();

        budget.locally_used = 20;
        budget.locally_available = 0;
        budget.cooldown_remaining = Duration::from_secs(5);

        assert!(budget.is_locally_exhausted());
        assert!(budget.is_cooling_down());
        assert_eq!(budget.utilization(), 1.0);
    }

    #[test]
    fn supports_budget_lookup() {
        let budget = sample_budget();
        let id = budget.id.clone();

        let snapshot = RateLimitSnapshot {
            budgets: vec![budget],
            total_pending_requests: 3,
            active_cooldowns: 0,
        };

        assert_eq!(snapshot.budget_count(), 1);
        assert!(snapshot.budget(&id).is_some());
    }
}
