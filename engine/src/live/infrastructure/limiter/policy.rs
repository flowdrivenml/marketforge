use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use crate::live::config::exchange::ExchangeId;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RateLimitPolicyError {
    #[error("rate-limit identifier '{0}' cannot be empty")]
    EmptyIdentifier(&'static str),

    #[error("rate-limit capacity must be greater than zero")]
    InvalidCapacity,

    #[error("rate-limit window must be greater than zero")]
    InvalidWindow,

    #[error("rate-limit request cost must be greater than zero")]
    InvalidCost,

    #[error("rate-limited operation requires at least one budget")]
    EmptyOperation,

    #[error("conflicting definitions for rate-limit budget {0:?}")]
    ConflictingBudget(BudgetId),

    #[error("unknown rate-limit budget {0:?}")]
    UnknownBudget(BudgetId),

    #[error("duplicate budget charge {0:?}")]
    DuplicateCharge(BudgetId),

    #[error(
        "request cost {cost} exceeds capacity {capacity} \
         for budget {budget:?}"
    )]
    CostExceedsCapacity {
        budget: BudgetId,
        cost: u64,
        capacity: u64,
    },
}

// ============================================================
// BUDGET SCOPE
// ============================================================

/// Identifies the resource against which the exchange
/// accounts rate-limit usage.
///
/// Endpoint-specific limits are represented by distinct
/// budget rule identifiers, not by a separate scope type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BudgetScope {
    /// Shared external network egress.
    ///
    /// Operations using the same exchange-enforced IP quota
    /// must use the same egress identity.
    ///
    /// The identity can describe a known public IP or
    /// a stable local egress-group identifier.
    Egress(String),

    /// One physical WebSocket connection.
    ///
    /// The ID must be unique across active connections
    /// within the runtime.
    Connection(u64),

    /// Exchange API credential or account scope.
    ///
    /// Use an opaque identifier, never the actual API key.
    Credential(String),

    /// Explicitly process-wide budget.
    ///
    /// This does not imply an exchange IP-based limit.
    Process,
}

// ============================================================
// BUDGET IDENTITY
// ============================================================

/// Unique identity of one exchange-enforced budget.
///
/// The service identifies the actual exchange API family.
///
/// Market categories must share a service identity only
/// when the exchange applies the same quota to them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BudgetId {
    pub exchange: ExchangeId,

    /// Examples:
    /// - spot_rest
    /// - coinm_rest
    /// - public_websocket
    ///
    /// The policy compiler determines which API families
    /// share or separate their budgets.
    pub service: String,

    /// Exchange-specific quota identifier.
    ///
    /// Examples:
    /// - request_weight
    /// - depth_snapshot
    /// - outbound_messages
    /// - connection_attempts
    /// - subscription_operations
    pub rule: String,

    pub scope: BudgetScope,
}

impl BudgetId {
    pub fn new(
        exchange: ExchangeId,
        service: impl Into<String>,
        rule: impl Into<String>,
        scope: BudgetScope,
    ) -> Result<Self, RateLimitPolicyError> {
        let service = service.into();
        let rule = rule.into();

        if service.trim().is_empty() {
            return Err(RateLimitPolicyError::EmptyIdentifier("service"));
        }

        if rule.trim().is_empty() {
            return Err(RateLimitPolicyError::EmptyIdentifier("rule"));
        }

        match &scope {
            BudgetScope::Egress(identity) | BudgetScope::Credential(identity)
                if identity.trim().is_empty() =>
            {
                return Err(RateLimitPolicyError::EmptyIdentifier("scope identity"));
            }

            _ => {}
        }

        Ok(Self {
            exchange,
            service,
            rule,
            scope,
        })
    }
}

// ============================================================
// TIME-WINDOW QUOTA
// ============================================================

/// Maximum weighted usage permitted during a time window.
///
/// The runtime budget implementation will enforce a
/// conservative sliding window using monotonic time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowQuota {
    pub capacity: u64,
    pub window: Duration,
}

impl WindowQuota {
    pub fn new(capacity: u64, window: Duration) -> Result<Self, RateLimitPolicyError> {
        if capacity == 0 {
            return Err(RateLimitPolicyError::InvalidCapacity);
        }

        if window.is_zero() {
            return Err(RateLimitPolicyError::InvalidWindow);
        }

        Ok(Self { capacity, window })
    }
}

// ============================================================
// BUDGET CHARGE
// ============================================================

/// The weighted cost of one operation against one budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetCharge {
    pub budget: BudgetId,
    pub cost: u64,
}

impl BudgetCharge {
    pub fn new(budget: BudgetId, cost: u64) -> Result<Self, RateLimitPolicyError> {
        if cost == 0 {
            return Err(RateLimitPolicyError::InvalidCost);
        }

        Ok(Self { budget, cost })
    }
}

// ============================================================
// REQUEST PRIORITY
// ============================================================

/// Scheduling preference for operations waiting on capacity.
///
/// Priority never bypasses exchange rate limits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RequestPriority {
    High,

    #[default]
    Normal,

    Low,
}

// ============================================================
// RATE-LIMITED OPERATION
// ============================================================

/// One operation requiring admission to all listed budgets.
///
/// The manager must acquire every applicable budget
/// atomically before the operation is permitted.
///
/// An empty charge list is rejected to prevent accidental
/// bypass of mandatory rate limiting.
#[derive(Debug, Clone)]
pub struct RateLimitedOperation {
    pub charges: Vec<BudgetCharge>,
    pub priority: RequestPriority,
}

impl RateLimitedOperation {
    pub fn new(charges: Vec<BudgetCharge>) -> Result<Self, RateLimitPolicyError> {
        if charges.is_empty() {
            return Err(RateLimitPolicyError::EmptyOperation);
        }

        let mut seen = HashSet::new();

        for charge in &charges {
            if !seen.insert(&charge.budget) {
                return Err(RateLimitPolicyError::DuplicateCharge(charge.budget.clone()));
            }
        }

        Ok(Self {
            charges,
            priority: RequestPriority::Normal,
        })
    }

    pub fn with_priority(mut self, priority: RequestPriority) -> Self {
        self.priority = priority;
        self
    }
}

// ============================================================
// POLICY SET
// ============================================================

/// Immutable-after-initialization collection of rate-limit
/// definitions.
///
/// The future RateLimitManager will create its mutable
/// budget state from this validated policy set.
#[derive(Debug, Clone, Default)]
pub struct RateLimitPolicySet {
    budgets: HashMap<BudgetId, WindowQuota>,
}

impl RateLimitPolicySet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a budget definition.
    ///
    /// Identical registrations are accepted because multiple
    /// exchange profiles may reference the same shared quota.
    ///
    /// Conflicting definitions of the same budget are rejected.
    pub fn insert(&mut self, id: BudgetId, quota: WindowQuota) -> Result<(), RateLimitPolicyError> {
        use std::collections::hash_map::Entry;

        match self.budgets.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(quota);
                Ok(())
            }

            Entry::Occupied(entry) if *entry.get() == quota => Ok(()),

            Entry::Occupied(entry) => {
                Err(RateLimitPolicyError::ConflictingBudget(entry.key().clone()))
            }
        }
    }

    pub fn get(&self, id: &BudgetId) -> Option<&WindowQuota> {
        self.budgets.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&BudgetId, &WindowQuota)> {
        self.budgets.iter()
    }

    pub fn len(&self) -> usize {
        self.budgets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.budgets.is_empty()
    }

    /// Validate that an operation references registered
    /// budgets and that each cost can fit its quota.
    ///
    /// This does not reserve capacity or enforce timing.
    pub fn validate_operation(
        &self,
        operation: &RateLimitedOperation,
    ) -> Result<(), RateLimitPolicyError> {
        if operation.charges.is_empty() {
            return Err(RateLimitPolicyError::EmptyOperation);
        }

        let mut seen = HashSet::new();

        for charge in &operation.charges {
            if charge.cost == 0 {
                return Err(RateLimitPolicyError::InvalidCost);
            }

            if !seen.insert(&charge.budget) {
                return Err(RateLimitPolicyError::DuplicateCharge(charge.budget.clone()));
            }

            let quota = self
                .budgets
                .get(&charge.budget)
                .ok_or_else(|| RateLimitPolicyError::UnknownBudget(charge.budget.clone()))?;

            if charge.cost > quota.capacity {
                return Err(RateLimitPolicyError::CostExceedsCapacity {
                    budget: charge.budget.clone(),
                    cost: charge.cost,
                    capacity: quota.capacity,
                });
            }
        }

        Ok(())
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(rule: &str) -> BudgetId {
        BudgetId::new(
            ExchangeId::Binance,
            "coinm_rest",
            rule,
            BudgetScope::Egress("primary".into()),
        )
        .unwrap()
    }

    #[test]
    fn rejects_invalid_quota() {
        assert!(WindowQuota::new(0, Duration::from_secs(1)).is_err());

        assert!(WindowQuota::new(100, Duration::ZERO).is_err());
    }

    #[test]
    fn allows_identical_shared_budget_definitions() {
        let mut policies = RateLimitPolicySet::new();

        let quota = WindowQuota::new(100, Duration::from_secs(60)).unwrap();

        policies.insert(budget("weight"), quota).unwrap();
        policies.insert(budget("weight"), quota).unwrap();

        assert_eq!(policies.len(), 1);
    }

    #[test]
    fn rejects_conflicting_budget_definitions() {
        let mut policies = RateLimitPolicySet::new();

        policies
            .insert(
                budget("weight"),
                WindowQuota::new(100, Duration::from_secs(60)).unwrap(),
            )
            .unwrap();

        assert!(
            policies
                .insert(
                    budget("weight"),
                    WindowQuota::new(200, Duration::from_secs(60),).unwrap(),
                )
                .is_err()
        );
    }

    #[test]
    fn rejects_duplicate_operation_charges() {
        let first = BudgetCharge::new(budget("weight"), 10).unwrap();

        let second = BudgetCharge::new(budget("weight"), 20).unwrap();

        assert!(RateLimitedOperation::new(vec![first, second]).is_err());
    }

    #[test]
    fn validates_multiple_budget_charges() {
        let mut policies = RateLimitPolicySet::new();

        policies
            .insert(
                budget("weight"),
                WindowQuota::new(100, Duration::from_secs(60)).unwrap(),
            )
            .unwrap();

        policies
            .insert(
                budget("depth_snapshot"),
                WindowQuota::new(10, Duration::from_secs(1)).unwrap(),
            )
            .unwrap();

        let operation = RateLimitedOperation::new(vec![
            BudgetCharge::new(budget("weight"), 20).unwrap(),
            BudgetCharge::new(budget("depth_snapshot"), 1).unwrap(),
        ])
        .unwrap();

        assert!(policies.validate_operation(&operation).is_ok());
    }
}
