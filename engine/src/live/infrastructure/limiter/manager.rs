use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::Notify;

use super::{
    budget::{BudgetError, RateLimitBudget},
    feedback::{RateLimitFeedback, RateLimitFeedbackError},
    policy::{
        BudgetId, BudgetScope, RateLimitPolicyError, RateLimitPolicySet, RateLimitedOperation,
        RequestPriority,
    },
    snapshot::{RateLimitBudgetSnapshot, RateLimitSnapshot, RateLimitUsageObservation},
};

// ============================================================
// CONSTANTS
// ============================================================

/// Increase a waiting request's effective priority every
/// five seconds.
const PRIORITY_AGING_SECONDS: u64 = 5;

/// Maximum interval between admission re-evaluations.
///
/// This ensures progress even when another eligible waiter
/// has not yet been scheduled by the Tokio runtime.
///
/// It is a scheduling fallback, not a rate-limit interval.
const MAX_RECHECK_INTERVAL: Duration = Duration::from_millis(250);

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RateLimitManagerError {
    #[error(transparent)]
    Policy(#[from] RateLimitPolicyError),

    #[error(transparent)]
    Budget(#[from] BudgetError),

    #[error(transparent)]
    Feedback(#[from] RateLimitFeedbackError),

    #[error("maximum pending requests must be greater than zero")]
    InvalidPendingLimit,

    #[error("rate-limit waiting queue is full")]
    QueueFull,

    #[error("rate-limit acquisition timed out")]
    Timeout,

    #[error("rate-limit request identifier exhausted")]
    RequestIdExhausted,

    #[error("pending rate-limit request no longer exists")]
    RequestNotFound,
}

// ============================================================
// PENDING REQUEST
// ============================================================

#[derive(Debug, Clone)]
struct PendingRequest {
    id: u64,
    operation: RateLimitedOperation,
    created_at: Instant,
}

impl PendingRequest {
    fn effective_priority(&self, now: Instant) -> u64 {
        let base = match self.operation.priority {
            RequestPriority::High => 2,
            RequestPriority::Normal => 1,
            RequestPriority::Low => 0,
        };

        let waited = now.saturating_duration_since(self.created_at).as_secs();

        base + waited / PRIORITY_AGING_SECONDS
    }
}

// ============================================================
// EXCHANGE USAGE OBSERVATION
// ============================================================

/// Latest exchange-reported usage for one budget.
///
/// Observations are informational in this implementation.
///
/// They do not replace or increment local sliding-window
/// accounting because doing so without request correlation
/// could double-count usage.
#[derive(Debug, Clone)]
pub struct ObservedUsage {
    pub used: u64,
    pub window: Duration,
    pub observed_at: Instant,
}

// ============================================================
// MANAGER STATE
// ============================================================

#[derive(Debug)]
struct ManagerState {
    // Local sliding-window accounting.
    budgets: HashMap<BudgetId, RateLimitBudget>,

    // Exchange-directed cooldown deadlines.
    cooldowns: HashMap<BudgetId, Instant>,

    // Latest external usage observations.
    observations: HashMap<BudgetId, ObservedUsage>,

    // Bounded waiting requests.
    pending: Vec<PendingRequest>,

    next_request_id: u64,
    max_pending: usize,
}

// ============================================================
// SHARED MANAGER
// ============================================================

#[derive(Debug)]
struct ManagerInner {
    state: Mutex<ManagerState>,
    changed: Notify,
}

/// Shared asynchronous rate-limit manager.
///
/// Clones share all budgets, cooldowns, observations,
/// and pending acquisition requests.
///
/// Operations consuming multiple budgets are admitted
/// atomically under a single synchronization boundary.
#[derive(Debug, Clone)]
pub struct RateLimitManager {
    inner: Arc<ManagerInner>,
}

// ============================================================
// PENDING REQUEST CLEANUP
// ============================================================

/// Ensures cancellation removes a request from the
/// manager's waiting queue.
struct PendingGuard {
    inner: Arc<ManagerInner>,
    request_id: u64,
    armed: bool,
}

impl PendingGuard {
    fn new(inner: Arc<ManagerInner>, request_id: u64) -> Self {
        Self {
            inner,
            request_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            state
                .pending
                .retain(|request| request.id != self.request_id);
        }

        self.inner.changed.notify_waiters();
    }
}

// ============================================================
// ADMISSION RESULT
// ============================================================

enum Admission {
    Granted,
    Wait(Duration),
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl RateLimitManager {
    /// Initialize all registered budgets with zero
    /// locally observed usage.
    pub fn new(
        policies: RateLimitPolicySet,
        max_pending: usize,
    ) -> Result<Self, RateLimitManagerError> {
        if max_pending == 0 {
            return Err(RateLimitManagerError::InvalidPendingLimit);
        }

        let budgets = policies
            .iter()
            .map(|(id, quota)| (id.clone(), RateLimitBudget::new(*quota)))
            .collect();

        Ok(Self {
            inner: Arc::new(ManagerInner {
                state: Mutex::new(ManagerState {
                    budgets,
                    cooldowns: HashMap::new(),
                    observations: HashMap::new(),
                    pending: Vec::new(),
                    next_request_id: 0,
                    max_pending,
                }),
                changed: Notify::new(),
            }),
        })
    }

    // ========================================================
    // PUBLIC ACQUISITION
    // ========================================================

    /// Wait until all applicable budgets permit the operation.
    ///
    /// Successful acquisition consumes every charge atomically.
    ///
    /// Dropping the future removes its pending request.
    pub async fn acquire(
        &self,
        operation: &RateLimitedOperation,
    ) -> Result<(), RateLimitManagerError> {
        let request_id = self.register(operation)?;

        let mut guard = PendingGuard::new(self.inner.clone(), request_id);

        loop {
            // Register the notification before checking state
            // to avoid losing a concurrent wake-up.
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            match self.try_admit(request_id)? {
                Admission::Granted => {
                    guard.disarm();
                    return Ok(());
                }

                Admission::Wait(duration) => {
                    tokio::select! {
                        _ = &mut notified => {}

                        _ = tokio::time::sleep(duration) => {}
                    }
                }
            }
        }
    }
    // ========================================================
    // RATE-LIMIT SNAPSHOT
    // ========================================================

    /// Capture the current operational state of every
    /// registered rate-limit budget.
    ///
    /// The snapshot is collected under one manager lock
    /// using one monotonic timestamp.
    ///
    /// Expired local usage is removed before reporting.
    ///
    /// Exchange-reported usage is observational only and
    /// does not modify local quota accounting.
    ///
    /// No rate-limit capacity is consumed.
    pub fn snapshot(&self) -> RateLimitSnapshot {
        let mut state = self.lock_state();

        let now = Instant::now();

        // ====================================================
        // PENDING REQUEST COUNTS
        // ====================================================

        // Count each pending operation once globally.
        //
        // A pending operation may reference multiple budgets.
        // Each referenced budget receives one pending count.
        let total_pending_requests = state.pending.len();

        let mut pending_by_budget: HashMap<BudgetId, usize> = HashMap::new();

        for request in &state.pending {
            for charge in &request.operation.charges {
                *pending_by_budget.entry(charge.budget.clone()).or_insert(0) += 1;
            }
        }

        // ====================================================
        // EXCHANGE COOLDOWNS
        // ====================================================

        // Remove expired cooldowns before collecting
        // the current snapshot.
        state.cooldowns.retain(|_, deadline| *deadline > now);

        let active_cooldowns = state.cooldowns.len();

        // Clone the small metadata maps so we can iterate
        // mutably over budget accounting without holding
        // overlapping borrows of ManagerState.
        let cooldowns = state.cooldowns.clone();
        let observations = state.observations.clone();

        // ====================================================
        // INDIVIDUAL BUDGET SNAPSHOTS
        // ====================================================

        let mut budgets = Vec::with_capacity(state.budgets.len());

        for (id, budget) in &mut state.budgets {
            // Expire old local usage before reporting.
            let (locally_used, locally_available) = budget.snapshot_usage(now);

            let quota = budget.quota();

            // Remaining exchange-imposed cooldown.
            let cooldown_remaining = cooldowns
                .get(id)
                .map(|deadline| deadline.saturating_duration_since(now))
                .unwrap_or(Duration::ZERO);

            // Latest externally reported usage.
            //
            // This remains separate from local accounting.
            let observed_exchange_usage =
                observations
                    .get(id)
                    .map(|observation| RateLimitUsageObservation {
                        used: observation.used,
                        window: observation.window,
                        age: now.saturating_duration_since(observation.observed_at),
                    });

            let pending_requests = pending_by_budget.get(id).copied().unwrap_or(0);

            budgets.push(RateLimitBudgetSnapshot {
                id: id.clone(),

                capacity: quota.capacity,
                window: quota.window,

                locally_used,
                locally_available,

                pending_requests,
                cooldown_remaining,

                observed_exchange_usage,
            });
        }

        // ====================================================
        // DETERMINISTIC ORDER
        // ====================================================

        // HashMap iteration order is not stable.
        //
        // Sort by exchange, service, rule, and scope
        // so REST and TUI output remains predictable.
        budgets.sort_by(|a, b| {
            a.id.exchange
                .as_str()
                .cmp(b.id.exchange.as_str())
                .then_with(|| a.id.service.cmp(&b.id.service))
                .then_with(|| a.id.rule.cmp(&b.id.rule))
                .then_with(|| {
                    fn scope_key(scope: &BudgetScope) -> (u8, String) {
                        match scope {
                            BudgetScope::Egress(value) => (0, value.clone()),

                            BudgetScope::Connection(value) => (1, format!("{value:020}")),

                            BudgetScope::Credential(value) => (2, value.clone()),

                            BudgetScope::Process => (3, String::new()),
                        }
                    }

                    scope_key(&a.id.scope).cmp(&scope_key(&b.id.scope))
                })
        });

        // ====================================================
        // COMPLETE SNAPSHOT
        // ====================================================

        RateLimitSnapshot {
            budgets,
            total_pending_requests,
            active_cooldowns,
        }
    }
    /// Acquire capacity before a timeout.
    ///
    /// Timeout cancellation automatically removes the
    /// waiting request through PendingGuard.
    pub async fn acquire_with_timeout(
        &self,
        operation: &RateLimitedOperation,
        timeout: Duration,
    ) -> Result<(), RateLimitManagerError> {
        tokio::time::timeout(timeout, self.acquire(operation))
            .await
            .map_err(|_| RateLimitManagerError::Timeout)?
    }

    // ========================================================
    // REGISTRATION
    // ========================================================

    fn register(&self, operation: &RateLimitedOperation) -> Result<u64, RateLimitManagerError> {
        let request_id;

        {
            let mut state = self.lock_state();

            Self::validate_operation(&state, operation)?;

            if state.pending.len() >= state.max_pending {
                return Err(RateLimitManagerError::QueueFull);
            }

            request_id = state.next_request_id;

            state.next_request_id = state
                .next_request_id
                .checked_add(1)
                .ok_or(RateLimitManagerError::RequestIdExhausted)?;

            state.pending.push(PendingRequest {
                id: request_id,
                operation: operation.clone(),
                created_at: Instant::now(),
            });
        }

        self.inner.changed.notify_waiters();

        Ok(request_id)
    }

    // ========================================================
    // OPERATION VALIDATION
    // ========================================================

    fn validate_operation(
        state: &ManagerState,
        operation: &RateLimitedOperation,
    ) -> Result<(), RateLimitManagerError> {
        if operation.charges.is_empty() {
            return Err(RateLimitPolicyError::EmptyOperation.into());
        }

        let mut seen = HashSet::new();

        for charge in &operation.charges {
            if charge.cost == 0 {
                return Err(RateLimitPolicyError::InvalidCost.into());
            }

            if !seen.insert(&charge.budget) {
                return Err(RateLimitPolicyError::DuplicateCharge(charge.budget.clone()).into());
            }

            let budget = state
                .budgets
                .get(&charge.budget)
                .ok_or_else(|| RateLimitPolicyError::UnknownBudget(charge.budget.clone()))?;

            if charge.cost > budget.quota().capacity {
                return Err(RateLimitPolicyError::CostExceedsCapacity {
                    budget: charge.budget.clone(),
                    cost: charge.cost,
                    capacity: budget.quota().capacity,
                }
                .into());
            }
        }

        Ok(())
    }

    // ========================================================
    // COOLDOWN STATE
    // ========================================================

    /// Return the remaining exchange-imposed cooldown.
    ///
    /// Expired cooldown entries are removed lazily.
    fn remaining_cooldown(state: &mut ManagerState, id: &BudgetId, now: Instant) -> Duration {
        match state.cooldowns.get(id).copied() {
            Some(deadline) if deadline > now => deadline.duration_since(now),

            Some(_) => {
                state.cooldowns.remove(id);
                Duration::ZERO
            }

            None => Duration::ZERO,
        }
    }

    // ========================================================
    // OPERATION READINESS
    // ========================================================

    /// Determine how long an operation must wait for
    /// all its budgets and cooldowns.
    ///
    /// A zero duration means it is eligible for admission.
    fn operation_wait(
        state: &mut ManagerState,
        operation: &RateLimitedOperation,
        now: Instant,
    ) -> Result<Duration, RateLimitManagerError> {
        let mut wait = Duration::ZERO;

        for charge in &operation.charges {
            let cooldown = Self::remaining_cooldown(state, &charge.budget, now);

            let budget = state
                .budgets
                .get_mut(&charge.budget)
                .ok_or_else(|| RateLimitPolicyError::UnknownBudget(charge.budget.clone()))?;

            let capacity_wait = budget.next_available(charge.cost, now)?;

            wait = wait.max(cooldown).max(capacity_wait);
        }

        Ok(wait)
    }

    // ========================================================
    // ATOMIC ADMISSION
    // ========================================================

    fn try_admit(&self, request_id: u64) -> Result<Admission, RateLimitManagerError> {
        let mut admitted = false;

        let result = {
            let mut state = self.lock_state();
            let now = Instant::now();

            let mut candidates = state.pending.clone();

            candidates.sort_by(|a, b| {
                b.effective_priority(now)
                    .cmp(&a.effective_priority(now))
                    .then_with(|| a.id.cmp(&b.id))
            });

            if !candidates.iter().any(|request| request.id == request_id) {
                return Err(RateLimitManagerError::RequestNotFound);
            }

            // Find the highest-priority request whose
            // complete set of budgets is available.
            //
            // A blocked request does not prevent unrelated
            // eligible requests from proceeding.
            let mut selected_id = None;
            let mut selected_operation = None;

            for candidate in &candidates {
                let wait = Self::operation_wait(&mut state, &candidate.operation, now)?;

                if wait.is_zero() {
                    selected_id = Some(candidate.id);
                    selected_operation = Some(candidate.operation.clone());
                    break;
                }
            }

            if selected_id == Some(request_id) {
                let operation = selected_operation.expect("selected request has an operation");

                // All charges were checked while holding
                // this same lock.
                //
                // No other acquisition can intervene before
                // the following commits.
                for charge in &operation.charges {
                    let budget = state.budgets.get_mut(&charge.budget).ok_or_else(|| {
                        RateLimitPolicyError::UnknownBudget(charge.budget.clone())
                    })?;

                    budget.acquire(charge.cost, now)?;
                }

                state.pending.retain(|request| request.id != request_id);

                admitted = true;

                Admission::Granted
            } else {
                // Determine the earliest useful recheck time.
                //
                // This is intentionally capped. If another
                // request is currently selected but its
                // future has not been polled, our request
                // must still make progress.
                let mut minimum_wait = MAX_RECHECK_INTERVAL;

                for candidate in &candidates {
                    let wait = Self::operation_wait(&mut state, &candidate.operation, now)?;

                    if !wait.is_zero() {
                        minimum_wait = minimum_wait.min(wait);
                    }
                }

                Admission::Wait(minimum_wait)
            }
        };

        if admitted {
            self.inner.changed.notify_waiters();
        }

        Ok(result)
    }

    // ========================================================
    // EXCHANGE FEEDBACK
    // ========================================================

    /// Apply normalized exchange feedback.
    ///
    /// Cooldowns affect admission immediately.
    ///
    /// Usage observations are retained for diagnostics
    /// but do not change local budget accounting.
    ///
    /// All feedback is validated before state mutation.
    pub fn observe_feedback(
        &self,
        feedback: &RateLimitFeedback,
    ) -> Result<(), RateLimitManagerError> {
        let mut state = self.lock_state();

        Self::validate_feedback(&state, feedback)?;

        let now = Instant::now();

        if let Some(cooldown) = &feedback.cooldown {
            let deadline = now + cooldown.duration;

            for id in &cooldown.budgets {
                state
                    .cooldowns
                    .entry(id.clone())
                    .and_modify(|existing| {
                        // Never shorten an active cooldown
                        // based on another feedback event.
                        if deadline > *existing {
                            *existing = deadline;
                        }
                    })
                    .or_insert(deadline);
            }
        }

        for usage in &feedback.usage {
            state.observations.insert(
                usage.budget.clone(),
                ObservedUsage {
                    used: usage.used,
                    window: usage.window,
                    observed_at: now,
                },
            );
        }

        drop(state);

        self.inner.changed.notify_waiters();

        Ok(())
    }

    // ========================================================
    // FEEDBACK VALIDATION
    // ========================================================

    fn validate_feedback(
        state: &ManagerState,
        feedback: &RateLimitFeedback,
    ) -> Result<(), RateLimitManagerError> {
        if let Some(cooldown) = &feedback.cooldown {
            if cooldown.budgets.is_empty() {
                return Err(RateLimitFeedbackError::EmptyBudgets.into());
            }

            if cooldown.duration.is_zero() {
                return Err(RateLimitFeedbackError::InvalidCooldown.into());
            }

            let mut seen = HashSet::new();

            for id in &cooldown.budgets {
                if !seen.insert(id) {
                    return Err(RateLimitFeedbackError::DuplicateBudgets.into());
                }

                Self::validate_feedback_budget(state, id)?;
            }
        }

        let mut seen_usage = HashSet::new();

        for usage in &feedback.usage {
            if usage.window.is_zero() {
                return Err(RateLimitFeedbackError::InvalidUsageWindow.into());
            }

            if !seen_usage.insert(&usage.budget) {
                return Err(RateLimitFeedbackError::DuplicateBudgets.into());
            }

            Self::validate_feedback_budget(state, &usage.budget)?;
        }

        Ok(())
    }

    fn validate_feedback_budget(
        state: &ManagerState,
        id: &BudgetId,
    ) -> Result<(), RateLimitManagerError> {
        if !state.budgets.contains_key(id) {
            return Err(RateLimitPolicyError::UnknownBudget(id.clone()).into());
        }

        Ok(())
    }

    // ========================================================
    // OBSERVABILITY
    // ========================================================

    /// Remaining cooldown for one registered budget.
    ///
    /// Returns None for an unknown budget.
    pub fn cooldown_remaining(&self, id: &BudgetId) -> Option<Duration> {
        let mut state = self.lock_state();

        if !state.budgets.contains_key(id) {
            return None;
        }

        Some(Self::remaining_cooldown(&mut state, id, Instant::now()))
    }

    /// Latest exchange-reported usage for one budget.
    ///
    /// The observation may be stale and is not an
    /// authoritative statement of current availability.
    pub fn observed_usage(&self, id: &BudgetId) -> Option<ObservedUsage> {
        let state = self.lock_state();

        state.observations.get(id).cloned()
    }

    pub fn pending_count(&self) -> usize {
        self.lock_state().pending.len()
    }

    pub fn budget_count(&self) -> usize {
        self.lock_state().budgets.len()
    }

    // ========================================================
    // INTERNAL SYNCHRONIZATION
    // ========================================================

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ManagerState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::config::exchange::ExchangeId;

    use super::super::{
        feedback::{CooldownFeedback, ThrottleKind, UsageFeedback},
        policy::{BudgetCharge, BudgetScope, WindowQuota},
    };

    fn budget_id(rule: &str) -> BudgetId {
        BudgetId::new(
            ExchangeId::Binance,
            "coinm_rest",
            rule,
            BudgetScope::Egress("primary".into()),
        )
        .unwrap()
    }

    fn setup(capacity: u64, window: Duration) -> (RateLimitManager, BudgetId) {
        let id = budget_id("request_weight");

        let mut policies = RateLimitPolicySet::new();

        policies
            .insert(id.clone(), WindowQuota::new(capacity, window).unwrap())
            .unwrap();

        (RateLimitManager::new(policies, 100).unwrap(), id)
    }

    fn operation(id: BudgetId, cost: u64) -> RateLimitedOperation {
        RateLimitedOperation::new(vec![BudgetCharge::new(id, cost).unwrap()]).unwrap()
    }

    #[tokio::test]
    async fn admits_available_operations() {
        let (manager, id) = setup(10, Duration::from_secs(60));

        manager.acquire(&operation(id.clone(), 4)).await.unwrap();

        manager.acquire(&operation(id, 6)).await.unwrap();

        assert_eq!(manager.pending_count(), 0);
    }

    #[tokio::test]
    async fn waits_for_capacity_to_return() {
        let (manager, id) = setup(1, Duration::from_millis(30));

        manager.acquire(&operation(id.clone(), 1)).await.unwrap();

        manager
            .acquire_with_timeout(&operation(id, 1), Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cooldown_blocks_admission() {
        let (manager, id) = setup(10, Duration::from_secs(60));

        let cooldown = CooldownFeedback::new(
            vec![id.clone()],
            ThrottleKind::RateLimited,
            Duration::from_millis(50),
        )
        .unwrap();

        manager
            .observe_feedback(&RateLimitFeedback::new().with_cooldown(cooldown))
            .unwrap();

        let result = manager
            .acquire_with_timeout(&operation(id.clone(), 1), Duration::from_millis(10))
            .await;

        assert!(matches!(result, Err(RateLimitManagerError::Timeout)));

        manager
            .acquire_with_timeout(&operation(id, 1), Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cooldown_only_affects_selected_budgets() {
        let first = budget_id("request_weight");
        let second = budget_id("snapshot_endpoint");

        let mut policies = RateLimitPolicySet::new();

        for id in [&first, &second] {
            policies
                .insert(
                    id.clone(),
                    WindowQuota::new(10, Duration::from_secs(60)).unwrap(),
                )
                .unwrap();
        }

        let manager = RateLimitManager::new(policies, 100).unwrap();

        let cooldown = CooldownFeedback::new(
            vec![first.clone()],
            ThrottleKind::RateLimited,
            Duration::from_secs(1),
        )
        .unwrap();

        manager
            .observe_feedback(&RateLimitFeedback::new().with_cooldown(cooldown))
            .unwrap();

        // An unrelated budget remains usable.
        manager
            .acquire_with_timeout(&operation(second, 1), Duration::from_millis(100))
            .await
            .unwrap();

        assert!(manager.cooldown_remaining(&first).unwrap() > Duration::ZERO);
    }

    #[tokio::test]
    async fn usage_feedback_does_not_double_count() {
        let (manager, id) = setup(10, Duration::from_secs(60));

        manager.acquire(&operation(id.clone(), 5)).await.unwrap();

        let feedback = RateLimitFeedback::new()
            .with_usage(UsageFeedback::new(id.clone(), 9, Duration::from_secs(60)).unwrap());

        manager.observe_feedback(&feedback).unwrap();

        let observed = manager.observed_usage(&id).unwrap();

        assert_eq!(observed.used, 9);

        // The observation does not increase locally
        // accounted usage from 5 to 14.
        manager.acquire(&operation(id, 5)).await.unwrap();
    }

    #[tokio::test]
    async fn timeout_removes_pending_request() {
        let (manager, id) = setup(1, Duration::from_secs(60));

        manager.acquire(&operation(id.clone(), 1)).await.unwrap();

        let result = manager
            .acquire_with_timeout(&operation(id, 1), Duration::from_millis(10))
            .await;

        assert!(matches!(result, Err(RateLimitManagerError::Timeout)));

        assert_eq!(manager.pending_count(), 0);
    }

    #[tokio::test]
    async fn multi_budget_admission_is_atomic() {
        let shared = budget_id("shared");
        let endpoint = budget_id("endpoint");

        let mut policies = RateLimitPolicySet::new();

        policies
            .insert(
                shared.clone(),
                WindowQuota::new(10, Duration::from_secs(60)).unwrap(),
            )
            .unwrap();

        policies
            .insert(
                endpoint.clone(),
                WindowQuota::new(1, Duration::from_secs(60)).unwrap(),
            )
            .unwrap();

        let manager = RateLimitManager::new(policies, 100).unwrap();

        manager
            .acquire(&operation(endpoint.clone(), 1))
            .await
            .unwrap();

        let combined = RateLimitedOperation::new(vec![
            BudgetCharge::new(shared.clone(), 5).unwrap(),
            BudgetCharge::new(endpoint, 1).unwrap(),
        ])
        .unwrap();

        let result = manager
            .acquire_with_timeout(&combined, Duration::from_millis(10))
            .await;

        assert!(matches!(result, Err(RateLimitManagerError::Timeout)));

        // The unsuccessful combined request must
        // not consume any of the shared budget.
        manager.acquire(&operation(shared, 10)).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_feedback_for_unknown_budget() {
        let (manager, _) = setup(10, Duration::from_secs(60));

        let unknown = budget_id("unknown");

        let feedback = RateLimitFeedback::new()
            .with_usage(UsageFeedback::new(unknown, 5, Duration::from_secs(60)).unwrap());

        assert!(matches!(
            manager.observe_feedback(&feedback),
            Err(RateLimitManagerError::Policy(
                RateLimitPolicyError::UnknownBudget(_)
            ))
        ));
    }
    #[tokio::test]
    async fn snapshot_expires_old_usage() {
        let (manager, id) = setup(10, Duration::from_millis(20));

        manager.acquire(&operation(id.clone(), 10)).await.unwrap();

        let initial = manager.snapshot();

        assert_eq!(initial.budget(&id).unwrap().locally_used, 10);

        tokio::time::sleep(Duration::from_millis(40)).await;

        let later = manager.snapshot();

        let budget = later.budget(&id).unwrap();

        assert_eq!(budget.locally_used, 0);
        assert_eq!(budget.locally_available, 10);
    }
    #[tokio::test]
    async fn snapshot_reports_cooldown_and_exchange_usage() {
        let (manager, id) = setup(100, Duration::from_secs(60));

        let cooldown = CooldownFeedback::new(
            vec![id.clone()],
            ThrottleKind::RateLimited,
            Duration::from_secs(5),
        )
        .unwrap();

        let usage = UsageFeedback::new(id.clone(), 85, Duration::from_secs(60)).unwrap();

        manager
            .observe_feedback(
                &RateLimitFeedback::new()
                    .with_cooldown(cooldown)
                    .with_usage(usage),
            )
            .unwrap();

        let snapshot = manager.snapshot();

        assert_eq!(snapshot.active_cooldowns, 1);

        let budget = snapshot.budget(&id).unwrap();

        assert!(budget.is_cooling_down());

        assert!(budget.cooldown_remaining <= Duration::from_secs(5));

        let observed = budget.observed_exchange_usage.as_ref().unwrap();

        assert_eq!(observed.used, 85);
        assert_eq!(observed.window, Duration::from_secs(60));

        // External observations are not added to
        // locally accounted usage.
        assert_eq!(budget.locally_used, 0);
        assert_eq!(budget.locally_available, 100);
    }
}
