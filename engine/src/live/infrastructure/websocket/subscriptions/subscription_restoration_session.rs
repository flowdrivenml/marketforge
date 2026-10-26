use std::collections::BTreeSet;

use thiserror::Error;

use super::{
    ConnectionFence, ConnectionStamp, PlannedSubscription, StreamRecoveryCoordinator,
    StreamRecoveryError, StreamRecoveryToken, SubscriptionRestorationPlan,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum RestorationPreparationError {
    #[error("restoration plan is stale")]
    StalePlan,

    #[error(
        "subscription '{stream_id}' belongs to route {actual:?}, \
         but this connection owns route {expected:?}"
    )]
    WrongRoute {
        stream_id: String,
        expected: Option<String>,
        actual: Option<String>,
    },

    #[error("duplicate restoration stream: {0}")]
    DuplicateStream(String),

    #[error("stream already registered in recovery authority: {0}")]
    ExistingStream(String),

    #[error(transparent)]
    Recovery(#[from] StreamRecoveryError),
    #[error("unsupported restoration channel: {0}")]
    UnsupportedChannel(String),
}

// ============================================================
// PREPARED ENTRY
// ============================================================

/// One native subscription request associated with an
/// exact source-stream recovery token.
///
/// This does not represent a transmitted or acknowledged
/// exchange subscription.
#[derive(Debug)]
pub struct PreparedRestorationEntry {
    pub token: StreamRecoveryToken,
    pub subscription: PlannedSubscription,
}

// ============================================================
// PREPARED RESTORATION
// ============================================================

/// All subscriptions prepared for one physical connection.
///
/// The recovery coordinator remains owned by the caller.
/// This object does not duplicate its lifecycle authority.
#[derive(Debug)]
pub struct PreparedRestoration {
    stamp: ConnectionStamp,
    demand_revision: u64,
    route: Option<String>,
    entries: Vec<PreparedRestorationEntry>,
}

impl PreparedRestoration {
    pub fn stamp(&self) -> &ConnectionStamp {
        &self.stamp
    }

    pub fn demand_revision(&self) -> u64 {
        self.demand_revision
    }

    pub fn route(&self) -> Option<&str> {
        self.route.as_deref()
    }

    pub fn entries(&self) -> &[PreparedRestorationEntry] {
        &self.entries
    }

    pub fn into_entries(self) -> Vec<PreparedRestorationEntry> {
        self.entries
    }
}

// ============================================================
// PREPARATION
// ============================================================

pub struct SubscriptionRestorationSession;

impl SubscriptionRestorationSession {
    /// Prepare the desired subscriptions for one physical
    /// WebSocket connection.
    ///
    /// The caller must serialize this operation with:
    /// - Demand revision changes.
    /// - Other stream-recovery coordinator mutations.
    /// - Subscription execution for this connection slot.
    ///
    /// A failure after epoch allocation begins requires
    /// abandoning this generation's prepared state.
    ///
    /// Do not reuse the recovery coordinator with another
    /// live connection concurrently.
    pub fn prepare(
        plan: SubscriptionRestorationPlan,
        fence: &ConnectionFence,
        current_demand_revision: u64,
        connection_route: Option<&str>,
        recovery: &mut StreamRecoveryCoordinator,
    ) -> Result<PreparedRestoration, RestorationPreparationError> {
        // --------------------------------------------------------
        // GENERATION AND DEMAND
        // --------------------------------------------------------

        if !plan.is_current(fence, current_demand_revision) {
            return Err(RestorationPreparationError::StalePlan);
        }

        // --------------------------------------------------------
        // PREFLIGHT ALL ENTRIES
        // --------------------------------------------------------

        let mut seen = BTreeSet::new();

        let mut desired = Vec::with_capacity(plan.entries().len());

        for entry in plan.entries() {
            // One physical connection owns one native route.
            if entry.resolved.route.as_deref() != connection_route {
                return Err(RestorationPreparationError::WrongRoute {
                    stream_id: entry.stream_id.clone(),
                    expected: connection_route.map(str::to_owned),
                    actual: entry.resolved.route.clone(),
                });
            }

            if !seen.insert(entry.stream_id.as_str()) {
                return Err(RestorationPreparationError::DuplicateStream(
                    entry.stream_id.clone(),
                ));
            }

            let kind = match entry.resolved.channel_name.as_str() {
                "trades" => super::RecoveryStreamKind::Trades,

                "depth" => super::RecoveryStreamKind::Depth,

                other => {
                    return Err(RestorationPreparationError::UnsupportedChannel(
                        other.to_owned(),
                    ));
                }
            };

            desired.push((entry.stream_id.clone(), kind));
        }

        // --------------------------------------------------------
        // ATOMIC RECOVERY RECONCILIATION
        // --------------------------------------------------------

        // This single synchronous operation:
        //
        // - Registers new streams.
        // - Rebinds existing streams.
        // - Rejects conflicting stream kinds.
        // - Retires absent streams.
        // - Allocates fresh epochs.
        //
        // Any error leaves the coordinator unchanged.

        let tokens = recovery.reconcile_and_bind(fence, plan.stamp(), &desired)?;

        // --------------------------------------------------------
        // BUILD PREPARED RESTORATION
        // --------------------------------------------------------

        let stamp = plan.stamp().clone();
        let demand_revision = plan.demand_revision();

        let mut entries = Vec::with_capacity(desired.len());

        for subscription in plan.into_entries() {
            let token = tokens
                .get(&subscription.stream_id)
                .expect("validated reconciliation returned every desired stream")
                .clone();

            entries.push(PreparedRestorationEntry {
                token,
                subscription,
            });
        }

        Ok(PreparedRestoration {
            stamp,
            demand_revision,
            route: connection_route.map(str::to_owned),
            entries,
        })
    }
}
