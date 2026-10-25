use thiserror::Error;

use tokio::sync::mpsc;

use super::{
    ConnectionFence, ConnectionStamp, ConnectionState, SharedRecoveryError, SharedStreamRecovery,
    StreamRecoveryPhase, SubscriptionDriverEvent,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionEventMonitorError {
    #[error("connection generation is stale")]
    StaleGeneration,

    #[error("desired subscription revision changed")]
    StaleDemand,

    #[error("subscription event has a stale recovery token")]
    StaleRecoveryToken,

    #[error("subscription confirmation has an invalid recovery phase")]
    InvalidRecoveryPhase,

    #[error("subscription driver event channel closed")]
    DriverEventChannelClosed,

    #[error("downstream subscription event queue is unavailable")]
    EventBackpressure,

    #[error("subscription failed for stream '{stream_id}': {reason}")]
    SubscriptionFailed { stream_id: String, reason: String },

    #[error(transparent)]
    Recovery(#[from] SharedRecoveryError),
}

// ============================================================
// MONITOR
// ============================================================

/// Validates subscription-driver events against current
/// connection and stream-recovery authority.
///
/// This monitor is not the ACK classifier.
///
/// It consumes events only AFTER SubscriptionDriver has
/// validated exchange responses and applied the
/// subscription-confirmed recovery transition.
///
/// Successful confirmations are forwarded to the
/// downstream source-integrity owner.
///
/// Failed subscriptions terminate this monitoring future.
/// The enclosing ConnectionSession must terminate the
/// physical session when that happens.
pub struct SubscriptionEventMonitor;

impl SubscriptionEventMonitor {
    pub async fn run<F>(
        mut inbound: mpsc::Receiver<SubscriptionDriverEvent>,
        downstream: mpsc::Sender<SubscriptionDriverEvent>,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        recovery: &SharedStreamRecovery,
        expected_demand_revision: u64,
        current_demand_revision: F,
    ) -> Result<(), SubscriptionEventMonitorError>
    where
        F: Fn() -> u64,
    {
        loop {
            let event = inbound
                .recv()
                .await
                .ok_or(SubscriptionEventMonitorError::DriverEventChannelClosed)?;

            // ------------------------------------------------
            // CONNECTION AUTHORITY
            // ------------------------------------------------

            if !current_generation(fence, stamp) {
                return Err(SubscriptionEventMonitorError::StaleGeneration);
            }

            // ------------------------------------------------
            // DEMAND AUTHORITY
            // ------------------------------------------------

            if current_demand_revision() != expected_demand_revision {
                return Err(SubscriptionEventMonitorError::StaleDemand);
            }

            // ------------------------------------------------
            // EVENT VALIDATION
            // ------------------------------------------------

            match &event {
                SubscriptionDriverEvent::Confirmed { token, .. } => {
                    if token.connection() != stamp {
                        return Err(SubscriptionEventMonitorError::StaleGeneration);
                    }

                    // Lock only for synchronous validation.
                    //
                    // Never hold this guard across an await.
                    {
                        let guard = recovery.lock()?;

                        if guard.token(token.stream_id()) != Some(token) {
                            return Err(SubscriptionEventMonitorError::StaleRecoveryToken);
                        }

                        // SubscriptionDriver has already
                        // advanced this stream after ACK
                        // validation.
                        if guard.phase(token.stream_id())
                            != Some(StreamRecoveryPhase::AwaitingRecovery)
                        {
                            return Err(SubscriptionEventMonitorError::InvalidRecoveryPhase);
                        }
                    }
                }

                SubscriptionDriverEvent::Failed { completion } => {
                    if &completion.stamp != stamp {
                        return Err(SubscriptionEventMonitorError::StaleGeneration);
                    }

                    let stream_id = completion.stream_id.clone();

                    let reason = format!("{:?}", completion.kind);

                    // Deliver failure information before
                    // requesting connection recycling.
                    downstream
                        .try_send(event)
                        .map_err(|_| SubscriptionEventMonitorError::EventBackpressure)?;

                    return Err(SubscriptionEventMonitorError::SubscriptionFailed {
                        stream_id,
                        reason,
                    });
                }
            }

            // ------------------------------------------------
            // FORWARD CONFIRMED EVENT
            // ------------------------------------------------

            // The receiver must continuously drain this
            // queue. Never silently drop completion events.
            //
            // On backpressure, fail the session rather than
            // block its event-monitoring task indefinitely.
            downstream
                .try_send(event)
                .map_err(|_| SubscriptionEventMonitorError::EventBackpressure)?;
        }
    }
}

// ============================================================
// CONNECTION VALIDATION
// ============================================================

fn current_generation(fence: &ConnectionFence, stamp: &ConnectionStamp) -> bool {
    fence.current_stamp().as_ref() == Some(stamp)
        && matches!(
            fence.state(),
            ConnectionState::Connected | ConnectionState::Ready
        )
}
