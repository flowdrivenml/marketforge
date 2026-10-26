use super::native_message_router::{NativeMessageRouterError, NativeMessageSelector};
use super::{
    ConnectionFence, ConnectionStamp, FencedMessage, ReconnectEvent, SubscriptionDriverEvent,
};
use std::collections::BTreeMap;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use super::slot_event_coordinator::{SlotEventCoordinator, SlotEventCoordinatorError};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum SlotEventLoopError {
    #[error("native market-data channel closed")]
    MarketDataChannelClosed,

    #[error("subscription event channel closed")]
    SubscriptionChannelClosed,

    #[error("reconnection event channel closed")]
    ReconnectChannelClosed,

    #[error("connection-slot runner stopped")]
    RunnerStopped,

    #[error("conflicting physical connection identity")]
    ConflictingConnectionIdentity,

    #[error(transparent)]
    Coordinator(#[from] SlotEventCoordinatorError),
    #[error("slot control channel closed")]
    ControlChannelClosed,
}

// ============================================================
// INTERNAL SLOT COMMANDS
// ============================================================

/// Control operations serialized with incoming connection
/// events and native market-data frames.
///
/// Commands are private to the WebSocket infrastructure.
pub(crate) enum SlotEventCommand {
    /// Extend the router before changing subscription demand.
    ///
    /// Response acknowledges that the event-processing owner
    /// has committed the routing update.
    ExtendRoutes {
        entries: Vec<(String, NativeMessageSelector)>,

        response: oneshot::Sender<Result<(), NativeMessageRouterError>>,
    },
}

// ============================================================
// CONNECTION GENERATION TRACKING
// ============================================================

/// Tracks physical-generation stamps observed directly
/// from provenance-aware market data or validated
/// subscription events.
///
/// ReconnectEvent currently carries a generation number,
/// not a complete ConnectionStamp.
///
/// We must not reconstruct provenance by reading whichever
/// generation happens to be current when an event is dequeued.
#[derive(Default)]
struct ObservedGenerations {
    stamps: BTreeMap<u64, ConnectionStamp>,
}

impl ObservedGenerations {
    fn observe(&mut self, stamp: &ConnectionStamp) -> Result<(), SlotEventLoopError> {
        let generation = stamp.generation().get();

        if let Some(existing) = self.stamps.get(&generation) {
            if existing != stamp {
                return Err(SlotEventLoopError::ConflictingConnectionIdentity);
            }

            return Ok(());
        }

        self.stamps.insert(generation, stamp.clone());

        Ok(())
    }

    fn remove(&mut self, generation: u64) -> Option<ConnectionStamp> {
        self.stamps.remove(&generation)
    }
}

// ============================================================
// SLOT EVENT LOOP
// ============================================================

/// Serial event-processing owner for one logical
/// WebSocket connection slot.
///
/// Consumes:
///
/// - Generation-stamped native frames.
/// - Validated subscription events.
/// - Physical connection lifecycle events.
///
/// Delegates stream-specific behavior to SlotEventCoordinator.
///
/// This object does not normalize market data, check
/// trade/depth sequences, or authorize canonical publication.
///
/// The manager must supervise this loop together with
/// the corresponding ConnectionSlotTask.
pub(crate) struct SlotEventLoop;

impl SlotEventLoop {
    pub async fn run(
        fence: ConnectionFence,
        mut market_data: mpsc::Receiver<FencedMessage<Message>>,
        mut subscriptions: mpsc::Receiver<SubscriptionDriverEvent>,
        mut reconnect_events: mpsc::Receiver<ReconnectEvent>,
        mut commands: mpsc::Receiver<SlotEventCommand>,
        mut coordinator: SlotEventCoordinator,
    ) -> Result<(), SlotEventLoopError> {
        let mut generations = ObservedGenerations::default();

        loop {
            tokio::select! {
                            biased;
            // ============================================
            // INTERNAL ROUTING CONTROL
            // ============================================

            command = commands.recv() => {
                let Some(command) = command else {
                    return Err(
                        SlotEventLoopError::ControlChannelClosed
                    );
                };

                match command {
                    SlotEventCommand::ExtendRoutes {
                        entries,
                        response,
                    } => {
                        let result = coordinator
                            .extend_routes(entries)
                            .map_err(|error| {
                                match error {
                                    SlotEventCoordinatorError::Frame(
                                        super::native_frame_dispatcher::
                                            NativeFrameDispatchError::Classification(
                                                routing_error
                                            )
                                    ) => routing_error,

                                    // ExtendRoutes only performs routing
                                    // validation, so other error variants
                                    // indicate a broken internal contract.
                                    other => {
                                        panic!(
                                            "unexpected route-extension error: {other}"
                                        );
                                    }
                                }
                            });

                        let _ = response.send(result);
                    }
                }
            }
                            // ============================================
                            // CONNECTION LIFECYCLE
                            // ============================================

                            event = reconnect_events.recv() => {
                                let event = event.ok_or(
                                    SlotEventLoopError::ReconnectChannelClosed
                                )?;

                                match event {
                                    ReconnectEvent::Connecting { .. } => {
                                        // No stream is available yet.
                                    }

                                    ReconnectEvent::Connected { .. } => {
                                        // A physical handshake is not
                                        // subscription confirmation.
                                    }

                                    ReconnectEvent::Disconnected {
                                        generation,
                                        ..
                                    } => {
                                        // ReconnectRunner has already
                                        // revoked the physical generation.
                                        //
                                        // Resolve only previously observed
                                        // authoritative stamps.
                                        //
                                        // Never use fence.current_stamp()
                                        // here: it may already be None
                                        // or belong to a newer generation.

                                        if let Some(stamp) =
                                            generations.remove(generation.get())
                                        {
                                            coordinator
                                                .on_connection_lost(&stamp)?;
                                        }
                                    }

                                    ReconnectEvent::Backoff { .. } => {
                                        // No additional availability
                                        // transition is required.
                                    }

                                    ReconnectEvent::Stopped => {
                                        // Stopping the connection runner
                                        // must terminate the supervisor's
                                        // coordination lifecycle.
                                        //
                                        // Do not report an ordinary
                                        // successful processing session.
                                        return Err(
                                            SlotEventLoopError::RunnerStopped
                                        );
                                    }
                                }
                            }

                            // ============================================
                            // VALIDATED SUBSCRIPTION EVENTS
                            // ============================================

                            event = subscriptions.recv() => {
                                let event = event.ok_or(
                                    SlotEventLoopError::SubscriptionChannelClosed
                                )?;

                                // SubscriptionDriverEvent contains
                                // authoritative connection provenance.
                                //
                                // Do not reconstruct it from current
                                // physical connection state.

                                let stamp = match &event {
                                    SubscriptionDriverEvent::Confirmed {
                                        token,
                                        ..
                                    } => token.connection().clone(),

                                    SubscriptionDriverEvent::Failed {
                                        completion,
                                    } => completion.stamp.clone(),
                                };

                                // Ignore events from revoked generations.
                                //
                                // A late ACK must never reauthorize
                                // an already disconnected stream.
                                if !Self::is_current(&fence, &stamp) {
                                    continue;
                                }

                                generations.observe(&stamp)?;

                                coordinator.on_subscription(event)?;
                            }

                            // ============================================
                            // NATIVE MARKET DATA
                            // ============================================

                            frame = market_data.recv() => {
                                let frame = frame.ok_or(
                                    SlotEventLoopError::MarketDataChannelClosed
                                )?;

                                let stamp = frame.stamp().clone();

                                // Frames already queued when the
                                // connection was revoked must not be
                                // admitted to stream processing.
                                //
                                // This rejection is intentional.
                                if !Self::is_current(&fence, &stamp) {
                                    continue;
                                }

                                generations.observe(&stamp)?;

                                coordinator.on_frame(frame)?;
                            }
                        }
        }
    }

    // ========================================================
    // CURRENT CONNECTION VALIDATION
    // ========================================================

    fn is_current(fence: &ConnectionFence, stamp: &ConnectionStamp) -> bool {
        fence.with_connected(stamp, || ()).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(slot_id: &str) -> ConnectionStamp {
        let fence = ConnectionFence::new("test-runtime", slot_id).unwrap();

        fence.begin_connect().unwrap()
    }

    #[test]
    fn records_original_connection_stamp() {
        let mut generations = ObservedGenerations::default();

        let original = stamp("slot-a");

        generations.observe(&original).unwrap();

        let recovered = generations.remove(original.generation().get()).unwrap();

        assert_eq!(recovered, original);
    }

    #[test]
    fn rejects_conflicting_slot_identity() {
        let mut generations = ObservedGenerations::default();

        let first = stamp("slot-a");
        let second = stamp("slot-b");

        // Both slots independently allocate generation 1.
        assert_eq!(first.generation().get(), second.generation().get());

        generations.observe(&first).unwrap();

        let result = generations.observe(&second);

        assert!(matches!(
            result,
            Err(SlotEventLoopError::ConflictingConnectionIdentity)
        ));
    }

    #[test]
    fn removing_unknown_generation_is_safe() {
        let mut generations = ObservedGenerations::default();

        assert!(generations.remove(42).is_none());
    }

    #[test]
    fn old_generation_cannot_become_current_again() {
        let fence = ConnectionFence::new("test-runtime", "slot-a").unwrap();

        let first = fence.begin_connect().unwrap();
        fence.connected(&first).unwrap();

        assert!(SlotEventLoop::is_current(&fence, &first));

        fence.failed(&first).unwrap();

        assert!(!SlotEventLoop::is_current(&fence, &first));

        let second = fence.begin_connect().unwrap();
        fence.connected(&second).unwrap();

        assert!(SlotEventLoop::is_current(&fence, &second));

        assert!(!SlotEventLoop::is_current(&fence, &first));
    }
}
