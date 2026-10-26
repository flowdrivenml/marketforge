use std::sync::{Arc, Weak};
use thiserror::Error;

use super::DesiredSubscriptionSnapshot;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use super::connection_slot_task::{
    ConnectionSlotTask, ConnectionSlotTaskConfig, ConnectionSlotTaskError,
};
use super::native_message_router::{
    NativeMessageRouter, NativeMessageRouterError, NativeMessageSelector,
};

use super::native_frame_dispatcher::NativeFrameDispatcher;
use super::slot_event_coordinator::{SlotEventCoordinator, SlotEventCoordinatorError};
use super::slot_event_loop::{SlotEventCommand, SlotEventLoop, SlotEventLoopError};
use super::stream_availability::StreamAvailability;
use super::stream_dispatch::StreamDispatcher;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum ManagedConnectionSlotError {
    #[error("connection-slot event queue capacity must be positive")]
    InvalidEventCapacity,

    #[error("connection-slot event loop terminated")]
    EventLoopTerminated,

    #[error(transparent)]
    Task(#[from] ConnectionSlotTaskError),

    #[error(transparent)]
    Coordinator(#[from] SlotEventCoordinatorError),

    #[error("slot control channel is unavailable")]
    ControlChannelUnavailable,

    #[error(transparent)]
    Routing(#[from] NativeMessageRouterError),
}
// ============================================================
// EVENT-LOOP TERMINATION GUARD
// ============================================================

/// Guarantees that the connection runner is cancelled and
/// the manager receives a terminal notification whenever
/// the event-loop task exits.
///
/// Drop runs on:
/// - Normal return.
/// - Error return.
/// - Panic unwinding.
/// - Tokio task cancellation.
///
/// Intentional manager shutdown is distinguished by the
/// manager's slot ownership and liveness checks.
struct EventLoopTerminationGuard {
    runner_abort: tokio::task::AbortHandle,
    failure_tx: Option<oneshot::Sender<()>>,
}

impl EventLoopTerminationGuard {
    fn new(runner_abort: tokio::task::AbortHandle, failure_tx: oneshot::Sender<()>) -> Self {
        Self {
            runner_abort,
            failure_tx: Some(failure_tx),
        }
    }
}

impl Drop for EventLoopTerminationGuard {
    fn drop(&mut self) {
        // A terminated event loop must not leave its
        // physical WebSocket runner operating independently.
        self.runner_abort.abort();

        // Signal termination even when the event-loop
        // future was cancelled or panicked.
        if let Some(sender) = self.failure_tx.take() {
            let _ = sender.send(());
        }
    }
}
// ============================================================
// MANAGED CONNECTION SLOT
// ============================================================

/// Owns one complete logical WebSocket connection slot.
///
/// Combines:
/// - Existing connection runner task.
/// - Generation-stamped market-data output.
/// - Validated subscription events.
/// - Physical connection lifecycle events.
/// - Native-message routing.
/// - Stream availability notifications.
///
/// This is an internal implementation component.
///
/// WebSocketManager will own multiple ManagedConnectionSlot
/// instances.
///
/// Stream processors never interact with this type.
pub(crate) struct ManagedConnectionSlot {
    task: Option<ConnectionSlotTask>,

    commands: mpsc::Sender<SlotEventCommand>,

    event_loop: Option<JoinHandle<Result<(), SlotEventLoopError>>>,
    failure_signal: Option<oneshot::Receiver<()>>,

    liveness: Arc<()>,
}

impl ManagedConnectionSlot {
    // ========================================================
    // START
    // ========================================================

    pub fn start(
        config: ConnectionSlotTaskConfig,
        router: NativeMessageRouter,
        streams: StreamDispatcher,
        event_capacity: usize,
        maximum_early_frames: usize,
    ) -> Result<Self, ManagedConnectionSlotError> {
        if event_capacity == 0 {
            return Err(ManagedConnectionSlotError::InvalidEventCapacity);
        }

        // ====================================================
        // BOUNDED EVENT CHANNELS
        // ====================================================

        let (market_data_tx, market_data_rx) = mpsc::channel(event_capacity);

        let (subscriptions_tx, subscriptions_rx) = mpsc::channel(event_capacity);

        let (reconnect_tx, reconnect_rx) = mpsc::channel(event_capacity);
        let (commands_tx, commands_rx) = mpsc::channel(event_capacity);
        // ====================================================
        // START EXISTING CONNECTION RUNNER
        // ====================================================

        let task = ConnectionSlotTask::start_fenced(
            config,
            market_data_tx,
            subscriptions_tx,
            reconnect_tx,
        )?;

        // Both the runner and event coordinator share
        // the exact same ConnectionFence authority.
        let fence = task.fence();

        // ====================================================
        // ASSEMBLE NATIVE MESSAGE DELIVERY
        // ====================================================

        let frame_dispatcher = NativeFrameDispatcher::new(fence.clone(), router, streams.clone());

        let availability = StreamAvailability::new(streams);

        let coordinator = SlotEventCoordinator::new(
            fence.clone(),
            frame_dispatcher,
            availability,
            maximum_early_frames,
        )?;

        // ====================================================
        // SUPERVISION
        // ====================================================

        // If native delivery fails, terminate the connection
        // runner rather than leaving a socket running without
        // a reliable downstream consumer.
        //
        // The existing runner's cancellation guard revokes
        // connection-generation authority.
        let runner_abort = task.abort_handle();
        let (failure_tx, failure_rx) = oneshot::channel::<()>();
        let event_loop = tokio::spawn(async move {
            // Construct before awaiting the event loop.
            //
            // This guard remains active for the entire lifetime
            // of the event-processing future.
            let _termination_guard = EventLoopTerminationGuard::new(runner_abort, failure_tx);

            let result = SlotEventLoop::run(
                fence,
                market_data_rx,
                subscriptions_rx,
                reconnect_rx,
                commands_rx,
                coordinator,
            )
            .await;

            if let Err(error) = &result {
                eprintln!("[MarketForge WebSocket] Slot event loop terminated: {error:?}");
            }

            result

            // _termination_guard is dropped here on ordinary return.
            //
            // It is also dropped during panic unwinding or
            // Tokio task cancellation.
        });

        Ok(Self {
            task: Some(task),
            commands: commands_tx,
            event_loop: Some(event_loop),
            failure_signal: Some(failure_rx),
            liveness: Arc::new(()),
        })
    }
    /// Transfer terminal-failure observation to WebSocketManager.
    ///
    /// The weak marker identifies this exact slot instance.
    ///
    /// A replacement using the same SlotId receives a different
    /// marker, so an old failure cannot retire its attachments.
    pub fn take_failure_monitor(&mut self) -> Option<(Weak<()>, oneshot::Receiver<()>)> {
        self.failure_signal
            .take()
            .map(|receiver| (Arc::downgrade(&self.liveness), receiver))
    }
    /// Add routing identities and then publish updated demand.
    ///
    /// The ordering is intentional:
    ///
    /// 1. Install additional routing identities.
    /// 2. Wait until the event-loop owner acknowledges them.
    /// 3. Publish the complete new subscription snapshot.
    ///
    /// This method supports additive routing updates.
    ///
    /// Removing or changing existing selectors requires
    /// supervised slot replacement.
    pub async fn extend_demand(
        &mut self,
        desired: DesiredSubscriptionSnapshot,
        additional_routes: Vec<(String, NativeMessageSelector)>,
    ) -> Result<(), ManagedConnectionSlotError> {
        if self.event_loop_finished() {
            return Err(ManagedConnectionSlotError::EventLoopTerminated);
        }

        // Preflight revision validation before installing routes.
        //
        // Expose the current revision through the task rather
        // than allowing a stale demand update after a successful
        // route extension.
        let current_revision = self
            .task
            .as_ref()
            .ok_or(ManagedConnectionSlotError::EventLoopTerminated)?
            .demand_revision();

        if desired.revision() <= current_revision {
            return Err(ManagedConnectionSlotError::Task(
                ConnectionSlotTaskError::StaleRevision,
            ));
        }

        let (response_tx, response_rx) = oneshot::channel();

        self.commands
            .send(SlotEventCommand::ExtendRoutes {
                entries: additional_routes,
                response: response_tx,
            })
            .await
            .map_err(|_| ManagedConnectionSlotError::ControlChannelUnavailable)?;

        response_rx
            .await
            .map_err(|_| ManagedConnectionSlotError::ControlChannelUnavailable)??;

        // New routes are installed before the runner can
        // observe the increased desired-demand revision.
        self.task
            .as_mut()
            .ok_or(ManagedConnectionSlotError::EventLoopTerminated)?
            .update_demand(desired)?;

        Ok(())
    }
    // ========================================================
    // UPDATE DESIRED SUBSCRIPTIONS
    // ========================================================

    /// Update the complete desired subscription projection
    /// for this logical connection slot.
    ///
    /// ConnectionSlotRunner will detect the changed revision
    /// and perform its existing reconnect-and-restore logic.
    ///
    /// The caller must serialize demand mutations.
    pub fn update_demand(
        &mut self,
        desired: DesiredSubscriptionSnapshot,
    ) -> Result<(), ManagedConnectionSlotError> {
        if self
            .event_loop
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            return Err(ManagedConnectionSlotError::EventLoopTerminated);
        }

        let task = self
            .task
            .as_mut()
            .ok_or(ManagedConnectionSlotError::EventLoopTerminated)?;

        task.update_demand(desired)?;

        Ok(())
    }

    // ========================================================
    // SHUTDOWN
    // ========================================================

    /// Gracefully stop the connection runner.
    ///
    /// The event loop is then cancelled and joined.
    ///
    /// No managed event-processing task remains running
    /// after this method completes.
    pub async fn shutdown(mut self) -> Result<(), ManagedConnectionSlotError> {
        // Shut down the physical connection authority first.
        //
        // This revokes its transport generation.
        let task_result = match self.task.take() {
            Some(task) => task.shutdown().await,
            None => Ok(()),
        };

        // The event loop no longer needs to process messages
        // after the connection runner has stopped.
        if let Some(event_loop) = self.event_loop.take() {
            event_loop.abort();

            // Cancellation is expected here.
            let _ = event_loop.await;
        }

        task_result?;

        Ok(())
    }

    // ========================================================
    // OBSERVABILITY
    // ========================================================

    /// Whether the slot's event-processing task has finished.
    ///
    /// This is a diagnostic check, not a synchronization
    /// guarantee that the task will remain alive.
    pub fn event_loop_finished(&self) -> bool {
        self.event_loop.as_ref().is_none_or(JoinHandle::is_finished)
    }
}

// ============================================================
// DROP SAFETY
// ============================================================

impl Drop for ManagedConnectionSlot {
    fn drop(&mut self) {
        // ConnectionSlotTask already implements cancellation
        // safety when dropped.
        //
        // Explicitly abort the event loop so it does not
        // become a detached task.
        if let Some(event_loop) = self.event_loop.take() {
            event_loop.abort();
        }

        // Dropping self.task will invoke the existing
        // ConnectionSlotTask cancellation behavior.
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_loop_termination_has_explicit_error() {
        let error = ManagedConnectionSlotError::EventLoopTerminated;

        assert_eq!(error.to_string(), "connection-slot event loop terminated");
    }

    #[test]
    fn zero_event_capacity_has_explicit_error() {
        let error = ManagedConnectionSlotError::InvalidEventCapacity;

        assert_eq!(
            error.to_string(),
            "connection-slot event queue capacity must be positive"
        );
    }
    #[tokio::test]
    async fn termination_guard_reports_task_cancellation() {
        use std::future::pending;

        let runner = tokio::spawn(async {
            pending::<()>().await;
        });

        let (failure_tx, failure_rx) = oneshot::channel::<()>();

        let runner_abort = runner.abort_handle();

        let event_loop = tokio::spawn(async move {
            let _guard = EventLoopTerminationGuard::new(runner_abort, failure_tx);

            // Never completes unless the task is cancelled.
            pending::<()>().await;
        });

        // Ensure the event-loop task has been polled and its
        // termination guard has actually been constructed.
        tokio::task::yield_now().await;

        event_loop.abort();

        let result = event_loop.await;

        assert!(result.unwrap_err().is_cancelled());

        // Cancellation must still signal the manager.
        assert!(failure_rx.await.is_ok());

        // And cancel the associated connection runner.
        let runner_result = runner.await;

        assert!(runner_result.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn termination_guard_reports_task_panic() {
        use std::future::pending;

        let runner = tokio::spawn(async {
            pending::<()>().await;
        });

        let (failure_tx, failure_rx) = oneshot::channel::<()>();

        let runner_abort = runner.abort_handle();

        let event_loop = tokio::spawn(async move {
            let _guard = EventLoopTerminationGuard::new(runner_abort, failure_tx);

            panic!("injected event-loop failure");
        });

        let result = event_loop.await;

        assert!(result.unwrap_err().is_panic());

        // Panic unwinding must execute the termination guard.
        assert!(failure_rx.await.is_ok());

        // The connection runner must not remain active.
        let runner_result = runner.await;

        assert!(runner_result.unwrap_err().is_cancelled());
    }
}
