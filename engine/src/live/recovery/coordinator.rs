use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use futures_util::{
    future::{AbortHandle, Abortable, Aborted},
    stream::{FuturesUnordered, StreamExt},
};

use tokio::sync::{mpsc, oneshot};

use crate::live::infrastructure::rest::ExchangeRestResponse;

use super::{
    executor::RecoveryExecutionError,
    model::{
        RecoveryJobId, RecoveryRequest, RecoveryRequestId, RecoveryRequester, RecoverySourceEpoch,
    },
    registry::RecoveryRegistration,
    scheduler::{
        RecoveryScheduledCancellation, RecoveryScheduledJob, RecoveryScheduler,
        RecoverySchedulerError, RecoverySchedulerLimits,
    },
};

// ============================================================
// RESULT
// ============================================================

/// The result of one REST acquisition.
///
/// Arc allows one successful acquisition to be delivered
/// to multiple streams without cloning its JSON body.
///
/// An HTTP success does not mean Integrity recovery succeeded.
pub type RecoveryJobResult = Result<Arc<ExchangeRestResponse>, Arc<RecoveryExecutionError>>;

/// One requester's completed recovery acquisition.
///
/// The result may share an Arc allocation with other
/// requesters of the same recovery job.
///
/// The requester identity is always the identity that
/// was registered with this particular request ID.
#[derive(Debug)]
pub struct RecoveryCompletion {
    pub request_id: RecoveryRequestId,
    pub job_id: RecoveryJobId,
    pub requester: RecoveryRequester,
    pub result: RecoveryJobResult,
}

// ============================================================
// COMPLETION TICKET
// ============================================================

pub struct RecoveryTicket {
    pub request_id: RecoveryRequestId,
    pub job_id: RecoveryJobId,

    pub completion: oneshot::Receiver<RecoveryCompletion>,
} // ============================================================
// HANDLE ERRORS
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RecoveryHandleError {
    #[error("recovery manager is unavailable")]
    ManagerUnavailable,

    #[error(transparent)]
    Scheduler(#[from] RecoverySchedulerError),

    #[error("recovery request does not belong to the specified requester")]
    RequestNotOwned,
}

// ============================================================
// COMMANDS
// ============================================================

enum RecoveryCommand {
    Register {
        request: RecoveryRequest,

        reply: oneshot::Sender<Result<RecoveryTicket, RecoverySchedulerError>>,
    },

    Cancel {
        request_id: RecoveryRequestId,

        reply: oneshot::Sender<Result<RecoveryScheduledCancellation, RecoverySchedulerError>>,
    },

    CancelGeneration {
        stream_id: String,
        generation: u64,

        reply: oneshot::Sender<Vec<RecoveryScheduledCancellation>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
    CancelOwned {
        request_id: RecoveryRequestId,
        requester: RecoveryRequester,

        reply: oneshot::Sender<Result<RecoveryScheduledCancellation, RecoveryHandleError>>,
    },

    CancelEpoch {
        stream_id: String,
        epoch: RecoverySourceEpoch,

        reply: oneshot::Sender<Vec<RecoveryScheduledCancellation>>,
    },
}

// ============================================================
// SHARED HANDLE
// ============================================================

/// Cloneable client for the single Recovery Manager.
///
/// Cloning does not create another scheduler, registry,
/// REST client, or recovery worker.
#[derive(Clone)]
pub struct RecoveryHandle {
    sender: mpsc::Sender<RecoveryCommand>,
}

impl RecoveryHandle {
    pub async fn request(
        &self,
        request: RecoveryRequest,
    ) -> Result<RecoveryTicket, RecoveryHandleError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.sender
            .send(RecoveryCommand::Register {
                request,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?;

        reply_rx
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?
            .map_err(Into::into)
    }
    /// Cancel a request only when its registered requester
    /// matches the supplied identity exactly.
    ///
    /// The ownership check and cancellation are processed
    /// atomically within the manager's serialized event loop.
    pub async fn cancel_owned(
        &self,
        requester: RecoveryRequester,
        request_id: RecoveryRequestId,
    ) -> Result<RecoveryScheduledCancellation, RecoveryHandleError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.sender
            .send(RecoveryCommand::CancelOwned {
                request_id,
                requester,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?;

        reply_rx
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?
    }

    /// Cancel all requests belonging to an exact source epoch.
    ///
    /// The match includes stream, connection, and subscription
    /// generations, rather than stream generation alone.
    pub async fn cancel_epoch(
        &self,
        stream_id: impl Into<String>,
        epoch: RecoverySourceEpoch,
    ) -> Result<Vec<RecoveryScheduledCancellation>, RecoveryHandleError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.sender
            .send(RecoveryCommand::CancelEpoch {
                stream_id: stream_id.into(),
                epoch,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?;

        reply_rx
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)
    }
    pub async fn cancel(
        &self,
        request_id: RecoveryRequestId,
    ) -> Result<RecoveryScheduledCancellation, RecoveryHandleError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.sender
            .send(RecoveryCommand::Cancel {
                request_id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?;

        reply_rx
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?
            .map_err(Into::into)
    }

    pub async fn cancel_generation(
        &self,
        stream_id: impl Into<String>,
        generation: u64,
    ) -> Result<Vec<RecoveryScheduledCancellation>, RecoveryHandleError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.sender
            .send(RecoveryCommand::CancelGeneration {
                stream_id: stream_id.into(),
                generation,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?;

        reply_rx
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)
    }
    pub async fn shutdown(&self) -> Result<(), RecoveryHandleError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.sender
            .send(RecoveryCommand::Shutdown { reply: reply_tx })
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)?;

        reply_rx
            .await
            .map_err(|_| RecoveryHandleError::ManagerUnavailable)
    }
}

// ============================================================
// COORDINATOR CONFIGURATION
// ============================================================

#[derive(Debug, Clone, Copy)]
pub struct RecoveryCoordinatorConfig {
    pub command_capacity: usize,
    pub scheduler: RecoverySchedulerLimits,
}

impl RecoveryCoordinatorConfig {
    pub fn validate(self) -> Result<(), RecoveryCoordinatorError> {
        if self.command_capacity == 0 {
            return Err(RecoveryCoordinatorError::InvalidCommandCapacity);
        }

        self.scheduler
            .validate()
            .map_err(RecoveryCoordinatorError::Scheduler)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryCoordinatorError {
    #[error("recovery command capacity must be positive")]
    InvalidCommandCapacity,

    #[error(transparent)]
    Scheduler(#[from] RecoverySchedulerError),
}

// ============================================================
// IN-FLIGHT FUTURES
// ============================================================

type AcquisitionOutput = (
    RecoveryJobId,
    Result<
        Result<crate::live::infrastructure::rest::ExchangeRestResponse, RecoveryExecutionError>,
        Aborted,
    >,
);

type AcquisitionFuture<'a> = Pin<Box<dyn Future<Output = AcquisitionOutput> + 'a>>;

// ============================================================
// COORDINATOR
// ============================================================

/// One shared recovery coordinator.
///
/// Owns all mutable recovery scheduling state.
///
/// Recovery requests reach it through a bounded Tokio
/// command channel. No global manager mutex is required.
///
/// The caller drives run() within the application's
/// asynchronous runtime.
pub struct RecoveryCoordinator {
    scheduler: RecoveryScheduler,

    receiver: mpsc::Receiver<RecoveryCommand>,

    completions: HashMap<RecoveryRequestId, PendingRecoveryCompletion>,

    aborts: HashMap<RecoveryJobId, AbortHandle>,

    shutdown_requested: bool,
}

struct PendingRecoveryCompletion {
    job_id: RecoveryJobId,
    requester: RecoveryRequester,
    sender: oneshot::Sender<RecoveryCompletion>,
}

impl RecoveryCoordinator {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        config: RecoveryCoordinatorConfig,
    ) -> Result<(Self, RecoveryHandle), RecoveryCoordinatorError> {
        config.validate()?;

        let scheduler = RecoveryScheduler::new(config.scheduler)?;

        let (sender, receiver) = mpsc::channel(config.command_capacity);

        let manager = Self {
            scheduler,
            receiver,
            completions: HashMap::new(),
            aborts: HashMap::new(),
            shutdown_requested: false,
        };

        let handle = RecoveryHandle { sender };

        Ok((manager, handle))
    }
    /// Complete one registered requester with an explicit
    /// acquisition or cancellation result.
    ///
    /// Returns false when the request no longer has a pending
    /// completion, which is expected for already-cancelled
    /// or obsolete requests.
    fn complete_request(
        &mut self,
        request_id: RecoveryRequestId,
        result: RecoveryJobResult,
    ) -> bool {
        let Some(pending) = self.completions.remove(&request_id) else {
            return false;
        };

        let completion = RecoveryCompletion {
            request_id,
            job_id: pending.job_id,
            requester: pending.requester,
            result,
        };

        let _ = pending.sender.send(completion);

        true
    }
    // ========================================================
    // ASYNC EVENT LOOP
    // ========================================================

    /// Drive the shared coordinator.
    ///
    /// `execute` is normally an adapter around:
    /// RecoveryRestExecutor::execute().
    ///
    /// Injecting the executor also makes the coordinator
    /// testable without HTTP connections.
    ///
    /// The coordinator remains alive while command handles
    /// exist or active recovery acquisitions remain.
    pub async fn run<F, Fut>(mut self, execute: F)
    where
        F: Fn(RecoveryScheduledJob) -> Fut,
        Fut: Future<Output = Result<ExchangeRestResponse, RecoveryExecutionError>>,
    {
        let mut running: FuturesUnordered<AcquisitionFuture<'_>> = FuturesUnordered::new();

        let mut commands_closed = false;

        loop {
            // ------------------------------------------------
            // DISPATCH
            // ------------------------------------------------

            // Drain already queued commands before scheduling.
            // This gives requests arriving together an
            // opportunity to share one queued REST job.
            while !self.shutdown_requested {
                match self.receiver.try_recv() {
                    Ok(command) => {
                        self.process_command(command);
                    }

                    Err(_) => break,
                }
            }

            if self.shutdown_requested {
                break;
            }

            self.dispatch_ready(&execute, &mut running);

            if commands_closed && running.is_empty() {
                break;
            }

            // ------------------------------------------------
            // NEXT EVENT
            // ------------------------------------------------

            tokio::select! {
                command = self.receiver.recv(),
                    if !commands_closed =>
                {
                    match command {
                        Some(command) => {
                            self.process_command(command);
                        }

                        None => {
                            commands_closed = true;
                        }
                    }
                }

                completion = running.next(),
                    if !running.is_empty() =>
                {
                    if let Some((job_id, result)) = completion {
                        self.finish_job(job_id, result);
                    }
                }
            }
            if self.shutdown_requested {
                break;
            }
        }
    }

    // ========================================================
    // COMMAND PROCESSING
    // ========================================================

    fn process_command(&mut self, command: RecoveryCommand) {
        if self.shutdown_requested {
            // A second shutdown command can be acknowledged.
            //
            // Other commands are discarded. Their oneshot reply
            // senders are dropped, so callers receive
            // ManagerUnavailable rather than hanging.
            if let RecoveryCommand::Shutdown { reply } = command {
                let _ = reply.send(());
            }

            return;
        }
        match command {
            RecoveryCommand::Register { request, reply } => {
                let requester = request.requester.clone();

                let registration = self.scheduler.register(request);

                let response = registration.map(
                    |RecoveryRegistration {
                         request_id, job_id, ..
                     }| {
                        let (completion_tx, completion_rx) = oneshot::channel();

                        self.completions.insert(
                            request_id,
                            PendingRecoveryCompletion {
                                job_id,
                                requester: requester.clone(),
                                sender: completion_tx,
                            },
                        );

                        RecoveryTicket {
                            request_id,
                            job_id,
                            completion: completion_rx,
                        }
                    },
                );

                if let Err(undelivered) = reply.send(response) {
                    if let Ok(ticket) = undelivered {
                        self.completions.remove(&ticket.request_id);

                        if let Ok(cancelled) = self.scheduler.cancel(ticket.request_id) {
                            self.apply_cancellation(cancelled);
                        }
                    }
                }
            }

            RecoveryCommand::Cancel { request_id, reply } => {
                let result = self.scheduler.cancel(request_id);

                if let Ok(cancelled) = &result {
                    self.complete_request(
                        request_id,
                        Err(Arc::new(RecoveryExecutionError::Cancelled)),
                    );

                    self.apply_cancellation(*cancelled);
                }

                let _ = reply.send(result);
            }

            RecoveryCommand::CancelGeneration {
                stream_id,
                generation,
                reply,
            } => {
                let cancelled = self.scheduler.cancel_generation(&stream_id, generation);

                for item in &cancelled {
                    self.complete_request(
                        item.cancellation.request_id,
                        Err(Arc::new(RecoveryExecutionError::Cancelled)),
                    );

                    self.apply_cancellation(*item);
                }

                let _ = reply.send(cancelled);
            }
            RecoveryCommand::Shutdown { reply } => {
                self.begin_shutdown();

                let _ = reply.send(());
            }
            RecoveryCommand::CancelOwned {
                request_id,
                requester,
                reply,
            } => {
                let result = match self.scheduler.request(request_id) {
                    Some(request) if request.requester == requester => self
                        .scheduler
                        .cancel(request_id)
                        .map_err(RecoveryHandleError::from),

                    Some(_) => Err(RecoveryHandleError::RequestNotOwned),

                    None => self
                        .scheduler
                        .cancel(request_id)
                        .map_err(RecoveryHandleError::from),
                };

                if let Ok(cancelled) = &result {
                    self.complete_request(
                        request_id,
                        Err(Arc::new(RecoveryExecutionError::Cancelled)),
                    );

                    self.apply_cancellation(*cancelled);
                }

                let _ = reply.send(result);
            }
            RecoveryCommand::CancelEpoch {
                stream_id,
                epoch,
                reply,
            } => {
                let request_ids = self.scheduler.request_ids_for_epoch(&stream_id, &epoch);

                let mut cancelled = Vec::with_capacity(request_ids.len());

                for request_id in request_ids {
                    let cancellation = self
                        .scheduler
                        .cancel(request_id)
                        .expect("selected epoch request must remain registered");

                    self.complete_request(
                        request_id,
                        Err(Arc::new(RecoveryExecutionError::Cancelled)),
                    );

                    self.apply_cancellation(cancellation);

                    cancelled.push(cancellation);
                }

                let _ = reply.send(cancelled);
            }
        }
    }

    // ========================================================
    // DISPATCH
    // ========================================================

    fn dispatch_ready<'a, F, Fut>(
        &mut self,
        execute: &'a F,
        running: &mut FuturesUnordered<AcquisitionFuture<'a>>,
    ) where
        F: Fn(RecoveryScheduledJob) -> Fut,
        Fut: Future<Output = Result<ExchangeRestResponse, RecoveryExecutionError>> + 'a,
    {
        let jobs = self
            .scheduler
            .start_ready_jobs()
            .expect("validated recovery scheduler must dispatch");

        for job in jobs {
            let job_id = job.job_id;

            let (abort, registration) = AbortHandle::new_pair();

            self.aborts.insert(job_id, abort);

            let future = async move {
                let result = Abortable::new(execute(job), registration).await;

                (job_id, result)
            };

            running.push(Box::pin(future));
        }
    }

    // ========================================================
    // CANCELLATION
    // ========================================================

    fn apply_cancellation(&mut self, cancelled: RecoveryScheduledCancellation) {
        if let Some(job_id) = cancelled.abort_job {
            if let Some(handle) = self.aborts.remove(&job_id) {
                handle.abort();
            }
        }
    }

    // ========================================================
    // COMPLETION
    // ========================================================

    fn finish_job(
        &mut self,
        job_id: RecoveryJobId,
        result: Result<Result<ExchangeRestResponse, RecoveryExecutionError>, Aborted>,
    ) {
        self.aborts.remove(&job_id);

        // An aborted job may have been removed from the
        // scheduler when its final requester cancelled.
        //
        // It must never deliver stale results.
        let Ok(result) = result else {
            return;
        };

        if !self.scheduler.is_running(job_id) {
            return;
        }

        let completion = self
            .scheduler
            .complete(job_id)
            .expect("running job completion must be consistent");

        let shared: RecoveryJobResult = result.map(Arc::new).map_err(Arc::new);

        for recipient in completion.recipients {
            self.complete_request(recipient.request_id, shared.clone());
        }
    }

    /// Terminate all outstanding recovery work.
    ///
    /// Every registered requester receives an explicit
    /// ManagerShutdown completion.
    ///
    /// All running acquisitions are signalled for abortion.
    ///
    /// This method is idempotent and is called only from
    /// the coordinator's serialized event loop.
    fn begin_shutdown(&mut self) {
        if self.shutdown_requested {
            return;
        }

        self.shutdown_requested = true;

        // Cancel all registered requesters using the
        // scheduler's existing cancellation rules.
        let cancellations = self.scheduler.cancel_all();

        for cancelled in cancellations {
            // The completion sender belongs to this exact
            // request ID. It cannot be redirected to a
            // newer request or generation.
            self.complete_request(
                cancelled.cancellation.request_id,
                Err(Arc::new(RecoveryExecutionError::ManagerShutdown)),
            );

            // Abort a running acquisition only when its
            // final requester has been detached.
            self.apply_cancellation(cancelled);
        }

        debug_assert_eq!(self.scheduler.request_count(), 0);
        debug_assert_eq!(self.scheduler.job_count(), 0);
        debug_assert_eq!(self.scheduler.running_count(), 0);
    }
}
