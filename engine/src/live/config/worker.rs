//! Dedicated bounded filesystem worker around the sole revision journal owner.
//! Cancellation after a write starts reports uncertainty, never rollback.
use super::store::FileRevisionJournal;
use crate::live::{
    foundation::{
        clock::MonotonicClock,
        resources::{ResourceBudget, ResourceReservation, ResourceUsage},
    },
    model::{
        configuration::*,
        error::{ControlError, ErrorCode},
        operation::{ResourceId, ResourceVersion},
        snapshot::{Page, PageRequest},
    },
    ports::{
        configuration::{ConfigurationCompilerPort, ConfigurationStorePort},
        context::CallContext,
        routing::RoutingFuture,
    },
    routing::admission::{DeadlineFuture, DeadlineTimer},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, mpsc},
    task::{Context, Poll, Waker},
};
enum Command {
    Load,
    Revision(ConfigRevisionId),
    History(PageRequest),
    Cas(ConfigRevisionId, DesiredConfiguration),
}
enum Reply {
    Revision(ConfigurationRevision),
    History(Page<ConfigurationRevision>),
}
struct Response {
    value: Option<Result<Reply, ErrorCode>>,
    waker: Option<Waker>,
    writing: bool,
    abandoned: bool,
    _charge: ResourceReservation,
}
struct Request {
    command: Command,
    context: CallContext,
    response: Arc<Mutex<Response>>,
}
pub struct ConfigurationWorker {
    sender: Mutex<Option<mpsc::SyncSender<Request>>>,
    budget: ResourceBudget,
    clock: Arc<dyn MonotonicClock>,
    timer: Arc<dyn DeadlineTimer>,
    owner: ResourceVersion,
    max_request_bytes: usize,
    max_reply_bytes: usize,
    exited: Arc<Mutex<(bool, Option<Waker>)>>,
}
fn error(code: ErrorCode) -> ControlError {
    ControlError::new(code, Some(ResourceId::Configuration))
}
impl ConfigurationWorker {
    /// The journal moves into exactly one native worker. A reservation covers
    /// queued work, running work and its retained reply (not only queue slots).
    pub fn start(
        journal: FileRevisionJournal,
        compiler: Arc<dyn ConfigurationCompilerPort>,
        timer: Arc<dyn DeadlineTimer>,
        limit: ResourceUsage,
    ) -> Result<Self, ErrorCode> {
        let clock = journal.clock.clone();
        let owner = journal.owner.clone();
        let max_request_bytes = journal.limits.bytes;
        let max_reply_bytes = journal.limits.bytes;
        let budget = ResourceBudget::new(limit)?;
        let (sender, receiver) = mpsc::sync_channel::<Request>(limit.items);
        let exited = Arc::new(Mutex::new((false, None::<Waker>)));
        let worker_exited = exited.clone();
        std::thread::Builder::new()
            .name("live-configuration".into())
            .spawn(move || {
                let mut failed = false;
                for request in receiver {
                    let response = request.response.clone();
                    let execute = || -> Result<Reply, ErrorCode> {
                        {
                            let state = response.lock().unwrap_or_else(|e| e.into_inner());
                            if state.abandoned {
                                return Err(ErrorCode::Cancelled);
                            }
                        }
                        request
                            .context
                            .check(journal.clock.as_ref(), Some(&journal.owner))?;
                        match request.command {
                            Command::Load => journal.load(&request.context).map(Reply::Revision),
                            Command::Revision(id) => {
                                journal.revision(&request.context, id).map(Reply::Revision)
                            }
                            Command::History(page) => {
                                let after = page
                                    .after
                                    .map(|v| {
                                        v.parse::<u64>()
                                            .map(ConfigRevisionId)
                                            .map_err(|_| ErrorCode::InvalidInput)
                                    })
                                    .transpose()?;
                                if page.limit == 0 || page.limit as usize > journal.limits.history {
                                    return Err(ErrorCode::Limit);
                                }
                                let items = journal.history(
                                    &request.context,
                                    after,
                                    page.limit as usize,
                                )?;
                                let continuation = if items.len() == page.limit as usize {
                                    items.last().map(|r| r.id.0.to_string())
                                } else {
                                    None
                                };
                                Ok(Reply::History(Page {
                                    items,
                                    continuation,
                                }))
                            }
                            Command::Cas(expected, desired) => {
                                let base = journal.load(&request.context)?;
                                let contents = ConfigurationRevision {
                                    id: expected,
                                    parent: None,
                                    created_at_ns: 0,
                                    desired,
                                };
                                let plan = compiler
                                    .compile(
                                        &base,
                                        &ConfigurationProposal::Restore {
                                            expected_revision: expected,
                                            contents,
                                        },
                                    )
                                    .map_err(|r| {
                                        r.issues.first().map_or(ErrorCode::InvalidInput, |i| i.code)
                                    })?;
                                let mut state = response.lock().unwrap_or_else(|e| e.into_inner());
                                if state.abandoned {
                                    return Err(ErrorCode::Cancelled);
                                }
                                request
                                    .context
                                    .check(journal.clock.as_ref(), Some(&journal.owner))?;
                                state.writing = true;
                                drop(state);
                                journal.commit(&request.context, &plan).map(Reply::Revision)
                            }
                        }
                    };
                    let result = if failed {
                        Err(ErrorCode::CommitUnknown)
                    } else {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(execute)) {
                            Ok(value) => value,
                            Err(_) => {
                                failed = true;
                                Err(ErrorCode::CommitUnknown)
                            }
                        }
                    };
                    let wake = {
                        let mut state = response.lock().unwrap_or_else(|e| e.into_inner());
                        state.value = Some(result);
                        state.waker.take()
                    };
                    if let Some(w) = wake {
                        w.wake();
                    }
                }
                drop(journal);
                let wake = {
                    let mut state = worker_exited.lock().unwrap_or_else(|e| e.into_inner());
                    state.0 = true;
                    state.1.take()
                };
                if let Some(w) = wake {
                    w.wake();
                }
            })
            .map_err(|_| ErrorCode::Unavailable)?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            budget,
            clock,
            timer,
            owner,
            max_request_bytes,
            max_reply_bytes,
            exited,
        })
    }
    pub fn snapshot(&self) -> crate::live::foundation::resources::ResourceBudgetSnapshot {
        self.budget.snapshot()
    }
    /// Stop admission and drain accepted worker obligations. Filesystem calls
    /// already in progress cannot be revoked; their reservations remain visible.
    pub fn close(&self) {
        self.budget.close();
        self.sender.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
    pub fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.close();
        if self.budget.poll_shutdown(cx).is_pending() {
            return Poll::Pending;
        }
        let mut state = self.exited.lock().unwrap_or_else(|e| e.into_inner());
        if state.0 {
            Poll::Ready(())
        } else {
            state.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    async fn request(
        &self,
        context: &CallContext,
        command: Command,
    ) -> Result<Reply, ControlError> {
        context
            .check(self.clock.as_ref(), Some(&self.owner))
            .map_err(error)?;
        let request_bytes = match &command {
            Command::Cas(_, desired) => super::store::encode(desired, self.max_request_bytes)
                .map_err(error)?
                .len(),
            Command::History(page) => page.after.as_ref().map_or(0, String::len),
            _ => 0,
        };
        if request_bytes > self.max_request_bytes {
            return Err(error(ErrorCode::Limit));
        }
        // Whole journal bound covers all revisions a query can return. Fixed
        // overhead also accounts for command, contexts, waker and queue slots.
        let bytes = request_bytes
            .checked_add(self.max_reply_bytes)
            .and_then(|n| n.checked_mul(64))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| error(ErrorCode::Limit))?;
        let charge = self
            .budget
            .reserve(ResourceUsage { items: 1, bytes })
            .map_err(error)?;
        let cancelled = context.cancellation.try_cancelled().map_err(error)?;
        let deadline = self
            .timer
            .try_sleep_until(context.deadline)
            .map_err(error)?;
        let response = Arc::new(Mutex::new(Response {
            value: None,
            waker: None,
            writing: false,
            abandoned: false,
            _charge: charge,
        }));
        let future = ResponseFuture {
            state: response.clone(),
            cancelled,
            deadline,
        };
        {
            let sender = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            sender
                .as_ref()
                .ok_or_else(|| error(ErrorCode::Unavailable))?
                .try_send(Request {
                    command,
                    context: context.clone(),
                    response,
                })
                .map_err(|e| {
                    error(match e {
                        mpsc::TrySendError::Full(_) => ErrorCode::Capacity,
                        mpsc::TrySendError::Disconnected(_) => ErrorCode::Unavailable,
                    })
                })?;
        }
        future.await.map_err(error)
    }
}
impl Drop for ConfigurationWorker {
    fn drop(&mut self) {
        self.close();
    }
}
struct ResponseFuture {
    state: Arc<Mutex<Response>>,
    cancelled: crate::live::foundation::cancellation::Cancelled,
    deadline: DeadlineFuture,
}
impl Future for ResponseFuture {
    type Output = Result<Reply, ErrorCode>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let cancel = Pin::new(&mut this.cancelled).poll(cx).is_ready();
        let deadline = this.deadline.as_mut().poll(cx).is_ready();
        let mut state = this.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(value) = state.value.take() {
            return Poll::Ready(value);
        }
        if cancel || deadline {
            state.abandoned = true;
            return Poll::Ready(Err(if state.writing {
                ErrorCode::CommitUnknown
            } else if cancel {
                ErrorCode::Cancelled
            } else {
                ErrorCode::Deadline
            }));
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
impl Drop for ResponseFuture {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.abandoned = true;
        state.waker = None;
    }
}
impl ConfigurationStorePort for ConfigurationWorker {
    fn load<'a>(
        &'a self,
        context: &'a CallContext,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>> {
        Box::pin(async move {
            match self.request(context, Command::Load).await? {
                Reply::Revision(r) => Ok(r),
                _ => Err(error(ErrorCode::Internal)),
            }
        })
    }
    fn revision<'a>(
        &'a self,
        context: &'a CallContext,
        id: ConfigRevisionId,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>> {
        Box::pin(async move {
            match self.request(context, Command::Revision(id)).await? {
                Reply::Revision(r) => Ok(r),
                _ => Err(error(ErrorCode::Internal)),
            }
        })
    }
    fn history<'a>(
        &'a self,
        context: &'a CallContext,
        page: PageRequest,
    ) -> RoutingFuture<'a, Result<Page<ConfigurationRevision>, ControlError>> {
        Box::pin(async move {
            match self.request(context, Command::History(page)).await? {
                Reply::History(r) => Ok(r),
                _ => Err(error(ErrorCode::Internal)),
            }
        })
    }
    fn compare_and_swap<'a>(
        &'a self,
        context: &'a CallContext,
        expected: ConfigRevisionId,
        desired: DesiredConfiguration,
    ) -> RoutingFuture<'a, Result<ConfigurationRevision, ControlError>> {
        Box::pin(async move {
            match self
                .request(context, Command::Cas(expected, desired))
                .await?
            {
                Reply::Revision(r) => Ok(r),
                _ => Err(error(ErrorCode::Internal)),
            }
        })
    }
}
