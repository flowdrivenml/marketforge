//! Cancellation-aware and deadline-aware admission coordination.
//!
//! Admission waiting is separate from routing commitment.
//!
//! A waiting operation can finish because:
//! - Capacity becomes available.
//! - Cancellation is requested.
//! - Its deadline expires.
//!
//! A successful admission must not subsequently be revoked by
//! cancellation of its original waiting context.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use crate::live::{
    foundation::cancellation::{CancellationToken, Cancelled},
    ports::routing::RoutingAdmissionFailure,
};

/// Boxed timer future.
///
/// A timer must arrange for the waiting task to be awakened
/// when the requested deadline is reached.
pub type DeadlineFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Runtime-independent deadline timer.
///
/// Production and deterministic test implementations must
/// satisfy the same interface.
pub trait DeadlineTimer: Send + Sync {
    fn sleep_until(&self, deadline: Instant) -> DeadlineFuture;
    /// Bounded implementations reject timer admission before waiting begins.
    fn try_sleep_until(
        &self,
        deadline: Instant,
    ) -> Result<DeadlineFuture, crate::live::model::error::ErrorCode> {
        Ok(self.sleep_until(deadline))
    }
}

/// Admission cancellation and deadline configuration.
#[derive(Debug, Clone)]
pub struct AdmissionContext {
    pub cancellation: CancellationToken,
    pub deadline: Option<Instant>,
}

impl AdmissionContext {
    pub fn new(cancellation: CancellationToken, deadline: Option<Instant>) -> Self {
        Self {
            cancellation,
            deadline,
        }
    }

    pub fn without_deadline(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            deadline: None,
        }
    }
}

/// Outcome of waiting for admission.
///
/// `Ready` means the supplied admission operation completed.
/// It does not independently prove that the router committed
/// responsibility for an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionWaitResult<T> {
    Ready(T),
    Rejected(RoutingAdmissionFailure),
}

/// Coordinates an admission operation with cancellation and timeout.
///
/// The admission operation itself must enforce queue capacity,
/// ordering, and atomic commitment.
///
/// This coordinator only manages waiting.
pub struct AdmissionWait<T> {
    operation: Pin<Box<dyn Future<Output = T> + Send>>,
    cancellation: Cancelled,
    deadline: Option<DeadlineFuture>,
    completed: bool,
}

impl<T> AdmissionWait<T> {
    pub fn new<F>(
        operation: F,
        context: AdmissionContext,
        timer: Option<&dyn DeadlineTimer>,
    ) -> Result<Self, AdmissionWaitError>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let deadline = match context.deadline {
            Some(deadline) => {
                let timer = timer.ok_or(AdmissionWaitError::MissingTimer)?;

                Some(
                    timer
                        .try_sleep_until(deadline)
                        .map_err(|_| AdmissionWaitError::TimerCapacity)?,
                )
            }
            None => None,
        };

        let cancellation = context
            .cancellation
            .try_cancelled()
            .map_err(|_| AdmissionWaitError::CancellationCapacity)?;

        Ok(Self {
            operation: Box::pin(operation),
            cancellation,
            deadline,
            completed: false,
        })
    }
}

/// Construction errors for admission waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionWaitError {
    MissingTimer,
    TimerCapacity,
    CancellationCapacity,
}

impl<T> Future for AdmissionWait<T> {
    type Output = AdmissionWaitResult<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "AdmissionWait polled after completion");

        // Cancellation has priority when it is already observable.
        if Pin::new(&mut self.cancellation).poll(cx).is_ready() {
            self.completed = true;

            return Poll::Ready(AdmissionWaitResult::Rejected(
                RoutingAdmissionFailure::Cancelled,
            ));
        }

        // The timer future is responsible for waking this task
        // when the deadline expires.
        if let Some(deadline) = self.deadline.as_mut() {
            if deadline.as_mut().poll(cx).is_ready() {
                self.completed = true;

                return Poll::Ready(AdmissionWaitResult::Rejected(
                    RoutingAdmissionFailure::DeadlineExceeded,
                ));
            }
        }

        // The admission operation is polled only after checking
        // cancellation and deadline completion.
        //
        // If it completes successfully, the result is returned.
        // Cancellation after this point cannot change that result.
        match self.operation.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.completed = true;

                Poll::Ready(AdmissionWaitResult::Ready(result))
            }

            Poll::Pending => Poll::Pending,
        }
    }
}
