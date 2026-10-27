use std::collections::HashSet;

use super::{
    model::{RecoveryJobId, RecoveryRequest, RecoveryRequestId, RecoverySourceEpoch},
    registry::{
        RecoveryCancellation, RecoveryJobCompletion, RecoveryRegistration, RecoveryRegistry,
        RecoveryRegistryError, RecoveryRegistryLimits,
    },
};

// ============================================================
// CONFIGURATION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverySchedulerLimits {
    pub max_requests: usize,
    pub max_jobs: usize,
    pub max_concurrent_jobs: usize,
}

impl RecoverySchedulerLimits {
    pub fn validate(self) -> Result<(), RecoverySchedulerError> {
        if self.max_requests == 0 || self.max_jobs == 0 || self.max_concurrent_jobs == 0 {
            return Err(RecoverySchedulerError::InvalidLimits);
        }

        Ok(())
    }
}

// ============================================================
// EXECUTION INSTRUCTION
// ============================================================

/// A job reserved for asynchronous execution.
///
/// The request supplies acquisition parameters only.
///
/// The coordinator must retain job_id when spawning work,
/// and later return its completion to this scheduler.
///
/// The request's stream-specific epoch and attempt are not
/// shared with other recipients.
#[derive(Debug, Clone)]
pub struct RecoveryScheduledJob {
    pub job_id: RecoveryJobId,
    pub request: RecoveryRequest,
}

// ============================================================
// CANCELLATION INSTRUCTION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryScheduledCancellation {
    pub cancellation: RecoveryCancellation,

    /// The coordinator must abort the corresponding
    /// asynchronous task when this field is Some.
    ///
    /// Cancellation of a queued job requires no task abort.
    pub abort_job: Option<RecoveryJobId>,
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RecoverySchedulerError {
    #[error("invalid recovery scheduler limits")]
    InvalidLimits,

    #[error("recovery registry error: {0:?}")]
    Registry(RecoveryRegistryError),

    #[error("missing request for recovery job {0:?}")]
    MissingJobRequest(RecoveryJobId),

    #[error("inconsistent running state for recovery job {0:?}")]
    InconsistentRunningState(RecoveryJobId),
}

impl From<RecoveryRegistryError> for RecoverySchedulerError {
    fn from(value: RecoveryRegistryError) -> Self {
        Self::Registry(value)
    }
}

// ============================================================
// SCHEDULER
// ============================================================

/// Bounded, deterministic recovery job scheduler.
///
/// Owns the authoritative RecoveryRegistry.
///
/// Does not:
/// - perform HTTP;
/// - spawn Tokio tasks;
/// - apply exchange rate limits;
/// - retry failed HTTP requests;
/// - normalize responses;
/// - update stream Integrity state.
///
/// The future coordinator will serialize scheduler operations
/// in its event loop.
#[derive(Debug)]
pub struct RecoveryScheduler {
    registry: RecoveryRegistry,
    limits: RecoverySchedulerLimits,

    running: HashSet<RecoveryJobId>,
}

impl RecoveryScheduler {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(limits: RecoverySchedulerLimits) -> Result<Self, RecoverySchedulerError> {
        limits.validate()?;

        let registry = RecoveryRegistry::new(RecoveryRegistryLimits {
            max_requests: limits.max_requests,
            max_jobs: limits.max_jobs,
        })?;

        Ok(Self {
            registry,
            limits,
            running: HashSet::new(),
        })
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn request_count(&self) -> usize {
        self.registry.request_count()
    }

    pub fn job_count(&self) -> usize {
        self.registry.job_count()
    }

    pub fn running_count(&self) -> usize {
        self.running.len()
    }

    pub fn available_slots(&self) -> usize {
        self.limits
            .max_concurrent_jobs
            .saturating_sub(self.running.len())
    }

    pub fn queued_jobs(&self) -> Vec<RecoveryJobId> {
        self.registry.queued_jobs()
    }

    pub fn is_running(&self, job_id: RecoveryJobId) -> bool {
        self.running.contains(&job_id)
    }

    // ========================================================
    // REQUEST ADMISSION
    // ========================================================

    /// Register one already-validated logical recovery request.
    ///
    /// This does not start an HTTP operation.
    ///
    /// The caller invokes start_ready_jobs() after processing
    /// the current set of incoming commands.
    pub fn register(
        &mut self,
        request: RecoveryRequest,
    ) -> Result<RecoveryRegistration, RecoverySchedulerError> {
        Ok(self.registry.register(request)?)
    }

    // ========================================================
    // JOB DISPATCH
    // ========================================================

    /// Reserve queued jobs up to the configured concurrency
    /// limit, in ascending job-ID order.
    ///
    /// Each returned job has been transitioned to Running
    /// inside the registry.
    ///
    /// The coordinator must either:
    /// - execute each returned job and report completion; or
    /// - cancel its requesters if execution cannot be started.
    ///
    /// This method must not be called by multiple independent
    /// executors operating on the same scheduler.
    pub fn start_ready_jobs(
        &mut self,
    ) -> Result<Vec<RecoveryScheduledJob>, RecoverySchedulerError> {
        let available = self.available_slots();

        if available == 0 {
            return Ok(Vec::new());
        }

        let queued = self.registry.queued_jobs();

        // Gather and validate all selected requests before
        // changing registry or scheduler state.
        let selected = queued
            .into_iter()
            .take(available)
            .map(|job_id| {
                let request = self
                    .registry
                    .job_request(job_id)
                    .ok_or(RecoverySchedulerError::MissingJobRequest(job_id))?
                    .clone();

                Ok(RecoveryScheduledJob { job_id, request })
            })
            .collect::<Result<Vec<_>, RecoverySchedulerError>>()?;

        for job in &selected {
            self.registry.start(job.job_id)?;

            let inserted = self.running.insert(job.job_id);

            debug_assert!(inserted, "a queued recovery job cannot already be running");
        }

        Ok(selected)
    }

    // ========================================================
    // INDIVIDUAL CANCELLATION
    // ========================================================

    /// Detach one requester from its recovery job.
    ///
    /// A shared running job remains active while at least
    /// one requester still needs its result.
    pub fn cancel(
        &mut self,
        request_id: RecoveryRequestId,
    ) -> Result<RecoveryScheduledCancellation, RecoverySchedulerError> {
        let cancellation = self.registry.cancel(request_id)?;

        Ok(self.apply_cancellation(cancellation))
    }

    // ========================================================
    // GENERATION CANCELLATION
    // ========================================================

    /// Detach requests belonging to one stream generation.
    ///
    /// Returns abort instructions for running jobs that
    /// became empty.
    ///
    /// Does not affect other streams sharing those jobs.
    pub fn cancel_generation(
        &mut self,
        stream_id: &str,
        stream_generation: u64,
    ) -> Vec<RecoveryScheduledCancellation> {
        self.registry
            .cancel_generation(stream_id, stream_generation)
            .into_iter()
            .map(|cancellation| self.apply_cancellation(cancellation))
            .collect()
    }

    // ========================================================
    // JOB COMPLETION
    // ========================================================

    /// Complete one currently running job.
    ///
    /// This removes the job and returns only the requesters
    /// still registered at completion time.
    ///
    /// The async coordinator owns the actual REST result
    /// and must deliver it independently to each recipient.
    pub fn complete(
        &mut self,
        job_id: RecoveryJobId,
    ) -> Result<RecoveryJobCompletion, RecoverySchedulerError> {
        if !self.running.contains(&job_id) {
            return Err(RecoverySchedulerError::InconsistentRunningState(job_id));
        }

        let completion = self.registry.complete(job_id)?;

        self.running.remove(&job_id);

        Ok(completion)
    }

    // ========================================================
    // INTERNAL CANCELLATION ACCOUNTING
    // ========================================================

    fn apply_cancellation(
        &mut self,
        cancellation: RecoveryCancellation,
    ) -> RecoveryScheduledCancellation {
        let abort_job = if cancellation.job_became_empty && cancellation.was_running {
            // Last requester detached from a running job.
            //
            // Release the scheduler slot and instruct the
            // coordinator to abort the acquisition.
            //
            // A late completion carrying this old job ID
            // must be discarded.
            let removed = self.running.remove(&cancellation.job_id);

            debug_assert!(removed, "running registry job missing from scheduler");

            Some(cancellation.job_id)
        } else {
            None
        };

        RecoveryScheduledCancellation {
            cancellation,
            abort_job,
        }
    }
    /// Cancel every registered recovery request.
    ///
    /// When cancellation removes the final requester from
    /// a running job, an abort instruction is returned.
    ///
    /// The registry and scheduler are empty after success.
    ///
    /// The asynchronous coordinator remains responsible
    /// for actually aborting the running futures.
    pub fn cancel_all(&mut self) -> Vec<RecoveryScheduledCancellation> {
        let request_ids = self.registry.request_ids();

        request_ids
            .into_iter()
            .map(|request_id| {
                self.cancel(request_id)
                    .expect("request selected for cancellation must exist")
            })
            .collect()
    }
    /// Inspect a currently registered recovery request.
    ///
    /// This is read-only and does not expose registry mutation.
    pub fn request(&self, request_id: RecoveryRequestId) -> Option<&RecoveryRequest> {
        self.registry.request(request_id)
    }

    /// Return the IDs of requests bound to an exact source epoch.
    ///
    /// This checks all three generation components.
    pub fn request_ids_for_epoch(
        &self,
        stream_id: &str,
        epoch: &RecoverySourceEpoch,
    ) -> Vec<RecoveryRequestId> {
        self.registry
            .request_ids()
            .into_iter()
            .filter(|request_id| {
                self.registry.request(*request_id).is_some_and(|request| {
                    request.requester.stream_id == stream_id && request.requester.epoch == *epoch
                })
            })
            .collect()
    }
}
