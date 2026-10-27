use std::collections::HashMap;

use super::{
    key::RecoveryAcquisitionKey,
    model::{RecoveryJobId, RecoveryRequest, RecoveryRequestId, RecoveryRequester},
};

// ============================================================
// CONFIGURATION
// ============================================================

#[derive(Debug, Clone, Copy)]
pub struct RecoveryRegistryLimits {
    pub max_requests: usize,
    pub max_jobs: usize,
}

impl RecoveryRegistryLimits {
    pub fn validate(self) -> Result<(), RecoveryRegistryError> {
        if self.max_requests == 0 || self.max_jobs == 0 {
            return Err(RecoveryRegistryError::InvalidLimits);
        }

        Ok(())
    }
}

// ============================================================
// JOB STATE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryJobState {
    Queued,
    Running,
}

// ============================================================
// REGISTRATION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryRegistration {
    pub request_id: RecoveryRequestId,
    pub job_id: RecoveryJobId,
    pub created_job: bool,
}

// ============================================================
// CANCELLATION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryCancellation {
    pub request_id: RecoveryRequestId,
    pub job_id: RecoveryJobId,

    /// True when cancelling this requester leaves
    /// no other requesters attached to the job.
    pub job_became_empty: bool,

    /// The job had already been marked Running.
    ///
    /// If job_became_empty is also true, the future
    /// scheduler/executor should cancel that acquisition.
    pub was_running: bool,
}

// ============================================================
// COMPLETION
// ============================================================

/// A requester that remained attached until job completion.
///
/// The complete original request is preserved, including its
/// trusted source epoch and stream-specific attempt identity.
#[derive(Debug)]
pub struct RecoveryCompletionRecipient {
    pub request_id: RecoveryRequestId,
    pub request: RecoveryRequest,
}

#[derive(Debug)]
pub struct RecoveryJobCompletion {
    pub job_id: RecoveryJobId,
    pub recipients: Vec<RecoveryCompletionRecipient>,
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryRegistryError {
    InvalidLimits,
    RequestCapacityExceeded,
    JobCapacityExceeded,
    DuplicateRequester,
    RequestIdExhausted,
    JobIdExhausted,
    UnknownRequest,
    UnknownJob,
    JobNotQueued,
    JobNotRunning,
}

// ============================================================
// INTERNAL RECORDS
// ============================================================

#[derive(Debug)]
struct RegisteredRequest {
    job_id: RecoveryJobId,
    request: RecoveryRequest,
}

#[derive(Debug)]
struct RegisteredJob {
    key: RecoveryAcquisitionKey,
    state: RecoveryJobState,
    request_ids: Vec<RecoveryRequestId>,
}

// ============================================================
// SHARED RECOVERY REGISTRY
// ============================================================

/// Deterministic in-memory state owned by the future
/// single Recovery Manager coordinator.
///
/// This registry:
/// - Does not perform I/O.
/// - Does not spawn tasks.
/// - Does not validate order-book sequences.
/// - Does not normalize REST responses.
/// - Does not determine whether recovery succeeded.
///
/// All methods require exclusive mutable access.
/// The future coordinator will serialize access through
/// its event loop rather than exposing a global mutex.
#[derive(Debug)]
pub struct RecoveryRegistry {
    limits: RecoveryRegistryLimits,

    next_request_id: u64,
    next_job_id: u64,

    requests: HashMap<RecoveryRequestId, RegisteredRequest>,
    requester_index: HashMap<RecoveryRequester, RecoveryRequestId>,

    jobs: HashMap<RecoveryJobId, RegisteredJob>,

    /// Only queued jobs appear here.
    ///
    /// Once a job starts, it is no longer eligible for
    /// coalescing by newly arriving requests.
    queued_by_key: HashMap<RecoveryAcquisitionKey, RecoveryJobId>,
}

impl RecoveryRegistry {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(limits: RecoveryRegistryLimits) -> Result<Self, RecoveryRegistryError> {
        limits.validate()?;

        Ok(Self {
            limits,
            next_request_id: 1,
            next_job_id: 1,
            requests: HashMap::new(),
            requester_index: HashMap::new(),
            jobs: HashMap::new(),
            queued_by_key: HashMap::new(),
        })
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    pub fn job_count(&self) -> usize {
        self.jobs.len()
    }

    pub fn job_state(&self, job_id: RecoveryJobId) -> Option<RecoveryJobState> {
        self.jobs.get(&job_id).map(|job| job.state)
    }

    pub fn requester_count(&self, job_id: RecoveryJobId) -> Option<usize> {
        self.jobs.get(&job_id).map(|job| job.request_ids.len())
    }

    pub fn queued_jobs(&self) -> Vec<RecoveryJobId> {
        let mut jobs: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|(id, job)| (job.state == RecoveryJobState::Queued).then_some(*id))
            .collect();

        jobs.sort_by_key(|id| id.0);
        jobs
    }

    pub fn request(&self, request_id: RecoveryRequestId) -> Option<&RecoveryRequest> {
        self.requests.get(&request_id).map(|entry| &entry.request)
    }

    pub fn job_key(&self, job_id: RecoveryJobId) -> Option<&RecoveryAcquisitionKey> {
        self.jobs.get(&job_id).map(|job| &job.key)
    }

    // ========================================================
    // REQUEST REGISTRATION
    // ========================================================

    pub fn register(
        &mut self,
        request: RecoveryRequest,
    ) -> Result<RecoveryRegistration, RecoveryRegistryError> {
        if self.requester_index.contains_key(&request.requester) {
            return Err(RecoveryRegistryError::DuplicateRequester);
        }

        if self.requests.len() >= self.limits.max_requests {
            return Err(RecoveryRegistryError::RequestCapacityExceeded);
        }

        let key = RecoveryAcquisitionKey::from_request(&request);

        let existing_job = self.queued_by_key.get(&key).copied();

        if existing_job.is_none() && self.jobs.len() >= self.limits.max_jobs {
            return Err(RecoveryRegistryError::JobCapacityExceeded);
        }

        // Check both ID spaces before mutating any index.
        let request_id = RecoveryRequestId(self.next_request_id);

        let next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(RecoveryRegistryError::RequestIdExhausted)?;

        let (job_id, next_job_id, created_job) = if let Some(job_id) = existing_job {
            (job_id, self.next_job_id, false)
        } else {
            let next = self
                .next_job_id
                .checked_add(1)
                .ok_or(RecoveryRegistryError::JobIdExhausted)?;

            (RecoveryJobId(self.next_job_id), next, true)
        };

        // No fallible validation remains below this point.
        self.next_request_id = next_request_id;
        self.next_job_id = next_job_id;

        if created_job {
            self.jobs.insert(
                job_id,
                RegisteredJob {
                    key: key.clone(),
                    state: RecoveryJobState::Queued,
                    request_ids: Vec::new(),
                },
            );

            self.queued_by_key.insert(key, job_id);
        }

        let job = self
            .jobs
            .get_mut(&job_id)
            .expect("queued job index must reference an existing job");

        debug_assert_eq!(job.state, RecoveryJobState::Queued);

        job.request_ids.push(request_id);

        self.requester_index
            .insert(request.requester.clone(), request_id);

        self.requests
            .insert(request_id, RegisteredRequest { job_id, request });

        Ok(RecoveryRegistration {
            request_id,
            job_id,
            created_job,
        })
    }

    // ========================================================
    // JOB START
    // ========================================================

    /// Reserve a queued job for execution.
    ///
    /// Removes the job from the coalescing index.
    ///
    /// This must occur before the scheduler starts its
    /// asynchronous REST acquisition.
    pub fn start(&mut self, job_id: RecoveryJobId) -> Result<(), RecoveryRegistryError> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(RecoveryRegistryError::UnknownJob)?;

        if job.state != RecoveryJobState::Queued {
            return Err(RecoveryRegistryError::JobNotQueued);
        }

        self.queued_by_key.remove(&job.key);
        job.state = RecoveryJobState::Running;

        Ok(())
    }

    // ========================================================
    // INDIVIDUAL CANCELLATION
    // ========================================================

    pub fn cancel(
        &mut self,
        request_id: RecoveryRequestId,
    ) -> Result<RecoveryCancellation, RecoveryRegistryError> {
        let registered = self
            .requests
            .remove(&request_id)
            .ok_or(RecoveryRegistryError::UnknownRequest)?;

        self.requester_index.remove(&registered.request.requester);

        let job_id = registered.job_id;

        let job = self
            .jobs
            .get_mut(&job_id)
            .expect("registered request must reference an existing job");

        job.request_ids.retain(|id| *id != request_id);

        let job_became_empty = job.request_ids.is_empty();

        let was_running = job.state == RecoveryJobState::Running;

        if job_became_empty {
            self.remove_job(job_id);
        }

        Ok(RecoveryCancellation {
            request_id,
            job_id,
            job_became_empty,
            was_running,
        })
    }

    // ========================================================
    // GENERATION CANCELLATION
    // ========================================================

    /// Detach all requests belonging to one exact logical
    /// stream generation.
    ///
    /// This is called when the owning stream supersedes
    /// that generation.
    ///
    /// The caller must still reject stale completions using
    /// the source epoch and Integrity synchronization token.
    pub fn cancel_generation(
        &mut self,
        stream_id: &str,
        stream_generation: u64,
    ) -> Vec<RecoveryCancellation> {
        let mut request_ids: Vec<_> = self
            .requests
            .iter()
            .filter_map(|(id, registered)| {
                let requester = &registered.request.requester;

                (requester.stream_id == stream_id
                    && requester.epoch.stream_generation == stream_generation)
                    .then_some(*id)
            })
            .collect();

        request_ids.sort_by_key(|id| id.0);

        request_ids
            .into_iter()
            .map(|id| {
                self.cancel(id)
                    .expect("selected request must still be registered")
            })
            .collect()
    }

    // ========================================================
    // JOB COMPLETION
    // ========================================================

    /// Finish one running acquisition and detach its
    /// remaining requesters.
    ///
    /// The actual REST result is held by the coordinator,
    /// not by this registry.
    ///
    /// The coordinator uses the returned recipients to
    /// construct stream-specific completion events.
    pub fn complete(
        &mut self,
        job_id: RecoveryJobId,
    ) -> Result<RecoveryJobCompletion, RecoveryRegistryError> {
        let state = self
            .job_state(job_id)
            .ok_or(RecoveryRegistryError::UnknownJob)?;

        if state != RecoveryJobState::Running {
            return Err(RecoveryRegistryError::JobNotRunning);
        }

        let job = self
            .jobs
            .remove(&job_id)
            .expect("job existence checked above");

        let recipients = job
            .request_ids
            .into_iter()
            .map(|request_id| {
                let registered = self
                    .requests
                    .remove(&request_id)
                    .expect("job membership must reference an existing request");

                self.requester_index.remove(&registered.request.requester);

                RecoveryCompletionRecipient {
                    request_id,
                    request: registered.request,
                }
            })
            .collect();

        Ok(RecoveryJobCompletion { job_id, recipients })
    }

    // ========================================================
    // INTERNAL JOB REMOVAL
    // ========================================================

    fn remove_job(&mut self, job_id: RecoveryJobId) {
        let Some(job) = self.jobs.remove(&job_id) else {
            return;
        };

        if job.state == RecoveryJobState::Queued {
            // Never remove a newer queued job that happens
            // to share this acquisition key.
            if self.queued_by_key.get(&job.key) == Some(&job_id) {
                self.queued_by_key.remove(&job.key);
            }
        }
    }
    /// Borrow one request belonging to a registered job.
    ///
    /// Every requester attached to a shared job has the same
    /// conservative acquisition key.
    ///
    /// The returned request is suitable for selecting the
    /// acquisition operation, but its requester identity must
    /// not be used as the identity of every attached stream.
    ///
    /// Individual requesters are recovered through complete().
    pub fn job_request(&self, job_id: RecoveryJobId) -> Option<&RecoveryRequest> {
        let job = self.jobs.get(&job_id)?;

        let request_id = job.request_ids.first()?;

        self.requests
            .get(request_id)
            .map(|registered| &registered.request)
    }
    ///
    /// IDs are sorted for deterministic bulk cancellation.
    ///
    /// The caller receives a snapshot of IDs, not mutable
    /// access to the registry's internal indexes.
    pub fn request_ids(&self) -> Vec<RecoveryRequestId> {
        let mut ids: Vec<_> = self.requests.keys().copied().collect();

        ids.sort_by_key(|id| id.0);

        ids
    }
}
