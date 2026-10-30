use std::collections::{HashMap, VecDeque};

use thiserror::Error;

/// Identifies one specific processor execution generation.
///
/// A restarted processor must receive a new generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProcessorToken {
    pub id: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessorState {
    Idle,
    Ready,
    Running,
    RunningNotified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLease {
    pub processor: ProcessorToken,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchedulerError {
    #[error("scheduler capacity must be greater than zero")]
    InvalidCapacity,

    #[error("processor is already registered")]
    AlreadyRegistered,

    #[error("processor is not registered")]
    UnknownProcessor,

    #[error("scheduler capacity exhausted")]
    Capacity,

    #[error("processor is currently executing")]
    ProcessorBusy,

    #[error("invalid processor execution state")]
    InvalidState,
}

/// Bounded, round-robin processor scheduler.
///
/// This is a scheduling state machine, not a thread pool.
///
/// Each processor can have at most one outstanding execution
/// activation. Notifications arriving during execution are
/// coalesced into one subsequent activation.
///
/// Capacity bounds ready AND running activations together.
/// Reserving capacity for running processors guarantees that
/// a notified running processor can be requeued on completion.
#[derive(Debug)]
pub struct ProcessingScheduler {
    capacity: usize,
    processors: HashMap<ProcessorToken, ProcessorState>,
    ready: VecDeque<ProcessorToken>,
    active: usize,
}

impl ProcessingScheduler {
    pub fn new(capacity: usize) -> Result<Self, SchedulerError> {
        if capacity == 0 {
            return Err(SchedulerError::InvalidCapacity);
        }

        Ok(Self {
            capacity,
            processors: HashMap::new(),
            ready: VecDeque::new(),
            active: 0,
        })
    }

    /// Register a new processor generation.
    ///
    /// Registration does not schedule execution.
    pub fn register(&mut self, token: ProcessorToken) -> Result<(), SchedulerError> {
        // A logical processor cannot have two simultaneously
        // registered generations.
        if self
            .processors
            .keys()
            .any(|existing| existing.id == token.id)
        {
            return Err(SchedulerError::AlreadyRegistered);
        }

        self.processors.insert(token, ProcessorState::Idle);

        Ok(())
    }

    /// Notify the scheduler that a processor has work.
    ///
    /// Repeated notifications do not create duplicate
    /// ready-queue entries.
    pub fn notify(&mut self, token: ProcessorToken) -> Result<(), SchedulerError> {
        let state = self
            .processors
            .get_mut(&token)
            .ok_or(SchedulerError::UnknownProcessor)?;

        match *state {
            ProcessorState::Idle => {
                if self.active >= self.capacity {
                    return Err(SchedulerError::Capacity);
                }

                self.ready.push_back(token);
                self.active += 1;
                *state = ProcessorState::Ready;
            }

            ProcessorState::Ready => {
                // Already scheduled.
            }

            ProcessorState::Running => {
                // Schedule one more turn after execution finishes.
                *state = ProcessorState::RunningNotified;
            }

            ProcessorState::RunningNotified => {
                // Additional notifications are coalesced.
            }
        }

        Ok(())
    }

    /// Select the next runnable processor.
    ///
    /// An execution lease remains owned by the worker until
    /// complete() is called.
    pub fn acquire(&mut self) -> Option<ExecutionLease> {
        let token = self.ready.pop_front()?;

        let state = self
            .processors
            .get_mut(&token)
            .expect("registered processor must exist");

        assert_eq!(*state, ProcessorState::Ready);

        *state = ProcessorState::Running;

        Some(ExecutionLease { processor: token })
    }

    /// Complete one processor execution turn.
    ///
    /// If another notification arrived during execution,
    /// the processor is appended to the back of the queue.
    pub fn complete(&mut self, lease: ExecutionLease) -> Result<(), SchedulerError> {
        let state = self
            .processors
            .get_mut(&lease.processor)
            .ok_or(SchedulerError::UnknownProcessor)?;

        match *state {
            ProcessorState::Running => {
                *state = ProcessorState::Idle;
                self.active -= 1;
            }

            ProcessorState::RunningNotified => {
                self.ready.push_back(lease.processor);
                *state = ProcessorState::Ready;
            }

            ProcessorState::Idle | ProcessorState::Ready => {
                return Err(SchedulerError::InvalidState);
            }
        }

        Ok(())
    }

    /// Unregister a processor that is no longer executing.
    ///
    /// Running processors must first complete their lease.
    pub fn unregister(&mut self, token: ProcessorToken) -> Result<(), SchedulerError> {
        let state = *self
            .processors
            .get(&token)
            .ok_or(SchedulerError::UnknownProcessor)?;

        match state {
            ProcessorState::Running | ProcessorState::RunningNotified => {
                return Err(SchedulerError::ProcessorBusy);
            }

            ProcessorState::Ready => {
                self.ready.retain(|queued| *queued != token);
                self.active -= 1;
            }

            ProcessorState::Idle => {}
        }

        self.processors.remove(&token);

        Ok(())
    }

    pub fn ready_count(&self) -> usize {
        self.ready.len()
    }

    pub fn active_count(&self) -> usize {
        self.active
    }

    pub fn registered_count(&self) -> usize {
        self.processors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(id: u64) -> ProcessorToken {
        ProcessorToken { id, generation: 1 }
    }

    #[test]
    fn rejects_zero_capacity() {
        assert!(matches!(
            ProcessingScheduler::new(0),
            Err(SchedulerError::InvalidCapacity)
        ));
    }

    #[test]
    fn notifications_are_coalesced() {
        let mut scheduler = ProcessingScheduler::new(4).unwrap();

        scheduler.register(token(1)).unwrap();

        scheduler.notify(token(1)).unwrap();
        scheduler.notify(token(1)).unwrap();
        scheduler.notify(token(1)).unwrap();

        assert_eq!(scheduler.ready_count(), 1);
        assert_eq!(scheduler.active_count(), 1);
    }

    #[test]
    fn processor_cannot_execute_twice_concurrently() {
        let mut scheduler = ProcessingScheduler::new(4).unwrap();

        scheduler.register(token(1)).unwrap();
        scheduler.notify(token(1)).unwrap();

        let first = scheduler.acquire().unwrap();

        assert_eq!(first.processor, token(1));

        // No second lease can be acquired while running.
        assert!(scheduler.acquire().is_none());

        // Notification while running schedules a later turn.
        scheduler.notify(token(1)).unwrap();

        assert!(scheduler.acquire().is_none());

        scheduler.complete(first).unwrap();

        let second = scheduler.acquire().unwrap();

        assert_eq!(second.processor, token(1));

        scheduler.complete(second).unwrap();

        assert_eq!(scheduler.active_count(), 0);
    }

    #[test]
    fn processors_execute_in_round_robin_order() {
        let mut scheduler = ProcessingScheduler::new(4).unwrap();

        for id in 1..=3 {
            scheduler.register(token(id)).unwrap();
            scheduler.notify(token(id)).unwrap();
        }

        let first = scheduler.acquire().unwrap();
        assert_eq!(first.processor, token(1));

        scheduler.notify(token(1)).unwrap();
        scheduler.complete(first).unwrap();

        let second = scheduler.acquire().unwrap();
        assert_eq!(second.processor, token(2));
        scheduler.complete(second).unwrap();

        let third = scheduler.acquire().unwrap();
        assert_eq!(third.processor, token(3));
        scheduler.complete(third).unwrap();

        let fourth = scheduler.acquire().unwrap();
        assert_eq!(fourth.processor, token(1));
        scheduler.complete(fourth).unwrap();

        assert_eq!(scheduler.active_count(), 0);
    }

    #[test]
    fn capacity_rejection_preserves_idle_processor() {
        let mut scheduler = ProcessingScheduler::new(1).unwrap();

        scheduler.register(token(1)).unwrap();
        scheduler.register(token(2)).unwrap();

        scheduler.notify(token(1)).unwrap();

        assert_eq!(scheduler.notify(token(2)), Err(SchedulerError::Capacity));

        let lease = scheduler.acquire().unwrap();
        scheduler.complete(lease).unwrap();

        // A rejected notification does not corrupt processor state.
        scheduler.notify(token(2)).unwrap();

        assert_eq!(scheduler.acquire().unwrap().processor, token(2));
    }

    #[test]
    fn cannot_unregister_running_processor() {
        let mut scheduler = ProcessingScheduler::new(2).unwrap();

        scheduler.register(token(1)).unwrap();
        scheduler.notify(token(1)).unwrap();

        let lease = scheduler.acquire().unwrap();

        assert_eq!(
            scheduler.unregister(token(1)),
            Err(SchedulerError::ProcessorBusy)
        );

        scheduler.complete(lease).unwrap();
        scheduler.unregister(token(1)).unwrap();

        assert_eq!(scheduler.registered_count(), 0);
    }

    #[test]
    fn new_generation_requires_old_generation_to_stop() {
        let mut scheduler = ProcessingScheduler::new(2).unwrap();

        let old = ProcessorToken {
            id: 1,
            generation: 1,
        };

        let new = ProcessorToken {
            id: 1,
            generation: 2,
        };

        scheduler.register(old).unwrap();
        scheduler.notify(old).unwrap();

        let lease = scheduler.acquire().unwrap();

        assert_eq!(
            scheduler.register(new),
            Err(SchedulerError::AlreadyRegistered)
        );

        assert_eq!(
            scheduler.unregister(old),
            Err(SchedulerError::ProcessorBusy)
        );

        scheduler.complete(lease).unwrap();
        scheduler.unregister(old).unwrap();

        scheduler.register(new).unwrap();

        // Late notifications from the old generation cannot
        // schedule the new processor.
        assert_eq!(scheduler.notify(old), Err(SchedulerError::UnknownProcessor));

        scheduler.notify(new).unwrap();

        assert_eq!(scheduler.acquire().unwrap().processor, new);
    }
}
