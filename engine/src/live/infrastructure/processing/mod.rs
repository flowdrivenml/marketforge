pub mod controller;
pub mod instance;
pub mod lifecycle;
pub mod mailbox;
pub mod pool;
pub mod scheduler;

pub use mailbox::{
    AdmissionError, BufferBudget, BufferStatus, MailboxError, MailboxLimits, MailboxPushError,
    MailboxStatus, ProcessorMailbox, WorkBatch,
};

pub use pool::{
    PoolError, PoolStats, ProcessingHandle, ProcessingPool, Processor, ProcessorFailure,
    ProcessorFailureKind, ShutdownOutcome,
};

pub use scheduler::{ExecutionLease, ProcessingScheduler, ProcessorToken, SchedulerError};

pub use instance::{InstanceError, ProcessorIngress, ProcessorInstance, StopOutcome};

pub use lifecycle::{FailureReconcileError, FailureReconcileOutcome, reconcile_processor_failure};

pub use controller::{ControllerError, DerivedProcessorController};

#[cfg(test)]
mod regressions;
