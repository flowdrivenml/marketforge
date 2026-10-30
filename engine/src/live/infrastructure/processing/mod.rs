pub mod controller;
pub mod instance;
pub mod lifecycle;
pub mod mailbox;
pub mod pool;
pub mod scheduler;

pub use mailbox::{
    AdmissionError, MailboxError, MailboxLimits, MailboxPushError, MailboxStatus, ProcessorMailbox,
};

pub use pool::{
    PoolError, PoolStats, ProcessingPool, Processor, ProcessorFailure, ProcessorFailureKind,
};

pub use scheduler::{ExecutionLease, ProcessingScheduler, ProcessorToken, SchedulerError};

pub use instance::{InstanceError, ProcessorIngress, ProcessorInstance, StopOutcome};

pub use lifecycle::{FailureReconcileError, FailureReconcileOutcome, reconcile_processor_failure};

pub use controller::{ControllerError, DerivedProcessorController};
