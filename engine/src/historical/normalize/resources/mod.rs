mod budget;
pub(crate) mod estimate;
mod scratch;

pub use budget::{ResourceBudget, ResourceRequirements, ResourceReservation, ResourceUsage};
pub use estimate::{
    EstimateBreakdown, estimate_breakdown, estimate_task_resources,
    estimate_task_resources_with_limits,
};
pub use scratch::ScratchWorkspace;

mod classification;
pub use classification::{SourceFormat, source_format};

pub(crate) use estimate::{retained_jobs_memory, retained_task_memory};

#[cfg(test)]
mod safety_tests;

pub(crate) use estimate::reserve_isolated;

pub(crate) use estimate::job_finalization_memory;

pub(crate) use estimate::reserve_compatibility_trade;

pub(crate) use estimate::prepared_task_memory;

pub(crate) mod compute;

mod admission;
pub use admission::{AdmissionDiagnostic, JobRetention, StreamAdmission, inspect_admission};

pub(crate) use estimate::boundary_read_limits;
