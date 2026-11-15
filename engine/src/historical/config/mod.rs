//! Offline archive-format inventory. Independent of Live configuration and runtime.
mod catalog;
mod job;
pub use catalog::{
    CompiledProfile, FormatCatalog, MarketCategory, PhysicalFormat, PhysicalRecord, Profile,
    RecordFormat, Rule, RuleTarget,
};
pub use job::{PROFILE_PROTOCOL_VERSION, ProfileTask, load_document, load_document_with_catalog};

pub use job::{
    NORMALIZATION_PROTOCOL_VERSION, NormalizationJobV3, NormalizationOperation,
    load_normalization_job,
};

mod batch;
pub use batch::*;

pub(crate) use batch::family_of;

#[cfg(test)]
pub(crate) use batch::selection_tests as selection_test_support;
