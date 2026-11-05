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
