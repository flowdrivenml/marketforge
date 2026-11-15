//! Reusable historical runtime settings, independent of processing operations.
#[cfg(feature = "historical")]
pub mod config;
pub mod integrity;
pub mod resources;
mod runtime_validation;

#[cfg(feature = "historical")]
pub use config::{
    ProcessingConfig, load_processing_config, project_root, resolve_path,
    validate_processing_config,
};
pub use integrity::*;
pub use resources::*;
pub use runtime_validation::{validate_integrity_policy, validate_resources};
pub mod budget;

pub mod limits;
