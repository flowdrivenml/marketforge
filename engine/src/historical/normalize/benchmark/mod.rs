//! Benchmark orchestration. Compilation, mocks and planning do not execute trials.
pub mod measure;
pub mod model;
pub mod plan;
pub mod provenance;
pub mod runner;
pub mod verification;
pub use model::*;
pub type Result<T> = std::result::Result<T, String>;

pub mod analysis;
pub mod comparison;
pub mod reports;
pub mod statistics;
