#![allow(dead_code)]
pub mod assertions;
pub mod fixtures;
pub mod runtime;
pub type TestResult<T> = std::result::Result<T, String>;
pub fn root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .canonicalize()
        .unwrap()
}
pub mod depth_assertions;
pub mod migration;
pub mod profile_cases;
use marketforge_engine as fixture_engine;
pub(crate) mod normalization_fixture;
