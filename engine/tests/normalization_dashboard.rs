//! Isolated dashboard tests avoid unrelated library-test targets.
use marketforge_engine as fixture_engine;
pub use marketforge_engine::{historical, shared};
#[allow(dead_code)]
#[path = "../src/tui/processing.rs"]
mod dashboard;
#[path = "historical/support/normalization_fixture.rs"]
mod normalization_fixture;
