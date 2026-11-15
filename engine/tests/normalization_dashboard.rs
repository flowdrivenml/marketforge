//! Isolated dashboard tests avoid unrelated library-test targets.
pub use marketforge_engine::{historical, shared};
#[allow(dead_code)]
#[path = "../src/tui/processing.rs"]
mod dashboard;
