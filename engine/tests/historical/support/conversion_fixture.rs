//! Baseline for both portable cases and the opt-in recording.
use std::path::{Path, PathBuf};

use marketforge_engine::historical::job::{ConvertJob, load_convert_job};

pub fn canonical_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/historical/fixtures/convert.json")
}

pub fn canonical_job() -> ConvertJob {
    let path = canonical_path();
    load_convert_job(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}
