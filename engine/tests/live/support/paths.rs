use std::path::{Path, PathBuf};

pub(crate) fn data(relative: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data")
        .join(relative)
}
