use std::path::Path;

pub(crate) fn read(path: impl AsRef<Path>) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}
