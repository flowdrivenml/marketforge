use std::{io, path::Path};

pub(crate) fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let path = path.as_ref();
    std::fs::read(path).map_err(|error| {
        io::Error::new(error.kind(), format!(
            "{}: {error}. Prepare Live fixtures with scripts/engine/live/fixtures.py (import or collect), then run check",
            path.display()
        ))
    })
}
