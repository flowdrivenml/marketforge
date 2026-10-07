use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use crate::error::{MarketForgeError, Result};

pub fn open_plain(path: impl AsRef<Path>) -> Result<BufReader<File>> {
    let path = path.as_ref();

    let file = File::open(path).map_err(|error| MarketForgeError::SourceOpen {
        path: path.to_path_buf(),
        source: error,
    })?;

    Ok(BufReader::new(file))
}

pub fn with_plain_reader<T, F>(path: impl AsRef<Path>, callback: F) -> Result<T>
where
    F: FnOnce(&mut dyn Read) -> Result<T>,
{
    let mut reader = open_plain(path)?;

    callback(&mut reader)
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Read};

    use super::*;

    #[test]
    fn opens_and_reads_plain_file() {
        let path = std::env::temp_dir().join("marketforge-test-plain-source.txt");

        fs::write(&path, b"marketforge").expect("failed to create test file");

        let mut reader = open_plain(&path).expect("failed to open source");

        let mut contents = String::new();

        reader
            .read_to_string(&mut contents)
            .expect("failed to read source");

        fs::remove_file(&path).expect("failed to remove test file");

        assert_eq!(contents, "marketforge");
    }

    #[test]
    fn missing_file_returns_source_open_error() {
        let path = std::env::temp_dir().join("marketforge-file-that-does-not-exist");

        let error = open_plain(&path).expect_err("open should fail");

        assert!(matches!(error, MarketForgeError::SourceOpen { .. }));
    }
}
