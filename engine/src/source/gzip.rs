use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

use flate2::read::GzDecoder;

use crate::error::{MarketForgeError, Result};

pub fn open_gzip(path: impl AsRef<Path>) -> Result<GzDecoder<BufReader<File>>> {
    let path = path.as_ref();

    let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
        path: path.to_path_buf(),
        source,
    })?;

    Ok(GzDecoder::new(BufReader::new(file)))
}

pub fn with_gzip_reader<T, F>(path: impl AsRef<Path>, callback: F) -> Result<T>
where
    F: FnOnce(&mut dyn Read) -> Result<T>,
{
    let mut reader = open_gzip(path)?;

    callback(&mut reader)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::{Read, Write},
    };

    use flate2::{Compression, write::GzEncoder};

    use super::*;

    #[test]
    fn streams_gzip_source() {
        let path = std::env::temp_dir().join("marketforge-test-source.csv.gz");

        let file = File::create(&path).expect("create gzip fixture");

        let mut encoder = GzEncoder::new(file, Compression::default());

        encoder
            .write_all(b"timestamp,price\n1,100\n")
            .expect("write gzip fixture");

        encoder.finish().expect("finish gzip fixture");

        let mut reader = open_gzip(&path).expect("open gzip source");

        let mut contents = String::new();

        reader
            .read_to_string(&mut contents)
            .expect("decompress source");

        fs::remove_file(&path).expect("remove gzip fixture");

        assert_eq!(contents, "timestamp,price\n1,100\n",);
    }
}
