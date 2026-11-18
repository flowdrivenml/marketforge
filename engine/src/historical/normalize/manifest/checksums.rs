//! Byte fingerprints for publication artifacts, written inside staging.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

pub fn file_sha256(path: &Path) -> io::Result<String> {
    hash_file(&mut File::open(path)?)
}

fn hash_file(file: &mut File) -> io::Result<String> {
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(crate::historical::normalize::hex_digest(digest.finalize()))
}

pub fn write_publication_checksums(
    staging: &Path,
    names: impl IntoIterator<Item = PathBuf>,
) -> io::Result<()> {
    let mut directories = BTreeSet::new();
    let mut hashes = BTreeMap::new();
    for name in names {
        let path = staging.join(&name);
        let mut file = File::open(&path)?;
        hashes.insert(name, hash_file(&mut file)?);
        file.sync_all()?;
        directories.extend(
            path.ancestors()
                .skip(1)
                .take_while(|dir| dir.starts_with(staging))
                .map(Path::to_path_buf),
        );
    }
    let mut output = File::create(staging.join("publication-checksums.json"))?;
    serde_json::to_writer(&mut output, &hashes)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    for directory in directories.iter().rev() {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}
