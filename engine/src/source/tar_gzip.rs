use std::{
    fs::File,
    io::{BufReader, Cursor, Read},
    path::Path,
};

use flate2::read::GzDecoder;
use tar::Archive;

use crate::error::{MarketForgeError, Result};

pub fn open_tar_gzip_buffered(
    path: impl AsRef<Path>,
    requested_member: Option<&str>,
) -> Result<Cursor<Vec<u8>>> {
    let path = path.as_ref();

    let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
        path: path.to_path_buf(),
        source,
    })?;

    let decoder = GzDecoder::new(BufReader::new(file));

    let mut archive = Archive::new(decoder);

    let entries = archive
        .entries()
        .map_err(|error| MarketForgeError::ArchiveOpen {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    let mut selected = None;

    for entry in entries {
        let mut entry = entry.map_err(|error| MarketForgeError::ArchiveOpen {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

        if !entry.header().entry_type().is_file() {
            continue;
        }

        let member_path = entry
            .path()
            .map_err(|error| MarketForgeError::ArchiveOpen {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        let member_name = member_path.to_string_lossy();

        if let Some(requested) = requested_member {
            if member_name != requested {
                continue;
            }

            let mut bytes = Vec::new();

            entry
                .read_to_end(&mut bytes)
                .map_err(|source| MarketForgeError::SourceRead {
                    path: path.to_path_buf(),
                    source,
                })?;

            return Ok(Cursor::new(bytes));
        }

        if selected.is_some() {
            return Err(MarketForgeError::AmbiguousArchiveMembers {
                path: path.to_path_buf(),
            });
        }

        let mut bytes = Vec::new();

        entry
            .read_to_end(&mut bytes)
            .map_err(|source| MarketForgeError::SourceRead {
                path: path.to_path_buf(),
                source,
            })?;

        selected = Some(bytes);
    }

    if let Some(member) = requested_member {
        return Err(MarketForgeError::ArchiveMemberNotFound {
            path: path.to_path_buf(),
            member: member.to_owned(),
        });
    }

    selected
        .map(Cursor::new)
        .ok_or_else(|| MarketForgeError::AmbiguousArchiveMembers {
            path: path.to_path_buf(),
        })
}

pub fn with_tar_gzip_reader<T, F>(
    path: impl AsRef<Path>,
    requested_member: Option<&str>,
    callback: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Read) -> Result<T>,
{
    let path = path.as_ref();

    let selected_member = resolve_tar_gzip_member(path, requested_member)?;

    let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
        path: path.to_path_buf(),
        source,
    })?;

    let decoder = GzDecoder::new(BufReader::new(file));

    let mut archive = Archive::new(decoder);

    let entries = archive
        .entries()
        .map_err(|error| MarketForgeError::ArchiveOpen {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    for entry in entries {
        let mut entry = entry.map_err(|error| MarketForgeError::ArchiveOpen {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

        if !entry.header().entry_type().is_file() {
            continue;
        }

        let member_path = entry
            .path()
            .map_err(|error| MarketForgeError::ArchiveOpen {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        if member_path.as_ref() == Path::new(&selected_member) {
            return callback(&mut entry);
        }
    }

    Err(MarketForgeError::ArchiveMemberNotFound {
        path: path.to_path_buf(),
        member: selected_member,
    })
}

fn resolve_tar_gzip_member(path: &Path, requested_member: Option<&str>) -> Result<String> {
    let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
        path: path.to_path_buf(),
        source,
    })?;

    let decoder = GzDecoder::new(BufReader::new(file));

    let mut archive = Archive::new(decoder);

    let entries = archive
        .entries()
        .map_err(|error| MarketForgeError::ArchiveOpen {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

    let mut selected = None;

    for entry in entries {
        let entry = entry.map_err(|error| MarketForgeError::ArchiveOpen {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;

        if !entry.header().entry_type().is_file() {
            continue;
        }

        let member_path = entry
            .path()
            .map_err(|error| MarketForgeError::ArchiveOpen {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        let member = member_path.to_string_lossy();

        if let Some(requested) = requested_member {
            if member == requested {
                return Ok(member.into_owned());
            }

            continue;
        }

        if selected.is_some() {
            return Err(MarketForgeError::AmbiguousArchiveMembers {
                path: path.to_path_buf(),
            });
        }

        selected = Some(member.into_owned());
    }

    if let Some(requested) = requested_member {
        return Err(MarketForgeError::ArchiveMemberNotFound {
            path: path.to_path_buf(),
            member: requested.to_owned(),
        });
    }

    selected.ok_or_else(|| MarketForgeError::AmbiguousArchiveMembers {
        path: path.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::Read,
    };

    use flate2::{Compression, write::GzEncoder};
    use tar::{Builder, Header};

    use super::*;

    #[test]
    fn opens_single_member_tar_gzip() {
        let path = std::env::temp_dir().join("marketforge-test-source.tar.gz");

        let file = File::create(&path).expect("create TAR.GZ");

        let encoder = GzEncoder::new(file, Compression::default());

        let mut builder = Builder::new(encoder);

        let contents = b"timestamp,price\n1,100\n";

        let mut header = Header::new_gnu();

        header.set_size(contents.len() as u64);

        header.set_mode(0o644);
        header.set_cksum();

        builder
            .append_data(&mut header, "trades.csv", &contents[..])
            .expect("append TAR member");

        let encoder = builder.into_inner().expect("finish TAR");

        encoder.finish().expect("finish GZIP");

        let mut reader = open_tar_gzip_buffered(&path, None).expect("open TAR.GZ");

        let mut result = String::new();

        reader.read_to_string(&mut result).expect("read TAR member");

        fs::remove_file(&path).expect("remove TAR.GZ");

        assert_eq!(result, "timestamp,price\n1,100\n",);
    }
}
