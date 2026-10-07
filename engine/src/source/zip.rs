use std::{
    fs::File,
    io::{Cursor, Read},
    path::Path,
};

use zip::ZipArchive;

use crate::error::{MarketForgeError, Result};

pub fn open_zip_buffered(
    path: impl AsRef<Path>,
    requested_member: Option<&str>,
) -> Result<Cursor<Vec<u8>>> {
    let path = path.as_ref();

    let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
        path: path.to_path_buf(),
        source,
    })?;

    let mut archive = ZipArchive::new(file).map_err(|error| MarketForgeError::ArchiveOpen {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;

    let member_index = match requested_member {
        Some(member) => find_member(&mut archive, path, member)?,

        None => find_unique_data_member(&mut archive, path)?,
    };

    let mut member =
        archive
            .by_index(member_index)
            .map_err(|error| MarketForgeError::ArchiveOpen {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

    let mut bytes = Vec::new();

    member
        .read_to_end(&mut bytes)
        .map_err(|source| MarketForgeError::SourceRead {
            path: path.to_path_buf(),
            source,
        })?;

    Ok(Cursor::new(bytes))
}

fn find_member(
    archive: &mut ZipArchive<File>,
    path: &Path,
    requested_member: &str,
) -> Result<usize> {
    for index in 0..archive.len() {
        let member = archive
            .by_index(index)
            .map_err(|error| MarketForgeError::ArchiveOpen {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        if !member.is_dir() && member.name() == requested_member {
            return Ok(index);
        }
    }

    Err(MarketForgeError::ArchiveMemberNotFound {
        path: path.to_path_buf(),
        member: requested_member.to_owned(),
    })
}

fn find_unique_data_member(archive: &mut ZipArchive<File>, path: &Path) -> Result<usize> {
    let mut candidate = None;

    for index in 0..archive.len() {
        let member = archive
            .by_index(index)
            .map_err(|error| MarketForgeError::ArchiveOpen {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        if member.is_dir() {
            continue;
        }

        if candidate.is_some() {
            return Err(MarketForgeError::AmbiguousArchiveMembers {
                path: path.to_path_buf(),
            });
        }

        candidate = Some(index);
    }

    candidate.ok_or_else(|| MarketForgeError::AmbiguousArchiveMembers {
        path: path.to_path_buf(),
    })
}

pub fn with_zip_reader<T, F>(
    path: impl AsRef<Path>,
    requested_member: Option<&str>,
    callback: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Read) -> Result<T>,
{
    let path = path.as_ref();

    let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
        path: path.to_path_buf(),
        source,
    })?;

    let mut archive = ZipArchive::new(file).map_err(|error| MarketForgeError::ArchiveOpen {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;

    let member_index = match requested_member {
        Some(member) => find_member(&mut archive, path, member)?,

        None => find_unique_data_member(&mut archive, path)?,
    };

    let mut member =
        archive
            .by_index(member_index)
            .map_err(|error| MarketForgeError::ArchiveOpen {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

    callback(&mut member)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::{Read, Write},
    };

    use zip::{ZipWriter, write::SimpleFileOptions};

    use super::*;

    #[test]
    fn opens_single_member_zip() {
        let path = std::env::temp_dir().join("marketforge-test-source.zip");

        let file = File::create(&path).expect("create ZIP");

        let mut writer = ZipWriter::new(file);

        writer
            .start_file("trades.csv", SimpleFileOptions::default())
            .expect("start ZIP member");

        writer
            .write_all(b"timestamp,price\n1,100\n")
            .expect("write ZIP member");

        writer.finish().expect("finish ZIP");

        let mut reader = open_zip_buffered(&path, None).expect("open ZIP");

        let mut contents = String::new();

        reader
            .read_to_string(&mut contents)
            .expect("read ZIP member");

        fs::remove_file(&path).expect("remove ZIP");

        assert_eq!(contents, "timestamp,price\n1,100\n",);
    }
}
