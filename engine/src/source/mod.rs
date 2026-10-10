mod gzip;
mod jsonl;
mod plain;
mod tar_gzip;
mod zip;

use std::{io::Read, path::Path};

use crate::{error::Result, job::SourceContainer};
pub use jsonl::JsonlDecoder;

pub use gzip::{open_gzip, with_gzip_reader};

pub use plain::{open_plain, with_plain_reader};

pub use tar_gzip::{open_tar_gzip_buffered, with_tar_gzip_reader};

pub use zip::{open_zip_buffered, with_zip_reader};

pub fn with_source_reader<T, F>(
    path: impl AsRef<Path>,
    container: SourceContainer,
    archive_member: Option<&str>,
    callback: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Read) -> Result<T>,
{
    let path = path.as_ref();

    match container {
        SourceContainer::Plain => with_plain_reader(path, callback),

        SourceContainer::Gzip => with_gzip_reader(path, callback),

        SourceContainer::Zip => with_zip_reader(path, archive_member, callback),

        SourceContainer::TarGzip => with_tar_gzip_reader(path, archive_member, callback),
    }
}
