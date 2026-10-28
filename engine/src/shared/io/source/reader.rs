use super::{with_gzip_reader, with_plain_reader, with_tar_gzip_reader, with_zip_reader};
use crate::shared::{error::Result, io::container::SourceContainer};
use std::{io::Read, path::Path};
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
