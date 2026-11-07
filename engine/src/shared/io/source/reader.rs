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

/// Full normalization source consumption validates compression trailers even when
/// a TAR member ends before the outer compressed stream. Prefix inspection uses
/// with_source_reader explicitly and makes no full-container validation claim.
pub fn with_validated_source_reader<T, F>(
    path: impl AsRef<Path>,
    container: SourceContainer,
    archive_member: Option<&str>,
    callback: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Read) -> Result<T>,
{
    let path = path.as_ref();
    if container == SourceContainer::TarGzip {
        return super::tar_gzip::with_tar_gzip_reader_validated(path, archive_member, callback);
    }
    with_source_reader(path, container, archive_member, |reader| {
        let result = callback(reader)?;
        std::io::copy(reader, &mut std::io::sink()).map_err(|source| {
            crate::shared::error::MarketForgeError::SourceRead {
                path: path.to_path_buf(),
                source,
            }
        })?;
        Ok(result)
    })
}
