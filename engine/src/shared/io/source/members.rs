//! Forward, fully validated member traversal for family sources. No extraction and
//! no member payload inventory: callers consume each bounded decoder in place.
use crate::shared::{
    error::{MarketForgeError, Result},
    historical::limits,
    io::container::SourceContainer,
};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufReader, Read},
    path::{Component, Path},
};

fn archive(path: &Path, message: impl ToString) -> MarketForgeError {
    MarketForgeError::ArchiveOpen {
        path: path.into(),
        message: message.to_string(),
    }
}
struct Inventory {
    names: BTreeSet<String>,
    bytes: u64,
}
impl Inventory {
    #[cfg(test)]
    fn add(&mut self, path: &Path, name: &str) -> Result<()> {
        self.add_accounted(path, name, &mut |_| Ok(()))
    }
    fn add_accounted(
        &mut self,
        path: &Path,
        name: &str,
        account: &mut dyn FnMut(u64) -> Result<()>,
    ) -> Result<()> {
        if name.is_empty()
            || name.contains('\\')
            || name.contains('\0')
            || name.as_bytes().get(1) == Some(&b':')
            || Path::new(name)
                .components()
                .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
            || name
                .strip_suffix('/')
                .unwrap_or(name)
                .split('/')
                .any(|c| c.is_empty() || c == "." || c == "..")
        {
            return Err(archive(
                path,
                format!("unsafe archive member path {name:?}"),
            ));
        }
        let bytes = self
            .bytes
            .checked_add(name.len() as u64 + 512)
            .ok_or_else(|| archive(path, "archive inventory overflow"))?;
        if let Some(l) = limits::current() {
            limits::check(
                "archive member names",
                l.record_bytes,
                name.len() as u64,
                "archive member inventory",
            )?;
            limits::check(
                "archive members",
                l.metric_entries,
                self.names.len() as u64 + 1,
                "archive member inventory",
            )?;
            limits::check(
                "archive inventory bytes",
                l.reconciliation_bytes,
                bytes,
                "archive member inventory",
            )?;
        }
        if self.names.contains(name) {
            return Err(archive(
                path,
                format!("duplicate archive member name {name}"),
            ));
        }
        account(bytes)?;
        self.names.insert(name.to_owned());
        self.bytes = bytes;
        Ok(())
    }
}
fn drain(path: &Path, reader: &mut dyn Read) -> Result<()> {
    std::io::copy(reader, &mut std::io::sink())
        .map_err(|source| limits::source_error(path.into(), source))?;
    Ok(())
}
/// Every regular member is visited once. Filtering is performed by the callback;
/// it cannot skip member/path/type checks or compression trailer validation.
pub fn with_validated_source_members(
    path: impl AsRef<Path>,
    container: SourceContainer,
    callback: impl FnMut(Option<&str>, &mut dyn Read) -> Result<()>,
) -> Result<()> {
    with_validated_source_members_accounted(path, container, |_| Ok(()), callback)
}
/// The accounting callback reserves the complete live inventory before growth.
/// Its caller owns/relinquishes that reservation on both success and failure.
pub fn with_validated_source_members_accounted(
    path: impl AsRef<Path>,
    container: SourceContainer,
    mut account: impl FnMut(u64) -> Result<()>,
    mut callback: impl FnMut(Option<&str>, &mut dyn Read) -> Result<()>,
) -> Result<()> {
    let path = path.as_ref();
    let mut inventory = Inventory {
        names: BTreeSet::new(),
        bytes: 0,
    };
    match container {
        SourceContainer::Plain | SourceContainer::Gzip => {
            super::with_validated_source_reader(path, container, None, |reader| {
                callback(None, reader)
            })
        }
        SourceContainer::TarGzip => {
            let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
                path: path.into(),
                source,
            })?;
            let decoder = flate2::read::MultiGzDecoder::new(BufReader::new(file));
            let mut tar = tar::Archive::new(decoder);
            for entry in tar.entries().map_err(|e| archive(path, e))? {
                let mut entry = entry.map_err(|e| archive(path, e))?;
                let name = entry
                    .path()
                    .map_err(|e| archive(path, e))?
                    .to_str()
                    .ok_or_else(|| archive(path, "non-UTF8 archive member name"))?
                    .to_owned();
                inventory.add_accounted(path, &name, &mut account)?;
                let kind = entry.header().entry_type();
                if kind.is_dir() {
                    continue;
                }
                if !kind.is_file() || name.ends_with('/') {
                    return Err(archive(
                        path,
                        format!("unsupported archive entry type: {name}"),
                    ));
                }
                callback(Some(&name), &mut entry)?;
                drain(path, &mut entry)?;
            }
            drain(path, &mut tar.into_inner())
        }
        SourceContainer::Zip => {
            let file = File::open(path).map_err(|source| MarketForgeError::SourceOpen {
                path: path.into(),
                source,
            })?;
            let mut zip = zip::ZipArchive::new(file).map_err(|e| archive(path, e))?;
            if let Some(l) = limits::current() {
                limits::check(
                    "archive members",
                    l.metric_entries,
                    zip.len() as u64,
                    "archive member inventory",
                )?;
            }
            for i in 0..zip.len() {
                let mut member = zip.by_index(i).map_err(|e| archive(path, e))?;
                let name = member.name().map_err(|e| archive(path, e))?.into_owned();
                inventory.add_accounted(path, &name, &mut account)?;
                let directory = member.is_dir();
                if member.unix_mode().is_some_and(|mode| {
                    let kind = mode & 0o170000;
                    if directory {
                        !matches!(kind, 0 | 0o040000)
                    } else {
                        !matches!(kind, 0 | 0o100000)
                    }
                }) {
                    return Err(archive(
                        path,
                        format!("unsupported archive entry type: {name}"),
                    ));
                }
                if directory {
                    continue;
                }
                callback(Some(&name), &mut member)?;
                drain(path, &mut member)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn directory() -> tempfile::TempDir {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("data/tests/historical/normalize/completion/member-safety-unit");
        std::fs::create_dir_all(&path).unwrap();
        tempfile::tempdir_in(path).unwrap()
    }
    #[test]
    fn unsafe_normalized_paths_duplicates_and_inventory_limits_fail() {
        for name in [
            "../data", "/data", "./data", "a/../b", "a//b", "C:/data", "a\\b", "",
        ] {
            let mut inventory = Inventory {
                names: BTreeSet::new(),
                bytes: 0,
            };
            assert!(
                inventory.add(Path::new("synthetic.tar.gz"), name).is_err(),
                "{name:?}"
            );
        }
        let mut inventory = Inventory {
            names: BTreeSet::new(),
            bytes: 0,
        };
        inventory
            .add(Path::new("synthetic.tar.gz"), "a.data")
            .unwrap();
        assert!(
            inventory
                .add(Path::new("synthetic.tar.gz"), "a.data")
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
        let _scope = limits::LimitScope::enter(
            limits::NormalizationLimits {
                metric_entries: 1,
                ..Default::default()
            },
            "synthetic member inventory".into(),
        );
        let error = inventory
            .add(Path::new("synthetic.tar.gz"), "b.data")
            .unwrap_err();
        assert!(matches!(error, MarketForgeError::ResourceLimit(_)));
    }
    #[test]
    fn skipping_payload_does_not_bypass_unsupported_tar_entry_validation() {
        let directory = directory();
        let path = directory.path().join("unsafe.tar.gz");
        let gzip = flate2::write::GzEncoder::new(
            File::create(&path).unwrap(),
            flate2::Compression::fast(),
        );
        let mut builder = tar::Builder::new(gzip);
        let mut header = tar::Header::new_gnu();
        header.set_size(2);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "good.data", b"{}".as_slice())
            .unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o644);
        header.set_link_name("good.data").unwrap();
        header.set_cksum();
        builder
            .append_data(&mut header, "link.data", std::io::empty())
            .unwrap();
        builder.into_inner().unwrap().finish().unwrap();
        let mut visited = 0;
        let error = with_validated_source_members(&path, SourceContainer::TarGzip, |_, _| {
            visited += 1;
            Ok(())
        })
        .unwrap_err();
        assert_eq!(visited, 1);
        assert!(error.to_string().contains("unsupported archive entry type"));
    }
}
