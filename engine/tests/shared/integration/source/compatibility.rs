//! Self-contained transport compatibility. Member enumeration deliberately stays
//! separate from the low-level unique-member reader contract.
use marketforge_engine::shared::{
    error::{MarketForgeError, Result},
    io::{container::SourceContainer, source::with_source_reader},
};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};
const FIRST: &[u8] = b"timestamp,price,quantity\n1000,123.45,2\n1001,123.46,3\n";
const SECOND: &[u8] = b"timestamp,price,quantity\n2000,125.00,4\n";
fn read_all(reader: &mut dyn Read) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|source| MarketForgeError::SourceRead {
            path: "synthetic source".into(),
            source,
        })?;
    Ok(bytes)
}
fn zip(path: &Path, members: &[(&str, &[u8])]) {
    let mut writer = zip::ZipWriter::new(File::create(path).unwrap());
    for (name, bytes) in members {
        writer
            .start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(bytes).unwrap();
    }
    drop(writer.finish().unwrap());
}
fn tar(path: &Path, members: &[(&str, &[u8])]) {
    let gzip =
        flate2::write::GzEncoder::new(File::create(path).unwrap(), flate2::Compression::fast());
    let mut writer = tar::Builder::new(gzip);
    for (name, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(bytes.len() as u64);
        header.set_mtime(0);
        header.set_cksum();
        writer.append_data(&mut header, *name, *bytes).unwrap();
    }
    drop(writer.into_inner().unwrap().finish().unwrap());
}
#[test]
fn synthetic_sources_decode_exact_payloads_and_preserve_member_selection() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("data/tests/historical/normalize/completion/policies/source-compatibility");
    std::fs::create_dir_all(&base).unwrap();
    let temp = tempfile::tempdir_in(base).unwrap();
    let gzip_path = temp.path().join("single.csv.gz");
    let mut gzip = flate2::write::GzEncoder::new(
        File::create(&gzip_path).unwrap(),
        flate2::Compression::fast(),
    );
    gzip.write_all(FIRST).unwrap();
    drop(gzip.finish().unwrap());
    assert_eq!(
        with_source_reader(&gzip_path, SourceContainer::Gzip, None, read_all).unwrap(),
        FIRST
    );
    let mut executed = 1;
    for container in [SourceContainer::Zip, SourceContainer::TarGzip] {
        let path = temp.path().join(if container == SourceContainer::Zip {
            "source.zip"
        } else {
            "source.tar.gz"
        });
        let write = |members: &[(&str, &[u8])]| {
            if container == SourceContainer::Zip {
                zip(&path, members)
            } else {
                tar(&path, members)
            }
        };
        write(&[("first.csv", FIRST)]);
        assert_eq!(
            with_source_reader(&path, container, None, read_all).unwrap(),
            FIRST
        );
        assert_eq!(
            with_source_reader(&path, container, Some("first.csv"), read_all).unwrap(),
            FIRST
        );
        write(&[("first.csv", FIRST), ("second.csv", SECOND)]);
        assert!(matches!(
            with_source_reader(&path, container, None, read_all),
            Err(MarketForgeError::AmbiguousArchiveMembers { .. })
        ));
        assert_eq!(
            with_source_reader(&path, container, Some("second.csv"), read_all).unwrap(),
            SECOND
        );
        assert_eq!(
            with_source_reader(&path, container, Some("first.csv"), read_all).unwrap(),
            FIRST
        );
        executed += 1;
    }
    assert_eq!(executed, 3);
}
