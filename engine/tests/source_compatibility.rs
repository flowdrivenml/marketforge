use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use marketforge_engine::{
    error::{MarketForgeError, Result},
    job::SourceContainer,
    source::with_source_reader,
};

#[test]
fn representative_raw_sources_can_be_opened() {
    let root = Path::new("../tests/fixtures/raw");

    assert!(
        root.exists(),
        "raw fixture directory does not exist: {}",
        root.display(),
    );

    let files = collect_representative_files(root);

    assert!(!files.is_empty(), "no raw fixture files found");

    println!();
    println!("MarketForge source compatibility");
    println!("================================");
    println!("Scope: spot + linear/inverse perpetuals");
    println!("Options: excluded");
    println!();

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut failures = Vec::new();

    for path in &files {
        let Some(container) = detect_container(path) else {
            continue;
        };

        print!(
            "TEST  {:<8}  {} ... ",
            container_name(container),
            path.display(),
        );

        let result = with_source_reader(path, container, None, read_prefix);

        match result {
            Ok(bytes_read) if bytes_read > 0 => {
                println!("PASS ({bytes_read} bytes)");
                passed += 1;
            }

            Ok(_) => {
                println!("FAIL (empty source)");
                failed += 1;

                failures.push(format!("{}: source is empty", path.display(),));
            }

            Err(error) => {
                println!("FAIL");
                println!("      {error}");

                failed += 1;

                failures.push(format!("{}: {error}", path.display(),));
            }
        }
    }

    println!();
    println!("================================");
    println!(
        "RESULT: {} passed, {} failed, {} total",
        passed,
        failed,
        passed + failed,
    );

    if !failures.is_empty() {
        println!();
        println!("Failures:");
        println!();

        for failure in &failures {
            println!("  - {failure}");
        }
    }

    assert!(
        failures.is_empty(),
        "{} source compatibility test(s) failed",
        failures.len(),
    );
}

fn read_prefix(reader: &mut dyn Read) -> Result<usize> {
    let mut buffer = [0_u8; 4096];

    reader
        .read(&mut buffer)
        .map_err(|source| MarketForgeError::SourceRead {
            path: PathBuf::from("<source stream>"),
            source,
        })
}

fn collect_representative_files(root: &Path) -> Vec<PathBuf> {
    let mut representatives = BTreeMap::<PathBuf, PathBuf>::new();

    collect_recursive(root, &mut representatives);

    representatives.into_values().collect()
}

fn collect_recursive(directory: &Path, representatives: &mut BTreeMap<PathBuf, PathBuf>) {
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display(),));

    for entry in entries {
        let entry = entry.expect("read directory entry");

        let path = entry.path();

        if path.is_dir() {
            if is_option_directory(&path) {
                continue;
            }

            collect_recursive(&path, representatives);

            continue;
        }

        if !is_data_file(&path) {
            continue;
        }

        let parent = path
            .parent()
            .expect("fixture file has parent")
            .to_path_buf();

        representatives
            .entry(parent)
            .and_modify(|current| {
                if path < *current {
                    *current = path.clone();
                }
            })
            .or_insert(path);
    }
}

fn is_option_directory(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some("option")
}

fn is_data_file(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();

    if name.ends_with(".manifest.json") || name.ends_with(".part") {
        return false;
    }

    name.ends_with(".csv.gz") || name.ends_with(".zip") || name.ends_with(".tar.gz")
}

fn detect_container(path: &Path) -> Option<SourceContainer> {
    let name = path.file_name()?.to_str()?;

    if name.ends_with(".tar.gz") {
        return Some(SourceContainer::TarGzip);
    }

    if name.ends_with(".csv.gz") {
        return Some(SourceContainer::Gzip);
    }

    if name.ends_with(".zip") {
        return Some(SourceContainer::Zip);
    }

    None
}

fn container_name(container: SourceContainer) -> &'static str {
    match container {
        SourceContainer::Plain => "plain",
        SourceContainer::Gzip => "gzip",
        SourceContainer::Zip => "zip",
        SourceContainer::TarGzip => "tar.gz",
    }
}
