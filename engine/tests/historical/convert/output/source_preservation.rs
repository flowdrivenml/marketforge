use std::{path::PathBuf, time::Instant};

use {
    marketforge_engine::historical::convert::output::validate_source_preservation,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn validates_full_source_preservation() {
    let root = root();

    let source = root.join("data/merged/conversion-multihop");

    let converted = root.join("data/converted/multihop-USDT");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&std::fs::read(source.join("manifest.json")).unwrap()).unwrap();

    let start = Instant::now();

    let metrics = validate_source_preservation(&source, &manifest, &converted).unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    assert_eq!(metrics.events_compared, 12_627_646);
    assert_eq!(metrics.columns_compared, 24);

    assert_eq!(metrics.converted_files, 1,);

    println!();
    println!("{}", "=".repeat(80));
    println!("MARKETFORGE — SOURCE PRESERVATION VALIDATION");
    println!("{}", "=".repeat(80));

    println!("Events compared   : {}", metrics.events_compared);
    println!("Columns compared  : {}", metrics.columns_compared);
    println!("Source batches    : {}", metrics.source_batches);
    println!("Converted batches : {}", metrics.converted_batches);
    println!("Converted files   : {}", metrics.converted_files);
    println!("Elapsed           : {:.3} s", elapsed);

    println!("STATUS            : PASS");
}
